use std::{
    any::Any,
    collections::{HashMap, HashSet},
    io,
    panic::AssertUnwindSafe,
    sync::{Arc, mpsc},
};

use blobsdb::{BlobsDBWithFs, Chunk, MAX_CONCURRENT_OPS};
use bytes::Bytes;
use futures_util::future::FutureExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use ver_fs::Fs;
use ver_kv::KVStore;

use crate::{
    commons::{get_head, resolve_snapshot_id, rt, write_head, write_history},
    context::{FilesystemContext, KVStoreContext, read_context},
    errors::VerError,
    show::to_text,
    snap::Snapshot,
    sync::find_common_ancestor,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct PartialMerge {
    pub snapshot_id_cur: [u8; 32],
    pub snapshot_id_inc: [u8; 32],
    pub incoming_context: String,
    pub clean_merge: Vec<(String, Vec<Chunk>, bool)>,
    pub deleted_paths: Vec<String>,
    pub resolved_paths: Vec<(String, String)>,
    pub modified_conflicting_paths: Vec<(String, Vec<Chunk>, Vec<Chunk>, Option<String>)>,
    pub deleted_conflicting_paths: Vec<(String, Option<Vec<Chunk>>, Option<Vec<Chunk>>)>,
    pub added_conflicting_paths: Vec<(String, Vec<Chunk>, Vec<Chunk>)>,
}

struct PartialMergeInner {
    snapshot_id_cur: [u8; 32],
    snapshot_id_inc: [u8; 32],
    incoming_context: String,
    clean_merge: Vec<(String, Vec<Chunk>, bool)>,
    deleted_paths: Vec<String>,
    resolved_paths: Vec<(String, String)>,
    modified_conflicting_paths: HashMap<String, (Vec<Chunk>, Vec<Chunk>, Option<String>)>,
    deleted_conflicting_paths: HashMap<String, (Option<Vec<Chunk>>, Option<Vec<Chunk>>)>,
    added_conflicting_paths: HashMap<String, (Vec<Chunk>, Vec<Chunk>)>,
}

impl From<PartialMergeInner> for PartialMerge {
    fn from(value: PartialMergeInner) -> Self {
        Self {
            snapshot_id_cur: value.snapshot_id_cur,
            snapshot_id_inc: value.snapshot_id_inc,
            incoming_context: value.incoming_context,
            clean_merge: value.clean_merge,
            deleted_conflicting_paths: value
                .deleted_conflicting_paths
                .into_iter()
                .map(|(k, (a, b))| (k, a, b))
                .collect(),
            deleted_paths: value.deleted_paths,
            resolved_paths: value.resolved_paths,
            added_conflicting_paths: value
                .added_conflicting_paths
                .into_iter()
                .map(|(k, (a, b))| (k, a, b))
                .collect(),
            modified_conflicting_paths: value
                .modified_conflicting_paths
                .into_iter()
                .map(|(k, (a, b, c))| (k, a, b, c))
                .collect(),
        }
    }
}

impl From<PartialMerge> for PartialMergeInner {
    fn from(value: PartialMerge) -> Self {
        Self {
            snapshot_id_cur: value.snapshot_id_cur,
            snapshot_id_inc: value.snapshot_id_inc,
            incoming_context: value.incoming_context,
            clean_merge: value.clean_merge,
            deleted_conflicting_paths: value
                .deleted_conflicting_paths
                .into_iter()
                .map(|(k, a, b)| (k, (a, b)))
                .collect(),
            deleted_paths: value.deleted_paths,
            resolved_paths: value.resolved_paths,
            added_conflicting_paths: value
                .added_conflicting_paths
                .into_iter()
                .map(|(k, a, b)| (k, (a, b)))
                .collect(),
            modified_conflicting_paths: value
                .modified_conflicting_paths
                .into_iter()
                .map(|(k, a, b, c)| (k, (a, b, c)))
                .collect(),
        }
    }
}

pub fn panic_msg(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

// get paths in base U ours U theirs
fn get_union_paths<'a>(
    base: &'a Snapshot,
    ours: &'a Snapshot,
    theirs: &'a Snapshot,
) -> HashSet<&'a str> {
    let mut set = HashSet::new();

    for k in base.repository_state.keys() {
        set.insert(k.as_str());
    }

    for o in ours.repository_state.keys() {
        set.insert(o.as_str());
    }

    for t in theirs.repository_state.keys() {
        set.insert(t.as_str());
    }

    set
}

fn compare_chunks(a: &[Chunk], b: &[Chunk]) -> bool {
    a.iter().all(|e| b.contains(e))
}

/// Concurrently fetch blob content for all conflicting paths that need a
/// three-way merge attempt, then resolve them serially.
///
/// Returns a map from path to the fetched (base, ours, theirs) contents.
fn fetch_conflict_contents(
    blobdb_a: &Arc<blobsdb::BlobsDBWithFs>,
    blobdb_b: &Arc<blobsdb::BlobsDBWithFs>,
    conflicts: &[(String, Vec<Chunk>, Vec<Chunk>, Vec<Chunk>)], // (path, base_chunks, our_chunks, their_chunks)
) -> Result<HashMap<String, (Vec<u8>, Vec<u8>, Vec<u8>)>, VerError> {
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (tx, rx) = mpsc::channel::<(usize, Result<(Vec<u8>, Vec<u8>, Vec<u8>), String>)>();

    for (i, (_, base_chunks, our_chunks, their_chunks)) in conflicts.iter().enumerate() {
        let tx = tx.clone();
        let permit = rt()
            .block_on(Arc::clone(&sem).acquire_owned())
            .expect("semaphore closed");
        let db_a = blobdb_a.clone();
        let db_b = blobdb_b.clone();
        let base_chunks = Arc::new(base_chunks.clone());
        let our_chunks = Arc::new(our_chunks.clone());
        let their_chunks = Arc::new(their_chunks.clone());

        rt().spawn(async move {
            let _permit = permit;
            let res = match AssertUnwindSafe(async move {
                let (base, our, their) = tokio::try_join!(
                    db_a.get(base_chunks),
                    db_a.get(our_chunks),
                    db_b.get(their_chunks),
                )?;
                Ok::<_, String>((base, our, their))
            })
            .catch_unwind()
            .await
            {
                Ok(r) => r,
                Err(payload) => Err(panic_msg(payload)),
            };
            let _ = tx.send((i, res));
        });
    }

    drop(tx);

    let mut out: Vec<Option<_>> = (0..conflicts.len()).map(|_| None).collect();
    for (i, res) in rx {
        out[i] = Some(res);
    }
    let results = out
        .into_iter()
        .map(|slot| slot.unwrap_or(Err("Lost conflict content".to_string())))
        .collect::<Vec<Result<(Vec<u8>, Vec<u8>, Vec<u8>), String>>>();

    let mut map = HashMap::new();
    for (i, r) in results.into_iter().enumerate() {
        let content = r.map_err(VerError::from)?;
        map.insert(conflicts[i].0.clone(), content);
    }

    Ok(map)
}

/// Concurrently write merged files to disk and ensure their chunks exist in
/// the local blob store.
fn write_merged_files(
    blobdb_a: &Arc<blobsdb::BlobsDBWithFs>,
    blobdb_b: &Arc<blobsdb::BlobsDBWithFs>,
    merge_state: &[(String, Vec<Chunk>, bool)],
) -> Result<(), VerError> {
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (tx, rx) = mpsc::channel::<(usize, Result<Vec<u8>, String>)>();

    for (i, (_, chunks, is_ours)) in merge_state.iter().enumerate() {
        let tx = tx.clone();
        let permit = rt()
            .block_on(Arc::clone(&sem).acquire_owned())
            .expect("semaphore closed");
        let db = if *is_ours {
            blobdb_a.clone()
        } else {
            blobdb_b.clone()
        };
        let chunks = Arc::new(chunks.clone());

        rt().spawn(async move {
            let _permit = permit;
            let res = match AssertUnwindSafe(async move { db.get(chunks).await })
                .catch_unwind()
                .await
            {
                Ok(r) => r,
                Err(payload) => Err(panic_msg(payload)),
            };
            let _ = tx.send((i, res));
        });
    }

    drop(tx);

    let mut out: Vec<Option<_>> = (0..merge_state.len()).map(|_| None).collect();
    for (i, res) in rx {
        out[i] = Some(res);
    }
    let results = out
        .into_iter()
        .map(|slot| slot.unwrap_or(Err("Lost merged file".to_string())))
        .collect::<Vec<Result<Vec<u8>, String>>>();

    // Write files serially (fs::write is fast, no need for concurrency)
    // and collect paths that need to be re-put into blobdb_a

    let mut paths_to_put: Vec<&str> = vec![];
    rt().block_on(async {
        for (i, r) in results.into_iter().enumerate() {
            let content = r.map_err(VerError::from)?;
            let (path, _, is_ours) = &merge_state[i];
            blobdb_a.fs.write(path, Bytes::from_owner(content)).await?;
            if !is_ours {
                paths_to_put.push(path);
            }
        }
        Ok::<(), VerError>(())
    })?;

    // Concurrently put files from the other repo into our blob store
    if !paths_to_put.is_empty() {
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
        let (tx, rx) = mpsc::channel::<(usize, Result<Vec<Chunk>, String>)>();

        for (i, path) in paths_to_put.iter().enumerate() {
            let tx = tx.clone();
            let permit = rt()
                .block_on(Arc::clone(&sem).acquire_owned())
                .expect("semaphore closed");
            let db = blobdb_a.clone();
            let path = path.to_string();

            rt().spawn(async move {
                let _permit = permit;
                let res = match AssertUnwindSafe(async move { db.put(&path).await })
                    .catch_unwind()
                    .await
                {
                    Ok(r) => r,
                    Err(payload) => Err(panic_msg(payload)),
                };
                let _ = tx.send((i, res));
            });
        }

        drop(tx);

        let mut out: Vec<Option<_>> = (0..paths_to_put.len()).map(|_| None).collect();
        for (i, res) in rx {
            out[i] = Some(res);
        }
        for slot in out {
            slot.unwrap_or(Err("Lost put result".to_string()))
                .map_err(VerError::from)?;
        }
    }

    Ok(())
}

/// Concurrently write resolved files to disk and put them into the blob store.
/// Returns the repo_state entries for resolved paths.
fn write_resolved_files(
    blobdb_a: &Arc<blobsdb::BlobsDBWithFs>,
    resolved_paths: &[(String, String)],
) -> Result<Vec<(String, Vec<Chunk>)>, VerError> {
    // Write resolved files to disk first
    rt().block_on(async {
        for (path, content) in resolved_paths {
            blobdb_a
                .fs
                .write(path, Bytes::copy_from_slice(content.as_bytes()))
                .await?;
        }
        Ok::<(), VerError>(())
    })?;

    // Concurrently put them into the blob store
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (tx, rx) = mpsc::channel::<(usize, Result<Vec<Chunk>, String>)>();

    for (i, (path, _)) in resolved_paths.iter().enumerate() {
        let tx = tx.clone();
        let permit = rt()
            .block_on(Arc::clone(&sem).acquire_owned())
            .expect("semaphore closed");
        let db = blobdb_a.clone();
        let path = path.clone();

        rt().spawn(async move {
            let _permit = permit;
            let res = match AssertUnwindSafe(async move { db.put(&path).await })
                .catch_unwind()
                .await
            {
                Ok(r) => r,
                Err(payload) => Err(panic_msg(payload)),
            };
            let _ = tx.send((i, res));
        });
    }

    drop(tx);

    let mut out: Vec<Option<_>> = (0..resolved_paths.len()).map(|_| None).collect();
    for (i, res) in rx {
        out[i] = Some(res);
    }
    let results = out
        .into_iter()
        .map(|slot| slot.unwrap_or(Err("Lost resolved file put".to_string())))
        .collect::<Vec<Result<Vec<Chunk>, String>>>();

    let mut entries = vec![];
    for (i, r) in results.into_iter().enumerate() {
        let chunks = r.map_err(VerError::from)?;
        entries.push((resolved_paths[i].0.clone(), chunks));
    }

    Ok(entries)
}

pub fn merge_run<O: io::Write, E: io::Write>(
    other_context: String,
    name: &str,
    snapshot_id_cur: Option<String>,
    snapshot_id_inc: Option<String>,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let unwrap_ctx = |opt: Option<(FilesystemContext, KVStoreContext)>,
                      context_name: &str|
     -> Result<(Arc<dyn Fs>, Arc<dyn KVStore>), VerError> {
        if opt.is_none() {
            return Err(format!("Context {context_name} found").into());
        }
        let (fs_des, kv_des) = opt.unwrap();
        let fs = fs_des.build_fs()?;
        let kv = kv_des.build_kv()?;
        Ok((fs, kv))
    };
    let ctx = read_context()?;
    let opt_ours = ctx.current_context();
    let opt_theirs = ctx.get_context(&other_context).ok();
    let (fs_a, kv_a) = unwrap_ctx(opt_ours, "current")?;
    let (fs_b, kv_b) = unwrap_ctx(opt_theirs, &other_context)?;

    let blobdb_a = Arc::new(BlobsDBWithFs::new(fs_a.clone()));
    let blobdb_b = Arc::new(BlobsDBWithFs::new(fs_b.clone()));

    let (snapshot_id_a, snapshot_id_b) = if let Some(ref cur) = snapshot_id_cur
        && let Some(ref inc) = snapshot_id_inc
    {
        (
            resolve_snapshot_id(cur, &kv_a)?,
            resolve_snapshot_id(inc, &kv_b)?,
        )
    } else if snapshot_id_cur.is_none() && snapshot_id_inc.is_none() {
        (
            get_head(&fs_a)?.expect("Should have at least one entry"),
            get_head(&fs_b)?.expect("Should have at least one entry"),
        )
    } else if let Some(ref cur) = (snapshot_id_cur)
        && snapshot_id_inc.is_none()
    {
        (
            resolve_snapshot_id(cur, &kv_a)?,
            get_head(&fs_b)?.expect("Should have at least one entry"),
        )
    } else {
        (
            get_head(&fs_a)?.expect("Should have at least one entry"),
            resolve_snapshot_id(snapshot_id_inc.as_deref().unwrap(), &kv_b)?,
        )
    };

    let opt_a = kv_a.get_snapshot(snapshot_id_a)?;
    let opt_b = kv_b.get_snapshot(snapshot_id_b)?;

    if let Some(snap_ac) = opt_a
        && let Some(snap_bc) = opt_b
    {
        let (opt_base, _, _) = find_common_ancestor(&kv_a, &kv_b, snapshot_id_a, snapshot_id_b)?;

        if let Some(base) = opt_base {
            let snap_basec = kv_a.get_snapshot(base)?.expect(
                "Base should be contained in the snapshots table, since it was already found",
            );
            let ours: Snapshot = postcard::from_bytes(&snap_ac)?;
            let theirs: Snapshot = postcard::from_bytes(&snap_bc)?;
            let commons: Snapshot = postcard::from_bytes(&snap_basec)?;

            let union_paths = get_union_paths(&commons, &ours, &theirs);

            // path -> (chunks, is_ours)
            let mut merge_state: Vec<(String, Vec<Chunk>, bool)> = vec![];
            let mut deleted_paths: Vec<String> = vec![];
            let mut modified_conflicting_paths: HashMap<
                String,
                (Vec<Chunk>, Vec<Chunk>, Option<String>),
            > = HashMap::new();
            let mut deleted_conflicting_paths: HashMap<
                String,
                (Option<Vec<Chunk>>, Option<Vec<Chunk>>),
            > = HashMap::new();
            let mut added_conflicting_paths: HashMap<String, (Vec<Chunk>, Vec<Chunk>)> =
                HashMap::new();
            let mut resolved_paths: Vec<(String, String)> = vec![];

            // Collect paths that need three-way merge attempts (both modified)
            // so we can fetch their blob contents concurrently.
            let mut needs_three_way: Vec<(String, Vec<Chunk>, Vec<Chunk>, Vec<Chunk>)> = vec![];

            for p in union_paths {
                let their = theirs.repository_state.get(p);
                let common = commons.repository_state.get(p);
                let our = ours.repository_state.get(p);

                // both base and theirs have the same file as ours
                if let Some(their_chunks) = their
                    && let Some(common_chunks) = common
                    && let Some(our_chunks) = our
                {
                    if compare_chunks(their_chunks, our_chunks) {
                        // none changed or both changed identically, keep ours
                        merge_state.push((p.to_string(), our_chunks.clone(), true));
                    } else if compare_chunks(their_chunks, common_chunks)
                        && !compare_chunks(their_chunks, our_chunks)
                    {
                        // ours changed, theirs didn't, keep ours
                        merge_state.push((p.to_string(), our_chunks.clone(), true));
                    } else if !compare_chunks(their_chunks, common_chunks)
                        && compare_chunks(common_chunks, our_chunks)
                    {
                        // ours didn't change, theirs did, keep theirs
                        merge_state.push((p.to_string(), their_chunks.clone(), false));
                    } else {
                        // ours modified, theirs too -> CONFLICT!
                        // Defer to concurrent fetch + resolve
                        needs_three_way.push((
                            p.to_string(),
                            common_chunks.clone(),
                            our_chunks.clone(),
                            their_chunks.clone(),
                        ));
                    }
                } else if let Some(_) = common
                    && our.is_none()
                    && their.is_none()
                {
                    // both deleted
                    deleted_paths.push(p.to_string());
                } else if let Some(common_chunks) = common
                    && let Some(our_chunks) = our
                    && their.is_none()
                {
                    if compare_chunks(our_chunks, common_chunks) {
                        // theirs deleted, ours unchanged -> delete
                        deleted_paths.push(p.to_string());
                    } else {
                        // ours modified, theirs deleted -> CONFLICT!
                        deleted_conflicting_paths
                            .insert(p.to_string(), (Some(our_chunks.clone()), None));
                    }
                } else if let Some(common_chunks) = common
                    && our.is_none()
                    && let Some(their_chunks) = their
                {
                    if !compare_chunks(their_chunks, common_chunks) {
                        // theirs modified, ours deleted -> CONFLICT!
                        deleted_conflicting_paths
                            .insert(p.to_string(), (None, Some(their_chunks.clone())));
                    } else {
                        // theirs unchanged, ours deleted -> delete
                        deleted_paths.push(p.to_string());
                    }
                } else if common.is_none() {
                    if their.is_none()
                        && let Some(our_chunks) = our
                    {
                        // ours created -> keep ours
                        merge_state.push((p.to_string(), our_chunks.clone(), true));
                    } else if our.is_none()
                        && let Some(their_chunks) = their
                    {
                        // theirs created -> keep theirs
                        merge_state.push((p.to_string(), their_chunks.clone(), false));
                    } else if let Some(our_chunks) = our
                        && let Some(their_chunks) = their
                    {
                        if compare_chunks(our_chunks, their_chunks) {
                            // both added in the same way -> keep ours
                            merge_state.push((p.to_string(), our_chunks.clone(), true));
                        } else {
                            // both added, but in different ways -> CONFLICT!
                            added_conflicting_paths
                                .insert(p.to_string(), (our_chunks.clone(), their_chunks.clone()));
                        }
                    } else {
                        unreachable!(
                            "It cannot be that a path does not exist in either ours or theirs if it does not exist in commons"
                        )
                    }
                }
            }

            // Concurrently fetch all conflicting file contents and resolve
            if !needs_three_way.is_empty() {
                let contents = fetch_conflict_contents(&blobdb_a, &blobdb_b, &needs_three_way)?;

                for (path, _, our_chunks, their_chunks) in &needs_three_way {
                    let (base_content, our_content, their_content) =
                        contents.get(path).expect("Fetched above");

                    if let Some(our_text) = to_text(our_content)
                        && let Some(their_text) = to_text(their_content)
                        && let Some(base_text) = to_text(base_content)
                    {
                        let merged = diffy::merge(base_text, our_text, their_text);
                        match merged {
                            Ok(s) => resolved_paths.push((path.clone(), s)),
                            Err(s) => {
                                let _ = modified_conflicting_paths.insert(
                                    path.clone(),
                                    (our_chunks.clone(), their_chunks.clone(), Some(s)),
                                );
                            }
                        }
                    } else {
                        modified_conflicting_paths.insert(
                            path.clone(),
                            (our_chunks.clone(), their_chunks.clone(), None),
                        );
                    }
                }
            }

            if modified_conflicting_paths.is_empty()
                && deleted_conflicting_paths.is_empty()
                && added_conflicting_paths.is_empty()
            {
                let mut repo_state: HashMap<String, Vec<Chunk>> = HashMap::new();

                write_merged_files(&blobdb_a, &blobdb_b, &merge_state)?;

                for (path, chunks, _) in &merge_state {
                    repo_state.insert(path.clone(), chunks.clone());
                }

                rt().block_on(async {
                    for d in &deleted_paths {
                        fs_a.delete(d).await?;
                    }
                    Ok::<(), VerError>(())
                })?;

                for n in &resolved_paths {
                    writeln!(std_out, "Automatically resolved conflict on {}", n.0)?;
                }

                let resolved_entries = write_resolved_files(&blobdb_a, &resolved_paths)?;
                for (path, chunks) in resolved_entries {
                    repo_state.insert(path, chunks);
                }

                let snap = Snapshot {
                    repository_state: repo_state,
                    parent_ids: vec![snapshot_id_a, snapshot_id_b],
                    partial: false,
                };
                let to_insert = postcard::to_allocvec(&snap)?;
                let snap_hash = blake3::hash(&to_insert);

                kv_a.put_snapshot(snap_hash.as_bytes().to_owned(), &to_insert)?;
                kv_a.put_name(name, snap_hash.as_bytes().to_owned())?;

                write_head(snap_hash.as_bytes().to_owned(), &fs_a)?;

                write_history(
                    snap_hash,
                    name,
                    Some(&format!(
                        "merged repository at {} with {} at snapshot ID {}",
                        blake3::Hash::from_bytes(snapshot_id_a),
                        &other_context,
                        blake3::Hash::from_bytes(snapshot_id_b)
                    )),
                    &fs_a,
                )?;

                writeln!(
                    std_out,
                    "Merge successfully completed and saved under snapshot {} ({})",
                    name, snap_hash
                )?;
                return Ok(());
            } else {
                writeln!(
                    std_err,
                    "There are conflicts between the two repository, so the merge could not be completed. Find details below."
                )?;

                if !deleted_conflicting_paths.is_empty() {
                    writeln!(std_err, "Files with deletion-related conflicts:")?;
                    for (k, v) in &deleted_conflicting_paths {
                        let des = if v.1.is_some() {
                            "ours modified, theirs deleted"
                        } else {
                            "theirs modified, ours deleted"
                        };
                        writeln!(std_err, "{}\t{}", k, des)?;
                    }
                    writeln!(std_err)?;
                }

                if !added_conflicting_paths.is_empty() {
                    writeln!(std_err, "Files with creation-related conflicts:")?;
                    for f in &added_conflicting_paths {
                        writeln!(std_err, "{}\tboth added, different content", f.0)?;
                    }
                    writeln!(std_err)?;
                }

                if !modified_conflicting_paths.is_empty() {
                    writeln!(std_err, "Files with modification-related conflicts:")?;
                    for (k, v) in &modified_conflicting_paths {
                        if let Some(ref merge_conflict) = v.2 {
                            writeln!(std_err, "{}", k)?;
                            writeln!(std_err)?;
                            writeln!(std_err, "{}", merge_conflict)?;
                            writeln!(std_err)?;
                        } else {
                            writeln!(
                                std_err,
                                "{}\tFile does not appear to be text-based, could not compute merge conflicts",
                                k
                            )?;
                        }
                    }
                    writeln!(std_err)?;
                }

                writeln!(
                    std_err,
                    "Please resolve conflicts, then run `ver merge pick` to pick a conflicting file version (theirs or ours), then finalize the merge with `ver merge end`"
                )?;

                let partial_merge: PartialMerge = PartialMergeInner {
                    snapshot_id_cur: snapshot_id_a,
                    snapshot_id_inc: snapshot_id_b,
                    incoming_context: other_context,
                    clean_merge: merge_state,
                    added_conflicting_paths,
                    deleted_conflicting_paths,
                    modified_conflicting_paths,
                    resolved_paths,
                    deleted_paths,
                }
                .into();

                let compressed = postcard::to_allocvec(&partial_merge)?;
                kv_a.put_merge(name, compressed.as_slice())?;

                return Ok(());
            }
        }

        return Err(
            "Could not find a common base for the two repositories, merging is not possible".into(),
        );
    }

    Err("Could not resolve the last snapshot in either of the repositories".into())
}

pub fn merge_pick<O: io::Write, E: io::Write>(
    name: &str,
    use_ours: &[&str],
    use_theirs: &[&str],
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let cur_ctx = ctx.current_context();
    if cur_ctx.is_none() {
        return Err("No current context found".into());
    }
    let (_, kv_des) = cur_ctx.unwrap();
    let kv = kv_des.build_kv()?;
    let opt = kv.get_merge(name)?;

    if let Some(compressed) = opt {
        let partial_merge: PartialMerge = postcard::from_bytes(&compressed)?;
        let mut inner = PartialMergeInner::from(partial_merge);
        for path in use_ours {
            let path_owned = path.to_string();
            if let Some(del) = inner.deleted_conflicting_paths.get(*path) {
                let del = del.clone();
                if let Some(ours) = del.0 {
                    inner.clean_merge.push((path_owned.clone(), ours, true));
                } else {
                    inner.deleted_paths.push(path_owned.clone());
                }
                inner.deleted_conflicting_paths.remove(*path);
            } else if let Some(add) = inner.added_conflicting_paths.get(*path) {
                let add = add.clone();
                inner.clean_merge.push((path_owned.clone(), add.0, true));
                inner.added_conflicting_paths.remove(*path);
            } else if let Some(modified) = inner.modified_conflicting_paths.get(*path) {
                let modified = modified.clone();
                inner
                    .clean_merge
                    .push((path_owned.clone(), modified.0, true));
                inner.modified_conflicting_paths.remove(*path);
            } else {
                writeln!(
                    std_err,
                    "Path {} does not appear to have any conflicts in merge {}",
                    path, name,
                )?;
            }
        }
        for path in use_theirs {
            let path_owned = path.to_string();
            if let Some(del) = inner.deleted_conflicting_paths.get(*path) {
                let del = del.clone();
                if let Some(theirs) = del.1 {
                    inner.clean_merge.push((path_owned.clone(), theirs, false));
                } else {
                    inner.deleted_paths.push(path_owned.clone());
                }
                inner.deleted_conflicting_paths.remove(*path);
            } else if let Some(add) = inner.added_conflicting_paths.get(*path) {
                let add = add.clone();
                inner.clean_merge.push((path_owned.clone(), add.1, false));
                inner.added_conflicting_paths.remove(*path);
            } else if let Some(modified) = inner.modified_conflicting_paths.get(*path) {
                let modified = modified.clone();
                inner
                    .clean_merge
                    .push((path_owned.clone(), modified.1, false));
                inner.modified_conflicting_paths.remove(*path);
            } else {
                writeln!(
                    std_err,
                    "Path {} does not appear to have any conflicts in merge {}",
                    path, name,
                )?;
            }
        }
        let new_merge = PartialMerge::from(inner);
        let new_compressed = postcard::to_allocvec(&new_merge)?;
        kv.put_merge(name, &new_compressed)?;

        writeln!(std_out, "Successfully applied merge picks")?;
        return Ok(());
    }

    Err(format!("Could not find partial merge under {}", name).into())
}

pub fn merge_end<O: io::Write, E: io::Write>(
    name: &str,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let cur_ctx = ctx.current_context();
    if cur_ctx.is_none() {
        return Err("No current context found".into());
    }
    let (fs_des, kv_des) = cur_ctx.unwrap();
    let kv_a = kv_des.build_kv()?;
    let fs_a = fs_des.build_fs()?;
    let opt = kv_a.get_merge(name)?;

    if let Some(compressed) = opt {
        let partial_merge: PartialMerge = postcard::from_bytes(&compressed)?;

        if !partial_merge.added_conflicting_paths.is_empty()
            || !partial_merge.deleted_conflicting_paths.is_empty()
            || !partial_merge.modified_conflicting_paths.is_empty()
        {
            writeln!(
                std_err,
                "You still need to resolve the following conflicts:\n"
            )?;
            if !partial_merge.added_conflicting_paths.is_empty() {
                writeln!(
                    std_err,
                    "Addition-related conflicts:\n- {}",
                    partial_merge
                        .added_conflicting_paths
                        .iter()
                        .map(|e| e.0.as_str())
                        .collect::<Vec<&str>>()
                        .join("\n- ")
                )?;
                writeln!(std_err)?;
            }
            if !partial_merge.deleted_conflicting_paths.is_empty() {
                writeln!(
                    std_err,
                    "Deletion-related conflicts:\n- {}",
                    partial_merge
                        .deleted_conflicting_paths
                        .iter()
                        .map(|s| s.0.as_str())
                        .collect::<Vec<&str>>()
                        .join("\n- ")
                )?;
                writeln!(std_err)?;
            }
            if !partial_merge.modified_conflicting_paths.is_empty() {
                writeln!(
                    std_err,
                    "Modification-related conflicts:\n- {}",
                    partial_merge
                        .modified_conflicting_paths
                        .iter()
                        .map(|s| s.0.as_str())
                        .collect::<Vec<&str>>()
                        .join("\n- ")
                )?;
                writeln!(std_err)?;
            }
            writeln!(
                std_err,
                "Please resolve conflicts, then run `ver merge pick` to pick a conflicting file version (theirs or ours), then finalize the merge with `ver merge end`"
            )?;
            return Ok(());
        }

        let inner = PartialMergeInner::from(partial_merge);
        let (fs_des_b, _) = ctx.get_context(&inner.incoming_context)?;
        let fs_b = fs_des_b.build_fs()?;

        let blobdb_a = Arc::new(BlobsDBWithFs::new(fs_a.clone()));
        let blobdb_b = Arc::new(BlobsDBWithFs::new(fs_b.clone()));

        let mut repo_state: HashMap<String, Vec<Chunk>> = HashMap::new();

        write_merged_files(&blobdb_a, &blobdb_b, &inner.clean_merge)?;

        for (path, chunks, _) in &inner.clean_merge {
            repo_state.insert(path.clone(), chunks.clone());
        }

        rt().block_on(async {
            for d in &inner.deleted_paths {
                fs_a.delete(d).await?;
            }
            Ok::<(), VerError>(())
        })?;

        for n in &inner.resolved_paths {
            writeln!(std_out, "Automatically resolved conflict on {}", n.0)?;
        }

        let resolved_entries = write_resolved_files(&blobdb_a, &inner.resolved_paths)?;
        for (path, chunks) in resolved_entries {
            repo_state.insert(path, chunks);
        }

        let snap = Snapshot {
            repository_state: repo_state,
            parent_ids: vec![inner.snapshot_id_cur, inner.snapshot_id_inc],
            partial: false,
        };
        let to_insert = postcard::to_allocvec(&snap)?;
        let snap_hash = blake3::hash(&to_insert);

        kv_a.put_snapshot(snap_hash.as_bytes().to_owned(), &to_insert)?;
        kv_a.put_name(name, snap_hash.as_bytes().to_owned())?;

        write_head(snap_hash.as_bytes().to_owned(), &fs_a)?;
        write_history(
            snap_hash,
            name,
            Some(&format!(
                "merged repository at {} with {:?} at snapshot ID {}",
                blake3::Hash::from_bytes(inner.snapshot_id_cur),
                inner.incoming_context,
                blake3::Hash::from_bytes(inner.snapshot_id_inc),
            )),
            &fs_a,
        )?;

        kv_a.delete_merge(name)?;

        writeln!(
            std_out,
            "Merge successfully completed and saved under snapshot {} ({})",
            name, snap_hash
        )?;

        return Ok(());
    }

    Err(format!("Could not find partial merge under {}", name).into())
}
