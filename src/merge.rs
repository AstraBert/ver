use std::{
    collections::{HashMap, HashSet, VecDeque},
    env, fs,
    io::Write,
    path::PathBuf,
    str::FromStr,
};

use redb::{Database, ReadOnlyTable, ReadableDatabase, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};

use crate::{
    commons::resolve_ver_dir_path,
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot},
    init::{BLOBDB_PATH, REDB_PATH, SNAPSHOT_HISTORY_FILE, VER_DIRECTORY, init_blobs_db},
    named::{get_snapshot_by_name, name_snapshot},
};

pub const PARTIAL_MERGES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("partial_merges");

#[derive(Debug, Serialize, Deserialize)]
pub struct PartialMerge<'a> {
    #[serde(borrow)]
    pub clean_merge: Vec<(&'a str, &'a [u8], bool)>,
    pub deleted_paths: Vec<&'a str>,
    pub resolved_paths: Vec<(&'a str, String)>,
    #[serde(borrow)]
    pub modified_conflicting_paths: Vec<(&'a str, &'a [u8], &'a [u8], Option<String>)>,
    #[serde(borrow)]
    pub deleted_conflicting_paths: Vec<(&'a str, Option<&'a [u8]>, Option<&'a [u8]>)>,
    #[serde(borrow)]
    pub added_conflicting_paths: Vec<(&'a str, &'a [u8], &'a [u8])>,
}

struct PartialMergeInner<'a> {
    clean_merge: Vec<(&'a str, &'a [u8; 32], bool)>,
    deleted_paths: Vec<&'a str>,
    resolved_paths: Vec<(&'a str, String)>,
    modified_conflicting_paths: Vec<(&'a str, &'a [u8; 32], &'a [u8; 32], Option<String>)>,
    deleted_conflicting_paths: Vec<(&'a str, Option<&'a [u8; 32]>, Option<&'a [u8; 32]>)>,
    added_conflicting_paths: Vec<(&'a str, &'a [u8; 32], &'a [u8; 32])>,
}

impl<'a> From<PartialMergeInner<'a>> for PartialMerge<'a> {
    fn from(value: PartialMergeInner<'a>) -> Self {
        Self {
            clean_merge: value
                .clean_merge
                .iter()
                .map(|(s, h, b)| (*s, h.as_slice(), *b))
                .collect(),
            deleted_conflicting_paths: value
                .deleted_conflicting_paths
                .iter()
                .map(|(s, h1, h2)| (*s, h1.map(|h| h.as_slice()), h2.map(|h| h.as_slice())))
                .collect(),
            deleted_paths: value.deleted_paths,
            resolved_paths: value.resolved_paths,
            added_conflicting_paths: value
                .added_conflicting_paths
                .iter()
                .map(|(s, h1, h2)| (*s, h1.as_slice(), h2.as_slice()))
                .collect(),
            modified_conflicting_paths: value
                .modified_conflicting_paths
                .iter()
                .map(|(s, h1, h2, c)| (*s, h1.as_slice(), h2.as_slice(), c.to_owned()))
                .collect(),
        }
    }
}

fn to_text<'a>(content: &'a [u8]) -> Option<&'a str> {
    let buf = if content.len() > 8192 {
        &content[..8192]
    } else {
        content
    };
    if buf.contains(&0) {
        None
    } else {
        std::str::from_utf8(buf).ok()
    }
}

fn get_parents(
    table: &ReadOnlyTable<[u8; 32], &[u8]>,
    snapshot_id: [u8; 32],
) -> Result<Vec<[u8; 32]>, VerError> {
    let snap = table.get(snapshot_id)?;
    if let Some(s) = snap {
        let p: Snapshot = postcard::from_bytes(s.value())?;
        return Ok(p.parent_ids);
    }

    Err("Could not resolve snapshot ID".into())
}

fn find_merge_base(
    table_a: &ReadOnlyTable<[u8; 32], &[u8]>,
    table_b: &ReadOnlyTable<[u8; 32], &[u8]>,
    snapshot_id_a: [u8; 32],
    snapshot_id_b: [u8; 32],
) -> Result<Option<[u8; 32]>, VerError> {
    if snapshot_id_a == snapshot_id_b {
        return Ok(Some(snapshot_id_a));
    }

    let mut visited_a = HashSet::from([snapshot_id_a]);
    let mut visited_b = HashSet::from([snapshot_id_b]);
    let mut frontier_a = VecDeque::from([snapshot_id_a]);
    let mut frontier_b = VecDeque::from([snapshot_id_b]);

    while !frontier_a.is_empty() || !frontier_b.is_empty() {
        if !frontier_a.is_empty() {
            // we know it's not empty
            let cur_a = frontier_a.pop_front().unwrap();
            let parents = get_parents(&table_a, cur_a)?;
            for par in parents {
                if visited_b.contains(&par) {
                    return Ok(Some(par));
                }
                if !visited_a.contains(&par) {
                    visited_a.insert(par);
                    frontier_a.push_back(par);
                }
            }
        }

        if !frontier_b.is_empty() {
            // we know it's not empty
            let cur_b = frontier_b.pop_front().unwrap();
            let parents = get_parents(&table_b, cur_b)?;

            for par in parents {
                if visited_a.contains(&par) {
                    return Ok(Some(par));
                }

                if !visited_b.contains(&par) {
                    visited_b.insert(par);
                    frontier_b.push_back(par);
                }
            }
        }
    }

    Ok(None)
}

// get paths in base U ours U theirs
fn get_union_paths<'a>(
    base: &'a Snapshot,
    ours: &'a Snapshot,
    theirs: &'a Snapshot,
) -> HashSet<&'a str> {
    let mut set = HashSet::new();

    for (k, _) in &base.repository_state {
        set.insert(k.as_str());
    }

    for (o, _) in &ours.repository_state {
        set.insert(o.as_str());
    }

    for (t, _) in &theirs.repository_state {
        set.insert(t.as_str());
    }

    set
}

pub fn merge_run(
    other_repo: PathBuf,
    name: &str,
    snapshot_id_cur: Option<String>,
    snapshot_id_inc: Option<String>,
) -> Result<(), VerError> {
    let resolved = resolve_ver_dir_path(env::current_dir()?)?;

    let blobdb_a = init_blobs_db(&resolved.join(BLOBDB_PATH))?;
    let blobdb_b = init_blobs_db(&other_repo.join(VER_DIRECTORY).join(BLOBDB_PATH))?;

    let redb_a = Database::open(resolved.join(REDB_PATH))?;
    let redb_b = Database::open(other_repo.join(VER_DIRECTORY).join(REDB_PATH))?;

    let txa = redb_a.begin_read()?;
    let table_a = txa.open_table(SNAPSHOT_TABLE)?;

    let txb = redb_b.begin_read()?;
    let table_b = txb.open_table(SNAPSHOT_TABLE)?;

    let (snapshot_id_a, snapshot_id_b) = if let Some(ref cur) = snapshot_id_cur
        && let Some(ref inc) = snapshot_id_inc
    {
        let maybe_hash_cur = blake3::Hash::from_str(cur);

        let hash_cur = match maybe_hash_cur {
            Ok(h) => h.as_bytes().to_owned(),
            Err(_) => get_snapshot_by_name(&redb_a, cur)?,
        };

        let maybe_hash_inc = blake3::Hash::from_str(&inc);

        let hash_inc = match maybe_hash_inc {
            Ok(h) => h.as_bytes().to_owned(),
            Err(_) => get_snapshot_by_name(&redb_b, &inc)?,
        };

        (hash_cur, hash_inc)
    } else if snapshot_id_cur.is_none() && snapshot_id_inc.is_none() {
        (
            table_a
                .last()?
                .expect("Should have at least one entry")
                .0
                .value(),
            table_b
                .last()?
                .expect("Should have at least one entry")
                .0
                .value(),
        )
    } else if let Some(ref cur) = (snapshot_id_cur)
        && snapshot_id_inc.is_none()
    {
        let maybe_hash_cur = blake3::Hash::from_str(cur);

        let hash_cur = match maybe_hash_cur {
            Ok(h) => h.as_bytes().to_owned(),
            Err(_) => get_snapshot_by_name(&redb_a, cur)?,
        };
        (
            hash_cur,
            table_b
                .last()?
                .expect("Should have at least one entry")
                .0
                .value(),
        )
    } else {
        let inc = snapshot_id_inc.as_deref().unwrap();
        let maybe_hash_inc = blake3::Hash::from_str(&inc);

        let hash_inc = match maybe_hash_inc {
            Ok(h) => h.as_bytes().to_owned(),
            Err(_) => get_snapshot_by_name(&redb_b, &inc)?,
        };
        (
            table_a
                .last()?
                .expect("Should have at least one entry")
                .0
                .value(),
            hash_inc,
        )
    };

    let opt_a = table_a.get(snapshot_id_a)?;
    let opt_b = table_b.get(snapshot_id_b)?;

    if let Some(snap_ac) = opt_a
        && let Some(snap_bc) = opt_b
    {
        let opt_base = find_merge_base(&table_a, &table_b, snapshot_id_a, snapshot_id_b)?;

        if let Some(base) = opt_base {
            let snap_basec = table_a.get(base)?.expect(
                "Base should be contained in the snapshots table, since it was already found",
            );
            let ours: Snapshot = postcard::from_bytes(snap_ac.value())?;
            let theirs: Snapshot = postcard::from_bytes(snap_bc.value())?;
            let commons: Snapshot = postcard::from_bytes(snap_basec.value())?;

            let union_paths = get_union_paths(&commons, &ours, &theirs);

            // path -> (hash, is_ours)
            let mut merge_state: Vec<(&str, &[u8; 32], bool)> = vec![];
            let mut deleted_paths: Vec<&str> = vec![];
            let mut modified_conflicting_paths: Vec<(&str, &[u8; 32], &[u8; 32], Option<String>)> =
                vec![];
            let mut deleted_conflicting_paths: Vec<(&str, Option<&[u8; 32]>, Option<&[u8; 32]>)> =
                vec![];
            let mut added_conflicting_paths: Vec<(&str, &[u8; 32], &[u8; 32])> = vec![];
            let mut resolved_paths: Vec<(&str, String)> = vec![];

            for p in union_paths {
                let their = theirs.repository_state.get(p);
                let common = commons.repository_state.get(p);
                let our = ours.repository_state.get(p);

                // both base and theirs have the same file as ours
                if let Some(their_hash) = their
                    && let Some(common_hash) = common
                    && let Some(our_hash) = our
                {
                    if their_hash == our_hash {
                        // none changed or both changed identically, keep ours
                        merge_state.push((p, our_hash, true));
                    } else if (their_hash == common_hash) && (their_hash != our_hash) {
                        // ours changed, theirs didn't, keep ours
                        merge_state.push((p, our_hash, true));
                    } else if (their_hash != common_hash) && (common_hash == our_hash) {
                        // ours didn't change, theirs did, keep theirs
                        merge_state.push((p, their_hash, false));
                    } else {
                        // ours modified, theirs too -> CONFLICT!
                        //
                        // try resolving with a diffy three-way merge
                        let base_content = blobdb_a
                            .get(common_hash)?
                            .expect("Should be contained in RocksDB");
                        let our_content = blobdb_a
                            .get(our_hash)?
                            .expect("Should be contained in RocksDB");
                        let their_content = blobdb_b
                            .get(their_hash)?
                            .expect("Should be contained in RocksDB");
                        if let Some(our_text) = to_text(&our_content)
                            && let Some(their_text) = to_text(&their_content)
                            && let Some(base_text) = to_text(&base_content)
                        {
                            let merged = diffy::merge(base_text, our_text, their_text);
                            match merged {
                                // resolved, let's store
                                Ok(s) => resolved_paths.push((p, s)),
                                // not resolved, store conflicts
                                Err(s) => modified_conflicting_paths.push((
                                    p,
                                    our_hash,
                                    their_hash,
                                    Some(s),
                                )),
                            }
                        } else {
                            // unresolvable (bytes), store nothing
                            modified_conflicting_paths.push((p, our_hash, their_hash, None));
                        }
                    }
                } else if let Some(_) = common
                    && our.is_none()
                    && their.is_none()
                {
                    // both deleted
                    deleted_paths.push(p);
                } else if let Some(common_hash) = common
                    && let Some(our_hash) = our
                    && their.is_none()
                {
                    if our_hash == common_hash {
                        // theirs deleted, ours unchanged -> delete
                        deleted_paths.push(p);
                    } else {
                        // ours modified, theirs deleted -> CONFLICT!
                        deleted_conflicting_paths.push((p, Some(our_hash), None));
                    }
                } else if let Some(common_hash) = common
                    && our.is_none()
                    && let Some(their_hash) = their
                {
                    if their_hash != common_hash {
                        // theirs modified, ours deleted -> CONFLICT!
                        deleted_conflicting_paths.push((p, None, Some(their_hash)));
                    } else {
                        // theirs unchanged, ours deleted -> delete
                        deleted_paths.push(p);
                    }
                } else if common.is_none() {
                    if their.is_none()
                        && let Some(our_hash) = our
                    {
                        // ours created -> keep ours
                        merge_state.push((p, our_hash, true));
                    } else if our.is_none()
                        && let Some(their_hash) = their
                    {
                        // theirs created -> keep theirs
                        merge_state.push((p, their_hash, false));
                    } else if let Some(our_hash) = our
                        && let Some(their_hash) = their
                    {
                        if our_hash == their_hash {
                            // both added in the same way -> keep ours
                            merge_state.push((p, our_hash, true));
                        } else {
                            // both added, but in different ways -> CONFLICT!
                            added_conflicting_paths.push((p, our_hash, their_hash));
                        }
                    } else {
                        unreachable!(
                            "It cannot be that a path does not exist in either ours or theirs if it does not exist in commons"
                        )
                    }
                }
            }

            if modified_conflicting_paths.is_empty()
                && deleted_conflicting_paths.is_empty()
                && added_conflicting_paths.is_empty()
            {
                let mut repo_state: HashMap<String, [u8; 32]> = HashMap::new();

                for (path, hash, is_ours) in merge_state {
                    let content = if is_ours {
                        blobdb_a.get(hash)?.expect("Should be contained in RocksDB")
                    } else {
                        blobdb_b.get(hash)?.expect("Should be contained in RocksDB")
                    };
                    fs::write(path, &content)?;
                    blobdb_a.put(hash, &content)?;
                    repo_state.insert(path.to_string(), hash.to_owned());
                }

                for d in deleted_paths {
                    fs::remove_file(d)?;
                }

                for n in resolved_paths {
                    println!("Automatically resolved conflict on {}", n.0);
                    let content = n.1.as_bytes();
                    let hash_key = blake3::hash(content).as_bytes().to_owned();
                    fs::write(n.0, &content)?;
                    blobdb_a.put(hash_key, content)?;
                    repo_state.insert(n.0.to_string(), hash_key);
                }

                let snap = Snapshot {
                    repository_state: repo_state,
                    parent_ids: vec![snapshot_id_a, snapshot_id_b],
                };
                let to_insert = postcard::to_allocvec(&snap)?;
                let snap_hash = blake3::hash(&to_insert);

                let wtx = redb_a.begin_write()?;
                {
                    let mut snap_table = wtx.open_table(SNAPSHOT_TABLE)?;
                    snap_table.insert(snap_hash.as_bytes().to_owned(), to_insert.as_slice())?;
                }
                wtx.commit()?;

                let mut fl = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(resolved.join(SNAPSHOT_HISTORY_FILE))?;

                writeln!(
                    fl,
                    "{} - {} ({}): merged repository at {} with {:?} at snapshot ID {}",
                    chrono::Utc::now().format("%d/%m/%Y %H:%M:%S"),
                    name,
                    snap_hash,
                    blake3::Hash::from_bytes(snapshot_id_a),
                    other_repo,
                    blake3::Hash::from_bytes(snapshot_id_b),
                )?;

                name_snapshot(&redb_a, snap_hash.as_bytes().to_owned(), name)?;

                println!(
                    "Merge successfully completed and saved under snapshot {} ({})",
                    name, snap_hash
                );
                return Ok(());
            } else {
                println!(
                    "There are conflicts between the two repository, so the merge could not be completed. Find details below."
                );

                if !deleted_conflicting_paths.is_empty() {
                    println!("Files with deletion-related conflicts:");
                    for f in &deleted_conflicting_paths {
                        let des = if f.1.is_some() {
                            "ours modified, theirs deleted"
                        } else {
                            "theirs modified, ours deleted"
                        };
                        println!("{}\t{}", f.0, des);
                    }
                    println!("");
                }

                if !added_conflicting_paths.is_empty() {
                    println!("Files with creation-related conflicts:");
                    for f in &added_conflicting_paths {
                        println!("{}\tboth added, different content", f.0);
                    }
                    println!("");
                }

                if !modified_conflicting_paths.is_empty() {
                    println!("Files with modification-related conflicts:");
                    for f in &modified_conflicting_paths {
                        if let Some(ref merge_conflict) = f.3 {
                            println!("{}", f.0);
                            println!("");
                            println!("{}", merge_conflict);
                            println!("");
                        } else {
                            println!(
                                "{}\t{}",
                                f.0,
                                "File does not appear to be text-based, could not compute merge conflicts"
                            );
                        }
                    }
                    println!("");
                }

                println!(
                    "Please resolve conflicts, then run `ver merge pick` to pick a conflicting file version (theirs or ours), then finalize the merge with `ver merge end`"
                );

                let partial_merge: PartialMerge = PartialMergeInner {
                    clean_merge: merge_state,
                    added_conflicting_paths,
                    deleted_conflicting_paths,
                    modified_conflicting_paths,
                    resolved_paths,
                    deleted_paths,
                }
                .into();

                let compressed = postcard::to_allocvec(&partial_merge)?;

                let wtx = redb_a.begin_write()?;
                {
                    let mut merge_table = wtx.open_table(PARTIAL_MERGES_TABLE)?;
                    merge_table.insert(name, compressed.as_slice())?;
                }
                wtx.commit()?;
            }
        }

        return Err(
            "Could not find a common base for the two repositories, merging is not possible".into(),
        );
    }

    Err("Could not resolve the last snapshot in either of the repositories".into())
}
