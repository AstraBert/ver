use std::{
    collections::{HashSet, VecDeque},
    io,
    sync::{Arc, mpsc},
};

use blobsdb::{BLOBSDB_PREFIX, BlobsDBWithFs, MAX_CONCURRENT_OPS};
use bytes::Bytes;
use tokio::sync::Semaphore;
use ver_fs::{Fs, local_disk_walker};
use ver_kv::KVStore;

use crate::{
    commons::{HISTORY_PATH, get_head, rt, write_head},
    context::{FilesystemContext, KVStoreContext, read_context},
    errors::VerError,
    snap::Snapshot,
};

pub fn get_parents(
    kv: &Arc<dyn KVStore>,
    snapshot_id: [u8; 32],
) -> Result<Vec<[u8; 32]>, VerError> {
    let compressed = kv.get_snapshot(snapshot_id)?;
    if compressed.is_none() {
        return Err("Could not find the requested snapshot".into());
    }
    let snap: Snapshot = postcard::from_bytes(&compressed.unwrap())?;
    Ok(snap.parent_ids)
}

pub fn find_common_ancestor(
    kv_a: &Arc<dyn KVStore>,
    kv_b: &Arc<dyn KVStore>,
    snapshot_id_a: [u8; 32],
    snapshot_id_b: [u8; 32],
) -> Result<(Option<[u8; 32]>, Vec<[u8; 32]>, Vec<[u8; 32]>), VerError> {
    if snapshot_id_a == snapshot_id_b {
        return Ok((Some(snapshot_id_a), vec![], vec![]));
    }

    let mut visited_a = HashSet::from([snapshot_id_a]);
    let mut visited_b = HashSet::from([snapshot_id_b]);
    let mut frontier_a = VecDeque::from([snapshot_id_a]);
    let mut frontier_b = VecDeque::from([snapshot_id_b]);

    while !frontier_a.is_empty() || !frontier_b.is_empty() {
        if !frontier_a.is_empty() {
            // we know it's not empty
            let cur_a = frontier_a.pop_front().unwrap();
            let parents = get_parents(kv_a, cur_a)?;
            for par in parents {
                if visited_b.contains(&par) {
                    return Ok((
                        Some(par),
                        visited_a.into_iter().collect(),
                        visited_b.into_iter().collect(),
                    ));
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
            let parents = get_parents(kv_b, cur_b)?;

            for par in parents {
                if visited_a.contains(&par) {
                    return Ok((
                        Some(par),
                        visited_a.into_iter().collect(),
                        visited_b.into_iter().collect(),
                    ));
                }

                if !visited_b.contains(&par) {
                    visited_b.insert(par);
                    frontier_b.push_back(par);
                }
            }
        }
    }

    Ok((None, vec![], vec![]))
}

pub fn reconstruct_head_snapshot(
    head: [u8; 32],
    db: &BlobsDBWithFs,
    kv: &Arc<dyn KVStore>,
) -> Result<(), VerError> {
    let compressed = kv.get_snapshot(head)?;
    if compressed.is_none() {
        return Err("Could not find snapshot in KV store".into());
    }
    let snapshot: Snapshot = postcard::from_bytes(&compressed.unwrap())?;
    let mut paths: Vec<String> = rt()
        .block_on(async { db.fs.list("", Some(Box::new(local_disk_walker))).await })?
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    let wsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let dsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (wtx, wrx) = mpsc::channel::<Result<(), (String, String)>>();
    let (dtx, drx) = mpsc::channel::<Result<(), (String, String)>>();

    for (path, chunks) in snapshot.repository_state {
        let idx = paths.iter().position(|a| a == &path);
        let permit = rt().block_on(async {
            wsem.clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Could not acquire writing semaphore: {e}"))
        })?;
        let db = db.clone();
        let wtx = wtx.clone();
        let chunks = Arc::new(chunks.clone());
        rt().spawn(async move {
            let _permit = permit;
            let res = db
                .get(chunks)
                .await
                .map_err(|e| (e.to_string(), path.clone()));
            match res {
                Ok(c) => {
                    let new = db
                        .fs
                        .write(&path, Bytes::from_owner(c))
                        .await
                        .map_err(|e| (e.to_string(), path));
                    wtx.send(new).unwrap()
                }
                Err(e) => wtx.send(Err(e)).unwrap(),
            }
        });
        if let Some(pos) = idx {
            paths.remove(pos);
        }
    }

    if !paths.is_empty() && !snapshot.partial {
        for path in paths {
            let permit = rt().block_on(async {
                dsem.clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| format!("Could not acquire deletion semaphore: {e}"))
            })?;
            let fs = db.fs.clone();
            let dtx = dtx.clone();
            rt().spawn(async move {
                let _permit = permit;
                let res = fs.delete(&path).await.map_err(|e| (e.to_string(), path));
                dtx.send(res).unwrap()
            });
        }
    }

    drop(wtx);
    drop(dtx);

    let mut del_failed: Vec<String> = vec![];
    let mut wrt_failed: Vec<String> = vec![];

    for res in drx {
        if res.is_err() {
            let err = res.unwrap_err();
            del_failed.push(err.1);
        }
    }

    for res in wrx {
        if res.is_err() {
            let err = res.unwrap_err();
            wrt_failed.push(err.1);
        }
    }

    if !del_failed.is_empty() || !wrt_failed.is_empty() {
        return Err(format!("Failed to delete the following files: {}\nFailed to reconstruct the following files: {}",
        del_failed.join(", "),
        wrt_failed.join(", ")).into());
    }

    Ok(())
}

pub fn copy_snapshot(
    snapshot_id: [u8; 32],
    kv_from: &Arc<dyn KVStore>,
    kv_to: &Arc<dyn KVStore>,
    fs_from: &Arc<dyn Fs>,
    fs_to: &Arc<dyn Fs>,
) -> Result<(), VerError> {
    let compressed = kv_from.get_snapshot(snapshot_id)?;
    if compressed.is_none() {
        return Err("Could not find the requested snapshot".into());
    }
    let content = compressed.unwrap();
    let snap: Snapshot = postcard::from_bytes(&content)?;
    for chunks in snap.repository_state.values() {
        let chunk_files: Vec<String> = chunks
            .iter()
            .map(|c| format!("{}/{}", BLOBSDB_PREFIX, blake3::Hash::from_bytes(c.hash)))
            .collect();
        rt().block_on(async {
            fs_from
                .copy_files(&fs_to, chunk_files.iter().map(|s| s.as_str()).collect())
                .await
        })?;
    }

    kv_to.put_snapshot(snapshot_id, &content)?;

    Ok(())
}

pub fn sync<O: io::Write>(
    from: Option<String>,
    to: String,
    is_from_scratch: bool,
    stdout: &mut O,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let opt_from = if from.is_none() {
        ctx.current_context()
    } else {
        ctx.get_context(from.as_deref().unwrap()).ok()
    };
    let opt_to = ctx.get_context(&to).ok();
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
    let (fs_from, kv_from) = unwrap_ctx(opt_from, from.as_deref().unwrap_or("current"))?;
    let (fs_to, kv_to) = unwrap_ctx(opt_to, &to)?;
    let head_from = get_head(&fs_from)?;
    if head_from.is_none() {
        return Err(
            "The source context does not have any head, cannot sync it with the target".into(),
        );
    }
    let head_to = get_head(&fs_to)?;
    let blobsdb_from = BlobsDBWithFs::new(fs_from.clone());
    let blobsdb_to = BlobsDBWithFs::new(fs_to.clone());
    if is_from_scratch || head_to.is_none() {
        let _ = writeln!(stdout, "Copying blobs to target context");
        rt().block_on(async { blobsdb_from.copy(&blobsdb_to).await })?;
        let _ = writeln!(stdout, "Copying snapshot history to target context");
        rt().block_on(async { fs_from.copy_files(&fs_to, vec![HISTORY_PATH]).await })?;
        let _ = writeln!(stdout, "Copying KV entries to target context");
        kv_from.copy(&kv_to)?;
        let head = head_from.unwrap();
        let _ = writeln!(stdout, "Reconstructing head snapshot in target context");
        reconstruct_head_snapshot(head, &blobsdb_from, &kv_to)?;
        let _ = writeln!(stdout, "Writing head to target context");
        write_head(head, &fs_to)?;
        writeln!(
            stdout,
            "Successfully copied source context to target (from scratch)"
        )?;
        return Ok(());
    }
    let head_from = head_from.unwrap();
    let head_to = head_to.unwrap();

    let (base, delta_from, _) = find_common_ancestor(&kv_from, &kv_to, head_from, head_to)?;

    if base.is_none() {
        return Err("The two contexts do not share any snapshot history, cannot sync them".into());
    }

    let base = base.unwrap();
    if base != head_to {
        return Err("The target context has added entries to its snapshot history independently from the source context. Please use `merge` instead of `sync`".into());
    }

    // Go through the list of snapshots
    // that the source context has and the target
    // one doesn't, and copy
    for snap_id in delta_from {
        let _ = writeln!(
            stdout,
            "Copying snapshot {} to target context",
            blake3::Hash::from_bytes(snap_id)
        );
        copy_snapshot(snap_id, &kv_from, &kv_to, &fs_from, &fs_to)?;
    }

    let _ = writeln!(stdout, "Reconstructing head snapshot in target context");
    reconstruct_head_snapshot(head_from, &blobsdb_from, &kv_to)?;

    let _ = writeln!(stdout, "Writing head to target context");
    write_head(head_from, &fs_to)?;

    let _ = writeln!(stdout, "Done syncing source with target context");

    Ok(())
}
