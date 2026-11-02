use std::{
    collections::{HashSet, VecDeque},
    io,
    sync::Arc,
};

use blobsdb::{BLOBSDB_PREFIX, BlobsDBWithFs};
use ver_fs::Fs;
use ver_kv::KVStore;

use crate::{
    commons::{
        HISTORY_PATH, build_context, current_fs_kv, get_head, load_snapshot, read_history,
        reconstruct_snapshot, rt, write_head, write_history,
    },
    context::read_context,
    errors::VerError,
    snap::Snapshot,
};

pub fn get_parents(
    kv: &Arc<dyn KVStore>,
    snapshot_id: [u8; 32],
) -> Result<Vec<[u8; 32]>, VerError> {
    let snap = load_snapshot(kv, snapshot_id)?;
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
    fs: &Arc<dyn Fs>,
    kv: &Arc<dyn KVStore>,
) -> Result<(), VerError> {
    let snapshot = load_snapshot(kv, head)?;
    let (del_failed, wrt_failed) = reconstruct_snapshot(snapshot, db, fs, vec![], true)?;

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
    let (fs_from, kv_from) = match from {
        None => current_fs_kv()?,
        Some(ref name) => build_context(ctx.get_context(name).ok(), name)?,
    };
    let (fs_to, kv_to) = build_context(ctx.get_context(&to).ok(), &to)?;
    let head_from = get_head(&fs_from)?;
    if head_from.is_none() {
        return Err(
            "The source context does not have any head, cannot sync it with the target".into(),
        );
    }
    let head_to = get_head(&fs_to)?;
    let blobsdb_from = BlobsDBWithFs::new(fs_from.clone());
    let blobsdb_to = BlobsDBWithFs::new(fs_to.clone());
    if is_from_scratch && head_to.is_none() {
        let _ = writeln!(stdout, "Copying blobs to target context");
        rt().block_on(async { blobsdb_from.copy(&blobsdb_to).await })?;
        let _ = writeln!(stdout, "Copying snapshot history to target context");
        rt().block_on(async { fs_from.copy_dir(&fs_to, Some(HISTORY_PATH), false).await })?;
        let _ = writeln!(stdout, "Copying KV entries to target context");
        kv_from.copy(&kv_to)?;
        let head = head_from.unwrap();
        let _ = writeln!(stdout, "Reconstructing head snapshot in target context");
        reconstruct_head_snapshot(head, &blobsdb_from, &fs_to, &kv_to)?;
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

    if head_to == head_from {
        writeln!(
            stdout,
            "The two contexts are already synced to the latest snapshot"
        )?;
        return Ok(());
    }

    let (base, delta_from, _) = find_common_ancestor(&kv_from, &kv_to, head_from, head_to)?;

    if base.is_none() {
        return Err("The two contexts do not share any snapshot history, cannot sync them".into());
    }

    let base = base.unwrap();
    if base != head_to {
        return Err("The target context has added entries to its snapshot history independently from the source context. Please use `merge` instead of `sync`".into());
    }

    let (history_from, _) = read_history(false, &fs_from)?;

    let get_name = |snap_id: [u8; 32]| -> Option<&str> {
        let snap = blake3::Hash::from_bytes(snap_id).to_string();
        let hist = history_from
            .iter()
            .filter(|s| s.contains(&snap))
            .map(|s| s.as_str())
            .collect::<Vec<&str>>();
        if hist.len() != 1 {
            return None;
        }
        Some(hist.first().unwrap().split("\t").collect::<Vec<&str>>()[2])
    };

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
        // get name from history
        let name = get_name(snap_id);
        if let Some(n) = name {
            write_history(blake3::Hash::from_bytes(snap_id), n, None, &fs_to)?;
        }
    }

    let _ = writeln!(stdout, "Reconstructing head snapshot in target context");
    reconstruct_head_snapshot(head_from, &blobsdb_from, &fs_to, &kv_to)?;

    let _ = writeln!(stdout, "Writing head to target context");
    write_head(head_from, &fs_to)?;

    let _ = writeln!(stdout, "Done syncing source with target context");

    Ok(())
}
