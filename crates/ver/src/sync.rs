use std::{
    collections::{HashSet, VecDeque},
    env, fs,
    io::Write,
};

use blobsdb::{Chunk, GetObjectStoreInput, get_object_store, get_remote_head};
use redb::{Database, ReadableDatabase};

use crate::{
    commons::{get_head, resolve_ver_dir_path},
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot, rt},
    init::{REDB_PATH, init_blobs_db},
};

pub fn sync_with_remote_storage<O: Write>(
    input: GetObjectStoreInput,
    std_out: &mut O,
) -> Result<(), VerError> {
    let resolved = resolve_ver_dir_path(env::current_dir()?)?;
    let current_head = get_head(&resolved)?;
    if current_head.is_none() {
        return Err(
            "No head in the current repository yet: run `ver snap´ first, then re-try the sync"
                .into(),
        );
    }
    let redb_content = fs::read(&resolved.join(REDB_PATH))?;

    let blobsdb = init_blobs_db(&resolved)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let remote_storage = get_object_store(input)?;
    let remote_head = rt().block_on(async { get_remote_head(&remote_storage).await })?;

    let rtx = redb.begin_read()?;
    let table = rtx.open_table(SNAPSHOT_TABLE)?;
    let mut queue = VecDeque::from([current_head.unwrap()]);

    let mut chunks: HashSet<Chunk> = HashSet::new();
    let mut found = false;

    if let Some(rh) = remote_head {
        writeln!(
            std_out,
            "Remote storage is at: {}",
            blake3::Hash::from_bytes(rh)
        )?;
        loop {
            let current = queue.pop_front();
            if let Some(cur) = current {
                if cur == rh {
                    found = true;
                    break;
                }
                let opt = table.get(cur)?;
                if let Some(bts) = opt {
                    writeln!(
                        std_out,
                        "Snapshot {} not in remote, adding it to the sync batch",
                        blake3::Hash::from_bytes(cur)
                    )?;
                    let snap: Snapshot = postcard::from_bytes(bts.value())?;
                    for (_, cs) in snap.repository_state {
                        chunks.extend(cs.iter());
                    }
                    queue.extend(snap.parent_ids.iter());
                }
                // if a parent is not found, it means it comes
                // from a different snapshot history (maybe
                // a copy of the current repository) as a
                // result of a merge. (same below)
            } else {
                // No more
                // elements in the queue,
                // break. (same below)
                break;
            }
        }
    } else {
        writeln!(std_out, "Remote storage does not have a head")?;
        loop {
            let current = queue.pop_front();
            if let Some(cur) = current {
                let opt = table.get(cur)?;
                if let Some(bts) = opt {
                    writeln!(
                        std_out,
                        "Adding snapshot {} to the sync batch",
                        blake3::Hash::from_bytes(cur)
                    )?;
                    let snap: Snapshot = postcard::from_bytes(bts.value())?;
                    for (_, cs) in snap.repository_state {
                        chunks.extend(cs.iter());
                    }
                    if snap.parent_ids.is_empty() {
                        found = true;
                        break;
                    }
                    queue.extend(snap.parent_ids.iter());
                }
            } else {
                break;
            }
        }
    }

    if !found {
        return Err("Could not find remote head in the snapshots history".into());
    }

    writeln!(
        std_out,
        "Syncing {} chunks, files.redb and head to remote storage",
        chunks.len()
    )?;

    rt().block_on(async {
        blobsdb
            .sync(remote_storage, chunks, current_head.unwrap(), redb_content)
            .await
    })?;

    writeln!(std_out, "Sync completed")?;

    Ok(())
}
