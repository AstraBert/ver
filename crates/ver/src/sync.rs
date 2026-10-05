use std::{
    collections::{HashSet, VecDeque},
    env,
};

use blobsdb::{Chunk, GetObjectStoreInput, get_object_store, get_remote_head};
use redb::{Database, ReadableDatabase};

use crate::{
    commons::{get_head, resolve_ver_dir_path},
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot, rt},
    init::{REDB_PATH, init_blobs_db},
};

pub fn sync_with_remote_storage(input: GetObjectStoreInput) -> Result<(), VerError> {
    let resolved = resolve_ver_dir_path(env::current_dir()?)?;
    let current_head = get_head(&resolved)?;
    if current_head.is_none() {
        return Err(
            "No head in the current repository yet: run `ver snap´ first, then re-try the sync"
                .into(),
        );
    }
    let head_path = resolved.join("head");

    let blobsdb = init_blobs_db(&resolved)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let remote_storage = get_object_store(input)?;
    let remote_head =
        rt().block_on(async { get_remote_head(&remote_storage, &head_path).await })?;

    let rtx = redb.begin_read()?;
    let table = rtx.open_table(SNAPSHOT_TABLE)?;
    let mut queue = VecDeque::from([current_head.unwrap()]);

    let mut chunks: HashSet<Chunk> = HashSet::new();
    let mut found = false;

    if let Some(rh) = remote_head {
        loop {
            let current = queue.pop_front();
            if let Some(cur) = current {
                if cur == rh {
                    found = true;
                    break;
                }
                let opt = table.get(cur)?;
                if let Some(bts) = opt {
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
        loop {
            let current = queue.pop_front();
            if let Some(cur) = current {
                let opt = table.get(cur)?;
                if let Some(bts) = opt {
                    let snap: Snapshot = postcard::from_bytes(bts.value())?;
                    if snap.parent_ids.is_empty() {
                        found = true;
                        break;
                    }
                    for (_, cs) in snap.repository_state {
                        chunks.extend(cs.iter());
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

    rt().block_on(async {
        blobsdb
            .sync(remote_storage, chunks, current_head.unwrap())
            .await
    })?;

    Ok(())
}
