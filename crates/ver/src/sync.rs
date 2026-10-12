use std::{
    collections::{HashSet, VecDeque},
    env, fs,
    io::Write,
};

use blobsdb::{Chunk, GetObjectStoreInput, get_object_store, get_remote_head};
use redb::{Database, ReadOnlyTable, ReadableDatabase};

use crate::{
    commons::{get_head, resolve_ver_dir_path},
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot, rt},
    init::{REDB_PATH, SNAPSHOT_HISTORY_FILE, init_blobs_db},
};

pub(crate) fn find_unsynced_snapshots<O: Write>(
    table: &ReadOnlyTable<[u8; 32], &[u8]>,
    remote_head: Option<[u8; 32]>,
    current_head: [u8; 32],
    std_out: &mut O,
) -> Result<HashSet<Chunk>, VerError> {
    let mut chunks: HashSet<Chunk> = HashSet::new();
    let mut found = false;
    let mut queue = VecDeque::from([current_head]);

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

    Ok(chunks)
}

pub fn sync_with_remote_storage<O: Write, E: Write>(
    remote_name: String,
    input: GetObjectStoreInput,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let resolved = resolve_ver_dir_path(env::current_dir()?)?;
    let current_head = get_head(&resolved)?;
    if current_head.is_none() {
        return Err(
            "No head in the current repository yet: run `ver snap´ first, then re-try the sync"
                .into(),
        );
    }
    let redb_content = fs::read(resolved.join(REDB_PATH))?;
    let history_content = fs::read(resolved.join(SNAPSHOT_HISTORY_FILE))?;

    let blobsdb = init_blobs_db(&resolved)?;
    let redb = Database::open(resolved.join(REDB_PATH))?;
    let remote_storage = get_object_store(input)?;
    let remote_head = rt().block_on(async { get_remote_head(&remote_storage).await })?;

    let rtx = redb.begin_read()?;
    let table = rtx.open_table(SNAPSHOT_TABLE)?;
    let chunks = find_unsynced_snapshots(&table, remote_head, current_head.unwrap(), std_out)?;

    writeln!(
        std_out,
        "Syncing {} chunks, files.redb and head to remote storage",
        chunks.len()
    )?;

    rt().block_on(async {
        blobsdb
            .sync(
                remote_name,
                remote_storage,
                chunks,
                current_head.unwrap(),
                redb_content,
                history_content,
                std_err,
            )
            .await
    })?;

    writeln!(std_out, "Sync completed")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tempdir::TempDir;

    // -----------------------------------------------------------------------
    // helpers
    // -----------------------------------------------------------------------

    /// Create a `Chunk` with a deterministic hash derived from a seed byte.
    fn chunk(seed: u8) -> Chunk {
        Chunk {
            offset: 0,
            length: 100,
            hash: [seed; 32],
        }
    }

    /// Create a `Snapshot` with the given parent IDs and file→chunks mapping.
    fn snapshot(parents: &[[u8; 32]], files: &[(&str, &[Chunk])]) -> Snapshot {
        let mut state = HashMap::new();
        for (name, chunks) in files {
            state.insert(name.to_string(), chunks.to_vec());
        }
        Snapshot {
            parent_ids: parents.to_vec(),
            repository_state: state,
        }
    }

    /// Insert a snapshot into the redb table under the given ID.
    fn insert_snapshot(db: &Database, id: [u8; 32], snap: &Snapshot) {
        let bytes = postcard::to_allocvec(snap).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let mut table = tx.open_table(SNAPSHOT_TABLE).unwrap();
            table.insert(id, bytes.as_slice()).unwrap();
        }
        tx.commit().unwrap();
    }

    /// Create a fresh redb database in a tempdir, returning both.
    fn setup_redb() -> (TempDir, Database) {
        let tmp = TempDir::new("ver_sync_test").unwrap();
        let db = Database::create(tmp.path().join("test.redb")).unwrap();
        let tx = db.begin_write().unwrap();
        {
            let _ = tx.open_table(SNAPSHOT_TABLE).unwrap();
        }
        tx.commit().unwrap();
        (tmp, db)
    }

    /// Run `find_unsynced_snapshots` against the given database.
    ///
    /// Opens a read transaction and table internally, avoiding
    /// lifetime issues with returning borrowed references.
    fn run_find(
        db: &Database,
        remote_head: Option<[u8; 32]>,
        current_head: [u8; 32],
    ) -> (Result<HashSet<Chunk>, VerError>, String) {
        let tx = db.begin_read().unwrap();
        let table = tx.open_table(SNAPSHOT_TABLE).unwrap();
        let mut out = Vec::new();
        let result = find_unsynced_snapshots(&table, remote_head, current_head, &mut out);
        let output = String::from_utf8(out).unwrap();
        (result, output)
    }

    /// Collect the hashes from a set of chunks into a sorted vec for
    /// deterministic comparison.
    fn chunk_hashes(chunks: &HashSet<Chunk>) -> Vec<[u8; 32]> {
        let mut v: Vec<[u8; 32]> = chunks.iter().map(|c| c.hash).collect();
        v.sort();
        v
    }

    // -----------------------------------------------------------------------
    // no remote head (fresh remote)
    // -----------------------------------------------------------------------

    #[test]
    fn no_remote_head_single_snapshot() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let snap = snapshot(&[], &[("a.txt", &[c1])]);
        let snap_id = [10u8; 32];
        insert_snapshot(&db, snap_id, &snap);

        let (result, output) = run_find(&db, None, snap_id);
        let result = result.unwrap();

        assert_eq!(chunk_hashes(&result), vec![[1u8; 32]]);
        assert!(output.contains("Remote storage does not have a head"));
        assert!(output.contains("Adding snapshot"));
    }

    #[test]
    fn no_remote_head_walks_to_root() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let c2 = chunk(2);
        let c3 = chunk(3);

        let root_id = [1u8; 32];
        let mid_id = [2u8; 32];
        let head_id = [3u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[c1])]));
        insert_snapshot(&db, mid_id, &snapshot(&[root_id], &[("b.txt", &[c2])]));
        insert_snapshot(&db, head_id, &snapshot(&[mid_id], &[("c.txt", &[c3])]));

        let (result, _output) = run_find(&db, None, head_id);
        let result = result.unwrap();

        assert_eq!(chunk_hashes(&result), vec![[1u8; 32], [2u8; 32], [3u8; 32]]);
    }

    #[test]
    fn no_remote_head_empty_snapshot() {
        let (_tmp, db) = setup_redb();

        let snap_id = [5u8; 32];
        insert_snapshot(&db, snap_id, &snapshot(&[], &[]));

        let (result, _output) = run_find(&db, None, snap_id);
        assert!(result.unwrap().is_empty());
    }

    // -----------------------------------------------------------------------
    // remote head matches current head (nothing to sync)
    // -----------------------------------------------------------------------

    #[test]
    fn remote_head_equals_current_head_returns_empty() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let snap_id = [10u8; 32];
        insert_snapshot(&db, snap_id, &snapshot(&[], &[("a.txt", &[c1])]));

        let (result, output) = run_find(&db, Some(snap_id), snap_id);
        let result = result.unwrap();

        assert!(
            result.is_empty(),
            "no chunks to sync when remote head == current head"
        );
        assert!(output.contains("Remote storage is at:"));
        assert!(!output.contains("not in remote"));
    }

    // -----------------------------------------------------------------------
    // remote head is an ancestor (partial sync)
    // -----------------------------------------------------------------------

    #[test]
    fn remote_head_is_ancestor_collects_only_newer() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let c2 = chunk(2);
        let c3 = chunk(3);

        let root_id = [1u8; 32];
        let mid_id = [2u8; 32];
        let head_id = [3u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[c1])]));
        insert_snapshot(&db, mid_id, &snapshot(&[root_id], &[("b.txt", &[c2])]));
        insert_snapshot(&db, head_id, &snapshot(&[mid_id], &[("c.txt", &[c3])]));

        let (result, output) = run_find(&db, Some(mid_id), head_id);
        let result = result.unwrap();

        assert_eq!(
            chunk_hashes(&result),
            vec![[3u8; 32]],
            "only chunks from the head snapshot should be returned"
        );
        assert!(output.contains("Remote storage is at:"));
        assert!(output.contains("not in remote"));
    }

    #[test]
    fn remote_head_is_root_collects_all_descendants() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let c2 = chunk(2);
        let c3 = chunk(3);

        let root_id = [1u8; 32];
        let mid_id = [2u8; 32];
        let head_id = [3u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[c1])]));
        insert_snapshot(&db, mid_id, &snapshot(&[root_id], &[("b.txt", &[c2])]));
        insert_snapshot(&db, head_id, &snapshot(&[mid_id], &[("c.txt", &[c3])]));

        let (result, _output) = run_find(&db, Some(root_id), head_id);
        let result = result.unwrap();

        assert_eq!(
            chunk_hashes(&result),
            vec![[2u8; 32], [3u8; 32]],
            "chunks from mid and head should be returned"
        );
    }

    // -----------------------------------------------------------------------
    // remote head not in local history (error)
    // -----------------------------------------------------------------------

    #[test]
    fn remote_head_not_in_history_returns_error() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let snap_id = [10u8; 32];
        insert_snapshot(&db, snap_id, &snapshot(&[], &[("a.txt", &[c1])]));

        let unknown_remote = [99u8; 32];

        let (result, _output) = run_find(&db, Some(unknown_remote), snap_id);

        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("Could not find remote head"),
            "unexpected error: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // multiple files per snapshot
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_files_per_snapshot_collects_all_chunks() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let c2 = chunk(2);
        let c3 = chunk(3);
        let c4 = chunk(4);

        let snap_id = [10u8; 32];
        insert_snapshot(
            &db,
            snap_id,
            &snapshot(
                &[],
                &[("a.txt", &[c1, c2]), ("b.txt", &[c3]), ("c.txt", &[c4])],
            ),
        );

        let (result, _output) = run_find(&db, None, snap_id);
        let result = result.unwrap();

        assert_eq!(result.len(), 4);
        assert_eq!(
            chunk_hashes(&result),
            vec![[1u8; 32], [2u8; 32], [3u8; 32], [4u8; 32]]
        );
    }

    // -----------------------------------------------------------------------
    // shared chunks across snapshots are deduplicated
    // -----------------------------------------------------------------------

    #[test]
    fn shared_chunks_across_snapshots_are_deduplicated() {
        let (_tmp, db) = setup_redb();

        let shared = chunk(1);
        let unique = chunk(2);

        let root_id = [1u8; 32];
        let head_id = [2u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[shared])]));
        insert_snapshot(
            &db,
            head_id,
            &snapshot(&[root_id], &[("a.txt", &[shared]), ("b.txt", &[unique])]),
        );

        let (result, _output) = run_find(&db, None, head_id);
        let result = result.unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(chunk_hashes(&result), vec![[1u8; 32], [2u8; 32]]);
    }

    // -----------------------------------------------------------------------
    // missing parent snapshot (merge / copied repo scenario)
    // -----------------------------------------------------------------------

    #[test]
    fn missing_parent_snapshot_is_skipped_gracefully() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let c2 = chunk(2);

        let root_id = [1u8; 32];
        let head_id = [2u8; 32];
        let missing_id = [99u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[c1])]));
        insert_snapshot(
            &db,
            head_id,
            &snapshot(&[root_id, missing_id], &[("b.txt", &[c2])]),
        );

        let (result, _output) = run_find(&db, None, head_id);
        let result = result.unwrap();

        assert_eq!(chunk_hashes(&result), vec![[1u8; 32], [2u8; 32]]);
    }

    // -----------------------------------------------------------------------
    // output messages
    // -----------------------------------------------------------------------

    #[test]
    fn output_messages_for_fresh_remote() {
        let (_tmp, db) = setup_redb();

        let snap_id = [10u8; 32];
        insert_snapshot(&db, snap_id, &snapshot(&[], &[]));

        let (_result, output) = run_find(&db, None, snap_id);

        assert!(output.contains("Remote storage does not have a head"));
        assert!(output.contains("Adding snapshot"));
        assert!(output.contains(&blake3::Hash::from_bytes(snap_id).to_string()));
    }

    #[test]
    fn output_messages_for_existing_remote() {
        let (_tmp, db) = setup_redb();

        let c1 = chunk(1);
        let root_id = [1u8; 32];
        let head_id = [2u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("a.txt", &[c1])]));
        insert_snapshot(&db, head_id, &snapshot(&[root_id], &[("b.txt", &[c1])]));

        let (_result, output) = run_find(&db, Some(root_id), head_id);

        assert!(output.contains("Remote storage is at:"));
        assert!(output.contains(&blake3::Hash::from_bytes(root_id).to_string()));
        assert!(output.contains("not in remote"));
        assert!(output.contains(&blake3::Hash::from_bytes(head_id).to_string()));
    }

    // -----------------------------------------------------------------------
    // longer chains
    // -----------------------------------------------------------------------

    #[test]
    fn long_chain_collects_correct_subset() {
        let (_tmp, db) = setup_redb();

        let ids: Vec<[u8; 32]> = (0..5).map(|i| [i as u8; 32]).collect();
        let chunks: Vec<Chunk> = (0..5).map(|i| chunk(100 + i as u8)).collect();

        for i in 0..5 {
            let parents: Vec<[u8; 32]> = if i == 0 { vec![] } else { vec![ids[i - 1]] };
            let name = format!("file_{i}.txt");
            insert_snapshot(&db, ids[i], &snapshot(&parents, &[(&name, &[chunks[i]])]));
        }

        // Remote is at s2, current head is s4 → should collect s3 and s4
        let (result, _output) = run_find(&db, Some(ids[2]), ids[4]);
        let result = result.unwrap();

        assert_eq!(result.len(), 2);
        let hashes = chunk_hashes(&result);
        assert!(hashes.contains(&[103u8; 32]));
        assert!(hashes.contains(&[104u8; 32]));
    }

    #[test]
    fn no_remote_head_long_chain_collects_everything() {
        let (_tmp, db) = setup_redb();

        let ids: Vec<[u8; 32]> = (0..5).map(|i| [i as u8; 32]).collect();
        let chunks: Vec<Chunk> = (0..5).map(|i| chunk(100 + i as u8)).collect();

        for i in 0..5 {
            let parents: Vec<[u8; 32]> = if i == 0 { vec![] } else { vec![ids[i - 1]] };
            let name = format!("file_{i}.txt");
            insert_snapshot(&db, ids[i], &snapshot(&parents, &[(&name, &[chunks[i]])]));
        }

        let (result, _output) = run_find(&db, None, ids[4]);
        let result = result.unwrap();

        assert_eq!(result.len(), 5);
        for i in 0..5u8 {
            assert!(result.iter().any(|c| c.hash == [100 + i; 32]));
        }
    }

    // -----------------------------------------------------------------------
    // merge snapshots (multiple parents)
    // -----------------------------------------------------------------------

    #[test]
    fn merge_snapshot_collects_from_both_branches() {
        let (_tmp, db) = setup_redb();

        let c_root = chunk(1);
        let c_a = chunk(2);
        let c_b = chunk(3);
        let c_merge = chunk(4);

        let root_id = [1u8; 32];
        let a1_id = [2u8; 32];
        let b1_id = [3u8; 32];
        let merge_id = [4u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("base.txt", &[c_root])]));
        insert_snapshot(&db, a1_id, &snapshot(&[root_id], &[("a.txt", &[c_a])]));
        insert_snapshot(&db, b1_id, &snapshot(&[root_id], &[("b.txt", &[c_b])]));
        insert_snapshot(
            &db,
            merge_id,
            &snapshot(&[a1_id, b1_id], &[("merged.txt", &[c_merge])]),
        );

        let (result, _output) = run_find(&db, None, merge_id);
        let result = result.unwrap();

        assert_eq!(result.len(), 4);
        let hashes = chunk_hashes(&result);
        for seed in 1..=4u8 {
            assert!(
                hashes.contains(&[seed; 32]),
                "missing chunk with seed {seed}"
            );
        }
    }

    #[test]
    fn merge_with_remote_at_one_branch() {
        let (_tmp, db) = setup_redb();

        let c_root = chunk(1);
        let c_a = chunk(2);
        let c_b = chunk(3);
        let c_merge = chunk(4);

        let root_id = [1u8; 32];
        let a1_id = [2u8; 32];
        let b1_id = [3u8; 32];
        let merge_id = [4u8; 32];

        insert_snapshot(&db, root_id, &snapshot(&[], &[("base.txt", &[c_root])]));
        insert_snapshot(&db, a1_id, &snapshot(&[root_id], &[("a.txt", &[c_a])]));
        insert_snapshot(&db, b1_id, &snapshot(&[root_id], &[("b.txt", &[c_b])]));
        insert_snapshot(
            &db,
            merge_id,
            &snapshot(&[a1_id, b1_id], &[("merged.txt", &[c_merge])]),
        );

        // Remote is at a1. BFS from merge_id pops merge, then a1 (match → break).
        // b1 and root are never visited.
        let (result, _output) = run_find(&db, Some(a1_id), merge_id);
        let result = result.unwrap();

        assert_eq!(chunk_hashes(&result), vec![[4u8; 32]]);
    }
}
