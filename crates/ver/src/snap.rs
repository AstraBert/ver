use std::{borrow::Cow, collections::HashMap, io};

use blobsdb::{BlobsDBWithFs, Chunk};
use serde::{Deserialize, Serialize};
use ver_fs::local_disk_walker;

use crate::{
    commons::{
        commit_snapshot, current_fs_kv, get_head, resolve_snapshot_id, rt, run_concurrent,
    },
    errors::VerError,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub repository_state: HashMap<String, Vec<Chunk>>,
    pub parent_ids: Vec<[u8; 32]>,
    #[serde(default)]
    pub partial: bool,
}

/// Put `files` into the blob store concurrently, updating `state` with the
/// resulting chunks. Returns the list of files that failed to be stored.
fn put_files(
    blobdb: &BlobsDBWithFs,
    files: Vec<String>,
    state: &mut HashMap<String, Vec<Chunk>>,
) -> Result<Vec<String>, VerError> {
    let results = run_concurrent(files, {
        let db = blobdb.clone();
        move |file| {
            let db = db.clone();
            async move { (db.put(&file).await, file) }
        }
    })?;

    let mut failed: Vec<String> = vec![];
    for (res, file) in results {
        match res {
            Ok(chunks) => {
                state.insert(file, chunks);
            }
            Err(_) => failed.push(file),
        }
    }

    Ok(failed)
}

pub fn full_snap<O: io::Write, E: io::Write>(
    name: &str,
    description: Option<&str>,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<(), VerError> {
    let (fs, kv) = current_fs_kv()?;
    let blobdb = BlobsDBWithFs::new(fs.clone());
    let files: Vec<String> =
        rt().block_on(async { fs.list("", Some(Box::new(local_disk_walker))).await })?
            .map(|e| e.path().to_string_lossy().into_owned())
            .collect();

    let mut state: HashMap<String, Vec<Chunk>> = HashMap::new();
    let failed = put_files(&blobdb, files, &mut state)?;

    if !failed.is_empty() {
        writeln!(
            stderr,
            "Snapshotting failed on the following files: {}. Please snapshot them individually",
            failed
                .iter()
                .map(|p| Cow::Borrowed(p.as_str()))
                .collect::<Vec<Cow<str>>>()
                .join(", ")
        )?;
        return Err("Snapshotting failed on one or more files".into());
    }

    let head = get_head(&fs)?;
    let parent_ids = if let Some(h) = head { vec![h] } else { vec![] };
    let snap = Snapshot {
        repository_state: state,
        parent_ids,
        partial: false,
    };

    let id = commit_snapshot(&snap, name, description, &fs, &kv)?;

    writeln!(stdout, "Snapshot {} saved under {}", name, id)?;

    Ok(())
}

pub fn files_snap<O: io::Write, E: io::Write>(
    snapshot_id_or_name: &str,
    description: Option<&str>,
    files: Vec<String>,
    existing: bool,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<(), VerError> {
    let (fs, kv) = current_fs_kv()?;
    let (mut state, parent_ids, snap_id) = if existing {
        let snap_id = resolve_snapshot_id(snapshot_id_or_name, &kv)?;
        match kv.get_snapshot(snap_id)? {
            Some(comp) => {
                let snap: Snapshot = postcard::from_bytes(&comp)?;
                (snap.repository_state, snap.parent_ids, Some(snap_id))
            }
            None => {
                return Err(format!(
                    "Could not resolve existing snapshot identifier from {snapshot_id_or_name}"
                )
                .into());
            }
        }
    } else {
        let head = get_head(&fs)?;
        let parent_ids = if let Some(h) = head { vec![h] } else { vec![] };
        (HashMap::new(), parent_ids, None)
    };

    let blobdb = BlobsDBWithFs::new(fs.clone());
    let failed = put_files(&blobdb, files, &mut state)?;

    if !failed.is_empty() {
        writeln!(
            stderr,
            "Snapshotting failed on the following files: {}. Please snapshot them individually",
            failed.join(", ")
        )?;
        return Err("Snapshotting failed on one or more files".into());
    }

    let snap = Snapshot {
        repository_state: state,
        parent_ids,
        partial: !existing,
    };

    if !existing {
        let id = commit_snapshot(&snap, snapshot_id_or_name, description, &fs, &kv)?;
        writeln!(
            stdout,
            "Snapshot {} saved under {}",
            snapshot_id_or_name, id
        )?;
    } else {
        let compressed = postcard::to_allocvec(&snap)?;
        kv.put_snapshot(snap_id.unwrap(), &compressed)?;
        writeln!(stdout, "Snapshot {} updated", snapshot_id_or_name)?;
    }

    Ok(())
}
