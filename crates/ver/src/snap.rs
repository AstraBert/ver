use std::{
    borrow::Cow,
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{Arc, mpsc},
};

use blobsdb::{BlobsDBWithFs, Chunk, MAX_CONCURRENT_OPS};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use ver_fs::local_disk_walker;

use crate::{
    commons::{get_head, resolve_snapshot_id, rt, write_head, write_history},
    context::read_context,
    errors::VerError,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub repository_state: HashMap<String, Vec<Chunk>>,
    pub parent_ids: Vec<[u8; 32]>,
}

pub fn full_snap<O: io::Write, E: io::Write>(
    name: &str,
    description: Option<&str>,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let opt = ctx.current_context();
    if opt.is_none() {
        return Err("No context is currently in use".into());
    }
    let (fs_des, kv_des) = opt.unwrap();
    let fs = fs_des.build_fs()?;
    let kv = kv_des.build_kv()?;
    let blobdb = Arc::new(BlobsDBWithFs::new(fs.clone()));
    let (tx, rx) = mpsc::channel();
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let mut it = rt().block_on(async { fs.list("", Some(Box::new(local_disk_walker))).await })?;
    while let Some(el) = it.next() {
        let permit = rt().block_on(async {
            sem.clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while trying to acquire semaphore: {e}"))
        })?;
        let db = blobdb.clone();
        let tx = tx.clone();

        rt().spawn(async move {
            let _permit = permit;
            let res = db.put(&el.path().to_string_lossy()).await;
            tx.send((res, el)).unwrap();
        });
    }

    drop(tx);

    let mut failed: Vec<PathBuf> = vec![];
    let mut state: HashMap<String, Vec<Chunk>> = HashMap::new();

    for res in rx {
        if res.0.is_err() {
            failed.push(res.1.path());
        } else {
            state.insert(res.1.path().to_string_lossy().to_string(), res.0.unwrap());
        }
    }

    if !failed.is_empty() {
        writeln!(
            stderr,
            "Snapshotting failed on the following files: {}. Please snapshot them individually",
            failed
                .iter()
                .map(|p| p.to_string_lossy())
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
    };

    let compressed = postcard::to_allocvec(&snap)?;
    let id = blake3::hash(&compressed);
    let bid = id.as_bytes().to_owned();
    kv.put_snapshot(bid, &compressed)?;
    kv.put_name(name, bid)?;
    write_head(bid, &fs)?;
    write_history(id, name, description, &fs)?;

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
    let ctx = read_context()?;
    let opt = ctx.current_context();
    if opt.is_none() {
        return Err("No context is currently in use".into());
    }
    let (fs_des, kv_des) = opt.unwrap();
    let fs = fs_des.build_fs()?;
    let kv = kv_des.build_kv()?;
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
    let blobdb = Arc::new(BlobsDBWithFs::new(fs.clone()));
    let (tx, rx) = mpsc::channel();
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    for file in files {
        let permit = rt().block_on(async {
            sem.clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while trying to acquire semaphore: {e}"))
        })?;
        let db = blobdb.clone();
        let tx = tx.clone();

        rt().spawn(async move {
            let _permit = permit;
            let res = db.put(&file).await;
            tx.send((res, file)).unwrap();
        });
    }

    drop(tx);

    let mut failed: Vec<String> = vec![];

    for res in rx {
        if res.0.is_err() {
            failed.push(res.1);
        } else {
            state.insert(res.1, res.0.unwrap());
        }
    }

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
    };

    let compressed = postcard::to_allocvec(&snap)?;
    if !existing {
        let id = blake3::hash(&compressed);
        let bid = id.as_bytes().to_owned();
        kv.put_snapshot(bid, &compressed)?;
        write_head(bid, &fs)?;
        write_history(id, snapshot_id_or_name, description, &fs)?;
        writeln!(
            stdout,
            "Snapshot {} saved under {}",
            snapshot_id_or_name, id
        )?;
    } else {
        kv.put_snapshot(snap_id.unwrap(), &compressed)?;
        writeln!(stdout, "Snapshot {} updated", snapshot_id_or_name)?;
    }

    Ok(())
}
