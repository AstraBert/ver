use std::{
    io::Write,
    sync::{Arc, mpsc},
};

use blobsdb::{BlobsDBWithFs, MAX_CONCURRENT_OPS};
use bytes::Bytes;
use tokio::sync::Semaphore;
use ver_fs::local_disk_walker;

use crate::{
    commons::{get_head, resolve_snapshot_id, rt, write_head},
    context::read_context,
    errors::VerError,
    snap::Snapshot,
};

pub fn goto<O: Write, E: Write>(
    identifier: &str,
    paths: Vec<String>,
    delete_if_not_in_snapshot: bool,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<(), VerError> {
    let ctx = read_context()?;
    let opt = ctx.current_context();
    if opt.is_none() {
        return Err("No current context found".into());
    }
    let (fs_des, kv_des) = opt.unwrap();
    let fs = fs_des.build_fs()?;
    let kv = kv_des.build_kv()?;
    let resolved = resolve_snapshot_id(identifier, &kv)?;
    let head = get_head(&fs)?;
    if head.is_none() {
        return Err("Cannot revert to any previous snapshot, since this project holds none".into());
    }
    let h = head.unwrap();
    if h == resolved {
        writeln!(stdout, "Already on {identifier}")?;
        return Ok(());
    }
    let compressed = kv.get_snapshot(resolved)?;
    if compressed.is_none() {
        return Err("Snapshot does not exist in KV store".into());
    }
    let snapshot: Snapshot = postcard::from_bytes(&compressed.unwrap())?;

    let paths: Vec<String> = if !paths.is_empty() {
        paths
    } else {
        rt().block_on(async { fs.list("", Some(Box::new(local_disk_walker))).await })?
            .map(|e| e.path().to_string_lossy().into_owned())
            .collect()
    };

    let db = Arc::new(BlobsDBWithFs::new(fs.clone()));
    let wsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let dsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (wtx, wrx) = mpsc::channel::<Result<(), (String, String)>>();
    let (dtx, drx) = mpsc::channel::<Result<(), (String, String)>>();

    for path in paths {
        if !snapshot.repository_state.contains_key(&path)
            && !snapshot.partial
            && delete_if_not_in_snapshot
        {
            let permit = rt().block_on(async {
                dsem.clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| format!("Could not acquire deletion semaphore: {e}"))
            })?;
            let fs = fs.clone();
            let dtx = dtx.clone();
            rt().spawn(async move {
                let _permit = permit;
                let res = fs.delete(&path).await.map_err(|e| (e.to_string(), path));
                dtx.send(res).unwrap()
            });
        } else if let Some(chunks) = snapshot.repository_state.get(&path) {
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
        writeln!(
            stderr,
            "Failed to delete the following files: {}\nFailed to reconstruct the following files: {}",
            del_failed.join(", "),
            wrt_failed.join(", ")
        )?;
    } else {
        write_head(resolved, &fs)?;
        writeln!(stdout, "Reverted to {}", identifier)?;
    }

    Ok(())
}
