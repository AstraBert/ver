use blobsdb::Chunk;
use fs_extra::dir::{CopyOptions, copy as copy_dir};
use futures_util::FutureExt;
use redb::{Database, ReadableDatabase};
use std::{
    collections::HashMap,
    env, fs, io,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, mpsc},
};
use tokio::sync::Semaphore;
use ver_fs::errors::FsError;
use ver_kv::{KVStore, RedbStore, RedisStore};

use crate::{
    commons::resolve_ver_dir_path,
    context::{VerContext, VerFsBackend, VerKVBackend, build_fs},
    errors::VerError,
    files::{MAX_INFLIGHT_READS, MAX_INFLIGHT_WRITES, SNAPSHOT_TABLE, Snapshot, panic_msg, rt},
    init::{REDB_PATH, VER_DIRECTORY, init_blobs_db},
    named::get_snapshot_by_name,
};

pub fn copy<O: io::Write>(
    snapshot_id: &str,
    destination: &str,
    std_out: &mut O,
) -> Result<(), VerError> {
    let (ctx, _) = VerContext::from_file()?;
    let (back_fs_cur, back_kv_cur) = ctx.get_backends(None)?;
    let (back_fs_dest, back_kv_dest) = ctx.get_backends(Some(destination))?;

    let fs_cur = build_fs(&back_fs_cur)?;
    let fs_dest = build_fs(&back_fs_dest)?;
    let kv_cur: Arc<dyn KVStore> = match back_kv_cur {
        VerKVBackend::Embedded => {
            let VerFsBackend::Local { base_dir } = &back_fs_cur else {
                return Err("Expecting local filesystem with an embedded KV".into());
            };
            let redb = Database::open(PathBuf::from(base_dir).join(VER_DIRECTORY).join(REDB_PATH))?;
            Arc::new(RedbStore::new(&redb))
        }
        VerKVBackend::Redis { connection_string } => Arc::new(RedisStore::new(connection_string)?),
    };
    let kv_dest: Arc<dyn KVStore> = match back_kv_dest {
        VerKVBackend::Embedded => {
            let VerFsBackend::Local { base_dir } = &back_fs_dest else {
                return Err("Expecting local filesystem with an embedded KV".into());
            };
            let redb = Database::open(PathBuf::from(base_dir).join(VER_DIRECTORY).join(REDB_PATH))?;
            Arc::new(RedbStore::new(&redb))
        }
        VerKVBackend::Redis { connection_string } => Arc::new(RedisStore::new(connection_string)?),
    };

    // open databases
    let blobdb = Arc::new(init_blobs_db(&Path::new("something"))?);

    let maybe_hash = blake3::Hash::from_str(snapshot_id);

    let hash = match maybe_hash {
        Ok(h) => h.as_bytes().to_owned(),
        Err(_) => get_snapshot_by_name(&kv_cur, snapshot_id)?,
    };

    let maybe_entry = kv_cur.get_snapshot(hash)?;

    if let Some(entry) = maybe_entry {
        let decompressed: Snapshot = postcard::from_bytes(entry.iter().as_slice())?;
        let mut copied = decompressed.repository_state.len();
        let state = decompressed
            .repository_state
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect::<HashMap<String, Arc<Vec<Chunk>>>>();
        let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
        let (tx, rx) = mpsc::channel::<(&Path, Result<Vec<u8>, String>)>();

        for (k, v) in state {
            let relative = Path::new(&k);

            let par = relative.parent();
            if let Some(parent) = par
                && !parent.is_empty()
            {
                if rt().block_on(fs_dest.exists(&parent.to_string_lossy()))? {
                    rt().block_on(fs_dest.create_dir(&parent.to_string_lossy()))?;
                }
            }

            let tx = tx.clone();
            // Blocks the calling (sync) thread until a slot is free
            let permit = rt()
                .block_on(Arc::clone(&sem).acquire_owned())
                .expect("semaphore closed");
            let db = blobdb.clone();

            rt().spawn(async move {
                let _permit = permit; // moved in, released when the task ends
                let res = match AssertUnwindSafe(async move { db.get(v).await })
                    .catch_unwind()
                    .await
                {
                    Ok(r) => r,
                    Err(payload) => Err(panic_msg(payload)),
                };
                let _ = tx.send((relative, res));
            });
        }

        drop(tx);

        let (txw, rxw) = mpsc::channel::<Result<(), String>>();
        let semw = Arc::new(Semaphore::new(MAX_INFLIGHT_WRITES));

        for (fl, res) in rx {
            if res.is_err() {
                return Err(res.unwrap_err().into());
            }
            let permitw = rt()
                .block_on(Arc::clone(&semw).acquire_owned())
                .expect("semaphore closed");
            rt().spawn(async move {
                let _permit = permitw; // moved in, released when the task ends
                let res = match AssertUnwindSafe(async move {
                    fs_dest
                        .write(&fl.to_string_lossy(), res.unwrap().as_slice())
                        .await
                })
                .catch_unwind()
                .await
                {
                    Ok(r) => r.map_err(|e| e.to_string()),
                    Err(payload) => Err(panic_msg(payload)),
                };
                let _ = txw.send(res);
            });
        }

        drop(txw);

        for r in rxw {
            if r.is_err() {
                return Err(r.unwrap_err().into());
            }
        }

        drop(blobdb);

        copy_dir(resolved, dest_path, &CopyOptions::new())?;

        copied += 1;

        writeln!(std_out, "Copied {copied} elements to {destination}")?;

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
