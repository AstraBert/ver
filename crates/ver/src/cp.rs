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

use crate::{
    commons::resolve_ver_dir_path,
    errors::VerError,
    files::{MAX_INFLIGHT_READS, SNAPSHOT_TABLE, Snapshot, panic_msg, rt},
    init::{REDB_PATH, init_blobs_db},
    named::get_snapshot_by_name,
};

pub fn copy<O: io::Write>(
    snapshot_id: &str,
    destination: &str,
    std_out: &mut O,
) -> Result<(), VerError> {
    let dest_path = Path::new(destination);

    if !dest_path.is_dir() {
        fs::create_dir_all(dest_path)?;
    }

    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(resolved.join(REDB_PATH))?;
    let blobdb = Arc::new(init_blobs_db(&resolved)?);

    let maybe_hash = blake3::Hash::from_str(snapshot_id);

    let hash = match maybe_hash {
        Ok(h) => h.as_bytes().to_owned(),
        Err(_) => get_snapshot_by_name(&redb, snapshot_id)?,
    };

    let trx = redb.begin_read()?;
    let table = trx.open_table(SNAPSHOT_TABLE)?;
    let maybe_entry = table.get(hash)?;

    if let Some(entry) = maybe_entry {
        let decompressed: Snapshot = postcard::from_bytes(entry.value())?;
        let mut copied = decompressed.repository_state.len();
        let state = decompressed
            .repository_state
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect::<HashMap<String, Arc<Vec<Chunk>>>>();
        let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
        let (tx, rx) = mpsc::channel::<(PathBuf, Result<Vec<u8>, String>)>();

        for (k, v) in state {
            let relative = Path::new(&k);

            let par = relative.parent();
            if let Some(parent) = par
                && parent != ""
            {
                let full_dir = dest_path.join(parent);
                if !full_dir.is_dir() {
                    fs::create_dir_all(full_dir)?;
                }
            }

            let full_fl = dest_path.join(relative);

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
                let _ = tx.send((full_fl, res));
            });
        }

        drop(tx);

        for (fl, res) in rx {
            if res.is_err() {
                return Err(res.unwrap_err().into());
            }
            fs::write(fl, res.unwrap())?;
        }

        copy_dir(resolved, dest_path, &CopyOptions::new())?;

        copied += 1;

        writeln!(std_out, "Copied {copied} elements to {destination}")?;

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
