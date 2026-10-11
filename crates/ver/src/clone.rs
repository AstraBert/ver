use blobsdb::{GetObjectStoreInput, download_remote_repository, get_object_store};
use futures_util::FutureExt;
use redb::{Database, ReadableDatabase};
use std::{
    fs,
    io::Write,
    panic::AssertUnwindSafe,
    path::PathBuf,
    sync::{Arc, mpsc},
};
use tokio::sync::Semaphore;

use crate::{
    commons::get_head,
    errors::VerError,
    files::{MAX_INFLIGHT_READS, SNAPSHOT_TABLE, Snapshot, panic_msg, rt},
    init::{REDB_PATH, init_blobs_db},
};

pub fn clone_from_remote<O: Write, E: Write>(
    remote_name: String,
    out_dir: String,
    input: GetObjectStoreInput,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let store = get_object_store(input)?;
    let p = PathBuf::from(&out_dir).join(".ver");

    if !p.exists() {
        fs::create_dir_all(&p)?;
    }

    if !p.join("blobs").exists() {
        fs::create_dir(&p.join("blobs"))?;
    }

    let downloaded_chunks =
        rt().block_on(async { download_remote_repository(&store, &p, std_err).await })?;

    writeln!(
        std_out,
        "Downloaded {} chunks from remote storage",
        downloaded_chunks
    )?;

    let blobdb = Arc::new(init_blobs_db(&p)?);

    rt().block_on(async { blobdb.create_sync_map_after_download(remote_name).await })?;

    let head = get_head(&p)?.ok_or_else(|| "No head found in the cloned ver repository")?;

    let redb = Database::open(&p.join(REDB_PATH))?;

    let tx = redb.begin_read()?;
    let table = tx.open_table(SNAPSHOT_TABLE)?;
    let head_snap = table.get(head)?;

    if let Some(hs) = head_snap {
        let snap: Snapshot = postcard::from_bytes(hs.value())?;

        let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
        let (tx, rx) = mpsc::channel::<(PathBuf, Result<Vec<u8>, String>)>();

        for (path, chunks) in snap.repository_state {
            let full_path = p
                .parent()
                .ok_or_else(|| "Could not find parent for the cloned ver repository")?
                .join(path);

            if let Some(par) = full_path.parent()
                && !par.exists()
            {
                fs::create_dir_all(par)?;
            }

            let tx = tx.clone();
            // Blocks the calling (sync) thread until a slot is free
            let permit = rt()
                .block_on(Arc::clone(&sem).acquire_owned())
                .expect("semaphore closed");
            let db = blobdb.clone();

            rt().spawn(async move {
                let _permit = permit; // moved in, released when the task ends
                let res = match AssertUnwindSafe(async move { db.get(Arc::new(chunks)).await })
                    .catch_unwind()
                    .await
                {
                    Ok(r) => r,
                    Err(payload) => Err(panic_msg(payload)),
                };
                let _ = tx.send((full_path, res));
            });
        }

        drop(tx);

        let mut written = 0;

        for (fl, res) in rx {
            if res.is_err() {
                return Err(res.unwrap_err().into());
            }
            fs::write(fl, res.unwrap())?;
            written += 1;
        }

        writeln!(
            std_out,
            "Written {} files to the cloned repository",
            written
        )?;

        return Ok(());
    }

    Err("Could not find head snapshot".into())
}
