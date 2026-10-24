use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{Arc, mpsc},
};

use blobsdb::{BlobsDBWithFs, Chunk, MAX_CONCURRENT_OPS};
use serde::{Deserialize, Serialize};
use tokio::sync::{AcquireError, Semaphore};
use ver_fs::local_disk_walker;

use crate::{commons::rt, context::read_context, errors::VerError};

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub repository_state: HashMap<String, Vec<Chunk>>,
    pub parent_ids: Vec<[u8; 32]>,
}

pub fn snap<O: io::Write, E: io::Write>(name: &str, description: &str) -> Result<(), VerError> {
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

    let mut failed: Vec<PathBuf> = vec![];
    let mut state: HashMap<String, Vec<Chunk>> = HashMap::new();

    for res in rx {
        if res.0.is_err() {
            failed.push(res.1.path());
        } else {
            state.insert(res.1.path().to_string_lossy().to_string(), res.0.unwrap());
        }
    }

    // To Be Continued...

    Ok(())
}
