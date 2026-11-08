use std::{
    any::Any,
    collections::HashMap,
    future::Future,
    io,
    str::FromStr,
    sync::{Arc, OnceLock, mpsc},
};

use blobsdb::{BlobsDBWithFs, MAX_CONCURRENT_OPS};
use bytes::Bytes;
use chrono::{DateTime, NaiveDateTime, Utc};
use minus::Pager;
use tokio::{runtime::Runtime, sync::Semaphore};
use ver_fs::{Fs, errors::FsError, local_disk_walker, noignore_local_disk_walker};
use ver_kv::KVStore;

use crate::{
    context::{FilesystemContext, KVStoreContext, read_context},
    errors::VerError,
    snap::Snapshot,
};

pub const HEAD_PATH: &str = ".ver/head";
pub const HISTORY_PATH: &str = ".ver/snapshots_history";
pub static RT: OnceLock<Runtime> = OnceLock::new();
pub const MAX_CONCURRENT_LOG_READS: usize = 64;

pub fn rt() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    })
}

pub fn panic_msg(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

pub fn write_head(snapshot_id: [u8; 32], fs: &Arc<dyn Fs>) -> Result<(), VerError> {
    rt().block_on(async {
        fs.write(
            HEAD_PATH,
            bytes::Bytes::copy_from_slice(snapshot_id.as_slice()),
        )
        .await
    })?;

    Ok(())
}

pub fn get_head(fs: &Arc<dyn Fs>) -> Result<Option<[u8; 32]>, VerError> {
    let bts = rt().block_on(async {
        if fs.exists(HEAD_PATH).await? {
            let content = fs.read(HEAD_PATH).await?;

            return Ok::<Option<bytes::Bytes>, FsError>(Some(content));
        }
        Ok(None)
    })?;

    if let Some(h) = bts {
        let head: [u8; 32] = h.as_ref().try_into()?;
        return Ok(Some(head));
    }

    Ok(None)
}

pub fn write_history(
    snapshot_id: blake3::Hash,
    name: &str,
    description: Option<&str>,
    fs: &Arc<dyn Fs>,
) -> Result<(), VerError> {
    let content = format!(
        "{}\t{}\t{}\t{}",
        chrono::Utc::now().format("%d/%m/%Y %H:%M:%S"),
        snapshot_id,
        name,
        description.unwrap_or_default()
    );
    rt().block_on(async {
        fs.write(
            &format!("{}/{}", HISTORY_PATH, snapshot_id),
            bytes::Bytes::copy_from_slice(content.as_bytes()),
        )
        .await
    })?;

    Ok(())
}

fn parse_date(s: &str) -> Result<DateTime<Utc>, chrono::ParseError> {
    let naive = NaiveDateTime::parse_from_str(s, "%d/%m/%Y %H:%M:%S")?;
    Ok(naive.and_utc())
}

pub fn read_history(
    tolerate_incomplete: bool,
    fs: &Arc<dyn Fs>,
) -> Result<(Vec<String>, bool), VerError> {
    let it = rt().block_on(async {
        fs.list(HISTORY_PATH, Some(Box::new(noignore_local_disk_walker)))
            .await
    })?;
    let (tx, rx) = mpsc::sync_channel(MAX_CONCURRENT_LOG_READS);
    for entry in it {
        let fs = fs.clone();
        let tx = tx.clone();
        let path = entry.path();
        rt().spawn(async move {
            let result = fs.read(&path.to_string_lossy()).await;
            tx.send(result).unwrap()
        });
    }
    drop(tx);

    let mut is_incomplete = false;
    let mut logs: Vec<(String, DateTime<Utc>)> = vec![];
    for res in rx {
        match res {
            Err(e) => {
                if !tolerate_incomplete {
                    return Err(e.into());
                }
                is_incomplete = true;
            }
            Ok(bts) => {
                let s = String::from_utf8_lossy(&bts).to_string();
                let parts = s.split("\t").collect::<Vec<&str>>();
                let dt = parts.first().expect("Should have at least 3 parts");
                let utc = parse_date(dt).expect("First part should be a date");
                logs.push((s, utc));
            }
        }
    }

    logs.sort_by(|a, b| b.1.cmp(&a.1));
    let sorted_logs = logs.into_iter().map(|(s, _)| s).collect();

    Ok((sorted_logs, is_incomplete))
}

pub fn resolve_snapshot_id(identifier: &str, kv: &Arc<dyn KVStore>) -> Result<[u8; 32], VerError> {
    let maybe_hash = blake3::Hash::from_str(identifier);
    match maybe_hash {
        Ok(id) => Ok(id.as_bytes().to_owned()),
        Err(_) => {
            let id = kv.get_name(identifier)?;
            if let Some(i) = id {
                return Ok(i);
            }
            Err(format!("Could not find {identifier} among snapshots").into())
        }
    }
}

/// Build the filesystem and KV store handles from a context descriptor pair,
/// failing with a descriptive error when the context is missing.
pub fn build_context(
    opt: Option<(FilesystemContext, KVStoreContext)>,
    context_name: Option<&str>,
) -> Result<(Arc<dyn Fs>, Arc<dyn KVStore>), VerError> {
    if opt.is_none() {
        return Err(format!("Context {} not found", context_name.unwrap_or("current")).into());
    }
    let (fs_des, kv_des) = opt.unwrap();
    let fs = fs_des.build_fs()?;
    let kv = kv_des.build_kv(context_name.unwrap())?;
    Ok((fs, kv))
}

/// Resolve the current context and build its filesystem and KV store handles.
pub fn current_fs_kv() -> Result<(Arc<dyn Fs>, Arc<dyn KVStore>), VerError> {
    let ctx = read_context()?;
    build_context(ctx.current_context(), ctx.current.as_deref())
}

/// Load and deserialize a snapshot from the KV store.
pub fn load_snapshot(kv: &Arc<dyn KVStore>, snapshot_id: [u8; 32]) -> Result<Snapshot, VerError> {
    let compressed = kv.get_snapshot(snapshot_id)?;
    if compressed.is_none() {
        return Err("Could not find the requested snapshot in the KV store".into());
    }
    Ok(postcard::from_bytes(&compressed.unwrap())?)
}

/// Serialize a snapshot, store it in the KV store under its content hash,
/// and register it under the given name.
pub fn store_snapshot(
    snap: &Snapshot,
    name: &str,
    kv: &Arc<dyn KVStore>,
) -> Result<blake3::Hash, VerError> {
    let compressed = postcard::to_allocvec(snap)?;
    let id = blake3::hash(&compressed);
    let bid = id.as_bytes().to_owned();
    kv.put_snapshot(bid, &compressed)?;
    kv.put_name(name, bid)?;
    Ok(id)
}

/// Serialize a snapshot, store it in the KV store, register it under the
/// given name, make it the current head and append it to the history.
pub fn commit_snapshot(
    snap: &Snapshot,
    name: &str,
    description: Option<&str>,
    fs: &Arc<dyn Fs>,
    kv: &Arc<dyn KVStore>,
) -> Result<blake3::Hash, VerError> {
    let id = store_snapshot(snap, name, kv)?;
    write_head(id.as_bytes().to_owned(), fs)?;
    write_history(id, name, description, fs)?;
    Ok(id)
}

/// Run `op` over `items` with bounded concurrency, collecting each result.
///
/// Every item is processed in its own spawned task guarded by a semaphore
/// capped at [`MAX_CONCURRENT_OPS`]. Results are collected and returned in
/// the same order as the input items.
pub fn run_concurrent<I, T, F, Fut>(items: Vec<I>, op: F) -> Result<Vec<T>, VerError>
where
    I: Send + 'static,
    T: Send + 'static,
    F: Fn(I) -> Fut,
    Fut: Future<Output = T> + Send + 'static,
{
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (tx, rx) = mpsc::channel::<(usize, T)>();

    for (i, item) in items.into_iter().enumerate() {
        let permit = rt().block_on(async {
            sem.clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while trying to acquire semaphore: {e}"))
        })?;
        let tx = tx.clone();
        let fut = op(item);
        rt().spawn(async move {
            let _permit = permit;
            let res = fut.await;
            let _ = tx.send((i, res));
        });
    }

    drop(tx);

    let mut collected: HashMap<usize, T> = HashMap::new();
    for (i, res) in rx {
        collected.insert(i, res);
    }

    let mut keys: Vec<usize> = collected.keys().copied().collect();
    keys.sort_unstable();
    let mut results = Vec::with_capacity(keys.len());
    for k in keys {
        results.push(collected.remove(&k).unwrap());
    }

    Ok(results)
}

/// Reconstruct the working tree described by `snapshot` on `fs`.
///
/// Files present in the snapshot are written from the blob store. Files
/// listed in `paths` (or every file on disk when `paths` is empty) that are
/// missing from the snapshot are deleted when the snapshot is full (not
/// partial) and `delete_if_not_in_snapshot` holds.
/// Returns the list of paths that failed to be deleted and the list of
/// paths that failed to be reconstructed.
pub fn reconstruct_snapshot(
    snapshot: Snapshot,
    db: &BlobsDBWithFs,
    fs: &Arc<dyn Fs>,
    paths: Vec<String>,
    delete_if_not_in_snapshot: bool,
) -> Result<(Vec<String>, Vec<String>), VerError> {
    let mut paths: Vec<String> = if !paths.is_empty() {
        paths
    } else {
        rt().block_on(async { fs.list("", Some(Box::new(local_disk_walker))).await })?
            .map(|e| e.path().to_string_lossy().into_owned())
            .collect()
    };

    let wsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let dsem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let (wtx, wrx) = mpsc::channel::<Result<(), (String, String)>>();
    let (dtx, drx) = mpsc::channel::<Result<(), (String, String)>>();

    for (path, chunks) in snapshot.repository_state {
        let idx = paths.iter().position(|a| a == &path);
        let permit = rt().block_on(async {
            wsem.clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Could not acquire writing semaphore: {e}"))
        })?;
        let db = db.clone();
        let fs = fs.clone();
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
                    let new = fs
                        .write(&path, Bytes::from_owner(c))
                        .await
                        .map_err(|e| (e.to_string(), path));
                    wtx.send(new).unwrap()
                }
                Err(e) => wtx.send(Err(e)).unwrap(),
            }
        });
        if let Some(pos) = idx {
            paths.remove(pos);
        }
    }

    if !paths.is_empty() && !snapshot.partial && delete_if_not_in_snapshot {
        for path in paths {
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
        }
    }

    drop(wtx);
    drop(dtx);

    let mut del_failed: Vec<String> = vec![];
    let mut wrt_failed: Vec<String> = vec![];

    for res in drx {
        if let Err((_, path)) = res {
            del_failed.push(path);
        }
    }

    for res in wrx {
        if let Err((_, path)) = res {
            wrt_failed.push(path);
        }
    }

    Ok((del_failed, wrt_failed))
}

pub struct PagerWriter {
    pub pager: Pager,
    pub pending: Vec<u8>, // incomplete UTF-8 sequence carried to the next write
    pub has_writes: bool,
}

impl PagerWriter {
    pub fn new(pager: Pager) -> Self {
        Self {
            pager,
            pending: Vec::new(),
            has_writes: false,
        }
    }
}

impl io::Write for PagerWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.pending.extend_from_slice(buf);

        let valid_up_to = match std::str::from_utf8(&self.pending) {
            Ok(s) => s.len(),
            // error_len() == None means "unexpected end of input": wait for more bytes
            Err(e) if e.error_len().is_none() => e.valid_up_to(),
            Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
        };

        // Safe: we just validated this prefix
        let text = std::str::from_utf8(&self.pending[..valid_up_to]).unwrap();
        self.pager.push_str(text).map_err(io::Error::other)?;
        self.pending.drain(..valid_up_to);

        self.has_writes = true;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // the pager has no buffering on our side to flush
    }
}
