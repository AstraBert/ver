use std::{
    any::Any,
    io,
    str::FromStr,
    sync::{Arc, OnceLock, mpsc},
};

use minus::Pager;
use tokio::runtime::Runtime;
use ver_fs::{Fs, errors::FsError, noignore_local_disk_walker};
use ver_kv::KVStore;

use crate::errors::VerError;

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

pub fn read_history(
    tolerate_incomplete: bool,
    fs: &Arc<dyn Fs>,
) -> Result<(Vec<String>, bool), VerError> {
    let mut it = rt().block_on(async {
        fs.list(HISTORY_PATH, Some(Box::new(noignore_local_disk_walker)))
            .await
    })?;
    let (tx, rx) = mpsc::sync_channel(MAX_CONCURRENT_LOG_READS);
    while let Some(entry) = it.next() {
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
    let mut logs: Vec<String> = vec![];
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
                logs.push(s);
            }
        }
    }

    logs.sort();
    logs.reverse();
    Ok((logs, is_incomplete))
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

pub struct PagerWriter {
    pub pager: Pager,
    pub pending: Vec<u8>, // incomplete UTF-8 sequence carried to the next write
}

impl PagerWriter {
    pub fn new(pager: Pager) -> Self {
        Self {
            pager,
            pending: Vec::new(),
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

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(()) // the pager has no buffering on our side to flush
    }
}
