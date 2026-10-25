use std::{
    any::Any,
    str::FromStr,
    sync::{Arc, OnceLock},
};

use tokio::runtime::Runtime;
use ver_fs::{Fs, errors::FsError};
use ver_kv::KVStore;

use crate::errors::VerError;

pub const HEAD_PATH: &str = ".ver/head";
pub const HISTORY_PATH: &str = ".ver/snapshots_history";
pub static RT: OnceLock<Runtime> = OnceLock::new();

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
