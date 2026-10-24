use std::{
    any::Any,
    sync::{Arc, OnceLock},
};

use tokio::runtime::Runtime;
use ver_fs::{Fs, errors::FsError};

use crate::errors::VerError;

pub const HEAD_PATH: &str = ".ver/head";
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

pub fn write_head(snapshot_id: &str, fs: &Arc<dyn Fs>) -> Result<(), VerError> {
    rt().block_on(async {
        fs.write(
            HEAD_PATH,
            bytes::Bytes::copy_from_slice(snapshot_id.as_bytes()),
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
