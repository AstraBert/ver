use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

#[cfg(feature = "minus")]
use minus::Pager;
#[cfg(feature = "minus")]
use std::io;

use crate::{
    errors::VerError,
    init::{HEAD_PATH, VER_CONTEXT, VER_DIRECTORY},
    secrets::get_secret,
};

pub fn resolve_ver_dir_path(current_dir: PathBuf) -> Result<PathBuf, VerError> {
    let ver_path = current_dir.join(VER_DIRECTORY);
    if ver_path.exists() {
        return Ok(ver_path);
    }
    for ancestor in current_dir.ancestors() {
        let ver = ancestor.join(VER_DIRECTORY);
        if ver.exists() {
            return Ok(ver);
        }
    }

    Err("Could not find a .ver folder in the current directory or in any of its ancestors".into())
}

/// Check whether the provided bytes are valid UTF-8 text, and, if so, return them as string
pub fn to_text(content: &[u8]) -> Option<&str> {
    let buf = if content.len() > 8192 {
        &content[..8192]
    } else {
        content
    };
    if buf.contains(&0) {
        None
    } else {
        std::str::from_utf8(buf).ok()
    }
}

pub fn get_head(resolved: &Path) -> Result<Option<[u8; 32]>, VerError> {
    let content = fs::read_to_string(resolved.join(HEAD_PATH))?;
    if content.is_empty() {
        return Ok(None);
    }
    let h = blake3::Hash::from_str(&content)?;
    Ok(Some(h.as_bytes().to_owned()))
}

pub fn write_head(resolved: &Path, snapshot_id: &blake3::Hash) -> Result<(), VerError> {
    fs::write(resolved.join(HEAD_PATH), snapshot_id.to_string())?;

    Ok(())
}

pub fn unwrap_maybe_secret(
    maybe_secret: Option<&str>,
    key_file: Option<&str>,
) -> Result<Option<String>, VerError> {
    if let Some(ms) = maybe_secret {
        if ms.starts_with("secrets/") {
            let secret = ms.replacen("secrets/", "", 1);
            let mut _out = Vec::new();
            let value = get_secret(&secret, key_file, false, &mut _out)?;
            return Ok(Some(value));
        } else {
            return Ok(Some(ms.to_owned()));
        }
    }
    Ok(None)
}

pub fn resolve_maybe_secret(ms: &str, key_file: Option<&str>) -> Result<String, VerError> {
    if ms.starts_with("secrets/") {
        let secret = ms.replacen("secrets/", "", 1);
        let mut _out = Vec::new();
        let value = get_secret(&secret, key_file, false, &mut _out)?;
        Ok(value)
    } else {
        Ok(ms.to_owned())
    }
}

#[cfg(feature = "minus")]
pub struct PagerWriter {
    pub pager: Pager,
    pub pending: Vec<u8>, // incomplete UTF-8 sequence carried to the next write
}

#[cfg(feature = "minus")]
impl PagerWriter {
    pub fn new(pager: Pager) -> Self {
        Self {
            pager,
            pending: Vec::new(),
        }
    }
}

#[cfg(feature = "minus")]
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

pub fn global_context() -> Result<PathBuf, VerError> {
    let ctx = dirs::home_dir()
        .expect("Expecting a HOME directory")
        .join(VER_DIRECTORY)
        .join(VER_CONTEXT);
    if let Some(par) = ctx.parent()
        && !par.exists()
    {
        fs::create_dir(par)?;
    }

    Ok(ctx)
}
