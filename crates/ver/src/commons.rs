use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{
    errors::VerError,
    init::{HEAD_PATH, VER_DIRECTORY},
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
