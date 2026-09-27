use std::path::PathBuf;

use crate::{errors::VerError, init::VER_DIRECTORY};
use memoize::memoize;

#[memoize]
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

pub fn to_text<'a>(content: &'a [u8]) -> Option<&'a str> {
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
