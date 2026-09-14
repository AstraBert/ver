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
