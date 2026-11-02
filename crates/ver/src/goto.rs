use std::io::Write;

use blobsdb::BlobsDBWithFs;

use crate::{
    commons::{
        current_fs_kv, get_head, load_snapshot, reconstruct_snapshot, resolve_snapshot_id,
        write_head,
    },
    errors::VerError,
};

pub fn goto<O: Write, E: Write>(
    identifier: &str,
    paths: Vec<String>,
    delete_if_not_in_snapshot: bool,
    stdout: &mut O,
    stderr: &mut E,
) -> Result<(), VerError> {
    let (fs, kv) = current_fs_kv()?;
    let resolved = resolve_snapshot_id(identifier, &kv)?;
    let head = get_head(&fs)?;
    if head.is_none() {
        return Err("Cannot revert to any previous snapshot, since this project holds none".into());
    }
    let h = head.unwrap();
    if h == resolved {
        writeln!(stdout, "Already on {identifier}")?;
        return Ok(());
    }
    let snapshot = load_snapshot(&kv, resolved)?;

    let db = BlobsDBWithFs::new(fs.clone());
    let (del_failed, wrt_failed) =
        reconstruct_snapshot(snapshot, &db, &fs, paths, delete_if_not_in_snapshot)?;

    if !del_failed.is_empty() || !wrt_failed.is_empty() {
        writeln!(
            stderr,
            "Failed to delete the following files: {}\nFailed to reconstruct the following files: {}",
            del_failed.join(", "),
            wrt_failed.join(", ")
        )?;
    } else {
        write_head(resolved, &fs)?;
        writeln!(stdout, "Reverted to {}", identifier)?;
    }

    Ok(())
}
