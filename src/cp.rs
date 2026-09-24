use std::{env, fs, path::Path, str::FromStr};

use fs_extra::dir::{CopyOptions, copy as copy_dir};
use redb::{Database, ReadableDatabase};

use crate::{
    commons::resolve_ver_dir_path,
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot},
    init::{REDB_PATH, init_blobs_db},
    named::get_snapshot_by_name,
};

pub fn copy(snapshot_id: &str, destination: &str) -> Result<(), VerError> {
    let dest_path = Path::new(destination);

    if !dest_path.is_dir() {
        fs::create_dir_all(dest_path)?;
    }

    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let repo = resolved.parent().unwrap();
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    let maybe_hash = blake3::Hash::from_str(snapshot_id);

    let hash = match maybe_hash {
        Ok(h) => h.as_bytes().to_owned(),
        Err(_) => get_snapshot_by_name(&redb, snapshot_id)?,
    };

    let tx = redb.begin_read()?;
    let table = tx.open_table(SNAPSHOT_TABLE)?;
    let maybe_entry = table.get(hash)?;

    if let Some(entry) = maybe_entry {
        let decompressed: Snapshot = postcard::from_bytes(entry.value())?;
        let mut copied = decompressed.repository_state.len();

        for (k, v) in decompressed.repository_state {
            let relative = Path::new(&k)
                .strip_prefix(repo)
                .expect("Repository should always be parent of its files");

            let par = relative.parent();
            if let Some(parent) = par
                && parent != ""
            {
                let full_dir = dest_path.join(parent);
                if !full_dir.is_dir() {
                    fs::create_dir_all(full_dir)?;
                }
            }

            let full_fl = dest_path.join(relative);

            let data = blobdb.get(v)?;

            if let Some(d) = data {
                fs::write(full_fl, d)?;
            }
        }

        copy_dir(resolved, dest_path, &CopyOptions::new())?;

        copied += 1;

        println!("Copied {copied} elements to {destination}");

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
