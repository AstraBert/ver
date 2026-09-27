use std::{env, str::FromStr};

use redb::{Database, ReadableDatabase};

use crate::{
    commons::{get_head, resolve_ver_dir_path, to_text},
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot},
    init::{REDB_PATH, init_blobs_db},
    named::get_snapshot_by_name,
};

#[cfg(not(feature = "pretty-hex"))]
fn hexdump(bytes: &[u8]) {
    for chunk in bytes.chunks(16) {
        for b in chunk {
            print!("{:02x} ", b);
        }
        println!();
    }
}

#[cfg(feature = "pretty-hex")]
fn hexdump(bytes: &[u8]) {
    use pretty_hex::PrettyHex;

    println!("{:?}", bytes.hex_dump());
}

pub fn show_file(
    path: &str,
    snapshot_id: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<(), VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let rx = redb.begin_read()?;
    let table = rx.open_table(SNAPSHOT_TABLE)?;

    let snap: Snapshot = if let Some(sid) = snapshot_id {
        let maybe_hash = blake3::Hash::from_str(&sid);

        let hash = match maybe_hash {
            Ok(h) => h.as_bytes().to_owned(),
            Err(_) => get_snapshot_by_name(&redb, &sid)?,
        };

        let content = table.get(hash)?;

        if let Some(accessmut) = content {
            postcard::from_bytes(accessmut.value())?
        } else {
            return Err(format!("Could not find snapshot associated with {}", sid).into());
        }
    } else {
        let cur = get_head(&resolved)?;

        if cur.is_none() {
            return Err("No snapshot saved yet, nothing to show".into());
        }

        let l = table.get(cur.unwrap())?;

        if let Some(content) = l {
            postcard::from_bytes(content.value())?
        } else {
            return Err("Head snapshot could not be found".into());
        }
    };

    let path_hash = snap.repository_state.get(path);

    if let Some(ha) = path_hash {
        let blobdb = init_blobs_db(&resolved)?;
        let opt = blobdb.get(ha)?;
        if let Some(content) = opt {
            let offset = offset.unwrap_or_default();
            let limit = content.len().min(limit.unwrap_or(content.len()));
            let new_cont = &content[offset..limit];
            if let Some(text) = to_text(&new_cont) {
                println!("{}", text);
            } else {
                eprintln!(
                    "WARNING: the requested file does not appear to be text-based, it will be hex-dumped"
                );
                hexdump(new_cont);
            }
        } else {
            return Err(format!("Could not find content of {} in blobsdb", path).into());
        }

        return Ok(());
    }

    Err(format!("Could not find {} in the requested snapshot", path).into())
}
