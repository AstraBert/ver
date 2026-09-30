use std::{env, io, str::FromStr};

use redb::{Database, ReadableDatabase};

use crate::{
    commons::{get_head, resolve_ver_dir_path, to_text},
    errors::VerError,
    files::{SNAPSHOT_TABLE, Snapshot},
    init::{REDB_PATH, init_blobs_db},
    named::get_snapshot_by_name,
};

fn resolve_snapshot(
    redb: &Database,
    table: &redb::ReadOnlyTable<[u8; 32], &[u8]>,
    snapshot_id: &str,
) -> Result<Snapshot, VerError> {
    let maybe_hash = blake3::Hash::from_str(snapshot_id);

    let hash = match maybe_hash {
        Ok(h) => h.as_bytes().to_owned(),
        Err(_) => get_snapshot_by_name(redb, snapshot_id)?,
    };

    let content = table.get(hash)?;

    if let Some(accessmut) = content {
        Ok(postcard::from_bytes(accessmut.value())?)
    } else {
        Err(format!("Could not find snapshot associated with {}", snapshot_id).into())
    }
}

fn get_file_content(
    resolved: &std::path::Path,
    snap: &Snapshot,
    path: &str,
) -> Result<Option<Vec<u8>>, VerError> {
    let path_hash = snap.repository_state.get(path);

    if let Some(ha) = path_hash {
        let blobdb = init_blobs_db(resolved)?;
        let opt = blobdb.get(ha)?;
        return Ok(opt);
    }

    Ok(None)
}

pub fn diff_file<O: io::Write, E: io::Write>(
    path: &str,
    from: Option<String>,
    against: String,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(resolved.join(REDB_PATH))?;
    let rx = redb.begin_read()?;
    let table = rx.open_table(SNAPSHOT_TABLE)?;

    // Resolve the "from" snapshot: use provided value or fall back to HEAD
    let snap_from: Snapshot = if let Some(ref sid) = from {
        resolve_snapshot(&redb, &table, sid)?
    } else {
        let cur = get_head(&resolved)?;

        if cur.is_none() {
            return Err("No snapshot saved yet, nothing to diff from".into());
        }

        let l = table.get(cur.unwrap())?;

        if let Some(content) = l {
            postcard::from_bytes(content.value())?
        } else {
            return Err("Head snapshot could not be found".into());
        }
    };

    let snap_against: Snapshot = resolve_snapshot(&redb, &table, &against)?;

    let content_from = get_file_content(&resolved, &snap_from, path)?;
    let content_against = get_file_content(&resolved, &snap_against, path)?;

    match (content_from, content_against) {
        (None, None) => {
            return Err(format!(
                "Could not find {} in either snapshot",
                path
            )
            .into());
        }
        (None, Some(_)) => {
            writeln!(
                std_err,
                "File {} does not exist in the \"from\" snapshot, only in \"against\"",
                path
            )?;
            return Ok(());
        }
        (Some(_), None) => {
            writeln!(
                std_err,
                "File {} does not exist in the \"against\" snapshot, only in \"from\"",
                path
            )?;
            return Ok(());
        }
        (Some(ref cf), Some(ref ca)) => {
            if cf == ca {
                writeln!(std_out, "Files are identical")?;
                return Ok(());
            }

            let text_from = to_text(cf);
            let text_against = to_text(ca);

            match (text_from, text_against) {
                (Some(tf), Some(ta)) => {
                    let from_label = from.as_deref().unwrap_or("HEAD");
                    let patch = diffy::DiffOptions::new()
                        .set_original_filename(format!("{} ({})", path, from_label))
                        .set_modified_filename(format!("{} ({})", path, against))
                        .create_patch(tf, ta);

                    let formatter = diffy::PatchFormatter::new();
                    write!(std_out, "{}", formatter.fmt_patch(&patch))?;
                }
                _ => {
                    writeln!(
                        std_err,
                        "Cannot diff {}: one or both versions do not appear to be text-based",
                        path
                    )?;
                }
            }
        }
    }

    Ok(())
}
