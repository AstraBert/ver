use std::{io, sync::Arc};

use blobsdb::BlobsDBWithFs;

use crate::{
    commons::{current_fs_kv, get_head, load_snapshot, resolve_snapshot_id, rt},
    errors::VerError,
};

#[cfg(not(feature = "pretty-hex"))]
fn hexdump(bytes: &[u8], mut out: impl io::Write) -> Result<(), VerError> {
    for chunk in bytes.chunks(16) {
        for b in chunk {
            write!(out, "{:02x} ", b)?;
        }
        writeln!(out)?;
    }

    Ok(())
}

#[cfg(feature = "pretty-hex")]
fn hexdump(bytes: &[u8], mut out: impl io::Write) -> Result<(), VerError> {
    use pretty_hex::PrettyHex;

    writeln!(out, "{:?}", bytes.hex_dump())?;

    Ok(())
}

pub fn to_text(content: &[u8]) -> Option<&str> {
    let buf = if content.len() > 8192 {
        &content[..8192]
    } else {
        content
    };
    if buf.contains(&0) {
        None
    } else {
        std::str::from_utf8(content).ok()
    }
}

pub fn show_file<O: io::Write>(
    path: &str,
    snapshot_id: Option<String>,
    offset: Option<usize>,
    limit: Option<usize>,
    stdout: &mut O,
) -> Result<(), VerError> {
    let (fs, kv) = current_fs_kv()?;
    let snap_id = if let Some(id) = snapshot_id {
        resolve_snapshot_id(&id, &kv)?
    } else {
        get_head(&fs)?.expect("The current project has no head")
    };
    let snap = load_snapshot(&kv, snap_id)?;
    let db = BlobsDBWithFs::new(fs.clone());
    if let Some(chunks) = snap.repository_state.get(path) {
        let chunks = Arc::new(chunks.clone());
        let content = rt().block_on(async { db.get(chunks).await })?;
        let content = &content[offset.unwrap_or(0)..limit.unwrap_or(content.len())];
        if let Some(s) = to_text(content) {
            writeln!(stdout, "{}", s)?;
        } else {
            hexdump(content, stdout)?;
        }
    } else {
        return Err(format!("Could not find {path} in the requested snapshot").into());
    }

    Ok(())
}
