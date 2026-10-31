use std::{io, sync::Arc};

use blobsdb::BlobsDBWithFs;

use crate::{
    commons::{get_head, resolve_snapshot_id, rt},
    context::read_context,
    errors::VerError,
    snap::Snapshot,
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
    let ctx = read_context()?;
    let opt = ctx.current_context();
    if opt.is_none() {
        return Err("No current context found".into());
    }
    let (fs_des, kv_des) = opt.unwrap();
    let fs = fs_des.build_fs()?;
    let kv = kv_des.build_kv()?;
    let snap_id = if let Some(id) = snapshot_id {
        resolve_snapshot_id(&id, &kv)?
    } else {
        get_head(&fs)?.expect("The current project has no head")
    };
    let compressed = kv.get_snapshot(snap_id)?;
    if compressed.is_none() {
        return Err("Could not find the requested snapshot in the KV store".into());
    }
    let snap: Snapshot = postcard::from_bytes(&compressed.unwrap())?;
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
