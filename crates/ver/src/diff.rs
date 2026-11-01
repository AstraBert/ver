use std::{io, sync::Arc};

use blobsdb::BlobsDBWithFs;

use crate::{
    commons::{get_head, resolve_snapshot_id, rt},
    context::read_context,
    errors::VerError,
    show::to_text,
    snap::Snapshot,
};

pub fn diff<O: io::Write>(
    path: &str,
    head_snapshot: Option<String>,
    base_snapshot: String,
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
    let head_snapshot_id = if let Some(ref h) = head_snapshot {
        resolve_snapshot_id(h, &kv)?
    } else {
        get_head(&fs)?.expect("The current project has no head")
    };
    let base_snapshot_id = resolve_snapshot_id(&base_snapshot, &kv)?;
    let base_compressed = kv.get_snapshot(base_snapshot_id)?;
    if base_compressed.is_none() {
        return Err("Could not find the requested base snapshot in the KV store".into());
    }
    let base_snap: Snapshot = postcard::from_bytes(&base_compressed.unwrap())?;
    let head_compressed = kv.get_snapshot(head_snapshot_id)?;
    if head_compressed.is_none() {
        return Err("Could not find the requested base snapshot in the KV store".into());
    }
    let head_snap: Snapshot = postcard::from_bytes(&head_compressed.unwrap())?;

    let base_chunks = base_snap.repository_state.get(path);
    let head_chunks = head_snap.repository_state.get(path);

    let db = BlobsDBWithFs::new(fs.clone());

    match (base_chunks, head_chunks) {
        (Some(bc), Some(hc)) => {
            let (base_content, head_content) = rt().block_on(async {
                let bcont = db.get(Arc::new(bc.clone())).await?;
                let hcont = db.get(Arc::new(hc.clone())).await?;
                Ok::<(Vec<u8>, Vec<u8>), String>((bcont, hcont))
            })?;
            let (base_text, head_text) = (to_text(&base_content), to_text(&head_content));
            match (base_text, head_text) {
                (Some(bt), Some(ht)) => {
                    let head_label = head_snapshot.as_deref().unwrap_or("HEAD");
                    let patch = diffy::DiffOptions::new()
                        .set_original_filename(format!("{} ({})", path, &base_snapshot))
                        .set_modified_filename(format!("{} ({})", path, head_label))
                        .create_patch(bt, ht);

                    let formatter = diffy::PatchFormatter::new();
                    write!(stdout, "{}", formatter.fmt_patch(&patch))?;
                }
                (Some(_), None) => {}
                (None, Some(_)) => {}
                (None, None) => {}
            }
        }
        (Some(_), None) => {
            writeln!(
                stdout,
                "Path {} does not exist in head snapshot, but exists in base",
                path
            )?;
        }
        (None, Some(_)) => {
            writeln!(
                stdout,
                "Path {} does not exist in base snapshot, but exists in head",
                path
            )?;
        }
        (None, None) => {
            writeln!(
                stdout,
                "Path {} does not exist in the specified snapshots",
                path
            )?;
        }
    }

    Ok(())
}
