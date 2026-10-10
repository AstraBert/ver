use std::{fs, io::Write, path::PathBuf};

use blobsdb::{GetObjectStoreInput, download_remote_repository, get_object_store};

use crate::{
    commons::get_head,
    errors::VerError,
    files::{goto, rt},
    init::init_blobs_db,
};

pub fn clone_from_remote<O: Write>(
    remote_name: String,
    out_dir: String,
    input: GetObjectStoreInput,
    std_out: &mut O,
) -> Result<(), VerError> {
    let store = get_object_store(input)?;
    let p = PathBuf::from(&out_dir).join(".ver");

    if !p.exists() {
        fs::create_dir_all(&p)?;
    }

    if !p.join("blobs").exists() {
        fs::create_dir(&p.join("blobs"))?;
    }

    let downloaded_chunks =
        rt().block_on(async { download_remote_repository(&store, &p).await })?;

    writeln!(
        std_out,
        "Downloaded {} chunks from remote storage",
        downloaded_chunks
    )?;

    let blobsdb = init_blobs_db(&p)?;

    rt().block_on(async { blobsdb.create_sync_map_after_download(remote_name).await })?;

    let head = get_head(&p)?.unwrap();

    let cur_dir = std::env::current_dir()?;

    std::env::set_current_dir(out_dir)?;

    goto::<String>(&blake3::Hash::from_bytes(head).to_string(), None)?;

    std::env::set_current_dir(cur_dir)?;

    Ok(())
}
