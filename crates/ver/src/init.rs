use std::fs;

use blobsdb::BLOBSDB_PREFIX;

use crate::{
    commons::{HISTORY_PATH, rt},
    context::{FilesystemContext, KVStoreContext, read_context},
    errors::VerError,
};

pub fn init(global: bool) -> Result<(), VerError> {
    if global {
        fs::create_dir_all(
            dirs::home_dir()
                .expect("Should have a HOME directory")
                .join(".ver"),
        )?;
        return Ok(());
    }

    let ctx = read_context()?;
    let opt = ctx.current_context();

    if opt.is_none() {
        return Err(
            "No context is currently active, cannot initialize the associated repository".into(),
        );
    }

    let (fs, kv) = opt.unwrap();

    // only needed for local fs, object storage
    // can't create folders
    if let FilesystemContext::Local { base_directory: _ } = fs {
        let f = fs.build_fs()?;
        rt().block_on(async {
            f.create_dir(BLOBSDB_PREFIX).await?;
            f.create_dir(HISTORY_PATH).await
        })?;
    }

    // only needed for embedded, redis is a separate
    // process
    if let KVStoreContext::Embedded { redb_path } = kv {
        redb::Database::create(redb_path)?;
    }

    Ok(())
}
