use std::fs;

use blobsdb::BLOBSDB_PREFIX;

use crate::{
    commons::{HISTORY_PATH, rt},
    context::{FilesystemContext, KVStoreContext, read_context},
    errors::VerError,
};

pub fn init_context(context: Option<&str>) -> Result<(), VerError> {
    let ctx = read_context()?;
    let opt = if context.is_none() {
        ctx.current_context()
    } else {
        ctx.get_context(context.unwrap()).ok()
    };

    if opt.is_none() {
        return Err(
            "The requested context does not seem to exist or be active, cannot initialize the associated project".into(),
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
        let db = redb::Database::create(redb_path)?;
        drop(db);
    }

    Ok(())
}

pub fn init(global: bool) -> Result<(), VerError> {
    if global {
        fs::create_dir_all(
            dirs::home_dir()
                .expect("Should have a HOME directory")
                .join(".ver"),
        )?;
        return Ok(());
    }

    init_context(None)
}
