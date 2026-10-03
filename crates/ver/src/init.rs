use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

use blobsdb::{BlobsDB, get_object_store};
use redb::{Database, Error as RedbError};

use crate::errors::VerError;

pub const VER_DIRECTORY: &str = ".ver";
pub const SNAPSHOT_HISTORY_FILE: &str = "snapshot_history";
pub const REDB_PATH: &str = "files.redb";
pub const BLOBDB_PATH: &str = "blobs";
pub const HEAD_PATH: &str = "head";

pub fn init_blobs_db(base_path: &Path) -> Result<BlobsDB, VerError> {
    let store = get_object_store(blobsdb::GetObjectStoreInput::Local)?;
    let p = base_path.join(BLOBDB_PATH);

    if !p.exists() {
        fs::create_dir_all(&p)?;
    }

    let db = BlobsDB::new(&p, store)?;

    Ok(db)
}

pub fn create_redb(base_path: &Path) -> Result<Database, RedbError> {
    let db = Database::create(base_path.join(REDB_PATH))?;

    Ok(db)
}

pub fn initialize<O: io::Write>(directory: Option<&str>, std_out: &mut O) -> Result<(), VerError> {
    let init_dir = directory.map_or(
        env::current_dir().expect("Could not infer the current directory"),
        PathBuf::from,
    );
    let _ = init_blobs_db(&init_dir.join(VER_DIRECTORY))?;
    let _ = create_redb(&init_dir.join(VER_DIRECTORY))?;
    fs::write(init_dir.join(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE), "")?;
    fs::write(init_dir.join(VER_DIRECTORY).join(HEAD_PATH), "")?;

    writeln!(std_out, "ver repository successfully initialized")?;

    Ok(())
}
