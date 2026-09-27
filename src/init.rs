use std::{
    env, fs,
    path::{Path, PathBuf},
};

use redb::{Database, Error as RedbError};
use rocksdb::{DB, Error as RocksDBError, Options};

use crate::errors::VerError;

pub const VER_DIRECTORY: &str = ".ver";
pub const SNAPSHOT_HISTORY_FILE: &str = "snapshot_history";
pub const REDB_PATH: &str = "files.redb";
pub const BLOBDB_PATH: &str = "blobs.db";
pub const HEAD_PATH: &str = "head";

pub fn init_blobs_db(base_path: &Path) -> Result<DB, RocksDBError> {
    let mut opts = Options::default();
    opts.create_if_missing(true);

    opts.set_enable_blob_files(true); // turns on key-value separation
    opts.set_min_blob_size(1024); // values ≥ this size go to blob files
    opts.set_blob_file_size(256 * 1024 * 1024); // size cap per blob file
    opts.set_blob_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_enable_blob_gc(true); // reclaim space from stale blobs during compaction

    let db = DB::open(&opts, base_path.join(BLOBDB_PATH))?;

    Ok(db)
}

pub fn create_redb(base_path: &Path) -> Result<Database, RedbError> {
    let db = Database::create(base_path.join(REDB_PATH))?;

    Ok(db)
}

pub fn initialize(directory: Option<&str>) -> Result<(), VerError> {
    let init_dir = directory.map_or(
        env::current_dir().expect("Could not infer the current directory"),
        |d| PathBuf::from(d),
    );
    let _ = init_blobs_db(&init_dir.join(VER_DIRECTORY))?;
    let _ = create_redb(&init_dir.join(VER_DIRECTORY))?;
    fs::write(
        &init_dir.join(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE),
        "",
    )?;
    fs::write(&init_dir.join(VER_DIRECTORY).join(HEAD_PATH), "")?;

    println!("ver repository successfully initialized");

    Ok(())
}
