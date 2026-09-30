use std::path::{Path, PathBuf};

use redb::Database;

pub struct FilesDB {
    pub redb: Database,
    pub directory: PathBuf,
}

impl FilesDB {
    pub fn new(directory: impl Into<PathBuf>, redb: Database) -> Self {
        Self {
            redb,
            directory: directory.into(),
        }
    }

    pub async fn put<P: AsRef<Path>>(snapshot_id: &[u8; 32], file_path: P) -> Result<(), String> {
        Ok(())
    }
}
