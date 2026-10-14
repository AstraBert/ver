use std::path::PathBuf;

use tokio::fs;

use crate::errors::FsError;

pub mod errors;

pub trait DirEntry: Send + Sync {
    fn path(&self) -> PathBuf;
}

#[async_trait::async_trait]
pub trait Fs {
    type Entry: DirEntry;
    type ListIter: Iterator<Item = Self::Entry>;

    async fn read<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Vec<u8>, FsError>;
    async fn write<S: AsRef<str> + Send + Sync, D: AsRef<[u8]> + Sync + Send>(
        &self,
        path: S,
        data: D,
    ) -> Result<(), FsError>;
    async fn delete<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError>;
    async fn list<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Self::ListIter, FsError>;
    async fn create_dir<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError>;
}

impl DirEntry for std::fs::DirEntry {
    fn path(&self) -> PathBuf {
        self.path()
    }
}

#[derive(Debug)]
pub struct DiskListIterator {
    inner: std::vec::IntoIter<std::fs::DirEntry>,
}

impl Iterator for DiskListIterator {
    type Item = std::fs::DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DiskFs {}

#[async_trait::async_trait]
impl Fs for DiskFs {
    type Entry = std::fs::DirEntry;
    type ListIter = DiskListIterator;

    async fn read<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Vec<u8>, FsError> {
        let res = fs::read(path.as_ref())
            .await
            .map_err(|e| FsError::Read(e.to_string()))?;

        Ok(res)
    }

    async fn write<S: AsRef<str> + Send + Sync, D: AsRef<[u8]> + Sync + Send>(
        &self,
        path: S,
        data: D,
    ) -> Result<(), FsError> {
        fs::write(path.as_ref(), data)
            .await
            .map_err(|e| FsError::Write(e.to_string()))
    }

    async fn delete<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError> {
        fs::remove_file(path.as_ref())
            .await
            .map_err(|e| FsError::Delete(e.to_string()))
    }

    async fn create_dir<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError> {
        fs::create_dir_all(path.as_ref())
            .await
            .map_err(|e| FsError::CreateDir(e.to_string()))
    }

    async fn list<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Self::ListIter, FsError> {
        let path = path.as_ref().to_owned();
        let entries = tokio::task::spawn_blocking(move || {
            std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()
        })
        .await
        .map_err(|e| FsError::List(e.to_string()))?
        .map_err(|e| FsError::List(e.to_string()))?;

        Ok(DiskListIterator {
            inner: entries.into_iter(),
        })
    }
}
