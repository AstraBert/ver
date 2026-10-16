use std::{path::PathBuf, sync::Arc};

use futures_util::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjPath};
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
    async fn write<S: AsRef<str> + Send + Sync, D: AsRef<[u8]> + Sync + Send + 'static>(
        &self,
        path: S,
        data: D,
    ) -> Result<(), FsError>;
    async fn delete<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError>;
    async fn list<
        S: AsRef<str> + Send + Sync + 'static,
        F: Fn(S) -> Result<Self::ListIter, FsError> + Send + Sync + 'static,
    >(
        &self,
        path: S,
        walker_fn: F,
    ) -> Result<Self::ListIter, FsError>;
    async fn create_dir<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError>;
}

impl DirEntry for ignore::DirEntry {
    fn path(&self) -> PathBuf {
        self.path().to_owned()
    }
}

#[derive(Debug)]
pub struct DiskListIterator {
    inner: std::vec::IntoIter<ignore::DirEntry>,
}

impl Iterator for DiskListIterator {
    type Item = ignore::DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DiskFs {}

#[async_trait::async_trait]
impl Fs for DiskFs {
    type Entry = ignore::DirEntry;
    type ListIter = DiskListIterator;

    async fn read<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Vec<u8>, FsError> {
        let res = fs::read(path.as_ref())
            .await
            .map_err(|e| FsError::Read(e.to_string()))?;

        Ok(res)
    }

    async fn write<S: AsRef<str> + Send + Sync, D: AsRef<[u8]> + Sync + Send + 'static>(
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

    async fn list<
        S: AsRef<str> + Send + Sync + 'static,
        F: Fn(S) -> Result<Self::ListIter, FsError> + Send + Sync + 'static,
    >(
        &self,
        path: S,
        walker_fn: F,
    ) -> Result<Self::ListIter, FsError> {
        let entries = tokio::task::spawn_blocking(move || walker_fn(path))
            .await
            .map_err(|e| FsError::List(e.to_string()))??;

        Ok(entries)
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreFs {
    pub object_store: Arc<dyn ObjectStore>,
}

#[allow(dead_code)]
impl ObjectStoreFs {
    async fn read<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Vec<u8>, FsError> {
        let response = self
            .object_store
            .get(&ObjPath::from_url_path(path).map_err(|e| FsError::Read(e.to_string()))?)
            .await
            .map_err(|e| FsError::Read(e.to_string()))?;
        let mut stream = response.into_stream();
        let mut v: Vec<u8> = Vec::new();
        while let Some(b) = stream.next().await {
            let bytes = b.map_err(|e| FsError::Read(e.to_string()))?;
            v.extend_from_slice(&bytes);
        }

        Ok(v)
    }

    async fn write<S: AsRef<str> + Send + Sync, D: AsRef<[u8]> + Sync + Send + 'static>(
        &self,
        path: S,
        data: D,
    ) -> Result<(), FsError> {
        self.object_store
            .put(
                &ObjPath::from_url_path(path).map_err(|e| FsError::Write(e.to_string()))?,
                PutPayload::from_iter(data.as_ref().iter().copied()),
            )
            .await
            .map_err(|e| FsError::Write(e.to_string()))?;

        Ok(())
    }
}
