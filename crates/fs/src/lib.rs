use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
};

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
        walker_fn: Option<F>,
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

#[derive(Debug, Default, Clone)]
pub struct DiskFs {
    pub base_dir: PathBuf,
}

impl DiskFs {
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }
}

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
        walker_fn: Option<F>,
    ) -> Result<Self::ListIter, FsError> {
        if let Some(walker) = walker_fn {
            let entries = tokio::task::spawn_blocking(move || walker(path))
                .await
                .map_err(|e| FsError::List(e.to_string()))??;

            return Ok(entries);
        }
        Err(FsError::List(
            "Walker function cannot be None for DiskFs".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreFs {
    pub object_store: Arc<dyn ObjectStore>,
}

#[derive(Debug, Clone)]
pub struct ObjectStoreEntry {
    pub location: ObjPath,
}

impl ObjectStoreEntry {
    pub fn new(location: ObjPath) -> Self {
        Self { location }
    }
}

impl DirEntry for ObjectStoreEntry {
    fn path(&self) -> PathBuf {
        PathBuf::from(self.location.to_string())
    }
}

#[derive(Debug)]
pub struct ObjectStoreIterator {
    inner: std::sync::mpsc::IntoIter<ObjectStoreEntry>,
}

impl Iterator for ObjectStoreIterator {
    type Item = ObjectStoreEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[async_trait::async_trait]
impl Fs for ObjectStoreFs {
    type Entry = ObjectStoreEntry;
    type ListIter = ObjectStoreIterator;

    async fn read<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<Vec<u8>, FsError> {
        let response = self
            .object_store
            .get(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Read(format!("Invalid object path: {}", e)))?,
            )
            .await
            .map_err(|e| FsError::Read(format!("Read operation failed: {}", e)))?;
        let mut stream = response.into_stream();
        let mut v: Vec<u8> = Vec::new();
        while let Some(b) = stream.next().await {
            let bytes = b.map_err(|e| FsError::Read(format!("Bytes streaming failed: {}", e)))?;
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
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Write(format!("Invalid object path: {}", e)))?,
                PutPayload::from_iter(data.as_ref().iter().copied()),
            )
            .await
            .map_err(|e| FsError::Write(format!("Write operation failed: {}", e)))?;

        Ok(())
    }

    async fn delete<S: AsRef<str> + Send + Sync>(&self, path: S) -> Result<(), FsError> {
        self.object_store
            .delete(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Delete(format!("Invalid object path: {}", e)))?,
            )
            .await
            .map_err(|e| FsError::Delete(format!("Delete operation failed: {}", e)))?;
        Ok(())
    }

    // no-op, object stores can't create directories
    async fn create_dir<S: AsRef<str> + Send + Sync>(&self, _path: S) -> Result<(), FsError> {
        Ok(())
    }

    async fn list<
        S: AsRef<str> + Send + Sync + 'static,
        F: Fn(S) -> Result<Self::ListIter, FsError> + Send + Sync + 'static,
    >(
        &self,
        path: S,
        walker_fn: Option<F>,
    ) -> Result<Self::ListIter, FsError> {
        if walker_fn.is_some() {
            return Err(FsError::List(
                "ObjectStore does not support custom walker functions".into(),
            ));
        }

        let (tx, rx) = mpsc::sync_channel(1);
        let handle = tokio::runtime::Handle::current();

        let stream = self
            .object_store
            .list(Some(&ObjPath::from_url_path(path).map_err(|e| {
                FsError::Delete(format!("Invalid object path: {}", e))
            })?));

        handle.spawn(async move {
            let mut stream = Box::pin(stream);
            while let Some(item) = stream.next().await {
                if item.is_ok() {
                    let entry = ObjectStoreEntry::new(item.unwrap().location);
                    if tx.send(entry).is_err() {
                        break; // consumer dropped the iterator
                    }
                } else {
                    break; // break on first error
                }
            }
        });

        Ok(ObjectStoreIterator {
            inner: rx.into_iter(),
        })
    }
}
