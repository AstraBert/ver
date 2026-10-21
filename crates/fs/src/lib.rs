use std::{
    path::PathBuf,
    sync::{Arc, mpsc},
};

use bytes::Bytes;
use futures_util::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjPath};
use tokio::fs;

use crate::errors::FsError;

pub mod errors;

pub trait DirEntry: Send + Sync {
    fn path(&self) -> PathBuf;
}

pub type EntryIter = Box<dyn Iterator<Item = Box<dyn DirEntry>> + Send>;
pub type Walker = Box<dyn Fn(&PathBuf) -> Result<EntryIter, FsError> + Send + Sync>;

#[async_trait::async_trait]
pub trait Fs: Send + Sync {
    async fn read(&self, path: &str) -> Result<Bytes, FsError>;
    async fn write(&self, path: &str, data: Bytes) -> Result<(), FsError>;
    async fn delete(&self, path: &str) -> Result<(), FsError>;
    async fn list(&self, path: &str, walker_fn: Option<Walker>) -> Result<EntryIter, FsError>;
    async fn create_dir(&self, path: &str) -> Result<(), FsError>;
    async fn exists(&self, path: &str) -> Result<bool, FsError>;
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

impl DiskListIterator {
    pub fn into_entry_iter(self) -> EntryIter {
        Box::new(self.map(|e| Box::new(e) as Box<dyn DirEntry>))
    }
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

impl DiskFs {
    fn prefix_path(&self, path: &str) -> PathBuf {
        let p = PathBuf::from(path);
        if p.starts_with(&self.base_dir) {
            p
        } else {
            self.base_dir.join(p)
        }
    }
}

#[async_trait::async_trait]
impl Fs for DiskFs {
    async fn read(&self, path: &str) -> Result<Bytes, FsError> {
        let res = fs::read(self.prefix_path(path))
            .await
            .map_err(|e| FsError::Read(e.to_string()))?;

        Ok(Bytes::from(res))
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<(), FsError> {
        fs::write(self.prefix_path(path), data)
            .await
            .map_err(|e| FsError::Write(e.to_string()))
    }

    async fn delete(&self, path: &str) -> Result<(), FsError> {
        fs::remove_file(self.prefix_path(path))
            .await
            .map_err(|e| FsError::Delete(e.to_string()))
    }

    async fn create_dir(&self, path: &str) -> Result<(), FsError> {
        fs::create_dir_all(self.prefix_path(path))
            .await
            .map_err(|e| FsError::CreateDir(e.to_string()))
    }

    async fn list(&self, path: &str, walker_fn: Option<Walker>) -> Result<EntryIter, FsError> {
        let p = self.prefix_path(path);
        if let Some(walker) = walker_fn {
            let entries = tokio::task::spawn_blocking(move || walker(&p))
                .await
                .map_err(|e| FsError::List(e.to_string()))??;

            return Ok(entries);
        }
        Err(FsError::List(
            "Walker function cannot be None for DiskFs".into(),
        ))
    }

    async fn exists(&self, path: &str) -> Result<bool, FsError> {
        let p = self.prefix_path(path);
        fs::try_exists(p)
            .await
            .map_err(|e| FsError::Exists(e.to_string()))
    }
}

#[derive(Debug, Clone)]
pub struct ObjectStoreFs {
    pub object_store: Arc<dyn ObjectStore>,
}

impl ObjectStoreFs {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self { object_store }
    }
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

impl ObjectStoreIterator {
    pub fn into_entry_iter(self) -> EntryIter {
        Box::new(self.map(|e| Box::new(e) as Box<dyn DirEntry>))
    }
}

impl Iterator for ObjectStoreIterator {
    type Item = ObjectStoreEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

#[async_trait::async_trait]
impl Fs for ObjectStoreFs {
    async fn read(&self, path: &str) -> Result<Bytes, FsError> {
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

        Ok(Bytes::from(v))
    }

    async fn write(&self, path: &str, data: Bytes) -> Result<(), FsError> {
        self.object_store
            .put(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Write(format!("Invalid object path: {}", e)))?,
                PutPayload::from_bytes(data),
            )
            .await
            .map_err(|e| FsError::Write(format!("Write operation failed: {}", e)))?;

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<(), FsError> {
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
    async fn create_dir(&self, _path: &str) -> Result<(), FsError> {
        Ok(())
    }

    async fn list(&self, path: &str, walker_fn: Option<Walker>) -> Result<EntryIter, FsError> {
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
        }
        .into_entry_iter())
    }

    // Object storage path should always be relative
    async fn exists(&self, path: &str) -> Result<bool, FsError> {
        let res = self
            .object_store
            .head(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Exists(format!("Invalid object path: {}", e)))?,
            )
            .await;

        match res {
            Ok(_) => Ok(true),
            Err(e) => match e {
                object_store::Error::NotFound { path: _, source: _ } => Ok(false),
                _ => Err(FsError::Exists(format!("Error while running head: {e}"))),
            },
        }
    }
}
