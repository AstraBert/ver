use std::{io, path::PathBuf, pin::Pin, sync::Arc};

use bytes::Bytes;
use futures_util::stream::StreamExt;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjPath};
use tokio::{fs, io::AsyncRead, sync::Semaphore, task::JoinSet};
use tokio_util::io::StreamReader;

use crate::errors::FsError;

pub mod errors;
pub mod object_store_factory;

pub trait DirEntry: Send + Sync {
    fn path(&self) -> PathBuf;
}

pub const MAX_CONCURRENT_COPIES: usize = 16;
pub type EntryIter = Box<dyn Iterator<Item = Box<dyn DirEntry>> + Send>;
pub type Walker = Box<dyn Fn(&PathBuf, &PathBuf) -> Result<EntryIter, FsError> + Send + Sync>;
pub type BoxReader = Pin<Box<dyn AsyncRead + Send>>;

#[async_trait::async_trait]
pub trait Fs: Send + Sync {
    async fn read(&self, path: &str) -> Result<Bytes, FsError>;
    async fn write(&self, path: &str, data: Bytes) -> Result<(), FsError>;
    async fn delete(&self, path: &str) -> Result<(), FsError>;
    async fn list(&self, path: &str, walker_fn: Option<Walker>) -> Result<EntryIter, FsError>;
    async fn create_dir(&self, path: &str) -> Result<(), FsError>;
    async fn exists(&self, path: &str) -> Result<bool, FsError>;
    async fn stream_read(&self, path: &str) -> Result<BoxReader, FsError>;
    async fn copy_dir(
        &self,
        dest: &Arc<dyn Fs>,
        path: Option<&str>,
        ignore: bool,
    ) -> Result<(), FsError>;
    async fn copy_files(&self, dest: &Arc<dyn Fs>, paths: Vec<&str>) -> Result<(), FsError>;
}

impl DirEntry for ignore::DirEntry {
    fn path(&self) -> PathBuf {
        self.path().to_owned()
    }
}

#[derive(Debug)]
pub struct RelativeDirEntry {
    pub path: PathBuf,
}

impl DirEntry for RelativeDirEntry {
    fn path(&self) -> PathBuf {
        self.path.to_owned()
    }
}

#[derive(Debug)]
pub struct DiskListIterator {
    inner: std::vec::IntoIter<RelativeDirEntry>,
}

impl DiskListIterator {
    pub fn into_entry_iter(self) -> EntryIter {
        Box::new(self.map(|e| Box::new(e) as Box<dyn DirEntry>))
    }
}

impl Iterator for DiskListIterator {
    type Item = RelativeDirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl DiskListIterator {
    pub fn new(inner: std::vec::IntoIter<RelativeDirEntry>) -> Self {
        Self { inner }
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

fn to_relative_path(entry: ignore::DirEntry, parent: &PathBuf) -> RelativeDirEntry {
    let p = entry.path();
    let rel = p
        .strip_prefix(parent)
        .expect("Should always start with self.base_dir");
    RelativeDirEntry {
        path: rel.to_path_buf(),
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
        let p = self.prefix_path(path);
        if !p.exists() && p.parent().is_some_and(|par| !par.exists()) {
            self.create_dir(&p.parent().unwrap().to_string_lossy())
                .await?;
        }
        fs::write(self.prefix_path(path), data)
            .await
            .map_err(|e| FsError::Write(e.to_string()))
    }

    async fn delete(&self, path: &str) -> Result<(), FsError> {
        let res = fs::remove_file(self.prefix_path(path)).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(()),
                _ => Err(FsError::Delete(format!("Error while deleting file: {e}"))),
            },
        }
    }

    async fn create_dir(&self, path: &str) -> Result<(), FsError> {
        fs::create_dir_all(self.prefix_path(path))
            .await
            .map_err(|e| FsError::CreateDir(e.to_string()))
    }

    async fn list(&self, path: &str, walker_fn: Option<Walker>) -> Result<EntryIter, FsError> {
        let p = self.prefix_path(path);
        let parent = self.base_dir.clone();
        if let Some(walker) = walker_fn {
            let entries = tokio::task::spawn_blocking(move || walker(&p, &parent))
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

    async fn stream_read(&self, path: &str) -> Result<BoxReader, FsError> {
        let p = self.prefix_path(path);
        let file = fs::File::open(p).await.map_err(|e| {
            FsError::Read(format!(
                "Error while opening the file for stream-reading: {e}"
            ))
        })?;

        Ok(Box::pin(file))
    }

    async fn copy_dir(
        &self,
        dest: &Arc<dyn Fs>,
        path: Option<&str>,
        ignore: bool,
    ) -> Result<(), FsError> {
        let iterator = if let Some(p) = path {
            if ignore {
                self.list(p, Some(Box::new(local_disk_walker))).await?
            } else {
                self.list(p, Some(Box::new(noignore_local_disk_walker)))
                    .await?
            }
        } else {
            if ignore {
                self.list("", Some(Box::new(local_disk_walker))).await?
            } else {
                self.list("", Some(Box::new(noignore_local_disk_walker)))
                    .await?
            }
        };
        let mut join_set = JoinSet::<Result<(), (FsError, String)>>::new();
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_COPIES));

        for entry in iterator {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| FsError::Copy(format!("Failed to acquire lock: {e}")))?;
            let fs_dest = dest.clone();
            let fs_src = self.clone();
            join_set.spawn(async move {
                let _permit = permit;
                let p = entry.path();
                // Normalize path separators to forward slashes for cross-platform
                // compatibility, especially when writing to object stores
                let path_str = p.to_string_lossy().replace('\\', "/");
                let content = fs_src
                    .read(&path_str)
                    .await
                    .map_err(|e| (e, path_str.clone()))?;
                fs_dest
                    .write(&path_str, content)
                    .await
                    .map_err(|e| (e, path_str.clone()))?;

                Ok(())
            });
        }

        let mut failed = vec![];

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(_)) => continue,
                Ok(Err((_, p))) => {
                    failed.push(p);
                }
                Err(e) => return Err(FsError::Copy(format!("Spawn job panicked: {e}"))),
            }
        }

        if !failed.is_empty() {
            return Err(FsError::Copy(format!(
                "Failed to copy the following paths: {}",
                failed.join(", ")
            )));
        }

        Ok(())
    }

    async fn copy_files(&self, dest: &Arc<dyn Fs>, paths: Vec<&str>) -> Result<(), FsError> {
        let mut join_set = JoinSet::<Result<(), (FsError, String)>>::new();
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_COPIES));

        for p in paths {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| FsError::Copy(format!("Failed to acquire lock: {e}")))?;
            let fs_dest = dest.clone();
            let fs_src = self.clone();
            let p = p.to_string();
            join_set.spawn(async move {
                let _permit = permit;
                let content = fs_src.read(&p).await.map_err(|e| (e, p.clone()))?;
                fs_dest.write(&p, content).await.map_err(|e| (e, p))?;

                Ok(())
            });
        }

        let mut failed = vec![];

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(_)) => continue,
                Ok(Err((_, p))) => {
                    failed.push(p);
                }
                Err(e) => return Err(FsError::Copy(format!("Spawn job panicked: {e}"))),
            }
        }

        if !failed.is_empty() {
            return Err(FsError::Copy(format!(
                "Failed to copy the following paths: {}",
                failed.join(", ")
            )));
        }

        Ok(())
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
    inner: std::vec::IntoIter<ObjectStoreEntry>,
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
        let res = self
            .object_store
            .delete(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Delete(format!("Invalid object path: {}", e)))?,
            )
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => match e {
                object_store::Error::NotFound { path: _, source: _ } => Ok(()),
                _ => Err(FsError::Delete(format!("Delete operation failed: {e}"))),
            },
        }
    }

    // no-op, object stores can't create directories
    async fn create_dir(&self, _path: &str) -> Result<(), FsError> {
        Ok(())
    }

    async fn list(&self, path: &str, _walker_fn: Option<Walker>) -> Result<EntryIter, FsError> {
        // Collect eagerly: returning a channel-backed iterator fed by a
        // spawned task would deadlock single-threaded runtimes, where the
        // blocking `next()` prevents the producer from ever running.
        let stream = self
            .object_store
            .list(Some(&ObjPath::from_url_path(path).map_err(|e| {
                FsError::Delete(format!("Invalid object path: {}", e))
            })?));

        let mut entries = Vec::new();
        let mut stream = Box::pin(stream);
        while let Some(item) = stream.next().await {
            let item =
                item.map_err(|e| FsError::List(format!("Error while listing objects: {e}")))?;
            entries.push(ObjectStoreEntry::new(item.location));
        }

        Ok(ObjectStoreIterator {
            inner: entries.into_iter(),
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

    async fn stream_read(&self, path: &str) -> Result<BoxReader, FsError> {
        let response = self
            .object_store
            .get(
                &ObjPath::from_url_path(path)
                    .map_err(|e| FsError::Read(format!("Invalid object path: {}", e)))?,
            )
            .await
            .map_err(|e| FsError::Read(format!("Read operation failed: {}", e)))?;
        let stream = response.into_stream();

        Ok(Box::pin(StreamReader::new(stream)))
    }

    async fn copy_dir(
        &self,
        dest: &Arc<dyn Fs>,
        path: Option<&str>,
        ignore: bool,
    ) -> Result<(), FsError> {
        let iterator = if let Some(p) = path {
            if ignore {
                self.list(p, None).await?
            } else {
                self.list(p, None).await?
            }
        } else {
            if ignore {
                self.list("", None).await?
            } else {
                self.list("", None).await?
            }
        };
        let mut join_set = JoinSet::<Result<(), (FsError, String)>>::new();
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_COPIES));

        for entry in iterator {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| FsError::Copy(format!("Failed to acquire lock: {e}")))?;
            let fs_dest = dest.clone();
            let fs_src = self.clone();
            join_set.spawn(async move {
                let _permit = permit;
                let p = entry.path();
                // Normalize path separators to forward slashes for cross-platform
                // compatibility, especially when writing to object stores
                let path_str = p.to_string_lossy().replace('\\', "/");
                let content = fs_src
                    .read(&path_str)
                    .await
                    .map_err(|e| (e, path_str.clone()))?;
                fs_dest
                    .write(&path_str, content)
                    .await
                    .map_err(|e| (e, path_str.clone()))?;

                Ok(())
            });
        }

        let mut failed = vec![];

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(_)) => continue,
                Ok(Err((_, p))) => {
                    failed.push(p);
                }
                Err(e) => return Err(FsError::Copy(format!("Spawn job panicked: {e}"))),
            }
        }

        if !failed.is_empty() {
            return Err(FsError::Copy(format!(
                "Failed to copy the following paths: {}",
                failed.join(", ")
            )));
        }

        Ok(())
    }

    async fn copy_files(&self, dest: &Arc<dyn Fs>, paths: Vec<&str>) -> Result<(), FsError> {
        let mut join_set = JoinSet::<Result<(), (FsError, String)>>::new();
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_COPIES));

        for p in paths {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| FsError::Copy(format!("Failed to acquire lock: {e}")))?;
            let fs_dest = dest.clone();
            let fs_src = self.clone();
            let p = p.to_string();
            join_set.spawn(async move {
                let _permit = permit;
                let content = fs_src.read(&p).await.map_err(|e| (e, p.clone()))?;
                fs_dest.write(&p, content).await.map_err(|e| (e, p))?;

                Ok(())
            });
        }

        let mut failed = vec![];

        while let Some(res) = join_set.join_next().await {
            match res {
                Ok(Ok(_)) => continue,
                Ok(Err((_, p))) => {
                    failed.push(p);
                }
                Err(e) => return Err(FsError::Copy(format!("Spawn job panicked: {e}"))),
            }
        }

        if !failed.is_empty() {
            return Err(FsError::Copy(format!(
                "Failed to copy the following paths: {}",
                failed.join(", ")
            )));
        }

        Ok(())
    }
}

pub fn local_disk_walker(path: &PathBuf, parent: &PathBuf) -> Result<EntryIter, FsError> {
    let walker = ignore::WalkBuilder::new(path)
        .add_custom_ignore_filename(".verignore")
        .hidden(false)
        .filter_entry(|d| {
            if d.path().is_file() {
                if d.path()
                    .ancestors()
                    .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
                {
                    return false;
                }
                true
            } else {
                if d.path().ends_with(".ver")
                    || d.path().ends_with(".ver/")
                    || d.path()
                        .ancestors()
                        .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
                {
                    return false;
                }
                true
            }
        })
        .build();

    let entries: Vec<ignore::DirEntry> = walker
        .filter(|p| {
            if p.is_ok() {
                return p.clone().unwrap().path().is_file();
            }
            true
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| FsError::List(format!("Error while collecting entries from disk: {e}")))?;

    let new_entries: Vec<RelativeDirEntry> = entries
        .into_iter()
        .map(|e| to_relative_path(e, parent))
        .collect();

    let disk_it = DiskListIterator::new(new_entries.into_iter());

    Ok(disk_it.into_entry_iter())
}

pub fn noignore_local_disk_walker(path: &PathBuf, parent: &PathBuf) -> Result<EntryIter, FsError> {
    let walker = ignore::WalkBuilder::new(path).hidden(false).build();

    let entries: Vec<ignore::DirEntry> = walker
        .filter(|p| {
            if p.is_ok() {
                return p.clone().unwrap().path().is_file();
            }
            true
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| FsError::List(format!("Error while collecting entries from disk: {e}")))?;

    let new_entries: Vec<RelativeDirEntry> = entries
        .into_iter()
        .map(|e| to_relative_path(e, parent))
        .collect();

    let disk_it = DiskListIterator::new(new_entries.into_iter());

    Ok(disk_it.into_entry_iter())
}
