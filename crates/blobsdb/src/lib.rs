use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    io::Write,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex, atomic::AtomicUsize},
    time::Duration,
};

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use fastcdc::v2020::*;
use object_store::{BackoffConfig, RetryConfig, memory::InMemory};
pub use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path as ObjPath,
};
use serde::{Deserialize, Serialize};
use tokio::{
    fs,
    io::{self, AsyncRead, AsyncWriteExt, BufReader},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_stream::StreamExt;

pub const MIN_CHUNK_SIZE: usize = 64 * 1024; // 64 KB
pub const AVG_CHUNK_SIZE: usize = 256 * 1024; // 256 KB
pub const MAX_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
pub const MAX_CONCURRENT_OPS: usize = 32;
pub const DUMP_EVERY_N_CHUNKS: usize = 1000;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct Chunk {
    pub offset: u64,
    pub length: usize,
    pub hash: [u8; 32],
}

impl Hash for Chunk {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

pub enum GetObjectStoreInput {
    /// only for testing
    InMemory,
    Local,
    S3 {
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        bucket: String,
    },
    Gcs {
        service_account_credentials_path: String,
        bucket: String,
    },
    Azure {
        account_name: Option<String>,
        account_key: Option<String>,
        endpoint: Option<String>,
        container_name: String,
    },
}

pub fn get_object_store(input: GetObjectStoreInput) -> Result<Arc<dyn ObjectStore>, String> {
    let retry_config = RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            base: 2_f64,
        },
        max_retries: 3,
        retry_timeout: Duration::from_secs(30),
    };

    match input {
        GetObjectStoreInput::InMemory => {
            let store = InMemory::new();
            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Local => {
            let store = LocalFileSystem::new();
            Ok(Arc::new(store))
        }
        GetObjectStoreInput::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            bucket,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if access_key_id.is_none()
                && secret_access_key.is_none()
                && region.is_none()
                && endpoint.is_none()
            {
                let store = AmazonS3Builder::from_env()
                    .with_retry(retry_config)
                    .with_bucket_name(bucket)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| format!("Error while building S3 storage from env: {e}"))?;
                return Ok(Arc::new(store));
            }

            let builder = AmazonS3Builder::new()
                .with_retry(retry_config)
                .with_access_key_id(access_key_id.unwrap_or_default())
                .with_bucket_name(bucket)
                .with_endpoint(endpoint.unwrap_or_default())
                .with_region(region.unwrap_or_default())
                .with_secret_access_key(secret_access_key.unwrap_or_default())
                .with_client_options(client_options);
            let store = builder
                .build()
                .map_err(|e| format!("Error while building S3 storage: {e}"))?;

            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Gcs {
            service_account_credentials_path,
            bucket,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            let store = GoogleCloudStorageBuilder::new()
                .with_retry(retry_config)
                .with_service_account_path(service_account_credentials_path)
                .with_bucket_name(bucket)
                .with_client_options(client_options)
                .build()
                .map_err(|e| format!("Error while buiding GCP storage: {e}"))?;

            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Azure {
            account_name,
            account_key,
            endpoint,
            container_name,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if account_key.is_none() && account_name.is_none() && endpoint.is_none() {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_retry(retry_config)
                    .with_container_name(container_name)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| {
                        format!("An error occurred while building Azure storage from env: {e}")
                    })?;

                return Ok(Arc::new(store));
            }

            let store = MicrosoftAzureBuilder::new()
                .with_retry(retry_config)
                .with_access_key(account_key.unwrap_or_default())
                .with_account(account_name.unwrap_or_default())
                .with_endpoint(endpoint.unwrap_or_default())
                .with_container_name(container_name)
                .with_client_options(client_options)
                .build()
                .map_err(|e| format!("An error occurred while building Azure storage: {e}"))?;

            Ok(Arc::new(store))
        }
    }
}

pub struct BlobsDB {
    pub directory: ObjPath,
    pub storage: Arc<dyn ObjectStore>,
    pub synced_map_path: PathBuf,
}

async fn read_chunk(
    storage: Arc<dyn ObjectStore>,
    path: &ObjPath,
    offset: u64,
) -> Result<(Vec<u8>, u64), String> {
    // Stream the response directly into decompressor
    let stream = storage
        .get(path)
        .await
        .map_err(|e| format!("Error while fetching the chunk content: {e}"))?
        .into_stream();

    let reader = tokio_util::io::StreamReader::new(stream);
    let mut decoder = ZstdDecoder::new(BufReader::new(reader));
    let mut content = Vec::with_capacity(MAX_CHUNK_SIZE);

    io::copy(&mut decoder, &mut content)
        .await
        .map_err(|e| format!("Error while decoding chunk: {e}"))?;

    Ok((content, offset))
}

async fn write_chunk<R: AsyncRead + Unpin>(
    storage: Arc<dyn ObjectStore>,
    mut source: R,
    destination: &ObjPath,
) -> Result<(), String> {
    // Use fastest compression level for better write performance
    let mut enc = ZstdEncoder::with_quality(Vec::new(), async_compression::Level::Fastest);
    io::copy(&mut source, &mut enc)
        .await
        .map_err(|e| format!("Error while encoding chunk content: {e}"))?;
    enc.shutdown()
        .await
        .map_err(|e| format!("Error while writing the encoded content: {e}"))?;
    let compressed: Vec<u8> = enc.into_inner();
    storage
        .put(destination, PutPayload::from(compressed))
        .await
        .map_err(|e| format!("Error while storing encoded content: {e}"))?;

    Ok(())
}

pub async fn get_remote_head(
    remote_storage: &Arc<dyn ObjectStore>,
) -> Result<Option<[u8; 32]>, String> {
    let result = remote_storage
        .get(
            &ObjPath::from_url_path("head")
                .map_err(|e| format!("Could not convert head path to object storage path: {e}"))?,
        )
        .await;

    let fetched = match result {
        Ok(f) => f
            .bytes()
            .await
            .map_err(|e| format!("Error while fetching head bytes: {e}"))?
            .to_vec(),
        Err(e) => match e {
            object_store::Error::NotFound { path: _, source: _ } => return Ok(None),
            _ => return Err(format!("Error while fetching head: {e}")),
        },
    };

    if fetched.len() != 32 {
        return Err(format!(
            "Corrupted head: expecting head to be 32-bytes long, got {}",
            fetched.len()
        ));
    }

    let hash =
        blake3::Hash::from_slice(&fetched).map_err(|e| format!("Invalid blake3 hash: {e}"))?;

    Ok(Some(hash.as_bytes().to_owned()))
}

pub async fn get_remote_file(
    path: &str,
    remote_storage: &Arc<dyn ObjectStore>,
) -> Result<Option<Vec<u8>>, String> {
    let result = remote_storage
        .get(&ObjPath::from_url_path(path).map_err(|e| {
            format!("Could not convert files.redb path to object storage path: {e}")
        })?)
        .await;

    let fetched = match result {
        Ok(f) => f
            .bytes()
            .await
            .map_err(|e| format!("Error while fetching head bytes: {e}"))?
            .to_vec(),
        Err(e) => match e {
            object_store::Error::NotFound { path: _, source: _ } => return Ok(None),
            _ => return Err(format!("Error while fetching head: {e}")),
        },
    };

    Ok(Some(fetched))
}

pub async fn download_remote_repository<P: AsRef<Path>, E: Write>(
    remote_storage: &Arc<dyn ObjectStore>,
    out_dir: P,
    std_err: &mut E,
) -> Result<usize, String> {
    let head_content = get_remote_head(remote_storage).await?;

    if head_content.is_none() {
        return Err("Remote repository does not have a head".into());
    }

    let redb_content = get_remote_file("files.redb", remote_storage).await?;

    if redb_content.is_none() {
        return Err("Remote repository does not have a files.redb".into());
    }

    let history = get_remote_file("snapshot_history", remote_storage).await?;

    let atomic_write = async |path: PathBuf, content: &[u8]| -> Result<(), String> {
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, content)
            .await
            .map_err(|e| format!("Error while writing tmp file: {e}"))?;
        fs::rename(&tmp, &path)
            .await
            .map_err(|e| format!("Error while renaming tmp file: {e}"))?;
        Ok(())
    };

    let mut stream_list = remote_storage.list(Some(
        &ObjPath::from_url_path("blobs").map_err(|e| format!("Illegal path: {e}"))?,
    ));

    let mut join_set: JoinSet<Result<(), String>> = JoinSet::new();
    let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
    let count = Arc::new(AtomicUsize::new(0));

    while let Some(s) = stream_list.next().await {
        let s = s.map_err(|e| format!("Error while listing chunks: {e}"))?;

        let storage = remote_storage.clone();
        let path = s.location;
        let filename = path
            .filename()
            .ok_or_else(|| "Path without a file name".to_string())?
            .to_string();
        let dest = out_dir.as_ref().join("blobs").join(filename);

        // skip fetching already-fetched
        // chunks
        if dest.try_exists().is_ok_and(|t| t) {
            continue;
        }

        let permit = sem
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| format!("Error while acquiring semaphore permit: {e}"))?;

        let c = count.clone();

        join_set.spawn(async move {
            let _permit = permit;
            let mut result = storage
                .get(&path)
                .await
                .map_err(|e| format!("Error while getting chunk {path}: {e}"))?
                .into_stream();

            let tmp = dest.with_extension("part");
            let mut file = fs::File::create(&tmp)
                .await
                .map_err(|e| format!("Could not create {}: {e}", tmp.display()))?;

            while let Some(next) = result.next().await {
                let chunk =
                    next.map_err(|e| format!("Error while streaming chunk content: {e}"))?;
                file.write_all(&chunk)
                    .await
                    .map_err(|e| format!("Error while writing streamed chunk to file: {e}"))?;
            }

            file.flush()
                .await
                .map_err(|e| format!("Flush failed for {}: {e}", tmp.display()))?;
            fs::rename(&tmp, &dest)
                .await
                .map_err(|e| format!("Rename failed for {}: {e}", dest.display()))?;

            c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

            Ok(())
        });
    }

    let mut failed = 0;

    while let Some(res) = join_set.join_next().await {
        let task_res = res.map_err(|e| format!("Task panicked while downloading: {e}"))?;
        match task_res {
            Ok(()) => {}
            Err(e) => {
                failed += 1;
                let _ = writeln!(std_err, "{}", e);
            }
        }
    }

    if failed > 0 {
        return Err(format!("Failed to download {} chunks", failed));
    }

    atomic_write(
        out_dir.as_ref().join("head"),
        blake3::Hash::from_bytes(head_content.unwrap())
            .to_string()
            .as_bytes(),
    )
    .await?;

    atomic_write(
        out_dir.as_ref().join("files.redb"),
        redb_content.unwrap().as_slice(),
    )
    .await?;

    if let Some(snap) = history {
        atomic_write(out_dir.as_ref().join("snapshot_history"), snap.as_slice()).await?;
    }

    Ok(count.load(std::sync::atomic::Ordering::Relaxed))
}

impl BlobsDB {
    pub fn new<O: ObjectStore, P: AsRef<Path>>(
        directory: P,
        synced_map_path: P,
        storage: O,
    ) -> Result<Self, String> {
        Ok(Self {
            directory: ObjPath::from_filesystem_path(directory)
                .map_err(|e| format!("Error while creating base object storage path: {e}"))?,
            storage: Arc::new(storage),
            synced_map_path: synced_map_path.as_ref().to_owned(),
        })
    }

    async fn load_synced_map(&self) -> Result<HashMap<String, HashMap<[u8; 32], bool>>, String> {
        let res = fs::read(self.synced_map_path.as_path()).await;
        match res {
            Ok(content) => {
                let map: HashMap<String, HashMap<[u8; 32], bool>> = postcard::from_bytes(&content)
                    .map_err(|e| format!("Error while deserializing sync map: {}", e))?;
                Ok(map)
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => Ok(HashMap::new()),
                _ => Err(format!(
                    "An error occurred while reading the synced map file: {}",
                    e
                )),
            },
        }
    }

    async fn dump_synced_map(
        &self,
        map: &HashMap<String, HashMap<[u8; 32], bool>>,
    ) -> Result<(), String> {
        let dumped = postcard::to_allocvec(map)
            .map_err(|e| format!("Error while serializing sync map: {}", e))?;
        let tmp = self.synced_map_path.with_extension("tmp");
        fs::write(&tmp, &dumped)
            .await
            .map_err(|e| format!("Error while writing to tmp file: {e}"))?;
        fs::rename(&tmp, &self.synced_map_path)
            .await
            .map_err(|e| format!("Error while renaming tmp file: {e}"))?;
        Ok(())
    }

    /// syncs the sync map after a repository
    /// download
    pub async fn create_sync_map_after_download(&self, remote_name: String) -> Result<(), String> {
        let mut stream = self.storage.list(Some(&self.directory));
        let mut values: HashMap<[u8; 32], bool> = HashMap::new();

        while let Some(p) = stream.next().await {
            let chunk_path = p.map_err(|e| format!("Error while streaming chunks: {e}"))?;
            let chunk_hex = chunk_path
                .location
                .filename()
                .ok_or_else(|| "Expected file to contain chunk hex".to_string())?
                .to_string();
            let maybe_hash = blake3::Hash::from_str(&chunk_hex);

            if let Ok(hash) = maybe_hash {
                values.insert(hash.as_bytes().to_owned(), true);
            }
        }

        let sync_map = HashMap::from([(remote_name, values)]);

        self.dump_synced_map(&sync_map).await?;

        Ok(())
    }

    pub async fn put<P: AsRef<Path>>(&self, file_path: P) -> Result<Vec<Chunk>, String> {
        let fl = fs::File::open(file_path.as_ref())
            .await
            .map_err(|e| format!("Error while opening file: {e}"))?;

        let mut chunks: Vec<Chunk> = vec![];

        let mut chunker = AsyncStreamCDC::new(fl, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        let mut stream = Box::pin(chunker.as_stream());
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
        let mut join_set = JoinSet::new();

        while let Some(result) = stream.next().await {
            let entry = result.map_err(|e| format!("Error while obtaining a file chunk: {e}"))?;
            let data = entry.data;
            let offset = entry.offset;
            let length = entry.length;

            // Clone data for hash computation to overlap with I/O
            let data_for_hash = data.clone();
            let hash_task = tokio::task::spawn_blocking(move || blake3::hash(&data_for_hash));

            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the semaphore: {e}"))?;
            let storage = self.storage.clone();
            let directory = self.directory.clone();

            join_set.spawn(async move {
                let _permit = permit;
                let hash = hash_task
                    .await
                    .map_err(|e| format!("Error while computing hash: {e}"))?;
                let p = directory.join(hash.to_string());
                write_chunk(storage, data.as_slice(), &p).await?;
                Ok::<_, String>((hash, length, offset))
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((hash, length, offset))) => {
                    chunks.push(Chunk {
                        hash: hash.as_bytes().to_owned(),
                        length,
                        offset,
                    });
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(format!("Panicked while writing chunks: {e}")),
            }
        }

        // Sort by offset to maintain order
        chunks.sort_by_key(|c| c.offset);
        Ok(chunks)
    }

    pub async fn get(&self, chunks: Arc<Vec<Chunk>>) -> Result<Vec<u8>, String> {
        let capacity = chunks.iter().fold(0, |acc, x| acc + x.length);
        let mut v = vec![0; capacity];
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
        let mut join_set = JoinSet::new();
        for c in chunks.iter() {
            let f = self
                .directory
                .clone()
                .join(blake3::Hash::from_bytes(c.hash).to_string());
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the semaphore: {e}"))?;
            let storage = self.storage.clone();
            let offset = c.offset;
            join_set.spawn(async move {
                let _permit = permit;
                read_chunk(storage, &f, offset).await
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok((content, offset))) => {
                    v[(offset as usize)..(offset as usize) + content.len()]
                        .copy_from_slice(&content);
                }
                Ok(Err(e)) => return Err(e.to_string()),
                Err(e) => return Err(format!("Panicked while fetching and reading a chunk: {e}")), // JoinError
            }
        }

        Ok(v)
    }

    pub async fn sync<E: Write>(
        &self,
        remote_name: String,
        remote_storage: Arc<dyn ObjectStore>,
        chunks: HashSet<Chunk>,
        current_head: [u8; 32],
        redb_content: Vec<u8>,
        history_content: Vec<u8>,
        std_err: &mut E,
    ) -> Result<(), String> {
        let sync_map = Arc::new(Mutex::new(self.load_synced_map().await?));
        let sem = Arc::new(Semaphore::new(MAX_CONCURRENT_OPS));
        let mut set: JoinSet<Result<(), String>> = JoinSet::new();

        let mut pending = 0_usize;

        for c in chunks.iter() {
            // skip already synced files
            if let Some(t) = sync_map
                .lock()
                .map_err(|e| format!("Sync map mutex lock is poisoned: {e}"))?
                .get(&remote_name)
                && let Some(b) = t.get(&c.hash)
                && *b
            {
                continue;
            }

            pending += 1;

            if pending.is_multiple_of(DUMP_EVERY_N_CHUNKS) {
                let intermediate: HashMap<String, HashMap<[u8; 32], bool>> = sync_map
                    .lock()
                    .map_err(|e| format!("Sync map mutex lock is poisoned: {e}"))?
                    .clone();
                // ignore error.
                // the map will be dumped at some
                // point in the future, no need
                // to break things for an intermediate
                // failure
                let _ = self.dump_synced_map(&intermediate).await;
            }

            let hash = c.hash;
            let hash_str = blake3::Hash::from_bytes(hash).to_string();
            let src_path = self.directory.clone().join(hash_str.as_str());
            let dst_path = ObjPath::from_url_path(format!("blobs/{}", hash_str))
                .map_err(|e| format!("Illegal path: {e}"))?;

            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the read semaphore: {e}"))?;

            let storage = self.storage.clone();
            let remote = remote_storage.clone();
            let sync_map_c = sync_map.clone();
            let rem_name = remote_name.clone();

            set.spawn(async move {
                let _permit = permit;

                let bytes = storage
                    .get(&src_path)
                    .await
                    .map_err(|e| format!("An error occurred while reading chunk: {e}"))?
                    .bytes()
                    .await
                    .map_err(|e| format!("An error occurred while getting bytes for chunk: {e}"))?;

                remote
                    .put(&dst_path, PutPayload::from(bytes))
                    .await
                    .map_err(|e| format!("An error occurred while uploading the chunk: {e}"))?;

                sync_map_c
                    .lock()
                    .map_err(|e| format!("Sync map mutex lock is poisoned: {e}"))?
                    .entry(rem_name)
                    .and_modify(|h| {
                        _ = h.insert(hash, true);
                    })
                    .or_insert(HashMap::from([(hash, true)]));

                Ok(())
            });
        }

        let mut failed = 0;

        while let Some(res) = set.join_next().await {
            let task_res =
                res.map_err(|e| format!("Task panicked while copying the chunk: {e}"))?;
            if let Some(e) = task_res.err() {
                // discard the result, this is just informative
                // for the user, it shouldn't cause the function
                // to return an error
                let _ = writeln!(std_err, "{}", e);
                failed += 1;
            }
        }

        if failed > 0 {
            return Err(format!(
                "{} chunks failed to upload to remote storage. Please make sure everything is working and retry.",
                failed
            ));
        }

        remote_storage
            .put(
                &ObjPath::from_url_path("files.redb").map_err(|e| format!("Illegal path: {e}"))?,
                PutPayload::from(redb_content),
            )
            .await
            .map_err(|e| {
                format!(
                    "An error occurred while putting the redb database in remote storage: {}",
                    e
                )
            })?;

        remote_storage
            .put(
                &ObjPath::from_url_path("snapshot_history")
                    .map_err(|e| format!("Illegal path: {e}"))?,
                PutPayload::from(history_content),
            )
            .await
            .map_err(|e| {
                format!(
                    "An error occurred while putting the head in remote storage: {}",
                    e
                )
            })?;

        remote_storage
            .put(
                &ObjPath::from_url_path("head").map_err(|e| format!("Illegal path: {e}"))?,
                PutPayload::from(current_head.to_vec()),
            )
            .await
            .map_err(|e| {
                format!(
                    "An error occurred while putting the head in remote storage: {}",
                    e
                )
            })?;

        let owned: HashMap<String, HashMap<[u8; 32], bool>> = sync_map
            .lock()
            .map_err(|e| format!("Sync map mutex lock is poisoned: {e}"))?
            .clone();

        self.dump_synced_map(&owned).await?;

        Ok(())
    }
}
