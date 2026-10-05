use std::{collections::HashSet, hash::Hash, path::Path, sync::Arc};

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use fastcdc::v2020::*;
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
pub const MAX_CONCURRENT_READS: usize = 256;
pub const MAX_CONCURRENT_WRITES: usize = 16;

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
    Local,
    S3 {
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        bucket: String,
    },
    Gcp {
        service_account_credentials_path: Option<String>,
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
    match input {
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
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if access_key_id.is_none()
                && secret_access_key.is_none()
                && region.is_none()
                && endpoint.is_none()
            {
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| format!("Error while building S3 storage from env: {e}"))?;
                return Ok(Arc::new(store));
            }

            let builder = AmazonS3Builder::new()
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
        GetObjectStoreInput::Gcp {
            service_account_credentials_path,
            bucket,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if service_account_credentials_path.is_none() {
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| format!("Error while building GCP storage from env: {e}"))?;

                return Ok(Arc::new(store));
            }

            let store = GoogleCloudStorageBuilder::new()
                .with_service_account_path(service_account_credentials_path.unwrap_or_default())
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
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if account_key.is_none() && account_name.is_none() && endpoint.is_none() {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_container_name(container_name)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| {
                        format!("An error occurred while building Azure storage from env: {e}")
                    })?;

                return Ok(Arc::new(store));
            }

            let store = MicrosoftAzureBuilder::new()
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
    head_path: &Path,
) -> Result<Option<[u8; 32]>, String> {
    let result = remote_storage
        .get(
            &ObjPath::from_filesystem_path(head_path)
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

    let hash =
        blake3::Hash::from_slice(&fetched).map_err(|e| format!("Invalid blake3 hash: {e}"))?;

    Ok(Some(hash.as_bytes().to_owned()))
}

impl BlobsDB {
    pub fn new<O: ObjectStore, P: AsRef<Path>>(directory: P, storage: O) -> Result<Self, String> {
        Ok(Self {
            directory: ObjPath::from_filesystem_path(directory)
                .map_err(|e| format!("Error while creating base object storage path: {e}"))?,
            storage: Arc::new(storage),
        })
    }

    pub async fn put<P: AsRef<Path>>(&self, file_path: P) -> Result<Vec<Chunk>, String> {
        let fl = fs::File::open(file_path.as_ref())
            .await
            .map_err(|e| format!("Error while opening file: {e}"))?;

        let mut chunks: Vec<Chunk> = vec![];

        let mut chunker = AsyncStreamCDC::new(fl, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
        let mut stream = Box::pin(chunker.as_stream());
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_WRITES));
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
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_READS));
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

    pub async fn sync(
        &self,
        remote_storage: Arc<dyn ObjectStore>,
        chunks: HashSet<Chunk>,
        current_head: [u8; 32],
    ) -> Result<(), String> {
        let head_path = self.directory.clone().join("head");
        remote_storage
            .put(&head_path, PutPayload::from(current_head.to_vec()))
            .await
            .map_err(|e| format!("Error while writing head to remote storage: {e}"))?;

        let mut join_set_read = JoinSet::new();
        let semaphore_read = Arc::new(Semaphore::new(MAX_CONCURRENT_READS));

        for c in chunks.iter() {
            let storage = remote_storage.clone();
            let f = self
                .directory
                .clone()
                .join(blake3::Hash::from_bytes(c.hash).to_string());
            let permit = semaphore_read
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the semaphore: {e}"))?;

            let offset = c.offset;

            join_set_read.spawn(async move {
                let _permit = permit;
                let result = read_chunk(storage, &f, offset).await;
                match result {
                    Ok((r, _)) => Ok((r, f)),
                    Err(e) => Err(e),
                }
            });
        }

        let mut write_join_set = JoinSet::new();
        let write_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_WRITES));

        while let Some(res) = join_set_read.join_next().await {
            match res {
                Ok(Ok((content, path))) => {
                    let write_permit =
                        write_semaphore.clone().acquire_owned().await.map_err(|e| {
                            format!(
                                "An error occurred while acquiring the write semaphore: {}",
                                e
                            )
                        })?;
                    let storage = remote_storage.clone();

                    write_join_set.spawn(async move {
                        let _permit = write_permit;

                        storage
                            .put(&path, PutPayload::from(content))
                            .await
                            .map_err(|e| {
                                format!("An error occurred while uploading the chunk: {}", e)
                            })
                    });
                }
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(format!("Task panicked while reading the chunk: {}", e)),
            }
        }

        while let Some(wr) = write_join_set.join_next().await {
            match wr {
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(format!("Task panicked while uploading the chunk: {}", e)),
            }
        }

        Ok(())
    }
}
