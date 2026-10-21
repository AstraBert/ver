use std::{hash::Hash, sync::Arc, time::Duration};

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use fastcdc::v2020::*;
use object_store::{BackoffConfig, RetryConfig, memory::InMemory};
pub use object_store::{
    ObjectStore, ObjectStoreExt, PutPayload, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, path::Path as ObjPath,
};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{self, AsyncRead, AsyncWriteExt, BufReader},
    sync::Semaphore,
    task::JoinSet,
};
use tokio_stream::StreamExt;
use ver_fs::Fs;

pub const MIN_CHUNK_SIZE: usize = 64 * 1024; // 64 KB
pub const AVG_CHUNK_SIZE: usize = 256 * 1024; // 256 KB
pub const MAX_CHUNK_SIZE: usize = 1024 * 1024; // 1 MB
pub const MAX_CONCURRENT_OPS: usize = 32;
pub const DUMP_EVERY_N_CHUNKS: usize = 1000;
pub const BLOBSDB_PREFIX: &str = ".ver/blobsdb";

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

async fn write_chunk_with_fs<R: AsyncRead + Unpin>(
    fs: &Arc<dyn Fs>,
    mut source: R,
    destination: &str,
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
    fs.write(&destination, compressed.into())
        .await
        .map_err(|e| format!("Error while storing encoded content: {e}"))?;

    Ok(())
}

async fn read_chunk_with_fs(
    fs: &Arc<dyn Fs>,
    path: &str,
    offset: u64,
) -> Result<(Vec<u8>, u64), String> {
    // Stream the response directly into decompressor
    let bts = fs
        .read(path)
        .await
        .map_err(|e| format!("Error while fetching the chunk content: {e}"))?;

    let mut decoder = ZstdDecoder::new(BufReader::new(bts.iter().as_slice()));
    let mut content = Vec::with_capacity(MAX_CHUNK_SIZE);

    io::copy(&mut decoder, &mut content)
        .await
        .map_err(|e| format!("Error while decoding chunk: {e}"))?;

    Ok((content, offset))
}

#[derive(Clone)]
pub struct BlobsDBWithFs {
    pub fs: Arc<dyn Fs>,
}

impl BlobsDBWithFs {
    pub fn new(fs: Arc<dyn Fs>) -> Self {
        Self { fs }
    }

    pub async fn put<D: AsRef<[u8]>>(&self, content: D) -> Result<Vec<Chunk>, String> {
        let mut chunks: Vec<Chunk> = vec![];

        let mut chunker = AsyncStreamCDC::new(
            content.as_ref(),
            MIN_CHUNK_SIZE,
            AVG_CHUNK_SIZE,
            MAX_CHUNK_SIZE,
        );
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

            let fs = self.fs.clone();

            join_set.spawn(async move {
                let _permit = permit;
                let hash = hash_task
                    .await
                    .map_err(|e| format!("Error while computing hash: {e}"))?;
                let p = format!("{}/{}", BLOBSDB_PREFIX, hash);
                write_chunk_with_fs(&fs, data.as_slice(), &p).await?;
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
            let f = format!("{}/{}", BLOBSDB_PREFIX, blake3::Hash::from_bytes(c.hash));
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the semaphore: {e}"))?;
            let fs = self.fs.clone();
            let offset = c.offset;
            join_set.spawn(async move {
                let _permit = permit;
                read_chunk_with_fs(&fs, &f, offset).await
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
}
