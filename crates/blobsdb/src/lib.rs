use std::{path::Path, sync::Arc};

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use fastcdc::v2020::*;
use object_store::{
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

pub const MIN_CHUNK_SIZE: usize = 4096;
pub const AVG_CHUNK_SIZE: usize = 8192;
pub const MAX_CHUNK_SIZE: usize = 16_384;
pub const MAX_CONCURRENT_READS: usize = 256;
pub const MAX_CONCURRENT_WRITES: usize = 16;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, Eq, PartialEq)]
pub struct Chunk {
    pub offset: u64,
    pub length: usize,
    pub hash: [u8; 32],
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
            if access_key_id.is_none()
                && secret_access_key.is_none()
                && region.is_none()
                && endpoint.is_none()
            {
                let store = AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| format!("Error while building S3 storage from env: {e}"))?;
                return Ok(Arc::new(store));
            }

            let builder = AmazonS3Builder::new()
                .with_access_key_id(access_key_id.unwrap_or_default())
                .with_bucket_name(bucket)
                .with_endpoint(endpoint.unwrap_or_default())
                .with_region(region.unwrap_or_default())
                .with_secret_access_key(secret_access_key.unwrap_or_default());
            let store = builder
                .build()
                .map_err(|e| format!("Error while building S3 storage: {e}"))?;

            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Gcp {
            service_account_credentials_path,
            bucket,
        } => {
            if service_account_credentials_path.is_none() {
                let store = GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket)
                    .build()
                    .map_err(|e| format!("Error while building GCP storage from env: {e}"))?;

                return Ok(Arc::new(store));
            }

            let store = GoogleCloudStorageBuilder::new()
                .with_service_account_path(service_account_credentials_path.unwrap_or_default())
                .with_bucket_name(bucket)
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
            if account_key.is_none() && account_name.is_none() && endpoint.is_none() {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_container_name(container_name)
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
    let content: Vec<u8> = storage
        .get(path)
        .await
        .map_err(|e| format!("Error while fetching the chunk content: {e}"))?
        .bytes()
        .await
        .map_err(|e| format!("Error while reading the bytes of the fetched content: {e}"))?
        .to_vec();
    let mut reader = ZstdDecoder::new(BufReader::new(content.as_slice()));
    let mut c = vec![];
    io::copy(&mut reader, &mut c)
        .await
        .map_err(|e| format!("Error while decoding chunk: {e}"))?;
    Ok((c, offset))
}

async fn write_chunk<R: AsyncRead + Unpin>(
    storage: Arc<dyn ObjectStore>,
    mut source: R,
    destination: &ObjPath,
) -> Result<(), String> {
    if storage.head(destination).await.is_ok() {
        return Ok(());
    }
    let mut enc = ZstdEncoder::new(Vec::new());
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
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_READS));
        let mut join_set = JoinSet::new();

        while let Some(result) = stream.next().await {
            let entry = result.map_err(|e| format!("Error while obtaining a file chunk: {e}"))?;
            let data = entry.data;
            let hash = blake3::hash(&data);
            let p = self.directory.to_owned().join(hash.to_string());
            let permit = semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|e| format!("Error while acquiring the semaphore: {e}"))?;
            let storage = self.storage.clone();
            join_set.spawn(async move {
                let _permit = permit;
                write_chunk(storage, data.as_slice(), &p).await
            });
            chunks.push(Chunk {
                hash: hash.as_bytes().to_owned(),
                length: entry.length,
                offset: entry.offset,
            });
        }

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(())) => {
                    continue;
                }
                Ok(Err(e)) => return Err(e.to_string()),
                Err(e) => return Err(format!("Panicked while writing chunks: {e}")),
            }
        }

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
}
