use std::{hash::Hash, sync::Arc};

use async_compression::tokio::{bufread::ZstdDecoder, write::ZstdEncoder};
use fastcdc::v2020::*;
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

    pub async fn put(&self, path: &str) -> Result<Vec<Chunk>, String> {
        let stream = self.fs.stream_read(path).await.map_err(|e| e.to_string())?;
        let mut chunks: Vec<Chunk> = vec![];

        let mut chunker =
            AsyncStreamCDC::new(stream, MIN_CHUNK_SIZE, AVG_CHUNK_SIZE, MAX_CHUNK_SIZE);
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

    pub async fn copy(&self, dest: &BlobsDBWithFs) -> Result<(), String> {
        self.fs
            .copy_dir(dest.fs.clone(), Some(BLOBSDB_PREFIX), false)
            .await
            .map_err(|e| e.to_string())
    }
}
