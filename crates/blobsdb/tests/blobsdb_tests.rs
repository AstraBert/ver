use std::sync::Arc;

use blobsdb::{BlobsDBWithFs, Chunk, MAX_CHUNK_SIZE};
use object_store::memory::InMemory;
use tempdir::TempDir;
use ver_fs::{DiskFs, Fs, ObjectStoreFs};

fn disk_blobsdb() -> (TempDir, BlobsDBWithFs) {
    let tmp = TempDir::new("blobsdb_test").unwrap();
    let fs: Arc<dyn Fs> = Arc::new(DiskFs::new(tmp.path().to_path_buf()));
    (tmp, BlobsDBWithFs::new(fs))
}

fn mem_blobsdb() -> BlobsDBWithFs {
    let fs: Arc<dyn Fs> = Arc::new(ObjectStoreFs::new(Arc::new(InMemory::new())));
    BlobsDBWithFs::new(fs)
}

fn write_file(tmp: &TempDir, name: &str, content: &[u8]) {
    std::fs::write(tmp.path().join(name), content).unwrap();
}

// ---------- put/get roundtrip ----------

#[tokio::test]
async fn put_get_small_file_roundtrip() {
    let (tmp, db) = disk_blobsdb();
    let content = b"hello blobsdb";
    write_file(&tmp, "a.txt", content);

    let chunks = db.put("a.txt").await.unwrap();
    assert!(!chunks.is_empty());

    let got = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(got, content);
}

#[tokio::test]
async fn put_get_empty_file() {
    let (tmp, db) = disk_blobsdb();
    write_file(&tmp, "empty.txt", b"");

    let chunks = db.put("empty.txt").await.unwrap();
    assert!(chunks.is_empty());

    let got = db.get(Arc::new(chunks)).await.unwrap();
    assert!(got.is_empty());
}

#[tokio::test]
async fn put_get_large_file_multiple_chunks() {
    let (tmp, db) = disk_blobsdb();
    let content: Vec<u8> = (0..3_000_000).map(|i| (i % 256) as u8).collect();
    write_file(&tmp, "large.bin", &content);

    let chunks = db.put("large.bin").await.unwrap();
    assert!(chunks.len() > 1, "large file should be split into chunks");

    let got = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(got, content);
}

#[tokio::test]
async fn put_get_binary_content() {
    let (tmp, db) = disk_blobsdb();
    let content: Vec<u8> = (0..=255).cycle().take(10_000).collect();
    write_file(&tmp, "bin.dat", &content);

    let chunks = db.put("bin.dat").await.unwrap();
    let got = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(got, content);
}

#[tokio::test]
async fn put_get_unicode_content() {
    let (tmp, db) = disk_blobsdb();
    let content = "Hello, 世界! 🦀🚀".as_bytes();
    write_file(&tmp, "uni.txt", content);

    let chunks = db.put("uni.txt").await.unwrap();
    let got = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(got, content);
}

#[tokio::test]
async fn put_get_with_object_store_backend() {
    let tmp = TempDir::new("blobsdb_test").unwrap();
    let content = b"object store backed";
    write_file(&tmp, "os.txt", content);

    // source file lives on disk, blobs land in the in-memory store
    let disk: Arc<dyn Fs> = Arc::new(DiskFs::new(tmp.path().to_path_buf()));
    let mem = mem_blobsdb();
    let db = BlobsDBWithFs { fs: disk };
    let chunks = db.put("os.txt").await.unwrap();

    let mut mem_chunks = Vec::new();
    for c in &chunks {
        let path = format!(
            "{}/{}",
            blobsdb::BLOBSDB_PREFIX,
            blake3::Hash::from_bytes(c.hash)
        );
        let data = db.fs.read(&path).await.unwrap();
        mem.fs.write(&path, data).await.unwrap();
        mem_chunks.push(*c);
    }

    let got = mem.get(Arc::new(mem_chunks)).await.unwrap();
    assert_eq!(got, content);
}

// ---------- chunk properties ----------

#[tokio::test]
async fn chunks_are_contiguous_and_cover_file() {
    let (tmp, db) = disk_blobsdb();
    let content: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();
    write_file(&tmp, "cov.bin", &content);

    let chunks = db.put("cov.bin").await.unwrap();
    let mut offset = 0u64;
    for c in &chunks {
        assert_eq!(c.offset, offset);
        assert!(c.length > 0);
        assert!(c.length <= MAX_CHUNK_SIZE);
        offset += c.length as u64;
    }
    assert_eq!(offset, content.len() as u64);
}

#[tokio::test]
async fn chunks_sorted_by_offset() {
    let (tmp, db) = disk_blobsdb();
    let content: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();
    write_file(&tmp, "sorted.bin", &content);

    let chunks = db.put("sorted.bin").await.unwrap();
    let mut sorted = chunks.clone();
    sorted.sort_by_key(|c| c.offset);
    assert_eq!(chunks, sorted);
}

#[tokio::test]
async fn put_is_deterministic() {
    let (tmp, db) = disk_blobsdb();
    let content = b"deterministic content".repeat(1000);
    write_file(&tmp, "det.txt", &content);

    let c1 = db.put("det.txt").await.unwrap();
    let c2 = db.put("det.txt").await.unwrap();
    assert_eq!(c1, c2);
}

#[tokio::test]
async fn chunk_hash_matches_content() {
    let (tmp, db) = disk_blobsdb();
    write_file(&tmp, "h.txt", b"hash me");

    let chunks = db.put("h.txt").await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(
        chunks[0].hash,
        blake3::hash(b"hash me").as_bytes().to_owned()
    );
}

// ---------- errors ----------

#[tokio::test]
async fn put_nonexistent_file_errors() {
    let (_tmp, db) = disk_blobsdb();
    assert!(db.put("nope.txt").await.is_err());
}

#[tokio::test]
async fn get_with_unknown_chunk_hash_errors() {
    let (_tmp, db) = disk_blobsdb();
    let chunks = vec![Chunk {
        offset: 0,
        length: 10,
        hash: [0u8; 32],
    }];
    assert!(db.get(Arc::new(chunks)).await.is_err());
}

// ---------- copy ----------

#[tokio::test]
async fn copy_replicates_blobs_to_destination() {
    let (tmp, src) = disk_blobsdb();
    let dst = mem_blobsdb();

    write_file(&tmp, "c1.txt", b"copy me one");
    write_file(&tmp, "c2.txt", b"copy me two");
    let chunks1 = src.put("c1.txt").await.unwrap();
    let chunks2 = src.put("c2.txt").await.unwrap();

    src.copy(&dst).await.unwrap();

    let got1 = dst.get(Arc::new(chunks1)).await.unwrap();
    let got2 = dst.get(Arc::new(chunks2)).await.unwrap();
    assert_eq!(got1, b"copy me one");
    assert_eq!(got2, b"copy me two");
}

// ---------- Chunk ----------

#[test]
fn chunk_serde_roundtrip() {
    let chunk = Chunk {
        offset: 42,
        length: 1024,
        hash: [7u8; 32],
    };
    let json = serde_json::to_string(&chunk).unwrap();
    let back: Chunk = serde_json::from_str(&json).unwrap();
    assert_eq!(chunk, back);
}

#[test]
fn chunk_equality_and_hash() {
    use std::collections::HashSet;
    use std::hash::{BuildHasher, Hasher};
    let a = Chunk { offset: 0, length: 1, hash: [1u8; 32] };
    let b = Chunk { offset: 5, length: 9, hash: [1u8; 32] };
    let c = Chunk { offset: 0, length: 1, hash: [2u8; 32] };

    // Hash impl only considers the content hash: a and b hash identically
    let hasher = std::collections::hash_map::RandomState::new();
    let mut h1 = hasher.build_hasher();
    std::hash::Hash::hash(&a, &mut h1);
    let mut h2 = hasher.build_hasher();
    std::hash::Hash::hash(&b, &mut h2);
    assert_eq!(h1.finish(), h2.finish());

    // ...but Eq is field-wise, so a HashSet still treats them as distinct
    let mut set = HashSet::new();
    set.insert(a);
    set.insert(b);
    set.insert(c);
    assert_eq!(set.len(), 3);

    assert_ne!(a, b);
    assert_eq!(a, Chunk { offset: 0, length: 1, hash: [1u8; 32] });
}
