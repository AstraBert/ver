use blobsdb::{BlobsDB, Chunk, GetObjectStoreInput, get_object_store};
use std::sync::Arc;
use tempdir::TempDir;

/// Helper function to create a temporary BlobsDB instance with local storage
fn setup_blobsdb() -> (TempDir, BlobsDB) {
    let tmp = TempDir::new("blobsdb_test").unwrap();
    let store = get_object_store(GetObjectStoreInput::Local).unwrap();
    let db = BlobsDB::new(tmp.path(), store).unwrap();
    (tmp, db)
}

/// Helper function to create a test file with given content
fn create_test_file(dir: &TempDir, name: &str, content: &[u8]) -> std::path::PathBuf {
    let file_path = dir.path().join(name);
    std::fs::write(&file_path, content).unwrap();
    file_path
}

#[tokio::test]
async fn test_put_and_get_small_file() {
    let (tmp, db) = setup_blobsdb();
    let content = b"Hello, blobsdb!";
    let file_path = create_test_file(&tmp, "test.txt", content);

    // Put the file
    let chunks = db.put(&file_path).await.unwrap();
    assert!(!chunks.is_empty());

    // Get the file back
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_put_and_get_empty_file() {
    let (tmp, db) = setup_blobsdb();
    let content = b"";
    let file_path = create_test_file(&tmp, "empty.txt", content);

    // Put the file
    let chunks = db.put(&file_path).await.unwrap();
    assert_eq!(chunks.len(), 0);

    // Get the file back
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_put_and_get_large_file() {
    let (tmp, db) = setup_blobsdb();
    // Create a file larger than MAX_CHUNK_SIZE to ensure multiple chunks
    let content: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();
    let file_path = create_test_file(&tmp, "large.bin", &content);

    // Put the file
    let chunks = db.put(&file_path).await.unwrap();
    assert!(chunks.len() > 1, "Large file should be split into multiple chunks");

    // Verify chunk properties
    let mut expected_offset = 0;
    for chunk in &chunks {
        assert_eq!(chunk.offset, expected_offset);
        assert!(chunk.length > 0);
        assert!(chunk.length <= blobsdb::MAX_CHUNK_SIZE);
        expected_offset += chunk.length as u64;
    }

    // Get the file back
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_chunk_deduplication() {
    let (tmp, db) = setup_blobsdb();
    // Create a file with repeated content
    let repeated_content = b"duplicate".repeat(1000);
    let file_path = create_test_file(&tmp, "dup.txt", &repeated_content);

    // Put the file
    let chunks = db.put(&file_path).await.unwrap();
    assert!(!chunks.is_empty());

    // Get the file back
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, repeated_content);
}

#[tokio::test]
async fn test_put_nonexistent_file() {
    let (_tmp, db) = setup_blobsdb();
    let result = db.put("/nonexistent/path/to/file.txt").await;
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("Error while opening file"));
}

#[tokio::test]
async fn test_get_with_invalid_chunks() {
    let (_tmp, db) = setup_blobsdb();
    // Create chunks with invalid hashes
    let invalid_chunks = vec![Chunk {
        offset: 0,
        length: 100,
        hash: [0u8; 32],
    }];

    let result = db.get(Arc::new(invalid_chunks)).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_chunk_serialization() {
    let chunk = Chunk {
        offset: 42,
        length: 1024,
        hash: [1u8; 32],
    };

    // Test JSON serialization
    let json = serde_json::to_string(&chunk).unwrap();
    let deserialized: Chunk = serde_json::from_str(&json).unwrap();
    assert_eq!(chunk, deserialized);
}

#[tokio::test]
async fn test_chunk_equality() {
    let chunk1 = Chunk {
        offset: 0,
        length: 100,
        hash: [1u8; 32],
    };
    let chunk2 = Chunk {
        offset: 0,
        length: 100,
        hash: [1u8; 32],
    };
    let chunk3 = Chunk {
        offset: 0,
        length: 100,
        hash: [2u8; 32],
    };

    assert_eq!(chunk1, chunk2);
    assert_ne!(chunk1, chunk3);
}

#[tokio::test]
async fn test_put_same_file_twice() {
    let (tmp, db) = setup_blobsdb();
    let content = b"test content";
    let file_path = create_test_file(&tmp, "test.txt", content);

    // Put the file twice
    let chunks1 = db.put(&file_path).await.unwrap();
    let chunks2 = db.put(&file_path).await.unwrap();

    // Chunks should be identical (same content = same chunks)
    assert_eq!(chunks1, chunks2);

    // Both should retrieve the same content
    let retrieved1 = db.get(Arc::new(chunks1)).await.unwrap();
    let retrieved2 = db.get(Arc::new(chunks2)).await.unwrap();
    assert_eq!(retrieved1, content);
    assert_eq!(retrieved2, content);
}

#[tokio::test]
async fn test_put_multiple_files() {
    let (tmp, db) = setup_blobsdb();
    let content1 = b"first file";
    let content2 = b"second file";
    let file_path1 = create_test_file(&tmp, "file1.txt", content1);
    let file_path2 = create_test_file(&tmp, "file2.txt", content2);

    // Put both files
    let chunks1 = db.put(&file_path1).await.unwrap();
    let chunks2 = db.put(&file_path2).await.unwrap();

    // Chunks should be different
    assert_ne!(chunks1, chunks2);

    // Both should retrieve their respective content
    let retrieved1 = db.get(Arc::new(chunks1)).await.unwrap();
    let retrieved2 = db.get(Arc::new(chunks2)).await.unwrap();
    assert_eq!(retrieved1, content1);
    assert_eq!(retrieved2, content2);
}

#[tokio::test]
async fn test_get_object_store_local() {
    let store = get_object_store(GetObjectStoreInput::Local);
    assert!(store.is_ok());
}

#[tokio::test]
async fn test_blobsdb_new() {
    let tmp = TempDir::new("blobsdb_test").unwrap();
    let store = get_object_store(GetObjectStoreInput::Local).unwrap();
    let db = BlobsDB::new(tmp.path(), store);
    assert!(db.is_ok());
}

#[tokio::test]
async fn test_chunk_boundaries() {
    let (tmp, db) = setup_blobsdb();
    // Create content that will definitely create multiple chunks
    let content: Vec<u8> = (0..2_000_000).map(|i| (i % 256) as u8).collect();
    let file_path = create_test_file(&tmp, "boundaries.bin", &content);

    let chunks = db.put(&file_path).await.unwrap();
    assert!(chunks.len() > 1);

    // Verify chunks are contiguous and cover the entire file
    let mut expected_offset = 0;
    for chunk in &chunks {
        assert_eq!(chunk.offset, expected_offset, "Chunks should be contiguous");
        expected_offset += chunk.length as u64;
    }
    assert_eq!(expected_offset, content.len() as u64, "Chunks should cover entire file");

    // Verify round-trip
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_binary_content() {
    let (tmp, db) = setup_blobsdb();
    // Test with binary content including null bytes
    let content: Vec<u8> = vec![0, 1, 2, 3, 255, 254, 253, 0, 0, 0];
    let file_path = create_test_file(&tmp, "binary.bin", &content);

    let chunks = db.put(&file_path).await.unwrap();
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_unicode_content() {
    let (tmp, db) = setup_blobsdb();
    let content = "Hello, 世界! 🦀 Rust is awesome! 🚀".as_bytes();
    let file_path = create_test_file(&tmp, "unicode.txt", content);

    let chunks = db.put(&file_path).await.unwrap();
    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_concurrent_puts() {
    let (tmp, db) = setup_blobsdb();
    let db = Arc::new(db);

    // Create multiple files
    let files: Vec<_> = (0..10)
        .map(|i| {
            let content = format!("content {}", i).into_bytes();
            create_test_file(&tmp, &format!("file{}.txt", i), &content)
        })
        .collect();

    // Put all files concurrently
    let mut handles = vec![];
    for file_path in files {
        let db = Arc::clone(&db);
        let handle = tokio::spawn(async move {
            db.put(&file_path).await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        let chunks = handle.await.unwrap().unwrap();
        assert!(!chunks.is_empty());
    }
}

#[tokio::test]
async fn test_concurrent_gets() {
    let (tmp, db) = setup_blobsdb();
    let content = b"shared content";
    let file_path = create_test_file(&tmp, "shared.txt", content);

    let chunks = db.put(&file_path).await.unwrap();
    let chunks = Arc::new(chunks);
    let db = Arc::new(db);

    // Get the same file concurrently
    let mut handles = vec![];
    for _ in 0..10 {
        let db = Arc::clone(&db);
        let chunks = Arc::clone(&chunks);
        let handle = tokio::spawn(async move {
            db.get(chunks).await
        });
        handles.push(handle);
    }

    // Wait for all to complete
    for handle in handles {
        let retrieved = handle.await.unwrap().unwrap();
        assert_eq!(retrieved, content);
    }
}

#[tokio::test]
async fn test_chunk_hash_consistency() {
    let (tmp, db) = setup_blobsdb();
    let content = b"consistent content";
    let file_path = create_test_file(&tmp, "consistent.txt", content);

    // Put the same file multiple times
    let chunks1 = db.put(&file_path).await.unwrap();
    let chunks2 = db.put(&file_path).await.unwrap();

    // Hashes should be identical
    assert_eq!(chunks1.len(), chunks2.len());
    for (c1, c2) in chunks1.iter().zip(chunks2.iter()) {
        assert_eq!(c1.hash, c2.hash);
        assert_eq!(c1.offset, c2.offset);
        assert_eq!(c1.length, c2.length);
    }
}

#[tokio::test]
async fn test_very_small_file() {
    let (tmp, db) = setup_blobsdb();
    let content = b"a";
    let file_path = create_test_file(&tmp, "tiny.txt", content);

    let chunks = db.put(&file_path).await.unwrap();
    assert_eq!(chunks.len(), 1);
    assert_eq!(chunks[0].length, 1);
    assert_eq!(chunks[0].offset, 0);

    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_file_exactly_min_chunk_size() {
    let (tmp, db) = setup_blobsdb();
    let content = vec![0u8; blobsdb::MIN_CHUNK_SIZE];
    let file_path = create_test_file(&tmp, "min_chunk.bin", &content);

    let chunks = db.put(&file_path).await.unwrap();
    assert!(!chunks.is_empty());

    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_file_exactly_max_chunk_size() {
    let (tmp, db) = setup_blobsdb();
    let content = vec![0u8; blobsdb::MAX_CHUNK_SIZE];
    let file_path = create_test_file(&tmp, "max_chunk.bin", &content);

    let chunks = db.put(&file_path).await.unwrap();
    assert!(!chunks.is_empty());

    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}

#[tokio::test]
async fn test_file_larger_than_max_chunk_size() {
    let (tmp, db) = setup_blobsdb();
    let content = vec![0u8; blobsdb::MAX_CHUNK_SIZE + 1];
    let file_path = create_test_file(&tmp, "over_max.bin", &content);

    let chunks = db.put(&file_path).await.unwrap();
    assert!(chunks.len() > 1, "File larger than MAX_CHUNK_SIZE should be split");

    let retrieved = db.get(Arc::new(chunks)).await.unwrap();
    assert_eq!(retrieved, content);
}
