use std::{
    collections::HashSet,
    fmt, io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use blobsdb::{
    BlobsDB, Chunk, GetObjectStoreInput, ObjPath, ObjectStoreExt, PutPayload, get_object_store,
    get_remote_head,
};
use futures::stream::BoxStream;
use object_store::{
    CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutResult, memory::InMemory,
};
use tempdir::TempDir;

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Create a temporary BlobsDB backed by local storage, plus a fresh
/// in-memory remote. Returns the tempdir guard, the db and the remote.
fn setup() -> (TempDir, Arc<BlobsDB>, Arc<dyn ObjectStore>) {
    let tmp = TempDir::new("blobsdb_sync_test").unwrap();
    let blobs_dir = tmp.path().join("blobs");
    std::fs::create_dir_all(&blobs_dir).unwrap();
    let store = get_object_store(GetObjectStoreInput::Local).unwrap();
    let db = BlobsDB::new(blobs_dir, tmp.path().join("sync_map"), store).unwrap();
    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    (tmp, Arc::new(db), remote)
}

/// Write a file with the given content into the tempdir and `put` it
/// into the local blobs store, returning its chunks.
async fn put_file(tmp: &TempDir, db: &BlobsDB, name: &str, content: &[u8]) -> Vec<Chunk> {
    let path = tmp.path().join(name);
    std::fs::write(&path, content).unwrap();
    db.put(&path).await.unwrap()
}

fn chunk_set(chunks: Vec<Chunk>) -> HashSet<Chunk> {
    chunks.into_iter().collect()
}

fn blob_path(hash: &[u8; 32]) -> ObjPath {
    ObjPath::from_url_path(format!("blobs/{}", blake3::Hash::from_bytes(*hash))).unwrap()
}

/// Run a sync against the given remote with throwaway head and redb
/// content.
async fn run_sync(
    db: &BlobsDB,
    remote_name: &str,
    remote: Arc<dyn ObjectStore>,
    chunks: HashSet<Chunk>,
    head: [u8; 32],
) -> Result<(), String> {
    let mut err_buf = io::stderr();
    db.sync(
        remote_name.to_string(),
        remote,
        chunks,
        head,
        b"fake-redb".to_vec(),
        b"fake-history".to_vec(),
        &mut err_buf,
    )
    .await
}

// ---------------------------------------------------------------------------
// test doubles
// ---------------------------------------------------------------------------

/// An `ObjectStore` wrapper that fails `put` calls according to a
/// configurable policy, counting every attempt. Used to simulate
/// transient and permanent remote failures.
#[derive(Debug)]
struct FlakyStore {
    inner: InMemory,
    /// number of `put` calls that should fail before succeeding
    remaining_failures: Mutex<usize>,
    /// paths that must always fail (simulating a permanently bad chunk)
    always_fail: Mutex<HashSet<ObjPath>>,
}

impl FlakyStore {
    fn new(fail_first_n: usize) -> Self {
        Self {
            inner: InMemory::new(),
            remaining_failures: Mutex::new(fail_first_n),
            always_fail: Mutex::new(HashSet::new()),
        }
    }
}

impl fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlakyStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.always_fail.lock().unwrap().contains(location) {
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: format!("injected permanent failure for {location}").into(),
            });
        }
        {
            let mut remaining = self.remaining_failures.lock().unwrap();
            if *remaining > 0 {
                *remaining -= 1;
                return Err(object_store::Error::Generic {
                    store: "flaky",
                    source: "injected transient failure".into(),
                });
            }
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: std::pin::Pin<
            Box<dyn futures::Stream<Item = object_store::Result<ObjPath>> + Send>,
        >,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Deterministic pseudo-random content that does not repeat, so that
/// content-defined chunking produces distinct chunk hashes.
fn unique_content(len: usize) -> Vec<u8> {
    // xorshift32 — deterministic, no extra deps
    let mut state = 0x9E3779B9u32;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            (state & 0xFF) as u8
        })
        .collect()
}

/// Counts `put` calls targeting the `blobs/` prefix.
#[derive(Debug)]
struct CountingPuts {
    inner: Arc<dyn ObjectStore>,
    blob_puts: AtomicUsize,
}

impl CountingPuts {
    fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner,
            blob_puts: AtomicUsize::new(0),
        }
    }

    fn blob_puts(&self) -> usize {
        self.blob_puts.load(Ordering::SeqCst)
    }
}

impl fmt::Display for CountingPuts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CountingPuts")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingPuts {
    async fn put_opts(
        &self,
        location: &ObjPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        if location.as_ref().starts_with("blobs/") {
            self.blob_puts.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &ObjPath,
        options: GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: std::pin::Pin<
            Box<dyn futures::Stream<Item = object_store::Result<ObjPath>> + Send>,
        >,
    ) -> BoxStream<'static, object_store::Result<ObjPath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &ObjPath,
        to: &ObjPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_uploads_all_chunks_and_head() {
    let (tmp, db, remote) = setup();
    let chunks = put_file(&tmp, &db, "a.txt", b"hello sync").await;
    let head = [7u8; 32];

    run_sync(&db, "test", remote.clone(), chunk_set(chunks.clone()), head)
        .await
        .unwrap();

    // every chunk must be present under blobs/<hash>
    for c in &chunks {
        remote.head(&blob_path(&c.hash)).await.unwrap();
    }

    // head must match
    let remote_head = get_remote_head(&remote).await.unwrap();
    assert_eq!(remote_head, Some(head));

    // files.redb must be present
    remote
        .head(&ObjPath::from_url_path("files.redb").unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn sync_is_idempotent_and_skips_known_chunks() {
    let (tmp, db, _remote) = setup();
    let chunks = put_file(&tmp, &db, "a.txt", b"idempotent content").await;
    let set = chunk_set(chunks);

    // first sync against a plain in-memory remote
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    run_sync(&db, "test", remote.clone(), set.clone(), [1u8; 32])
        .await
        .unwrap();

    // second sync against a counting wrapper around the same remote:
    // no chunk `put` should be attempted at all (only head + files.redb
    // + snapshot_history go through)
    let counting = Arc::new(CountingPuts::new(remote));
    let as_store: Arc<dyn ObjectStore> = counting.clone();
    run_sync(&db, "test", as_store, set, [1u8; 32])
        .await
        .unwrap();

    assert_eq!(
        counting.blob_puts(),
        0,
        "second sync must not re-upload any chunk"
    );
}

#[tokio::test]
async fn sync_map_is_namespaced_per_remote() {
    let (tmp, db, _r) = setup();
    let chunks = put_file(&tmp, &db, "a.txt", b"namespaced").await;
    let set = chunk_set(chunks);

    let remote_a: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let counting_b = Arc::new(CountingPuts::new(
        Arc::new(InMemory::new()) as Arc<dyn ObjectStore>
    ));

    run_sync(&db, "remote-a", remote_a, set.clone(), [1u8; 32])
        .await
        .unwrap();

    // syncing the same chunks to a *different* remote must upload
    // everything again: the sync map is per-remote
    let as_store: Arc<dyn ObjectStore> = counting_b.clone();
    run_sync(&db, "remote-b", as_store, set.clone(), [1u8; 32])
        .await
        .unwrap();

    assert_eq!(
        counting_b.blob_puts(),
        set.len(),
        "a new remote must receive all chunks"
    );
}

#[tokio::test]
async fn sync_collects_chunk_failures_and_does_not_advance_head() {
    let (tmp, db, _r) = setup();
    // content large enough to produce several chunks
    let content: Vec<u8> = unique_content(1_500_000);
    let chunks = put_file(&tmp, &db, "big.bin", &content).await;
    assert!(chunks.len() > 1);

    let flaky = Arc::new(FlakyStore::new(0));
    // make one specific chunk always fail
    let victim = &chunks[0];
    let victim_path = blob_path(&victim.hash);
    flaky
        .always_fail
        .lock()
        .unwrap()
        .insert(victim_path.clone());

    let as_store: Arc<dyn ObjectStore> = flaky.clone();
    let res = run_sync(
        &db,
        "test",
        as_store.clone(),
        chunk_set(chunks.clone()),
        [9u8; 32],
    )
    .await;

    let err = res.unwrap_err();
    assert!(
        err.contains("failed to upload"),
        "unexpected error message: {err}"
    );

    // head must NOT have been written
    assert_eq!(get_remote_head(&as_store).await.unwrap(), None);

    // the victim chunk must be missing, the others present
    assert!(flaky.head(&victim_path).await.is_err());
    for c in &chunks[1..] {
        flaky.head(&blob_path(&c.hash)).await.unwrap();
    }
}

#[tokio::test]
async fn sync_resumes_after_transient_failure() {
    let (tmp, db, _r) = setup();
    let content: Vec<u8> = unique_content(1_500_000);
    let chunks = put_file(&tmp, &db, "big.bin", &content).await;
    let set = chunk_set(chunks.clone());
    assert!(
        set.len() >= 4,
        "need several distinct chunks for this test, got {}",
        set.len()
    );

    // fail the first 3 chunk puts, then let everything through
    let flaky = Arc::new(FlakyStore::new(3));
    let as_store: Arc<dyn ObjectStore> = flaky.clone();

    let first = run_sync(&db, "test", as_store.clone(), set.clone(), [3u8; 32]).await;
    assert!(first.is_err(), "first sync should fail");

    // second attempt must succeed...
    let mut err_buf: Vec<u8> = Vec::new();
    let second = db
        .sync(
            "test".to_string(),
            as_store.clone(),
            set.clone(),
            [3u8; 32],
            b"fake-redb".to_vec(),
            b"fake-history".to_vec(),
            &mut err_buf,
        )
        .await;
    assert!(
        second.is_ok(),
        "second sync failed: {:?}, chunk errors: {}",
        second.err(),
        String::from_utf8_lossy(&err_buf)
    );

    // ...and the remote must be complete
    let remote_head = get_remote_head(&as_store).await.unwrap();
    assert_eq!(remote_head, Some([3u8; 32]));

    for c in &chunks {
        as_store.head(&blob_path(&c.hash)).await.unwrap();
    }
}

#[tokio::test]
async fn sync_writes_files_redb_and_history() {
    let (tmp, db, remote) = setup();
    let chunks = put_file(&tmp, &db, "a.txt", b"meta test").await;

    run_sync(&db, "test", remote.clone(), chunk_set(chunks), [5u8; 32])
        .await
        .unwrap();

    let redb = remote
        .get(&ObjPath::from_url_path("files.redb").unwrap())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&redb[..], b"fake-redb");

    let history = remote
        .get(&ObjPath::from_url_path("snapshot_history").unwrap())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(&history[..], b"fake-history");
}

#[tokio::test]
async fn corrupted_remote_head_is_rejected() {
    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    remote
        .put(
            &ObjPath::from_url_path("head").unwrap(),
            PutPayload::from(vec![1u8, 2, 3]), // too short
        )
        .await
        .unwrap();

    let err = get_remote_head(&remote).await.unwrap_err();
    assert!(
        err.contains("32"),
        "error should mention the expected length: {err}"
    );
}

#[tokio::test]
async fn sync_with_empty_chunk_set_still_writes_head() {
    let (_tmp, db, remote) = setup();

    run_sync(&db, "test", remote.clone(), HashSet::new(), [42u8; 32])
        .await
        .unwrap();

    assert_eq!(get_remote_head(&remote).await.unwrap(), Some([42u8; 32]));
}
