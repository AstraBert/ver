//! Tests for `ver::clone::clone_from_remote`.
//!
//! Follows the same patterns as `blobsdb/tests/sync.rs`:
//!   - in-memory `ObjectStore` remotes (no credentials needed)
//!   - `TempDir`-based sandboxing
//!   - test doubles (`FlakyStore`) for failure injection
//!   - deterministic pseudo-random content for multi-chunk files
//!
//! Because `clone_from_remote` takes a `GetObjectStoreInput` (which
//! builds a fresh store internally), the success-path tests use a
//! `clone_with_store` helper that replicates the same logic but
//! accepts an `Arc<dyn ObjectStore>` directly — the same pattern
//! used in `sync.rs` where `BlobsDB::sync` is called with a custom
//! remote.

use std::{
    collections::HashSet,
    fmt, fs, io,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
};

use blobsdb::{
    GetObjectStoreInput, ObjPath, ObjectStoreExt, PutPayload, download_remote_repository,
    get_object_store,
};
use futures::stream::BoxStream;
use futures_util::FutureExt;
use object_store::{
    CopyOptions, GetOptions, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutResult, memory::InMemory,
};
use redb::{Database, ReadableDatabase};
use serial_test::serial;
use tempdir::TempDir;
use tokio::sync::Semaphore;
use ver::{
    clone::clone_from_remote,
    commons::get_head,
    errors::VerError,
    files::{self, MAX_INFLIGHT_READS, SNAPSHOT_TABLE, Snapshot, panic_msg, rt},
    init::{HEAD_PATH, REDB_PATH, SNAPSHOT_HISTORY_FILE, VER_DIRECTORY, init_blobs_db, initialize},
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// RAII guard that restores the original working directory on drop.
struct CwdGuard {
    original: PathBuf,
}

impl CwdGuard {
    fn change_to(dir: &Path) -> Self {
        let original = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();
        Self { original }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.original);
    }
}

/// Initialize a ver repository in a fresh tempdir and return the
/// tempdir guard together with a `CwdGuard` positioned inside it.
fn setup_repo() -> (TempDir, CwdGuard) {
    let tmp = TempDir::new("ver_clone_test").unwrap();
    let mut out = Vec::new();
    initialize(Some(tmp.path().to_str().unwrap()), &mut out).unwrap();
    let guard = CwdGuard::change_to(tmp.path());
    (tmp, guard)
}

/// Initialize a repo, write the given files, snapshot them, and return
/// the tempdir + guard.
fn setup_repo_with_files(file_list: &[(&str, &[u8])]) -> (TempDir, CwdGuard) {
    let (tmp, guard) = setup_repo();
    for (name, content) in file_list {
        let p = tmp.path().join(name);
        if let Some(parent) = p.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(&p, content).unwrap();
    }
    let mut out = Vec::new();
    files::snap("test-snap", None, &mut out).unwrap();
    (tmp, guard)
}

/// Sync the current repo (cwd) to the given remote under `remote_name`.
///
/// Replicates what `sync_with_remote_storage` does, but accepts an
/// `Arc<dyn ObjectStore>` directly so tests can use in-memory remotes.
fn sync_to_remote(remote_name: &str, remote: Arc<dyn ObjectStore>) {
    let resolved = ver::commons::resolve_ver_dir_path(std::env::current_dir().unwrap()).unwrap();
    let current_head = get_head(&resolved).unwrap().unwrap();
    let redb_content = fs::read(resolved.join(REDB_PATH)).unwrap();
    let history_content = fs::read(resolved.join(SNAPSHOT_HISTORY_FILE)).unwrap();
    let blobdb = init_blobs_db(&resolved).unwrap();

    let redb = Database::open(resolved.join(REDB_PATH)).unwrap();
    let rtx = redb.begin_read().unwrap();
    let table = rtx.open_table(SNAPSHOT_TABLE).unwrap();
    let head_snap_bytes = table.get(current_head).unwrap().unwrap();
    let snap: Snapshot = postcard::from_bytes(head_snap_bytes.value()).unwrap();

    let mut chunks = HashSet::new();
    for (_, cs) in &snap.repository_state {
        chunks.extend(cs.iter());
    }

    let mut err_buf = io::stderr();
    rt().block_on(async {
        blobdb
            .sync(
                remote_name.to_string(),
                remote,
                chunks,
                current_head,
                redb_content,
                history_content,
                &mut err_buf,
            )
            .await
    })
    .unwrap();
}

/// Clone from a remote using an `Arc<dyn ObjectStore>` directly.
///
/// This replicates the logic of `clone_from_remote` but accepts a
/// store instance, which allows tests to pre-populate an in-memory
/// remote.  The code path is identical to what `clone_from_remote`
/// does after `get_object_store(input)`.
fn clone_with_store<O: Write, E: Write>(
    remote_name: String,
    out_dir: &str,
    store: Arc<dyn ObjectStore>,
    std_out: &mut O,
    std_err: &mut E,
) -> Result<(), VerError> {
    let p = PathBuf::from(out_dir).join(".ver");

    if !p.exists() {
        fs::create_dir_all(&p)?;
    }

    if !p.join("blobs").exists() {
        fs::create_dir(&p.join("blobs"))?;
    }

    let downloaded_chunks =
        rt().block_on(async { download_remote_repository(&store, &p, std_err).await })?;

    writeln!(
        std_out,
        "Downloaded {} chunks from remote storage",
        downloaded_chunks
    )?;

    let blobdb = Arc::new(init_blobs_db(&p)?);

    rt().block_on(async { blobdb.create_sync_map_after_download(remote_name).await })?;

    let head = get_head(&p)?.ok_or_else(|| "No head found in the cloned ver repository")?;

    let redb = Database::open(&p.join(REDB_PATH))?;

    let tx = redb.begin_read()?;
    let table = tx.open_table(SNAPSHOT_TABLE)?;
    let head_snap = table.get(head)?;

    if let Some(hs) = head_snap {
        let snap: Snapshot = postcard::from_bytes(hs.value())?;

        let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
        let (tx, rx) = mpsc::channel::<(PathBuf, Result<Vec<u8>, String>)>();

        for (path, chunks) in snap.repository_state {
            let full_path = p
                .parent()
                .ok_or_else(|| "Could not find parent for the cloned ver repository")?
                .join(path);

            if let Some(par) = full_path.parent()
                && !par.exists()
            {
                fs::create_dir_all(par)?;
            }

            let tx = tx.clone();
            let permit = rt()
                .block_on(Arc::clone(&sem).acquire_owned())
                .expect("semaphore closed");
            let db = blobdb.clone();

            rt().spawn(async move {
                let _permit = permit;
                let res =
                    match std::panic::AssertUnwindSafe(
                        async move { db.get(Arc::new(chunks)).await },
                    )
                    .catch_unwind()
                    .await
                    {
                        Ok(r) => r,
                        Err(payload) => Err(panic_msg(payload)),
                    };
                let _ = tx.send((full_path, res));
            });
        }

        drop(tx);

        let mut written = 0;

        for (fl, res) in rx {
            if res.is_err() {
                return Err(res.unwrap_err().into());
            }
            fs::write(fl, res.unwrap())?;
            written += 1;
        }

        writeln!(
            std_out,
            "Written {} files to the cloned repository",
            written
        )?;

        return Ok(());
    }

    Err("Could not find head snapshot".into())
}

/// Deterministic pseudo-random content that does not repeat, so that
/// content-defined chunking produces distinct chunk hashes.
fn unique_content(len: usize) -> Vec<u8> {
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

#[allow(dead_code)]
fn blob_path(hash: &[u8; 32]) -> ObjPath {
    ObjPath::from_url_path(format!("blobs/{}", blake3::Hash::from_bytes(*hash))).unwrap()
}

// ---------------------------------------------------------------------------
// test doubles
// ---------------------------------------------------------------------------

/// An `ObjectStore` wrapper that fails `get` calls for paths in a
/// configurable deny-list.  Used to simulate remote chunk corruption
/// or unavailability during clone.
#[derive(Debug)]
struct FlakyStore {
    inner: InMemory,
    /// paths that must always fail on `get`
    fail_gets: Mutex<HashSet<ObjPath>>,
}

impl FlakyStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_gets: Mutex::new(HashSet::new()),
        }
    }

    fn fail_get(&self, path: ObjPath) {
        self.fail_gets.lock().unwrap().insert(path);
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
        if self.fail_gets.lock().unwrap().contains(location) {
            return Err(object_store::Error::Generic {
                store: "flaky",
                source: format!("injected get failure for {location}").into(),
            });
        }
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
// tests: error paths via clone_from_remote (GetObjectStoreInput)
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn clone_fails_when_remote_has_no_head() {
    // An empty in-memory remote — nothing was synced to it
    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    let result = clone_from_remote(
        "origin".to_string(),
        clone_dir.to_str().unwrap().to_string(),
        GetObjectStoreInput::InMemory,
        &mut out,
        &mut err,
    );

    assert!(result.is_err(), "clone from empty remote should fail");
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("head") || msg.contains("Head"),
        "error should mention head: {msg}"
    );
}

// ---------------------------------------------------------------------------
// tests: success paths via clone_with_store (Arc<dyn ObjectStore>)
// ---------------------------------------------------------------------------

#[test]
#[serial]
fn clone_restores_single_file() {
    let content = b"hello clone";
    let (_src_tmp, _guard) = setup_repo_with_files(&[("hello.txt", content)]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let cloned_content = fs::read(clone_dir.join("hello.txt")).unwrap();
    assert_eq!(cloned_content, content);

    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Downloaded"),
        "expected download message: {output}"
    );
    assert!(
        output.contains("Written"),
        "expected written message: {output}"
    );
}

#[test]
#[serial]
fn clone_restores_multiple_files() {
    let file_list: Vec<(&str, &[u8])> = vec![
        ("a.txt", b"alpha"),
        ("b.txt", b"beta"),
        ("sub/dir/c.txt", b"gamma"),
    ];
    let (_src_tmp, _guard) = setup_repo_with_files(&file_list);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    for (name, expected) in &file_list {
        let path = clone_dir.join(name);
        assert!(path.exists(), "missing cloned file: {name}");
        assert_eq!(
            fs::read(&path).unwrap(),
            *expected,
            "content mismatch for {name}"
        );
    }
}

#[test]
#[serial]
fn clone_restores_large_multi_chunk_file() {
    let content = unique_content(1_500_000); // > 1 MB, guarantees multiple chunks
    let (_src_tmp, _guard) = setup_repo_with_files(&[("big.bin", &content)]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let cloned = fs::read(clone_dir.join("big.bin")).unwrap();
    assert_eq!(cloned.len(), content.len());
    assert_eq!(cloned, content);
}

#[test]
#[serial]
fn clone_creates_ver_directory_structure() {
    let (_src_tmp, _guard) = setup_repo_with_files(&[("f.txt", b"data")]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let ver_dir = clone_dir.join(VER_DIRECTORY);
    assert!(ver_dir.exists(), ".ver directory must exist");
    assert!(ver_dir.join("blobs").is_dir(), ".ver/blobs must exist");
    assert!(
        ver_dir.join(REDB_PATH).exists(),
        ".ver/files.redb must exist"
    );
    assert!(ver_dir.join(HEAD_PATH).exists(), ".ver/head must exist");
    assert!(
        ver_dir.join(SNAPSHOT_HISTORY_FILE).exists(),
        ".ver/snapshot_history must exist"
    );
}

#[test]
#[serial]
fn clone_head_matches_source_head() {
    let (_src_tmp, _guard) = setup_repo_with_files(&[("f.txt", b"head check")]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let src_resolved =
        ver::commons::resolve_ver_dir_path(std::env::current_dir().unwrap()).unwrap();
    let src_head = get_head(&src_resolved).unwrap().unwrap();

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let clone_head = get_head(&clone_dir.join(VER_DIRECTORY)).unwrap().unwrap();
    assert_eq!(src_head, clone_head, "cloned head must match source head");
}

#[test]
#[serial]
fn clone_into_deeply_nested_output_path() {
    let (_src_tmp, _guard) = setup_repo_with_files(&[("nested.txt", b"nested")]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("deep").join("nested").join("clone");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    assert_eq!(fs::read(clone_dir.join("nested.txt")).unwrap(), b"nested");
}

#[test]
#[serial]
fn clone_with_empty_snapshot_succeeds() {
    let (_tmp, _guard) = setup_repo();
    let mut out = Vec::new();
    files::snap("empty", None, &mut out).unwrap();

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let output = String::from_utf8(out).unwrap();
    assert!(
        output.contains("Written 0 files"),
        "expected 0 files written: {output}"
    );
}

#[test]
#[serial]
fn clone_then_goto_roundtrip() {
    let content_v1 = b"version 1";
    let (_src_tmp, _guard) = setup_repo_with_files(&[("file.txt", content_v1)]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    // Change into the cloned repo and verify we can goto the snapshot
    let _clone_guard = CwdGuard::change_to(&clone_dir);

    fs::write("file.txt", b"modified").unwrap();

    files::goto::<String>("test-snap", None).unwrap();
    assert_eq!(fs::read("file.txt").unwrap(), content_v1);
}

#[test]
#[serial]
fn clone_downloads_all_chunks_from_remote() {
    let content = unique_content(1_500_000);
    let (_src_tmp, _guard) = setup_repo_with_files(&[("data.bin", &content)]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    // Count how many chunks are in the remote
    let chunk_count = rt().block_on(async {
        let mut stream = remote.list(Some(&ObjPath::from_url_path("blobs").unwrap()));
        let mut n = 0;
        while let Some(item) = futures::StreamExt::next(&mut stream).await {
            item.unwrap();
            n += 1;
        }
        n
    });
    assert!(chunk_count > 1, "need multiple chunks for this test");

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    let output = String::from_utf8(out).unwrap();
    let expected_msg = format!("Downloaded {chunk_count} chunks");
    assert!(
        output.contains(&expected_msg),
        "expected '{expected_msg}' in output: {output}"
    );

    // Verify all chunks exist in the local blobs directory
    let blobs_dir = clone_dir.join(VER_DIRECTORY).join("blobs");
    let local_chunks: Vec<_> = fs::read_dir(&blobs_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .collect();
    assert_eq!(
        local_chunks.len(),
        chunk_count,
        "local blobs dir should have the same number of chunks as the remote"
    );
}

#[test]
#[serial]
fn clone_with_binary_content() {
    let mut content = Vec::new();
    content.extend_from_slice(b"\x00\x01\x02\x03");
    content.extend_from_slice(&[0xFF, 0xFE, 0xFD]);
    content.extend_from_slice(b"\x00middle\x00");
    content.extend_from_slice(&unique_content(100_000));

    let (_src_tmp, _guard) = setup_repo_with_files(&[("binary.dat", &content)]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    assert_eq!(fs::read(clone_dir.join("binary.dat")).unwrap(), content);
}

#[test]
#[serial]
fn clone_fails_when_remote_has_head_but_no_redb() {
    // Put only a head (no files.redb) into the remote
    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    let head = [7u8; 32];
    rt().block_on(async {
        remote
            .put(
                &ObjPath::from_url_path("head").unwrap(),
                PutPayload::from(head.to_vec()),
            )
            .await
            .unwrap();
    });

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    let result = clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    );

    assert!(result.is_err(), "clone without files.redb should fail");
}

#[test]
#[serial]
fn clone_fails_when_a_chunk_is_missing_from_remote() {
    let content = unique_content(1_500_000);
    let (_src_tmp, _guard) = setup_repo_with_files(&[("big.bin", &content)]);

    // Sync to a FlakyStore so we can selectively fail chunk downloads
    let flaky = Arc::new(FlakyStore::new());
    let as_store: Arc<dyn ObjectStore> = flaky.clone();
    sync_to_remote("origin", as_store.clone());

    // Find one chunk in the remote and make it fail on get
    let first_chunk_path = rt().block_on(async {
        let mut stream = as_store.list(Some(&ObjPath::from_url_path("blobs").unwrap()));
        let item = futures::StreamExt::next(&mut stream)
            .await
            .expect("should have at least one chunk")
            .unwrap();
        item.location
    });
    flaky.fail_get(first_chunk_path);

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    let result = clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        as_store,
        &mut out,
        &mut err,
    );

    assert!(
        result.is_err(),
        "clone should fail when a chunk is unavailable"
    );
}

#[test]
#[serial]
fn clone_sync_map_is_created() {
    let (_src_tmp, _guard) = setup_repo_with_files(&[("f.txt", b"sync map test")]);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    // The sync map should exist after cloning
    let sync_map_path = clone_dir.join(VER_DIRECTORY).join("sync_map");
    assert!(
        sync_map_path.exists(),
        "sync_map should be created after clone"
    );
}

#[test]
#[serial]
fn clone_multiple_files_in_subdirectories() {
    let file_list: Vec<(&str, &[u8])> = vec![
        ("src/main.rs", b"fn main() {}"),
        ("src/lib.rs", b"pub mod foo;"),
        ("src/foo/mod.rs", b"pub fn bar() {}"),
        ("Cargo.toml", b"[package]"),
        ("README.md", b"# Project"),
    ];
    let (_src_tmp, _guard) = setup_repo_with_files(&file_list);

    let remote = get_object_store(GetObjectStoreInput::InMemory).unwrap();
    sync_to_remote("origin", remote.clone());

    let clone_tmp = TempDir::new("ver_clone_dest").unwrap();
    let clone_dir = clone_tmp.path().join("cloned");
    let mut out = Vec::new();
    let mut err = Vec::new();

    clone_with_store(
        "origin".to_string(),
        clone_dir.to_str().unwrap(),
        remote,
        &mut out,
        &mut err,
    )
    .unwrap();

    for (name, expected) in &file_list {
        let path = clone_dir.join(name);
        assert!(path.exists(), "missing cloned file: {name}");
        assert_eq!(
            fs::read(&path).unwrap(),
            *expected,
            "content mismatch for {name}"
        );
    }
}
