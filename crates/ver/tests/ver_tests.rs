use std::{
    collections::HashMap,
    io::Write,
    str::FromStr,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use blobsdb::BlobsDBWithFs;
use tempdir::TempDir;
use ver::{
    commons::{
        self, PagerWriter, commit_snapshot, get_head, load_snapshot, read_history,
        reconstruct_snapshot, resolve_snapshot_id, rt, run_concurrent, store_snapshot, write_head,
        write_history,
    },
    context::{self, FSType, KVType, VerContext},
    diff::diff,
    goto::goto,
    init::init_context,
    log::log,
    merge::{merge_end, merge_pick, merge_run},
    show::{show_file, to_text},
    snap::{Snapshot, files_snap, full_snap},
    sync::{copy_snapshot, find_common_ancestor, get_parents, sync},
};
use ver_fs::{DiskFs, Fs};
use ver_kv::{KVStore, RedbStore};

// The global context file is process-global (OnceLock), so all tests that
// touch it must be serialized through this lock.
static LOCK: Mutex<()> = Mutex::new(());

fn guard() -> MutexGuard<'static, ()> {
    let g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // Ensure the global context points at a throwaway tempdir before any
    // test (or library code) can initialize it with the real $HOME path.
    global_context_path();
    g
}

fn global_context_path() -> &'static std::path::Path {
    static DIR: OnceLock<TempDir> = OnceLock::new();
    let dir = DIR.get_or_init(|| TempDir::new("ver_global_ctx").unwrap());
    let path = dir.path().join("contexts.toml");
    context::GLOBAL_CONTEXT.set(path.clone()).ok();
    context::GLOBAL_CONTEXT.get().unwrap().as_path()
}

struct TestRepo {
    _dir: TempDir,
    _kv_dir: TempDir,
    name: String,
    fs: Arc<dyn Fs>,
}

/// Create a fresh local context registered in the global context file and
/// marked as current. Keeps any previously created contexts registered.
fn setup_repo(name: &str, extra: &[&TestRepo]) -> TestRepo {
    let dir = TempDir::new("ver_repo").unwrap();
    let kv_dir = TempDir::new("ver_kv").unwrap();
    let base = dir.path().to_string_lossy().into_owned();
    let redb_path = kv_dir.path().join("kv.redb").to_string_lossy().into_owned();

    let mut ctx = VerContext::default();
    for repo in extra {
        ctx.scaffold(&repo.name, FSType::Local, KVType::Embedded).unwrap();
        if let Some(context::FilesystemContext::Local { base_directory }) =
            ctx.fs.get_mut(&repo.name)
        {
            *base_directory = repo.fs_base();
        }
        if let Some(context::KVStoreContext::Embedded { redb_path }) =
            ctx.kv.get_mut(&repo.name)
        {
            *redb_path = repo.kv_path();
        }
    }
    ctx.scaffold(name, FSType::Local, KVType::Embedded).unwrap();
    if let Some(context::FilesystemContext::Local { base_directory }) = ctx.fs.get_mut(name) {
        *base_directory = base.clone();
    }
    if let Some(context::KVStoreContext::Embedded { redb_path: rp }) = ctx.kv.get_mut(name) {
        *rp = redb_path.clone();
    }
    ctx.switch(name).unwrap();
    ctx.to_global_context().unwrap();

    // init first: it creates the redb file; opening it twice concurrently
    // would fail on redb's file lock
    init_context(Some(name)).unwrap();

    let fs: Arc<dyn Fs> = Arc::new(DiskFs::new(&base));

    TestRepo {
        _dir: dir,
        _kv_dir: kv_dir,
        name: name.to_string(),
        fs,
    }
}

impl TestRepo {
    fn fs_base(&self) -> String {
        self._dir.path().to_string_lossy().into_owned()
    }

    fn kv_path(&self) -> String {
        self._kv_dir
            .path()
            .join("kv.redb")
            .to_string_lossy()
            .into_owned()
    }

    /// Open a fresh handle on this repo's KV store.
    ///
    /// redb allows only one open instance per file, and library functions
    /// open their own handle through `current_fs_kv`. Drop the returned
    /// handle before calling into the library.
    fn kv(&self) -> Arc<dyn KVStore> {
        let db = redb::Database::open(self.kv_path()).unwrap();
        Arc::new(RedbStore::new(Arc::new(db)))
    }

    fn write(&self, path: &str, content: &str) {
        let p = self._dir.path().join(path);
        if let Some(parent) = p.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(p, content).unwrap();
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self._dir.path().join(path)).unwrap()
    }

    fn exists(&self, path: &str) -> bool {
        self._dir.path().join(path).exists()
    }
}

fn make_snapshot(pairs: &[(&str, &str)], parents: Vec<[u8; 32]>) -> Snapshot {
    let mut state = HashMap::new();
    for (path, content) in pairs {
        state.insert(
            path.to_string(),
            vec![blobsdb::Chunk {
                offset: 0,
                length: content.len(),
                hash: blake3::hash(content.as_bytes()).as_bytes().to_owned(),
            }],
        );
    }
    Snapshot {
        repository_state: state,
        parent_ids: parents,
        partial: false,
    }
}

fn switch_to(name: &str) {
    let mut ctx = context::read_context().unwrap();
    ctx.switch(name).unwrap();
    ctx.to_global_context().unwrap();
}

// ---------- commons ----------

#[test]
fn head_write_read_roundtrip() {
    let _g = guard();
    let repo = setup_repo("head", &[]);
    let id = [42u8; 32];

    assert!(get_head(&repo.fs).unwrap().is_none());
    write_head(id, &repo.fs).unwrap();
    assert_eq!(get_head(&repo.fs).unwrap().unwrap(), id);
}

#[test]
fn history_write_read_roundtrip() {
    let _g = guard();
    let repo = setup_repo("history", &[]);
    let id = blake3::hash(b"snap");

    write_history(id, "first", Some("a description"), &repo.fs).unwrap();
    let (logs, incomplete) = read_history(false, &repo.fs).unwrap();

    assert!(!incomplete);
    assert_eq!(logs.len(), 1);
    assert!(logs[0].contains(&id.to_string()));
    assert!(logs[0].contains("first"));
    assert!(logs[0].contains("a description"));
}

#[test]
fn history_reads_multiple_entries() {
    let _g = guard();
    let repo = setup_repo("history_multi", &[]);

    write_history(blake3::hash(b"a"), "a", None, &repo.fs).unwrap();
    write_history(blake3::hash(b"b"), "b", None, &repo.fs).unwrap();

    let (logs, incomplete) = read_history(false, &repo.fs).unwrap();
    assert!(!incomplete);
    assert_eq!(logs.len(), 2);
    assert!(logs.iter().any(|l| l.contains("\ta\t")));
    assert!(logs.iter().any(|l| l.contains("\tb\t")));
}

#[test]
fn resolve_snapshot_id_by_hash_and_name() {
    let _g = guard();
    let repo = setup_repo("resolve", &[]);
    let id = blake3::hash(b"some-snapshot").as_bytes().to_owned();

    {
        let kv = repo.kv();
        kv.put_name("mysnap", id).unwrap();

        // by name
        assert_eq!(resolve_snapshot_id("mysnap", &kv).unwrap(), id);
        // by hash hex string
        let hex = blake3::Hash::from_bytes(id).to_string();
        assert_eq!(resolve_snapshot_id(&hex, &kv).unwrap(), id);
        // unknown
        assert!(resolve_snapshot_id("!!!unknown!!!", &kv).is_err());
    }
}

#[test]
fn store_and_load_snapshot_roundtrip() {
    let _g = guard();
    let repo = setup_repo("store_load", &[]);
    let snap = make_snapshot(&[("a.txt", "alpha")], vec![]);

    let kv = repo.kv();
    let id = store_snapshot(&snap, "s1", &kv).unwrap();
    let loaded = load_snapshot(&kv, id.as_bytes().to_owned()).unwrap();

    assert_eq!(loaded.repository_state.len(), 1);
    assert!(loaded.repository_state.contains_key("a.txt"));
    assert!(loaded.parent_ids.is_empty());
    assert_eq!(
        kv.get_name("s1").unwrap().unwrap(),
        id.as_bytes().to_owned()
    );
}

#[test]
fn load_missing_snapshot_errors() {
    let _g = guard();
    let repo = setup_repo("load_missing", &[]);
    assert!(load_snapshot(&repo.kv(), [9u8; 32]).is_err());
}

#[test]
fn commit_snapshot_sets_head_and_history() {
    let _g = guard();
    let repo = setup_repo("commit", &[]);
    let snap = make_snapshot(&[("f.txt", "content")], vec![]);

    let id = commit_snapshot(&snap, "c1", Some("desc"), &repo.fs, &repo.kv()).unwrap();

    assert_eq!(get_head(&repo.fs).unwrap().unwrap(), id.as_bytes().to_owned());
    let (logs, _) = read_history(false, &repo.fs).unwrap();
    assert_eq!(logs.len(), 1);
    assert!(logs[0].contains("c1"));
}

#[test]
fn run_concurrent_preserves_order() {
    let results = run_concurrent((0..100).collect::<Vec<usize>>(), |i| async move {
        i * 2
    })
    .unwrap();
    let expected: Vec<usize> = (0..100).map(|i| i * 2).collect();
    assert_eq!(results, expected);
}

#[test]
fn run_concurrent_empty_input() {
    let results: Vec<usize> = run_concurrent(Vec::<usize>::new(), |i| async move { i }).unwrap();
    assert!(results.is_empty());
}

#[test]
fn reconstruct_snapshot_writes_files_and_deletes_extras() {
    let _g = guard();
    let repo = setup_repo("reconstruct", &[]);
    let db = BlobsDBWithFs::new(repo.fs.clone());

    repo.write("keep.txt", "keep me");
    repo.write("extra.txt", "delete me");

    let chunks = rt().block_on(async { db.put("keep.txt").await.unwrap() });
    let snap = Snapshot {
        repository_state: HashMap::from([("keep.txt".to_string(), chunks)]),
        parent_ids: vec![],
        partial: false,
    };

    // remove the file so reconstruction has something to restore
    std::fs::remove_file(repo._dir.path().join("keep.txt")).unwrap();

    let (del_failed, wrt_failed) =
        reconstruct_snapshot(snap, &db, &repo.fs, vec![], true).unwrap();

    assert!(del_failed.is_empty());
    assert!(wrt_failed.is_empty());
    assert_eq!(repo.read("keep.txt"), "keep me");
    assert!(!repo.exists("extra.txt"));
}

#[test]
fn reconstruct_partial_snapshot_keeps_extra_files() {
    let _g = guard();
    let repo = setup_repo("reconstruct_partial", &[]);
    let db = BlobsDBWithFs::new(repo.fs.clone());

    repo.write("keep.txt", "keep me");
    repo.write("extra.txt", "leave me alone");

    let chunks = rt().block_on(async { db.put("keep.txt").await.unwrap() });
    let snap = Snapshot {
        repository_state: HashMap::from([("keep.txt".to_string(), chunks)]),
        parent_ids: vec![],
        partial: true,
    };

    let (del_failed, wrt_failed) =
        reconstruct_snapshot(snap, &db, &repo.fs, vec![], true).unwrap();

    assert!(del_failed.is_empty());
    assert!(wrt_failed.is_empty());
    assert!(repo.exists("extra.txt"));
}

#[test]
fn pager_writer_handles_split_utf8() {
    let pager = minus::Pager::new();
    let mut w = PagerWriter::new(pager);

    // '世' is 3 bytes; split across two writes
    let bytes = "世".as_bytes();
    w.write_all(&bytes[..1]).unwrap();
    assert!(w.has_writes);
    w.write_all(&bytes[1..]).unwrap();
    w.flush().unwrap();
}

#[test]
fn panic_msg_extracts_strings() {
    let msg = commons::panic_msg(Box::new("boom"));
    assert_eq!(msg, "boom");
    let msg = commons::panic_msg(Box::new("bang".to_string()));
    assert_eq!(msg, "bang");
    let msg = commons::panic_msg(Box::new(42usize));
    assert_eq!(msg, "unknown panic");
}

// ---------- context ----------

#[test]
fn context_validate_ok_for_local_embedded() {
    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::Local {
            base_directory: "/tmp/x".into(),
        },
    );
    ctx.kv.insert(
        "a".into(),
        context::KVStoreContext::Embedded {
            redb_path: "/tmp/x.redb".into(),
        },
    );
    assert!(ctx.validate().is_ok());
}

#[test]
fn context_validate_rejects_mismatched_lengths() {
    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::Local {
            base_directory: "/tmp/x".into(),
        },
    );
    assert!(ctx.validate().is_err());
}

#[test]
fn context_validate_rejects_mismatched_keys() {
    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::Local {
            base_directory: "/tmp/x".into(),
        },
    );
    ctx.kv.insert(
        "b".into(),
        context::KVStoreContext::Embedded {
            redb_path: "/tmp/x.redb".into(),
        },
    );
    assert!(ctx.validate().is_err());
}

#[test]
fn context_validate_rejects_embedded_kv_with_remote_fs() {
    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::S3 {
            access_key_id: "k".into(),
            secret_access_key: "s".into(),
            region: "r".into(),
            endpoint: "e".into(),
            bucket: "b".into(),
        },
    );
    ctx.kv.insert(
        "a".into(),
        context::KVStoreContext::Embedded {
            redb_path: "/tmp/x.redb".into(),
        },
    );
    assert!(ctx.validate().is_err());
}

#[test]
fn context_validate_rejects_relative_paths() {
    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::Local {
            base_directory: "relative/path".into(),
        },
    );
    ctx.kv.insert(
        "a".into(),
        context::KVStoreContext::Embedded {
            redb_path: "/tmp/x.redb".into(),
        },
    );
    assert!(ctx.validate().is_err());

    let mut ctx = VerContext::default();
    ctx.fs.insert(
        "a".into(),
        context::FilesystemContext::Local {
            base_directory: "/tmp/x".into(),
        },
    );
    ctx.kv.insert(
        "a".into(),
        context::KVStoreContext::Embedded {
            redb_path: "relative.redb".into(),
        },
    );
    assert!(ctx.validate().is_err());
}

#[test]
fn context_scaffold_and_get() {
    let mut ctx = VerContext::default();
    ctx.scaffold("new", FSType::Local, KVType::Embedded).unwrap();
    assert!(ctx.fs.contains_key("new"));
    assert!(ctx.kv.contains_key("new"));
    assert!(ctx.get_context("new").is_ok());
    // duplicate scaffold fails
    assert!(ctx.scaffold("new", FSType::Local, KVType::Embedded).is_err());
    // missing context fails
    assert!(ctx.get_context("nope").is_err());
}

#[test]
fn context_switch_and_current() {
    let mut ctx = VerContext::default();
    ctx.scaffold("a", FSType::Local, KVType::Embedded).unwrap();
    ctx.scaffold("b", FSType::Local, KVType::Embedded).unwrap();

    assert!(ctx.current_context().is_none());
    ctx.switch("a").unwrap();
    assert!(ctx.current_context().is_some());
    assert!(ctx.switch("nope").is_err());
}

#[test]
fn context_delete_rules() {
    let mut ctx = VerContext::default();
    ctx.scaffold("a", FSType::Local, KVType::Embedded).unwrap();
    ctx.scaffold("b", FSType::Local, KVType::Embedded).unwrap();
    ctx.switch("a").unwrap();

    // cannot delete the current context
    assert!(ctx.delete("a").is_err());
    ctx.delete("b").unwrap();
    assert!(!ctx.fs.contains_key("b"));
    assert!(!ctx.kv.contains_key("b"));
}

#[test]
fn fstype_kvtype_from_str() {
    assert!(matches!(FSType::from_str("local").unwrap(), FSType::Local));
    assert!(matches!(FSType::from_str("s3").unwrap(), FSType::S3));
    assert!(matches!(FSType::from_str("azure").unwrap(), FSType::Azure));
    assert!(matches!(FSType::from_str("gcs").unwrap(), FSType::GCS));
    assert!(FSType::from_str("nope").is_err());

    assert!(matches!(KVType::from_str("embedded").unwrap(), KVType::Embedded));
    assert!(matches!(KVType::from_str("redis").unwrap(), KVType::Redis));
    assert!(KVType::from_str("nope").is_err());
}

#[test]
fn read_context_roundtrip_through_global_file() {
    let _g = guard();
    let path = global_context_path();

    let mut ctx = VerContext::default();
    ctx.scaffold("rt", FSType::Local, KVType::Embedded).unwrap();
    if let Some(context::FilesystemContext::Local { base_directory }) = ctx.fs.get_mut("rt") {
        *base_directory = "/tmp/rt".into();
    }
    if let Some(context::KVStoreContext::Embedded { redb_path }) = ctx.kv.get_mut("rt") {
        *redb_path = "/tmp/rt.redb".into();
    }
    ctx.switch("rt").unwrap();
    ctx.to_global_context().unwrap();

    assert!(path.exists());
    let loaded = context::read_context().unwrap();
    assert_eq!(loaded.current.as_deref(), Some("rt"));
    assert!(loaded.fs.contains_key("rt"));
}

// ---------- init ----------

#[test]
fn init_context_creates_ver_structure() {
    let _g = guard();
    let repo = setup_repo("init", &[]);

    assert!(repo._dir.path().join(".ver/blobsdb").is_dir());
    assert!(repo._dir.path().join(".ver/snapshots_history").is_dir());
    assert!(repo._kv_dir.path().join("kv.redb").exists());
}

#[test]
fn init_context_unknown_context_errors() {
    let _g = guard();
    setup_repo("init_unknown", &[]);
    assert!(init_context(Some("does-not-exist")).is_err());
}

// ---------- snap / goto ----------

#[test]
fn full_snap_and_goto_roundtrip() {
    let _g = guard();
    let repo = setup_repo("snap_goto", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("hello.txt", "version 1");
    full_snap("first", None, &mut out, &mut err).unwrap();

    repo.write("hello.txt", "version 2");
    full_snap("second", None, &mut out, &mut err).unwrap();

    goto("first", vec![], true, &mut out, &mut err).unwrap();
    assert_eq!(repo.read("hello.txt"), "version 1");

    goto("second", vec![], true, &mut out, &mut err).unwrap();
    assert_eq!(repo.read("hello.txt"), "version 2");
}

#[test]
fn full_snap_outputs_name_and_id() {
    let _g = guard();
    let repo = setup_repo("snap_out", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("named", None, &mut out, &mut err).unwrap();

    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("named"));
    assert!(output.contains("Snapshot"));
}

#[test]
fn goto_deletes_files_not_in_snapshot() {
    let _g = guard();
    let repo = setup_repo("goto_delete", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("old.txt", "old");
    full_snap("with-old", None, &mut out, &mut err).unwrap();

    std::fs::remove_file(repo._dir.path().join("old.txt")).unwrap();
    repo.write("new.txt", "new");
    full_snap("with-new", None, &mut out, &mut err).unwrap();

    goto("with-old", vec![], true, &mut out, &mut err).unwrap();
    assert!(repo.exists("old.txt"));
    assert!(!repo.exists("new.txt"));
}

#[test]
fn goto_same_snapshot_reports_already_there() {
    let _g = guard();
    let repo = setup_repo("goto_same", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("only", None, &mut out, &mut err).unwrap();

    out.clear();
    goto("only", vec![], true, &mut out, &mut err).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Already on"));
}

#[test]
fn goto_unknown_snapshot_errors() {
    let _g = guard();
    let repo = setup_repo("goto_unknown", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("only", None, &mut out, &mut err).unwrap();

    assert!(goto("!!!nope!!!", vec![], true, &mut out, &mut err).is_err());
}

#[test]
fn files_snap_creates_partial_snapshot() {
    let _g = guard();
    let repo = setup_repo("files_snap", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("one.txt", "1");
    repo.write("two.txt", "2");
    files_snap("partial", None, vec!["one.txt".into()], false, &mut out, &mut err).unwrap();

    let kv = repo.kv();
    let id = resolve_snapshot_id("partial", &kv).unwrap();
    let snap = load_snapshot(&kv, id).unwrap();
    assert!(snap.partial);
    assert_eq!(snap.repository_state.len(), 1);
    assert!(snap.repository_state.contains_key("one.txt"));
}

#[test]
fn files_snap_patches_existing_snapshot() {
    let _g = guard();
    let repo = setup_repo("files_snap_patch", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("one.txt", "1");
    full_snap("base", None, &mut out, &mut err).unwrap();

    repo.write("two.txt", "2");
    files_snap("base", None, vec!["two.txt".into()], true, &mut out, &mut err).unwrap();

    let kv = repo.kv();
    let id = resolve_snapshot_id("base", &kv).unwrap();
    let snap = load_snapshot(&kv, id).unwrap();
    assert!(snap.repository_state.contains_key("one.txt"));
    assert!(snap.repository_state.contains_key("two.txt"));
}

#[test]
fn files_snap_missing_file_errors() {
    let _g = guard();
    let repo = setup_repo("files_snap_missing", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("real.txt", "r");
    let res = files_snap("p", None, vec!["ghost.txt".into()], false, &mut out, &mut err);
    assert!(res.is_err());
}

// ---------- log ----------

#[test]
fn log_lists_snapshots() {
    let _g = guard();
    let repo = setup_repo("log", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("first", None, &mut out, &mut err).unwrap();
    full_snap("second", None, &mut out, &mut err).unwrap();

    out.clear();
    log(false, None, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("first"));
    assert!(output.contains("second"));
}

#[test]
fn log_respects_limit() {
    let _g = guard();
    let repo = setup_repo("log_limit", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("one", None, &mut out, &mut err).unwrap();
    full_snap("two", None, &mut out, &mut err).unwrap();

    out.clear();
    log(false, Some(1), &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert_eq!(output.lines().count(), 1);
}

// ---------- show ----------

#[test]
fn to_text_detects_text_and_binary() {
    assert_eq!(to_text(b"hello"), Some("hello"));
    assert_eq!(to_text("unicode 世界".as_bytes()), Some("unicode 世界"));
    assert_eq!(to_text(&[0, 1, 2]), None);
    assert_eq!(to_text(&[0xff, 0xfe]), None);
}

#[test]
fn show_file_outputs_text_content() {
    let _g = guard();
    let repo = setup_repo("show", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("f.txt", "file contents here");
    full_snap("s", None, &mut out, &mut err).unwrap();

    out.clear();
    show_file("f.txt", None, None, None, &mut out).unwrap();
    assert!(String::from_utf8(out).unwrap().contains("file contents here"));
}

#[test]
fn show_file_binary_falls_back_to_hex() {
    let _g = guard();
    let repo = setup_repo("show_bin", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    std::fs::write(repo._dir.path().join("bin.dat"), [0xdeu8, 0xad, 0xbe, 0xef, 0x00]).unwrap();
    full_snap("s", None, &mut out, &mut err).unwrap();

    out.clear();
    show_file("bin.dat", None, None, None, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("de") && output.contains("ad"));
}

#[test]
fn show_file_unknown_path_errors() {
    let _g = guard();
    let repo = setup_repo("show_missing", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("f.txt", "x");
    full_snap("s", None, &mut out, &mut err).unwrap();

    assert!(show_file("nope.txt", None, None, None, &mut out).is_err());
}

// ---------- diff ----------

#[test]
fn diff_shows_changes_between_snapshots() {
    let _g = guard();
    let repo = setup_repo("diff", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("f.txt", "line one\n");
    full_snap("before", None, &mut out, &mut err).unwrap();

    repo.write("f.txt", "line two\n");
    full_snap("after", None, &mut out, &mut err).unwrap();

    out.clear();
    diff("f.txt", None, "before".into(), &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("-line one"));
    assert!(output.contains("+line two"));
}

#[test]
fn diff_reports_file_only_in_base() {
    let _g = guard();
    let repo = setup_repo("diff_base_only", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("gone.txt", "x");
    full_snap("before", None, &mut out, &mut err).unwrap();

    std::fs::remove_file(repo._dir.path().join("gone.txt")).unwrap();
    full_snap("after", None, &mut out, &mut err).unwrap();

    out.clear();
    diff("gone.txt", None, "before".into(), &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("does not exist in head snapshot"));
}

#[test]
fn diff_reports_file_only_in_head() {
    let _g = guard();
    let repo = setup_repo("diff_head_only", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    repo.write("a.txt", "a");
    full_snap("before", None, &mut out, &mut err).unwrap();

    repo.write("new.txt", "new");
    full_snap("after", None, &mut out, &mut err).unwrap();

    out.clear();
    diff("new.txt", None, "before".into(), &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("does not exist in base snapshot"));
}

// ---------- sync ----------

#[test]
fn get_parents_returns_parent_ids() {
    let _g = guard();
    let repo = setup_repo("parents", &[]);
    let parent = blake3::hash(b"parent").as_bytes().to_owned();
    let snap = make_snapshot(&[], vec![parent]);

    let kv = repo.kv();
    let id = store_snapshot(&snap, "child", &kv).unwrap();

    let parents = get_parents(&kv, id.as_bytes().to_owned()).unwrap();
    assert_eq!(parents, vec![parent]);
}

#[test]
fn find_common_ancestor_same_snapshot() {
    let _g = guard();
    let repo = setup_repo("ancestor_same", &[]);
    let id = [1u8; 32];

    let kv = repo.kv();
    let (base, da, db) = find_common_ancestor(&kv, &kv, id, id).unwrap();
    assert_eq!(base, Some(id));
    assert!(da.is_empty());
    assert!(db.is_empty());
}

#[test]
fn find_common_ancestor_linear_history() {
    let _g = guard();
    let repo = setup_repo("ancestor_linear", &[]);
    let kv = repo.kv();

    let snap_a = make_snapshot(&[], vec![]);
    let id_a = store_snapshot(&snap_a, "a", &kv).unwrap().as_bytes().to_owned();
    let snap_b = make_snapshot(&[], vec![id_a]);
    let id_b = store_snapshot(&snap_b, "b", &kv).unwrap().as_bytes().to_owned();
    let snap_c = make_snapshot(&[], vec![id_b]);
    let id_c = store_snapshot(&snap_c, "c", &kv).unwrap().as_bytes().to_owned();

    let (base, _, _) = find_common_ancestor(&kv, &kv, id_c, id_a).unwrap();
    assert_eq!(base, Some(id_a));
}

#[test]
fn find_common_ancestor_diverged_histories() {
    let _g = guard();
    let repo = setup_repo("ancestor_diverged", &[]);
    let kv = repo.kv();

    let root = make_snapshot(&[], vec![]);
    let id_root = store_snapshot(&root, "root", &kv).unwrap().as_bytes().to_owned();
    let left = make_snapshot(&[], vec![id_root]);
    let id_left = store_snapshot(&left, "left", &kv).unwrap().as_bytes().to_owned();
    let right = make_snapshot(&[], vec![id_root]);
    let id_right = store_snapshot(&right, "right", &kv).unwrap().as_bytes().to_owned();

    let (base, da, db) = find_common_ancestor(&kv, &kv, id_left, id_right).unwrap();
    assert_eq!(base, Some(id_root));
    assert!(da.contains(&id_left));
    assert!(db.contains(&id_right));
}

#[test]
fn find_common_ancestor_unrelated_histories() {
    let _g = guard();
    let repo = setup_repo("ancestor_unrelated", &[]);
    let kv = repo.kv();

    let a = make_snapshot(&[], vec![]);
    let id_a = store_snapshot(&a, "a", &kv).unwrap().as_bytes().to_owned();
    let b = make_snapshot(&[], vec![]);
    let id_b = store_snapshot(&b, "b", &kv).unwrap().as_bytes().to_owned();

    let (base, _, _) = find_common_ancestor(&kv, &kv, id_a, id_b).unwrap();
    assert!(base.is_none());
}

#[test]
fn copy_snapshot_moves_snapshot_and_blobs() {
    let _g = guard();
    let src = setup_repo("copy_src", &[]);
    let dst = setup_repo("copy_dst", &[&src]);

    src.write("f.txt", "copy me");
    let db_src = BlobsDBWithFs::new(src.fs.clone());
    let chunks = rt().block_on(async { db_src.put("f.txt").await.unwrap() });
    let snap = Snapshot {
        repository_state: HashMap::from([("f.txt".to_string(), chunks)]),
        parent_ids: vec![],
        partial: false,
    };

    let src_kv = src.kv();
    let dst_kv = dst.kv();
    let id = store_snapshot(&snap, "s", &src_kv).unwrap().as_bytes().to_owned();

    copy_snapshot(id, &src_kv, &dst_kv, &src.fs, &dst.fs).unwrap();

    let loaded = load_snapshot(&dst_kv, id).unwrap();
    assert!(loaded.repository_state.contains_key("f.txt"));

    // blobs must be retrievable from the destination store
    let db_dst = BlobsDBWithFs::new(dst.fs.clone());
    let content = rt().block_on(async {
        db_dst
            .get(Arc::new(loaded.repository_state["f.txt"].clone()))
            .await
            .unwrap()
    });
    assert_eq!(content, b"copy me");
}

#[test]
fn sync_from_scratch_copies_everything() {
    let _g = guard();
    let src = setup_repo("sync_src", &[]);
    let dst = setup_repo("sync_dst", &[&src]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("sync_src");
    src.write("a.txt", "alpha");
    src.write("b.txt", "beta");
    full_snap("s1", None, &mut out, &mut err).unwrap();

    sync(None, "sync_dst".into(), true, &mut out).unwrap();

    assert_eq!(dst.read("a.txt"), "alpha");
    assert_eq!(dst.read("b.txt"), "beta");
    assert_eq!(get_head(&dst.fs).unwrap(), get_head(&src.fs).unwrap());
    assert!(resolve_snapshot_id("s1", &dst.kv()).is_ok());
}

#[test]
fn sync_delta_copies_new_snapshots() {
    let _g = guard();
    let src = setup_repo("syncd_src", &[]);
    let dst = setup_repo("syncd_dst", &[&src]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("syncd_src");
    src.write("a.txt", "v1");
    full_snap("s1", None, &mut out, &mut err).unwrap();
    sync(None, "syncd_dst".into(), true, &mut out).unwrap();

    src.write("a.txt", "v2");
    full_snap("s2", None, &mut out, &mut err).unwrap();
    sync(None, "syncd_dst".into(), false, &mut out).unwrap();

    assert_eq!(dst.read("a.txt"), "v2");
    assert!(resolve_snapshot_id("s2", &dst.kv()).is_ok());
    assert_eq!(get_head(&dst.fs).unwrap(), get_head(&src.fs).unwrap());
}

#[test]
fn sync_already_synced_reports() {
    let _g = guard();
    let src = setup_repo("syncs_src", &[]);
    let _dst = setup_repo("syncs_dst", &[&src]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("syncs_src");
    src.write("a.txt", "v1");
    full_snap("s1", None, &mut out, &mut err).unwrap();
    sync(None, "syncs_dst".into(), true, &mut out).unwrap();

    out.clear();
    sync(None, "syncs_dst".into(), false, &mut out).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("already synced"));
}

#[test]
fn sync_without_source_head_errors() {
    let _g = guard();
    let _src = setup_repo("synce_src", &[]);
    let _dst = setup_repo("synce_dst", &[&_src]);
    let mut out = Vec::new();

    assert!(sync(None, "synce_dst".into(), true, &mut out).is_err());
}

// ---------- merge ----------

#[test]
fn merge_clean_run_combines_changes() {
    let _g = guard();
    let a = setup_repo("merge_a", &[]);
    let b = setup_repo("merge_b", &[&a]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    // common base in a, synced to b
    switch_to("merge_a");
    a.write("shared.txt", "shared");
    full_snap("base", None, &mut out, &mut err).unwrap();
    sync(None, "merge_b".into(), true, &mut out).unwrap();

    // diverge: a touches a.txt, b touches b.txt
    a.write("a.txt", "from a");
    full_snap("a-change", None, &mut out, &mut err).unwrap();

    switch_to("merge_b");
    b.write("b.txt", "from b");
    full_snap("b-change", None, &mut out, &mut err).unwrap();

    // merge b into a (a is "ours")
    switch_to("merge_a");

    out.clear();
    merge_run("merge_b".into(), "m1", None, None, &mut out, &mut err).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Merge successfully completed"), "{}", output);

    // merged head contains both files
    let head = get_head(&a.fs).unwrap().unwrap();
    let snap = load_snapshot(&a.kv(), head).unwrap();
    assert!(snap.repository_state.contains_key("a.txt"));
    assert!(snap.repository_state.contains_key("b.txt"));
    assert!(snap.repository_state.contains_key("shared.txt"));
    assert_eq!(snap.parent_ids.len(), 2);

    // working tree updated
    assert_eq!(a.read("a.txt"), "from a");
    assert_eq!(a.read("b.txt"), "from b");
}

#[test]
fn merge_conflicting_run_stores_partial_merge() {
    let _g = guard();
    let a = setup_repo("mconf_a", &[]);
    let b = setup_repo("mconf_b", &[&a]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("mconf_a");
    a.write("conflict.txt", "base\n");
    full_snap("base", None, &mut out, &mut err).unwrap();
    sync(None, "mconf_b".into(), true, &mut out).unwrap();

    a.write("conflict.txt", "ours\n");
    full_snap("a-change", None, &mut out, &mut err).unwrap();

    switch_to("mconf_b");
    b.write("conflict.txt", "theirs\n");
    full_snap("b-change", None, &mut out, &mut err).unwrap();

    switch_to("mconf_a");

    out.clear();
    err.clear();
    merge_run("mconf_b".into(), "m1", None, None, &mut out, &mut err).unwrap();
    let e = String::from_utf8(err.clone()).unwrap();
    assert!(e.contains("conflicts"), "{}", e);

    // partial merge persisted
    assert!(a.kv().get_merge("m1").unwrap().is_some());

    // pick ours, then finalize
    merge_pick("m1", &["conflict.txt"], &[], &mut out, &mut err).unwrap();
    out.clear();
    merge_end("m1", &mut out, &mut err).unwrap();
    let output = String::from_utf8(out).unwrap();
    assert!(output.contains("Merge successfully completed"), "{}", output);

    // partial merge cleaned up, content is ours
    assert!(a.kv().get_merge("m1").unwrap().is_none());
    assert_eq!(a.read("conflict.txt"), "ours\n");
}

#[test]
fn merge_pick_theirs_uses_incoming_content() {
    let _g = guard();
    let a = setup_repo("mpick_a", &[]);
    let b = setup_repo("mpick_b", &[&a]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("mpick_a");
    a.write("c.txt", "base\n");
    full_snap("base", None, &mut out, &mut err).unwrap();
    sync(None, "mpick_b".into(), true, &mut out).unwrap();

    a.write("c.txt", "ours\n");
    full_snap("a-change", None, &mut out, &mut err).unwrap();

    switch_to("mpick_b");
    b.write("c.txt", "theirs\n");
    full_snap("b-change", None, &mut out, &mut err).unwrap();

    switch_to("mpick_a");

    merge_run("mpick_b".into(), "m1", None, None, &mut out, &mut err).unwrap();
    merge_pick("m1", &[], &["c.txt"], &mut out, &mut err).unwrap();
    merge_end("m1", &mut out, &mut err).unwrap();

    assert_eq!(a.read("c.txt"), "theirs\n");
}

#[test]
fn merge_end_with_pending_conflicts_refuses() {
    let _g = guard();
    let a = setup_repo("mpend_a", &[]);
    let b = setup_repo("mpend_b", &[&a]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    switch_to("mpend_a");
    a.write("c.txt", "base\n");
    full_snap("base", None, &mut out, &mut err).unwrap();
    sync(None, "mpend_b".into(), true, &mut out).unwrap();

    a.write("c.txt", "ours\n");
    full_snap("a-change", None, &mut out, &mut err).unwrap();

    switch_to("mpend_b");
    b.write("c.txt", "theirs\n");
    full_snap("b-change", None, &mut out, &mut err).unwrap();

    switch_to("mpend_a");

    merge_run("mpend_b".into(), "m1", None, None, &mut out, &mut err).unwrap();

    err.clear();
    merge_end("m1", &mut out, &mut err).unwrap();
    let e = String::from_utf8(err).unwrap();
    assert!(e.contains("still need to resolve"), "{}", e);
    // merge state still present
    assert!(a.kv().get_merge("m1").unwrap().is_some());
}

#[test]
fn merge_pick_unknown_merge_errors() {
    let _g = guard();
    let _repo = setup_repo("mpick_unknown", &[]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    assert!(merge_pick("nope", &["x"], &[], &mut out, &mut err).is_err());
}

#[test]
fn merge_without_common_base_errors() {
    let _g = guard();
    let a = setup_repo("mnobase_a", &[]);
    let b = setup_repo("mnobase_b", &[&a]);
    let mut out = Vec::new();
    let mut err = Vec::new();

    // independent histories
    switch_to("mnobase_a");
    a.write("a.txt", "a");
    full_snap("a-snap", None, &mut out, &mut err).unwrap();

    switch_to("mnobase_b");
    b.write("b.txt", "b");
    full_snap("b-snap", None, &mut out, &mut err).unwrap();

    switch_to("mnobase_a");

    assert!(merge_run("mnobase_b".into(), "m1", None, None, &mut out, &mut err).is_err());
}
