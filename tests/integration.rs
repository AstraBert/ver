use serial_test::serial;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use tempdir::TempDir;
use ver::init::{SNAPSHOT_HISTORY_FILE, VER_DIRECTORY, initialize};

/// RAII guard that restores the original working directory on drop.
struct CwdGuard {
    original: PathBuf,
}

impl CwdGuard {
    fn change_to(dir: &Path) -> Self {
        let original = env::current_dir().unwrap();
        env::set_current_dir(dir).unwrap();
        Self { original }
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.original).unwrap();
    }
}

fn setup_repo() -> (TempDir, CwdGuard) {
    let tmp = TempDir::new("ver_test").unwrap();
    initialize(Some(tmp.path().to_str().unwrap())).unwrap();
    let guard = CwdGuard::change_to(tmp.path());
    (tmp, guard)
}

// ---------- init ----------

#[test]
#[serial]
fn init_creates_ver_directory_structure() {
    let tmp = TempDir::new("ver_test").unwrap();
    initialize(Some(tmp.path().to_str().unwrap())).unwrap();

    let ver_dir = tmp.path().join(VER_DIRECTORY);
    assert!(ver_dir.exists());
    assert!(ver_dir.join(SNAPSHOT_HISTORY_FILE).exists());
    assert!(ver_dir.join("files.redb").exists());
    assert!(ver_dir.join("blobs.db").exists());
}

// ---------- snap & goto ----------

#[test]
#[serial]
fn snap_and_goto_roundtrip_restores_file_content() {
    let (_tmp, _guard) = setup_repo();

    fs::write("hello.txt", "version 1").unwrap();
    ver::files::snap("first", None).unwrap();

    fs::write("hello.txt", "version 2").unwrap();
    ver::files::snap("second", None).unwrap();

    ver::files::goto::<String>("first", None).unwrap();
    assert_eq!(fs::read_to_string("hello.txt").unwrap(), "version 1");

    ver::files::goto::<String>("second", None).unwrap();
    assert_eq!(fs::read_to_string("hello.txt").unwrap(), "version 2");
}

#[test]
#[serial]
fn goto_by_snapshot_id_hash_works() {
    let (_tmp, _guard) = setup_repo();

    fs::write("a.txt", "alpha").unwrap();
    ver::files::snap("named", None).unwrap();

    // Extract the snapshot hash from the history file
    let history = fs::read_to_string(
        Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE),
    )
    .unwrap();
    let hash = history
        .split('(')
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap()
        .to_string();

    fs::write("a.txt", "changed").unwrap();
    ver::files::goto::<String>(&hash, None).unwrap();
    assert_eq!(fs::read_to_string("a.txt").unwrap(), "alpha");
}

#[test]
#[serial]
fn goto_removes_files_not_in_snapshot() {
    let (_tmp, _guard) = setup_repo();

    fs::write("keep.txt", "keep me").unwrap();
    ver::files::snap("base", None).unwrap();

    fs::write("extra.txt", "should be deleted").unwrap();
    ver::files::goto::<String>("base", None).unwrap();

    assert!(!Path::new("extra.txt").exists());
    assert_eq!(fs::read_to_string("keep.txt").unwrap(), "keep me");
}

#[test]
#[serial]
fn goto_with_specific_paths_only_reverts_those() {
    let (_tmp, _guard) = setup_repo();

    fs::write("one.txt", "one-v1").unwrap();
    fs::write("two.txt", "two-v1").unwrap();
    ver::files::snap("v1", None).unwrap();

    fs::write("one.txt", "one-v2").unwrap();
    fs::write("two.txt", "two-v2").unwrap();

    ver::files::goto("v1", Some(vec!["one.txt".to_string()])).unwrap();

    assert_eq!(fs::read_to_string("one.txt").unwrap(), "one-v1");
    assert_eq!(fs::read_to_string("two.txt").unwrap(), "two-v2");
}

#[test]
#[serial]
fn goto_nonexistent_snapshot_returns_error() {
    let (_tmp, _guard) = setup_repo();
    let result = ver::files::goto::<String>("nonexistent", None);
    assert!(result.is_err());
}

// ---------- .verignore ----------

#[test]
#[serial]
fn verignore_excludes_files_from_snapshot() {
    let (_tmp, _guard) = setup_repo();

    fs::write(".verignore", "ignored.txt").unwrap();
    fs::write("ignored.txt", "secret").unwrap();
    fs::write("tracked.txt", "tracked").unwrap();

    ver::files::snap("s1", None).unwrap();

    // Modify both, then revert — ignored.txt should keep its new content
    fs::write("ignored.txt", "modified-secret").unwrap();
    fs::write("tracked.txt", "modified-tracked").unwrap();

    ver::files::goto::<String>("s1", None).unwrap();

    // ignored.txt was never snapshotted and is excluded by .verignore,
    // so goto leaves it untouched
    assert_eq!(fs::read_to_string("ignored.txt").unwrap(), "modified-secret");
    // tracked.txt was snapshotted, so it gets restored
    assert_eq!(fs::read_to_string("tracked.txt").unwrap(), "tracked");
}

// ---------- named snapshots ----------

#[test]
#[serial]
fn snapshot_names_resolve_to_correct_ids() {
    let (_tmp, _guard) = setup_repo();

    fs::write("f.txt", "content-a").unwrap();
    ver::files::snap("alpha", None).unwrap();

    fs::write("f.txt", "content-b").unwrap();
    ver::files::snap("beta", None).unwrap();

    ver::files::goto::<String>("alpha", None).unwrap();
    assert_eq!(fs::read_to_string("f.txt").unwrap(), "content-a");

    ver::files::goto::<String>("beta", None).unwrap();
    assert_eq!(fs::read_to_string("f.txt").unwrap(), "content-b");
}

// ---------- log ----------

#[test]
#[serial]
fn log_output_contains_snapshot_names() {
    let (_tmp, _guard) = setup_repo();

    fs::write("x.txt", "1").unwrap();
    ver::files::snap("checkpoint-1", Some("first snap")).unwrap();

    fs::write("x.txt", "2").unwrap();
    ver::files::snap("checkpoint-2", None).unwrap();

    let history = fs::read_to_string(
        Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE),
    )
    .unwrap();

    assert!(history.contains("checkpoint-1"));
    assert!(history.contains("checkpoint-2"));
    assert!(history.contains("first snap"));
}

// ---------- resolve_ver_dir_path ----------

#[test]
#[serial]
fn resolve_ver_dir_finds_ver_in_ancestor() {
    let (_tmp, _guard) = setup_repo();

    let sub = Path::new("sub/dir");
    fs::create_dir_all(sub).unwrap();
    let resolved = ver::commons::resolve_ver_dir_path(
        env::current_dir().unwrap().join(sub),
    )
    .unwrap();

    assert!(resolved.ends_with(VER_DIRECTORY));
}

#[test]
#[serial]
fn resolve_ver_dir_errors_when_no_ver_exists() {
    let tmp = TempDir::new("ver_test_no_repo").unwrap();
    let result = ver::commons::resolve_ver_dir_path(tmp.path().to_path_buf());
    assert!(result.is_err());
}
