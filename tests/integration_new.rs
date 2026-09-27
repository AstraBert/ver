use serial_test::serial;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use tempdir::TempDir;
use ver::init::{HEAD_PATH, REDB_PATH, SNAPSHOT_HISTORY_FILE, VER_DIRECTORY, initialize};

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
        let _ = env::set_current_dir(&self.original);
    }
}

fn setup_repo() -> (TempDir, CwdGuard) {
    let tmp = TempDir::new("ver_test").unwrap();
    initialize(Some(tmp.path().to_str().unwrap())).unwrap();
    let guard = CwdGuard::change_to(tmp.path());
    (tmp, guard)
}

fn setup_repo_with_snapshot() -> (TempDir, CwdGuard) {
    let (tmp, guard) = setup_repo();
    fs::write("test.txt", "initial content").unwrap();
    ver::files::snap("initial", Some("first snapshot")).unwrap();
    (tmp, guard)
}

// ---------- cp command ----------

#[test]
#[serial]
fn cp_creates_destination_directory_if_missing() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let dest_path = dest.path().join("new_subdir");

    ver::cp::copy("initial", dest_path.to_str().unwrap()).unwrap();

    assert!(dest_path.exists());
    assert!(dest_path.join(VER_DIRECTORY).exists());
}

#[test]
#[serial]
fn cp_copies_snapshot_files_to_destination() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    fs::write("extra.txt", "extra content").unwrap();
    ver::files::snap("second", None).unwrap();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let dest_path = dest.path().join("copied");

    ver::cp::copy("initial", dest_path.to_str().unwrap()).unwrap();

    // Should only contain files from "initial" snapshot
    assert!(dest_path.join("test.txt").exists());
    assert!(!dest_path.join("extra.txt").exists());
    assert_eq!(
        fs::read_to_string(dest_path.join("test.txt")).unwrap(),
        "initial content"
    );
}

#[test]
#[serial]
fn cp_copies_ver_directory() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let dest_path = dest.path().join("copied");

    ver::cp::copy("initial", dest_path.to_str().unwrap()).unwrap();

    assert!(dest_path.join(VER_DIRECTORY).exists());
    assert!(dest_path.join(VER_DIRECTORY).join(REDB_PATH).exists());
    assert!(dest_path.join(VER_DIRECTORY).join("blobs.db").exists());
    assert!(
        dest_path
            .join(VER_DIRECTORY)
            .join(SNAPSHOT_HISTORY_FILE)
            .exists()
    );
    assert!(dest_path.join(VER_DIRECTORY).join(HEAD_PATH).exists());
}

#[test]
#[serial]
fn cp_with_snapshot_id_hash_works() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    let hash = history
        .split('(')
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap()
        .to_string();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let dest_path = dest.path().join("copied");

    ver::cp::copy(&hash, dest_path.to_str().unwrap()).unwrap();

    assert!(dest_path.join("test.txt").exists());
}

#[test]
#[serial]
fn cp_nonexistent_snapshot_returns_error() {
    let (_tmp, _guard) = setup_repo();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let result = ver::cp::copy("nonexistent", dest.path().to_str().unwrap());

    assert!(result.is_err());
}

#[test]
#[serial]
fn cp_with_nested_file_structure() {
    let (_tmp, _guard) = setup_repo();

    fs::create_dir_all("src/nested").unwrap();
    fs::write("src/main.rs", "fn main() {}").unwrap();
    fs::write("src/nested/lib.rs", "pub fn lib() {}").unwrap();
    fs::write("README.md", "# Test").unwrap();

    ver::files::snap("structured", None).unwrap();

    let dest = TempDir::new("ver_cp_dest").unwrap();
    let dest_path = dest.path().join("copied");

    ver::cp::copy("structured", dest_path.to_str().unwrap()).unwrap();

    assert!(dest_path.join("src/main.rs").exists());
    assert!(dest_path.join("src/nested/lib.rs").exists());
    assert!(dest_path.join("README.md").exists());
}

// ---------- show command ----------

#[test]
#[serial]
fn show_displays_file_content_from_head() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let mut output = Vec::new();
    let result = ver::show::show_file("test.txt", None, None, None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    assert!(content.contains("initial content"));
}

#[test]
#[serial]
fn show_with_specific_snapshot_name() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    fs::write("test.txt", "modified content").unwrap();
    ver::files::snap("modified", None).unwrap();

    // Show from the first snapshot
    let mut output = Vec::new();
    let result = ver::show::show_file(
        "test.txt",
        Some("initial".to_string()),
        None,
        None,
        &mut output,
    );
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    assert!(content.contains("initial content"));
    assert!(!content.contains("modified content"));
}

#[test]
#[serial]
fn show_with_snapshot_id_hash() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    let hash = history
        .split('(')
        .nth(1)
        .unwrap()
        .split(')')
        .next()
        .unwrap()
        .to_string();

    let mut output = Vec::new();
    let result = ver::show::show_file("test.txt", Some(hash), None, None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    assert!(content.contains("initial content"));
}

#[test]
#[serial]
fn show_with_offset_and_limit() {
    let (_tmp, _guard) = setup_repo();

    fs::write("long.txt", "0123456789abcdefghijklmnopqrstuvwxyz").unwrap();
    ver::files::snap("long", None).unwrap();

    // Show bytes 10-20
    let mut output = Vec::new();
    let result = ver::show::show_file("long.txt", None, Some(10), Some(20), &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    assert_eq!(content.trim(), "abcdefghij");
}

#[test]
#[serial]
fn show_nonexistent_file_returns_error() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let mut output = Vec::new();
    let result = ver::show::show_file("nonexistent.txt", None, None, None, &mut output);
    assert!(result.is_err());
}

#[test]
#[serial]
fn show_nonexistent_snapshot_returns_error() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let mut output = Vec::new();
    let result =
        ver::show::show_file("test.txt", Some("nonexistent".to_string()), None, None, &mut output);
    assert!(result.is_err());
}

#[test]
#[serial]
fn show_without_snapshots_returns_error() {
    let (_tmp, _guard) = setup_repo();

    let mut output = Vec::new();
    let result = ver::show::show_file("test.txt", None, None, None, &mut output);
    assert!(result.is_err());
}

#[test]
#[serial]
fn show_with_binary_file_hexdumps() {
    let (_tmp, _guard) = setup_repo();

    let binary_content: Vec<u8> = vec![0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD];
    fs::write("binary.bin", &binary_content).unwrap();
    ver::files::snap("binary", None).unwrap();

    let mut output = Vec::new();
    let result = ver::show::show_file("binary.bin", None, None, None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    // Should contain hex representation
    assert!(content.contains("00") || content.contains("ff"));
}

// ---------- log command ----------

#[test]
#[serial]
fn log_with_head_limits_output() {
    let (_tmp, _guard) = setup_repo();

    for i in 0..5 {
        fs::write("file.txt", format!("content {}", i)).unwrap();
        ver::files::snap(&format!("snap{}", i), None).unwrap();
    }

    let mut output = Vec::new();
    let result = ver::log::log(Some(3), &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    // Should only show last 3 snapshots (in reverse order)
    assert_eq!(lines.len(), 3);
    assert!(content.contains("snap4"));
    assert!(content.contains("snap3"));
    assert!(content.contains("snap2"));
    assert!(!content.contains("snap1"));
    assert!(!content.contains("snap0"));
}

#[test]
#[serial]
fn log_without_head_shows_all() {
    let (_tmp, _guard) = setup_repo();

    for i in 0..3 {
        fs::write("file.txt", format!("content {}", i)).unwrap();
        ver::files::snap(&format!("snap{}", i), None).unwrap();
    }

    let mut output = Vec::new();
    let result = ver::log::log(None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    // Should show all 3 snapshots
    assert_eq!(lines.len(), 3);
    assert!(content.contains("snap0"));
    assert!(content.contains("snap1"));
    assert!(content.contains("snap2"));
}

#[test]
#[serial]
fn log_empty_history_works() {
    let (_tmp, _guard) = setup_repo();

    let mut output = Vec::new();
    let result = ver::log::log(None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    // Empty history should produce empty or minimal output
    assert!(content.trim().is_empty() || content.lines().count() <= 1);
}

#[test]
#[serial]
fn log_output_in_reverse_chronological_order() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "first").unwrap();
    ver::files::snap("first_snap", None).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(10));

    fs::write("file.txt", "second").unwrap();
    ver::files::snap("second_snap", None).unwrap();

    let mut output = Vec::new();
    let result = ver::log::log(None, &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = content.lines().collect();

    // Most recent should be first
    assert!(lines[0].contains("second_snap"));
    assert!(lines[1].contains("first_snap"));
}

#[test]
#[serial]
fn log_with_head_larger_than_history() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();
    ver::files::snap("only_snap", None).unwrap();

    let mut output = Vec::new();
    let result = ver::log::log(Some(100), &mut output);
    assert!(result.is_ok());

    let content = String::from_utf8(output).unwrap();
    // Should show all available snapshots
    assert!(content.contains("only_snap"));
}

#[test]
#[serial]
fn log_with_head_zero() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();
    ver::files::snap("test_snap", None).unwrap();

    let mut output = Vec::new();
    let result = ver::log::log(Some(0), &mut output);
    assert!(result.is_ok());

    // head=0 should show no snapshots or handle gracefully
    let content = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = content.lines().filter(|l| !l.is_empty()).collect();
    assert!(lines.is_empty() || lines.len() <= 1);
}

// ---------- named module ----------

#[test]
#[serial]
fn name_snapshot_and_get_snapshot_by_name_roundtrip() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();
    ver::files::snap("test_snapshot", None).unwrap();

    let redb = redb::Database::open(Path::new(VER_DIRECTORY).join(REDB_PATH)).unwrap();

    let hash = ver::named::get_snapshot_by_name(&redb, "test_snapshot").unwrap();
    assert_eq!(hash.len(), 32);
}

#[test]
#[serial]
fn get_snapshot_by_name_nonexistent_returns_error() {
    let (_tmp, _guard) = setup_repo();

    let redb = redb::Database::open(Path::new(VER_DIRECTORY).join(REDB_PATH)).unwrap();

    let result = ver::named::get_snapshot_by_name(&redb, "nonexistent");
    assert!(result.is_err());
}

#[test]
#[serial]
fn multiple_names_for_different_snapshots() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "v1").unwrap();
    ver::files::snap("first", None).unwrap();

    fs::write("file.txt", "v2").unwrap();
    ver::files::snap("second", None).unwrap();

    let redb = redb::Database::open(Path::new(VER_DIRECTORY).join(REDB_PATH)).unwrap();

    let hash1 = ver::named::get_snapshot_by_name(&redb, "first").unwrap();
    let hash2 = ver::named::get_snapshot_by_name(&redb, "second").unwrap();

    assert_ne!(hash1, hash2);
}

// ---------- commons module ----------

#[test]
#[serial]
fn get_head_returns_none_when_empty() {
    let (_tmp, _guard) = setup_repo();

    let resolved = ver::commons::resolve_ver_dir_path(env::current_dir().unwrap()).unwrap();
    let head = ver::commons::get_head(&resolved).unwrap();

    assert!(head.is_none());
}

#[test]
#[serial]
fn get_head_returns_some_after_snapshot() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let resolved = ver::commons::resolve_ver_dir_path(env::current_dir().unwrap()).unwrap();
    let head = ver::commons::get_head(&resolved).unwrap();

    assert!(head.is_some());
    assert_eq!(head.unwrap().len(), 32);
}

#[test]
#[serial]
fn write_head_and_get_head_roundtrip() {
    let (_tmp, _guard) = setup_repo();

    let resolved = ver::commons::resolve_ver_dir_path(env::current_dir().unwrap()).unwrap();

    let test_hash = blake3::hash(b"test content");
    ver::commons::write_head(&resolved, &test_hash).unwrap();

    let head = ver::commons::get_head(&resolved).unwrap();
    assert!(head.is_some());
    assert_eq!(head.unwrap(), test_hash.as_bytes().to_owned());
}

#[test]
#[serial]
fn to_text_with_valid_utf8() {
    let content = b"Hello, world!";
    let result = ver::commons::to_text(content);

    assert!(result.is_some());
    assert_eq!(result.unwrap(), "Hello, world!");
}

#[test]
#[serial]
fn to_text_with_invalid_utf8() {
    let content = &[0xFF, 0xFE, 0x00, 0x01];
    let result = ver::commons::to_text(content);

    assert!(result.is_none());
}

#[test]
#[serial]
fn to_text_with_null_bytes() {
    let content = b"Hello\0World";
    let result = ver::commons::to_text(content);

    assert!(result.is_none());
}

#[test]
#[serial]
fn to_text_with_long_content() {
    // Content longer than 8192 bytes should still work if valid UTF-8
    let content = "a".repeat(10000);
    let result = ver::commons::to_text(content.as_bytes());

    assert!(result.is_some());
}

// ---------- head file ----------

#[test]
#[serial]
fn head_file_is_created_on_init() {
    let tmp = TempDir::new("ver_test").unwrap();
    initialize(Some(tmp.path().to_str().unwrap())).unwrap();

    let head_path = tmp.path().join(VER_DIRECTORY).join(HEAD_PATH);
    assert!(head_path.exists());
}

#[test]
#[serial]
fn head_file_is_updated_on_snapshot() {
    let (_tmp, _guard) = setup_repo();

    let head_path = Path::new(VER_DIRECTORY).join(HEAD_PATH);
    let initial_content = fs::read_to_string(&head_path).unwrap();
    assert!(initial_content.is_empty());

    fs::write("test.txt", "content").unwrap();
    ver::files::snap("test", None).unwrap();

    let updated_content = fs::read_to_string(&head_path).unwrap();
    assert!(!updated_content.is_empty());
    assert_eq!(updated_content.len(), 64); // blake3 hash hex length
}

// ---------- merge commands ----------

/// Helper to create two repos with a common base snapshot
fn setup_two_repos_with_common_base() -> (TempDir, TempDir, CwdGuard) {
    // Create first repo with a snapshot
    let tmp_a = TempDir::new("ver_repo_a").unwrap();
    initialize(Some(tmp_a.path().to_str().unwrap())).unwrap();

    let guard = CwdGuard::change_to(tmp_a.path());
    fs::write("shared.txt", "base content").unwrap();
    fs::write("only_a.txt", "a content").unwrap();
    ver::files::snap("base", None).unwrap();

    // Copy to create second repo
    let tmp_b = TempDir::new("ver_repo_b").unwrap();
    let dest_b = tmp_b.path().join("repo");
    fs::create_dir_all(&dest_b).unwrap();

    ver::cp::copy("base", dest_b.to_str().unwrap()).unwrap();

    (tmp_a, tmp_b, guard)
}

/// Helper to create two repos with conflicting modifications
fn setup_repos_with_modification_conflict() -> (TempDir, TempDir, CwdGuard) {
    let (tmp_a, tmp_b, guard) = setup_two_repos_with_common_base();
    let dest_b = tmp_b.path().join("repo");

    // Modify shared.txt differently in both repos
    fs::write("shared.txt", "modified by repo A").unwrap();
    ver::files::snap("a_change", None).unwrap();

    env::set_current_dir(&dest_b).unwrap();
    fs::write("shared.txt", "modified by repo B").unwrap();
    ver::files::snap("b_change", None).unwrap();

    env::set_current_dir(tmp_a.path()).unwrap();

    (tmp_a, tmp_b, guard)
}

/// Helper to create two repos with addition conflicts (both added same file differently)
fn setup_repos_with_addition_conflict() -> (TempDir, TempDir, CwdGuard) {
    let (tmp_a, tmp_b, guard) = setup_two_repos_with_common_base();
    let dest_b = tmp_b.path().join("repo");

    // Both repos add the same file with different content
    fs::write("new_file.txt", "added by A").unwrap();
    ver::files::snap("a_add", None).unwrap();

    env::set_current_dir(&dest_b).unwrap();
    fs::write("new_file.txt", "added by B").unwrap();
    ver::files::snap("b_add", None).unwrap();

    env::set_current_dir(tmp_a.path()).unwrap();

    (tmp_a, tmp_b, guard)
}

/// Helper to create two repos with deletion conflicts
fn setup_repos_with_deletion_conflict() -> (TempDir, TempDir, CwdGuard) {
    let tmp_a = TempDir::new("ver_repo_a").unwrap();
    initialize(Some(tmp_a.path().to_str().unwrap())).unwrap();

    let guard = CwdGuard::change_to(tmp_a.path());
    fs::write("to_delete.txt", "will be deleted").unwrap();
    fs::write("to_modify.txt", "will be modified").unwrap();
    ver::files::snap("base", None).unwrap();

    let tmp_b = TempDir::new("ver_repo_b").unwrap();
    let dest_b = tmp_b.path().join("repo");
    fs::create_dir_all(&dest_b).unwrap();

    ver::cp::copy("base", dest_b.to_str().unwrap()).unwrap();

    // A deletes the file, B modifies it
    fs::remove_file("to_delete.txt").unwrap();
    ver::files::snap("a_delete", None).unwrap();

    env::set_current_dir(&dest_b).unwrap();
    fs::write("to_delete.txt", "B modified this").unwrap();
    ver::files::snap("b_modify", None).unwrap();

    env::set_current_dir(tmp_a.path()).unwrap();

    (tmp_a, tmp_b, guard)
}

/// Helper to create two repos that can merge cleanly
fn setup_repos_for_clean_merge() -> (TempDir, TempDir, CwdGuard) {
    let (tmp_a, tmp_b, guard) = setup_two_repos_with_common_base();
    let dest_b = tmp_b.path().join("repo");

    // A adds a new file, B adds a different new file
    fs::write("a_new.txt", "new from A").unwrap();
    ver::files::snap("a_new", None).unwrap();

    env::set_current_dir(&dest_b).unwrap();
    fs::write("b_new.txt", "new from B").unwrap();
    ver::files::snap("b_new", None).unwrap();

    env::set_current_dir(tmp_a.path()).unwrap();

    (tmp_a, tmp_b, guard)
}

// ----- merge_run tests -----

#[test]
#[serial]
fn merge_run_clean_merge_completes_successfully() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(other_repo, "clean_merge", None, None);
    assert!(result.is_ok());

    // Verify merged files exist
    assert!(Path::new("a_new.txt").exists());
    assert!(Path::new("b_new.txt").exists());
    assert!(Path::new("shared.txt").exists());
    assert!(Path::new("only_a.txt").exists());
}

#[test]
#[serial]
fn merge_run_with_modification_conflict_creates_partial_merge() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(other_repo, "conflict_merge", None, None);
    assert!(result.is_ok());

    // Partial merge should be stored - verify by trying to pick
    let pick_result = ver::merge::merge_pick("conflict_merge", &["shared.txt"], &[]);
    assert!(pick_result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_addition_conflict_creates_partial_merge() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_addition_conflict();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(other_repo, "add_conflict", None, None);
    assert!(result.is_ok());

    // Partial merge should exist
    let pick_result = ver::merge::merge_pick("add_conflict", &["new_file.txt"], &[]);
    assert!(pick_result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_deletion_conflict_creates_partial_merge() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_deletion_conflict();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(other_repo, "del_conflict", None, None);
    assert!(result.is_ok());

    // Partial merge should exist
    let pick_result = ver::merge::merge_pick("del_conflict", &["to_delete.txt"], &[]);
    assert!(pick_result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_specific_snapshot_names() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(
        other_repo,
        "named_merge",
        Some("a_new".to_string()),
        Some("b_new".to_string()),
    );
    assert!(result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_only_our_snapshot_specified() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result =
        ver::merge::merge_run(other_repo, "our_specified", Some("a_new".to_string()), None);
    assert!(result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_only_their_snapshot_specified() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result =
        ver::merge::merge_run(other_repo, "their_specified", None, Some("b_new".to_string()));
    assert!(result.is_ok());
}

#[test]
#[serial]
fn merge_run_with_nonexistent_our_snapshot_errors() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(
        other_repo,
        "bad_merge",
        Some("nonexistent".to_string()),
        None,
    );
    assert!(result.is_err());
}

#[test]
#[serial]
fn merge_run_with_nonexistent_their_snapshot_errors() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_for_clean_merge();
    let other_repo = _tmp_b.path().join("repo");

    let result = ver::merge::merge_run(
        other_repo,
        "bad_merge",
        None,
        Some("nonexistent".to_string()),
    );
    assert!(result.is_err());
}

#[test]
#[serial]
fn merge_run_with_nonexistent_repo_errors() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let result = ver::merge::merge_run(
        PathBuf::from("/nonexistent/path"),
        "bad_merge",
        None,
        None,
    );
    assert!(result.is_err());
}

// ----- merge_pick tests -----

#[test]
#[serial]
fn merge_pick_ours_resolves_modification_conflict() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "pick_test", None, None).unwrap();

    let result = ver::merge::merge_pick("pick_test", &["shared.txt"], &[]);
    assert!(result.is_ok());

    // Should be able to end merge now
    let end_result = ver::merge::merge_end("pick_test");
    assert!(end_result.is_ok());

    // Verify our version was kept
    assert_eq!(
        fs::read_to_string("shared.txt").unwrap(),
        "modified by repo A"
    );
}

#[test]
#[serial]
fn merge_pick_theirs_resolves_modification_conflict() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "pick_test", None, None).unwrap();

    let result = ver::merge::merge_pick("pick_test", &[], &["shared.txt"]);
    assert!(result.is_ok());

    let end_result = ver::merge::merge_end("pick_test");
    assert!(end_result.is_ok());

    // Verify their version was kept
    assert_eq!(
        fs::read_to_string("shared.txt").unwrap(),
        "modified by repo B"
    );
}

#[test]
#[serial]
fn merge_pick_ours_resolves_addition_conflict() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_addition_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "add_pick", None, None).unwrap();

    let result = ver::merge::merge_pick("add_pick", &["new_file.txt"], &[]);
    assert!(result.is_ok());

    let end_result = ver::merge::merge_end("add_pick");
    assert!(end_result.is_ok());

    assert_eq!(fs::read_to_string("new_file.txt").unwrap(), "added by A");
}

#[test]
#[serial]
fn merge_pick_theirs_resolves_addition_conflict() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_addition_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "add_pick", None, None).unwrap();

    let result = ver::merge::merge_pick("add_pick", &[], &["new_file.txt"]);
    assert!(result.is_ok());

    let end_result = ver::merge::merge_end("add_pick");
    assert!(end_result.is_ok());

    assert_eq!(fs::read_to_string("new_file.txt").unwrap(), "added by B");
}

#[test]
#[serial]
fn merge_pick_ours_resolves_deletion_conflict_keep_deleted() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_deletion_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "del_pick", None, None).unwrap();

    // Ours deleted the file, so picking ours means file stays deleted
    let result = ver::merge::merge_pick("del_pick", &["to_delete.txt"], &[]);
    assert!(result.is_ok());

    let end_result = ver::merge::merge_end("del_pick");
    assert!(end_result.is_ok());

    // File should be deleted
    assert!(!Path::new("to_delete.txt").exists());
}

#[test]
#[serial]
fn merge_pick_theirs_resolves_deletion_conflict_keep_file() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_deletion_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "del_pick", None, None).unwrap();

    // Theirs modified the file, so picking theirs keeps the file
    let result = ver::merge::merge_pick("del_pick", &[], &["to_delete.txt"]);
    assert!(result.is_ok());

    let end_result = ver::merge::merge_end("del_pick");
    assert!(end_result.is_ok());

    // File should exist with their content
    assert!(Path::new("to_delete.txt").exists());
    assert_eq!(
        fs::read_to_string("to_delete.txt").unwrap(),
        "B modified this"
    );
}

#[test]
#[serial]
fn merge_pick_nonexistent_merge_errors() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let result = ver::merge::merge_pick("nonexistent_merge", &["file.txt"], &[]);
    assert!(result.is_err());
}

#[test]
#[serial]
fn merge_pick_nonexistent_path_warns_but_succeeds() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "warn_pick", None, None).unwrap();

    // Picking a path that doesn't have conflicts should warn but not error
    let result = ver::merge::merge_pick("warn_pick", &["nonexistent.txt", "shared.txt"], &[]);
    assert!(result.is_ok());
}

// ----- merge_end tests -----

#[test]
#[serial]
fn merge_end_with_unresolved_conflicts_does_not_complete() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "unresolved", None, None).unwrap();

    // Try to end without picking - should return Ok but not complete
    let result = ver::merge::merge_end("unresolved");
    assert!(result.is_ok()); // Returns Ok but prints warning

    // Partial merge should still exist - we can still pick
    let pick_result = ver::merge::merge_pick("unresolved", &["shared.txt"], &[]);
    assert!(pick_result.is_ok());
}

#[test]
#[serial]
fn merge_end_after_all_picks_completes() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "complete_merge", None, None).unwrap();
    ver::merge::merge_pick("complete_merge", &["shared.txt"], &[]).unwrap();

    let result = ver::merge::merge_end("complete_merge");
    assert!(result.is_ok());

    // Partial merge should be removed - trying to pick again should fail
    let pick_again = ver::merge::merge_pick("complete_merge", &["shared.txt"], &[]);
    assert!(pick_again.is_err());
}

#[test]
#[serial]
fn merge_end_creates_merge_snapshot_in_history() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    ver::merge::merge_run(other_repo, "history_test", None, None).unwrap();
    ver::merge::merge_pick("history_test", &["shared.txt"], &[]).unwrap();
    ver::merge::merge_end("history_test").unwrap();

    // Verify the merge snapshot exists in history
    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    assert!(history.contains("history_test"));
    assert!(history.contains("merged repository"));
}

#[test]
#[serial]
fn merge_end_nonexistent_merge_errors() {
    let (_tmp, _guard) = setup_repo_with_snapshot();

    let result = ver::merge::merge_end("nonexistent_merge");
    assert!(result.is_err());
}

#[test]
#[serial]
fn merge_end_updates_head() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    let resolved = ver::commons::resolve_ver_dir_path(env::current_dir().unwrap()).unwrap();
    let head_before = ver::commons::get_head(&resolved).unwrap();

    ver::merge::merge_run(other_repo, "head_test", None, None).unwrap();
    ver::merge::merge_pick("head_test", &["shared.txt"], &[]).unwrap();
    ver::merge::merge_end("head_test").unwrap();

    let head_after = ver::commons::get_head(&resolved).unwrap();
    assert!(head_after.is_some());
    assert_ne!(head_before, head_after);
}

// ----- merge integration tests -----

#[test]
#[serial]
fn full_merge_workflow_modification_conflict() {
    let (_tmp_a, _tmp_b, _guard) = setup_repos_with_modification_conflict();
    let other_repo = _tmp_b.path().join("repo");

    // Run merge
    let run_result = ver::merge::merge_run(other_repo, "full_workflow", None, None);
    assert!(run_result.is_ok());

    // Pick ours
    let pick_result = ver::merge::merge_pick("full_workflow", &["shared.txt"], &[]);
    assert!(pick_result.is_ok());

    // End merge
    let end_result = ver::merge::merge_end("full_workflow");
    assert!(end_result.is_ok());

    // Verify final state
    assert_eq!(
        fs::read_to_string("shared.txt").unwrap(),
        "modified by repo A"
    );
    assert!(Path::new("only_a.txt").exists());
}

#[test]
#[serial]
fn full_merge_workflow_multiple_conflicts() {
    let tmp_a = TempDir::new("ver_repo_a").unwrap();
    initialize(Some(tmp_a.path().to_str().unwrap())).unwrap();

    {
        let _guard = CwdGuard::change_to(tmp_a.path());
        fs::write("file1.txt", "base1").unwrap();
        fs::write("file2.txt", "base2").unwrap();
        fs::write("file3.txt", "base3").unwrap();
        ver::files::snap("base", None).unwrap();
    }

    let tmp_b = TempDir::new("ver_repo_b").unwrap();
    let dest_b = tmp_b.path().join("repo");
    fs::create_dir_all(&dest_b).unwrap();

    {
        let _guard = CwdGuard::change_to(tmp_a.path());
        ver::cp::copy("base", dest_b.to_str().unwrap()).unwrap();
    }

    // Create conflicts in all files
    {
        let _guard = CwdGuard::change_to(tmp_a.path());
        fs::write("file1.txt", "A1").unwrap();
        fs::write("file2.txt", "A2").unwrap();
        fs::write("file3.txt", "A3").unwrap();
        ver::files::snap("a_changes", None).unwrap();
    }

    {
        let _guard = CwdGuard::change_to(&dest_b);
        fs::write("file1.txt", "B1").unwrap();
        fs::write("file2.txt", "B2").unwrap();
        fs::write("file3.txt", "B3").unwrap();
        ver::files::snap("b_changes", None).unwrap();
    }

    let _guard = CwdGuard::change_to(tmp_a.path());

    // Run merge
    ver::merge::merge_run(dest_b, "multi_conflict", None, None).unwrap();

    // Pick different versions for different files
    ver::merge::merge_pick("multi_conflict", &["file1.txt"], &["file2.txt", "file3.txt"]).unwrap();

    // End merge
    ver::merge::merge_end("multi_conflict").unwrap();

    // Verify final state
    assert_eq!(fs::read_to_string("file1.txt").unwrap(), "A1");
    assert_eq!(fs::read_to_string("file2.txt").unwrap(), "B2");
    assert_eq!(fs::read_to_string("file3.txt").unwrap(), "B3");
}

#[test]
#[serial]
fn merge_preserves_unconflicted_files() {
    let (tmp_a, tmp_b, _guard) = setup_two_repos_with_common_base();
    let dest_b = tmp_b.path().join("repo");

    // A modifies shared.txt, B adds a new file
    {
        let _guard = CwdGuard::change_to(tmp_a.path());
        fs::write("shared.txt", "A modified").unwrap();
        ver::files::snap("a_mod", None).unwrap();
    }

    {
        let _guard = CwdGuard::change_to(&dest_b);
        fs::write("b_added.txt", "B added this").unwrap();
        ver::files::snap("b_add", None).unwrap();
    }

    let _guard = CwdGuard::change_to(tmp_a.path());

    ver::merge::merge_run(dest_b, "preserve_test", None, None).unwrap();

    // If there's a conflict, resolve it
    let _ = ver::merge::merge_pick("preserve_test", &["shared.txt"], &[]);
    let _ = ver::merge::merge_end("preserve_test");

    // Verify all files exist
    assert!(Path::new("shared.txt").exists());
    assert!(Path::new("only_a.txt").exists());
    assert!(Path::new("b_added.txt").exists());
}

// ---------- edge cases and error handling ----------

#[test]
#[serial]
fn snap_with_empty_description() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();
    let result = ver::files::snap("empty_desc", Some(""));

    assert!(result.is_ok());

    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    assert!(history.contains("empty_desc"));
}

#[test]
#[serial]
fn snap_with_multiline_description() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();
    let result = ver::files::snap("multiline", Some("line1\nline2\twith\ttabs"));

    assert!(result.is_ok());

    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    // Newlines and tabs should be replaced
    assert!(history.contains("line1 line2 with tabs"));
}

#[test]
#[serial]
fn goto_with_multiple_specific_paths() {
    let (_tmp, _guard) = setup_repo();

    fs::write("one.txt", "one-v1").unwrap();
    fs::write("two.txt", "two-v1").unwrap();
    fs::write("three.txt", "three-v1").unwrap();
    ver::files::snap("v1", None).unwrap();

    fs::write("one.txt", "one-v2").unwrap();
    fs::write("two.txt", "two-v2").unwrap();
    fs::write("three.txt", "three-v2").unwrap();

    ver::files::goto("v1", Some(vec!["one.txt".to_string(), "three.txt".to_string()])).unwrap();

    assert_eq!(fs::read_to_string("one.txt").unwrap(), "one-v1");
    assert_eq!(fs::read_to_string("two.txt").unwrap(), "two-v2");
    assert_eq!(fs::read_to_string("three.txt").unwrap(), "three-v1");
}

#[test]
#[serial]
fn snapshot_with_binary_file() {
    let (_tmp, _guard) = setup_repo();

    let binary_content: Vec<u8> = (0..=255).collect();
    fs::write("binary.bin", &binary_content).unwrap();

    let result = ver::files::snap("binary", None);
    assert!(result.is_ok());

    // Modify and restore
    fs::write("binary.bin", b"different").unwrap();
    ver::files::goto::<String>("binary", None).unwrap();

    let restored = fs::read("binary.bin").unwrap();
    assert_eq!(restored, binary_content);
}

#[test]
#[serial]
fn snapshot_with_empty_file() {
    let (_tmp, _guard) = setup_repo();

    fs::write("empty.txt", "").unwrap();
    ver::files::snap("empty", None).unwrap();

    fs::write("empty.txt", "now has content").unwrap();
    ver::files::goto::<String>("empty", None).unwrap();

    assert_eq!(fs::read_to_string("empty.txt").unwrap(), "");
}

#[test]
#[serial]
fn snapshot_with_unicode_filename() {
    let (_tmp, _guard) = setup_repo();

    fs::write("файл.txt", "unicode content").unwrap();
    fs::write("日本語.txt", "japanese content").unwrap();

    let result = ver::files::snap("unicode", None);
    assert!(result.is_ok());

    fs::write("файл.txt", "modified").unwrap();
    ver::files::goto::<String>("unicode", None).unwrap();

    assert_eq!(fs::read_to_string("файл.txt").unwrap(), "unicode content");
}

#[test]
#[serial]
fn snapshot_with_deeply_nested_structure() {
    let (_tmp, _guard) = setup_repo();

    let deep_path = "a/b/c/d/e/f/g/h/i/j/file.txt";
    fs::create_dir_all(Path::new(deep_path).parent().unwrap()).unwrap();
    fs::write(deep_path, "deep content").unwrap();

    let result = ver::files::snap("deep", None);
    assert!(result.is_ok());

    fs::write(deep_path, "modified").unwrap();
    ver::files::goto::<String>("deep", None).unwrap();

    assert_eq!(fs::read_to_string(deep_path).unwrap(), "deep content");
}

#[test]
#[serial]
fn verignore_with_patterns() {
    let (_tmp, _guard) = setup_repo();

    fs::write(".verignore", "*.log\n*.tmp").unwrap();
    fs::write("debug.log", "log content").unwrap();
    fs::write("cache.tmp", "cache content").unwrap();
    fs::write("tracked.txt", "tracked content").unwrap();

    ver::files::snap("ignore_test", None).unwrap();

    // Modify all files
    fs::write("debug.log", "modified log").unwrap();
    fs::write("cache.tmp", "modified cache").unwrap();
    fs::write("tracked.txt", "modified tracked").unwrap();

    ver::files::goto::<String>("ignore_test", None).unwrap();

    // Ignored files should keep modifications
    assert_eq!(fs::read_to_string("debug.log").unwrap(), "modified log");
    assert_eq!(fs::read_to_string("cache.tmp").unwrap(), "modified cache");
    // Tracked file should be restored
    assert_eq!(fs::read_to_string("tracked.txt").unwrap(), "tracked content");
}

#[test]
#[serial]
fn goto_updates_head() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "v1").unwrap();
    ver::files::snap("first", None).unwrap();

    let resolved = ver::commons::resolve_ver_dir_path(env::current_dir().unwrap()).unwrap();
    let head1 = ver::commons::get_head(&resolved).unwrap().unwrap();

    fs::write("file.txt", "v2").unwrap();
    ver::files::snap("second", None).unwrap();

    let head2 = ver::commons::get_head(&resolved).unwrap().unwrap();
    assert_ne!(head1, head2);

    ver::files::goto::<String>("first", None).unwrap();

    let head_after_goto = ver::commons::get_head(&resolved).unwrap().unwrap();
    assert_eq!(head_after_goto, head1);
}

#[test]
#[serial]
fn multiple_snapshots_same_content() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "same content").unwrap();
    ver::files::snap("snap1", None).unwrap();

    // Same content, different name
    ver::files::snap("snap2", None).unwrap();

    let history = fs::read_to_string(Path::new(VER_DIRECTORY).join(SNAPSHOT_HISTORY_FILE)).unwrap();
    assert!(history.contains("snap1"));
    assert!(history.contains("snap2"));

    // Both should be resolvable
    ver::files::goto::<String>("snap1", None).unwrap();
    ver::files::goto::<String>("snap2", None).unwrap();
}

#[test]
#[serial]
fn snapshot_name_with_special_characters() {
    let (_tmp, _guard) = setup_repo();

    fs::write("file.txt", "content").unwrap();

    // Test various special characters in names
    let names = vec![
        "name-with-dashes",
        "name_with_underscores",
        "name.with.dots",
        "name123with456numbers",
    ];

    for name in names {
        let result = ver::files::snap(name, None);
        assert!(result.is_ok(), "Failed to snap with name: {}", name);

        let goto_result = ver::files::goto::<String>(name, None);
        assert!(goto_result.is_ok(), "Failed to goto with name: {}", name);
    }
}
