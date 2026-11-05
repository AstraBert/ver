use std::sync::Arc;

use object_store::memory::InMemory;
use tempdir::TempDir;
use ver_fs::{DiskFs, Fs, ObjectStoreFs, local_disk_walker, noignore_local_disk_walker};

fn disk_fs() -> (TempDir, Arc<DiskFs>) {
    let tmp = TempDir::new("ver_fs_test").unwrap();
    let fs = Arc::new(DiskFs::new(tmp.path().to_path_buf()));
    (tmp, fs)
}

fn mem_fs() -> Arc<ObjectStoreFs> {
    Arc::new(ObjectStoreFs::new(Arc::new(InMemory::new())))
}

// ---------- DiskFs ----------

#[tokio::test]
async fn disk_write_read_roundtrip() {
    let (_tmp, fs) = disk_fs();
    fs.write("a.txt", b"hello".to_vec().into()).await.unwrap();
    let content = fs.read("a.txt").await.unwrap();
    assert_eq!(content.as_ref(), b"hello");
}

#[tokio::test]
async fn disk_write_creates_parent_dirs() {
    let (_tmp, fs) = disk_fs();
    fs.write("sub/dir/file.txt", b"nested".to_vec().into())
        .await
        .unwrap();
    let content = fs.read("sub/dir/file.txt").await.unwrap();
    assert_eq!(content.as_ref(), b"nested");
}

#[tokio::test]
async fn disk_read_nonexistent_errors() {
    let (_tmp, fs) = disk_fs();
    assert!(fs.read("nope.txt").await.is_err());
}

#[tokio::test]
async fn disk_delete_removes_file() {
    let (_tmp, fs) = disk_fs();
    fs.write("del.txt", b"x".to_vec().into()).await.unwrap();
    assert!(fs.exists("del.txt").await.unwrap());
    fs.delete("del.txt").await.unwrap();
    assert!(!fs.exists("del.txt").await.unwrap());
}

#[tokio::test]
async fn disk_delete_nonexistent_is_ok() {
    let (_tmp, fs) = disk_fs();
    assert!(fs.delete("nope.txt").await.is_ok());
}

#[tokio::test]
async fn disk_exists() {
    let (_tmp, fs) = disk_fs();
    assert!(!fs.exists("x.txt").await.unwrap());
    fs.write("x.txt", b"1".to_vec().into()).await.unwrap();
    assert!(fs.exists("x.txt").await.unwrap());
}

#[tokio::test]
async fn disk_create_dir() {
    let (_tmp, fs) = disk_fs();
    fs.create_dir("my/dir").await.unwrap();
    assert!(fs.exists("my/dir").await.unwrap());
}

#[tokio::test]
async fn disk_list_with_walker() {
    let (_tmp, fs) = disk_fs();
    fs.write("one.txt", b"1".to_vec().into()).await.unwrap();
    fs.write("two.txt", b"2".to_vec().into()).await.unwrap();
    fs.write("sub/three.txt", b"3".to_vec().into())
        .await
        .unwrap();

    let entries: Vec<_> = fs
        .list("", Some(Box::new(local_disk_walker)))
        .await
        .unwrap()
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    assert!(entries.contains(&"one.txt".to_string()));
    assert!(entries.contains(&"two.txt".to_string()));
    assert!(entries.iter().any(|e| e.ends_with("three.txt")));
}

#[tokio::test]
async fn disk_list_excludes_ver_directory() {
    let (tmp, fs) = disk_fs();
    fs.write("tracked.txt", b"t".to_vec().into()).await.unwrap();
    // create a .ver directory with a file inside
    std::fs::create_dir_all(tmp.path().join(".ver")).unwrap();
    std::fs::write(tmp.path().join(".ver/hidden.txt"), b"h").unwrap();

    let entries: Vec<_> = fs
        .list("", Some(Box::new(local_disk_walker)))
        .await
        .unwrap()
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    assert!(entries.contains(&"tracked.txt".to_string()));
    assert!(!entries.iter().any(|e| e.contains(".ver")));
}

#[tokio::test]
async fn disk_list_noignore_includes_ver_directory() {
    let (tmp, fs) = disk_fs();
    fs.write("tracked.txt", b"t".to_vec().into()).await.unwrap();
    std::fs::create_dir_all(tmp.path().join(".ver")).unwrap();
    std::fs::write(tmp.path().join(".ver/hidden.txt"), b"h").unwrap();

    let entries: Vec<_> = fs
        .list("", Some(Box::new(noignore_local_disk_walker)))
        .await
        .unwrap()
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();
    dbg!(entries.clone());

    assert!(
        entries
            .iter()
            .any(|e| e.contains(".ver") && e.contains("hidden.txt"))
    );
}

#[tokio::test]
async fn disk_list_respects_verignore() {
    let (_tmp, fs) = disk_fs();
    fs.write(".verignore", b"ignored.txt".to_vec().into())
        .await
        .unwrap();
    fs.write("ignored.txt", b"i".to_vec().into()).await.unwrap();
    fs.write("tracked.txt", b"t".to_vec().into()).await.unwrap();

    let entries: Vec<_> = fs
        .list("", Some(Box::new(local_disk_walker)))
        .await
        .unwrap()
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    assert!(entries.contains(&"tracked.txt".to_string()));
    assert!(!entries.contains(&"ignored.txt".to_string()));
}

#[tokio::test]
async fn disk_stream_read() {
    use tokio::io::AsyncReadExt;
    let (_tmp, fs) = disk_fs();
    fs.write("stream.txt", b"streamed".to_vec().into())
        .await
        .unwrap();
    let mut reader = fs.stream_read("stream.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"streamed");
}

#[tokio::test]
async fn disk_copy_files() {
    let (_src_tmp, src) = disk_fs();
    let (_dst_tmp, dst) = disk_fs();
    src.write("a.txt", b"aaa".to_vec().into()).await.unwrap();
    src.write("b.txt", b"bbb".to_vec().into()).await.unwrap();

    let dst_trait: Arc<dyn Fs> = dst;
    src.copy_files(&dst_trait, vec!["a.txt", "b.txt"])
        .await
        .unwrap();

    assert_eq!(dst_trait.read("a.txt").await.unwrap().as_ref(), b"aaa");
    assert_eq!(dst_trait.read("b.txt").await.unwrap().as_ref(), b"bbb");
}

#[tokio::test]
async fn disk_copy_dir() {
    let (_src_tmp, src) = disk_fs();
    let (_dst_tmp, dst) = disk_fs();
    src.write("x/1.txt", b"1".to_vec().into()).await.unwrap();
    src.write("x/2.txt", b"2".to_vec().into()).await.unwrap();

    let dst_trait: Arc<dyn Fs> = dst;
    src.copy_dir(&dst_trait, Some("x"), false).await.unwrap();

    assert_eq!(dst_trait.read("x/1.txt").await.unwrap().as_ref(), b"1");
    assert_eq!(dst_trait.read("x/2.txt").await.unwrap().as_ref(), b"2");
}

// ---------- ObjectStoreFs (InMemory) ----------

#[tokio::test]
async fn mem_write_read_roundtrip() {
    let fs = mem_fs();
    fs.write("a.txt", b"hello".to_vec().into()).await.unwrap();
    let content = fs.read("a.txt").await.unwrap();
    assert_eq!(content.as_ref(), b"hello");
}

#[tokio::test]
async fn mem_read_nonexistent_errors() {
    let fs = mem_fs();
    assert!(fs.read("nope.txt").await.is_err());
}

#[tokio::test]
async fn mem_delete_removes_file() {
    let fs = mem_fs();
    fs.write("del.txt", b"x".to_vec().into()).await.unwrap();
    assert!(fs.exists("del.txt").await.unwrap());
    fs.delete("del.txt").await.unwrap();
    assert!(!fs.exists("del.txt").await.unwrap());
}

#[tokio::test]
async fn mem_delete_nonexistent_is_ok() {
    let fs = mem_fs();
    assert!(fs.delete("nope.txt").await.is_ok());
}

#[tokio::test]
async fn mem_exists() {
    let fs = mem_fs();
    assert!(!fs.exists("x.txt").await.unwrap());
    fs.write("x.txt", b"1".to_vec().into()).await.unwrap();
    assert!(fs.exists("x.txt").await.unwrap());
}

#[tokio::test]
async fn mem_create_dir_is_noop() {
    let fs = mem_fs();
    assert!(fs.create_dir("anything").await.is_ok());
}

#[tokio::test]
async fn mem_list() {
    let fs = mem_fs();
    fs.write("one.txt", b"1".to_vec().into()).await.unwrap();
    fs.write("two.txt", b"2".to_vec().into()).await.unwrap();

    let entries: Vec<_> = fs
        .list("", None)
        .await
        .unwrap()
        .map(|e| e.path().to_string_lossy().into_owned())
        .collect();

    assert!(entries.contains(&"one.txt".to_string()));
    assert!(entries.contains(&"two.txt".to_string()));
}

#[tokio::test]
async fn mem_stream_read() {
    use tokio::io::AsyncReadExt;
    let fs = mem_fs();
    fs.write("s.txt", b"streamed".to_vec().into())
        .await
        .unwrap();
    let mut reader = fs.stream_read("s.txt").await.unwrap();
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf).await.unwrap();
    assert_eq!(buf, b"streamed");
}

#[tokio::test]
async fn mem_copy_files() {
    let src = mem_fs();
    let dst = mem_fs();
    src.write("a.txt", b"aaa".to_vec().into()).await.unwrap();

    let dst_trait: Arc<dyn Fs> = dst;
    src.copy_files(&dst_trait, vec!["a.txt"]).await.unwrap();

    assert_eq!(dst_trait.read("a.txt").await.unwrap().as_ref(), b"aaa");
}

#[tokio::test]
async fn mem_copy_dir() {
    let src = mem_fs();
    let dst = mem_fs();
    src.write("d/1.txt", b"1".to_vec().into()).await.unwrap();
    src.write("d/2.txt", b"2".to_vec().into()).await.unwrap();

    let dst_trait: Arc<dyn Fs> = dst;
    src.copy_dir(&dst_trait, Some("d"), false).await.unwrap();

    assert_eq!(dst_trait.read("d/1.txt").await.unwrap().as_ref(), b"1");
    assert_eq!(dst_trait.read("d/2.txt").await.unwrap().as_ref(), b"2");
}

// ---------- cross-backend ----------

#[tokio::test]
async fn disk_to_memory_copy_dir() {
    let (_tmp, src) = disk_fs();
    let dst = mem_fs();
    src.write("f.txt", b"cross".to_vec().into()).await.unwrap();

    let dst_trait: Arc<dyn Fs> = dst;
    src.copy_dir(&dst_trait, None, false).await.unwrap();

    assert_eq!(dst_trait.read("f.txt").await.unwrap().as_ref(), b"cross");
}
