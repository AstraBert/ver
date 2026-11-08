use std::sync::Arc;

use tempdir::TempDir;
use ver_kv::{KVStore, RedbStore, RedisStore};

fn redb_store() -> (TempDir, Arc<RedbStore>) {
    let tmp = TempDir::new("ver_kv_test").unwrap();
    let db = redb::Database::create(tmp.path().join("test.redb")).unwrap();
    (tmp, Arc::new(RedbStore::new(Arc::new(db))))
}

fn key(byte: u8) -> [u8; 32] {
    [byte; 32]
}

/// Build a RedisStore from REDIS_URI, or None if the var is unset (test skips).
fn redis_store() -> Option<Arc<RedisStore>> {
    let uri = std::env::var("REDIS_URI").ok()?;
    let ns = std::env::var("REDIS_NAMESPACE")
        .ok()
        .unwrap_or("test".to_string());
    let store = RedisStore::new(uri, ns).expect("REDIS_URI should be a valid connection string");
    Some(Arc::new(store))
}

/// Random-ish suffix so parallel test runs against a shared server don't clash.
fn uniq() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let pid = std::process::id();
    blake3::hash(format!("{pid}-{nanos}").as_bytes()).to_string()[..16].to_string()
}

macro_rules! redis_test {
    ($kv:ident, $body:block) => {
        let Some($kv) = redis_store() else {
            eprintln!("REDIS_URI not set, skipping");
            return;
        };
        $body
    };
}

// ---------- snapshots ----------

#[test]
fn snapshot_put_get_roundtrip() {
    let (_tmp, kv) = redb_store();
    kv.put_snapshot(key(1), b"snapshot-bytes").unwrap();
    let got = kv.get_snapshot(key(1)).unwrap().unwrap();
    assert_eq!(got.as_ref(), b"snapshot-bytes");
}

#[test]
fn snapshot_get_missing_returns_none() {
    let (_tmp, kv) = redb_store();
    assert!(kv.get_snapshot(key(9)).unwrap().is_none());
}

#[test]
fn snapshot_put_overwrites() {
    let (_tmp, kv) = redb_store();
    kv.put_snapshot(key(1), b"first").unwrap();
    kv.put_snapshot(key(1), b"second").unwrap();
    let got = kv.get_snapshot(key(1)).unwrap().unwrap();
    assert_eq!(got.as_ref(), b"second");
}

#[test]
fn snapshot_put_empty_value() {
    let (_tmp, kv) = redb_store();
    kv.put_snapshot(key(2), b"").unwrap();
    let got = kv.get_snapshot(key(2)).unwrap().unwrap();
    assert_eq!(got.as_ref(), b"");
}

// ---------- names ----------

#[test]
fn name_put_get_roundtrip() {
    let (_tmp, kv) = redb_store();
    kv.put_name("main", key(3)).unwrap();
    let got = kv.get_name("main").unwrap().unwrap();
    assert_eq!(got, key(3));
}

#[test]
fn name_get_missing_returns_none() {
    let (_tmp, kv) = redb_store();
    assert!(kv.get_name("nope").unwrap().is_none());
}

#[test]
fn name_put_overwrites() {
    let (_tmp, kv) = redb_store();
    kv.put_name("main", key(1)).unwrap();
    kv.put_name("main", key(2)).unwrap();
    assert_eq!(kv.get_name("main").unwrap().unwrap(), key(2));
}

#[test]
fn names_are_independent_of_snapshots() {
    let (_tmp, kv) = redb_store();
    kv.put_name("snap", key(7)).unwrap();
    // same bytes as a snapshot key should not collide
    assert!(kv.get_snapshot(key(7)).unwrap().is_none());
}

// ---------- merges ----------

#[test]
fn merge_put_get_roundtrip() {
    let (_tmp, kv) = redb_store();
    kv.put_merge("merge1", b"partial-merge").unwrap();
    let got = kv.get_merge("merge1").unwrap().unwrap();
    assert_eq!(got.as_ref(), b"partial-merge");
}

#[test]
fn merge_get_missing_returns_none() {
    let (_tmp, kv) = redb_store();
    assert!(kv.get_merge("nope").unwrap().is_none());
}

#[test]
fn merge_delete_removes_entry() {
    let (_tmp, kv) = redb_store();
    kv.put_merge("m", b"v").unwrap();
    kv.delete_merge("m").unwrap();
    assert!(kv.get_merge("m").unwrap().is_none());
}

#[test]
fn merge_delete_nonexistent_is_ok() {
    let (_tmp, kv) = redb_store();
    assert!(kv.delete_merge("nope").is_ok());
}

// ---------- copy ----------

#[test]
fn copy_moves_all_tables() {
    let (_t1, src) = redb_store();
    let (_t2, dst) = redb_store();
    let dst_trait: Arc<dyn KVStore> = dst.clone();

    kv_put_fixtures(src.as_ref());

    src.copy(&dst_trait).unwrap();

    assert_eq!(
        dst.get_snapshot(key(1)).unwrap().unwrap().as_ref(),
        b"snap1"
    );
    assert_eq!(
        dst.get_snapshot(key(2)).unwrap().unwrap().as_ref(),
        b"snap2"
    );
    assert_eq!(dst.get_name("main").unwrap().unwrap(), key(1));
    assert_eq!(dst.get_merge("m1").unwrap().unwrap().as_ref(), b"merge1");
}

#[test]
fn copy_from_empty_store_is_ok() {
    let (_t1, src) = redb_store();
    let (_t2, dst) = redb_store();
    let dst_trait: Arc<dyn KVStore> = dst.clone();

    src.copy(&dst_trait).unwrap();

    assert!(dst.get_snapshot(key(1)).unwrap().is_none());
    assert!(dst.get_name("main").unwrap().is_none());
    assert!(dst.get_merge("m").unwrap().is_none());
}

#[test]
fn copy_preserves_existing_destination_entries() {
    let (_t1, src) = redb_store();
    let (_t2, dst) = redb_store();
    let dst_trait: Arc<dyn KVStore> = dst.clone();

    dst.put_name("existing", key(5)).unwrap();
    src.put_name("main", key(1)).unwrap();

    src.copy(&dst_trait).unwrap();

    assert_eq!(dst.get_name("existing").unwrap().unwrap(), key(5));
    assert_eq!(dst.get_name("main").unwrap().unwrap(), key(1));
}

fn kv_put_fixtures(kv: &RedbStore) {
    kv.put_snapshot(key(1), b"snap1").unwrap();
    kv.put_snapshot(key(2), b"snap2").unwrap();
    kv.put_name("main", key(1)).unwrap();
    kv.put_merge("m1", b"merge1").unwrap();
}

// ---------- persistence ----------

#[test]
fn redb_persists_across_reopen() {
    let tmp = TempDir::new("ver_kv_test").unwrap();
    let path = tmp.path().join("persist.redb");
    {
        let db = redb::Database::create(&path).unwrap();
        let kv = RedbStore::new(Arc::new(db));
        kv.put_snapshot(key(4), b"durable").unwrap();
        kv.put_name("durable", key(4)).unwrap();
    }
    {
        let db = redb::Database::open(&path).unwrap();
        let kv = RedbStore::new(Arc::new(db));
        assert_eq!(
            kv.get_snapshot(key(4)).unwrap().unwrap().as_ref(),
            b"durable"
        );
        assert_eq!(kv.get_name("durable").unwrap().unwrap(), key(4));
    }
}

// ---------- RedisStore (require REDIS_URI) ----------

#[test]
fn redis_snapshot_put_get_roundtrip() {
    redis_test!(kv, {
        let k = blake3::hash(uniq().as_bytes()).as_bytes().to_owned();
        kv.put_snapshot(k, b"redis-snapshot").unwrap();
        let got = kv.get_snapshot(k).unwrap().unwrap();
        assert_eq!(got.as_ref(), b"redis-snapshot");
    });
}

#[test]
fn redis_snapshot_get_missing_returns_none() {
    redis_test!(kv, {
        let k = blake3::hash(uniq().as_bytes()).as_bytes().to_owned();
        assert!(kv.get_snapshot(k).unwrap().is_none());
    });
}

#[test]
fn redis_snapshot_put_overwrites() {
    redis_test!(kv, {
        let k = blake3::hash(uniq().as_bytes()).as_bytes().to_owned();
        kv.put_snapshot(k, b"first").unwrap();
        kv.put_snapshot(k, b"second").unwrap();
        assert_eq!(kv.get_snapshot(k).unwrap().unwrap().as_ref(), b"second");
    });
}

#[test]
fn redis_name_put_get_roundtrip() {
    redis_test!(kv, {
        let name = format!("name-{}", uniq());
        kv.put_name(&name, key(8)).unwrap();
        assert_eq!(kv.get_name(&name).unwrap().unwrap(), key(8));
    });
}

#[test]
fn redis_name_get_missing_returns_none() {
    redis_test!(kv, {
        let name = format!("name-{}", uniq());
        assert!(kv.get_name(&name).unwrap().is_none());
    });
}

#[test]
fn redis_name_put_overwrites() {
    redis_test!(kv, {
        let name = format!("name-{}", uniq());
        kv.put_name(&name, key(1)).unwrap();
        kv.put_name(&name, key(2)).unwrap();
        assert_eq!(kv.get_name(&name).unwrap().unwrap(), key(2));
    });
}

#[test]
fn redis_merge_put_get_delete() {
    redis_test!(kv, {
        let name = format!("merge-{}", uniq());
        kv.put_merge(&name, b"partial").unwrap();
        assert_eq!(kv.get_merge(&name).unwrap().unwrap().as_ref(), b"partial");
        kv.delete_merge(&name).unwrap();
        assert!(kv.get_merge(&name).unwrap().is_none());
    });
}

#[test]
fn redis_merge_get_missing_returns_none() {
    redis_test!(kv, {
        let name = format!("merge-{}", uniq());
        assert!(kv.get_merge(&name).unwrap().is_none());
    });
}

#[test]
fn redis_merge_delete_nonexistent_is_ok() {
    redis_test!(kv, {
        let name = format!("merge-{}", uniq());
        assert!(kv.delete_merge(&name).is_ok());
    });
}

#[test]
fn redis_copy_to_redb() {
    redis_test!(kv, {
        let snap_key = blake3::hash(uniq().as_bytes()).as_bytes().to_owned();
        let name = format!("name-{}", uniq());
        let merge = format!("merge-{}", uniq());
        kv.put_snapshot(snap_key, b"to-copy").unwrap();
        kv.put_name(&name, snap_key).unwrap();
        kv.put_merge(&merge, b"merge-copy").unwrap();

        let (_t, dst) = redb_store();
        let dst_trait: Arc<dyn KVStore> = dst.clone();
        kv.copy(&dst_trait).unwrap();

        // copy scans whole namespaces, so the destination may hold more
        // than our keys on a shared server: assert ours made it over
        assert_eq!(
            dst.get_snapshot(snap_key).unwrap().unwrap().as_ref(),
            b"to-copy"
        );
        assert_eq!(dst.get_name(&name).unwrap().unwrap(), snap_key);
        assert_eq!(
            dst.get_merge(&merge).unwrap().unwrap().as_ref(),
            b"merge-copy"
        );
    });
}

#[test]
fn redb_copy_to_redis() {
    redis_test!(kv, {
        let (_t, src) = redb_store();
        let snap_key = blake3::hash(uniq().as_bytes()).as_bytes().to_owned();
        let name = format!("name-{}", uniq());
        let merge = format!("merge-{}", uniq());
        src.put_snapshot(snap_key, b"from-redb").unwrap();
        src.put_name(&name, snap_key).unwrap();
        src.put_merge(&merge, b"merge-from-redb").unwrap();

        let dst_trait: Arc<dyn KVStore> = kv.clone();
        src.copy(&dst_trait).unwrap();

        assert_eq!(
            kv.get_snapshot(snap_key).unwrap().unwrap().as_ref(),
            b"from-redb"
        );
        assert_eq!(kv.get_name(&name).unwrap().unwrap(), snap_key);
        assert_eq!(
            kv.get_merge(&merge).unwrap().unwrap().as_ref(),
            b"merge-from-redb"
        );

        // cleanup: merges are plain keys we can delete
        kv.delete_merge(&merge).unwrap();
    });
}
