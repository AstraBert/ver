use std::{
    ops::RangeFull,
    str::FromStr,
    sync::{Arc, mpsc},
    thread,
};

use bytes::Bytes;
use redb::{Database, ReadableDatabase, TableDefinition};
pub use redis::RedisError;
use redis::{Client, Commands};

use crate::errors::KVError;

pub mod errors;

pub const SNAPSHOT_TABLE: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("snapshots");
pub const PARTIAL_MERGES_TABLE: TableDefinition<&str, &[u8]> =
    TableDefinition::new("partial_merges");
pub const NAMES_TABLE: TableDefinition<&str, [u8; 32]> = TableDefinition::new("names");
pub const BOUNDED_CONCURRENCY: usize = 64;
pub const SNAPSHOTS_NAMESPACE: &str = "snapshots";
pub const NAMES_NAMESPACE: &str = "names";
pub const PARTIAL_MERGES_NAMESPACE: &str = "partial_merges";

pub trait KVStore: Send + Sync {
    fn get_snapshot(&self, key: [u8; 32]) -> Result<Option<Bytes>, KVError>;
    fn put_snapshot(&self, key: [u8; 32], value: &[u8]) -> Result<(), KVError>;
    fn get_name(&self, key: &str) -> Result<Option<[u8; 32]>, KVError>;
    fn put_name(&self, key: &str, value: [u8; 32]) -> Result<(), KVError>;
    fn get_merge(&self, key: &str) -> Result<Option<Bytes>, KVError>;
    fn put_merge(&self, key: &str, value: &[u8]) -> Result<(), KVError>;
    fn copy(&self, dest: Arc<dyn KVStore>) -> Result<(), KVError>;
}

#[derive(Debug, Clone)]
pub struct RedbStore {
    pub db: Arc<Database>,
}

impl RedbStore {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl KVStore for RedbStore {
    fn get_snapshot(&self, key: [u8; 32]) -> Result<Option<Bytes>, KVError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| KVError::Get(format!("Error while opening redb read transaction: {e}")))?;
        let table = rx.open_table(SNAPSHOT_TABLE).map_err(|e| {
            KVError::Get(format!(
                "Error while opening redb snapshots table in readonly mode: {e}"
            ))
        })?;
        let opt = table.get(key).map_err(|e| {
            KVError::Get(format!(
                "Error while performing get operation on the snapshots table: {e}"
            ))
        })?;

        if let Some(value) = opt {
            return Ok(Some(Bytes::copy_from_slice(value.value())));
        }
        Ok(None)
    }

    fn put_snapshot(&self, key: [u8; 32], value: &[u8]) -> Result<(), KVError> {
        let wx = self.db.begin_write().map_err(|e| {
            KVError::Put(format!("Error while opening redb write transaction: {e}"))
        })?;
        {
            let mut table = wx.open_table(SNAPSHOT_TABLE).map_err(|e| {
                KVError::Put(format!(
                    "Error while opening redb snapshots table in writing mode: {e}"
                ))
            })?;
            table
                .insert(key, value)
                .map_err(|e| KVError::Put(format!("Insert failed on snapshots table: {e}")))?;
        }

        wx.commit()
            .map_err(|e| KVError::Put(format!("Commit failed for write operation: {e}")))?;

        Ok(())
    }

    fn get_name(&self, key: &str) -> Result<Option<[u8; 32]>, KVError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| KVError::Get(format!("Error while opening redb read transaction: {e}")))?;
        let table = rx.open_table(NAMES_TABLE).map_err(|e| {
            KVError::Get(format!(
                "Error while opening redb names table in readonly mode: {e}"
            ))
        })?;
        let opt = table.get(key).map_err(|e| {
            KVError::Get(format!(
                "Error while performing get operation on the names table: {e}"
            ))
        })?;

        if let Some(value) = opt {
            return Ok(Some(value.value()));
        }
        Ok(None)
    }

    fn put_name(&self, key: &str, value: [u8; 32]) -> Result<(), KVError> {
        let wx = self.db.begin_write().map_err(|e| {
            KVError::Put(format!("Error while opening redb write transaction: {e}"))
        })?;
        {
            let mut table = wx.open_table(NAMES_TABLE).map_err(|e| {
                KVError::Put(format!(
                    "Error while opening redb names table in writing mode: {e}"
                ))
            })?;
            table
                .insert(key, value)
                .map_err(|e| KVError::Put(format!("Insert failed on names table: {e}")))?;
        }

        wx.commit()
            .map_err(|e| KVError::Put(format!("Commit failed for write operation: {e}")))?;

        Ok(())
    }

    fn get_merge(&self, key: &str) -> Result<Option<Bytes>, KVError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| KVError::Get(format!("Error while opening redb read transaction: {e}")))?;
        let table = rx.open_table(PARTIAL_MERGES_TABLE).map_err(|e| {
            KVError::Get(format!(
                "Error while opening redb partial_merges table in readonly mode: {e}"
            ))
        })?;
        let opt = table.get(key).map_err(|e| {
            KVError::Get(format!(
                "Error while performing get operation on the partial_merges table: {e}"
            ))
        })?;

        if let Some(value) = opt {
            return Ok(Some(Bytes::copy_from_slice(value.value())));
        }
        Ok(None)
    }

    fn put_merge(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        let wx = self.db.begin_write().map_err(|e| {
            KVError::Put(format!("Error while opening redb write transaction: {e}"))
        })?;
        {
            let mut table = wx.open_table(PARTIAL_MERGES_TABLE).map_err(|e| {
                KVError::Put(format!(
                    "Error while opening redb partial_merges table in writing mode: {e}"
                ))
            })?;
            table
                .insert(key, value)
                .map_err(|e| KVError::Put(format!("Insert failed on partial_merges table: {e}")))?;
        }

        wx.commit()
            .map_err(|e| KVError::Put(format!("Commit failed for write operation: {e}")))?;

        Ok(())
    }

    fn copy(&self, dest: Arc<dyn KVStore>) -> Result<(), KVError> {
        let rx = self.db.begin_read().map_err(|e| {
            KVError::Copy(format!(
                "Error while opening the read transaction for the copy: {e}"
            ))
        })?;

        let snapshots = rx.open_table(SNAPSHOT_TABLE).map_err(|e| {
            KVError::Copy(format!(
                "Error while opening the snapshots table for the copy: {e}"
            ))
        })?;

        let (chan_tx, chan_rx) = mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        #[allow(for_loops_over_fallibles)]
        for range in snapshots.range_owned::<[u8; 32]>(RangeFull) {
            if let Some(Ok(entry)) = range.into_iter().next() {
                let (k, v) = entry;
                let dest_kv = dest.clone();
                let sender = chan_tx.clone();
                thread::spawn(move || {
                    let res = dest_kv.put_snapshot(k.value(), v.value()).map_err(|e| {
                        KVError::Copy(format!(
                            "Error while writing value to the destination KV: {e}"
                        ))
                    });
                    sender.send(res).unwrap();
                });
            }
        }

        drop(chan_tx);

        for res in chan_rx {
            match res {
                Ok(()) => continue,
                Err(e) => return Err(KVError::Copy(format!("Error while copying snapshots: {e}"))),
            }
        }

        let merges = rx.open_table(PARTIAL_MERGES_TABLE).map_err(|e| {
            KVError::Copy(format!(
                "Error while opening the partial_merges table for the copy: {e}"
            ))
        })?;

        let (merges_tx, merges_rx) = mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        #[allow(for_loops_over_fallibles)]
        for range in merges.range_owned::<&str>(RangeFull) {
            for entry in range {
                let (k, v) = entry
                    .map_err(|e| KVError::Copy(format!("Error while listing redb entries: {e}")))?;
                let dest_kv = dest.clone();
                let sender = merges_tx.clone();
                thread::spawn(move || {
                    let res = dest_kv.put_merge(k.value(), v.value()).map_err(|e| {
                        KVError::Copy(format!(
                            "Error while writing value to the destination KV: {e}"
                        ))
                    });
                    sender.send(res).unwrap();
                });
            }
        }

        drop(merges_tx);

        for res in merges_rx {
            match res {
                Ok(()) => continue,
                Err(e) => {
                    return Err(KVError::Copy(format!(
                        "Error while copying partial_merges: {e}"
                    )));
                }
            }
        }

        let names = rx.open_table(NAMES_TABLE).map_err(|e| {
            KVError::Copy(format!(
                "Error while opening the names table for the copy: {e}"
            ))
        })?;

        let (names_tx, names_rx) = mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        #[allow(for_loops_over_fallibles)]
        for range in names.range_owned::<&str>(RangeFull) {
            for entry in range {
                let (k, v) = entry
                    .map_err(|e| KVError::Copy(format!("Error while listing redb entries: {e}")))?;
                let dest_kv = dest.clone();
                let sender = names_tx.clone();
                thread::spawn(move || {
                    let res = dest_kv.put_name(k.value(), v.value()).map_err(|e| {
                        KVError::Copy(format!(
                            "Error while writing value to the destination KV: {e}"
                        ))
                    });
                    sender.send(res).unwrap();
                });
            }
        }

        drop(names_tx);

        for res in names_rx {
            match res {
                Ok(()) => continue,
                Err(e) => {
                    return Err(KVError::Copy(format!("Error while copying names: {e}")));
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RedisStore {
    pub client: Arc<Client>,
}

impl RedisStore {
    pub fn new(uri: impl Into<String>) -> Result<Self, RedisError> {
        let client = Arc::new(redis::Client::open(uri.into())?);
        Ok(Self { client })
    }
}

impl KVStore for RedisStore {
    fn get_snapshot(&self, key: [u8; 32]) -> Result<Option<Bytes>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("Error while getting Redis connection: {e}")))?;
        let new_key = blake3::Hash::from_bytes(key);
        let res: Option<Vec<u8>> = con
            .get(format!("{}/{}", SNAPSHOTS_NAMESPACE, new_key))
            .map_err(|e| KVError::Get(format!("Error while executing Redis `get` command: {e}")))?;

        Ok(res.map(Bytes::from))
    }

    fn put_snapshot(&self, key: [u8; 32], value: &[u8]) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("Error while getting Redis connection: {e}")))?;
        let new_key = blake3::Hash::from_bytes(key);
        con.set(format!("{}/{}", SNAPSHOTS_NAMESPACE, new_key), value)
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }

    fn get_name(&self, key: &str) -> Result<Option<[u8; 32]>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("Error while getting Redis connection: {e}")))?;

        let res: Option<Vec<u8>> = con
            .get(format!("{}/{}", NAMES_NAMESPACE, key))
            .map_err(|e| KVError::Get(format!("Error while executing Redis `get` command: {e}")))?;

        if let Some(v) = res {
            if v.len() != 32 {
                return Err(KVError::Put(format!(
                    "Expected 32 bytes when getting the snapshot ID associated to {}, got {}",
                    key,
                    v.len()
                )));
            }
            return Ok(Some(v.try_into().expect(
                "This should convert, since we made sure it has exactly len == 32",
            )));
        }
        Ok(None)
    }

    fn put_name(&self, key: &str, value: [u8; 32]) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("Error while getting Redis connection: {e}")))?;
        con.set(format!("{}/{}", NAMES_NAMESPACE, key), value.as_slice())
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }

    fn get_merge(&self, key: &str) -> Result<Option<Bytes>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("Error while getting Redis connection: {e}")))?;
        let res: Option<Vec<u8>> = con
            .get(format!("{}/{}", PARTIAL_MERGES_NAMESPACE, key))
            .map_err(|e| KVError::Get(format!("Error while executing Redis `get` command: {e}")))?;

        Ok(res.map(Bytes::from))
    }

    fn put_merge(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("Error while getting Redis connection: {e}")))?;
        con.set(format!("{}/{}", PARTIAL_MERGES_NAMESPACE, key), value)
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }

    fn copy(&self, dest: Arc<dyn KVStore>) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Copy(format!("Error while getting Redis connection: {e}")))?;

        let snapshots = con
            .scan_match::<_, String>(format!("{}/", SNAPSHOTS_NAMESPACE))
            .map_err(|e| {
                KVError::Copy(format!(
                    "Error while executing Redis `scan_match` command: {e}"
                ))
            })?;

        let (snapshots_tx, snapshots_rx) =
            mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        for snapshot_key in snapshots {
            let mut key = snapshot_key
                .map_err(|e| KVError::Copy(format!("Error while getting a snapshot key: {e}")))?;
            let hashed = blake3::Hash::from_str(&key.split_off(SNAPSHOTS_NAMESPACE.len() + 1))
                .map_err(|e| {
                    KVError::Copy(format!(
                        "Error while reconstructing the hash from hex string: {e}"
                    ))
                })?;
            let kv_src = self.clone();
            let kv_dest = dest.clone();
            let tx = snapshots_tx.clone();
            let bts = hashed.as_bytes().to_owned();

            thread::spawn(move || {
                let res = kv_src.get_snapshot(bts);
                match res {
                    Ok(val) => {
                        let put_res = kv_dest.put_snapshot(bts, val.unwrap().iter().as_slice());
                        tx.send(put_res)
                    }
                    Err(e) => tx.send(Err(e)),
                }
            });
        }

        for res in snapshots_rx {
            match res {
                Ok(()) => continue,
                Err(e) => return Err(KVError::Copy(format!("Error while copying snapshots: {e}"))),
            }
        }

        let names = con
            .scan_match::<_, String>(format!("{}/", NAMES_NAMESPACE))
            .map_err(|e| {
                KVError::Copy(format!(
                    "Error while executing Redis `scan_match` command: {e}"
                ))
            })?;

        let (names_tx, names_rx) = mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        for name_key in names {
            let key = name_key
                .map_err(|e| KVError::Copy(format!("Error while getting a snapshot key: {e}")))?
                .split_off(NAMES_NAMESPACE.len() + 1);
            let kv_src = self.clone();
            let kv_dest = dest.clone();
            let tx = names_tx.clone();

            thread::spawn(move || {
                let res = kv_src.get_name(&key);
                match res {
                    Ok(val) => {
                        let put_res = kv_dest.put_name(&key, val.unwrap());
                        tx.send(put_res)
                    }
                    Err(e) => tx.send(Err(e)),
                }
            });
        }

        for res in names_rx {
            match res {
                Ok(()) => continue,
                Err(e) => return Err(KVError::Copy(format!("Error while copying names: {e}"))),
            }
        }

        let merges = con
            .scan_match::<_, String>(format!("{}/", PARTIAL_MERGES_NAMESPACE))
            .map_err(|e| {
                KVError::Copy(format!(
                    "Error while executing Redis `scan_match` command: {e}"
                ))
            })?;

        let (merges_tx, merges_rx) = mpsc::sync_channel::<Result<(), KVError>>(BOUNDED_CONCURRENCY);

        for merge_key in merges {
            let key = merge_key
                .map_err(|e| KVError::Copy(format!("Error while getting a snapshot key: {e}")))?
                .split_off(PARTIAL_MERGES_NAMESPACE.len() + 1);

            let kv_src = self.clone();
            let kv_dest = dest.clone();
            let tx = merges_tx.clone();

            thread::spawn(move || {
                let res = kv_src.get_merge(&key);
                match res {
                    Ok(val) => {
                        let put_res = kv_dest.put_merge(&key, val.unwrap().iter().as_slice());
                        tx.send(put_res)
                    }
                    Err(e) => tx.send(Err(e)),
                }
            });
        }

        for res in merges_rx {
            match res {
                Ok(()) => continue,
                Err(e) => return Err(KVError::Copy(format!("Error while copying parts: {e}"))),
            }
        }

        Ok(())
    }
}
