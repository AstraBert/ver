use std::sync::Arc;

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

pub trait KVStore: Send + Sync {
    fn get_snapshot(&self, key: [u8; 32]) -> Result<Option<Bytes>, KVError>;
    fn put_snapshot(&self, key: [u8; 32], value: &[u8]) -> Result<(), KVError>;
    fn get_name(&self, key: &str) -> Result<Option<[u8; 32]>, KVError>;
    fn put_name(&self, key: &str, value: [u8; 32]) -> Result<(), KVError>;
    fn get_merge(&self, key: &str) -> Result<Option<Bytes>, KVError>;
    fn put_merge(&self, key: &str, value: &[u8]) -> Result<(), KVError>;
}

pub struct RedbStore<'a> {
    pub db: &'a Database,
}

impl<'a> RedbStore<'a> {
    pub fn new(db: &'a Database) -> Self {
        Self { db }
    }
}

impl<'a> KVStore for RedbStore<'a> {
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
}

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
            .get(format!("snapshots/{}", new_key))
            .map_err(|e| KVError::Get(format!("Error while executing Redis `get` command: {e}")))?;

        Ok(res.map(Bytes::from))
    }

    fn put_snapshot(&self, key: [u8; 32], value: &[u8]) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("Error while getting Redis connection: {e}")))?;
        let new_key = blake3::Hash::from_bytes(key);
        con.set(format!("snapshots/{}", new_key), value)
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }

    fn get_name(&self, key: &str) -> Result<Option<[u8; 32]>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("Error while getting Redis connection: {e}")))?;

        let res: Option<Vec<u8>> = con
            .get(format!("names/{}", key))
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
        con.set(format!("names/{}", key), value.as_slice())
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }

    fn get_merge(&self, key: &str) -> Result<Option<Bytes>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("Error while getting Redis connection: {e}")))?;
        let res: Option<Vec<u8>> = con
            .get(format!("partial_merges/{}", key))
            .map_err(|e| KVError::Get(format!("Error while executing Redis `get` command: {e}")))?;

        Ok(res.map(Bytes::from))
    }

    fn put_merge(&self, key: &str, value: &[u8]) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("Error while getting Redis connection: {e}")))?;
        con.set(format!("partial_merges/{}", key), value)
            .map_err(|e| KVError::Put(format!("Error while executing Redis `set` command: {e}")))
    }
}
