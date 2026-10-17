use std::{marker::PhantomData, sync::Arc};

use redb::{Database, Key, ReadableDatabase, TableDefinition, Value};
use redis::Commands;

use crate::errors::KVError;

pub mod errors;

pub trait KVStore {
    type KeyType;
    type ValueType;

    fn get(&self, key: &Self::KeyType) -> Result<Option<Self::ValueType>, KVError>;
    fn put(&self, key: Self::KeyType, value: Self::ValueType) -> Result<(), KVError>;
}

pub struct RedbStore<K, V, KO, VO>
where
    K: Key + 'static,
    V: Value + 'static,
{
    pub db: Database,
    pub definition: TableDefinition<'static, K, V>,
    _owned: PhantomData<(KO, VO)>,
}

impl<K, V, KO, VO> KVStore for RedbStore<K, V, KO, VO>
where
    K: Key + 'static,
    V: Value + 'static,
    KO: std::borrow::Borrow<K::SelfType<'static>>,
    for<'a> VO: From<V::SelfType<'a>> + std::borrow::Borrow<<V as redb::Value>::SelfType<'static>>,
{
    type KeyType = KO;
    type ValueType = VO;

    fn get(&self, key: &KO) -> Result<Option<VO>, KVError> {
        let rx = self
            .db
            .begin_read()
            .map_err(|e| KVError::Get(format!("failed to begin read transaction: {e}")))?;
        let table = rx
            .open_table(self.definition)
            .map_err(|e| KVError::Get(format!("failed to open table: {e}")))?;
        let v = table
            .get(key.borrow())
            .map_err(|e| KVError::Get(format!("failed to read value from table: {e}")))?;

        Ok(v.map(|guard| VO::from(guard.value())))
    }

    fn put(&self, key: Self::KeyType, value: Self::ValueType) -> Result<(), KVError> {
        let wx = self
            .db
            .begin_write()
            .map_err(|e| KVError::Put(format!("failed to begin write transaction: {e}")))?;
        {
            let mut table = wx
                .open_table(self.definition)
                .map_err(|e| KVError::Put(format!("failed to open table: {e}")))?;
            table
                .insert(key, value)
                .map_err(|e| KVError::Put(format!("failed to insert value: {e}")))?;
        }

        Ok(())
    }
}

pub struct RedisStore {
    pub client: Arc<redis::Client>,
}

impl KVStore for RedisStore {
    type KeyType = String;
    type ValueType = Vec<u8>;

    fn get(&self, key: &Self::KeyType) -> Result<Option<Self::ValueType>, KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Get(format!("could not obtain Redis connection: {}", e)))?;

        let res: Option<Vec<u8>> = con
            .get(key)
            .map_err(|e| KVError::Get(format!("failed to perform get: {e}")))?;

        Ok(res)
    }

    fn put(&self, key: Self::KeyType, value: Self::ValueType) -> Result<(), KVError> {
        let mut con = self
            .client
            .get_connection()
            .map_err(|e| KVError::Put(format!("could not obtain Redis connection: {}", e)))?;

        con.set(key, value)
            .map_err(|e| KVError::Put(format!("failed to perform put: {e}")))
    }
}
