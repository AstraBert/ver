use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use serde::{Deserialize, Serialize};
use ver_fs::{
    DiskFs, Fs, ObjectStoreFs,
    object_store_factory::{GetObjectStoreInput, get_object_store},
};
use ver_kv::{KVStore, RedbStore, RedisStore};

use crate::errors::VerError;

pub static GLOBAL_CONTEXT: OnceLock<PathBuf> = OnceLock::new();

fn global_context() -> &'static PathBuf {
    GLOBAL_CONTEXT.get_or_init(|| {
        dirs::home_dir()
            .expect("Missing HOME directory")
            .join(".ver/contexts.toml")
    })
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum FilesystemContext {
    Local {
        base_directory: String,
    },
    S3 {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        endpoint: String,
        bucket: String,
    },
    Gcs {
        service_account_credentials_path: String,
        bucket: String,
    },
    Azure {
        account_name: String,
        account_key: String,
        endpoint: String,
        container_name: String,
    },
}

impl FilesystemContext {
    pub fn build_fs(self) -> Result<Arc<dyn Fs>, VerError> {
        let fs: Arc<dyn Fs> = match self {
            Self::Azure {
                account_name,
                account_key,
                endpoint,
                container_name,
            } => {
                let object_st = get_object_store(GetObjectStoreInput::Azure {
                    account_name: Some(account_name),
                    account_key: Some(account_key),
                    endpoint: Some(endpoint),
                    container_name: container_name,
                })?;
                Arc::new(ObjectStoreFs::new(object_st))
            }
            Self::Gcs {
                service_account_credentials_path,
                bucket,
            } => {
                let object_st = get_object_store(GetObjectStoreInput::Gcs {
                    service_account_credentials_path,
                    bucket,
                })?;
                Arc::new(ObjectStoreFs::new(object_st))
            }
            Self::S3 {
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket,
            } => {
                let object_st = get_object_store(GetObjectStoreInput::S3 {
                    access_key_id: Some(access_key_id),
                    secret_access_key: Some(secret_access_key),
                    region: Some(region),
                    endpoint: Some(endpoint),
                    bucket,
                })?;
                Arc::new(ObjectStoreFs::new(object_st))
            }
            Self::Local { base_directory } => Arc::new(DiskFs::new(base_directory)),
        };

        Ok(fs)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum KVStoreContext {
    Embedded { redb_path: String },
    Redis { uri: String },
}

impl KVStoreContext {
    pub fn build_kv(&self) -> Result<Arc<dyn KVStore>, VerError> {
        match self {
            Self::Embedded { redb_path } => {
                let db = redb::Database::open(redb_path)?;
                Ok(Arc::new(RedbStore::new(Arc::new(db))))
            }
            Self::Redis { uri } => Ok(Arc::new(RedisStore::new(uri)?)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VerContext {
    pub fs: HashMap<String, FilesystemContext>,
    pub kv: HashMap<String, KVStoreContext>,
    pub current: Option<String>,
}

impl Default for VerContext {
    fn default() -> Self {
        VerContext {
            fs: HashMap::new(),
            kv: HashMap::new(),
            current: None,
        }
    }
}

impl VerContext {
    pub fn validate(&self) -> Result<(), VerError> {
        if self.fs.len() != self.kv.len() {
            return Err("Number of KV and Filesystem backends differ".into());
        }

        if !self.fs.keys().all(|k| self.kv.contains_key(k)) {
            return Err(
                "KV and Filesystem backends do not belong to the same set of contexts".into(),
            );
        }

        if self.fs.iter().any(|(k, v)| {
            !matches!(v, &FilesystemContext::Local { base_directory: _ })
                && matches!(
                    self.kv.get(k).unwrap(),
                    &KVStoreContext::Embedded { redb_path: _ }
                )
        }) {
            return Err(
                "Cannot have an embeddable KV store for a object storage filesystem".into(),
            );
        }

        if !self.fs.values().all(|v| {
            if let &FilesystemContext::Local { base_directory } = &v {
                return PathBuf::from(base_directory).is_absolute();
            }
            true
        }) {
            return Err("One or more contexts have an associated local filesystem whose base directory is not an absolute path".into());
        }

        if !self.kv.values().all(|v| {
            if let &KVStoreContext::Embedded { redb_path } = &v {
                return PathBuf::from(redb_path).is_absolute();
            }
            true
        }) {
            return Err("One or more contexts have an associated embedded KV store whose redb path is not an absolute path".into());
        }

        Ok(())
    }

    pub fn delete(&mut self, context: &str) -> Result<(), VerError> {
        if let Some(ref ctx) = self.current
            && context == ctx
        {
            return Err("Cannot delete a context marked as current. Switch and re-try".into());
        }

        self.fs.remove(context);
        self.kv.remove(context);

        Ok(())
    }

    pub fn switch(&mut self, context: &str) -> Result<(), VerError> {
        // assuming we are dealing with a validated ctx
        // so fs and kv have the same keys
        if !self.fs.contains_key(context) {
            return Err("Cannot switch to a context that does not exist".into());
        }

        self.current = Some(context.to_string());

        Ok(())
    }

    pub fn current_context(&self) -> Option<(FilesystemContext, KVStoreContext)> {
        if let Some(ref current) = self.current {
            let fs = self.fs.get(current).unwrap().clone();
            let kv = self.kv.get(current).unwrap().clone();
            return Some((fs, kv));
        }

        None
    }
}

pub fn read_context() -> Result<VerContext, VerError> {
    let path = global_context();
    if !path.exists() {
        return Ok(VerContext::default());
    }

    let content = fs::read_to_string(&path)?;
    let ctx: VerContext = toml::from_str(&content)?;
    ctx.validate()?;

    Ok(ctx)
}
