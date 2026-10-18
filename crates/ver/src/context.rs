use std::{
    collections::{HashMap, HashSet},
    env, fs,
    path::PathBuf,
    sync::Arc,
};

use blobsdb::get_object_store;
use serde::{Deserialize, Serialize};
use ver_fs::{DiskFs, Fs, ObjectStoreFs};

use crate::{commons::global_context, errors::VerError};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum VerFsBackend {
    Local {
        base_dir: String,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum VerKVBackend {
    Embedded,
    Redis { connection_string: String },
}

enum ContextAction<'a> {
    SetFs(&'a str, &'a VerFsBackend),
    SetKv(&'a str, &'a VerKVBackend),
    SwitchCtx(&'a str),
    UpdateFs(&'a str, &'a VerFsBackend),
    UpdateKv(&'a str, &'a VerKVBackend),
    RemoveCtx(&'a str),
}

#[derive(Debug, Serialize)]
struct CtxDescription<'a> {
    fs: &'a VerFsBackend,
    kv: &'a VerKVBackend,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VerContext {
    pub fs: HashMap<String, VerFsBackend>,
    pub kv: HashMap<String, VerKVBackend>,
    pub current: String,
}

impl Default for VerContext {
    fn default() -> Self {
        Self {
            fs: HashMap::from([(
                "default".to_string(),
                VerFsBackend::Local {
                    base_dir: env::current_dir()
                        .expect("Should be able to infer current directory")
                        .to_string_lossy()
                        .to_string(),
                },
            )]),
            kv: HashMap::from([("default".to_string(), VerKVBackend::Embedded)]),
            current: "default".to_string(),
        }
    }
}

impl VerContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_file() -> Result<(Self, PathBuf), VerError> {
        let resolved = global_context()?;
        if !resolved.exists() {
            return Ok((Self::new(), resolved));
        }
        let s = fs::read_to_string(&resolved)?;
        let new: Self = serde_json::from_str(&s)?;
        new.validate_default()?;
        Ok((new, resolved))
    }

    pub fn list_ctx(&self) -> Vec<String> {
        let fs_keys: HashSet<&str> = self.fs.keys().map(|s| s.as_str()).collect();
        let kv_keys: HashSet<&str> = self.kv.keys().map(|s| s.as_str()).collect();
        let intersection: Vec<String> = fs_keys
            .intersection(&kv_keys)
            .map(|s| s.to_string())
            .collect();
        intersection
    }

    pub fn describe_ctx(&self, context_name: &str) -> Result<String, VerError> {
        self.validate_action(ContextAction::SwitchCtx(context_name))?;
        let fs = self.fs.get(context_name).unwrap();
        let kv = self.kv.get(context_name).unwrap();
        let des = serde_json::to_string_pretty(&CtxDescription { kv, fs })?;
        Ok(des)
    }

    pub fn get_backends(
        &self,
        context_name: Option<&str>,
    ) -> Result<(VerFsBackend, VerKVBackend), VerError> {
        if let Some(ctx) = context_name {
            self.validate_action(ContextAction::SwitchCtx(ctx))?;
            let fs = self.fs.get(ctx).unwrap();
            let kv = self.kv.get(ctx).unwrap();
            return Ok((fs.clone(), kv.clone()));
        }
        let fs = self.fs.get(&self.current).unwrap();
        let kv = self.kv.get(&self.current).unwrap();
        Ok((fs.clone(), kv.clone()))
    }

    pub fn add_fs(&mut self, context_name: String, backend: VerFsBackend) -> Result<(), VerError> {
        self.validate_action(ContextAction::SetFs(&context_name, &backend))?;
        self.fs.insert(context_name, backend);
        Ok(())
    }

    pub fn add_kv(&mut self, context_name: String, backend: VerKVBackend) -> Result<(), VerError> {
        self.validate_action(ContextAction::SetKv(&context_name, &backend))?;
        self.kv.insert(context_name, backend);
        Ok(())
    }

    pub fn remove_ctx(&mut self, context_name: &str) -> Result<(), VerError> {
        self.validate_action(ContextAction::RemoveCtx(context_name))?;
        self.kv.remove(context_name);
        self.fs.remove(context_name);
        if self.current == context_name {
            self.current = "default".into();
        }
        Ok(())
    }

    pub fn update_kv(
        &mut self,
        context_name: String,
        new_backend: VerKVBackend,
    ) -> Result<(), VerError> {
        self.validate_action(ContextAction::UpdateKv(&context_name, &new_backend))?;
        self.kv
            .entry(context_name)
            .and_modify(|v| *v = new_backend.clone())
            .or_insert(new_backend);
        Ok(())
    }

    pub fn update_fs(
        &mut self,
        context_name: String,
        new_backend: VerFsBackend,
    ) -> Result<(), VerError> {
        self.validate_action(ContextAction::UpdateFs(&context_name, &new_backend))?;
        self.fs
            .entry(context_name)
            .and_modify(|v| *v = new_backend.clone())
            .or_insert(new_backend);
        Ok(())
    }

    pub fn switch(&mut self, context_name: String) -> Result<(), VerError> {
        self.validate_action(ContextAction::SwitchCtx(&context_name))?;
        self.current = context_name;
        Ok(())
    }

    fn validate_action<'a>(&self, action: ContextAction<'a>) -> Result<(), VerError> {
        match action {
            ContextAction::SetFs(name, backend) | ContextAction::UpdateFs(name, backend) => {
                if name == "default" {
                    return Err("`default` is the default context and cannot be modified".into());
                }
                if let Some(kv) = self.kv.get(name)
                    && matches!(kv, &VerKVBackend::Embedded)
                    && !matches!(backend, &VerFsBackend::Local { base_dir: _ })
                {
                    return Err(
                        "Cannot set a non-local filesystem backend when the KV store is embedded"
                            .into(),
                    );
                }
            }
            ContextAction::SetKv(name, backend) | ContextAction::UpdateKv(name, backend) => {
                if name == "default" {
                    return Err("`default` is the default context and cannot be modified".into());
                }
                if let Some(fs) = self.fs.get(name)
                    && !matches!(fs, &VerFsBackend::Local { base_dir: _ })
                    && matches!(backend, &VerKVBackend::Embedded)
                {
                    return Err(
                        "Cannot set an embedded KV store when the filesystem backend is non-local"
                            .into(),
                    );
                }
            }
            ContextAction::SwitchCtx(name) => {
                if !self.fs.contains_key(name) || !self.kv.contains_key(name) {
                    return Err(format!("{name} not found among KV and/or FS backends").into());
                }
            }
            ContextAction::RemoveCtx(name) => {
                if name == "default" {
                    return Err("`default` is the default context and cannot be removed".into());
                }
            }
        }

        Ok(())
    }

    fn validate_default(&self) -> Result<(), VerError> {
        if !self.fs.contains_key("default") || !self.kv.contains_key("default") {
            return Err("`default` is the default context and cannot be removed".into());
        }
        let def_kv = self.kv.get("default").unwrap();
        let def_fs = self.fs.get("default").unwrap();
        if !matches!(def_kv, &VerKVBackend::Embedded)
            || !matches!(def_fs, &VerFsBackend::Local { base_dir: _ })
        {
            return Err("`default` is the default context and cannot be modified".into());
        }
        Ok(())
    }
}

pub fn build_fs(backend: &VerFsBackend) -> Result<Arc<dyn Fs>, VerError> {
    let fs: Arc<dyn Fs> = match backend {
        VerFsBackend::Azure {
            account_name,
            account_key,
            endpoint,
            container_name,
        } => {
            let object_store = get_object_store(blobsdb::GetObjectStoreInput::Azure {
                account_name: Some(account_name.to_owned()),
                account_key: Some(account_key.to_owned()),
                endpoint: Some(endpoint.to_owned()),
                container_name: container_name.to_owned(),
            })?;
            Arc::new(ObjectStoreFs::new(object_store))
        }
        VerFsBackend::Gcs {
            service_account_credentials_path,
            bucket,
        } => {
            let object_store = get_object_store(blobsdb::GetObjectStoreInput::Gcs {
                service_account_credentials_path: service_account_credentials_path.to_owned(),
                bucket: bucket.to_owned(),
            })?;
            Arc::new(ObjectStoreFs::new(object_store))
        }
        VerFsBackend::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            bucket,
        } => {
            let object_store = get_object_store(blobsdb::GetObjectStoreInput::S3 {
                access_key_id: Some(access_key_id.to_owned()),
                secret_access_key: Some(secret_access_key.to_owned()),
                region: Some(region.to_owned()),
                endpoint: Some(endpoint.to_owned()),
                bucket: bucket.to_owned(),
            })?;
            Arc::new(ObjectStoreFs::new(object_store))
        }
        VerFsBackend::Local { base_dir } => Arc::new(DiskFs::new(base_dir)),
    };
    Ok(fs)
}
