use std::{sync::Arc, time::Duration};

use object_store::{
    BackoffConfig, ObjectStore, RetryConfig, aws::AmazonS3Builder, azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder, local::LocalFileSystem, memory::InMemory,
};

pub enum GetObjectStoreInput {
    /// only for testing
    InMemory,
    Local,
    S3 {
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        region: Option<String>,
        endpoint: Option<String>,
        bucket: String,
    },
    Gcs {
        service_account_credentials_path: String,
        bucket: String,
    },
    Azure {
        account_name: Option<String>,
        account_key: Option<String>,
        endpoint: Option<String>,
        container_name: String,
    },
}

pub fn get_object_store(input: GetObjectStoreInput) -> Result<Arc<dyn ObjectStore>, String> {
    let retry_config = RetryConfig {
        backoff: BackoffConfig {
            init_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(10),
            base: 2_f64,
        },
        max_retries: 3,
        retry_timeout: Duration::from_secs(30),
    };

    match input {
        GetObjectStoreInput::InMemory => {
            let store = InMemory::new();
            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Local => {
            let store = LocalFileSystem::new();
            Ok(Arc::new(store))
        }
        GetObjectStoreInput::S3 {
            access_key_id,
            secret_access_key,
            region,
            endpoint,
            bucket,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if access_key_id.is_none()
                && secret_access_key.is_none()
                && region.is_none()
                && endpoint.is_none()
            {
                let store = AmazonS3Builder::from_env()
                    .with_retry(retry_config)
                    .with_bucket_name(bucket)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| format!("Error while building S3 storage from env: {e}"))?;
                return Ok(Arc::new(store));
            }

            let builder = AmazonS3Builder::new()
                .with_retry(retry_config)
                .with_access_key_id(access_key_id.unwrap_or_default())
                .with_bucket_name(bucket)
                .with_endpoint(endpoint.unwrap_or_default())
                .with_region(region.unwrap_or_default())
                .with_secret_access_key(secret_access_key.unwrap_or_default())
                .with_client_options(client_options);
            let store = builder
                .build()
                .map_err(|e| format!("Error while building S3 storage: {e}"))?;

            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Gcs {
            service_account_credentials_path,
            bucket,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            let store = GoogleCloudStorageBuilder::new()
                .with_retry(retry_config)
                .with_service_account_path(service_account_credentials_path)
                .with_bucket_name(bucket)
                .with_client_options(client_options)
                .build()
                .map_err(|e| format!("Error while buiding GCP storage: {e}"))?;

            Ok(Arc::new(store))
        }
        GetObjectStoreInput::Azure {
            account_name,
            account_key,
            endpoint,
            container_name,
        } => {
            let client_options = object_store::ClientOptions::new()
                .with_allow_http(true)
                .with_pool_max_idle_per_host(32)
                .with_http2_keep_alive_interval(std::time::Duration::from_secs(30))
                .with_timeout(std::time::Duration::from_secs(60));

            if account_key.is_none() && account_name.is_none() && endpoint.is_none() {
                let store = MicrosoftAzureBuilder::from_env()
                    .with_retry(retry_config)
                    .with_container_name(container_name)
                    .with_client_options(client_options)
                    .build()
                    .map_err(|e| {
                        format!("An error occurred while building Azure storage from env: {e}")
                    })?;

                return Ok(Arc::new(store));
            }

            let store = MicrosoftAzureBuilder::new()
                .with_retry(retry_config)
                .with_access_key(account_key.unwrap_or_default())
                .with_account(account_name.unwrap_or_default())
                .with_endpoint(endpoint.unwrap_or_default())
                .with_container_name(container_name)
                .with_client_options(client_options)
                .build()
                .map_err(|e| format!("An error occurred while building Azure storage: {e}"))?;

            Ok(Arc::new(store))
        }
    }
}
