use std::{collections::HashMap, fs};

use ver::context::{FilesystemContext, KVStoreContext, VerContext};

fn main() {
    let ctx: VerContext = VerContext {
        fs: HashMap::from([
            (
                "local".to_string(),
                FilesystemContext::Local {
                    base_directory: "test".to_string(),
                },
            ),
            (
                "s3".to_string(),
                FilesystemContext::S3 {
                    access_key_id: "a".to_string(),
                    secret_access_key: "s".to_string(),
                    region: "r".to_string(),
                    endpoint: "e".to_string(),
                    bucket: "b".to_string(),
                },
            ),
            (
                "gcs".to_string(),
                FilesystemContext::Gcs {
                    service_account_credentials_path: "test".to_string(),
                    bucket: "b".to_string(),
                },
            ),
            (
                "azure".to_string(),
                FilesystemContext::Azure {
                    account_name: "a".to_string(),
                    account_key: "k".to_string(),
                    endpoint: "e".to_string(),
                    container_name: "c".to_string(),
                },
            ),
        ]),
        kv: HashMap::from([
            ("local".to_string(), KVStoreContext::Embedded),
            (
                "redis".to_string(),
                KVStoreContext::Redis {
                    uri: "u".to_string(),
                },
            ),
        ]),
        current: Some("test".to_string()),
    };

    let s = toml::to_string_pretty(&ctx).expect("should not fail");
    fs::write("test.toml", &s).expect("Should not fail");
}
