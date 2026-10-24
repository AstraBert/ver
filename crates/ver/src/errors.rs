use std::{array::TryFromSliceError, error::Error, fmt::Display, io, string::FromUtf8Error};
use ver_fs::errors::FsError;
use ver_kv::{RedisError, errors::KVError};

/// Internal error that abstracts errors from other
/// crates
#[derive(Debug, Clone)]
pub struct VerError {
    pub message: String,
}

impl Display for VerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for VerError {}

impl From<TryFromSliceError> for VerError {
    fn from(value: TryFromSliceError) -> Self {
        Self {
            message: format!("Error while converting slice to [u8; 32]: {value}"),
        }
    }
}

impl From<&str> for VerError {
    fn from(value: &str) -> Self {
        Self {
            message: value.to_string(),
        }
    }
}

impl From<String> for VerError {
    fn from(value: String) -> Self {
        Self { message: value }
    }
}

impl From<io::Error> for VerError {
    fn from(value: io::Error) -> Self {
        Self {
            message: format!("IO error: {}", value),
        }
    }
}

impl From<std::fmt::Error> for VerError {
    fn from(value: std::fmt::Error) -> Self {
        Self {
            message: format!("Formatting error: {}", value),
        }
    }
}

impl From<FromUtf8Error> for VerError {
    fn from(value: FromUtf8Error) -> Self {
        Self {
            message: format!("Error while converting raw bytes to UTF-8: {}", value),
        }
    }
}

impl From<serde_json::Error> for VerError {
    fn from(value: serde_json::Error) -> Self {
        Self {
            message: format!("JSON serialization/deserialization error: {}", value),
        }
    }
}

impl From<toml::de::Error> for VerError {
    fn from(value: toml::de::Error) -> Self {
        Self {
            message: format!("TOML deserialization error: {}", value),
        }
    }
}

impl From<redb::DatabaseError> for VerError {
    fn from(value: redb::DatabaseError) -> Self {
        Self {
            message: format!(
                "An error occurred while opening the redb database: {}",
                value
            ),
        }
    }
}

impl From<toml::ser::Error> for VerError {
    fn from(value: toml::ser::Error) -> Self {
        Self {
            message: format!("TOML serialization error: {}", value),
        }
    }
}

impl From<RedisError> for VerError {
    fn from(value: RedisError) -> Self {
        Self {
            message: format!("Redis error: {}", value),
        }
    }
}

impl From<FsError> for VerError {
    fn from(value: FsError) -> Self {
        Self {
            message: format!("FS error: {}", value),
        }
    }
}

impl From<KVError> for VerError {
    fn from(value: KVError) -> Self {
        Self {
            message: format!("KV error: {}", value),
        }
    }
}
