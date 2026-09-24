use blake3::HexError;
use ignore::Error as IgnoreError;
use postcard::Error as PostcardError;
use redb::{
    CommitError, DatabaseError, Error as RedbError, StorageError, TableError, TransactionError,
};
use rocksdb::Error as RocksdbError;
use std::{error::Error, fmt::Display, io};

/// Internal error that abstracts errors from other
/// sources
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

impl From<HexError> for VerError {
    fn from(value: HexError) -> Self {
        Self {
            message: format!("Invalid snapshot ID: {}", value),
        }
    }
}

impl From<RocksdbError> for VerError {
    fn from(value: RocksdbError) -> Self {
        Self {
            message: format!("An error occurred with RocksDB: {}", value),
        }
    }
}

impl From<RedbError> for VerError {
    fn from(value: RedbError) -> Self {
        Self {
            message: format!(
                "An error occurred while creating the redb database: {}",
                value
            ),
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

impl From<DatabaseError> for VerError {
    fn from(value: DatabaseError) -> Self {
        Self {
            message: format!(
                "An error occurred while opening the redb database: {}",
                value
            ),
        }
    }
}

impl From<TransactionError> for VerError {
    fn from(value: TransactionError) -> Self {
        Self {
            message: format!("An error occurred with a redb transaction: {}", value),
        }
    }
}

impl From<TableError> for VerError {
    fn from(value: TableError) -> Self {
        Self {
            message: format!("An error occurred while opening a redb table: {}", value),
        }
    }
}

impl From<StorageError> for VerError {
    fn from(value: StorageError) -> Self {
        Self {
            message: format!(
                "An error occurred while attempting to write to redb: {}",
                value
            ),
        }
    }
}

impl From<CommitError> for VerError {
    fn from(value: CommitError) -> Self {
        Self {
            message: format!(
                "An error occurred while committing a write to redb: {}",
                value
            ),
        }
    }
}

impl From<io::Error> for VerError {
    fn from(value: io::Error) -> Self {
        Self {
            message: format!("IO error: {}", value),
        }
    }
}

impl From<PostcardError> for VerError {
    fn from(value: PostcardError) -> Self {
        Self {
            message: format!(
                "Error while serializing or deserializing with postcard: {}",
                value
            ),
        }
    }
}

impl From<fs_extra::error::Error> for VerError {
    fn from(value: fs_extra::error::Error) -> Self {
        return Self {
            message: format!(
                "Error while copying the .ver folder to the new destination: {}",
                value
            ),
        };
    }
}

impl From<IgnoreError> for VerError {
    fn from(value: IgnoreError) -> Self {
        Self {
            message: format!("Error while walking the directory recursively: {}", value),
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

#[cfg(feature = "minus")]
impl From<minus::MinusError> for VerError {
    fn from(value: minus::MinusError) -> Self {
        Self {
            message: format!("Error while displaying the snapshot history: {}", value),
        }
    }
}
