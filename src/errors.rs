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
            message: format!(
                "An error occurred while creating a redb write transaction: {}",
                value
            ),
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

impl From<IgnoreError> for VerError {
    fn from(value: IgnoreError) -> Self {
        Self {
            message: format!("Error while walking the directory recursively: {}", value),
        }
    }
}
