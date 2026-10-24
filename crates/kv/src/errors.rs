use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum KVError {
    Put(String),
    Get(String),
    Copy(String),
}

impl Display for KVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Get(s) => write!(f, "GET error: {}", s),
            Self::Put(s) => write!(f, "PUT error: {}", s),
            Self::Copy(s) => write!(f, "Copy error: {}", s),
        }
    }
}

impl Error for KVError {}
