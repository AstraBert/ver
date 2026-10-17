use std::{error::Error, fmt::Display};

#[derive(Debug)]
pub enum FsError {
    Read(String),
    Write(String),
    List(String),
    Delete(String),
    CreateDir(String),
}

unsafe impl Send for FsError {}

unsafe impl Sync for FsError {}

impl Display for FsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CreateDir(s) => write!(f, "An error occurred with directory creation: {}", s),
            Self::Write(s) => write!(f, "An error occurred while writing: {}", s),
            Self::Read(s) => write!(f, "An error occurred while reading: {}", s),
            Self::Delete(s) => write!(f, "An error occurred while deleting: {}", s),
            Self::List(s) => write!(f, "An error occurred while listing: {}", s),
        }
    }
}

impl Error for FsError {}
