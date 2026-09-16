use redb::{Database, ReadableDatabase, ReadableTableMetadata};
use std::{env, fs};

use crate::{
    commons::resolve_ver_dir_path,
    errors::VerError,
    init::{BRANCHES_TABLE, CURRENT_BRANCH_FILE, REDB_PATH},
};

pub fn get_current_branch() -> Result<String, VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let path = resolved.join(CURRENT_BRANCH_FILE);
    let branch = fs::read_to_string(path)?;
    Ok(branch)
}

pub fn get_branch_id(branch: &str) -> Result<u32, VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let db = Database::open(resolved.join(REDB_PATH))?;
    let txn = db.begin_read()?;
    let table = txn.open_table(BRANCHES_TABLE)?;
    let branch_id = table.get(branch)?;
    if let Some(bi) = branch_id {
        return Ok(bi.value());
    }
    Err("No branch ID associated with this branch".into())
}

pub fn create_branch(branch_name: &str) -> Result<(), VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let db = Database::open(resolved.join(REDB_PATH))?;
    let txn = db.begin_read()?;
    let table = txn.open_table(BRANCHES_TABLE)?;
    // ids are 0-based, so the next ID
    // to be added equals the length
    // of the table
    let new_id = table.len()?;
    let write_txn = db.begin_write()?;
    {
        let mut tb = write_txn.open_table(BRANCHES_TABLE)?;
        tb.insert(branch_name, new_id as u32)?;
    }
    fs::write(resolved.join(CURRENT_BRANCH_FILE), branch_name)?;

    Ok(())
}
