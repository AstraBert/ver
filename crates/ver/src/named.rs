use std::sync::Arc;

use redb::{Database, ReadableDatabase, TableDefinition};
use ver_kv::KVStore;

use crate::errors::VerError;

pub const NAMES_TABLE: TableDefinition<&str, [u8; 32]> = TableDefinition::new("names");

pub fn name_snapshot(redb: &Database, snapshot_id: [u8; 32], name: &str) -> Result<(), VerError> {
    let tx = redb.begin_write()?;
    {
        let mut table = tx.open_table(NAMES_TABLE)?;
        table.insert(name, snapshot_id)?;
    }

    tx.commit()?;

    Ok(())
}

pub fn get_snapshot_by_name(kv: &Arc<dyn KVStore>, name: &str) -> Result<[u8; 32], VerError> {
    let maybe_entry = kv.get_name(name)?;

    if let Some(entry) = maybe_entry {
        return Ok(entry);
    }

    Err(format!("No snapshot named {} was found", name).into())
}
