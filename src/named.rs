use redb::{Database, ReadableDatabase, TableDefinition};

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

pub fn get_snapshot_by_name(redb: &Database, name: &str) -> Result<[u8; 32], VerError> {
    let tx = redb.begin_read()?;
    let table = tx.open_table(NAMES_TABLE)?;
    let maybe_entry = table.get(name)?;

    if let Some(entry) = maybe_entry {
        return Ok(entry.value());
    }

    Err(format!("No snapshot named {} was found", name).into())
}
