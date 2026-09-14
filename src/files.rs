use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{env, fs, io, path::Path, sync::OnceLock, time::UNIX_EPOCH};
use uuid::Uuid;
extern crate alloc;
use alloc::vec::Vec as AllocVec;

use crate::{
    branches::{get_branch_id, get_current_branch},
    commons::resolve_ver_dir_path,
    errors::VerError,
    init::{REDB_PATH, VER_DIRECTORY, init_blobs_db},
};

pub const METADATA_TABLE: TableDefinition<[u8; 20], &[u8]> = TableDefinition::new("metadata");
pub const FINGERPRINTS_TABLE: TableDefinition<[u8; 20], [u8; 24]> =
    TableDefinition::new("fingerprints");
pub static UUID_NAMESPACE: OnceLock<Uuid> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataEntry {
    pub parent_hash: Option<[u8; 32]>,
    pub current_hash: [u8; 32],
    pub is_text_file: bool,
}

pub fn uuid_namespace() -> &'static Uuid {
    UUID_NAMESPACE.get_or_init(|| Uuid::NAMESPACE_DNS)
}

pub fn get_file_id(file: &str) -> u128 {
    let file_id = Uuid::new_v5(uuid_namespace(), file.as_bytes());
    file_id.as_u128()
}

pub fn get_file_fingerprint(file: &str) -> Result<[u8; 24], io::Error> {
    let stat = fs::metadata(file)?;
    let size = stat.len();
    let modified = stat.modified()?;
    let unix_time = modified
        .duration_since(UNIX_EPOCH)
        .expect("Expected file to be modified after the Unix epoch")
        .as_nanos();
    let mut combined = [0u8; 24];
    combined[..16].copy_from_slice(&unix_time.to_be_bytes());
    combined[16..].copy_from_slice(&size.to_be_bytes());
    Ok(combined)
}

pub fn make_entry_id(file_id: u128, branch_id: u32) -> [u8; 20] {
    let fi = file_id.to_be_bytes();
    let bi = branch_id.to_be_bytes();
    let mut combined = [0u8; 20];
    combined[..16].copy_from_slice(&fi);
    combined[16..].copy_from_slice(&bi);

    combined
}

pub fn store_file(file_path: &str) -> Result<(), VerError> {
    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    // compute hash
    let content = fs::read(file_path)?;
    let hash = blake3::hash(&content).as_bytes().to_owned();

    // get branch ID
    let branch = get_current_branch()?;
    let branch_id = get_branch_id(&branch)?;

    // get file and entry ID
    let file_id = get_file_id(file_path);
    let entry_id = make_entry_id(file_id, branch_id);

    // see if entry already exists
    let tx = redb.begin_read()?;
    let table = tx.open_table(METADATA_TABLE)?;
    let maybe_entry = table.get(entry_id)?;

    // modify entry if it already
    // exists
    if let Some(entry) = maybe_entry {
        let mut e: MetadataEntry = postcard::from_bytes(entry.value())?;
        if e.current_hash == hash {
            // error in the fingerprint-based
            // change detection
            return Ok(());
        }
        let buf = &content[..8192];
        let is_text = if buf.contains(&0) {
            false
        } else {
            std::str::from_utf8(buf).is_ok()
        };
        e.parent_hash = Some(e.current_hash);
        e.current_hash = hash;
        e.is_text_file = is_text;
        let compressed: AllocVec<u8> = postcard::to_allocvec(&e)?;

        // insert metadata
        let write_tx = redb.begin_write()?;
        {
            let mut tb = write_tx.open_table(METADATA_TABLE)?;
            tb.insert(entry_id, compressed.as_slice())?;
        }
        write_tx.commit()?;

        // insert in blob db
        blobdb.put(hash, content)?;

        return Ok(());
    }

    // get details (is_text_file, fingerprint) since they do
    // not already exist
    let buf = &content[..8192];
    let is_text = if buf.contains(&0) {
        false
    } else {
        std::str::from_utf8(buf).is_ok()
    };

    // create and compress entry
    let entry = MetadataEntry {
        parent_hash: None,
        current_hash: hash,
        is_text_file: is_text,
    };
    let compressed: AllocVec<u8> = postcard::to_allocvec(&entry)?;

    // insert metadata
    let write_tx = redb.begin_write()?;
    {
        let mut tb = write_tx.open_table(METADATA_TABLE)?;
        tb.insert(entry_id, compressed.as_slice())?;
    }
    write_tx.commit()?;

    // insert in blob db
    blobdb.put(hash, content)?;
    Ok(())
}

pub fn add(paths: Vec<&str>) -> Result<(), VerError> {
    for p in paths {
        let fp = Path::new(p);
        if fp.is_file() {
            store_file(p)?;
        } else {
            let walker = ignore::WalkBuilder::new(p)
                .add_custom_ignore_filename(".verignore")
                .filter_entry(|d| {
                    if d.path().is_file() {
                        if d.path()
                            .ancestors()
                            .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
                        {
                            return false;
                        }
                        return true;
                    } else {
                        if d.path().ends_with(".ver")
                            || d.path().ends_with(".ver/")
                            || d.path()
                                .ancestors()
                                .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
                        {
                            return false;
                        }
                        return true;
                    }
                })
                .build();
            for entry in walker {
                let entry = entry?;
                if entry.path().is_file() {
                    store_file(&entry.path().to_string_lossy().to_string())?;
                }
            }
        }
    }
    Ok(())
}
