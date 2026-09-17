use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    env, fs, io,
    path::{Path, PathBuf},
    sync::OnceLock,
    time::UNIX_EPOCH,
};
use uuid::Uuid;
extern crate alloc;
use alloc::vec::Vec as AllocVec;

use crate::{
    branches::{get_branch_id, get_current_branch},
    commons::resolve_ver_dir_path,
    errors::VerError,
    init::{REDB_PATH, init_blobs_db},
};

pub const METADATA_TABLE: TableDefinition<[u8; 20], &[u8]> = TableDefinition::new("metadata");
pub const FINGERPRINT_TABLE: TableDefinition<[u8; 20], [u8; 24]> =
    TableDefinition::new("fingerprints");
pub const CHECKPOINT_TABLE: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("checkpoints");
pub static UUID_NAMESPACE: OnceLock<Uuid> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize)]
pub struct MetadataEntry {
    pub parent_hash: Option<[u8; 32]>,
    pub current_hash: [u8; 32],
    pub is_text_file: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub modified: Option<Vec<[u8; 20]>>,
    pub deleted: Option<Vec<[u8; 20]>>,
    pub added: Option<Vec<[u8; 20]>>,
}

#[derive(Debug)]
struct CheckpointIR {
    checkpoint: Checkpoint,
    added_id_to_path: Option<HashMap<[u8; 20], String>>,
    modified_id_to_path: Option<HashMap<[u8; 20], String>>,
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

fn get_fingerprints() -> Result<HashMap<[u8; 20], [u8; 24]>, VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;

    let tx = redb.begin_read()?;

    let res = tx.open_table(FINGERPRINT_TABLE);
    let existing_fingerprints = match res {
        Ok(table) => {
            let all_entries = table.iter()?;
            let mut map = HashMap::<[u8; 20], [u8; 24]>::new();
            for entry in all_entries {
                let (k, v) = entry?;
                map.insert(k.value(), v.value());
            }
            map
        }
        Err(e) => match e {
            TableError::TableDoesNotExist(_) => HashMap::new(),
            _ => return Err(e.into()),
        },
    };

    Ok(existing_fingerprints)
}

fn get_checkpoint(paths: Vec<PathBuf>, branch_id: u32) -> Result<CheckpointIR, VerError> {
    let stored_fingerprints = get_fingerprints()?;
    let mut current_fingerprints = HashMap::<[u8; 20], [u8; 24]>::new();
    let mut id_to_path: HashMap<[u8; 20], String> = HashMap::new();
    for p in paths {
        let fl = p.to_string_lossy().into_owned();
        let entry_id = make_entry_id(get_file_id(&fl), branch_id);
        let fingerprint = get_file_fingerprint(&fl)?;
        current_fingerprints.insert(entry_id, fingerprint);
        id_to_path.insert(entry_id, fl);
    }

    // // the case in which no fingerprints
    // // have been recorded yet is a no-op
    if stored_fingerprints.is_empty() {
        return Ok(CheckpointIR {
            checkpoint: Checkpoint {
                modified: None,
                deleted: None,
                added: Some(stored_fingerprints.iter().map(|(k, _)| *k).collect()),
            },
            modified_id_to_path: None,
            added_id_to_path: Some(id_to_path),
        });
    }

    let old_set: HashSet<[u8; 20]> = stored_fingerprints.iter().map(|(k, _)| *k).collect();
    let new_set: HashSet<[u8; 20]> = current_fingerprints.iter().map(|(k, _)| *k).collect();

    let deleted: HashSet<[u8; 20]> = old_set.difference(&new_set).copied().collect();
    let new: HashSet<[u8; 20]> = new_set.difference(&old_set).copied().collect();
    let intersection: HashSet<[u8; 20]> = new_set.intersection(&old_set).copied().collect();

    let mut changed: Vec<[u8; 20]> = Vec::new();
    let mut modified_path_to_id: HashMap<[u8; 20], String> = HashMap::new();

    for i in intersection {
        let new_entry = current_fingerprints.get(&i).unwrap();
        let old_entry = stored_fingerprints.get(&i).unwrap();
        if new_entry != old_entry {
            changed.push(i);
            modified_path_to_id.insert(i, id_to_path[&i].clone());
        }
    }

    let added_id_to_path = if !new.is_empty() {
        Some(
            new.iter()
                .map(|v| (*v, id_to_path[v].clone()))
                .collect::<HashMap<[u8; 20], String>>(),
        )
    } else {
        None
    };

    Ok(CheckpointIR {
        checkpoint: Checkpoint {
            modified: Some(changed),
            deleted: Some(deleted.iter().copied().collect()),
            added: Some(new.iter().copied().collect()),
        },
        added_id_to_path,
        modified_id_to_path: Some(modified_path_to_id),
    })
}

// pub fn store_file(file_path: &str, branch_id: u32) -> Result<(), VerError> {
//     // open databases
//     let current_dir = env::current_dir()?;
//     let resolved = resolve_ver_dir_path(current_dir)?;
//     let redb = Database::open(&resolved.join(REDB_PATH))?;
//     let blobdb = init_blobs_db(&resolved)?;

//     // get file and entry ID
//     let file_id = get_file_id(file_path);
//     let entry_id = make_entry_id(file_id, branch_id);
//     let fingerprint = get_file_fingerprint(file_path)?;

//     // see if entry already exists
//     let tx = redb.begin_read()?;
//     let res = tx.open_table(METADATA_TABLE);
//     let maybe_entry = match res {
//         Ok(table) => table.get(entry_id)?,
//         Err(e) => match e {
//             TableError::TableDoesNotExist(_) => None,
//             _ => return Err(e.into()),
//         },
//     };

//     // modify entry if it already
//     // exists
//     if let Some(entry) = maybe_entry {
//         let mut e: MetadataEntry = postcard::from_bytes(entry.value())?;
//         if e.fingerprint == fingerprint {
//             return Ok(());
//         }

//         let content = fs::read(file_path)?;
//         let hash = blake3::hash(&content).as_bytes().to_owned();

//         if e.current_hash == hash {
//             // error in the fingerprint-based
//             // change detection
//             return Ok(());
//         }

//         println!("detected a modification in {file_path}, re-ingesting");
//         let buf = if content.len() >= 8192 {
//             &content[..8192]
//         } else {
//             &content
//         };
//         let is_text = if buf.contains(&0) {
//             false
//         } else {
//             std::str::from_utf8(buf).is_ok()
//         };
//         e.parent_hash = Some(e.current_hash);
//         e.current_hash = hash;
//         e.is_text_file = is_text;
//         e.fingerprint = fingerprint;
//         let compressed: AllocVec<u8> = postcard::to_allocvec(&e)?;

//         // insert metadata
//         let write_tx = redb.begin_write()?;
//         {
//             let mut tb = write_tx.open_table(METADATA_TABLE)?;
//             tb.insert(entry_id, compressed.as_slice())?;
//         }
//         write_tx.commit()?;

//         // insert in blob db
//         blobdb.put(hash, content)?;

//         return Ok(());
//     }

//     println!("detected a new file: {}", file_path);
//     // compute hash
//     let content = fs::read(file_path)?;
//     let hash = blake3::hash(&content).as_bytes().to_owned();

//     // get details (is_text_file, fingerprint) since they do
//     // not already exist
//     let buf = if content.len() >= 8192 {
//         &content[..8192]
//     } else {
//         &content
//     };
//     let is_text = if buf.contains(&0) {
//         false
//     } else {
//         std::str::from_utf8(buf).is_ok()
//     };

//     // create and compress entry
//     let entry = MetadataEntry {
//         parent_hash: None,
//         current_hash: hash,
//         is_text_file: is_text,
//         fingerprint,
//     };
//     let compressed: AllocVec<u8> = postcard::to_allocvec(&entry)?;

//     // insert metadata
//     let write_tx = redb.begin_write()?;
//     {
//         let mut tb = write_tx.open_table(METADATA_TABLE)?;
//         tb.insert(entry_id, compressed.as_slice())?;
//     }
//     write_tx.commit()?;

//     // insert in blob db
//     blobdb.put(hash, content)?;
//     Ok(())
// }

fn normalize_path(path: &str) -> io::Result<PathBuf> {
    let normalized = path.replace("\\", "/");
    let fp = Path::new(&normalized);
    let can = fp.canonicalize()?;
    return Ok(can);
}

// pub fn add<S: AsRef<str>>(paths: Vec<S>) -> Result<(), VerError> {
//     // get branch ID
//     let branch = get_current_branch()?;
//     let branch_id = get_branch_id(&branch)?;

//     for p in paths {
//         let norm = normalize_path(p.as_ref())?;
//         let fp = Path::new(&norm);
//         if fp.is_file() {
//             store_file(&norm, branch_id)?;
//         } else {
//             let walker = ignore::WalkBuilder::new(fp)
//                 .add_custom_ignore_filename(".verignore")
//                 .filter_entry(|d| {
//                     if d.path().is_file() {
//                         if d.path()
//                             .ancestors()
//                             .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
//                         {
//                             return false;
//                         }
//                         return true;
//                     } else {
//                         if d.path().ends_with(".ver")
//                             || d.path().ends_with(".ver/")
//                             || d.path()
//                                 .ancestors()
//                                 .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
//                         {
//                             return false;
//                         }
//                         return true;
//                     }
//                 })
//                 .build();
//             for entry in walker {
//                 let entry = entry?;
//                 if entry.path().is_file() {
//                     let p = normalize_path(&entry.path().to_string_lossy().to_string())?;
//                     store_file(&p, branch_id)?;
//                 }
//             }
//         }
//     }
//     Ok(())
// }
