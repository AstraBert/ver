use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    env, fs,
    io::{self, Write},
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
    init::{REDB_PATH, SNAPSHOT_HISTORY_FILE, init_blobs_db},
};

pub const FINGERPRINT_TABLE: TableDefinition<[u8; 20], &[u8]> =
    TableDefinition::new("fingerprints");
pub const SNAPSHOT_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("snapshots");
pub static UUID_NAMESPACE: OnceLock<Uuid> = OnceLock::new();

#[derive(Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    pub modified: Option<Vec<[u8; 20]>>,
    pub deleted: Option<Vec<String>>,
    pub added: Option<Vec<[u8; 20]>>,
}

#[derive(Debug)]
struct CheckpointIR {
    checkpoint: Checkpoint,
    added_id_to_path: Option<HashMap<[u8; 20], String>>,
    modified_id_to_path: Option<HashMap<[u8; 20], String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Fingerprint {
    pub id: [u8; 24],
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub branch_id: u32,
    pub added: Option<Vec<(String, [u8; 32])>>,
    pub modified: Option<Vec<(String, [u8; 32])>>,
    pub deleted: Option<Vec<String>>,
}

impl CheckpointIR {
    fn is_empty(&self) -> bool {
        let mut no_added = true;
        let mut no_modifed = true;
        let mut no_deleted = true;
        if let Some(ref added) = self.checkpoint.added {
            no_added = added.is_empty();
        }
        if let Some(ref changed) = self.checkpoint.modified {
            no_modifed = changed.is_empty();
        }
        if let Some(ref deleted) = self.checkpoint.deleted {
            no_deleted = deleted.is_empty();
        }

        no_added && no_modifed && no_deleted
    }
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

fn get_fingerprints() -> Result<HashMap<[u8; 20], Fingerprint>, VerError> {
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;

    let tx = redb.begin_read()?;

    let res = tx.open_table(FINGERPRINT_TABLE);
    let existing_fingerprints = match res {
        Ok(table) => {
            let all_entries = table.iter()?;
            let mut map = HashMap::<[u8; 20], Fingerprint>::new();
            for entry in all_entries {
                let (k, v) = entry?;
                let decompressed: Fingerprint = postcard::from_bytes(&v.value())?;
                map.insert(k.value(), decompressed);
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

fn get_checkpoint(
    paths: Vec<PathBuf>,
    branch_id: u32,
) -> Result<(CheckpointIR, HashMap<[u8; 20], [u8; 24]>), VerError> {
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

    // the case in which no fingerprints
    // have been recorded yet is a no-op
    if stored_fingerprints.is_empty() {
        return Ok((
            CheckpointIR {
                checkpoint: Checkpoint {
                    modified: None,
                    deleted: None,
                    added: Some(current_fingerprints.iter().map(|(k, _)| *k).collect()),
                },
                modified_id_to_path: None,
                added_id_to_path: Some(id_to_path),
            },
            current_fingerprints,
        ));
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
        if new_entry != &old_entry.id {
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

    Ok((
        CheckpointIR {
            checkpoint: Checkpoint {
                modified: Some(changed),
                deleted: Some(
                    deleted
                        .iter()
                        .map(|d| stored_fingerprints.get(d).unwrap().path.clone())
                        .collect(),
                ),
                added: Some(new.iter().copied().collect()),
            },
            added_id_to_path,
            modified_id_to_path: Some(modified_path_to_id),
        },
        current_fingerprints,
    ))
}

fn store_files(
    branch_id: u32,
    checkpoint: CheckpointIR,
    fingerprints: HashMap<[u8; 20], [u8; 24]>,
) -> Result<Option<(String, Snapshot)>, VerError> {
    if checkpoint.is_empty() {
        return Ok(None);
    }

    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    let mut added_hash: Option<Vec<(String, [u8; 32])>> = None;
    let mut modified_hash: Option<Vec<(String, [u8; 32])>> = None;

    if let Some(ref added) = checkpoint.checkpoint.added
        && let Some(added_id_to_path) = checkpoint.added_id_to_path
    {
        for f in added {
            let path = added_id_to_path.get(f).unwrap().to_owned();
            let content = fs::read(&path)?;
            let hash = blake3::hash(&content).as_bytes().to_owned();

            added_hash
                .get_or_insert_with(Vec::new)
                .push((path.clone(), hash));

            let fg = fingerprints.get(f).unwrap().to_owned();
            let fingerprint = Fingerprint { id: fg, path: path };
            let fingerprint_compressed: AllocVec<u8> = postcard::to_allocvec(&fingerprint)?;

            // insert metadata and fingerprint
            let write_tx = redb.begin_write()?;
            {
                let mut fb = write_tx.open_table(FINGERPRINT_TABLE)?;

                fb.insert(f, fingerprint_compressed.as_slice())?;
            }
            write_tx.commit()?;

            // insert in blob db
            blobdb.put(hash, content)?;
        }
    }

    if let Some(ref modified) = checkpoint.checkpoint.modified
        && let Some(modified_id_to_path) = checkpoint.modified_id_to_path
    {
        for m in modified {
            let path = modified_id_to_path.get(m).unwrap().to_owned();
            let content = fs::read(&path)?;
            let hash = blake3::hash(&content).as_bytes().to_owned();

            modified_hash
                .get_or_insert_with(Vec::new)
                .push((path.clone(), hash));

            let fg = fingerprints.get(m).unwrap().to_owned();
            let fingerprint = Fingerprint { id: fg, path: path };
            let fingerprint_compressed: AllocVec<u8> = postcard::to_allocvec(&fingerprint)?;

            let wx = redb.begin_write()?;
            {
                let mut ftable = wx.open_table(FINGERPRINT_TABLE)?;

                ftable.insert(m, fingerprint_compressed.as_slice())?;
            }
            wx.commit()?;

            blobdb.put(hash, content)?;
        }
    }

    if let Some(ref deleted) = checkpoint.checkpoint.deleted {
        for d in deleted {
            let entry_id = make_entry_id(get_file_id(d), branch_id);

            let wx = redb.begin_write()?;
            {
                let mut ftable = wx.open_table(FINGERPRINT_TABLE)?;

                ftable.remove(entry_id)?;
            }
            wx.commit()?;
        }
    }

    let snapshot = Snapshot {
        branch_id,
        deleted: checkpoint.checkpoint.deleted,
        added: added_hash,
        modified: modified_hash,
    };

    let snapshot_id = uuid::Uuid::new_v4().to_string();

    Ok(Some((snapshot_id, snapshot)))
}

fn normalize_path(path: &str) -> io::Result<PathBuf> {
    let normalized = path.replace("\\", "/");
    let fp = Path::new(&normalized);
    let can = fp.canonicalize()?;
    return Ok(can);
}

pub fn snap<S: AsRef<str>>(paths: Vec<S>, message: &str) -> Result<(), VerError> {
    // get branch ID
    let branch = get_current_branch()?;
    let branch_id = get_branch_id(&branch)?;
    let mut collected: Vec<PathBuf> = vec![];

    for p in paths {
        let norm = normalize_path(p.as_ref())?;
        if norm.is_file() {
            collected.push(norm);
        } else {
            let walker = ignore::WalkBuilder::new(norm)
                .add_custom_ignore_filename(".verignore")
                .hidden(false)
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
                    let p = normalize_path(&entry.path().to_string_lossy().to_string())?;
                    collected.push(p);
                }
            }
        }
    }

    let (checkpoint_ir, fingerprints) = get_checkpoint(collected, branch_id)?;
    let opt = store_files(branch_id, checkpoint_ir, fingerprints)?;

    if let Some((snapshot_id, snapshot)) = opt {
        let current_dir = env::current_dir()?;
        let resolved = resolve_ver_dir_path(current_dir)?;
        let redb = Database::open(&resolved.join(REDB_PATH))?;

        let tx = redb.begin_write()?;
        {
            let mut table = tx.open_table(SNAPSHOT_TABLE)?;
            let compressed: AllocVec<u8> = postcard::to_allocvec(&snapshot)?;

            table.insert(snapshot_id.as_str(), compressed.as_slice())?;
        }
        tx.commit()?;

        let mut fl = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(resolved.join(SNAPSHOT_HISTORY_FILE))?;
        writeln!(
            fl,
            "({}) {}: {}",
            chrono::Utc::now().format("%d/%m/%Y %H:%M:%S"),
            snapshot_id,
            message.replace("\n", " ").replace("\t", " ")
        )?;

        println!(
            "Saved snapshot {snapshot_id}.\nCreated {:?} files, modified {:?}, deleted {:?}.",
            snapshot.added.map_or(0, |f| f.len()),
            snapshot.modified.map_or(0, |f| f.len()),
            snapshot.deleted.map_or(0, |f| f.len())
        );
        return Ok(());
    }

    println!("No changes detected, nothing to do.");

    Ok(())
}

pub fn goto(snapshot_id: &str) -> Result<(), VerError> {
    // open databases
    let branch_id = get_branch_id(&get_current_branch()?)?;
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    let tx = redb.begin_read()?;
    let table = tx.open_table(SNAPSHOT_TABLE)?;
    let maybe_entry = table.get(snapshot_id)?;

    if let Some(entry) = maybe_entry {
        let decompressed: Snapshot = postcard::from_bytes(entry.value())?;
        if decompressed.branch_id != branch_id {
            return Err("Cannot revert to a snapshot outside of the current branch".into());
        }

        if let Some(added) = decompressed.added {
            for f in added {
                let opt = blobdb.get(f.1)?;
                if let Some(content) = opt {
                    fs::write(&f.0, content)?;
                    let fingerprint = get_file_fingerprint(&f.0)?;
                    let entry_id = make_entry_id(get_file_id(&f.0), branch_id);
                    let fingerprint_s = Fingerprint {
                        id: fingerprint,
                        path: f.0,
                    };
                    let compressed: AllocVec<u8> = postcard::to_allocvec(&fingerprint_s)?;
                    let tx = redb.begin_write()?;
                    {
                        let mut table = tx.open_table(FINGERPRINT_TABLE)?;
                        table.insert(entry_id, compressed.as_slice())?;
                    }
                    tx.commit()?;
                } else {
                    eprintln!("Could not find content for {} in {}", f.0, snapshot_id);
                }
            }
        }

        if let Some(modified) = decompressed.modified {
            for f in modified {
                let opt = blobdb.get(f.1)?;
                if let Some(content) = opt {
                    fs::write(&f.0, content)?;
                    let fingerprint = get_file_fingerprint(&f.0)?;
                    let entry_id = make_entry_id(get_file_id(&f.0), branch_id);
                    let fingerprint_s = Fingerprint {
                        id: fingerprint,
                        path: f.0,
                    };
                    let compressed: AllocVec<u8> = postcard::to_allocvec(&fingerprint_s)?;
                    let tx = redb.begin_write()?;
                    {
                        let mut table = tx.open_table(FINGERPRINT_TABLE)?;
                        table.insert(entry_id, compressed.as_slice())?;
                    }
                    tx.commit()?;
                } else {
                    eprintln!("Could not find content for {} in {}", f.0, snapshot_id);
                }
            }
        }

        if let Some(deleted) = decompressed.deleted {
            for f in deleted {
                if Path::new(&f).exists() {
                    fs::remove_file(&f)?;
                    let entry_id = make_entry_id(get_file_id(&f), branch_id);
                    let tx = redb.begin_write()?;
                    {
                        let mut table = tx.open_table(FINGERPRINT_TABLE)?;
                        table.remove(entry_id)?;
                    }
                    tx.commit()?;
                }
            }
        }

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
