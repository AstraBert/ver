use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableError};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    str::FromStr,
};
extern crate alloc;
use alloc::vec::Vec as AllocVec;

use crate::{
    commons::resolve_ver_dir_path,
    errors::VerError,
    init::{REDB_PATH, SNAPSHOT_HISTORY_FILE, init_blobs_db},
    named::{get_snapshot_by_name, name_snapshot},
};

pub const SNAPSHOT_TABLE: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("snapshots");

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub parent_ids: Vec<[u8; 32]>,
    pub repository_state: HashMap<String, [u8; 32]>,
}

fn store_files() -> Result<(blake3::Hash, AllocVec<u8>), VerError> {
    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let repo = resolved.parent().unwrap();
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    let files = collect_files(&[&*repo.to_string_lossy()])?;

    let mut repo_state: HashMap<String, [u8; 32]> = HashMap::new();

    for f in files {
        let content = fs::read(&f)?;
        let hash = blake3::hash(&content).as_bytes().to_owned();

        repo_state.insert(f.to_string_lossy().to_string(), hash);

        // insert in blob db
        blobdb.put(hash, content)?;
    }

    let rx = redb.begin_read()?;
    let snap_tab = rx.open_table(SNAPSHOT_TABLE);

    let parent_ids = match snap_tab {
        Ok(table) => {
            let last = table.last()?;
            if let Some(l) = last {
                vec![l.0.value()]
            } else {
                vec![]
            }
        }
        Err(e) => match e {
            TableError::TableDoesNotExist(_) => vec![],
            _ => return Err(e.into()),
        },
    };

    rx.close()?;

    let snapshot = Snapshot {
        parent_ids,
        repository_state: repo_state,
    };

    let compressed: AllocVec<u8> = postcard::to_allocvec(&snapshot)?;

    let snapshot_id = blake3::hash(&compressed);

    Ok((snapshot_id, compressed))
}

fn normalize_path(path: &str) -> io::Result<PathBuf> {
    let normalized = path.replace("\\", "/");
    let fp = Path::new(&normalized);
    let can = fp.canonicalize()?;
    return Ok(can);
}

fn collect_files<S: AsRef<str>>(paths: &[S]) -> Result<Vec<PathBuf>, VerError> {
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

    Ok(collected)
}

pub fn snap(name: &str, message: Option<&str>) -> Result<(), VerError> {
    let (snapshot_id, snapshot) = store_files()?;

    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;

    let tx = redb.begin_write()?;
    {
        let mut table = tx.open_table(SNAPSHOT_TABLE)?;

        table.insert(snapshot_id.as_bytes().to_owned(), snapshot.as_slice())?;
    }
    tx.commit()?;

    let mut fl = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(resolved.join(SNAPSHOT_HISTORY_FILE))?;
    writeln!(
        fl,
        "{} - {} ({}){}",
        chrono::Utc::now().format("%d/%m/%Y %H:%M:%S"),
        name,
        snapshot_id,
        message.map_or("".to_string(), |m| format!(
            ": {}",
            m.replace("\n", " ").replace("\t", " ")
        ))
    )?;

    name_snapshot(&redb, snapshot_id.as_bytes().to_owned(), name)?;

    println!("Saved current snapshot under: {}\n", snapshot_id);

    Ok(())
}

pub fn goto<S: AsRef<str>>(snapshot_id: &str, paths: Option<Vec<S>>) -> Result<(), VerError> {
    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(&resolved.join(REDB_PATH))?;
    let blobdb = init_blobs_db(&resolved)?;

    let maybe_hash = blake3::Hash::from_str(snapshot_id);

    let hash = match maybe_hash {
        Ok(h) => h.as_bytes().to_owned(),
        Err(_) => get_snapshot_by_name(&redb, snapshot_id)?,
    };

    let tx = redb.begin_read()?;
    let table = tx.open_table(SNAPSHOT_TABLE)?;
    let maybe_entry = table.get(hash)?;

    if let Some(entry) = maybe_entry {
        let decompressed: Snapshot = postcard::from_bytes(entry.value())?;

        if let Some(ref pas) = paths {
            let ps = collect_files(pas)?;
            for p in ps {
                let str_p = &*p.to_string_lossy();
                if let Some(f) = decompressed.repository_state.get(str_p) {
                    let opt = blobdb.get(f)?;
                    if let Some(content) = opt {
                        fs::write(str_p, content)?;
                    } else {
                        eprintln!("Could not find snapshotted content for: {}", str_p);
                    }
                } else {
                    // not in snapshot, so delete
                    fs::remove_file(str_p)?;
                }
            }
        } else {
            let repo = &*resolved.parent().unwrap().to_string_lossy();
            let ps = collect_files(&[repo])?;
            for p in ps {
                let str_p = &*p.to_string_lossy();
                if let Some(f) = decompressed.repository_state.get(str_p) {
                    let opt = blobdb.get(f)?;
                    if let Some(content) = opt {
                        fs::write(str_p, content)?;
                    } else {
                        eprintln!("Could not find snapshotted content for: {}", str_p);
                    }
                } else {
                    // not in snapshot, so delete
                    fs::remove_file(str_p)?;
                }
            }
        }

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
