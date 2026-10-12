use blobsdb::Chunk;
use futures_util::FutureExt;
use redb::{Database, ReadableDatabase, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{
    any::Any,
    collections::HashMap,
    env, fs,
    io::{self, Write},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, OnceLock, mpsc},
};
use tokio::{runtime::Runtime, sync::Semaphore};
extern crate alloc;
use alloc::vec::Vec as AllocVec;

use crate::{
    commons::{get_head, resolve_ver_dir_path, write_head},
    errors::VerError,
    init::{REDB_PATH, SNAPSHOT_HISTORY_FILE, init_blobs_db},
    named::{get_snapshot_by_name, name_snapshot},
};

pub const SNAPSHOT_TABLE: TableDefinition<[u8; 32], &[u8]> = TableDefinition::new("snapshots");
pub const MAX_INFLIGHT_WRITES: usize = 16;
pub const MAX_INFLIGHT_READS: usize = 32;

pub static RT: OnceLock<Runtime> = OnceLock::new();

pub fn rt() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    })
}

pub fn panic_msg(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_string()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Snapshot {
    pub parent_ids: Vec<[u8; 32]>,
    pub repository_state: HashMap<String, Vec<Chunk>>,
}

fn store_files() -> Result<(blake3::Hash, AllocVec<u8>), VerError> {
    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let repo = resolved
        .parent()
        .unwrap()
        .canonicalize()
        .unwrap_or_else(|_| resolved.parent().unwrap().to_path_buf());
    let blobdb = Arc::new(init_blobs_db(&resolved)?);

    let files = collect_files(&[&*repo.to_string_lossy()])?;

    let mut repo_state: HashMap<String, Vec<Chunk>> = HashMap::new();

    let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_WRITES));
    let (tx, rx) = mpsc::channel::<(usize, Result<Vec<Chunk>, String>)>();

    for (i, f) in files.clone().into_iter().enumerate() {
        let tx = tx.clone();
        // Blocks the calling (sync) thread until a slot is free
        let permit = rt()
            .block_on(Arc::clone(&sem).acquire_owned())
            .expect("semaphore closed");
        let db = blobdb.clone();

        rt().spawn(async move {
            let _permit = permit; // moved in, released when the task ends
            let res = match AssertUnwindSafe(async move { db.put(&f).await })
                .catch_unwind()
                .await
            {
                Ok(r) => r,
                Err(payload) => Err(panic_msg(payload)),
            };
            let _ = tx.send((i, res));
        });
    }

    drop(tx);

    let mut out: Vec<Option<_>> = (0..files.len()).map(|_| None).collect();
    for (i, res) in rx {
        out[i] = Some(res);
    }
    let results = out
        .into_iter()
        .map(|slot| slot.unwrap_or(Err("Lost file".to_string())))
        .collect::<Vec<Result<Vec<Chunk>, String>>>();

    for (n, r) in results.into_iter().enumerate() {
        if r.is_err() {
            return Err(r.unwrap_err().into());
        }
        let rel = files[n].strip_prefix(&repo).expect("Should strip prefix");
        let chunks = r.unwrap();
        repo_state.insert(rel.to_string_lossy().to_string(), chunks);
    }

    let maybe_parent_id = get_head(&resolved)?;

    // normal snapshot don't have more than
    // one parents, so it is safe to read the
    // parent snapshot from the head of the
    // repository
    let parent_ids = if let Some(parent_id) = maybe_parent_id {
        vec![parent_id]
    } else {
        vec![]
    };

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
    Ok(can)
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
                        true
                    } else {
                        if d.path().ends_with(".ver")
                            || d.path().ends_with(".ver/")
                            || d.path()
                                .ancestors()
                                .any(|a| a.ends_with(".ver") || a.ends_with(".ver/"))
                        {
                            return false;
                        }
                        true
                    }
                })
                .build();
            for entry in walker {
                let entry = entry?;
                if entry.path().is_file() {
                    let p = normalize_path(entry.path().to_string_lossy().as_ref())?;
                    collected.push(p);
                }
            }
        }
    }

    Ok(collected)
}

pub fn snap<O: io::Write>(
    name: &str,
    message: Option<&str>,
    std_out: &mut O,
) -> Result<(), VerError> {
    let (snapshot_id, snapshot) = store_files()?;

    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let redb = Database::open(resolved.join(REDB_PATH))?;

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

    write_head(&resolved, &snapshot_id)?;
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

    writeln!(std_out, "Saved current snapshot under: {}\n", snapshot_id)?;

    Ok(())
}

pub fn goto<S: AsRef<str>>(snapshot_id: &str, paths: Option<Vec<S>>) -> Result<(), VerError> {
    // open databases
    let current_dir = env::current_dir()?;
    let resolved = resolve_ver_dir_path(current_dir)?;
    let repo = resolved
        .parent()
        .unwrap()
        .canonicalize()
        .unwrap_or_else(|_| resolved.parent().unwrap().to_path_buf());
    let redb = Database::open(resolved.join(REDB_PATH))?;
    let blobdb = Arc::new(init_blobs_db(&resolved)?);

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
        let state = decompressed
            .repository_state
            .into_iter()
            .map(|(k, v)| (k, Arc::new(v)))
            .collect::<HashMap<String, Arc<Vec<Chunk>>>>();

        if let Some(ref pas) = paths {
            let ps = collect_files(pas)?;
            let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
            let (tx, rx) = mpsc::channel::<(PathBuf, Result<Vec<u8>, String>)>();

            for p in ps {
                let rel = p
                    .strip_prefix(&repo)
                    .expect("Repository should always contain its files");
                let str_p = &*rel.to_string_lossy();
                if let Some(f) = state.get(str_p).cloned() {
                    let tx = tx.clone();
                    // Blocks the calling (sync) thread until a slot is free
                    let permit = rt()
                        .block_on(Arc::clone(&sem).acquire_owned())
                        .expect("semaphore closed");
                    let db = blobdb.clone();

                    rt().spawn(async move {
                        let _permit = permit; // moved in, released when the task ends
                        let res = match AssertUnwindSafe(async move { db.get(f).await })
                            .catch_unwind()
                            .await
                        {
                            Ok(r) => r,
                            Err(payload) => Err(panic_msg(payload)),
                        };
                        let _ = tx.send((p, res));
                    });
                } else {
                    // not in snapshot or does not exist, so try deleting
                    // or skip if it does not exist
                    let res = fs::remove_file(p);
                    match res {
                        Ok(_) => {}
                        Err(e) => match e.kind() {
                            io::ErrorKind::NotFound => continue,
                            _ => return Err(e.into()),
                        },
                    }
                }
            }

            drop(tx);

            for (fl, res) in rx {
                if res.is_err() {
                    return Err(res.unwrap_err().into());
                }
                fs::write(fl, res.unwrap())?;
            }
        } else {
            let ps = collect_files(&[&*repo.to_string_lossy()])?;
            let sem = Arc::new(Semaphore::new(MAX_INFLIGHT_READS));
            let (tx, rx) = mpsc::channel::<(PathBuf, Result<Vec<u8>, String>)>();

            for p in ps {
                let rel = p
                    .strip_prefix(&repo)
                    .expect("Repository should always contain its files");
                let str_p = &*rel.to_string_lossy();
                if let Some(f) = state.get(str_p).cloned() {
                    let tx = tx.clone();
                    // Blocks the calling (sync) thread until a slot is free
                    let permit = rt()
                        .block_on(Arc::clone(&sem).acquire_owned())
                        .expect("semaphore closed");
                    let db = blobdb.clone();

                    rt().spawn(async move {
                        let _permit = permit; // moved in, released when the task ends
                        let res = match AssertUnwindSafe(async move { db.get(f).await })
                            .catch_unwind()
                            .await
                        {
                            Ok(r) => r,
                            Err(payload) => Err(panic_msg(payload)),
                        };
                        let _ = tx.send((p, res));
                    });
                } else {
                    // not in snapshot, so delete
                    let res = fs::remove_file(p);
                    // ignore not found files
                    if res.is_err_and(|e| matches!(e.kind(), io::ErrorKind::NotFound)) {}
                }
            }

            drop(tx);

            for (fl, res) in rx {
                if res.is_err() {
                    return Err(res.unwrap_err().into());
                }
                fs::write(fl, res.unwrap())?;
            }
        }

        write_head(&resolved, &blake3::Hash::from_bytes(hash))?;

        return Ok(());
    }

    Err(format!("Could not find snapshot with ID: {}", snapshot_id).into())
}
