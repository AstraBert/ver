use std::{borrow::Cow, io};

use clap::{Parser, Subcommand};
use ver::{
    commons::PagerWriter,
    context::{FSType, KVType, read_context},
    diff::diff,
    errors::VerError,
    goto::goto,
    init::init,
    log::log,
    merge::{merge_end, merge_pick, merge_run},
    show::show_file,
    snap::{files_snap, full_snap},
    sync::sync,
};

#[derive(Debug, Subcommand)]
enum ContextCommands {
    /// Scaffold a new context
    /// in the ~/.ver/contexts.toml
    /// file.
    Scaffold {
        /// Name of the context
        context_name: String,
        /// Filesystem backend to scaffold for
        #[arg(long, short)]
        fs: FSType,
        /// Key-value store backend to scaffold for
        #[arg(long, short)]
        kv: KVType,
    },
    /// Switch to a new context
    Switch {
        /// Name of the context to switch to
        context_name: String,
    },
    /// Delete a context
    Delete {
        /// Name of the context to delete
        context_name: String,
    },
    /// Show the current context
    Show {
        /// Whether to display the full context spec
        /// as JSON. If disabled, only displays the name
        /// of the context.
        #[arg(long, short, default_value_t = false)]
        json: bool,
    },
    /// List all the available contexts
    List {
        /// Whether to display the full context spec
        /// as JSON. If disabled, only displays the name
        /// of the context.
        #[arg(long, short, default_value_t = false)]
        json: bool,
    },
    /// Initialize ver (globally or for the current context).
    Init {
        /// Initialize ver globally. If not provided, it will
        /// try to initializa the project associated
        /// with the current context
        #[arg(long, short, default_value_t = false)]
        global: bool,
    },
}

#[derive(Debug, Subcommand)]
enum MergeCommands {
    /// Run a merge between the
    /// current ver context
    /// and another context on the same project,
    /// starting from their latest snapshot
    /// ID or from a provided ID/name.
    Run {
        /// Name of this
        /// merge run
        name: String,
        /// Target context
        /// for the merge run
        #[arg(short, long)]
        from: String,
        /// Snapshot ID or name
        /// from the current project
        #[arg(short, long, default_value = None)]
        our_snapshot: Option<String>,
        /// Snapshot ID or name
        /// for the incoming merge
        #[arg(short, long, default_value = None)]
        their_snapshot: Option<String>,
    },

    /// After resolving conflicts where possible,
    /// pick a version of the conflicting
    /// file (theirs or ours)
    Pick {
        /// Name of the merge
        /// operation this pick
        /// refers to
        name: String,
        /// For this path,
        /// pick our conflicting
        /// version (can be used for
        /// multiple paths).
        #[arg(short, long)]
        our: Vec<String>,
        /// For this path,
        /// pick their conflicting
        /// version (can be used for
        /// multiple paths).
        #[arg(short, long)]
        their: Vec<String>,
    },

    /// After picking the
    /// conflicting versions
    /// for the merge, finalize
    /// it
    End {
        /// Name of the merge
        /// to finalize
        name: String,
    },
}

#[derive(Debug, Subcommand)]
enum SnapCommands {
    /// Take a full snapshot of the project
    Full {
        /// Name of the snapshot
        #[arg(long, short)]
        name: String,
        /// Description of the snapshot
        #[arg(long, short, default_value = None)]
        description: Option<String>,
    },
    /// Patch an existing snapshot by adding files to it
    Patch {
        /// Name or ID of the snapshot
        #[arg(long, short)]
        identifier: String,
        /// File to be added to the snapshot.
        /// Can be used multiple times
        #[arg(long, short)]
        file: Vec<String>,
    },
    /// Create a snapshot from specific files instead
    /// of the full project
    From {
        /// Name of the snapshot
        #[arg(long, short)]
        name: String,
        /// Description of the snapshot
        #[arg(long, short, default_value = None)]
        description: Option<String>,
        /// File to be added to the snapshot.
        /// Can be used multiple times
        #[arg(long, short)]
        file: Vec<String>,
    },
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Context-related operations
    Ctx {
        #[command(subcommand)]
        cmd: ContextCommands,
    },
    /// Snapshot-related operations
    Snap {
        #[command(subcommand)]
        cmd: SnapCommands,
    },
    /// Display the snapshots history associated with the current
    /// context
    Log {
        /// Maximum number of log entries to display
        #[arg(long, short, default_value = None)]
        limit: Option<usize>,
        /// Whether to tolerate errors (and thus an incomplete
        /// history) when reading the history
        #[arg(long, default_value_t = false)]
        incomplete_ok: bool,
    },
    /// Revert the whole project or some specified paths
    /// to a previous snapshot
    Goto {
        /// Snapshot ID or name
        snapshot_id_or_name: String,
        /// Relative path to a specific file to restore.
        /// Can be used multiple times.
        #[arg(long, short)]
        path: Vec<String>,
        /// Do not delete files
        /// if they are not in
        /// the target snapshot (only
        /// applicable if the target
        /// snapshot was a full snapshot)
        #[arg(long, short, default_value_t = false)]
        no_delete: bool,
    },
    /// Show the content of a file, optionally
    /// specifing the snapshot to read the
    /// file from
    Show {
        /// Relative path of the file
        /// whose contents are to display
        path: String,
        /// ID or name of the snapshot
        /// to read the file from.
        /// Defaults to the current head.
        #[arg(long, short, default_value = None)]
        snapshot: Option<String>,
        /// Offset (in bytes)
        #[arg(long, short, default_value = None)]
        offset: Option<usize>,
        /// Maximum number of bytes to read
        #[arg(long, short, default_value = None)]
        limit: Option<usize>,
    },
    /// Show the diff on a file
    /// between two snapshots
    Diff {
        /// Path of the file
        /// to diff
        path: String,
        /// Head snapshot (defaults
        /// to current head)
        #[arg(long, default_value = None)]
        head: Option<String>,
        /// Base snapshot to diff
        /// against
        #[arg(long, short)]
        base: String,
    },
    /// Sync a context with another.
    /// Assumes both are associated
    /// with the same project and
    /// the target context is behind
    /// in snapshot history
    Sync {
        #[arg(short, long, default_value = None)]
        source: Option<String>,
        #[arg(short, long)]
        target: String,
        #[arg(short, long, default_value_t = false)]
        from_scratch: bool,
    },
    /// Merge two contexts
    /// associated with the same
    /// project.
    /// Assumes the snapshot
    /// history of the contexts
    /// diverged: if not, use `sync`.
    Merge {
        #[command(subcommand)]
        merge_cmd: MergeCommands,
    },
}

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<(), VerError> {
    let args = Args::parse();

    match args.cmd {
        Commands::Ctx { cmd: ctx_cmd } => match ctx_cmd {
            ContextCommands::Delete { context_name } => {
                let mut ctx = read_context()?;
                ctx.delete(&context_name)?;
                ctx.to_global_context()?;
                println!("Deleted context {context_name}");
                Ok(())
            }
            ContextCommands::Switch { context_name } => {
                let mut ctx = read_context()?;
                ctx.switch(&context_name)?;
                ctx.to_global_context()?;
                println!("Switched to {context_name}");
                Ok(())
            }
            ContextCommands::Scaffold {
                context_name,
                fs,
                kv,
            } => {
                let mut ctx = read_context()?;
                ctx.scaffold(&context_name, fs, kv)?;
                ctx.to_global_context()?;
                println!("Scaffolded {context_name}");
                Ok(())
            }
            ContextCommands::Show { json } => {
                let ctx = read_context()?;
                if !json {
                    println!(
                        "{}",
                        ctx.current
                            .unwrap_or("WARNING: no current context".to_string())
                    );
                } else {
                    let opt = ctx.current_context();
                    if opt.is_none() {
                        println!("WARNING: no current context");
                        return Ok(());
                    }
                    let (fs_des, kv_des) = opt.unwrap();
                    let val = serde_json::json!({
                        "fs": &serde_json::to_value(&fs_des)?,
                        "kv": &serde_json::to_value(&kv_des)?,
                    });
                    println!("{}", serde_json::to_string_pretty(&val)?,);
                }

                Ok(())
            }
            ContextCommands::List { json } => {
                let ctx = read_context()?;
                if !json {
                    println!(
                        "{}",
                        ctx.fs
                            .keys()
                            .map(|s| Cow::Borrowed(s.as_str()))
                            .collect::<Vec<Cow<str>>>()
                            .join("\n")
                    );
                    return Ok(());
                }
                println!("{}", serde_json::to_string_pretty(&ctx)?);
                Ok(())
            }
            ContextCommands::Init { global } => {
                init(global)?;
                if global {
                    println!("Successfully initialized ver globally");
                } else {
                    println!("Successfully initialized ver for the current context");
                }
                Ok(())
            }
        },
        Commands::Snap { cmd: cmd_snap } => match cmd_snap {
            SnapCommands::Full { name, description } => {
                let out = io::stdout();
                let err = io::stderr();

                let mut outlock = out.lock();
                let mut errlock = err.lock();
                full_snap(&name, description.as_deref(), &mut outlock, &mut errlock)
            }
            SnapCommands::Patch { identifier, file } => {
                let out = io::stdout();
                let err = io::stderr();

                let mut outlock = out.lock();
                let mut errlock = err.lock();

                files_snap(&identifier, None, file, true, &mut outlock, &mut errlock)
            }
            SnapCommands::From {
                name,
                description,
                file,
            } => {
                let out = io::stdout();
                let err = io::stderr();

                let mut outlock = out.lock();
                let mut errlock = err.lock();

                files_snap(
                    &name,
                    description.as_deref(),
                    file,
                    false,
                    &mut outlock,
                    &mut errlock,
                )
            }
        },
        Commands::Log {
            limit,
            incomplete_ok,
        } => {
            let pager = minus::Pager::new();
            let mut stdout = PagerWriter::new(pager);
            log(incomplete_ok, limit, &mut stdout)?;
            minus::page_all(stdout.pager).map_err(|e| e.into())
        }
        Commands::Goto {
            snapshot_id_or_name,
            path,
            no_delete,
        } => {
            let stdout = io::stdout();
            let mut lock = stdout.lock();
            let stderr = io::stderr();
            let mut errlock = stderr.lock();
            goto(
                &snapshot_id_or_name,
                path,
                !no_delete,
                &mut lock,
                &mut errlock,
            )
        }
        Commands::Show {
            path,
            snapshot,
            offset,
            limit,
        } => {
            let pager = minus::Pager::new();
            let mut stdout = PagerWriter::new(pager);
            show_file(&path, snapshot, offset, limit, &mut stdout)?;
            minus::page_all(stdout.pager).map_err(|e| e.into())
        }
        Commands::Diff { path, head, base } => {
            let pager = minus::Pager::new();
            let mut stdout = PagerWriter::new(pager);
            diff(&path, head, base, &mut stdout)?;
            minus::page_all(stdout.pager).map_err(|e| e.into())
        }
        Commands::Sync {
            source,
            target,
            from_scratch,
        } => {
            let stdout = io::stdout();
            let mut lock = stdout.lock();
            sync(source, target, from_scratch, &mut lock)
        }
        Commands::Merge { merge_cmd } => match merge_cmd {
            MergeCommands::Run {
                name,
                from,
                our_snapshot,
                their_snapshot,
            } => {
                let pager = minus::Pager::new();
                let mut stderr = PagerWriter::new(pager);
                {
                    let stdout = io::stdout();
                    let mut lock = stdout.lock();
                    merge_run(
                        from,
                        &name,
                        our_snapshot,
                        their_snapshot,
                        &mut lock,
                        &mut stderr,
                    )?;
                } // lock released here
                if stderr.has_writes {
                    minus::page_all(stderr.pager)?;
                }
                Ok(())
            }
            MergeCommands::Pick { name, our, their } => {
                let stderr = io::stderr();
                let mut errlock = stderr.lock();
                let stdout = io::stdout();
                let mut lock = stdout.lock();
                merge_pick(
                    &name,
                    our.iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>()
                        .as_slice(),
                    their
                        .iter()
                        .map(|s| s.as_str())
                        .collect::<Vec<&str>>()
                        .as_slice(),
                    &mut lock,
                    &mut errlock,
                )
            }
            MergeCommands::End { name } => {
                let stderr = io::stderr();
                let mut errlock = stderr.lock();
                let stdout = io::stdout();
                let mut lock = stdout.lock();
                merge_end(&name, &mut lock, &mut errlock)
            }
        },
    }
}
