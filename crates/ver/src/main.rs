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
    show::show_file,
    snap::{files_snap, full_snap},
    sync::sync,
};

#[derive(Debug, Subcommand)]
enum ContextCommands {
    /// Scaffold a new context
    /// in the `~/.ver/contexts.toml`
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
}

#[derive(Debug, Subcommand)]
enum SnapCommands {
    /// Take a full snapshot of the repository
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
    /// of the full repository
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
    /// Initialize ver (globally or for a specific context).
    Init {
        /// Initialize ver globally. If not provided, it will
        /// try to initializa the repository associated
        /// with the current context\
        #[arg(long, short, default_value_t = false)]
        global: bool,
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
        Commands::Init { global } => {
            init(global)?;
            if global {
                println!("Successfully initialized ver globally");
            } else {
                println!("Successfully initialized ver for the current context");
            }
            Ok(())
        }
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
    }
}
