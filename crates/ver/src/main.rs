use std::io;

use clap::{Parser, Subcommand};
use ver::{
    context::{FSType, KVType, read_context},
    errors::VerError,
    init::init,
    snap::{files_snap, full_snap},
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
    }
}
