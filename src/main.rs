mod commons;
mod errors;
mod files;
mod init;
mod log;

use clap::{Parser, Subcommand};

use crate::{
    files::{goto, snap},
    init::initialize,
    log::log,
};

#[derive(Debug, Subcommand)]
enum Commands {
    /// Initialize a ver repository
    Init {
        /// Directory to initialize
        /// as a ver repository.
        /// Defaults to the current directory
        directory: Option<String>,
    },

    /// Create a snapshot of the current state
    /// of the repository
    Snap {
        /// Snapshot description
        description: String,
    },

    /// Revert to the state of the repository
    /// at a specific snapshot ID
    Goto {
        /// ID of the snapshot to revert to
        snapshot_id: String,
        /// Path to the files and folders
        /// you want to revert to `snapshot_id`.
        /// Reverts all files if not provided.
        paths: Option<Vec<String>>,
    },

    /// Show the snapshot history
    /// up until now
    Log {
        /// Show only the last N snapshots
        #[arg(long)]
        head: Option<usize>,
    },
}

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<(), errors::VerError> {
    let args = Args::parse();

    match args.cmd {
        Commands::Snap { description } => snap(&description),
        Commands::Init { directory } => initialize(directory.as_deref()),
        Commands::Log { head } => log(head),
        Commands::Goto { snapshot_id, paths } => goto(&snapshot_id, paths),
    }
}
