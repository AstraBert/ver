mod branches;
mod commons;
mod errors;
mod files;
mod init;
mod log;

use clap::{Parser, Subcommand};

use crate::{
    branches::create_branch,
    files::{goto, snap},
    init::initialize,
    log::log,
};

#[derive(Debug, Subcommand)]
enum BranchCommands {
    /// Create a new branch
    /// and switch to it
    New {
        /// name of the branch to create
        branch_name: String,
    },
}

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
        /// Path to the files and folders whose
        /// snapshots you want to be taken
        paths: Vec<String>,
        /// Snapshot description
        #[arg(long, short)]
        description: String,
    },

    /// Revert to the state of the repository
    /// at a specific snapshot ID
    Goto {
        /// ID of the snapshot to revert to
        snapshot_id: String,
    },

    /// Show the snapshot history
    /// up until now
    Log {
        /// Show the last N snapshots
        #[arg(long)]
        head: Option<usize>,
    },

    /// Branch-related commands
    Branch {
        #[command(subcommand)]
        branchcmd: BranchCommands,
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
        Commands::Snap { paths, description } => snap(paths, &description),
        Commands::Init { directory } => initialize(directory.as_deref()),
        Commands::Branch { branchcmd } => match branchcmd {
            BranchCommands::New { branch_name } => create_branch(&branch_name),
        },
        Commands::Log { head } => log(head),
        Commands::Goto { snapshot_id } => goto(&snapshot_id),
    }
}
