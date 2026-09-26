use clap::{Parser, Subcommand};

use ver::{
    cp::copy as ver_copy,
    errors::VerError,
    files::{goto, snap},
    init::initialize,
    log::log,
    merge::merge_run,
};

#[derive(Debug, Subcommand)]
enum MergeCommands {
    /// Run a merge between the
    /// current ver repository
    /// and another repository,
    /// starting from their latest snapshot
    /// ID or from a provided ID/name.
    Run {
        /// Name of this
        /// merge run
        name: String,
        /// Repository the incoming
        /// merge is from
        #[arg(short, long)]
        from: String,
        /// Snapshot ID or name
        /// from the current repository
        #[arg(short, long, default_value = None)]
        our_snapshot: Option<String>,
        /// Snapshot ID or name
        /// for the incoming merge
        #[arg(short, long, default_value = None)]
        their_snapshot: Option<String>,
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
        /// Name of the snapshot, as a shorthand
        /// to revert to it
        name: String,
        /// Snapshot description
        #[arg(long, short, default_value = None)]
        description: Option<String>,
    },

    /// Revert to the state of the repository
    /// at a specific snapshot ID
    Goto {
        /// ID (or name) of the snapshot to revert to
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

    /// Create a copy
    /// of the current ver repository
    /// from a specific snapshot
    Cp {
        /// Snapshot ID or name
        snapshot_id: String,
        /// Destination where to copy
        /// the repository. Will be
        /// created if it does not
        /// already exists
        destination: String,
    },

    Merge {
        #[command(subcommand)]
        merge_cmd: MergeCommands,
    },
}

#[derive(Debug, Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Commands,
}

fn main() -> Result<(), VerError> {
    let args = Args::parse();

    match args.cmd {
        Commands::Snap { name, description } => snap(&name, description.as_deref()),
        Commands::Init { directory } => initialize(directory.as_deref()),
        Commands::Log { head } => log(head),
        Commands::Goto { snapshot_id, paths } => goto(&snapshot_id, paths),
        Commands::Cp {
            snapshot_id,
            destination,
        } => ver_copy(&snapshot_id, &destination),
        Commands::Merge { merge_cmd } => match merge_cmd {
            MergeCommands::Run {
                name,
                from,
                our_snapshot,
                their_snapshot,
            } => merge_run(from.into(), &name, our_snapshot, their_snapshot),
        },
    }
}
