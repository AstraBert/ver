use std::io;

use clap::{Parser, Subcommand};

use ver::{
    cp::copy as ver_copy,
    errors::VerError,
    files::{goto, snap},
    init::initialize,
    log::log,
    merge::{merge_end, merge_pick, merge_run},
    show::show_file,
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
        /// Name of the snapshot
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

    /// Merge-related sub-commands
    Merge {
        #[command(subcommand)]
        merge_cmd: MergeCommands,
    },

    /// Show the content of a file,
    /// providing its relative path
    /// and (optionally) the snapshot
    /// ID or name
    Show {
        /// Path to the file
        /// whose content you
        /// want to show
        path: String,
        /// ID or name of the
        /// snapshot that
        /// holds the content
        /// of the file
        #[arg(long, short, default_value = None)]
        snapshot_id: Option<String>,
        /// Offset (in bytes) for the file's content
        #[arg(long, short, default_value = None)]
        offset: Option<usize>,
        /// Maximum number of bytes to show,
        /// starting from the offset
        #[arg(long, short, default_value = None)]
        limit: Option<usize>,
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
        Commands::Log { head } => {
            let stdout = io::stdout();
            let handle = stdout.lock();
            log(head, handle)
        }
        Commands::Goto { snapshot_id, paths } => goto(&snapshot_id, paths),
        Commands::Cp {
            snapshot_id,
            destination,
        } => ver_copy(&snapshot_id, &destination),
        Commands::Show {
            path,
            snapshot_id,
            offset,
            limit,
        } => {
            let out = io::stdout();
            let handle = out.lock();
            show_file(&path, snapshot_id, offset, limit, handle)
        }
        Commands::Merge { merge_cmd } => match merge_cmd {
            MergeCommands::Run {
                name,
                from,
                our_snapshot,
                their_snapshot,
            } => merge_run(from.into(), &name, our_snapshot, their_snapshot),
            MergeCommands::Pick { name, our, their } => merge_pick(
                &name,
                our.iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>()
                    .as_slice(),
                &their
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<&str>>()
                    .as_slice(),
            ),
            MergeCommands::End { name } => merge_end(&name),
        },
    }
}
