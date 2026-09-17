mod branches;
mod commons;
mod errors;
mod files;
mod init;

use clap::{Parser, Subcommand};

use crate::{branches::create_branch, init::initialize};

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

    /// Add current changes to ver's database
    Add {
        /// Path to the files and folders whose
        /// changes you want to include
        paths: Vec<String>,
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
        Commands::Add { paths: _ } => {
            println!("Unsupported");
            Ok(())
        }
        Commands::Init { directory } => initialize(directory.as_deref()),
        Commands::Branch { branchcmd } => match branchcmd {
            BranchCommands::New { branch_name } => create_branch(&branch_name),
        },
    }
}
