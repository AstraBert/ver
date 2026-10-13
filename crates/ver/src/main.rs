use std::io;

use blobsdb::GetObjectStoreInput;
use clap::{Parser, Subcommand};

#[cfg(feature = "minus")]
use ver::commons;
use ver::{
    clone::clone_from_remote,
    commons::unwrap_maybe_secret,
    cp::copy as ver_copy,
    diff::diff_file,
    errors::VerError,
    files::{goto, snap},
    init::initialize,
    log::log,
    merge::{merge_end, merge_pick, merge_run},
    secrets::{add_recipient, get_secret, init_secrets, set_secret},
    show::show_file,
    sync::sync_with_remote_storage,
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

/// Secrets-related subcommands
#[derive(Debug, Subcommand)]
enum SecretsCommands {
    /// Initialize secrets management
    /// for ver globally.
    /// If you need multiple secrets
    /// managed with different keys,
    /// provide `--key-file`, otherwise
    /// a global key pair will be provisioned
    /// to manage all ver secrets.
    Init {
        /// Name of the recipient identity
        /// to associate with the
        /// key
        recipient_name: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to generate
        /// a new key pair for global
        /// management of ver-related secrets.
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
    /// Set a secret locally in your repository.
    /// Creates .versecrets.json at the root
    /// of your repository if it does not already
    /// exists
    Set {
        /// Secret name
        secret_name: String,
        /// Secret value
        secret_value: String,
    },
    /// Get the decrypted value of a secret,
    /// using either a private key provided
    /// under a key file or a the global
    /// key generated with `init`
    Get {
        secret_name: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to use the
        /// key pair for global
        /// management of ver-related secrets
        /// generated with `init`
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
    /// Add a new recipient identity
    /// that can access the secrets
    /// by providing their public
    /// key
    AddRecipient {
        /// Name to associate with the recipient
        /// identity
        recipient_name: String,
        /// Public key for the recipient
        public_key: String,
        /// File where a x25519 age key pair
        /// is stored to decrypt existing secrets
        /// and re-encrypt them with the new recipient.
        /// Leave empty to use the
        /// key pair for global
        /// management of ver-related secrets
        /// generated with `init`
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
}

/// Sync-related subcommands
#[derive(Debug, Subcommand)]
enum SyncCommands {
    /// Sync with S3.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    S3 {
        /// Name of the remote storage you are syncing with
        remote_name: String,
        /// Access key ID for the secret access key
        #[arg(short, long, default_value = None)]
        access_key_id: Option<String>,
        /// Secret access key
        #[arg(short, long, default_value = None)]
        secret_access_key: Option<String>,
        /// Region for the S3 deployments
        #[arg(short, long, default_value = None)]
        region: Option<String>,
        /// S3 Endpoint
        #[arg(short, long, default_value = None)]
        endpoint: Option<String>,
        /// Name of the bucket where
        /// this repository should be stored.
        /// Assumes one bucket per repository.
        #[arg(short, long)]
        bucket: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to generate
        /// a new key pair for global
        /// management of ver-related secrets.
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
    /// Sync with Google Cloud Storage.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    GCS {
        /// Name of the remote storage you are syncing with
        remote_name: String,
        /// Path to the service account credentials file
        #[arg(short, long)]
        service_account_credentials_path: String,
        /// Name of the bucket where
        /// this repository should be stored.
        /// Assumes one bucket per repository
        #[arg(short, long)]
        bucket: String,
    },
    /// Sync with Azure Blob Storage.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    Azure {
        /// Name of the remote storage you are syncing with
        remote_name: String,
        /// Account connected to Azure Blob Storage
        #[arg(long, default_value = None)]
        account_name: Option<String>,
        /// Secret key associated with the account
        #[arg(long, default_value = None)]
        account_key: Option<String>,
        /// Azure Blob Storage endpoint
        #[arg(short, long, default_value = None)]
        endpoint: Option<String>,
        /// Name of the container where
        /// this repository should be stored.
        /// Assumes one container per repository
        #[arg(short, long)]
        container: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to generate
        /// a new key pair for global
        /// management of ver-related secrets.
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
}

/// Sync-related subcommands
#[derive(Debug, Subcommand)]
enum CloneCommands {
    /// Clone from S3.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    S3 {
        /// Name of the remote storage you are cloning from
        remote_name: String,
        /// Destination folder you are cloning to.
        /// It gets created if it doesn't exist.
        dest: String,
        /// Access key ID for the secret access key
        #[arg(short, long, default_value = None)]
        access_key_id: Option<String>,
        /// Secret access key
        #[arg(short, long, default_value = None)]
        secret_access_key: Option<String>,
        /// Region for the S3 deployments
        #[arg(short, long, default_value = None)]
        region: Option<String>,
        /// S3 Endpoint
        #[arg(short, long, default_value = None)]
        endpoint: Option<String>,
        /// Name of the bucket where
        /// this repository is stored.
        /// Assumes one bucket per repository.
        #[arg(short, long)]
        bucket: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to generate
        /// a new key pair for global
        /// management of ver-related secrets.
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
    },
    /// Clone from Google Cloud Storage.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    GCS {
        /// Name of the remote storage you are cloning from
        remote_name: String,
        /// Destination folder you are cloning to.
        /// It gets created if it doesn't exist.
        dest: String,
        /// Path to the service account credentials file
        #[arg(short, long)]
        service_account_credentials_path: String,
        /// Name of the bucket where
        /// this repository is stored.
        /// Assumes one bucket per repository
        #[arg(short, long)]
        bucket: String,
    },
    /// Clone from Azure Blob Storage.
    /// Leave all optional arguments
    /// blank to infer the configuration
    /// from the environment.
    /// Prefix a value with `secrets/` to
    /// load from local secrets (`.versecrets.json`)
    Azure {
        /// Name of the remote storage you are cloning from
        remote_name: String,
        /// Destination folder you are cloning to.
        /// It gets created if it doesn't exist.
        dest: String,
        /// Account connected to Azure Blob Storage
        #[arg(long, default_value = None)]
        account_name: Option<String>,
        /// Secret key associated with the account
        #[arg(long, default_value = None)]
        account_key: Option<String>,
        /// Azure Blob Storage endpoint
        #[arg(short, long, default_value = None)]
        endpoint: Option<String>,
        /// Name of the container where
        /// this repository is stored.
        /// Assumes one container per repository
        #[arg(short, long)]
        container: String,
        /// File where a x25519 age key pair
        /// is stored. Leave empty to generate
        /// a new key pair for global
        /// management of ver-related secrets.
        #[arg(long, short, default_value = None)]
        key_file: Option<String>,
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

    /// Diff a file between two snapshots
    Diff {
        /// Path to the file to diff
        path: String,
        /// Snapshot ID or name to diff from.
        /// Defaults to the current HEAD snapshot
        #[arg(long, short, default_value = None)]
        from: Option<String>,
        /// Snapshot ID or name to diff against
        #[arg(long, short)]
        against: String,
    },

    /// Subcommands for secrets management
    Secrets {
        #[command(subcommand)]
        secret_cmd: SecretsCommands,
    },

    /// Sync the local repository
    /// to a remote object storage (S3, GCS, Azure)
    Sync {
        #[command(subcommand)]
        sync_cmd: SyncCommands,
    },

    /// Clone a repository
    /// from a remote object storage (S3, GCS, Azure)
    Clone {
        #[command(subcommand)]
        clone_cmd: CloneCommands,
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
        Commands::Snap { name, description } => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            snap(&name, description.as_deref(), &mut handle)
        }
        Commands::Init { directory } => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            initialize(directory.as_deref(), &mut handle)
        }
        Commands::Log { head } => {
            let stdout = io::stdout();
            let handle = stdout.lock();
            log(head, handle)
        }
        Commands::Goto { snapshot_id, paths } => goto(&snapshot_id, paths),
        Commands::Cp {
            snapshot_id,
            destination,
        } => {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            ver_copy(&snapshot_id, &destination, &mut handle)
        }
        Commands::Show {
            path,
            snapshot_id,
            offset,
            limit,
        } => {
            let stderr = io::stderr();
            let mut err_handle = stderr.lock();

            #[cfg(not(feature = "minus"))]
            {
                let out = io::stdout();
                let mut handle = out.lock();

                show_file(
                    &path,
                    snapshot_id,
                    offset,
                    limit,
                    &mut handle,
                    &mut err_handle,
                )
            }
            #[cfg(feature = "minus")]
            {
                let pager = minus::Pager::new();
                let mut handle = commons::PagerWriter::new(pager);
                show_file(
                    &path,
                    snapshot_id,
                    offset,
                    limit,
                    &mut handle,
                    &mut err_handle,
                )?;
                minus::page_all(handle.pager)?;
                Ok(())
            }
        }
        Commands::Diff {
            path,
            from,
            against,
        } => {
            let stderr = io::stderr();
            let mut err_handle = stderr.lock();
            #[cfg(not(feature = "minus"))]
            {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                diff_file(&path, from, against, &mut handle, &mut err_handle)
            }
            #[cfg(feature = "minus")]
            {
                let pager = minus::Pager::new();
                let mut handle = commons::PagerWriter::new(pager);
                diff_file(&path, from, against, &mut handle, &mut err_handle)?;
                minus::page_all(handle.pager)?;
                Ok(())
            }
        }
        Commands::Merge { merge_cmd } => match merge_cmd {
            MergeCommands::Run {
                name,
                from,
                our_snapshot,
                their_snapshot,
            } => {
                let stderr = io::stderr();
                let mut err_handle = stderr.lock();
                #[cfg(not(feature = "minus"))]
                {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    merge_run::<io::StdoutLock<'_>, io::StderrLock<'_>>(
                        from.into(),
                        &name,
                        our_snapshot,
                        their_snapshot,
                        &mut handle,
                        &mut err_handle,
                    )
                }
                #[cfg(feature = "minus")]
                {
                    let pager = minus::Pager::new();
                    let mut handle = commons::PagerWriter::new(pager);
                    merge_run::<commons::PagerWriter, io::StderrLock<'_>>(
                        from.into(),
                        &name,
                        our_snapshot,
                        their_snapshot,
                        &mut handle,
                        &mut err_handle,
                    )?;
                    minus::page_all(handle.pager)?;
                    Ok(())
                }
            }
            MergeCommands::Pick { name, our, their } => {
                let stderr = io::stderr();
                let mut err_handle = stderr.lock();

                #[cfg(not(feature = "minus"))]
                {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();

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
                        &mut handle,
                        &mut err_handle,
                    )
                }
                #[cfg(feature = "minus")]
                {
                    let pager = minus::Pager::new();
                    let mut handle = commons::PagerWriter::new(pager);

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
                        &mut handle,
                        &mut err_handle,
                    )?;
                    minus::page_all(handle.pager)?;
                    Ok(())
                }
            }
            MergeCommands::End { name } => {
                let stderr = io::stderr();
                let mut err_handle = stderr.lock();

                #[cfg(not(feature = "minus"))]
                {
                    let stdout = io::stdout();
                    let mut handle = stdout.lock();
                    merge_end(&name, &mut handle, &mut err_handle)
                }
                #[cfg(feature = "minus")]
                {
                    let pager = minus::Pager::new();
                    let mut handle = commons::PagerWriter::new(pager);
                    merge_end(&name, &mut handle, &mut err_handle)?;
                    minus::page_all(handle.pager)?;
                    Ok(())
                }
            }
        },
        Commands::Secrets { secret_cmd } => match secret_cmd {
            SecretsCommands::AddRecipient {
                recipient_name,
                public_key,
                key_file,
            } => {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                add_recipient(recipient_name, public_key, key_file.as_deref(), &mut handle)
            }
            SecretsCommands::Get {
                secret_name,
                key_file,
            } => {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                let _ = get_secret(&secret_name, key_file.as_deref(), true, &mut handle)?;
                Ok(())
            }
            SecretsCommands::Init {
                recipient_name,
                key_file,
            } => {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                init_secrets(key_file.as_deref(), recipient_name, &mut handle)
            }
            SecretsCommands::Set {
                secret_name,
                secret_value,
            } => {
                let stdout = io::stdout();
                let mut handle = stdout.lock();
                set_secret(&secret_name, &secret_value, &mut handle)
            }
        },
        Commands::Sync { sync_cmd } => match sync_cmd {
            SyncCommands::S3 {
                remote_name,
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket,
                key_file,
            } => {
                let key_id = unwrap_maybe_secret(access_key_id.as_deref(), key_file.as_deref())?;
                let secret_key =
                    unwrap_maybe_secret(secret_access_key.as_deref(), key_file.as_deref())?;
                let reg = unwrap_maybe_secret(region.as_deref(), key_file.as_deref())?;
                let end = unwrap_maybe_secret(endpoint.as_deref(), key_file.as_deref())?;
                let buck = unwrap_maybe_secret(Some(&bucket), key_file.as_deref())?;
                let input = GetObjectStoreInput::S3 {
                    access_key_id: key_id,
                    secret_access_key: secret_key,
                    region: reg,
                    endpoint: end,
                    bucket: buck.unwrap(),
                };

                let out = io::stdout();
                let mut handle = out.lock();
                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                sync_with_remote_storage(remote_name, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
            SyncCommands::Azure {
                remote_name,
                account_name,
                account_key,
                endpoint,
                container,
                key_file,
            } => {
                let account_n = unwrap_maybe_secret(account_name.as_deref(), key_file.as_deref())?;
                let account_k = unwrap_maybe_secret(account_key.as_deref(), key_file.as_deref())?;
                let end = unwrap_maybe_secret(endpoint.as_deref(), key_file.as_deref())?;
                let cont = unwrap_maybe_secret(Some(&container), key_file.as_deref())?;
                let input = GetObjectStoreInput::Azure {
                    account_key: account_k,
                    account_name: account_n,
                    endpoint: end,
                    container_name: cont.unwrap(),
                };

                let out = io::stdout();
                let mut handle = out.lock();

                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                sync_with_remote_storage(remote_name, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
            SyncCommands::GCS {
                remote_name,
                service_account_credentials_path,
                bucket,
            } => {
                let input = GetObjectStoreInput::Gcs {
                    service_account_credentials_path,
                    bucket,
                };

                let out = io::stdout();
                let mut handle = out.lock();

                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                sync_with_remote_storage(remote_name, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
        },
        Commands::Clone { clone_cmd } => match clone_cmd {
            CloneCommands::S3 {
                remote_name,
                dest,
                access_key_id,
                secret_access_key,
                region,
                endpoint,
                bucket,
                key_file,
            } => {
                let key_id = unwrap_maybe_secret(access_key_id.as_deref(), key_file.as_deref())?;
                let secret_key =
                    unwrap_maybe_secret(secret_access_key.as_deref(), key_file.as_deref())?;
                let reg = unwrap_maybe_secret(region.as_deref(), key_file.as_deref())?;
                let end = unwrap_maybe_secret(endpoint.as_deref(), key_file.as_deref())?;
                let buck = unwrap_maybe_secret(Some(&bucket), key_file.as_deref())?;
                let input = GetObjectStoreInput::S3 {
                    access_key_id: key_id,
                    secret_access_key: secret_key,
                    region: reg,
                    endpoint: end,
                    bucket: buck.unwrap(),
                };

                let out = io::stdout();
                let mut handle = out.lock();

                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                clone_from_remote(remote_name, dest, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
            CloneCommands::Azure {
                remote_name,
                dest,
                account_name,
                account_key,
                endpoint,
                container,
                key_file,
            } => {
                let account_n = unwrap_maybe_secret(account_name.as_deref(), key_file.as_deref())?;
                let account_k = unwrap_maybe_secret(account_key.as_deref(), key_file.as_deref())?;
                let end = unwrap_maybe_secret(endpoint.as_deref(), key_file.as_deref())?;
                let cont = unwrap_maybe_secret(Some(&container), key_file.as_deref())?;
                let input = GetObjectStoreInput::Azure {
                    account_key: account_k,
                    account_name: account_n,
                    endpoint: end,
                    container_name: cont.unwrap(),
                };

                let out = io::stdout();
                let mut handle = out.lock();

                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                clone_from_remote(remote_name, dest, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
            CloneCommands::GCS {
                remote_name,
                dest,
                service_account_credentials_path,
                bucket,
            } => {
                let input = GetObjectStoreInput::Gcs {
                    service_account_credentials_path,
                    bucket,
                };

                let out = io::stdout();
                let mut handle = out.lock();

                let stderr = io::stderr();
                let mut errhandle = stderr.lock();

                clone_from_remote(remote_name, dest, input, &mut handle, &mut errhandle)?;

                Ok(())
            }
        },
    }
}
