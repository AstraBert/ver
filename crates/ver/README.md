# ver

**A new spring for version control.**

> **Alpha release**: expect rough edges, breaking changes, and missing features. Use at your own risk.

`ver` is an intuitive, easy-to-use, and non-git-like version control system. It tracks your files as content-defined chunks, stores them in a local content-addressable store, and can sync to S3, GCS, or Azure Blob Storage.

## Installation

### Pre-built binaries

```sh
cargo install r-vcs
```

### Build from source

```sh
git clone https://github.com/AstraBert/ver.git
cd ver
cargo install --path crates/ver
```

## Quick start

```sh
# Initialize a repository
ver init

# Create some files and take a snapshot
echo "hello" > hello.txt
ver snap my-first-snapshot

# Make changes and snapshot again
echo "world" >> hello.txt
ver snap my-second-snapshot

# View history
ver log

# Go back to the first snapshot
ver goto my-first-snapshot

# See what a file looks like at a snapshot
ver show hello.txt -s my-second-snapshot

# Diff a file between snapshots
ver diff hello.txt -a my-second-snapshot
```

## Command reference

### `ver init [directory]`

Initialize a new ver repository. Creates a `.ver/` directory containing the content-addressable blob store, snapshot database, and history file. Defaults to the current directory.

### `ver snap <name> [-d description]`

Create a snapshot of the current repository state. All tracked files are chunked, hashed, and stored. The snapshot is recorded with the given name and an optional description.

### `ver goto <snapshot-id-or-name> [paths...]`

Restore the repository to the state captured in a snapshot. Optionally pass specific file/directory paths to only revert those. Files not present in the snapshot are deleted.

### `ver log [--head N]`

Show the snapshot history in reverse chronological order. Use `--head N` to limit output to the last N entries.

### `ver show <path> [-s snapshot] [-o offset] [-l limit]`

Print the content of a file at a given snapshot (defaults to current HEAD). Supports byte offset and limit for large files. Binary files are hex-dumped.

### `ver diff <path> [-f from] -a <against>`

Show a unified diff of a file between two snapshots. `from` defaults to the current HEAD.

### `ver cp <snapshot-id-or-name> <destination>`

Copy the repository at a specific snapshot to a new directory. The destination is created if it doesn't exist.

### `ver merge run <name> --from <repo-path> [-o our-snapshot] [-t their-snapshot]`

Start a merge between the current repository and another ver repository. Defaults to the latest snapshots on both sides.

### `ver merge pick <name> [-o paths...] [-t paths...]`

Resolve merge conflicts by picking "our" or "their" version for specific file paths.

### `ver merge end <name>`

Finalize a merge after all conflicts have been resolved.

### `ver sync <backend> <remote-name> [options...]`

Sync the local repository to remote object storage. Only new snapshots (those not already on the remote) are uploaded. Supported backends:

- `ver sync s3`: Amazon S3 (or S3-compatible like MinIO)
- `ver sync gcs`: Google Cloud Storage
- `ver sync azure`: Azure Blob Storage

All backends accept credentials via flags or environment variables. Prefix any value with `secrets/` to load it from local secrets.

### `ver clone <backend> <remote-name> <dest> [options...]`

Clone a repository from remote object storage into a local directory. Downloads all chunks, reconstructs files, and sets up the `.ver/` directory. Same backends and credential options as `sync`.

### `ver secrets init <recipient-name> [-k key-file]`

Initialize secrets management. Generates an x25519 age key pair for encrypting secrets, or uses an existing key file.

### `ver secrets set <name> <value>`

Store an encrypted secret in `.versecrets.json` at the repository root.

### `ver secrets get <name> [-k key-file]`

Retrieve and decrypt a secret.

### `ver secrets add-recipient <name> <public-key> [-k key-file]`

Add a new recipient who can decrypt the repository's secrets.

## Ignoring files

Create a `.verignore` file in the repository root. It follows the same syntax as `.gitignore`.

## How it works

- Files are split into content-defined chunks using FastCDC
- Chunks are hashed with BLAKE3 and stored compressed (zstd) in `.ver/blobs/`
- Snapshots are stored in an embedded [redb](https://github.com/cberner/redb) database
- Syncing uploads only the chunks that the remote is missing
- Cloning downloads chunks and reconstructs files from the snapshot graph

## License

MIT
