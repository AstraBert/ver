# ver

**A new spring for version control**

> **Alpha release**: expect rough edges, breaking changes, and missing features. Use at your own risk.

`ver` (from the Latin _ver_, "spring") is an intuitive, easy-to-use, and non-git-like version control system. It tracks your files as content-defined chunks, stores them in a content-addressable blob store, and syncs to S3, GCS, or Azure Blob Storage.

Full documentation: https://ver-vcs.net

## Installation

```sh
cargo install ver-vcs
```

Or build from source:

```sh
git clone https://github.com/AstraBert/ver.git
cd ver
cargo install --path crates/ver
```

### Agent skill

Install the `ver` skill so your coding agent knows how to use it:

```sh
npx skills add AstraBert/ver
```

Or paste this into your agent: `Install and set up ver from https://ver-vcs.net/llms.txt`

## Quick start

```sh
# Create ~/.ver/, where your contexts live
ver ctx init --global

# Scaffold a local context, then fill in the paths in ~/.ver/contexts.toml
ver ctx scaffold myproject --fs local --kv embedded
ver ctx switch myproject
ver ctx init

# Take a snapshot
echo "hello" > hello.txt
ver snap full --name my-first-snapshot

# Change something and snapshot again
echo "world" >> hello.txt
ver snap full --name my-second-snapshot

# View history
ver log

# Go back to the first snapshot
ver goto my-first-snapshot

# See a file at a snapshot, and diff it against another
ver show hello.txt --snapshot my-second-snapshot
ver diff hello.txt --base my-first-snapshot --head my-second-snapshot
```

## Concepts

- **Contexts** pair a filesystem backend (local disk, S3, GCS, Azure) with a KV backend (embedded redb, or Redis). They are declared in `~/.ver/contexts.toml`, and every command runs against the current one. An embedded KV only pairs with a local filesystem.
- **Snapshots** map file paths to lists of chunks. Their ID is a BLAKE3 hash, and names are pointers to IDs; you can use either on the CLI. There is no staging area and no branches.
- **Chunks** come from FastCDC content-defined chunking, are hashed with BLAKE3, and are stored zstd-compressed. Identical content is never stored twice.

## Command reference

Every command operates on the current context unless it takes another context by name. Snapshot arguments accept a name or an ID.

### `ver ctx`

| Command | Description |
| --- | --- |
| `ver ctx init [--global]` | With `--global`, create `~/.ver/`. Without, initialize the project of the current context. |
| `ver ctx scaffold <name> --fs <local\|s3\|gcs\|azure> --kv <embedded\|redis>` | Add an empty context to `contexts.toml`. |
| `ver ctx switch <name>` | Make a context current. |
| `ver ctx delete <name>` | Remove a context (not the current one). |
| `ver ctx show [--json]` | Print the current context. |
| `ver ctx list [--json]` | List all contexts. |

### `ver snap`

| Command | Description |
| --- | --- |
| `ver snap full --name <name> [--description <text>]` | Snapshot every tracked file. |
| `ver snap from --name <name> [--description <text>] --file <path>...` | Snapshot only the given files (partial snapshot). |
| `ver snap patch --identifier <name-or-id> --file <path>...` | Add or update files in an existing snapshot. |

### `ver goto <name-or-id> [--path <path>...] [--no-delete]`

Restore the project to a snapshot, or only the given paths. For full snapshots, files missing from the snapshot are deleted unless `--no-delete` is passed.

### `ver log [--limit <n>] [--incomplete-ok]`

Print snapshot history, newest first.

### `ver show <path> [--snapshot <name-or-id>] [--offset <n>] [--limit <n>]`

Print a file at a snapshot (default: current head). Binary files are hex-dumped.

### `ver diff <path> --base <name-or-id> [--head <name-or-id>]`

Unified diff of a file between two snapshots. `--head` defaults to the current head.

### `ver sync --target <context> [--source <context>] [--from-scratch]`

Copy snapshots from the source (default: current context) to the target. `--from-scratch` bulk-copies everything into an empty target; otherwise only missing snapshots are transferred. Refuses when histories have diverged; merge instead.

### `ver merge`

| Command | Description |
| --- | --- |
| `ver merge run <name> --from <context> [--our-snapshot <id>] [--their-snapshot <id>]` | Three-way merge another context into the current one. |
| `ver merge pick <name> [--our <path>...] [--their <path>...]` | Resolve conflicts by picking a side. |
| `ver merge end <name>` | Finalize a fully resolved merge. |

## Ignoring files

Create a `.verignore` file at the project root. It follows `.gitignore` syntax. The `.ver/` directory is always excluded.

## Roadmap

This is a coarse roadmap of future development for `ver`. You can expect these changes to happen all throughout the end of 2026 and into 2027.

- [ ] Unified I/O interface that encompasses FS and object storage to allow for operations like `cp` and `merge` to happen across storage backends
- [ ] Single-node server to manage sync/clones
- [ ] UI attached to server to display, navigate and share ver repositories
- [ ] Multi-node server

## License

MIT
