---
name: ver
description: Install, set up, and use ver — an intuitive, non git-like version control system. Fetch the right ver documentation page (as Markdown) for the task at hand.
---

# ver

`ver` is an intuitive, easy-to-use, non git-like version control system. It tracks files as content-defined chunks in a content-addressable blob store and syncs to S3, GCS, or Azure Blob Storage. There is no staging area and no branches; history is a sequence of named snapshots.

## Fetching the documentation

Always fetch the **Markdown version** of a page — the `.md` URL — never the HTML documentation page. The Markdown is the same content without markup noise, and it is what the docs publish for agents.

Start from the index when you are unsure which page covers a task:

- `https://ver-vcs.net/llms.txt` — index of every page with its Markdown link.

## Which page to fetch for which function

| Task | Fetch |
| --- | --- |
| Install ver, initialize it, create a context, take a first snapshot | `https://ver-vcs.net/getting-started/index.md` |
| Understand contexts, snapshots, chunks, storage layout, `.verignore` | `https://ver-vcs.net/concepts/index.md` |
| Take full or partial snapshots (`ver snap`), patch a snapshot, restore one (`ver goto`), browse history (`ver log`) | `https://ver-vcs.net/guides/snapshotting/index.md` |
| Show a file at a snapshot (`ver show`), diff files across snapshots (`ver diff`) | `https://ver-vcs.net/guides/inspecting/index.md` |
| Replicate snapshots between contexts or to object storage (`ver sync`) | `https://ver-vcs.net/guides/syncing/index.md` |
| Reconcile diverged histories, resolve merge conflicts (`ver merge`) | `https://ver-vcs.net/guides/merging/index.md` |
| Look up an exact command, subcommand, or flag | `https://ver-vcs.net/reference/cli/index.md` |
| Edit or validate `~/.ver/contexts.toml`, choose filesystem/KV backends | `https://ver-vcs.net/reference/contexts/index.md` |
| Overview of what ver is and where to start | `https://ver-vcs.net/index.md` |

If you need everything at once, `https://ver-vcs.net/llms-full.txt` is the whole documentation in one Markdown document.

## Setting ver up for a user

1. Fetch `https://ver-vcs.net/getting-started/index.md` and follow it: install with `cargo install ver-vcs`, run `ver ctx init --global`, scaffold a context with `ver ctx scaffold <name> --fs local --kv embedded`.
2. Context paths in `~/.ver/contexts.toml` must be **absolute** — ver refuses to run against relative paths. Fetch `https://ver-vcs.net/reference/contexts/index.md` before editing that file.
3. Switch to the context (`ver ctx switch <name>`), run `ver ctx init` inside it, then take a snapshot with `ver snap full --name <name>`.

ver is in alpha: expect rough edges and check the CLI reference when a command behaves unexpectedly.
