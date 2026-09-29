# wiki_db — Local Music Database from Wikidata

[![CI](https://github.com/user/wiki_db/actions/workflows/ci.yml/badge.svg)](https://github.com/user/wiki_db/actions/workflows/ci.yml)

Build a fast, offline, queryable music database from the Wikidata entity dump.

Streams the ~145 GB gzipped Wikidata JSON dump, filters to musical acts, albums, and tracks, normalizes into a relational schema, and stores the result in an embedded DuckDB database — all from a single statically-compiled Rust binary.

## Features

- **Streaming ingestion**: Line-by-line gzip decompression — memory usage stays bounded regardless of dump size
- **Smart filtering**: Role classifier detects music entities and routes them into one of three roles (agent / album / track) via occupation, group type, work class, and catch-all properties
- **Normalized schema**: 16-table relational database with artists, genres, albums, tracks, instruments, group membership, and Q-ID label lookups
- **Full-text search**: DuckDB `fts` extension powers fast, unified search across artists, albums, genres, and tracks
- **Incremental updates**: `update` merges entities changed since the last sync via the Wikidata SPARQL endpoint and Wikimedia REST API
- **Name resolution**: `populate` resolves remaining Q-ID name placeholders (en label → enwiki sitelink fallback) and fills in release dates, record labels, and track durations
- **Resumable**: `--resume` flag skips already-written intermediate files for interrupted runs
- **Idempotent**: `INSERT OR IGNORE` throughout — re-running produces identical database state
- **Intermediate Parquet**: Filtered data is written to columnar Parquet files before database loading, enabling independent testing and resumability
- **Single binary**: `cargo install wiki_db` gives you the complete tool

## Quick Start

### Prerequisites

- Rust toolchain (stable, edition 2024)
- ~155 GB free disk space (145 GB dump + ~5 GB Parquet + ~1-2 GB DuckDB)

### Installation

```bash
git clone https://github.com/user/wiki_db.git
cd wiki_db
cargo build --release
```

### Pipeline

> **Note**: Running a full bootstrap takes 30-60 minutes and requires ~155 GB of
> free disk space.  The next three commands are all you need for a complete build:
> download, bootstrap, and populate.

**Step 1 — Download** the Wikidata dump (resumable, ~145 GB):

```bash
cargo run -- download
```

**Step 2 — Bootstrap** the database from the dump (takes 30-60 minutes on a modern CPU):

```bash
cargo run --release -- bootstrap
```

**Step 3 — Resolve names** (optional but recommended).  After bootstrap,
entities whose labels are present in the dump already carry real names;
entities without an English label keep a Q-ID placeholder (e.g., `Q1636124`)
and `release_date`, `record_label`, and `duration_seconds` may be NULL.
`populate` re-scans the dump, resolves the remaining placeholders
(en label, with an `enwiki` sitelink-title fallback), and fills enrichment:

```bash
cargo run --release -- populate
```

That's the complete build.  By default the dump lands as `latest-all.json.gz` in the current
directory, the DuckDB database is created as `music.duckdb`, and intermediate
Parquet files are kept under `parquet-dir/`.  All of these paths can be
customised through `wiki_db.toml` (see below).

Customising paths via CLI flags is also supported:

```bash
cargo run -- download --output /data/dump.json.gz
cargo run --release -- bootstrap --dump /data/dump.json.gz --db /data/music.duckdb
cargo run --release -- populate --dump /data/dump.json.gz --db /data/music.duckdb
```

---

## Configuration

Set all your defaults in a single `wiki_db.toml` file so you don't need to
repeat CLI flags.  Place it in the project root and run the commands above
as-is — they'll pick up the configured paths automatically.

```toml
# =============================================================================
# wiki_db.toml — every field is optional; CLI flags override these values
# =============================================================================

# ---------- Download & bootstrap paths --------------------------------------

dump = "~/wiki_db/latest-all.json.gz"       # download, bootstrap, and populate use this
# db = "~/wiki_db/music.duckdb"              # bootstrap/query/update/populate output (default: music.duckdb)
# parquet_dir = "~/wiki_db/parquet-dir"      # intermediate files (default: parquet-dir)

# ---------- Behaviour flags -------------------------------------------------

# cleanup_parquet = true                     # delete Parquet files after load
# resume = false                             # skip completed Parquet on re-run (bootstrap, populate)

# ---------- Logging ---------------------------------------------------------

# log_level = "info"                         # trace | debug | info | warn | error
```

> **Path expansion**: Leading `~/` in any path is automatically expanded to your
> home directory.  Relative paths are resolved from the working directory.

### Precedence

```
CLI flag > Config file > Hardcoded default
```

Every tool can be driven entirely by CLI flags — the config file is never
required.  When both exist, CLI flags win.

If the config file doesn't exist, the tool silently continues.  If it exists
but contains invalid TOML, a warning is logged and CLI defaults are used — the
tool never refuses to run over a bad config.

### Custom config path

Use `--config` to point to a different file:

```bash
cargo run -- --config /etc/wiki_db/production.toml bootstrap
```

By default the tool looks for `wiki_db.toml` in the current directory.

### Full config reference

| Field | Type | Default | CLI override | Used by |
|---|---|---|---|---|
| `dump` | string | — | `--dump` / `--output` | `download`, `bootstrap`, `populate` |
| `db` | string | `music.duckdb` | `--db` | `bootstrap`, `query`, `update`, `populate` |
| `parquet_dir` | string | `parquet-dir` | `--parquet-dir` | `bootstrap`, `populate` |
| `cleanup_parquet` | bool | `false` | `--cleanup-parquet` | `bootstrap` |
| `resume` | bool | `false` | `--resume` | `bootstrap`, `populate` |
| `log_level` | string | `info` | `RUST_LOG` env var | all |

#### `[download]` subsection

| Field | Type | Default | CLI override | Used by |
|---|---|---|---|---|
| `url` | string | — | — | `download` |
| `output` | string | — | `--output` | `download` |
| `user_agent` | string | — | — | `download` |
| `quiet` | bool | `false` | `--quiet` | `download` |

#### `[update]` subsection

| Field | Type | Default | CLI override | Used by |
|---|---|---|---|---|
| `since` | string | — | `--since` | `update` |
| `dry_run` | bool | `false` | `--dry-run` | `update` |

These subsections are accepted by the config loader for the corresponding
subcommands.  CLI flags remain authoritative — pass them on the command line to
override the file values.

---

## Pipeline: Download → Bootstrap → Populate → Query

### Step 1 — Download the Wikidata dump

```bash
cargo run -- download
```

This calls `scripts/download_dump.sh` using the configured `dump` path (or
`latest-all.json.gz` by default).  The download script supports:

- **Automatic resume** — interrupted transfers continue where they left off
  (HTTP range requests via `curl -C -`)
- **MD5 verification** — checksum checked automatically after download
- **Gzip integrity check** — validates the archive before reporting success
- **Signal safety** — `Ctrl-C` leaves the partial file in place for resume

```bash
cargo run -- download --help
```

### Step 2 — Bootstrap the database

```bash
cargo run --release -- bootstrap
```

The bootstrap subcommand reads `dump`, `db`, `parquet_dir`, `cleanup_parquet`,
and `resume` from the config file if they aren't given as CLI flags.

```bash
# Override paths for a one-off build
cargo run --release -- bootstrap \
    --dump /tmp/dump.json.gz \
    --db /tmp/experiment.duckdb

# Resume an interrupted run
cargo run --release -- bootstrap --resume
```

### Step 3 — Populate names, dates, and labels

```bash
cargo run --release -- populate
```

After bootstrap, entities whose labels are present in the dump already carry
real names; label-less entities keep Q-ID placeholders (e.g., `Q1636124`) and
`release_date`, `record_label`, and `duration_seconds` may be NULL.  `populate`
re-scans the dump, resolves the remaining placeholders (en label, with an
`enwiki` sitelink-title fallback), and fills in those columns.  It can be
skipped if you only care about artists, or if names are already resolved
(re-runs exit with a message and change nothing).

```bash
# Re-run populate against a different database or dump
cargo run --release -- populate --dump /tmp/dump.json.gz --db /tmp/experiment.duckdb
```

### Step 4 — Query the database

```bash
# Search for an artist by name
cargo run --release -- query artist --name "Miles Davis"

# Search for a genre and list associated artists
cargo run --release -- query genre --name "Jazz"

# Search for an album with track listing
cargo run --release -- query album --name "Kind of Blue"

# Search across all entity types simultaneously
cargo run --release -- query search --term "Miles"
```

Query results are displayed in a formatted terminal output with colored headers.
Artists show their genres, albums, instruments, and dates. Albums show their
artist line-up, genre tags, and full track listing with durations — once
`populate` (Step 3) has resolved track names.

### Step 5 — Incremental update

```bash
# Fetch and merge recent changes from Wikidata
cargo run --release -- update

# Preview changes without writing
cargo run --release -- update --dry-run
```

The update subcommand queries the Wikidata SPARQL endpoint for entities modified
since the last sync, fetches their full data via the Wikimedia REST API, and
upserts them into the database.

---

## Command Reference

### `download` — Download the Wikidata dump

```
cargo run -- download [OPTIONS]

Options:
  --output <PATH>   Target file path (default: from wiki_db.toml `dump`, or latest-all.json.gz)
  --force           Re-download even if a complete valid dump exists
  --quiet           Suppress progress output
  --config <PATH>   TOML config file (default: wiki_db.toml)
```

### `bootstrap` — Build the database from a Wikidata dump

```
cargo run -- bootstrap [OPTIONS]

Options:
  --dump <PATH>          Path to the Wikidata JSON dump (from wiki_db.toml or required)
  --db <PATH>            Path to the output DuckDB database (default: music.duckdb)
  --parquet-dir <PATH>   Directory for intermediate Parquet files (default: parquet-dir)
  --cleanup-parquet      Delete intermediate Parquet files after successful load
  --resume               Skip already-written Parquet files to resume interrupted run
  --config <PATH>        TOML config file (default: wiki_db.toml)
```

### `populate` — Resolve names, dates, and labels

```
cargo run -- populate [OPTIONS]

Options:
  -d, --dump <PATH>      Path to the Wikidata JSON dump (gzipped) (from wiki_db.toml or required)
  --db <PATH>            Path to the DuckDB database (default: music.duckdb)
  --parquet-dir <PATH>   Directory for intermediate Parquet files (default: parquet-dir)
  --resume               Skip dump re-scan when Parquet files already exist
  --force                Re-populate even if names are already resolved
  --config <PATH>        TOML config file (default: wiki_db.toml)
```

Requires a `bootstrap`ed database at schema v2 (16 tables).  Re-scans the dump
(~tens of minutes) to extract labels and enrichment claims, then backfills
`album.name`, `track.name`, `release_date`, `record_label`, and
`duration_seconds`.  Names use the en-label → enwiki-sitelink fallback;
`duration_seconds` accepts only P2047 amounts whose unit is seconds (Q11574),
otherwise NULL.  Skips with a message when all names are already resolved.

### `update` — Incrementally update the database

```
cargo run -- update [OPTIONS]

Options:
  --since <TIMESTAMP>   Sync from a specific timestamp (default: last sync state)
  --dry-run             Print changes without writing to the database
  --config <PATH>       TOML config file (default: wiki_db.toml)
```

### `query` — Search the database

```
cargo run -- query artist   --name <NAME>
cargo run -- query genre    --name <NAME> [--limit N] [--offset N]
cargo run -- query album    --name <NAME>
cargo run -- query search   --term <TERM>
```

### `completion` — Generate shell completion scripts

```
cargo run -- completion <SHELL> [--output <DIR>]

Arguments:
  <SHELL>               Shell to generate completions for (bash, zsh, fish, powershell, elvish)

Options:
  --output, -o <DIR>    Output directory (default: current directory)
  --config <PATH>       TOML config file (default: wiki_db.toml)
```

**Installation examples:**

```bash
# Bash
cargo run -- completion bash --output ~/.local/share/bash-completion/completions/
echo "source ~/.local/share/bash-completion/completions/wiki_db" >> ~/.bashrc

# Zsh (with oh-my-zsh)
cargo run -- completion zsh --output ~/.zsh/completion/
echo "fpath=(~/.zsh/completion \$fpath)" >> ~/.zshrc

# Fish
cargo run -- completion fish --output ~/.config/fish/completions/
```

### Global flags

These flags are available on every subcommand:

```
  --config <PATH>   TOML config file (default: wiki_db.toml)
  -v, --verbose     Increase log verbosity to debug level
  -q, --quiet       Decrease log verbosity to warn level
  -h, --help        Print help
  -V, --version     Print version
```

Log level precedence: `RUST_LOG` env var > `--verbose`/`--quiet` > config file > `info` (default).

## Database Schema

Each filtered entity is classified into exactly one role (see
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Filtering Strategy):

- **agent** (people and groups) → `artist` and its join tables
- **album** (albums, EPs, singles, compilations, …) → `album`, `album_artist`, `album_genre`
- **track** (songs, vocal tracks, instrumentals, …) → `track`, `track_artist`, `track_album`

```
artist               — Core entity: person (musician) or group (band)
genre                — Genre taxonomy (id, name)
instrument           — Instrument lookup table (id, name)
record_label         — Record label lookup table (id, name)
artist_genre         — Many-to-many artist ↔ genre
artist_instrument    — Many-to-many artist ↔ instrument
artist_member_of     — Person → group membership
album                — Albums, EPs, singles, compilations
album_artist         — Many-to-many album ↔ artist (with role)
album_genre          — Many-to-many album ↔ genre
track                — Individual tracks/songs
track_album          — Many-to-many track ↔ album
track_artist         — Many-to-many track ↔ artist (with role)
qid_label            — English label/description lookup by Q-ID
sync_state           — Last-sync timestamp for incremental updates
schema_version       — Migration version tracking
```

See [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) for the full schema with column definitions, indexes, and foreign keys.

## Development

### Building

```bash
cargo build          # debug build
cargo build --release  # optimized build for production use
```

### Testing

```bash
cargo test           # run all tests
cargo test test_name # run a specific test
```

### Linting and formatting

```bash
cargo clippy -- -D warnings
cargo fmt --check
cargo fmt            # auto-fix formatting
```

### Security audit

```bash
cargo audit
```

## Project Structure

```
src/
  cli/             — CLI subcommand definitions
  db/              — Schema initialization and Parquet → DuckDB loading
  wikidata/        — Wikidata JSON model, streaming parser, music filter
  extraction.rs    — MusicEntity extraction from filtered entities
  parquet_writer.rs — Batch Parquet writer with file rotation
  error.rs         — Domain error types
  lib.rs           — Public module exports
  main.rs          — CLI entry point and pipeline orchestration
tests/             — Integration and unit tests
docs/              — Architecture and research documents
scripts/           — Helper scripts (dump downloader)
```

## Dependencies

| Crate | Purpose |
|---|---|
| `duckdb` (bundled) | Embedded DuckDB database |
| `clap` | CLI argument parsing |
| `serde` + `serde_json` | Wikidata JSON deserialization |
| `parquet` + `arrow` | Intermediate columnar format |
| `flate2` | Gzip decompression |
| `tracing` + `indicatif` | Logging and progress bars |
| `tokio` + `reqwest` | Async HTTP for the update pipeline (`src/sparql.rs` wraps the `reqwest` client in a Tokio runtime) |
| `anyhow` + `thiserror` | Error handling |
| `chrono` | Date/time parsing |
| `regex` | Text matching |

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Full architecture: module map, data flow, schema, design decisions, and limitations
- [`docs/research/`](docs/research/) — Feature research and implementation plans
- [`AGENTS.md`](AGENTS.md) — Instructions for AI coding agents working on this project

## Limitations

- **Label fallback**: Names resolve as English label → sanitized `enwiki` sitelink title → NULL; entities with neither get NULL names (stored, not rejected)
- **Album/track names**: Only label-less entities keep Q-ID placeholders after bootstrap; `release_date`, `record_label`, and `duration_seconds` are NULL until `populate` runs
- **Resume v1**: `--resume` restarts streaming from the beginning; only Parquet files are skipped

## License

See [LICENSE](LICENSE) if one exists.
