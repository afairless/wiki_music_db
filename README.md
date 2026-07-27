# wiki_db — Local Music Database from Wikidata

Build a fast, offline, queryable music database from the Wikidata entity dump.

Streams the ~35 GB gzipped Wikidata JSON dump, filters to musical acts & artists, normalizes into a relational schema, and stores the result in an embedded DuckDB database — all from a single statically-compiled Rust binary.

## Features

- **Streaming ingestion**: Line-by-line gzip decompression — memory usage stays bounded regardless of dump size
- **Smart filtering**: Multi-criteria music entity detection (occupation, group type, catch-all properties)
- **Normalized schema**: 12-table relational database with artists, genres, albums, tracks, instruments, and group membership
- **Resumable**: `--resume` flag skips already-written intermediate files for interrupted runs
- **Idempotent**: `INSERT OR IGNORE` throughout — re-running produces identical database state
- **Intermediate Parquet**: Filtered data is written to columnar Parquet files before database loading, enabling independent testing and resumability
- **Single binary**: `cargo install wiki_db` gives you the complete tool

## Quick Start

### Prerequisites

- Rust toolchain (stable, edition 2024)
- ~50 GB free disk space (35 GB dump + ~5 GB Parquet + ~1-2 GB DuckDB)

### Installation

```bash
git clone https://github.com/user/wiki_db.git
cd wiki_db
cargo build --release
```

### Pipeline

> **Note**: Running a full bootstrap takes 30-60 minutes and requires ~50 GB of
> free disk space.  The next two commands are all you need for a complete build.

**Step 1 — Download** the Wikidata dump (resumable, ~35 GB):

```bash
cargo run -- download
```

**Step 2 — Import** into DuckDB (takes 30-60 minutes on a modern CPU):

```bash
cargo run --release -- bootstrap
```

That's it.  By default the dump lands as `latest-all.json.gz` in the current
directory, the DuckDB database is created as `music.duckdb`, and intermediate
Parquet files are kept under `parquet-dir/`.  All of these paths can be
customised through `wiki_db.toml` (see below).

Customising paths via CLI flags is also supported:

```bash
cargo run -- download --output /data/dump.json.gz
cargo run --release -- bootstrap --dump /data/dump.json.gz --db /data/music.duckdb
```

---

## Configuration

Set all your defaults in a single `wiki_db.toml` file so you don't need to
repeat CLI flags.  Place it in the project root and run the two commands above
as-is — they'll pick up the configured paths automatically.

```toml
# =============================================================================
# wiki_db.toml — every field is optional; CLI flags override these values
# =============================================================================

# ---------- Download & bootstrap paths --------------------------------------

dump = "~/wiki_db/latest-all.json.gz"       # download and bootstrap both use this
# db = "~/wiki_db/music.duckdb"              # bootstrap output (default: music.duckdb)
# parquet_dir = "~/wiki_db/parquet-dir"      # intermediate files (default: parquet-dir)

# ---------- Behaviour flags -------------------------------------------------

# cleanup_parquet = true                     # delete Parquet files after load
# resume = false                             # skip completed Parquet on re-run

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
| `dump` | string | — | `--dump` / `--output` | `download`, `bootstrap` |
| `db` | string | `music.duckdb` | `--db` | `bootstrap` |
| `parquet_dir` | string | `parquet-dir` | `--parquet-dir` | `bootstrap` |
| `cleanup_parquet` | bool | `false` | `--cleanup-parquet` | `bootstrap` |
| `resume` | bool | `false` | `--resume` | `bootstrap` |
| `log_level` | string | `info` | `RUST_LOG` env var | all |

---

## Pipeline: Download → Import

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

### Step 3 — Query the database

```bash
cargo run --release -- query artist --name "Miles Davis"
cargo run --release -- query genre --name "Jazz"
cargo run --release -- query album --name "Kind of Blue"
```

> **Note**: The `query` and `update` subcommands currently log "not yet implemented"
> and exit.  Only `download` and `bootstrap` are functional.

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

### `update` — Incrementally update the database (not yet implemented)

```
cargo run -- update [--since <TIMESTAMP>] [--dry-run]
```

### `query` — Search the database

```
cargo run -- query artist   --name <NAME>
cargo run -- query genre    --name <NAME> [--limit N] [--offset N]
cargo run -- query album    --name <NAME>
cargo run -- query search   --term <TERM>
```

> **Note**: The `query` and `update` subcommands currently log "not yet implemented"
> and exit. Only `download` and `bootstrap` are functional.

## Database Schema

```
artist               — Core entity: person (musician) or group (band)
genre                — Genre taxonomy (id, name)
artist_genre         — Many-to-many artist ↔ genre
artist_instrument    — Many-to-many artist ↔ instrument (Q-ID)
artist_member_of     — Person → group membership
album                — Albums, EPs, singles, compilations
album_artist         — Many-to-many album ↔ artist (with role)
album_genre          — Many-to-many album ↔ genre
track                — Individual tracks/songs
track_album          — Many-to-many track ↔ album
track_artist         — Many-to-many track ↔ artist (with role)
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
| `tokio` + `reqwest` | Async runtime (Phase 7, not yet used) |
| `anyhow` + `thiserror` | Error handling |
| `chrono` | Date/time parsing |
| `regex` | Text matching |

## Documentation

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — Full architecture: module map, data flow, schema, design decisions, and limitations
- [`docs/research/`](docs/research/) — Feature research and implementation plans
- [`AGENTS.md`](AGENTS.md) — Instructions for AI coding agents working on this project

## Limitations

- **English-only**: Artists without English labels get NULL names (stored, not rejected)
- **Album/track names**: Use Wikidata Q-ID placeholders — actual name resolution is deferred
- **No full-text search**: Phase 5 (FTS indexes) not yet implemented
- **No incremental updates**: Re-bootstrap for fresh data (Phase 7 not yet implemented)
- **Resume v1**: `--resume` restarts streaming from the beginning; only Parquet files are skipped

## License

See [LICENSE](LICENSE) if one exists.
