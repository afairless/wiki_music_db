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

### Usage

**1. Download the Wikidata dump** (~35 GB, resumable):

```bash
./scripts/download_dump.sh --output latest-all.json.gz
```

**2. Bootstrap the database** (30-60 minutes on a modern CPU):

```bash
cargo run --release -- bootstrap \
    --dump latest-all.json.gz \
    --db music.duckdb \
    --parquet-dir parquet-dir \
    --cleanup-parquet
```

**3. Query the database** (not yet fully implemented):

```bash
cargo run --release -- query artist --name "Miles Davis"
cargo run --release -- query genre --name "Jazz"
cargo run --release -- query album --name "Kind of Blue"
```

### Command Reference

#### `bootstrap` — Build the database from a Wikidata dump

```
cargo run -- bootstrap --dump <PATH> [OPTIONS]

Options:
  --dump <PATH>          Path to the Wikidata JSON dump (gzipped, required)
  --db <PATH>            Path to the output DuckDB database (default: music.duckdb)
  --parquet-dir <PATH>   Directory for intermediate Parquet files (default: parquet-dir)
  --cleanup-parquet      Delete intermediate Parquet files after successful load
  --resume               Skip already-written Parquet files to resume interrupted run
```

#### `update` — Incrementally update the database (not yet implemented)

```
cargo run -- update [--since <TIMESTAMP>] [--dry-run]
```

#### `query` — Search the database

```
cargo run -- query artist   --name <NAME>
cargo run -- query genre    --name <NAME> [--limit N] [--offset N]
cargo run -- query album    --name <NAME>
cargo run -- query search   --term <TERM>
```

> **Note**: The `query` and `update` subcommands currently log "not yet implemented" and exit. Only `bootstrap` is functional.

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
