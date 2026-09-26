# wiki_db

## Purpose

Builds a local, single-user DuckDB database of musical acts and artists from the Wikidata JSON dump. Streams, filters, and normalizes ~35 GB of gzipped Wikidata entities into a relational schema, then exposes CLI queries over the result.

## Stack

- **Language**: Rust (edition 2024)
- **Database**: DuckDB 1.x (embedded, via the `duckdb` crate with `bundled` feature)
- **CLI**: `clap` 4.x with derive macros
- **Async runtime**: Tokio 1.x (multi-threaded) — used by `src/sparql.rs` (update pipeline) to wrap the async `reqwest` client in a runtime
- **Serialization**: `serde` + `serde_json` (streaming entity deserialization)
- **Parquet**: `parquet` 59 + `arrow` 59 (intermediate columnar format)
- **Compression**: `flate2` (gzip decoding)
- **Logging & progress**: `tracing` + `tracing-subscriber` + `indicatif`
- **Error handling**: `anyhow` (application) + `thiserror` (library)
- **Testing**: `cargo test` + `tempfile` + `proptest`
- **Linting**: `cargo clippy`
- **Formatting**: `rustfmt`
- **Package manager**: `cargo`

## Project Structure

```
src/
  cli/             — CLI subcommand definitions (bootstrap, download, populate, query, update)
  config.rs        — TOML configuration loading and path expansion
  db/              — Schema init (schema.rs), Parquet → DuckDB loading (load.rs), queries (query.rs)
  wikidata/        — Wikidata JSON model, music entity filter, and streaming parser
  extraction.rs    — MusicEntity extraction from filtered Wikidata entities
  label_extractor.rs — Q-ID label/claim extraction from the dump (populate)
  parquet_writer.rs — Batch-write MusicEntity records to Parquet files
  sparql.rs        — SPARQL query builder + async HTTP client (update pipeline)
  error.rs         — Domain error types (thiserror)
  lib.rs           — Public module exports
  main.rs          — CLI entry point, pipeline orchestration, progress reporting
tests/
  fixtures/        — Hand-crafted JSON fixtures for integration tests
  bootstrap_test.rs — Full bootstrap pipeline integration tests
  stream_test.rs   — Streaming parser tests
  filter_tests.rs  — Property-based tests for the music entity filter
  query_test.rs    — Query subcommand integration tests
  update_test.rs   — Incremental update integration tests
docs/
  research/        — Feature research and implementation plans
scripts/
  download_dump.sh — Resumable Wikidata dump downloader with MD5 verification
```

## Commands

| Action | Command |
|---|---|
| Build | `cargo build` |
| Build release | `cargo build --release` |
| Test all | `cargo test` |
| Test single | `cargo test test_name` |
| Lint | `cargo clippy -- -D warnings` |
| Format check | `cargo fmt --check` |
| Format fix | `cargo fmt` |
| Run download | `cargo run -- download` (resumable, MD5-verified) |
| Run bootstrap | `cargo run --release -- bootstrap --dump <path> --db <path>` |
| Run populate | `cargo run --release -- populate --dump <path> --db <path>` (resolve Q-ID placeholders) |
| Run update | `cargo run --release -- update` |
| Run query | `cargo run --release -- query artist --name <name>` |
| Generate completions | `cargo run -- completion bash --output <dir>` |

## Conventions

### Rust

- All public items MUST have doc comments (`///` or `//!`).
- Errors use `thiserror` for library code; `anyhow` for CLI/orchestration code only.
- No `unwrap()` in production code — use `?` or `expect` with a clear message.
- Follow the `rust-dev` skill for Rust-specific conventions.
- Follow the `testing-guide` skill for test structure (Arrange-Act-Assert, None-One-Many).
- Follow the `incremental-development` skill for commit workflow.

### Error handling

- Missing optional data (labels, descriptions, dates) is stored as NULL — never rejected.
- Only malformed JSON and entities without a Q-ID are rejected/error.
- Date parsing failures are logged at WARN and stored as NULL.
- `INSERT OR IGNORE` is used throughout for idempotent database operations.

### Commit messages

- Use conventional commits (the `conventional-commit` skill).
- Format: `type(scope): description` — e.g., `feat(db): implement DuckDB loader for genre table`.

## Security

- The Wikidata JSON dump is treated as untrusted external input.
- All database operations use DuckDB parameterized queries or `INSERT … SELECT` — no string concatenation with entity data.
- No API keys, passwords, or credentials are hardcoded. Wikimedia APIs are public.
- `.gitignore` excludes `music.duckdb`, `*.duckdb`, `parquet-dir/`, `.env`, `target/`.
- Run `cargo audit` before each release to check dependencies for known vulnerabilities.

## Current Implementation State

**Completed phases (from `docs/research/2026-07_music_db_rust_plan.md`):**

- Phase 1: Project scaffold & schema
- Phase 2a: Wikidata entity model & deserialization
- Phase 2b: Filter + streaming parser
- Phase 3a: Parquet writer & MusicEntity extraction
- Phase 3b: DuckDB loader & bootstrap CLI
- Phase 5: Full-text search (DuckDB `fts` extension)
- Phase 6: Query subcommand (artist, genre, album, search)
- Phase 7: Incremental updates (SPARQL + Wikimedia REST API; async `reqwest` in a Tokio runtime via `src/sparql.rs`)
- Phase 8: Polish & distribution (--verbose/--quiet, shell completions, CI pipeline, documentation)
- Phase 9: Populate & name resolution — Q-ID label/claim extraction, `populate` subcommand, FK-safe backfill (2026-08)
- Phase 10: FK-safety hardening — enrichment FK guards, FK-safe backfill with temp-table swap (2026-08)

The schema is at **v2 with 16 tables** (`schema_version`, `artist`, `genre`, `artist_genre`, `album`, `album_artist`, `album_genre`, `track`, `track_album`, `track_artist`, `artist_instrument`, `artist_member_of`, `sync_state`, `qid_label`, `instrument`, `record_label`).

## Documentation

- `ARCHITECTURE.md` — system design, module map, data flow, and key decisions.
- `docs/research/*.md` — feature plans and design proposals.
- `TODO.md` — the current implementation plan; completed plans are archived in `docs/research/` with `Status: Implemented` and closing commits.

### Documentation conventions

- When a research doc's plan is implemented, update its `Status:` header to `Implemented` (with closing commit references) in the same commit series that closes the plan.
- Doc-update steps (README, ARCHITECTURE, AGENTS) are mandatory plan steps, not optional follow-ups.
- Documentation is instance-agnostic: describe pipeline stage semantics — bootstrap seeds Q-ID placeholders, `populate` resolves them, `query` surfaces them — never the state of a particular database file or path. A reader must be able to (a) build a database from scratch following the docs, or (b) understand how an existing database was created.

Other documents (e.g., research docs) reference this subsection instead of restating the convention.

## Agent Instructions

- Read `ARCHITECTURE.md` before making structural changes.
- When implementing a new phase, first read the relevant plan from `docs/research/`.
- Run `cargo test` before declaring a task complete. If a test fails, fix the code — do not modify the test unless the test is incorrect.
- Run `cargo clippy` and fix all warnings before committing.
- Use `INSERT OR IGNORE` for all DuckDB data loading — idempotency is a hard requirement.
- When adding new phases, update `AGENTS.md` (implementation state section), `ARCHITECTURE.md`, and `README.md` in the same commit series.
- Do not edit files outside `src/`, `tests/`, `docs/`, `scripts/`, or the project root without explicit user instruction.
- Follow the `incremental-development` skill: one small logical unit per commit, with tests passing each time.
