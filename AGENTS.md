# wiki_db

## Purpose

Builds a local, single-user DuckDB database of musical acts and artists from the Wikidata JSON dump. Streams, filters, and normalizes ~35 GB of gzipped Wikidata entities into a relational schema, then exposes CLI queries over the result.

## Stack

- **Language**: Rust (edition 2024)
- **Database**: DuckDB 1.x (embedded, via the `duckdb` crate with `bundled` feature)
- **CLI**: `clap` 4.x with derive macros
- **Async runtime**: Tokio 1.x (multi-threaded; not yet used in production code — deferred to Phase 7 incremental updates)
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
  cli/             — CLI subcommand definitions (bootstrap, update, query)
  db/              — Database schema initialization and Parquet → DuckDB loading
  wikidata/        — Wikidata JSON model, music entity filter, and streaming parser
  extraction.rs    — MusicEntity extraction from filtered Wikidata entities
  parquet_writer.rs — Batch-write MusicEntity records to Parquet files
  error.rs         — Domain error types (thiserror)
  lib.rs           — Public module exports
  main.rs          — CLI entry point, pipeline orchestration, progress reporting
tests/
  fixtures/        — Hand-crafted JSON fixtures for integration tests
  bootstrap_test.rs — Full bootstrap pipeline integration tests
  stream_test.rs   — Streaming parser tests
  filter_tests.rs  — Property-based tests for the music entity filter
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
| Run bootstrap | `cargo run --release -- bootstrap --dump <path> --db <path>` |

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
- Phase 7: Incremental updates (SPARQL + Wikimedia REST API)
- Phase 8: Polish & distribution (--verbose/--quiet, shell completions, CI pipeline, documentation)

## Documentation

- `ARCHITECTURE.md` — system design, module map, data flow, and key decisions.
- `docs/research/*.md` — feature plans and design proposals.
- `TODO.md` — the current implementation plan (Phase 8 polish & distribution).

## Agent Instructions

- Read `ARCHITECTURE.md` before making structural changes.
- When implementing a new phase, first read the relevant plan from `docs/research/`.
- Run `cargo test` before declaring a task complete. If a test fails, fix the code — do not modify the test unless the test is incorrect.
- Run `cargo clippy` and fix all warnings before committing.
- Use `INSERT OR IGNORE` for all DuckDB data loading — idempotency is a hard requirement.
- When adding new phases, update `AGENTS.md` (implementation state section), `ARCHITECTURE.md`, and `README.md` in the same commit series.
- Do not edit files outside `src/`, `tests/`, `docs/`, `scripts/`, or the project root without explicit user instruction.
- Follow the `incremental-development` skill: one small logical unit per commit, with tests passing each time.
