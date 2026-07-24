# Implementation Plan: Phase 1 — Project Scaffold & Schema

Source: `docs/research/2026-07_music_db_rust_plan.md` (Phase 1 only)

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `chore: initialize cargo project and gitignore` | Project skeleton | `Cargo.toml`, `src/main.rs`, `.gitignore` | — |
| 2 | `chore: add all crate dependencies to Cargo.toml` | Dependency manifest | `Cargo.toml` (updated with duckdb, clap, serde, serde_json, tokio, reqwest, parquet, tracing, tracing-subscriber, indicatif, flate2, anyhow, thiserror, regex, chrono) | — |
| 3 | `feat: define error types` | Error types module | `src/error.rs` | Unit |
| 4 | `feat: implement database schema module` | Schema + DB init | `src/db/mod.rs`, `src/db/schema.rs` | Unit |
| 5 | `feat: implement CLI structure with clap subcommands` | CLI skeleton | `src/cli/mod.rs`, `src/cli/bootstrap.rs`, `src/cli/update.rs`, `src/cli/query.rs` | Smoke |
| 6 | `feat: wire up main.rs with tracing and CLI` | Entry point | `src/main.rs` (updated) | Smoke |
| 7 | `test: add integration test for database initialization` | Integration test | `tests/integration/bootstrap_test.rs`, `tests/fixtures/` | Integration |
