# Implementation Plan: Fix Missing FK Guards for album_id and track_id

Source: `docs/research/2026-08_fix_enrichment_fk_album_track_guards.md`

## Context

The prior FK fix (commits 0f67175..ea66274) added guards for `genre_id` in `album_genre` and
`parent_album_qid` in `track_album`, but missed two other FK columns:

1. `album_genre.album_id → album(id)` — entity_qid is never checked against the album table
2. `track_album.track_id → track(id)` — entity_qid is never checked against the track table

When `--resume` reuses stale `enrichment.parquet` files from a different database state, these
missing guards cause hard `Constraint Error` crashes. DuckDB's `INSERT OR IGNORE` does not
suppress FK violations.

The fix is purely additive — two `AND entity_qid IN (SELECT id FROM ...)` clauses following
the established pattern in `load_artist_genre()` and `load_artist_member_of()`.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(db): add album and track FK guards to enrichment load` | FK guards + unit tests | `src/db/load.rs` — two SQL WHERE clauses, two new unit tests | Unit |

## Step details

### Step 1 — `fix(db): add album and track FK guards to enrichment load`

**Rationale:** Both missing FK guards (`album_genre.album_id → album(id)` and
`track_album.track_id → track(id)`) must be added to `load_enrichment()`, with tests
that verify violators are silently dropped and valid rows are inserted.

**Deliverables:**

- `src/db/load.rs`, `load_enrichment()` function:
  - `album_genre_sql`: add `AND e.entity_qid IN (SELECT id FROM album)` after the existing `AND e.genre_qid IN (SELECT id FROM genre)`
  - `track_album_sql`: add `AND e.entity_qid IN (SELECT id FROM track)` after the existing `AND e.parent_album_qid IN (SELECT id FROM album)`

- `src/db/load.rs`, `#[cfg(test)] mod tests` — add two new tests using the existing `write_test_enrichment_parquet` helper:
  1. **`test_load_enrichment_album_genre_album_id_fk_guard`** — enrichment Parquet with two album rows (entity_qid exists vs. doesn't exist in album table). Pre-populate album and genre. Verify album_genre has exactly 1 row.
  2. **`test_load_enrichment_track_album_track_id_fk_guard`** — enrichment Parquet with two track rows (entity_qid exists vs. doesn't exist in track table). Pre-populate track and album. Verify track_album has exactly 1 row.

**Test strategy:** Unit tests — each test verifies the guard silently drops the violator and inserts the valid row. Both tests reuse the existing `write_test_enrichment_parquet` helper.

### Step 2 — Verify with full test suite

**Rationale:** Run the full test suite, linters, and formatter to confirm the fix introduces no regressions. No code changes.

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```
