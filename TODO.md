# Implementation Plan: Fix FK Constraint Violations in Enrichment Loading

Source: `docs/research/2026-08_fix_enrichment_fk_constraints.md`

## Context

The `populate` subcommand's `load_enrichment()` function in `src/db/load.rs` fails with foreign key constraint violations when loading into `album_genre` and `track_album` junction tables. The `album_genre` insert lacks a `WHERE genre_qid IN (SELECT id FROM genre)` FK guard, and `track_album` lacks a `WHERE parent_album_qid IN (SELECT id FROM album)` FK guard. Both are needed because enrichment data references genres/albums that may not exist in the database.

The fix is purely additive SQL — two `AND ... IN (SELECT ...)` clauses following the established pattern in `load_artist_genre()` and `load_artist_member_of()`.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(db): guard album_genre FK constraint in enrichment loader` | FK guard — album_genre | `src/db/load.rs` — add `AND e.genre_qid IN (SELECT id FROM genre)` to `album_genre_sql` | — |
| 2 | `fix(db): guard track_album FK constraint in enrichment loader` | FK guard — track_album | `src/db/load.rs` — add `AND e.parent_album_qid IN (SELECT id FROM album)` to `track_album_sql` | — |
| 3 | `test(db): add FK guard tests for enrichment loading` | Unit tests | `src/db/load.rs` — `write_test_enrichment_parquet` helper, `test_load_enrichment_album_genre_fk_guard`, `test_load_enrichment_track_album_fk_guard` | Unit |
| 4 | `chore: verify fix with full test suite and linters` | Verification | — | All existing tests, clippy, fmt |

## Step details

### Step 1 — `fix(db): guard album_genre FK constraint in enrichment loader`

**Rationale:** The `album_genre` insert in `load_enrichment()` does not filter against the `genre` table. Genre Q-IDs from album P136 claims may reference genres not present in the `genre` table (measured: 6 out of 56,074 links, 0.01%). DuckDB's `INSERT OR IGNORE` does not suppress FK violations — a hard `Constraint Error` is raised.

**Deliverables:**

- `src/db/load.rs`, `load_enrichment()` function, around line 193:
  - Add `AND e.genre_qid IN (SELECT id FROM genre)` to the `album_genre_sql` SQL string

**Test strategy:** No tests for this step alone — tests for both guards are added in Step 3.

---

### Step 2 — `fix(db): guard track_album FK constraint in enrichment loader`

**Rationale:** The `track_album` insert in `load_enrichment()` does not filter against the `album` table. Track P361 (parent album) claims reference albums that are not linked to any artist in the database (measured: 7,087 out of 7,091 links, 99.9%).

**Deliverables:**

- `src/db/load.rs`, `load_enrichment()` function, around line 207:
  - Add `AND e.parent_album_qid IN (SELECT id FROM album)` to the `track_album_sql` SQL string

**Test strategy:** No tests for this step alone — tests for both guards are added in Step 3.

---

### Step 3 — `test(db): add FK guard tests for enrichment loading`

**Rationale:** Add unit tests that verify both FK guards work correctly, following the established pattern in the existing test module.

**Deliverables:**

- `src/db/load.rs`, `#[cfg(test)] mod tests`:
  1. Add a `write_test_enrichment_parquet(dir: &Path)` helper that writes `enrichment.parquet` with the exact production schema (7 columns: `entity_qid`, `entity_type`, `release_date`, `record_label_qid`, `duration_seconds`, `genre_qid`, `parent_album_qid`), mirroring the existing `write_test_genres_parquet` / `write_test_albums_tracks_parquet` helpers using `StringBuilder` / `ArrowWriter`.
  2. Add `test_load_enrichment_album_genre_fk_guard` — writes two album rows (`entity_type='album'`): one with `genre_qid` that exists in the `genre` table, one with `genre_qid` that does not. Pre-populates the `genre` table (with the valid genre) and the `album` table (with the valid row's `entity_qid`). Asserts `album_genre` contains exactly the valid row (count = 1, correct `genre_id`).
  3. Add `test_load_enrichment_track_album_fk_guard` — writes two track rows (`entity_type='track'`): one with `parent_album_qid` that exists in the `album` table, one that does not. Pre-populates the `album` table (with the valid `parent_album_qid`) and the `track` table (with the valid row's `entity_qid`). Asserts `track_album` contains exactly the valid row (count = 1, correct `album_id`).

**Test strategy:** Unit tests — each test verifies both directions of one guard (valid row inserted, invalid row silently dropped).

---

### Step 4 — `chore: verify fix with full test suite and linters`

**Rationale:** Run the full test suite, linters, and formatter to confirm the fix introduces no regressions. No code changes.

**Deliverables:**

- `cargo test` — all tests pass
- `cargo clippy -- -D warnings` — no warnings
- `cargo fmt --check` — formatting is clean

**Commit:** None — verification step only.
