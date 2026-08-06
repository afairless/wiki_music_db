# Implementation Plan: Fix Backfill FK Violations with Temp-Table Swap

Source: `docs/research/2026-08_fix_backfill_fk_safe_emptying.md`

## Context

The `populate` subcommand fails with a DuckDB FK constraint violation when `UPDATE album SET name = ...` runs because `album_artist` and `track_artist` (populated during bootstrap) already contain child rows referencing `album(id)` and `track(id)`. DuckDB's RESTRICT enforcement fires on **all** parent-table UPDATEs — even non-key column updates — and the bundled version provides no working `PRAGMA foreign_keys = OFF`.

The fix wraps name and enrichment-field UPDATEs in a single transaction that temporarily backs up child-table rows to temp tables, DELETEs them from the child tables, runs all UPDATEs, then restores the rows. Since child-row deletion is always allowed, the UPDATEs proceed without FK violations.

Two prior fix attempts are superseded (commits `b870d31` and `aaef0a3`). The old approach in those commits is replaced entirely.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(db): add FK-safe backfill with temp-table swap and remove dead code` | `backfill_all_safe` function | `src/db/load.rs` — new `backfill_all_safe()`, remove `backfill_names()`, `backfill_names_inner()`, `backfill_enrichment_fields()`, `backfill_enrichment_inner()` | Unit (2 tests) |
| 2 | `fix(db): integrate FK-safe backfill into populate pipeline` | Pipeline orchestration | `src/db/load.rs` — `load_label_and_enrichment()` updated to call `backfill_all_safe()` | — |
| 3 | *(verification)* Run full test suite, linters, formatter | Verify | — | — |
| 4 | *(operational)* Clean stale enrichment data and re-run populate | Populate verification | — | — |

## Step details

### Step 0 — Pre-work

**Branch:** `agent/fix-backfill-album-name-fk`. Verify workspace clean and tests pass before starting.

```bash
git status
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

The research document `docs/research/2026-08_fix_backfill_fk_safe_emptying.md` is untracked — add it before the first commit.

### Step 1 — `fix(db): add FK-safe backfill with temp-table swap and remove dead code`

**Rationale:** Replace the four backfill functions (`backfill_names`, `backfill_names_inner`, `backfill_enrichment_fields`, `backfill_enrichment_inner`) with a single `backfill_all_safe()` that atomically backs up child tables, empties them, runs all UPDATEs, and restores them within one transaction.

**Changes in `src/db/load.rs`:**

1. **Add `backfill_all_safe(conn: &Connection, parquet_dir: &Path) -> Result<()>`:**
   - `BEGIN TRANSACTION`
   - Create temp tables: `CREATE TEMP TABLE _bak_album_artist AS SELECT * FROM album_artist` (repeat for `_bak_track_artist`, `_bak_album_genre`, `_bak_track_album`)
   - `DELETE FROM` all four child tables (in reverse dependency order: `track_album`, `album_genre`, `track_artist`, `album_artist`)
   - Run the three name UPDATEs from `backfill_names_inner` (album, track, artist)
   - Run the three enrichment-field UPDATEs from `backfill_enrichment_inner` (release_date, record_label, duration_seconds) using `read_parquet('{enrichment_path}')`
   - `INSERT INTO ... SELECT * FROM _bak_*` for all four child tables
   - `DROP TABLE IF EXISTS _bak_*` for all four temp tables
   - `COMMIT`
   - On error: `ROLLBACK`, return the error
   - Per-step `tracing::info!`/`tracing::debug!` calls matching the plan in §3.3

2. **Remove functions:** `backfill_names()`, `backfill_names_inner()`, `backfill_enrichment_fields()`, `backfill_enrichment_inner()`

3. **Remove old test:** `test_backfill_with_active_fk_constraints` (tests the reordering approach, superseded)

4. **Add two new tests:**

   - **`test_backfill_all_safe_with_child_rows`** — Pre-seed `qid_label`, populate `album` and `track` with Q-ID placeholders, insert rows into all four child tables (`album_artist`, `track_artist`, `album_genre`, `track_album`) with valid FK references, write a real `enrichment.parquet` with release_date/record_label/duration_seconds data, call `backfill_all_safe`, assert names and enrichment fields were updated, assert all four child-table row counts and FK integrity are preserved.

   - **`test_backfill_all_safe_rollback_on_error`** — Pre-seed data, set up child rows, create enrichment.parquet at a path that will cause an error (e.g., nonexistent enrichment path, or corrupt data), call `backfill_all_safe`, assert it returns an error, assert all child tables have their original row counts and parent-table names are unchanged (no partial state).

### Step 2 — `fix(db): integrate FK-safe backfill into populate pipeline`

**Rationale:** Wire the new `backfill_all_safe()` into `load_label_and_enrichment()` and update its doc comment to reflect the new pipeline order.

**Changes in `src/db/load.rs`, `load_label_and_enrichment()`:**

1. Replace the two calls `backfill_names(conn, parquet_dir)?` and `backfill_enrichment_fields(conn, parquet_dir)?` with a single `backfill_all_safe(conn, parquet_dir)?`
2. Update the doc comment to describe the temp-table swap approach:
   - Step order: `load_labels` → `backfill_all_safe` → `load_enrichment` → `create_fts_indexes`
   - Explain that child tables are temporarily emptied within a transaction to avoid FK violations

### Step 3 — Verify

**Rationale:** Confirm no regressions and all code quality checks pass.

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

If the old `test_backfill_with_active_fk_constraints` was removed properly, no tests should reference the old functions. If the new tests exercise all four child tables with real enrichment data, the FK-safe approach is validated.

### Step 4 — Re-run populate (operational)

**Rationale:** Verify the fix works on production data. Prior failed runs left stale enrichment data that must be cleared first.

```bash
# Clear stale enrichment data from the failed run
duckdb /home/tr/wiki_db/music.duckdb -c "
DELETE FROM album_genre;
DELETE FROM track_album;
DELETE FROM record_label;
DELETE FROM instrument;
"

# Re-run with --resume to reuse existing Parquet files
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz \
  --resume
```

Expected outcome: `backfill_all_safe` completes without FK errors, enrichment loads, FTS indexes rebuilt, summary shown. After completion, run the FK integrity queries from §4.2 of the research plan — all should return 0.
