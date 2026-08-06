# Implementation Plan: Fix DuckDB FK Violation During album.name Backfill

Source: `docs/research/2026-08_fix_backfill_album_name_fk_violation.md`

## Context

The `populate` subcommand fails during the backfill step with a DuckDB foreign key constraint violation when updating `album.name`. DuckDB applies `RESTRICT` semantics to **all** parent-table UPDATEs (including non-PK column updates), so child rows in `album_genre`, `track_album`, and `album_artist` cause the UPDATE to fail. The fix wraps the backfill transaction with `PRAGMA foreign_keys = OFF` / `ON` since only non-key columns are touched.

This plan builds on the prior FK guard work from `agent/fix-enrichment-fk-track-album` (commits 0f67175..33eef88).

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(db): disable FK enforcement during backfill to work around DuckDB FK bug` | PRAGMA fix | `src/db/load.rs` — `backfill_names()` with `PRAGMA foreign_keys = OFF`/`ON` | — |
| 2 | `test(db): add backfill FK resilience test` | Backfill FK test | `src/db/load.rs` — `test_backfill_with_active_fk_constraints` in `#[cfg(test)] mod tests` | Unit |
| 3 | *(verification)* Run full test suite, linters, formatter | Verify | — | — |
| 4 | *(operational)* Re-run populate with `--resume` | Populate verification | — | — |
| 5 | *(verification)* Verify FK integrity after populate | Integrity check | — | — |

## Step details

### Step 0 — Check workspace health

The current branch is `agent/fix-enrichment-fk-track-album`. Ensure the workspace is clean and all tests pass before starting:

```bash
git status          # should be clean (except untracked research doc)
cargo test          # all tests pass
cargo clippy -- -D warnings
cargo fmt --check
```

If the research document `docs/research/2026-08_fix_backfill_album_name_fk_violation.md` is untracked, add it now. Otherwise create a new branch for this work.

### Step 1 — `fix(db): disable FK enforcement during backfill to work around DuckDB FK bug`

**Rationale:** DuckDB rejects `UPDATE album SET name = ...` when child rows exist in referencing tables (`album_genre`, `track_album`, `album_artist`), even though only the non-PK `name` column is updated. Disabling FK enforcement around the backfill transaction is safe because no PK or FK columns are modified.

**Changes in `src/db/load.rs`, `backfill_names()`:**

1. Add `PRAGMA foreign_keys = OFF` before `BEGIN TRANSACTION`
2. On success path: add `PRAGMA foreign_keys = ON` after `COMMIT`
3. On error path: add `PRAGMA foreign_keys = ON` after `ROLLBACK`
4. Add a reference comment pointing to the research document

Exact diff follows the plan in `docs/research/2026-08_fix_backfill_album_name_fk_violation.md` §5.

**Test strategy:** None for this step — covered by Step 2.

### Step 2 — `test(db): add backfill FK resilience test`

**Rationale:** Guarantee the fix works by creating a full-dependency scenario: an album referenced by all three child tables (`album_genre`, `track_album`, `album_artist`) that must be backfilled from `qid_label`.

**New test in `src/db/load.rs`, `#[cfg(test)] mod tests`:**

`test_backfill_with_active_fk_constraints`:

1. Pre-seed `qid_label` with an English label for a known album Q-ID
2. Pre-populate `album` with `name` = the Q-ID (the placeholder)
3. Insert rows into **all three** child tables referencing the album:
   - `album_genre` (create a valid genre first, then a row)
   - `track_album` (create a valid track first, then a row)
   - `album_artist` (create a valid artist first, then a row)
4. Create an empty `enrichment.parquet` (backfill needs the file to exist)
5. Call `backfill_names()` directly
6. Assert the album's `name` column was updated to the English label
7. Assert all child-table rows still reference the album correctly

This test would fail before the PRAGMA fix and succeed after it.

**Test approach:** Call `backfill_names()` directly rather than `load_label_and_enrichment()` (which also requires a `labels.parquet` file). The `qid_label` table is pre-seeded, so the name-resolution subqueries in `backfill_names_inner()` will find the labels. An empty `enrichment.parquet` is created so the enrichment-field UPDATEs (steps 4-6 in `backfill_names_inner`) are no-ops.

### Step 3 — Verify with full test suite

**Rationale:** Confirm no regressions from the PRAGMA change.

```bash
git add -A && git commit -m "test(db): add backfill FK resilience test"
# or run verification before committing
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

If any test fails or clippy reports warnings, fix them. When everything passes, commit.

### Step 4 — Re-run populate (operational)

**Rationale:** Verify the fix works on production data by re-running populate with `--resume`.

```bash
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz \
  --resume
```

Expected outcome:

- Enrichment loads without FK violation
- All backfill UPDATEs complete
- FTS indexes rebuilt
- Summary output shown

### Step 5 — Verify FK integrity after populate

**Rationale:** Confirm no referential integrity was lost despite disabling FK enforcement during backfill.

```sql
-- All album_genre albums exist in album table
SELECT COUNT(*) FROM album_genre ag
LEFT JOIN album a ON ag.album_id = a.id
WHERE a.id IS NULL;
-- Expected: 0

-- All track_album albums exist in album table
SELECT COUNT(*) FROM track_album ta
LEFT JOIN album a ON ta.album_id = a.id
WHERE a.id IS NULL;
-- Expected: 0

-- All album_artist albums exist in album table
SELECT COUNT(*) FROM album_artist aa
LEFT JOIN album a ON aa.album_id = a.id
WHERE a.id IS NULL;
-- Expected: 0
```
