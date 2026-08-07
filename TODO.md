# Implementation Plan: Fix FK Constraint Violation During artist.name Backfill

Source: `docs/research/2026-08_fix_artist_backfill_fk_violation.md`

## Context

The `populate` subcommand fails with a DuckDB FK constraint violation on the `UPDATE artist SET name = ...` statement. The prior fix (`backfill_all_safe`) only backs up and empties four child tables (`album_artist`, `track_artist`, `album_genre`, `track_album`), but three additional tables referencing `artist(id)` are not emptied: `artist_genre`, `artist_instrument`, `artist_member_of`. These three tables contain ~2.7M rows referencing artists with `name IS NULL` (577,504 artists), causing the FK violation.

Additionally, the prior fix runs Phase 1 DELETEs outside the transaction (auto-committed), which permanently lost data on a failed run. The fix moves all DELETEs inside the transaction.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(db): add artist child tables to FK-safe backfill and fix transaction boundary` | Extend `backfill_all_safe` | `src/db/load.rs` — add 3 temp tables, move DELETEs inside transaction, add restore/drop for artist tables | Unit (existing tests updated) |
| 2 | `test(db): add coverage for artist child tables in backfill and rollback` | Test coverage for artist tables | `src/db/load.rs` — update `test_backfill_all_safe_with_child_rows`, `test_backfill_all_safe_rollback_on_error`, add `test_backfill_all_safe_rollback_preserves_artist_tables` | Unit (3 tests) |
| 3 | *(verification)* Run full test suite, linters, formatter | Verify | — | — |

## Step details

### Step 0 — Pre-work

**Branch:** `agent/fix-artist-backfill-fk-violation`. Verify workspace clean and tests pass before starting.

```bash
git status
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

The research document `docs/research/2026-08_fix_artist_backfill_fk_violation.md` is untracked — add it before the first commit.

### Step 1 — `fix(db): add artist child tables to FK-safe backfill and fix transaction boundary`

**Rationale:** The prior fix's `backfill_all_safe` only handles four child tables referencing `album(id)` and `track(id)`. Three tables referencing `artist(id)` — `artist_genre`, `artist_instrument`, `artist_member_of` — are not emptied, causing the `UPDATE artist` to fail with FK violations. Additionally, the prior fix runs DELETEs outside the transaction (auto-committed), which permanently loses data on failure. This fix extends the backup/delete/restore cycle to cover all seven child tables and moves DELETEs inside the transaction.

**Changes in `src/db/load.rs`:**

1. **Add three new temp table creations in `backfill_all_safe`:**
   - `CREATE TEMP TABLE IF NOT EXISTS _bak_artist_genre AS SELECT * FROM artist_genre`
   - `CREATE TEMP TABLE IF NOT EXISTS _bak_artist_instrument AS SELECT * FROM artist_instrument`
   - `CREATE TEMP TABLE IF NOT EXISTS _bak_artist_member_of AS SELECT * FROM artist_member_of`

2. **Move the seven DELETE statements inside the transaction** (after `BEGIN TRANSACTION`, before `backfill_all_safe_inner`):
   - `DELETE FROM track_album`
   - `DELETE FROM album_genre`
   - `DELETE FROM track_artist`
   - `DELETE FROM album_artist`
   - `DELETE FROM artist_member_of`
   - `DELETE FROM artist_instrument`
   - `DELETE FROM artist_genre`

   Remove the four DELETEs from their current location (outside the transaction). The CREATE TEMP TABLE statements remain outside the transaction (temp tables are session-scoped, not transaction-scoped).

3. **Add three new restores and drops to `backfill_all_safe_inner`:**
   - `INSERT INTO artist_genre SELECT * FROM _bak_artist_genre`
   - `INSERT INTO artist_instrument SELECT * FROM _bak_artist_instrument`
   - `INSERT INTO artist_member_of SELECT * FROM _bak_artist_member_of`
   - `DROP TABLE IF EXISTS _bak_artist_genre`
   - `DROP TABLE IF EXISTS _bak_artist_instrument`
   - `DROP TABLE IF EXISTS _bak_artist_member_of`

4. **Update doc comments** on both `backfill_all_safe` and `backfill_all_safe_inner` to:
   - Describe the full set of seven child tables
   - Document the new transaction boundary (DELETEs inside the transaction)
   - Remove the incorrect rationale about auto-committing Phase 1

5. **Run the existing test suite** — all existing tests must pass. The existing `test_backfill_all_safe_with_child_rows` and `test_backfill_all_safe_rollback_on_error` may fail or need updating (see Step 2).

**Commit:** `fix(db): add artist child tables to FK-safe backfill and fix transaction boundary`

### Step 2 — `test(db): add coverage for artist child tables in backfill and rollback`

**Rationale:** The existing tests only cover the four album/track child tables. New test coverage verifies the three artist child tables are preserved after backfill and survive rollback.

**Changes in `src/db/load.rs` (tests module):**

1. **Update `test_backfill_all_safe_with_child_rows`:**
   - Add pre-seeded rows for `artist_genre`, `artist_instrument`, `artist_member_of` with valid FK references to `artist(id)`
   - For `artist_member_of`, include a row where both `artist_id` and `group_id` reference valid artists (exercises the self-referencing FK edge case)
   - Assert row counts are preserved after backfill for all seven child tables
   - Add FK integrity checks for all seven child tables (not just the four current ones)

2. **Update `test_backfill_all_safe_rollback_on_error`:**
   - After moving Phase 1 DELETEs inside the transaction, a rollback should preserve child-table rows
   - Remove the assertion that accepts empty child tables as expected behavior
   - Add assertions that `album_artist` rows are intact after rollback
   - Add similar assertions for artist child tables (`artist_genre`, `artist_instrument`, `artist_member_of`)
   - The temp table backup assertion (`_bak_album_artist`) should be updated to verify all seven temp tables hold backup data

3. **Add `test_backfill_all_safe_rollback_preserves_artist_tables`:**
   - A focused test that specifically validates the gap from the prior fix
   - Pre-seed `artist_genre`, `artist_instrument`, and `artist_member_of` with rows referencing valid artists
   - Trigger a rollback (e.g., missing `enrichment.parquet`)
   - Assert all row counts are preserved in the three artist child tables
   - Assert FK integrity holds for all three tables

**Commit:** `test(db): add coverage for artist child tables in backfill and rollback`

### Step 3 — Verify

**Rationale:** Confirm no regressions and all code quality checks pass.

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

Expected test results:

- `test_backfill_all_safe_with_child_rows` — now asserts all 7 child tables preserved
- `test_backfill_all_safe_rollback_on_error` — now asserts child rows preserved after rollback
- `test_backfill_all_safe_rollback_preserves_artist_tables` (new) — passes

### Post-implementation: Operational recovery

After the fix is committed, recover the permanently lost `album_artist` and `track_artist` rows by re-bootstrapping:

```bash
rm /home/tr/wiki_db/music.duckdb
rm -f parquet-dir/labels.parquet parquet-dir/enrichment.parquet
cargo run --release -- bootstrap \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz
```
