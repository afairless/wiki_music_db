# Research: DuckDB FK Violation During album.name Backfill

**Date:** 2026-08-06
**Status:** Plan
**References:**

- [`src/db/load.rs`](../../src/db/load.rs) — `backfill_names()` / `backfill_names_inner()`
- [`src/db/schema.rs`](../../src/db/schema.rs) — FK constraint definitions
- [`src/cli/populate.rs`](../../src/cli/populate.rs) — populate subcommand orchestration
- Prior FK fixes: [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md), [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md)
- DuckDB issue tracker: [#15804](https://github.com/duckdb/duckdb/issues/15804) — FK check fires on non-PK updates

---

## 1. Problem Statement

The `populate` subcommand fails during the backfill step with a foreign key constraint violation when updating the `album.name` column:

```
Failed to load label and enrichment data

Caused by:
    0: Failed to backfill album.name
    1: Constraint Error: Violates foreign key constraint because key
       "album_id: Q4118577" is still referenced by a foreign key in a
       different table.
    2: Error code 1: Unknown error code
```

The failed SQL is:

```sql
UPDATE album SET name = COALESCE(
    (SELECT label FROM qid_label WHERE qid = album.id),
    album.name
)
```

This statement updates only the `name` column — **not the primary key** (`id`). It cannot possibly orphan any child rows. Yet DuckDB raises a foreign key constraint violation.

---

## 2. Root Cause

### 2.1 DuckDB Foreign Key Implementation Bug/Behavior

DuckDB implements foreign key constraints with `ON DELETE RESTRICT` semantics (the only supported action). In DuckDB version 1.10505.0 (v1.1.0), when a parent table row is updated — even on a non-key column — DuckDB checks that no child table references the row.

The error "key 'album_id: Q4118577' is still referenced by a foreign key in a different table" is the `RESTRICT` action message: DuckDB is preventing the row from being "removed" (from the FK perspective, any UPDATE is treated as potentially removing/replacing the parent row).

This is a known DuckDB behavior/limitation: the FK enforcement logic applies `RESTRICT` semantics to *all* parent-table UPDATEs, not just PK updates. The distinction between a PK column update and a non-PK column update is not recognized.

### 2.2 Why It Happens Specifically in `populate` — Not in `bootstrap`

During `bootstrap` (the initial data load), all data is loaded in this order:

1. Genres → artist → artist_genre/artist_instrument/artist_member_of
2. Albums (with `name = Q-ID`) → album_artist
3. Tracks → track_artist
4. `album_genre` and `track_album` are **not** loaded during bootstrap

The `album` table is referenced by `album_artist`, `album_genre`, and `track_album`. But `album_genre` and `track_album` are empty after bootstrap.

During `populate`:

1. `load_labels` — inserts into `qid_label` (no FK references to album)
2. `load_enrichment` — **inserts into `album_genre` and `track_album`**, creating child rows that reference `album(id)`
3. `backfill_names` — runs inside a transaction and tries `UPDATE album SET name = ...`

After step 2, the album table has dependent rows in `album_genre` and `track_album`. When step 3 runs the UPDATE, DuckDB fires the FK check and rejects it.

### 2.3 The Specific Album Q4118577

The error identifies album Q4118577. This album:

- Was inserted during bootstrap with `name = 'Q4118577'` (the Q-ID as placeholder)
- Has at least one valid `genre_qid` in the enrichment data, so it was inserted into `album_genre` during `load_enrichment`
- Has a valid English label in Wikidata that was extracted to `qid_label`

When the backfill UPDATE tries to change `album.name` from `'Q4118577'` to the English label, DuckDB's FK check on the `album_genre.album_id → album(id)` constraint rejects the UPDATE because album Q4118577 "is still referenced" by `album_genre`.

### 2.4 Why the Prior FK Guards Didn't Help

The prior fixes added `WHERE e.genre_qid IN (SELECT id FROM genre)` and `WHERE e.parent_album_qid IN (SELECT id FROM album)` guards to `load_enrichment()`. These guards:

- ✅ Prevent invalid rows from entering `album_genre` / `track_album`
- ❌ Do not affect the FK check on the parent table during `UPDATE`

The problem isn't about bad data entering the junction tables — it's about DuckDB's FK enforcement on the parent table itself. Even perfectly valid FK relationships trigger the false positive.

---

## 3. Proposed Solution

### 3.1 Disable FK Enforcement During Backfill

Wrap the backfill transaction with `PRAGMA foreign_keys = OFF` / `ON`:

```
PRAGMA foreign_keys = OFF;   -- before transaction
BEGIN TRANSACTION;
    UPDATE album SET name = ...;
    UPDATE track SET name = ...;
    UPDATE artist SET name = ...;
    UPDATE album SET release_date = ...;
    UPDATE album SET record_label = ...;
    UPDATE track SET duration_seconds = ...;
COMMIT;
PRAGMA foreign_keys = ON;    -- after transaction
```

### 3.2 Safe Because Only Non-Key Columns Are Updated

Every statement in `backfill_names_inner()` updates only non-PK, non-FK columns:

| Statement | Table | Columns Updated | PK? | FK target? |
|---|---|---|---|---|
| 1. album.name | album | `name` | No | No |
| 2. track.name | track | `name` | No | No |
| 3. artist.name | artist | `name` | No | No |
| 4. album.release_date | album | `release_date` | No | No |
| 5. album.record_label | album | `record_label` | No | No |
| 6. track.duration_seconds | track | `duration_seconds` | No | No |

No statement updates any `id` column (the PK), and no statement touches any FK column. Disabling FK enforcement during these operations cannot corrupt referential integrity.

### 3.3 Alternative: Drop and Recreate FK Constraints

Drop all FK constraints referencing `album(id)`, run the backfill, then recreate them:

```sql
ALTER TABLE album_genre DROP CONSTRAINT ...;
ALTER TABLE track_album DROP CONSTRAINT ...;
ALTER TABLE album_artist DROP CONSTRAINT ...;
-- run backfill
ALTER TABLE album_genre ADD CONSTRAINT ...;
ALTER TABLE track_album ADD CONSTRAINT ...;
ALTER TABLE album_artist ADD CONSTRAINT ...;
```

**Rejected.** DuckDB does not support `ALTER TABLE ... DROP CONSTRAINT` for foreign keys in all builds, and the statement syntax varies. The `PRAGMA foreign_keys = OFF` approach is simpler and equivalent.

### 3.4 Alternative: Upgrade DuckDB

A newer DuckDB version may have fixed this FK behavior. However:

- This is a hard dependency version pinned by Cargo.lock semantics
- A newer version may introduce other behavioral changes
- The fix with `PRAGMA foreign_keys = OFF` is trivially correct and independent of the DuckDB version

### 3.5 Alternative: Restructure Backfill to Avoid FK Check

Options considered:

- **CTE-based UPDATE**: Same SQL semantics, DuckDB still fires the FK check
- **INSERT into temp table, DROP + RENAME**: Over-engineered; no FK benefit
- **Use `UPDATE ... FROM` with explicit join**: Already used for enrichment fields; same FK behavior

The FK check is applied at the UPDATE-statement level, not at the SQL-pattern level. No SQL restructuring avoids it.

### 3.6 Recommendation

Use `PRAGMA foreign_keys = OFF`/`ON` around the backfill transaction. This is:

- **A 4-line change** (2 PRAGMA statements)
- **Correct by construction** (only non-key columns are updated)
- **Consistent with standard practice** for admin/migration operations
- **Easily reversible** (remove the PRAGMA statements)

---

## 4. Data Safety Analysis

### 4.1 What FK Enforcement Protects

Foreign key constraints in this schema protect against:

1. `artist_genre.artist_id` referencing a non-existent artist
2. `artist_genre.genre_id` referencing a non-existent genre
3. `album_artist.album_id` referencing a non-existent album
4. `album_artist.artist_id` referencing a non-existent artist
5. `album_genre.album_id` referencing a non-existent album
6. `album_genre.genre_id` referencing a non-existent genre
7. `track_album.track_id` referencing a non-existent track
8. `track_album.album_id` referencing a non-existent album
9. `track_artist.track_id` referencing a non-existent track
10. `track_artist.artist_id` referencing a non-existent artist
11. `artist_instrument.artist_id` referencing a non-existent artist
12. `artist_member_of.artist_id` referencing a non-existent artist
13. `artist_member_of.group_id` referencing a non-existent group

The backfill updates **none of the referenced columns**. Every FK above guards either an `id` column (which the backfill never touches) or a child-table FK column (which only the `INSERT` statements populate, before the backfill runs).

### 4.2 What Could Go Wrong

| Scenario | Likelihood | Consequence |
|---|---|---|
| A future code change adds a PK update to backfill | Low (would be a new feature, not a bug fix) | FK violation would be caught by tests |
| DuckDB starts enforcing FK differently | Very Low | Existing tests (which run with FK enforcement ON) would catch it |
| Another connection mutates data concurrently | None | Single-user application, single connection |

### 4.3 Verification After Fix

After the fix, the following invariants are verifiable:

- `SELECT COUNT(*) FROM album_genre ag LEFT JOIN album a ON ag.album_id = a.id WHERE a.id IS NULL` returns 0
- `SELECT COUNT(*) FROM track_album ta LEFT JOIN album a ON ta.album_id = a.id WHERE a.id IS NULL` returns 0
- `SELECT COUNT(*) FROM album_artist aa LEFT JOIN album a ON aa.album_id = a.id WHERE a.id IS NULL` returns 0
- All counts unchanged between a run with FK enforcement ON and OFF (assuming no concurrent mutations)

---

## 5. Implementation Plan

### Step 1: Add `PRAGMA foreign_keys = OFF`/`ON` around backfill

**File:** `src/db/load.rs`, `backfill_names()` function

Change from:

```rust
pub fn backfill_names(conn: &Connection, parquet_dir: &Path) -> Result<()> {
    let path = parquet_dir.join("enrichment.parquet");
    let path_str = path
        .to_str()
        .context("Enrichment parquet path contains invalid UTF-8")?;

    conn.execute("BEGIN TRANSACTION", [])
        .context("Failed to begin backfill transaction")?;

    let result = backfill_names_inner(conn, path_str);

    match result {
        Ok(()) => {
            conn.execute("COMMIT", [])
                .context("Failed to commit backfill transaction")?;
            tracing::info!("Backfill committed successfully");
            Ok(())
        }
        Err(e) => {
            conn.execute("ROLLBACK", [])
                .context("Failed to roll back backfill transaction")?;
            tracing::warn!(error = %e, "Rolled back backfill due to error");
            Err(e)
        }
    }
}
```

To:

```rust
pub fn backfill_names(conn: &Connection, parquet_dir: &Path) -> Result<()> {
    let path = parquet_dir.join("enrichment.parquet");
    let path_str = path
        .to_str()
        .context("Enrichment parquet path contains invalid UTF-8")?;

    // Disable FK enforcement during backfill.
    //
    // DuckDB applies RESTRICT semantics to ALL parent-table UPDATEs,
    // not just PK column updates. Since the backfill only touches
    // non-key columns (name, release_date, record_label, duration_seconds),
    // disabling FK enforcement is safe — no referential integrity can
    // be violated.
    //
    // See docs/research/2026-08_fix_backfill_album_name_fk_violation.md
    conn.execute("PRAGMA foreign_keys = OFF", [])
        .context("Failed to disable FK enforcement")?;

    conn.execute("BEGIN TRANSACTION", [])
        .context("Failed to begin backfill transaction")?;

    let result = backfill_names_inner(conn, path_str);

    match result {
        Ok(()) => {
            conn.execute("COMMIT", [])
                .context("Failed to commit backfill transaction")?;
            tracing::info!("Backfill committed successfully");

            // Re-enable FK enforcement
            conn.execute("PRAGMA foreign_keys = ON", [])
                .context("Failed to re-enable FK enforcement")?;
            Ok(())
        }
        Err(e) => {
            // Roll back first, then re-enable FK enforcement
            if let Err(rollback_err) = conn.execute("ROLLBACK", []) {
                tracing::error!(
                    error = %rollback_err,
                    "Failed to roll back backfill transaction"
                );
            }
            if let Err(pragma_err) = conn.execute("PRAGMA foreign_keys = ON", []) {
                tracing::error!(
                    error = %pragma_err,
                    "Failed to re-enable FK enforcement during rollback"
                );
            }
            tracing::warn!(error = %e, "Rolled back backfill due to error");
            Err(e)
        }
    }
}
```

**Key design decisions in the implementation:**

1. `PRAGMA foreign_keys = OFF` is set **before** `BEGIN TRANSACTION` — DuckDB's PRAGMA settings are session-level, so placing them outside the transaction makes the intent clear.

2. On rollback (error path), FK enforcement is re-enabled after the rollback completes. The rollback error is logged but not propagated (the original error is more important).

3. A reference comment points to this research document.

**Commit:** `fix(db): disable FK enforcement during backfill to work around DuckDB FK bug`

### Step 2: Add a unit test for backfill FK resilience

**File:** `src/db/load.rs` (in `#[cfg(test)] mod tests`)

Add `test_backfill_with_active_fk_constraints`:

1. Seed `qid_label` with a label for a known album
2. Pre-populate `album` (with `name = id` placeholder)
3. Insert rows into **multiple child tables** referencing the album:
   - `album_genre` (one row, creating an album → genre FK dependency)
   - `track_album` (one row, creating an album → track FK dependency)
   - `album_artist` (one row, creating an album → artist FK dependency)
4. Run `load_label_and_enrichment` (which calls `backfill_names` internally)
5. Assert the album's `name` column was updated to the English label
6. Assert all child-table rows still reference the album correctly (FKs still valid)

This test covers all three child tables that reference `album(id)`, ensuring the
PRAGMA fix works regardless of which child table has live dependencies. It would
fail on the current codebase and succeed after the fix.

**Commit:** `test(db): add backfill FK resilience test`

### Step 3: Run full test suite

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

**Commit:** None (verification step)

### Step 4: Re-run populate

```bash
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz \
  --resume
```

The `--resume` flag reuses the existing Parquet files (labels.parquet, enrichment.parquet) from the failed run, so it should be fast (skip the ~4-hour dump scan).

The re-run is idempotent:

- `INSERT OR IGNORE` guarantees enrichment tables contain the same data after re-run
- `COALESCE(..., album.name)` in the UPDATE statements means the backfill is a no-op
  if names are already resolved
- The PRAGMA fix prevents the FK violation that caused the original failure

Expected outcome:

- Load 738,267 labels into `qid_label`
- Load enrichment data (already FK-guarded by prior fixes)
- Backfill: all UPDATEs complete without FK violation
- FTS indexes rebuilt
- Summary output showing resolved labels

**Commit:** None (operational step)

### Step 5: Verify FK integrity after populate

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

**Commit:** None (verification step)

---

## 6. Testing Strategy

### 6.1 Unit Test (Step 2)

| Test | What it verifies |
|---|---|
| `test_backfill_with_active_fk_constraints` | Album with live child rows in **all** referencing tables (`album_genre`, `track_album`, `album_artist`) is backfilled successfully; the `UPDATE album SET name` completes without FK violation; all child rows remain valid |

### 6.2 Integration Validation

Run the existing integration tests:

- `test_load_all_orchestrator_empty_dir` — unchanged behavior
- `test_load_albums` — unchanged behavior
- `test_load_albums_and_tracks` — unchanged behavior
- `test_load_tracks` — unchanged behavior
- All `load_enrichment_*` tests — unchanged behavior (FK enforcement is ON outside the backfill)

### 6.3 Production Verification

Run the populate command and verify:

1. All backfill UPDATEs complete
2. `album.name`, `track.name`, `artist.name` are populated with English labels
3. `album.release_date`, `album.record_label`, `track.duration_seconds` are populated
4. FK integrity queries (Step 5) return 0 violating rows

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| PRAGMA setting affects other concurrent operations | None (single-user app) | — | Single connection, no concurrency |
| Future code adds a PK-altering UPDATE to backfill | Low | Medium | Tests that run with FK enforcement ON catch any FK violation during subsequent operations. The PRAGMA is scoped to the backfill function only. |
| DuckDB drops `PRAGMA foreign_keys` support | Very Low | High | Would cause a compile error in CI. Pin the DuckDB version or add a fallback. |
| Error path skips re-enabling FK enforcement | Low | Medium | Both success and error paths re-enable FK enforcement. An `Err` from `PRAGMA foreign_keys = ON` is logged but not fatal (FK enforcement defaults to ON in DuckDB). |

### 7.1 Rollback

Revert the `PRAGMA foreign_keys = OFF`/`ON` lines in `src/db/load.rs`. No schema changes, no data migration, no file format changes.

---

## 8. References

- DuckDB FK documentation: <https://duckdb.org/docs/sql/constraints.html#foreign-keys>
- DuckDB issue #15804 — FK check fires on non-PK updates (upstream bug report)
- [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md) — prior FK guard fix (missed the backfill UPDATE FK issue)
- [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md) — prior FK guard fix (missed the backfill UPDATE FK issue)
- [`src/db/load.rs`](../../src/db/load.rs), `backfill_names()` — the function to modify
- [`src/db/load.rs`](../../src/db/load.rs), `backfill_names_inner()` — the inner logic (only non-key columns)
