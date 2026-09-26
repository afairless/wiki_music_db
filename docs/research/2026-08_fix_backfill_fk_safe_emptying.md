# Research: Fix Backfill FK Violations by Temporarily Emptying Child Tables

**Date:** 2026-08-06
**Status:** Implemented
**Implemented:** 2026-08 — closes with commits `989d133` (plan) + `1094283` (FK-safe backfill with temp-table swap) — see git log.

*Status updated per the [documentation conventions in AGENTS.md](../../AGENTS.md) — implemented plans carry `Status: Implemented` with their closing commits.*
**References:**

- [2026-08_fix_backfill_album_name_fk_violation.md](./2026-08_fix_backfill_album_name_fk_violation.md) — prior fix attempts (PRAGMA, then reordering)
- [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md) — FK guard fixes for enrichment loading
- [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md) — album/track FK guard fixes
- [`src/db/load.rs`](../../src/db/load.rs) — `backfill_names()`, `backfill_enrichment_fields()`, `load_label_and_enrichment()`
- [`src/db/schema.rs`](../../src/db/schema.rs) — FK constraint definitions
- [`src/cli/populate.rs`](../../src/cli/populate.rs) — populate subcommand orchestration

---

## 1. Problem Statement

The `populate` subcommand fails with a foreign key constraint violation when `backfill_names()` runs `UPDATE album SET name = ...`. The error:

```
Constraint Error: Violates foreign key constraint because key "album_id: Q4118577"
is still referenced by a foreign key in a different table
```

DuckDB v1.1.0 (bundled) fires foreign-key RESTRICT enforcement on **all** parent-table UPDATEs — even when the column being updated (`name`) is neither a primary key nor a foreign key column. Two prior fix attempts have failed:

### 1.1 Prior fix attempt 1: `PRAGMA foreign_keys = OFF` (commit `b870d31`)

The research plan correctly diagnosed the problem and proposed wrapping the backfill with `PRAGMA foreign_keys = OFF` / `ON`. This was implemented but **the bundled DuckDB build does not support this PRAGMA** — it is a silent no-op. The `backfill_names` UPDATE still triggered FK enforcement.

### 1.2 Prior fix attempt 2: Pipeline reordering (commit `aaef0a3`)

With PRAGMA non-functional, the developer pivoted to reordering the pipeline so `backfill_names` runs **before** `load_enrichment`:

| Step | Operation | Child tables referencing album/track |
|---|---|---|
| 1 | `load_labels` | (none) |
| 2 | `backfill_names` | ← assumed empty |
| 3 | `load_enrichment` | populates `album_genre`, `track_album` |
| 4 | `backfill_enrichment_fields` | — |
| 5 | `create_fts_indexes` | — |

**The fatal flaw**: the reordering only ensures `album_genre` and `track_album` are empty before step 2. But `album_artist` and `track_artist` — which also have FK constraints referencing `album(id)` and `track(id)` — are populated during **bootstrap** (`load_all` → `load_albums_and_tracks`), not during `populate`. By the time `populate` runs, these tables already contain FK references:

| Child table | FK constraint | Rows (from bootstrap) | Source |
|---|---|---|---|
| `album_artist` | `album_artist.album_id → album(id)` | 646,106 | `load_all` |
| `track_artist` | `track_artist.track_id → track(id)` | 61,699 | `load_all` |
| `album_genre` | `album_genre.album_id → album(id)` | 0 → 56,068 | `load_enrichment` (or prior failed run) |
| `track_album` | `track_album.album_id → album(id)`, `track_album.track_id → track(id)` | 0 → 4 | `load_enrichment` (or prior failed run) |

Running `backfill_names` before `load_enrichment` only helps against rows 3-4. Rows 1-2 are **always present** after bootstrap and **always** trigger FK enforcement on `UPDATE album/track SET name = ...`.

Additionally, `load_enrichment` uses no wrapping transaction — each `INSERT OR IGNORE` auto-commits. A previous failed `populate` run that got past `load_enrichment` but crashed at `backfill_names` leaves 56,068 rows in `album_genre` and 4 rows in `track_album` permanently committed. These compound the FK problem on every retry.

### 1.3 The test doesn't catch this

`test_backfill_with_active_fk_constraints` (line 2462) tests the reordering approach but inserts child rows **after** `backfill_names` succeeds, then only calls `backfill_enrichment_fields` with an **empty enrichment Parquet** (four `&[]` slices). It never exercises the real scenario: `UPDATE album SET name = ...` with pre-existing rows in `album_artist`.

---

## 2. Root Cause Summary

DuckDB's FK RESTRICT enforcement fires on `UPDATE album SET name = ...` for **every** row that has child references in any table with `REFERENCES album(id)`. Four child tables have this constraint, and at least `album_artist` and `track_artist` always have data (from bootstrap). Neither SQL restructuring, transaction ordering, nor PRAGMA can suppress this check.

The only way to run a parent-table UPDATE without hitting FK restrictions is to ensure **zero child rows exist** at the time the UPDATE executes.

---

## 3. Proposed Solution

### 3.1 Core approach: temporarily empty child tables within a transaction

The key insight is that FK RESTRICT only prevents parent-row deletion/update — **deleting rows from a child table is always allowed**. We can:

1. Back up all child-table rows to DuckDB temporary tables
2. DELETE all rows from the child tables (safe: no FK check on child-row deletion)
3. Run the name and enrichment-field UPDATEs (safe: zero child rows = no FK violation)
4. Re-insert all backed-up rows
5. Drop the temporary backup tables

All of this runs within a single `BEGIN TRANSACTION` / `COMMIT` block for atomicity — if anything fails, `ROLLBACK` restores everything.

### 3.2 Child tables to empty

Only tables referencing `album(id)` or `track(id)` need emptying. The `artist` UPDATE in `backfill_names_inner` already has `WHERE name IS NULL OR name = id`, and bootstrap always populates artist names from Wikidata labels — this UPDATE is effectively a no-op (zero rows matched). However, for defense-in-depth, we handle it anyway.

The four child tables and their sizes:

| Table | FK constraint(s) | Typical row count |
|---|---|---|
| `album_artist` | `album_id → album(id)`, `artist_id → artist(id)` | ~646K |
| `track_artist` | `track_id → track(id)`, `artist_id → artist(id)` | ~62K |
| `album_genre` | `album_id → album(id)` | ~56K (from prior run; 0 on fresh populate) |
| `track_album` | `track_id → track(id)`, `album_id → album(id)` | ~4 (from prior run; 0 on fresh populate) |

The total child row count (~764K) fits comfortably in DuckDB temporary tables (disk-backed, not memory-bound).

### 3.3 Implementation design

Create a new `backfill_all_safe()` function that performs name and enrichment-field backfills within a single safe transaction. The enrichment backfill is folded in to avoid needing a separate FK-safe transaction later. The old `backfill_names()`, `backfill_names_inner()`, `backfill_enrichment_fields()`, and `backfill_enrichment_inner()` functions are removed entirely — git history preserves them for reference.

Uses `DELETE FROM` rather than `TRUNCATE` because `TRUNCATE` is not supported inside DuckDB transactions in all configurations. At ~764K child rows, `DELETE FROM` is fast enough (< 1s) — no batching needed.

```
backfill_all_safe(conn, parquet_dir):
    enrichment_path = parquet_dir / "enrichment.parquet"

    tracing::info!("Beginning FK-safe backfill")
    conn.execute("BEGIN TRANSACTION")

    // 1. Backup child tables
    tracing::debug!("Backing up child tables")
    conn.execute("CREATE TEMP TABLE _bak_album_artist AS SELECT * FROM album_artist")
    conn.execute("CREATE TEMP TABLE _bak_track_artist AS SELECT * FROM track_artist")
    conn.execute("CREATE TEMP TABLE _bak_album_genre  AS SELECT * FROM album_genre")
    conn.execute("CREATE TEMP TABLE _bak_track_album  AS SELECT * FROM track_album")

    // 2. Empty child tables
    // FK RESTRICT only prevents parent-row deletion — deleting child rows is always allowed.
    tracing::debug!("Emptying child tables")
    conn.execute("DELETE FROM track_album")
    conn.execute("DELETE FROM album_genre")
    conn.execute("DELETE FROM track_artist")
    conn.execute("DELETE FROM album_artist")

    // 3. Run all UPDATEs safely (zero child rows = no FK violations)
    tracing::debug!("Running name and enrichment UPDATEs")
    // 3a. Album name backfill
    conn.execute("UPDATE album SET name = COALESCE(...)")
    // 3b. Track name backfill
    conn.execute("UPDATE track SET name = COALESCE(...)")
    // 3c. Artist name backfill (likely a no-op; included for safety)
    conn.execute("UPDATE artist SET name = COALESCE(...) WHERE name IS NULL OR name = id")
    // 3d. Enrichment field backfills
    conn.execute("UPDATE album SET release_date = ... FROM read_parquet(...)")
    conn.execute("UPDATE album SET record_label = ... FROM read_parquet(...)")
    conn.execute("UPDATE track SET duration_seconds = ... FROM read_parquet(...)")

    // 4. Restore child tables
    tracing::debug!("Restoring child tables")
    conn.execute("INSERT INTO album_artist SELECT * FROM _bak_album_artist")
    conn.execute("INSERT INTO track_artist SELECT * FROM _bak_track_artist")
    conn.execute("INSERT INTO album_genre  SELECT * FROM _bak_album_genre")
    conn.execute("INSERT INTO track_album  SELECT * FROM _bak_track_album")

    // 5. Clean up temp tables
    conn.execute("DROP TABLE IF EXISTS _bak_album_artist")
    conn.execute("DROP TABLE IF EXISTS _bak_track_artist")
    conn.execute("DROP TABLE IF EXISTS _bak_album_genre")
    conn.execute("DROP TABLE IF EXISTS _bak_track_album")

    conn.execute("COMMIT")
    tracing::info!("FK-safe backfill committed successfully")
```

On error, `ROLLBACK` restores everything (child table data, temp tables, UPDATEs) to the pre-transaction state. The function returns the error, and callers see no partial state.

### 3.4 Why this is safe

| Concern | Analysis |
|---|---|
| Data loss during backup/restore | All within a transaction — any failure triggers ROLLBACK, restoring original state |
| FK integrity after restore | Child rows reference the same parent IDs as before; parent IDs never changed (only `name` columns were updated) |
| Large transaction size (~764K + 764K rows) | DuckDB is disk-backed; 1.5M row operations are well within its capability |
| Concurrent access | Single-user application, single connection — no concurrency risk |
| Artist FK tables (artist_genre, artist_instrument, artist_member_of) | Not emptied because the artist UPDATE has `WHERE name IS NULL OR name = id` and bootstrap always resolves artist names. Even if matched, a newly inserted artist with NULL name wouldn't have child rows yet |

### 3.5 Why the enrichment backfill is folded in

The current code runs `backfill_names` before `load_enrichment` and `backfill_enrichment_fields` after. With the new safe approach, `backfill_enrichment_fields` could theoretically remain separate (it uses `UPDATE ... FROM read_parquet(...)` which may or may not trigger FK enforcement — this was never tested with non-empty enrichment data and child rows). Rather than rely on an unverified assumption, folding both backfills into one safe transaction is simpler and provably correct.

After `backfill_all_safe()` completes, `load_enrichment()` populates `album_genre` and `track_album` as before (with FK guards from the prior fixes).

### 3.6 Updated pipeline order

| Step | Operation | Notes |
|---|---|---|
| 1 | `load_labels` | Populates `qid_label` (unchanged) |
| 2 | `backfill_all_safe` | **New**: names + enrichment fields in one FK-safe transaction |
| 3 | `load_enrichment` | Populates `album_genre`, `track_album`, `instrument`, `record_label` (unchanged) |
| 4 | `create_fts_indexes` | Rebuilds FTS indexes (unchanged) |

---

## 4. Data Safety Analysis

### 4.1 What FK enforcement protects during backfill

In the original code, FK enforcement on `UPDATE album` protects against... nothing useful. The `name` column is not a key. The enforcement here is a false positive — DuckDB treats any parent-row UPDATE as a potential row replacement.

### 4.2 Verification queries

After the fix, these invariants must hold:

```sql
-- No orphaned child rows (all album references are valid)
SELECT COUNT(*) FROM album_artist aa LEFT JOIN album a ON aa.album_id = a.id WHERE a.id IS NULL;
-- Expected: 0

SELECT COUNT(*) FROM track_artist ta LEFT JOIN track t ON ta.track_id = t.id WHERE t.id IS NULL;
-- Expected: 0

SELECT COUNT(*) FROM album_genre ag LEFT JOIN album a ON ag.album_id = a.id WHERE a.id IS NULL;
-- Expected: 0

SELECT COUNT(*) FROM track_album ta LEFT JOIN album a ON ta.album_id = a.id WHERE a.id IS NULL;
-- Expected: 0

-- Row counts unchanged after backfill
-- Compare COUNT(*) from child tables before and after: identical
```

### 4.3 Idempotency

The backfill UPDATEs use `COALESCE((SELECT label FROM qid_label WHERE qid = ...), name)` — if a name is already resolved, the COALESCE returns the existing value, making the UPDATE a no-op for that row. Combined with the atomic transaction, this means `backfill_all_safe` can be called any number of times with the same result.

The child-table backup/restore is also idempotent: if child tables are already empty (fresh populate, first attempt), the CREATE TEMP TABLE creates empty temp tables, DELETE deletes zero rows, and INSERT inserts zero rows.

---

## 5. Implementation Plan

### Step 1: Create `backfill_all_safe()` function + tests + remove dead code

**File:** `src/db/load.rs`

Replace the `backfill_names()`, `backfill_names_inner()`, `backfill_enrichment_fields()`, and `backfill_enrichment_inner()` functions with a single `backfill_all_safe()` function that:

1. Wraps everything in `BEGIN TRANSACTION` / `COMMIT` / `ROLLBACK` with per-step tracing
2. Creates four temp tables as `SELECT *` snapshots of the child tables
3. DELETEs all rows from the four child tables
4. Runs album, track, and artist name UPDATEs (inline, no separate `backfill_names_inner`)
5. Runs enrichment field UPDATEs (inline, using the enrichment Parquet path)
6. INSERTs all rows back from the temp tables
7. DROPs the temp tables

Remove the old functions entirely — git history preserves them for reference. Add the two tests described in §5 Step 1 (now merged into this step).

**Commit:** `fix(db): add FK-safe backfill with temp-table swap and remove dead backfill code`

### Step 2: Update `load_label_and_enrichment()` pipeline

**File:** `src/db/load.rs`

Update the orchestration to call `backfill_all_safe` instead of the old sequence:

```rust
pub fn load_label_and_enrichment(conn: &Connection, parquet_dir: &Path) -> Result<()> {
    let labels = load_labels(conn, parquet_dir)?;
    tracing::info!(labels, "Loaded labels");

    // Backfill names AND enrichment fields in a single FK-safe transaction.
    // Temporarily empties child tables, runs all UPDATEs, restores child tables.
    backfill_all_safe(conn, parquet_dir)?;
    tracing::info!("Backfill complete");

    let (instruments, record_labels, album_genres, track_albums) =
        load_enrichment(conn, parquet_dir)?;
    tracing::info!(...);

    crate::db::schema::create_fts_indexes(conn)?;

    Ok(())
}
```

Update the doc comment to reflect the new pipeline order and explain the child-table emptying approach.

**Commit:** `fix(db): integrate FK-safe backfill into populate pipeline`

### Step 3: Run full test suite

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

**Commit:** None (verification step)

### Step 4: Re-run populate

Since a previous run partially completed (labels loaded, enrichment tables partly populated), the database needs cleanup before the fix can work:

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

The `--resume` flag skips the expensive dump scan. The backfill will proceed in the FK-safe transaction. `load_enrichment` re-populates `album_genre`, `track_album`, `instrument`, and `record_label` with correct FK guards.

**Commit:** None (operational step)

---

## 6. Testing Strategy

### 6.1 Unit Tests (Step 1)

| Test | What it verifies |
|---|---|
| Test | What it verifies |
|---|---|
| `test_backfill_all_safe_with_child_rows` | Album and track name backfill succeeds with pre-existing rows in all four child tables. Enrichment field backfill succeeds with real Parquet data. Child table row counts and FK integrity are preserved. Both album-side and track-side child references survive. |
| `test_backfill_all_safe_rollback_on_error` | A mid-transaction failure triggers ROLLBACK. After rollback, all child tables have their original row counts and parent-table names are unchanged. |

### 6.2 Regression Tests

All existing tests must continue to pass. The old `test_backfill_with_active_fk_constraints` is removed (it tested the reordering approach, superseded by this fix). All tests now call `backfill_all_safe` instead of `backfill_names`. Tests that call `load_label_and_enrichment` end-to-end will work unchanged since the orchestration function is updated.

### 6.3 Production Verification

After the fix, run `populate` and verify:

1. `backfill_all_safe` completes without FK errors
2. `album.name`, `track.name` columns are populated with English labels
3. `album.release_date`, `album.record_label`, `track.duration_seconds` are populated
4. FK integrity queries (§4.2) return 0 violating rows
5. Child table row counts after populate match expected values
6. FTS indexes are functional

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Transaction too large for DuckDB (~764K rows × 2 operations) | Low | Medium | DuckDB is disk-backed and handles multi-million-row transactions. If performance is an issue, the backup/restore can be batched (unlikely needed at this scale). |
| `CREATE TEMP TABLE AS SELECT *` fails due to disk space | Low | Low | The temp tables are disk-backed in DuckDB's temp directory. 764K rows with small columns (TEXT IDs) is ~tens of MB. |
| Error during restore leaves partial data | None | — | Everything is within a single transaction. Any error triggers ROLLBACK, restoring the exact pre-transaction state. |
| Artist UPDATE matches rows (contrary to analysis) | Very Low | Medium | The WHERE clause `name IS NULL OR name = id` limits the blast radius. Even if matched, the affected artists would be newly inserted (no child rows yet), so no FK violation. |
| Concurrent connection modifies child tables during transaction | None (single-user app) | — | Single connection, no concurrency. |

### 7.1 Alternative considered: `INSERT OR REPLACE` rebuild

An alternative approach would be to use `INSERT OR REPLACE INTO album (id, name, ...) SELECT ...` to recreate rows with resolved names. This was rejected because DuckDB implements `OR REPLACE` as `DELETE` + `INSERT` internally, and the DELETE step triggers the same FK RESTRICT enforcement.

### 7.2 Alternative considered: separate connection

Opening a second DuckDB connection (from the same process) and configuring it differently to avoid FK enforcement. Rejected because the bundled DuckDB build doesn't support `PRAGMA foreign_keys` or any other FK-disabling mechanism, regardless of connection.

### 7.3 Rollback

Reverting to the previous behavior is a simple `git revert`. No schema changes, no data migration, no file format changes. The replaced functions (`backfill_names`, `backfill_enrichment_fields`) are removed from the codebase but git history preserves them for reference.

---

## 8. References

- DuckDB FK documentation: <https://duckdb.org/docs/sql/constraints.html#foreign-keys>
- DuckDB issue #15804 — FK check fires on non-PK updates
- Prior fix plan: [2026-08_fix_backfill_album_name_fk_violation.md](./2026-08_fix_backfill_album_name_fk_violation.md)
- Prior FK guard fixes: [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md), [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md)
- FK constraint definitions: [`src/db/schema.rs`](../../src/db/schema.rs)
- Current backfill code: [`src/db/load.rs`](../../src/db/load.rs), lines 67–230
- Populate orchestration: [`src/cli/populate.rs`](../../src/cli/populate.rs)
- Production row counts: `album_artist`=646106, `track_artist`=61699, `album_genre`=56068, `track_album`=4
