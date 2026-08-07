# Research: Fix FK Constraint Violation During artist.name Backfill

**Date:** 2026-08-06
**Status:** Plan
**References:**

- [2026-08_fix_backfill_fk_safe_emptying.md](./2026-08_fix_backfill_fk_safe_emptying.md) — prior fix (the one with the gap)
- [`src/db/load.rs`](../../src/db/load.rs) — `backfill_all_safe()`, `backfill_all_safe_inner()`
- [`src/db/schema.rs`](../../src/db/schema.rs) — FK constraint definitions
- [`src/cli/populate.rs`](../../src/cli/populate.rs) — populate subcommand orchestration

---

## 1. Problem Statement

The `populate` subcommand, using the `backfill_all_safe` function from the prior fix, fails with:

```
Error: Failed to load label and enrichment data

Caused by:
    0: Failed to backfill artist.name
    1: Constraint Error: Violates foreign key constraint because key "artist_id: Q11270725"
       is still referenced by a foreign key in a different table.
```

The failed SQL is:

```sql
UPDATE artist SET name = COALESCE(
    (SELECT label FROM qid_label WHERE qid = artist.id),
    artist.name
)
WHERE name IS NULL OR name = id
```

The prior fix's `backfill_all_safe` function backs up and empties only four child tables:

- `album_artist` — references `album(id)`, `artist(id)`
- `track_artist` — references `track(id)`, `artist(id)`
- `album_genre`  — references `album(id)`
- `track_album`  — references `track(id)`, `album(id)`

Three additional child tables with `REFERENCES artist(id)` are **not** emptied:

- `artist_genre`       — `REFERENCES artist(id)`
- `artist_instrument`  — `REFERENCES artist(id)`
- `artist_member_of`   — `REFERENCES artist(id)` × 2 (both `artist_id` and `group_id`)

When the `UPDATE artist` runs, DuckDB's FK RESTRICT enforcement fires on every matched `artist` row that has child references in any of these three tables.

### 1.1 Production Row Counts

At the time of failure (Phase 1 auto-committed, Phase 2 rolled back):

| Table | Rows | Emptied by `backfill_all_safe`? |
|---|---|---|
| `album_artist` | 0 | ✓ (data lost) |
| `track_artist` | 0 | ✓ (data lost) |
| `album_genre` | 0 | ✓ |
| `track_album` | 0 | ✓ |
| `artist_genre` | **2,455,645** | ✗ |
| `artist_instrument` | **229,938** | ✗ |
| `artist_member_of` | **41,059** | ✗ |

### 1.2 Why Artists Match the WHERE Clause

The artist UPDATE has `WHERE name IS NULL OR name = id`. In the production database after bootstrap, **577,504** of 2,659,668 artists have `name IS NULL`. These are artists that appear in junction tables (`album_artist`, `track_artist`, `artist_member_of`) but were never directly processed as their own Wikidata entity during extraction, so they received no `name` value in the Parquet files. `load_artists` inserts them with `name = NULL` because the Parquet `name` column is null for these rows.

This is a pre-existing data state: the artist UPDATE was always going to match ~500K rows. What changed is that the previous `backfill_names()` function was never able to reach the artist UPDATE (it always crashed on the album UPDATE first). The prior fix fixed album and track UPDATEs but did not fix the artist UPDATE.

---

## 2. Root Cause

### 2.1 The Gap in the Prior Fix

The research plan for the prior fix (§3.2 of `2026-08_fix_backfill_fk_safe_emptying.md`) states:

> *"Only tables referencing album(id) or track(id) need emptying. The artist UPDATE already has `WHERE name IS NULL OR name = id`, and bootstrap always populates artist names from Wikidata labels — this UPDATE is effectively a no-op (zero rows matched). However, for defense-in-depth, we handle it anyway."*

And §3.4:

> *"Even if matched, a newly inserted artist with NULL name wouldn't have child rows yet."*

**Both assumptions were wrong:**

1. **Bootstrap does NOT always populate artist names.** 577,504 artists have `name IS NULL` after bootstrap. These artists appear in junction tables via their relationships to other entities but were never directly processed as standalone Wikidata entities during extraction.

2. **NULL-name artists DO have child rows.** Artists with NULL names were inserted during `load_artists` (step 2 of bootstrap), and their `artist_genre`, `artist_instrument`, and `artist_member_of` relationships were populated in steps 3–5 of bootstrap — all before the `artist.name` backfill ever runs. At the time of the artist UPDATE, these three tables contain 2.7M rows of FK references.

### 2.2 All Six FK-Referencing Artist Child Tables

From `schema.rs`:

```sql
-- album_artist: album_id → album(id), artist_id → artist(id)
-- track_artist:  track_id → track(id),  artist_id → artist(id)
-- album_genre:   album_id → album(id)  -- does NOT reference artist
-- track_album:   track_id → track(id), album_id → album(id)  -- does NOT reference artist
-- artist_genre:  artist_id → artist(id), genre_id → genre(id)
-- artist_instrument: artist_id → artist(id)  (instrument_id has no FK constraint)
-- artist_member_of:  artist_id → artist(id), group_id → artist(id)
```

For the `UPDATE album` statement, tables referencing `album(id)` must be empty: `album_artist`, `album_genre`, `track_album`. ✓ handled.

For the `UPDATE track` statement, tables referencing `track(id)` must be empty: `track_artist`, `track_album`. ✓ handled.

For the `UPDATE artist` statement, tables referencing `artist(id)` must be empty: `album_artist`, `track_artist`, `artist_genre`, `artist_instrument`, `artist_member_of`. ✗ the last three were missed.

### 2.3 Data Safety of the Gap

The prior fix's Phase 1 runs **outside** any transaction (auto-committed). This means:

- The four DELETEd child tables (`album_artist`: 646K rows, `track_artist`: 62K rows, `album_genre`: 0 rows, `track_album`: 0 rows) are **permanently lost**. Rows are gone.
- The four corresponding temp tables (`_bak_*`) were created in the DuckDB session. Since the process exited, the session is closed, the temp tables are gone, and the backup data is unrecoverable.

The artist child tables (`artist_genre`, `artist_instrument`, `artist_member_of`) are intact — they were never touched.

---

## 3. Proposed Solution

### 3.1 Extend `backfill_all_safe` to Cover All Six Child Tables

Add `artist_genre`, `artist_instrument`, and `artist_member_of` to the backup/delete/restore/drop cycle.

The new pipeline inside `backfill_all_safe` + `backfill_all_safe_inner`:

**Phase 1 (auto-committed):**

```
CREATE TEMP TABLE _bak_album_artist    AS SELECT * FROM album_artist
CREATE TEMP TABLE _bak_track_artist    AS SELECT * FROM track_artist
CREATE TEMP TABLE _bak_album_genre     AS SELECT * FROM album_genre
CREATE TEMP TABLE _bak_track_album     AS SELECT * FROM track_album
CREATE TEMP TABLE _bak_artist_genre      AS SELECT * FROM artist_genre
CREATE TEMP TABLE _bak_artist_instrument AS SELECT * FROM artist_instrument
CREATE TEMP TABLE _bak_artist_member_of  AS SELECT * FROM artist_member_of

DELETE FROM track_album
DELETE FROM album_genre
DELETE FROM track_artist
DELETE FROM album_artist
DELETE FROM artist_member_of    -- ← NEW
DELETE FROM artist_instrument   -- ← NEW
DELETE FROM artist_genre        -- ← NEW
```

**Phase 2 (inside transaction):**

```
BEGIN TRANSACTION

-- Name UPDATEs (unchanged)
UPDATE album SET name = COALESCE(...)
UPDATE track SET name = COALESCE(...)
UPDATE artist SET name = COALESCE(...) WHERE name IS NULL OR name = id

-- Enrichment UPDATEs (unchanged)
UPDATE album SET release_date = ...
UPDATE album SET record_label = ...
UPDATE track SET duration_seconds = ...

-- Restore all child tables
INSERT INTO album_artist    SELECT * FROM _bak_album_artist
INSERT INTO track_artist    SELECT * FROM _bak_track_artist
INSERT INTO album_genre     SELECT * FROM _bak_album_genre
INSERT INTO track_album     SELECT * FROM _bak_track_album
INSERT INTO artist_genre      SELECT * FROM _bak_artist_genre      -- ← NEW
INSERT INTO artist_instrument SELECT * FROM _bak_artist_instrument -- ← NEW
INSERT INTO artist_member_of  SELECT * FROM _bak_artist_member_of  -- ← NEW

-- Drop temp tables
DROP TABLE IF EXISTS _bak_album_artist
DROP TABLE IF EXISTS _bak_track_artist
DROP TABLE IF EXISTS _bak_album_genre
DROP TABLE IF EXISTS _bak_track_album
DROP TABLE IF EXISTS _bak_artist_genre      -- ← NEW
DROP TABLE IF EXISTS _bak_artist_instrument -- ← NEW
DROP TABLE IF EXISTS _bak_artist_member_of  -- ← NEW

COMMIT
```

On error: `ROLLBACK`, temp tables remain for recovery.

### 3.2 Updated Row Sizes

With the three new tables, the total row churn:

| Table | Rows |
|---|---|
| `artist_genre` | ~2.46M |
| `artist_instrument` | ~230K |
| `artist_member_of` | ~41K |
| `album_artist` | ~646K |
| `track_artist` | ~62K |
| `album_genre` | ~56K |
| `track_album` | ~4 |
| **Total** | **~3.5M** |

> **Note on `album_genre`:** On the first `populate` attempt, `album_genre` is 0 rows
> because it is populated by `load_enrichment` *after* `backfill_all_safe` runs. On
> subsequent `populate --resume` runs (where `load_enrichment` already populated it),
> the ~56K rows will be backed up and restored.

3.5M rows is well within DuckDB's capability for a single transaction. Each row has 2–3 TEXT columns (~tens of bytes each), making the total temp-table footprint ~100–200 MB — easily handled by DuckDB's disk-backed temp storage.

### 3.3 Phase 1 Must Run Inside the Transaction (Design Change)

The current implementation runs Phase 1 **outside** a transaction (auto-committed). This was an intentional design decision with this rationale:

> *"These run outside any explicit transaction (auto-committed) so that DuckDB's FK constraint check on subsequent UPDATEs sees the committed state of zero child rows."*

However, this design is **dangerous** — if the process crashes between Phase 1 and Phase 2, or if Phase 2's `ROLLBACK` leaves Phase 1's auto-committed state intact, child-table data is permanently lost. This already happened: `album_artist` (646K rows) and `track_artist` (62K rows) are gone from the production database.

DuckDB's FK RESTRICT enforcement operates at the statement level within a transaction. A DELETE inside a transaction immediately removes the rows from the FK-check perspective for subsequent statements **within the same transaction**. The auto-commit of Phase 1 is unnecessary.

**DELETE order**: The order of DELETEs is arbitrary — DuckDB's FK RESTRICT only fires on parent-table mutations (UPDATE or DELETE on the referenced table). DELETEs against child tables never trigger FK enforcement, so ordering the seven DELETEs in any sequence is safe.

**New design**: Move everything into a single transaction. The CREATE TEMP TABLE statements remain outside the transaction (temp tables are session-scoped, not transaction-scoped), but the DELETEs, UPDATEs, restores, and drops all happen inside `BEGIN TRANSACTION … COMMIT`.

**Partial temp-table creation**: If a `CREATE TEMP TABLE` statement fails partway through (e.g., `_bak_artist_genre` succeeds but `_bak_artist_instrument` fails), the function returns with some but not all temp tables created. No data has been deleted yet (DELETEs come later, inside the transaction), so this is a benign state — nothing is lost and the operation is retryable. The `IF NOT EXISTS` clause (see §3.4) ensures that on retry within the same connection, already-created temp tables are reused rather than re-created.

```
backfill_all_safe(conn, parquet_dir):
    // Temp tables: outside transaction (session-scoped)
    CREATE TEMP TABLE _bak_album_artist AS SELECT * FROM album_artist
    CREATE TEMP TABLE _bak_track_artist AS SELECT * FROM track_artist
    CREATE TEMP TABLE _bak_album_genre  AS SELECT * FROM album_genre
    CREATE TEMP TABLE _bak_track_album  AS SELECT * FROM track_album
    CREATE TEMP TABLE _bak_artist_genre      AS SELECT * FROM artist_genre
    CREATE TEMP TABLE _bak_artist_instrument AS SELECT * FROM artist_instrument
    CREATE TEMP TABLE _bak_artist_member_of  AS SELECT * FROM artist_member_of

    // Everything else: inside transaction
    BEGIN TRANSACTION
    DELETE FROM all seven child tables
    run UPDATEs
    INSERT ... SELECT * FROM all seven backup temp tables
    DROP TABLE all seven backup temp tables
    COMMIT

    on error: ROLLBACK
```

### 3.4 Why Moving Phase 1 Inside the Transaction Works

DuckDB's FK RESTRICT behavior is a **statement-level** check within a transaction. When a `DELETE FROM album_artist` runs inside a transaction, the rows are immediately removed from the table for the purposes of subsequent statements in the same transaction. When the subsequent `UPDATE album SET name = ...` runs, DuckDB sees zero child rows in `album_artist` and allows the UPDATE.

This is verified by DuckDB's documented behavior: DML statements within a transaction see the effects of prior DML statements in the same transaction context. FK checks reflect the committed-or-in-transaction state.

The only reason Phase 1 was outside the transaction before was an abundance of caution — but it was incorrect caution, and the resulting data loss proves it was the wrong choice.

**`IF NOT EXISTS` on temp tables**: The current code uses `CREATE TEMP TABLE IF NOT EXISTS` for all backup tables. After the design change, temp tables are created outside the transaction and survive both `COMMIT` and `ROLLBACK` (they are session-scoped). If `backfill_all_safe` is retried on the same connection after a rollback, `IF NOT EXISTS` skips recreation and the temp tables from the first attempt are reused. This is correct because the data in those temp tables hasn't changed between attempts and the DELETEs were rolled back.

### 3.5 Test Updates

**Existing tests to update:**

- **`test_backfill_all_safe_with_child_rows`** — Add rows to `artist_genre`, `artist_instrument`, and `artist_member_of`. Verify they are preserved after backfill. This test currently only exercises the four album/track child tables.

- **`test_backfill_all_safe_rollback_on_error`** — After the design change to move Phase 1 inside the transaction, this test must assert that child-table rows are **preserved** after rollback (not emptied with only the backup temp table surviving). Currently the test accepts empty child tables as expected behavior, which was wrong.

**New test:**

- **`test_backfill_all_safe_rollback_preserves_artist_tables`** — Verifies that a rollback preserves `artist_genre`, `artist_instrument`, and `artist_member_of` rows. This is the explicit test for the gap this fix addresses.

---

## 4. Data Recovery

### 4.1 What Was Lost

The two failed populate runs' Phase 1 auto-committed DELETEs permanently removed:

| Table | Rows lost |
|---|---|
| `album_artist` | ~646,106 |
| `track_artist` | ~61,699 |

`album_genre` and `track_album` were already empty (they get populated during `load_enrichment`, which hadn't run yet on these attempts).

### 4.2 Recovery Procedure

The lost rows can only be recovered by re-running bootstrap:

```bash
# Reset the database entirely (re-bootstrap from scratch)
rm /home/tr/wiki_db/music.duckdb
cargo run --release -- bootstrap \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz

# Then run populate (without --resume the first time after code fix)
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz
```

This is expensive (must re-scan the 35 GB dump), but it is the only way to recover the lost `album_artist` and `track_artist` rows. The Parquet files from the prior extraction can be deleted to force fresh extraction:

```bash
rm -f parquet-dir/labels.parquet parquet-dir/enrichment.parquet
```

### 4.3 State of Artist Child Tables

The artist child tables (`artist_genre`, `artist_instrument`, `artist_member_of`) are intact in the current database. The `artist` table has 577,504 rows with `name IS NULL` awaiting backfill. These tables do NOT need recovery — only the lost `album_artist` and `track_artist` rows require re-bootstrap.

---

## 5. Implementation Plan

### Step 0 — Pre-work

**Branch:** `agent/fix-artist-backfill-fk-violation`. Verify workspace clean:

```bash
git status
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

### Step 1 — `fix(db): add artist child tables to FK-safe backfill and fix transaction boundary`

**File:** `src/db/load.rs`

1. **Add three new temp tables to `backfill_all_safe`:**

   Add `_bak_artist_genre`, `_bak_artist_instrument`, `_bak_artist_member_of` to the CREATE TEMP TABLE block. Add corresponding DELETEs.

2. **Move Phase 1 DELETEs inside the transaction:**

   Move the seven DELETE statements to inside `BEGIN TRANSACTION` (just after it, before `backfill_all_safe_inner` is called). The CREATE TEMP TABLE statements stay outside the transaction (temp tables are session-scoped).

3. **Add three new restores and drops to `backfill_all_safe_inner`:**

   Add `INSERT INTO artist_genre SELECT * FROM _bak_artist_genre` (and the other two) to the restore block. Add corresponding DROP TABLE statements.

4. **Update doc comments** on both functions to accurately describe the new transaction boundary and the full set of seven child tables.

5. **Run the existing test suite** — all existing tests must pass. The existing `test_backfill_all_safe_with_child_rows` and `test_backfill_all_safe_rollback_on_error` may fail or need updating (see Step 2).

**Commit:** `fix(db): add artist child tables to FK-safe backfill and fix transaction boundary`

### Step 2 — `test(db): add coverage for artist child tables in backfill and rollback`

**File:** `src/db/load.rs` (tests module)

1. **Update `test_backfill_all_safe_with_child_rows`:**

   Add pre-seeded rows for `artist_genre`, `artist_instrument`, `artist_member_of`. For `artist_member_of`, include a row where both `artist_id` and `group_id` reference valid artists (exercises the self-referencing FK edge case). Assert row counts are preserved after backfill. Add FK integrity checks for all seven child tables.

2. **Update `test_backfill_all_safe_rollback_on_error`:**

   After moving Phase 1 inside the transaction, a rollback should preserve child-table rows. Remove the assertion that child tables are empty. Add assertions that `album_artist` rows are intact. Add similar assertions for artist child tables.

3. **Add `test_backfill_all_safe_rollback_preserves_artist_tables`:**

   A focused test: pre-seed `artist_genre`, `artist_instrument`, and `artist_member_of` with rows, trigger a rollback (e.g., missing `enrichment.parquet`), assert all row counts are preserved and FK integrity holds.

**Commit:** `test(db): add coverage for artist child tables in backfill and rollback`

### Step 3 — Verify

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

Confirm all tests pass, including:

- `test_backfill_all_safe_with_child_rows` — now asserts all 7 child tables preserved
- `test_backfill_all_safe_rollback_on_error` — now asserts child rows preserved after rollback
- `test_backfill_all_safe_rollback_preserves_artist_tables` (new)

### Post-implementation: Operational recovery

The database has permanently lost `album_artist` and `track_artist` rows. A full re-bootstrap is required:

```bash
# Delete old database and stale Parquet files
rm /home/tr/wiki_db/music.duckdb
rm -f parquet-dir/labels.parquet parquet-dir/enrichment.parquet

# Re-bootstrap (expensive: re-scans the 35 GB dump)
cargo run --release -- bootstrap \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz

# Run populate
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz
```

Expected outcome: `backfill_all_safe` completes without FK errors. All 577,504 NULL-name artists have their names resolved. All seven child tables have correct row counts and FK integrity holds.

---

## 6. Testing Strategy

### 6.1 Unit Tests

| Test | What it verifies |
|---|---|
| `test_backfill_all_safe_with_child_rows` (updated) | Seven child tables (not four) have rows preserved and FK integrity maintained after backfill. Includes `artist_member_of` rows with dual `artist(id)` FK references to exercise the self-referencing edge case. |
| `test_backfill_all_safe_rollback_on_error` (updated) | After rollback, child-table rows are preserved (not lost) and parent-table names are unchanged |
| `test_backfill_all_safe_rollback_preserves_artist_tables` (new) | Specifically validates `artist_genre`, `artist_instrument`, and `artist_member_of` rows survive rollback — the gap from the prior fix |

### 6.2 Regression Tests

All existing tests must pass. The design change from auto-committed Phase 1 to fully-transactional may affect tests that depended on the old behavior (specifically `test_backfill_all_safe_rollback_on_error`, which expects empty child tables after rollback — this assertion is being changed to expect preserved rows).

### 6.3 Production Verification

After the fix and re-bootstrap:

```sql
-- Verify no orphaned FK references in any child table
SELECT 'album_artist' AS tbl, COUNT(*) FROM album_artist aa
  LEFT JOIN album a ON aa.album_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'album_artist (artist)', COUNT(*) FROM album_artist aa
  LEFT JOIN artist a ON aa.artist_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'track_artist (track)', COUNT(*) FROM track_artist ta
  LEFT JOIN track t ON ta.track_id = t.id WHERE t.id IS NULL
UNION ALL
SELECT 'track_artist (artist)', COUNT(*) FROM track_artist ta
  LEFT JOIN artist a ON ta.artist_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'album_genre', COUNT(*) FROM album_genre ag
  LEFT JOIN album a ON ag.album_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'track_album', COUNT(*) FROM track_album ta
  LEFT JOIN album a ON ta.album_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'artist_genre', COUNT(*) FROM artist_genre ag
  LEFT JOIN artist a ON ag.artist_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'artist_instrument', COUNT(*) FROM artist_instrument ai
  LEFT JOIN artist a ON ai.artist_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'artist_member_of (artist)', COUNT(*) FROM artist_member_of amo
  LEFT JOIN artist a ON amo.artist_id = a.id WHERE a.id IS NULL
UNION ALL
SELECT 'artist_member_of (group)', COUNT(*) FROM artist_member_of amo
  LEFT JOIN artist a ON amo.group_id = a.id WHERE a.id IS NULL;
-- All must return 0

-- Verify NULL-name artists were resolved
SELECT COUNT(*) FROM artist WHERE name IS NULL;
-- Expected: 0

-- Verify child-table row counts match pre-backfill expectations
SELECT 'artist_genre', COUNT(*) FROM artist_genre
UNION ALL SELECT 'artist_instrument', COUNT(*) FROM artist_instrument
UNION ALL SELECT 'artist_member_of', COUNT(*) FROM artist_member_of;
```

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Moving DELETEs inside transaction still triggers FK RESTRICT on UPDATEs | Very Low | High | DuckDB's statement-level FK enforcement sees in-transaction DELETEs. This is verified by the test suite. If DuckDB upgrades change this behavior, the tests will catch it. |
| 3.5M-row transaction exceeds DuckDB limits | Low | Low | DuckDB is disk-backed; 3.5M rows of short TEXT columns is ~150 MB — well within limits. |
| Re-bootstrap takes hours | High | Medium | Bootstrap re-parses the 35 GB Wikidata dump. This is the cost of the data loss from the prior fix. Future runs with the transactional design won't lose data on failure. |
| `artist_member_of` self-referencing FK issues | Very Low | Medium | `artist_member_of` has two FK references to `artist(id)`: `artist_id` and `group_id`. DuckDB handles this correctly — both references are to the same table but different columns. The DELETE and INSERT handle both naturally. This edge case is covered by the updated `test_backfill_all_safe_with_child_rows`. |
| `artist_genre` has FK to `genre(id)` | None | — | `artist_genre` references both `artist(id)` and `genre(id)`. The `genre` table is never updated during backfill, so only the `artist` FK side needs emptying. No action needed for the `genre` FK. |
| `artist_instrument` has FK to `artist(id)` only | None | — | The `instrument_id` column in `artist_instrument` has no FK constraint (it's a plain `TEXT NOT NULL`). Only the `artist_id → artist(id)` FK side needs emptying. |

### 7.1 Alternative Considered: Partial Fix (Only Artist Tables)

Adding only the three artist child tables to the backup/delete/restore cycle, without moving Phase 1 inside the transaction. Rejected because it perpetuates the dangerous auto-commit design. Any future failure between Phase 1 and Phase 2 would lose data in all seven tables (3.5M rows), not just the four we already lost.

### 7.2 Rollback

Reverting the code changes restores the prior (buggy) behavior. The data loss in `album_artist` and `track_artist` still requires re-bootstrap regardless.

---

## 8. References

- Prior fix (the one with the gap): [2026-08_fix_backfill_fk_safe_emptying.md](./2026-08_fix_backfill_fk_safe_emptying.md)
- Original FK violation diagnosis: [2026-08_fix_backfill_album_name_fk_violation.md](./2026-08_fix_backfill_album_name_fk_violation.md)
- Enrichment FK guards: [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md), [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md)
- Current backfill code: [`src/db/load.rs`](../../src/db/load.rs), `backfill_all_safe()` (line 84), `backfill_all_safe_inner()` (line 161)
- FK constraint definitions: [`src/db/schema.rs`](../../src/db/schema.rs), CREATE_TABLE_STATEMENTS
- Bootstrap orchestration: [`src/db/load.rs`](../../src/db/load.rs), `load_all()` (line 34)
- Populate orchestration: [`src/cli/populate.rs`](../../src/cli/populate.rs)
- DuckDB FK documentation: <https://duckdb.org/docs/sql/constraints.html#foreign-keys>
- DuckDB issue #15804 — FK check fires on non-PK updates
