# Research: Fix Missing FK Guards for album_genre.album_id and track_album.track_id

**Date:** 2026-08-06
**Status:** Plan
**References:**

- [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md) — prior fix (applied; only addressed genre_id and parent_album_id)
- [`src/db/load.rs`](../../src/db/load.rs) — `load_enrichment()` contains the remaining gap
- [`src/db/schema.rs`](../../src/db/schema.rs) — FK constraint definitions
- [`src/cli/populate.rs`](../../src/cli/populate.rs) — resume mode logic

---

## 1. Problem Statement

The `populate` subcommand with `--resume` fails with a foreign key constraint violation when loading enrichment data into `album_genre`:

```
Error: Failed to load label and enrichment data
Caused by:
    0: Failed to load album_genre from enrichment
    1: Constraint Error: Violates foreign key constraint because key "id: Q20511568" does not exist in the referenced table
    2: Error code 1: Unknown error code
```

The `album_genre` table has two foreign keys:

- `album_genre.album_id → album(id)` ← **this is the one failing**
- `album_genre.genre_id → genre(id)` ← **already guarded (prior fix applied)**

The prior fix ([2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md)) added FK guards for `genre_id` in `album_genre` and `parent_album_qid` in `track_album`. Those guards were applied successfully. However, the prior fix **missed two FK guards**:

1. `album_genre.album_id → album(id)` — **NOT guarded**
2. `track_album.track_id → track(id)` — **NOT guarded**

The immediate trigger is the `--resume` flag reusing stale `enrichment.parquet` files from a previous populate run on a different (or rebuilt) database, where the `album` table contains different Q-IDs. The album Q-ID `Q20511568` exists in the stale enrichment.parquet but not in the current database's `album` table.

Even without resume mode, the missing FK guards represent a correctness gap: any future edge case where an album or track Q-ID enters enrichment.parquet without a corresponding row in the referenced table would trigger the same error.

---

## 2. Root Cause

### 2.1 Missing FK Guards in `load_enrichment()`

The current state of `load_enrichment()` in `src/db/load.rs`:

**album_genre SQL** (line ~187):

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
  AND e.genre_qid IN (SELECT id FROM genre)   -- ✅ genre FK guarded
  -- ❌ album FK NOT guarded: entity_qid is never checked against album(id)
```

**track_album SQL** (line ~201):

```sql
INSERT OR IGNORE INTO track_album (track_id, album_id)
SELECT DISTINCT e.entity_qid, e.parent_album_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'track'
  AND e.parent_album_qid IS NOT NULL
  AND e.parent_album_qid IN (SELECT id FROM album)  -- ✅ album FK guarded
  -- ❌ track FK NOT guarded: entity_qid is never checked against track(id)
```

### 2.2 Why `INSERT OR IGNORE` Doesn't Help

DuckDB's `INSERT OR IGNORE` only suppresses primary-key/unique constraint violations. Foreign-key violations raise a hard `Constraint Error` regardless of `OR IGNORE`. This is confirmed by both the current production failure and the prior fix document (§3.3 Note).

### 2.3 Why Resume Mode Triggers It

The `--resume` flag (in `src/cli/populate.rs`) skips the expensive dump scan when `labels.parquet` and `enrichment.parquet` already exist:

```rust
let skip_extraction = args.resume && labels_path.exists() && enrichment_path.exists();
```

It does **not** validate that the Parquet files match the current database state. If the database was rebuilt (bootstrap re-run, or a different dump version was used), the `album` table will contain different Q-IDs than those in the stale `enrichment.parquet`.

Without the `--resume` flag, the extraction step queries `SELECT DISTINCT id FROM album` and only generates enrichment rows for albums that exist at that moment — so the FK guard would not be needed in fresh runs. But resume mode bypasses this consistency guarantee.

### 2.4 Comparison with Existing Correct Patterns

The codebase already has correct FK guard patterns in `load_artist_genre()` and `load_artist_member_of()`:

```sql
-- load_artist_genre (line 425): guards BOTH sides
INSERT OR IGNORE INTO artist_genre (artist_id, genre_id)
SELECT id, genre_qid FROM (
    SELECT a.id, unnest(string_split(a.genres, '|')) as genre_qid
    FROM read_parquet('...') a
) sq
WHERE sq.genre_qid IN (SELECT id FROM genre)   -- guards genre FK
-- (artist_id from a.id is known to exist since a comes from the artist parquet)
```

The enrichment loader should follow the same convention: guard every FK column with an `IN (SELECT id FROM ...)` subquery.

---

## 3. Proposed Solution

### 3.1 Add Album FK Guard to album_genre

Add `AND e.entity_qid IN (SELECT id FROM album)`:

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
  AND e.genre_qid IN (SELECT id FROM genre)
  AND e.entity_qid IN (SELECT id FROM album)   -- ← ADD: album FK guard
```

### 3.2 Add Track FK Guard to track_album

Add `AND e.entity_qid IN (SELECT id FROM track)`:

```sql
INSERT OR IGNORE INTO track_album (track_id, album_id)
SELECT DISTINCT e.entity_qid, e.parent_album_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'track'
  AND e.parent_album_qid IS NOT NULL
  AND e.parent_album_qid IN (SELECT id FROM album)
  AND e.entity_qid IN (SELECT id FROM track)   -- ← ADD: track FK guard
```

### 3.3 Discarded Alternative: Resume-Mode Validation

Adding a validation step to `--resume` that compares the Parquet contents against the current database state (e.g., checking that all `entity_qid` values in enrichment.parquet exist in `album`/`track`) was considered but rejected for this fix:

- **Complexity**: Extracting and comparing Q-ID sets from Parquet requires deserializing the file; the current resume mode is intentionally a simple existence check.
- **The FK guards are sufficient**: With both guards in place, resume mode using stale parquet files will silently drop FK-violating rows instead of crashing. This is acceptable behavior for a resume optimization.
- **User-facing**: A user who deletes and rebuilds the database should delete the Parquet files too (or not use `--resume`). This is a reasonable operational expectation.

A future enhancement could add a `--resume` validation check, but it's out of scope for this fix.

**Observability note:** The FK guards silently drop violating rows without logging. For operational visibility, consider adding `tracing::warn!` logs or a post-load row-count comparison to surface how many rows were dropped. This is deferred to avoid scope creep — the existing guards in `load_artist_genre()` and `load_artist_member_of()` follow the same silent-drop convention.

---

## 4. Data Impact Analysis

### 4.1 Rows Dropped by New Guards (estimate from prior fix document)

| Guard | Rows dropped | % of total |
|---|---|---|
| Album FK (album_genre) | Same as genre FK: ~6 rows (0.01%) | Negligible |
| Track FK (track_album) | ~0 (tracks always have stub rows) | Negligible |

For the resume-mode staleness scenario, more rows would be dropped — but this is an expected consequence of using stale enrichment data against a rebuilt database. The FK guard ensures the load completes instead of crashing, which is the correct behavior.

### 4.2 Downstream Impact

No change. The `backfill_names()` function (called after `load_enrichment()`) does not use `album_genre` or `track_album` — it reads from `enrichment.parquet` directly and joins against `qid_label`. The FK-guarded junction tables are independent of the backfill step.

---

## 5. Implementation Plan

### Step 1: Add FK guards and unit tests

**File:** `src/db/load.rs`, `load_enrichment()` function

Add both FK guards:

- `AND e.entity_qid IN (SELECT id FROM album)` to the `album_genre_sql` WHERE clause.
- `AND e.entity_qid IN (SELECT id FROM track)` to the `track_album_sql` WHERE clause.

Reuse the existing `write_test_enrichment_parquet` helper (added during the prior fix). Add two new tests:

1. **`test_load_enrichment_album_genre_album_id_fk_guard`** — Write an enrichment Parquet with two album rows: one where `entity_qid` exists in `album` (with a valid `genre_qid` in `genre`), and one where `entity_qid` does NOT exist in `album`. Pre-populate both `album` and `genre` tables. Verify `album_genre` contains exactly 1 row (the valid one).

2. **`test_load_enrichment_track_album_track_id_fk_guard`** — Write an enrichment Parquet with two track rows: one where `entity_qid` exists in `track` (with a valid `parent_album_qid` in `album`), and one where `entity_qid` does NOT exist in `track`. Pre-populate both `track` and `album` tables. Verify `track_album` contains exactly 1 row (the valid one).

**Commit:** `fix(db): add album and track FK guards to enrichment load with tests`

### Step 2: Add integration test for enrichment FK guards

**File:** `tests/bootstrap_test.rs` (or a new `tests/enrichment_test.rs`)

Add an integration test that calls `load_label_and_enrichment` end-to-end with a complete set of Parquet files including an `enrichment.parquet` containing FK-violating rows. This verifies the full enrichment pipeline handles guard-dropped rows without crashing and produces correct counts in all junction tables.

**Commit:** `test(db): add integration test for enrichment FK guards`

### Step 3: Verify with full test suite

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

**Commit:** None (verification step)

### Step 4: Re-run populate

Choose the appropriate path based on your database state:

- **If the database was rebuilt** (bootstrap re-run, different dump version): delete stale Parquet files so extraction runs fresh against the current database state:

    ```bash
    rm -f parquet-dir/labels.parquet parquet-dir/enrichment.parquet
    cargo run --release -- populate \
      --db /home/tr/wiki_db/music.duckdb \
      --dump /home/tr/wiki_db/latest-all.json.gz
    ```

- **If only the populate step failed** (same database, just a constraint error on the first populate try): re-run with `--resume`. The FK guards will silently drop violators instead of crashing:

    ```bash
    cargo run --release -- populate \
      --db /home/tr/wiki_db/music.duckdb \
      --dump /home/tr/wiki_db/latest-all.json.gz \
      --resume
    ```

**Commit:** None (operational step)

---

## 6. Testing Strategy

### 6.1 Unit Tests (Step 1)

| Test | What it verifies |
|---|---|
| `test_load_enrichment_album_genre_album_id_fk_guard` | Album with missing `album` table row is silently dropped; valid row is inserted |
| `test_load_enrichment_track_album_track_id_fk_guard` | Track with missing `track` table row is silently dropped; valid row is inserted |

### 6.2 Regression Tests

Run the full `cargo test` suite. All existing tests must pass without modification. The fix is purely additive — two new WHERE conditions that match the established codebase pattern.

### 6.3 Production Verification

After the fix, run `populate` (with or without `--resume`):

1. The command completes without FK errors
2. `album_genre` has valid rows (any FK-violating rows silently dropped)
3. `track_album` has valid rows (any FK-violating rows silently dropped)
4. `album.name`, `track.name`, `artist.name` are populated
5. Backfill steps complete successfully
6. FTS indexes are rebuilt

---

## 7. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| DuckDB IN subquery performance regression | Low | Low | The `album` table has ~109K rows; `IN (SELECT id FROM album)` is a semi-join that DuckDB optimizes efficiently |
| Future stale resume still drops valid data | Low | Medium | The FK guard silently drops violators — if resume mode is used correctly (same database state), zero rows are dropped. If used incorrectly (different database), the load completes with partial data instead of crashing, which is the preferable outcome |
| `track` FK guard drops valid tracks | Negligible | Low | Tracks are always stub-inserted during bootstrap from artist Parquet files, so any track in enrichment.parquet should have a corresponding row in `track` |

### 7.1 Rollback

Reverting the two SQL changes restores original behavior. No schema changes, no data migration, no file format changes.

---

## 8. References

- Prior FK fix (applied): [2026-08_fix_enrichment_fk_constraints.md](./2026-08_fix_enrichment_fk_constraints.md)
- Correct FK guard pattern: [`src/db/load.rs`](../../src/db/load.rs), `load_artist_genre()`, line 430
- FK constraint definitions: [`src/db/schema.rs`](../../src/db/schema.rs), `album_genre` and `track_album` DDL
- Resume mode logic: [`src/cli/populate.rs`](../../src/cli/populate.rs), line 175
- Current production error: Q20511568 missing from `album` table during `populate --resume`
