# Research: Fix Foreign Key Constraint Violations in Enrichment Loading

**Date:** 2026-08-06
**Status:** Implemented
**Implemented:** 2026-08 — closes with commits `0f67175` (album_genre FK guard) + `34207d4` (track_album FK guard) — see git log.

*Status updated per the [documentation conventions in AGENTS.md](../../AGENTS.md) — implemented plans carry `Status: Implemented` with their closing commits.*
**References:**

- [2026-08_populate_names_research.md](./2026-08_populate_names_research.md) — original populate design & implementation
- [ARCHITECTURE.md](../ARCHITECTURE.md) — system design & module map
- [`src/db/load.rs`](../../src/db/load.rs) — `load_enrichment()` contains the bug
- [`src/db/schema.rs`](../../src/db/schema.rs) — FK constraint definitions

---

## 1. Problem Statement

The `populate` subcommand (`src/cli/populate.rs` → `src/db/load.rs` → `load_enrichment()`) fails with a foreign key constraint violation when loading enrichment data from the intermediate Parquet file into the `album_genre` and `track_album` junction tables. The error manifests as:

```
Error: Failed to load label and enrichment data

Caused by:
    0: Failed to load album_genre from enrichment
    1: Constraint Error: Violates foreign key constraint because key "id: Q56410100" does not exist in the referenced table
    2: Error code 1: Unknown error code
```

The `album_genre` table has two foreign keys:

- `album_genre.album_id → album(id)`
- `album_genre.genre_id → genre(id)`

The `genre_id` side is the one that fails: genre Q-IDs extracted from album P136 (genre) claims may reference genres that are not present in the `genre` table.

### 1.1 Same Bug in track_album

The `track_album` junction table has the identical bug — it does not filter `parent_album_qid` against the `album` table. The `album` FK is massively violated because tracks' P361 (parent album) claims reference albums that were never created as stubs during bootstrap (they are not linked to any artist and thus not in the artist Parquet files' `albums` JSON column).

---

## 2. Root Cause

### 2.1 Missing FK Filter in `load_enrichment()`

The `load_enrichment()` function in `src/db/load.rs` (lines 155-219) does not filter junction table inserts against the referenced tables. Compare with the existing, correct pattern in `load_artist_genre()` and `load_artist_member_of()`:

**Correct pattern** (from `load_artist_genre`, line 425):

```sql
INSERT OR IGNORE INTO artist_genre (artist_id, genre_id)
SELECT id, genre_qid FROM (
    SELECT a.id, unnest(string_split(a.genres, '|')) as genre_qid
    FROM read_parquet('...') a
    WHERE a.genres IS NOT NULL AND a.genres != ''
) sq
WHERE sq.genre_qid IN (SELECT id FROM genre)   -- ← FK guard
```

**Buggy pattern** (from `load_enrichment`, line 193):

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL                    -- ← missing IN (SELECT id FROM genre)
```

### 2.2 Why Genres Are Missing from the genre Table

The `genre` table is populated during bootstrap from `genres.parquet`. The `genres.parquet` file is written during the extraction phase **only for Q-IDs in the `genres` set** — which is collected from `SELECT DISTINCT id FROM genre`. During the initial bootstrap, genres are extracted from the artist Parquet files' `genres` column (pipe-delimited P136 genre Q-IDs from artists). A genre referenced by an album's P136 claim **but not referenced by any artist** will never be in the `genre` table.

**Measured impact:** 6 genre Q-IDs out of 56,074 album-genre links are missing from the `genre` table (0.01%).

### 2.3 Why Albums Are Missing from the album Table

The `album` table is populated during bootstrap from the artist Parquet files' `albums` JSON column. These are albums linked to artists via P175 (performer), P358 (discography), etc. A track's P361 (parent album) claim references an album that may not be linked to any artist in the database — the track is in the database because its performer (P175) is an artist we track, but the parent album is not necessarily linked to that artist (or any artist) in our schema.

**Measured impact:** 7,087 out of 7,091 track→album links reference albums missing from the `album` table (99.9%).

---

## 3. Proposed Solution

### 3.1 Add FK Guards to `load_enrichment()`

Add a `WHERE ... IN (SELECT id FROM ...)` subquery filter to both the `album_genre` and `track_album` inserts, matching the established pattern used by `load_artist_genre()` and `load_artist_member_of()`.

**album_genre fix** (add `AND e.genre_qid IN (SELECT id FROM genre)`):

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
  AND e.genre_qid IN (SELECT id FROM genre)        -- ← FK guard added
```

**track_album fix** (add `AND e.parent_album_qid IN (SELECT id FROM album)`):

```sql
INSERT OR IGNORE INTO track_album (track_id, album_id)
SELECT DISTINCT e.entity_qid, e.parent_album_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'track'
  AND e.parent_album_qid IS NOT NULL
  AND e.parent_album_qid IN (SELECT id FROM album)  -- ← FK guard added
```

### 3.2 Discarded Alternative: Upsert Missing Rows

An alternative approach would be to insert placeholder rows for the missing genres/albums before loading the junction tables:

```sql
INSERT OR IGNORE INTO genre (id, name)
SELECT DISTINCT e.genre_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
  AND e.genre_qid NOT IN (SELECT id FROM genre);
```

This was rejected for two reasons:

1. **Inconsistent with the rest of the codebase.** All other junction table loads (`load_artist_genre`, `load_artist_member_of`, `load_artist_instrument`) silently drop FK-violating rows rather than inserting placeholders. Consistency is more important than completeness.

2. **Information loss is negligible.** 6 out of 56,074 genre links (0.01%) are dropped. The `backfill_names` step later resolves album names, release dates, and record labels from the `qid_label` table — row counts there are unaffected. The dropped genres represent genuinely obscure genres that lack any English label in Wikidata (they are absent from the `genre` table for a reason). For the `track_album` case, the 7,087 dropped links represent albums that are not in our database at all — inserting a placeholder Q-ID as the album name would produce a useless `(unknown)` reference.

### 3.3 No Schema Changes Needed

The fix is purely in the SQL query text. No schema migrations, no new tables, no new columns, no new Parquet files. The `INSERT OR IGNORE` already handles the case where the same valid pair appears multiple times — the FK guard just prevents the INSERT from trying to insert invalid pairs.

> **Note on `INSERT OR IGNORE` semantics:** DuckDB's `INSERT OR IGNORE` ignores primary-key/unique conflicts but **does not suppress foreign-key violations** — FK violations raise a hard `Constraint Error` even under `INSERT OR IGNORE`. This is confirmed by the production failure: the buggy `load_enrichment()` SQL already used `INSERT OR IGNORE` and still aborted on the missing `Q56410100` genre. This is why the SQL-level FK guard is required, not just the `INSERT OR IGNORE` clause.

---

## 4. Data Impact Analysis

### 4.1 Enrichment Row Counts

| Category | Total | After fix | Dropped |
|---|---|---|---|
| Enrichment rows (total) | 155,197 | 155,197 | 0 |
| Album enrichment rows | 109,031 | 109,031 | 0 |
| Track enrichment rows | 46,166 | 46,166 | 0 |
| Album-genre links (from enrichment) | 56,074 | 56,068 | 6 |
| Track-album links (from enrichment) | 7,091 | 4 | 7,087 |

### 4.2 Downstream Impact

The `backfill_names()` function (called after `load_enrichment()`) performs these UPDATEs:

| Backfill step | Source | Affected by dropped rows? |
|---|---|---|
| `album.name` ← `qid_label.label` | `qid_label` table | **No** — unaffected |
| `track.name` ← `qid_label.label` | `qid_label` table | **No** — unaffected |
| `artist.name` ← `qid_label.label` | `qid_label` table | **No** — unaffected |
| `album.release_date` ← `enrichment.release_date` | enrichment Parquet | **No** — uses `entity_qid`, not `genre_qid` or `parent_album_qid` |
| `album.record_label` ← `qid_label.label` on record_label_qid | enrichment Parquet + `qid_label` | **No** — uses `record_label_qid`, not `genre_qid` or `parent_album_qid` |
| `track.duration_seconds` ← `enrichment.duration_seconds` | enrichment Parquet | **No** — uses `entity_qid`, not `genre_qid` or `parent_album_qid` |

The dropped rows only affect the `album_genre` and `track_album` junction tables — no other data is impacted. Query results that use these junction tables will miss a small number of genre/track-album associations, but this is a minor completeness gap rather than a correctness bug.

### 4.3 Comparison with Bootstrap

The bootstrap phase already drops FK-violating rows silently in `load_artist_genre` (line 425: `WHERE sq.genre_qid IN (SELECT id FROM genre)`) and `load_artist_member_of` (line 505: `WHERE sq.member_qid IN (SELECT id FROM artist)`). The enrichment loader should follow the same convention.

---

## 5. Implementation Plan

### Step 1: Add FK guard to album_genre insert

**File:** `src/db/load.rs`

Change the `album_genre_sql` SQL string in `load_enrichment()` (around line 193) to add `AND e.genre_qid IN (SELECT id FROM genre)`.

**Before:**

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
```

**After:**

```sql
INSERT OR IGNORE INTO album_genre (album_id, genre_id)
SELECT DISTINCT e.entity_qid, e.genre_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'album'
  AND e.genre_qid IS NOT NULL
  AND e.genre_qid IN (SELECT id FROM genre)
```

**Commit:** `fix(db): guard album_genre FK constraint in enrichment loader`

### Step 2: Add FK guard to track_album insert

**File:** `src/db/load.rs`

Change the `track_album_sql` SQL string in `load_enrichment()` (around line 207) to add `AND e.parent_album_qid IN (SELECT id FROM album)`.

**Before:**

```sql
INSERT OR IGNORE INTO track_album (track_id, album_id)
SELECT DISTINCT e.entity_qid, e.parent_album_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'track'
  AND e.parent_album_qid IS NOT NULL
```

**After:**

```sql
INSERT OR IGNORE INTO track_album (track_id, album_id)
SELECT DISTINCT e.entity_qid, e.parent_album_qid
FROM read_parquet('{path_str}') e
WHERE e.entity_type = 'track'
  AND e.parent_album_qid IS NOT NULL
  AND e.parent_album_qid IN (SELECT id FROM album)
```

**Commit:** `fix(db): guard track_album FK constraint in enrichment loader`

### Step 3: Add unit tests for FK guards

**File:** `src/db/load.rs` (in `#[cfg(test)] mod tests`)

First, add a `write_test_enrichment_parquet(dir: &Path)` helper, mirroring the existing `write_test_genres_parquet` / `write_test_albums_tracks_parquet` helpers. It must write `enrichment.parquet` with the **exact production schema** (see `write_enrichment_parquet` in `src/label_extractor.rs`, line 662):

| Column | Type | Nullable |
|---|---|---|
| `entity_qid` | UTF-8 | No |
| `entity_type` | UTF-8 | No |
| `release_date` | UTF-8 | Yes |
| `record_label_qid` | UTF-8 | Yes |
| `duration_seconds` | UTF-8 | Yes |
| `genre_qid` | UTF-8 | Yes |
| `parent_album_qid` | UTF-8 | Yes |

Then add two tests to the existing test module. Each test writes **both a valid row and an invalid row** to the enrichment Parquet, so the Assert phase verifies both directions of the guard in one pass (valid row inserted, invalid row silently dropped):

1. **`test_load_enrichment_album_genre_fk_guard`** — Write two album rows (`entity_type='album'`): one with `genre_qid` that exists in `genre`, one with `genre_qid` that does not. Pre-populate **both** referenced tables: `genre` with the valid genre AND `album` with the valid row's `entity_qid` (the `album_genre.album_id → album(id)` FK is checked on every insert — see note in §3.3, an unpopulated album row would raise a hard error, not be ignored). `album.name` is `NOT NULL`, so seed it with a placeholder name (e.g. `'QTestAlbum'`). Verify `album_genre` contains exactly the valid row (count = 1, correct `genre_id`).

2. **`test_load_enrichment_track_album_fk_guard`** — Write two track rows (`entity_type='track'`): one with `parent_album_qid` that exists in `album`, one with `parent_album_qid` that does not. Pre-populate **both** referenced tables: `album` with the valid `parent_album_qid` AND `track` with the valid row's `entity_qid` (satisfies `track_album.track_id → track(id)`). Both `album.name` and `track.name` are `NOT NULL` — seed placeholder names. Verify `track_album` contains exactly the valid row (count = 1, correct `album_id`).

Both tests follow the existing test patterns in `src/db/load.rs` (use `tempfile::TempDir` for Parquet files, `test_conn()` for the in-memory DuckDB database, and the `StringBuilder`/`ArrowWriter` helpers for Parquet file creation).

**Commit:** `test(db): add FK guard tests for enrichment loading`

### Step 4: Run full test suite

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
```

**Commit:** None (verification step only)

### Step 5: Re-run populate with --resume

Once the fix is applied, re-run the populate command. The `--resume` flag skips the expensive dump scan (the Parquet files already exist):

```bash
cargo run --release -- populate \
  --db /home/tr/wiki_db/music.duckdb \
  --dump /home/tr/wiki_db/latest-all.json.gz \
  --resume
```

Expected outcome:

- Skip dump extraction (~4 hours saved)
- Load 738,267 labels from `labels.parquet` (idempotent, `INSERT OR IGNORE`)
- Load 155,197 enrichment rows from `enrichment.parquet` (this time with FK guards)
- Insert 56,068/56,074 valid album-genre links
- Insert 4/7,091 valid track-album links
- Backfill album/track/artist names from `qid_label`
- Backfill release dates, record labels, durations from enrichment
- Rebuild FTS indexes

**Commit:** None (operational step)

---

## 6. Testing Strategy

### 6.1 Unit Tests (Step 3)

| Test | What it verifies |
|---|---|
| `test_load_enrichment_album_genre_fk_guard` | Enrichment Parquet contains one valid album-genre pair (both FKs pre-populated) and one pair whose `genre_qid` is missing from `genre`. Asserts exactly the valid row lands in `album_genre`; the invalid row is silently dropped |
| `test_load_enrichment_track_album_fk_guard` | Enrichment Parquet contains one valid track-album pair (both FKs pre-populated) and one pair whose `parent_album_qid` is missing from `album`. Asserts exactly the valid row lands in `track_album`; the invalid row is silently dropped |

### 6.2 Integration Test

Run the existing `test_load_all_orchestrator_empty_dir` test to verify no regression in the loading pipeline. (This test expects an error for missing Parquet files, which is unchanged.)

### 6.3 Regression Test

Run the full `cargo test` suite. All existing tests must pass without modification. The fix is purely additive — no existing behavior changes.

Additionally, confirm `backfill_names` is unaffected by the dropped rows: the existing `backfill_names` tests (and any test that runs `load_label_and_enrichment` end-to-end) must still pass with the FK-guarded row counts. Per §4.2 the backfill operates on `entity_qid` / `record_label_qid` — never on `genre_qid` or `parent_album_qid` — so no behavior change is expected; the check is a regression guard, not a new test.

### 6.4 Production Verification

After the fix, re-run `populate --resume` and verify:

1. The command completes without FK errors
2. `album_genre` has ~56,068 rows
3. `track_album` has at least 4 rows (the small number of valid album references)
4. `album.name`, `track.name`, `artist.name` columns are populated with English labels
5. `album.release_date` and `album.record_label` are populated
6. `track.duration_seconds` is populated
7. FTS indexes are functional

---

## 7. Risk Assessment

### 7.1 Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| More FK-violating rows in future dump runs | Medium | Low | The FK guard is a silent filter — new FK violations are automatically dropped. If the gap becomes significant, a future enhancement could insert placeholder rows instead. |
| `INSERT OR IGNORE` no longer suffices | Low | Medium | The FK guard prevents the constraint violation before the INSERT. `INSERT OR IGNORE` handles the remaining case of duplicate valid pairs. |
| Performance regression from subquery | Low | Low | DuckDB optimizes `IN (SELECT ...)` to a semi-join. The `genre` table (13,082 rows) and `album` table (109,044 rows) are small enough that the subquery is effectively free. |

### 7.2 Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Insert placeholder rows for missing genres/albums (§3.2) | Inconsistent with established codebase pattern; negligible completeness gain (6/56,074 genres, 0.01%) |
| Disable FK enforcement temporarily | Dangerous — would allow silent data corruption. DuckDB's default FK enforcement is a correctness feature, not a performance knob. |
| Add a pre-check to skip the entire enrichment load if no FK violations exist | Over-engineering. The FK guard is a one-line SQL change; a pre-check would add complexity for no measurable benefit. |
| Modify the extraction phase to also extract labels for genre Q-IDs from album P136 claims | Would address the root cause (missing genres) but introduces a second pass over the dump or a complex memory buffer. The FK guard is simpler and sufficient. |

### 7.3 Rollback

Reverting the two SQL changes restores the original behavior. No schema changes, no data migration, no file format changes. The database is in a consistent state before `populate` runs, so a rollback + re-run produces a clean database.

---

## 8. References

- Current populate research: [2026-08_populate_names_research.md](./2026-08_populate_names_research.md)
- Source of bug: [`src/db/load.rs`](../../src/db/load.rs), `load_enrichment()`, lines 193-213
- Correct pattern (reference): [`src/db/load.rs`](../../src/db/load.rs), `load_artist_genre()`, line 425
- Correct pattern (reference): [`src/db/load.rs`](../../src/db/load.rs), `load_artist_member_of()`, line 505
- FK constraints: [`src/db/schema.rs`](../../src/db/schema.rs), `album_genre` and `track_album` table definitions
- Error diagnosis: production run on 2026-08-06, `Q56410100` missing from `genre` table
