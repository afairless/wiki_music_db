# Research: Populating Album, Track, and Other Name Columns

**Date:** 2026-08-02
**Status:** Reviewed / Plan
**Reviewed:** 2026-08-02 — see [review notes](#12-review-findings)
**References:**

- [ARCHITECTURE.md](../ARCHITECTURE.md) — full schema, pipeline, and design decisions
- [2026-07_music_db_rust_plan.md](./2026-07_music_db_rust_plan.md) — Rust implementation plan
- [2026-07_music_db_options.md](./2026-07_music_db_options.md) — design options research

---

## 1. Problem Statement

The wiki_db pipeline produces a normalized DuckDB database with 12 tables. Most columns that should contain human-readable names are populated correctly, but several are not:

| Column | Rows | With real names | Status |
|--------|------|-----------------|--------|
| `artist.name` | 2,659,668 | 2,082,158 (78%) | ✅ Mostly populated; 577,504 NULL + 6 QID mirrors |
| `artist.description` | 2,659,668 | 2,280,441 (86%) | ✅ Mostly populated; 379,227 NULL |
| `genre.name` | 13,082 | 13,082 (100%) | ✅ Fully populated |
| **`album.name`** | **109,044** | **0 (0%)** | ❌ All QID mirrors |
| **`track.name`** | **46,166** | **0 (0%)** | ❌ All QID mirrors |
| **`album.release_date`** | **109,044** | **0 (0%)** | ❌ All NULL |
| **`album.record_label`** | **109,044** | **0 (0%)** | ❌ All NULL |
| **`track.duration_seconds`** | **46,166** | **0 (0%)** | ❌ All NULL |

**Why this happened:** The pipeline extracts album and track data from the `albums` and `tracks` JSON arrays in the Wikidata entity for each artist. These arrays contain only `{"album_id": "Qxxxxx", "role": "performer"}` — the raw Q-IDs. The architecture doc ([ARCHITECTURE.md](../ARCHITECTURE.md), §"Decision: Album and track names use Q-ID placeholders") explicitly calls this out:

> *"Seeding the album and track tables with Q-ID placeholders as names... A future phase can resolve actual names."*

---

## 2. Database Inventory — All Columns Needing Attention

### 2.1 Primary Columns — Name Resolution

These are columns where a human-readable name should appear but currently contains a QID or NULL.

| # | Column | Cardinality | Current state | Preferred source |
|---|--------|-------------|---------------|------------------|
| 1 | `album.name` | 109,044 | All QID mirrors | Wikidata English label for each album QID |
| 2 | `track.name` | 46,166 | All QID mirrors | Wikidata English label for each track QID |
| 3 | `artist.name` | 577,504 NULLs | 6 are QID mirrors, rest are NULL | Wikidata English label (already done for 2.08M) |
| 4 | `album.release_date` | 109,044 | All NULL | Wikidata `P577` (publication date) claim |
| 5 | `album.record_label` | 109,044 | All NULL | Wikidata `P264` (record label) claim — label name |
| 6 | `track.duration_seconds` | 46,166 | All NULL | Wikidata `P2047` (duration) claim |

### 2.2 Reference Tables — Missing Lookup Tables

These tables store Q-IDs in foreign-key columns but have no companion table to resolve them to names.

| # | Table | Column | Unique Q-IDs | Description |
|---|-------|--------|-------------|-------------|
| 7 | `artist_instrument` | `instrument_id` | 1,305 | Q-IDs for musical instruments |

**Note:** `artist_member_of.group_id` is already a foreign key to `artist.id` — the group name is available via `artist.name`. No additional table is needed. Instruments are the only Q-IDs without a resolution path.

### 2.3 Empty Junction Tables

These tables have 0 rows — confirmed via code review to be **unimplemented features**, not data bugs.

| # | Table | Expected purpose | Current state | Root cause |
|---|-------|-----------------|---------------|------------|
| 9 | `album_genre` | Many-to-many album ↔ genre | 0 rows | `extract_music_entity()` only extracts P136 genres from artist entities, never from album entities. |
| 10 | `track_album` | Many-to-many track ↔ album | 0 rows | `extract_music_entity()` only extracts P658 track references from artist entities; track-to-album links (P361 on track entities) are never extracted. |

These require extracting data from album and track Wikidata entities (not artist entities), which the current pipeline doesn't do. This can be addressed during the label extraction pass since that pass reads **all** entities from the dump.

---

## 3. Data Source Options

There are several possible sources for resolving Q-IDs to names:

### Option A: Full Wikidata Dump Re-scan (Offline, Local)

**How it works:** Re-stream `latest-all.json.gz` (155 GB, already on disk at `/home/tr/wiki_db/latest-all.json.gz`), but this time collect labels for all referenced album, track, and instrument Q-IDs instead of just music entities.

**Pros:**

- Fully offline — no network dependency
- Covers all Q-IDs in one pass
- Leverages existing `StreamReader` infrastructure
- Gets all entity types (albums, tracks, instruments, record labels) in one sweep

**Cons:**

- 155 GB scan takes 30–60 minutes
- Need to store the mapping (potentially millions of Q-ID → label pairs)
- Large memory/disk for the intermediate label map

**Estimated unique Q-IDs to resolve:**

- `album.id`: 109,044
- `track.id`: 46,166
- `artist.id` (NULL names): ~577,504
- `instrument_id`: 1,305
- `record_label` Q-IDs: unknown, but likely hundreds to low thousands
- **Total:** ~735,000 unique Q-IDs (some overlap expected)

### Option B: Wikidata REST API Batch Resolution (Online)

**How it works:** Use the Wikimedia REST API endpoint `https://www.wikidata.org/wiki/Special:EntityData/{QID}.json` to fetch individual entity data, or the batch API `https://www.wikidata.org/wiki/Special:EntityData/{QID1|QID2|...}.json` for up to 50 Q-IDs per request.

**Pros:**

- No need to re-scan the dump
- Gets full entity data (labels, descriptions, claims) for each Q-ID
- Can be done incrementally

**Cons:**

- ~735,000 API calls (or ~14,700 batches of 50) — slow
- Rate limits apply (~1 req/s recommended)
- Network dependency
- ~735K requests × ~0.2s = ~41 hours minimum (even batched)

### Option C: Wikidata SPARQL Query (Online, Bulk)

**How it works:** Query the Wikidata SPARQL endpoint for labels of all referenced Q-IDs in one or a few queries.

**Example query:**

```sparql
SELECT ?item ?itemLabel WHERE {
  VALUES ?item { wd:Q4118577 wd:Q483927 ... }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
}
```

**Pros:**

- Single query for up to ~10,000 Q-IDs
- Fast (seconds to minutes)
- Returns English labels directly

**Cons:**

- 60-second timeout on public endpoint — need to paginate
- Rate limits on public endpoint
- ~74 queries for 735K Q-IDs (at 10K per query)

### Option D: Hybrid — Local Dump for Bulk, API for Incremental (Recommended)

**How it works:** Re-scan the local Wikidata dump once to build a complete Q-ID → label mapping table, then use the SPARQL/REST API for incremental updates going forward.

**Pros:**

- One-time offline bulk resolution
- Subsequent updates are small and fast
- Leverages existing infrastructure

**Cons:**

- Requires the 30–60 minute dump scan
- Need to store the label mapping table

---

## 4. Recommended Approach

### Phase 1: Build a Q-ID Label Resolution Table

Add new tables to the DuckDB schema:

```sql
CREATE TABLE IF NOT EXISTS qid_label (
    qid         TEXT PRIMARY KEY,
    label       TEXT,
    description TEXT,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instrument (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS record_label (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
```

**Design decisions (resolved):**

- **Instruments**: Dedicated `instrument` table (not folded into `qid_label`). Consistent with the existing `genre` table pattern — each domain entity gets its own normalized table. Instruments without English labels are omitted.
- **Record labels**: Normalized `record_label` table as a reference lookup (same pattern as `genre`/`instrument`). `album.record_label` keeps its TEXT type and stores the resolved label name — no FK constraint change. DuckDB does not support `ALTER TABLE … ALTER COLUMN` for constraint changes, so the relationship to `record_label(id)` is logical, not enforced.

**Bootstrap integration:** This label extraction pass **replaces** the existing genre-label second pass in `cmd_bootstrap`. After this plan is implemented, the existing `extract_genre_labels()` function and its call in `cmd_bootstrap` are deprecated and removed. The `bootstrap` subcommand does not automatically call `populate` — users must run `populate` separately after `bootstrap`. (A future UX enhancement could integrate them.)

**Implementation approach:**

1. Collect all referenced Q-IDs from the existing database into a `HashSet<String>`:
   - `album.id` (109,044)
   - `track.id` (46,166)
   - `artist.id` WHERE `name IS NULL OR name = id` (~577,504)
   - `artist_instrument.instrument_id` (1,305)
   - Album and track entity Q-IDs (for extracting album genres and track-album links)
2. During the scan, dynamically add any P264 record-label Q-IDs discovered on album entities to the match set. This allows their English labels to be extracted if the label entity appears later in the dump (higher Q-ID). Record label Q-IDs that appeared earlier in the dump (lower Q-ID than the album entity) cannot be resolved in this pass — they remain as raw Q-IDs in `album.record_label`, consistent with the Q-ID-placeholder pattern. A subsequent re-run of `populate` or the incremental update pipeline resolves remaining Q-IDs.
3. Stream the dump, extracting `(id, en_label, en_description, claims)` for any Q-ID in the set
4. Write results to intermediate Parquet files: `labels.parquet`, `enrichment.parquet`
5. Load Parquet → `qid_label`, `instrument`, `record_label`, `album_genre`, `track_album`

**Memory consideration:** ~735K Q-IDs in a `HashSet<String>` at ~100 bytes each ≈ 73 MB. This is acceptable for a one-time bulk operation. A bloom-filter pre-check could reduce deserialization overhead (skip lines that match zero Q-IDs).

**File sizes:** `labels.parquet` at ~735K rows × ~100 bytes ≈ 73 MB (single file, no rotation needed). `enrichment.parquet` at a similar order of magnitude. Both fit comfortably in memory and on disk.

### Phase 2: Backfill the Existing Columns

Once `qid_label`, `instrument`, and `record_label` are populated, backfill the columns. **All UPDATEs must be wrapped in a DuckDB transaction** so a failure leaves the database unchanged.

```sql
-- album.name
UPDATE album SET name = COALESCE(
    (SELECT label FROM qid_label WHERE qid = album.id),
    album.name  -- keep Q-ID placeholder when no English label exists
);

-- track.name
UPDATE track SET name = COALESCE(
    (SELECT label FROM qid_label WHERE qid = track.id),
    track.name  -- keep Q-ID placeholder when no English label exists
);

-- artist.name (NULLs and QID mirrors)
UPDATE artist SET name = COALESCE(
    (SELECT label FROM qid_label WHERE qid = artist.id),
    artist.name  -- keep Q-ID placeholder — do NOT invent "Unknown"
) WHERE name IS NULL OR name = id;

-- instrument lookup table
INSERT OR IGNORE INTO instrument (id, name)
SELECT DISTINCT ai.instrument_id, ql.label
FROM artist_instrument ai
INNER JOIN qid_label ql ON ql.qid = ai.instrument_id
WHERE ql.label IS NOT NULL;

-- record_label lookup table
-- Populated from enrichment.parquet data, not from album table (which is all NULL before backfill).
-- The enrichment extractor writes (record_label_qid, record_label_name) pairs discovered during the scan.
INSERT OR IGNORE INTO record_label (id, name)
SELECT DISTINCT record_label_qid, record_label_name
FROM read_parquet('{parquet_dir}/enrichment.parquet')
WHERE record_label_qid IS NOT NULL
  AND record_label_name IS NOT NULL;
```

### Phase 3: Populate Enrichment Data (Dates, Labels, Durations)

The dump re-scan also extracts claims for:

- `album.release_date` ← Wikidata `P577` (publication date), parsed to DATE
- `album.record_label` ← Wikidata `P264` (record label Q-ID). The raw P264 Q-ID is extracted alongside the album entity's claims and written to `enrichment.parquet`. During the label extraction pass, any label entity whose Q-ID matches a discovered P264 target also has its English label extracted to `qid_label` (see "Implementation approach" step 2 on dynamic Q-ID collection). The loader resolves `album.record_label` via `qid_label` lookup. Record label Q-IDs that cannot be resolved in this pass remain as raw Q-IDs — consistent with the existing Q-ID-placeholder pattern for album/track names.
- `track.duration_seconds` ← Wikidata `P2047` (duration), parsed to INTEGER (seconds)

These are written to a separate `enrichment.parquet` file during the label extraction pass, then loaded in a separate step.

### Phase 4: Populate Empty Junction Tables

The dump re-scan for labels also reads **all** entities, including album and track entities. During this pass, also extract:

- **`album_genre`**: When a Q-ID in our set is an album entity, extract its P136 (genre) claims and emit `(album_id, genre_id)` pairs.
- **`track_album`**: When a Q-ID in our set is a track entity, extract its P361 (part of) claims — these link tracks to their parent albums — and emit `(track_id, album_id)` pairs.

These are written to `enrichment.parquet` alongside the date/label/duration data.

---

## 5. Implementation Plan

### Step 1: Add new tables to schema

Add `qid_label`, `instrument`, and `record_label` tables to `src/db/schema.rs`. Bump `SCHEMA_VERSION` from 1 to 2. Add a migration path for existing v1 databases.

**Schema changes:**

```sql
CREATE TABLE IF NOT EXISTS qid_label (
    qid         TEXT PRIMARY KEY,
    label       TEXT,
    description TEXT,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instrument (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS record_label (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
```

**Album schema change:** `album.record_label` keeps its TEXT type — stores the resolved label name directly. No FK constraint change (DuckDB does not support `ALTER TABLE … ALTER COLUMN` for constraint changes). The relationship to `record_label(id)` is logical, not enforced by DuckDB. The `record_label` table is a reference lookup, not an enforced foreign key target.

**Commit:** `feat(db): add qid_label, instrument, and record_label tables`

### Step 2: Implement label + claims extractor

Add a new module `src/label_extractor.rs` that:

1. Queries the existing DuckDB database for all referenced Q-IDs:
   - `SELECT DISTINCT id FROM album`
   - `SELECT DISTINCT id FROM track`
   - `SELECT DISTINCT id FROM artist WHERE name IS NULL OR name = id`
   - `SELECT DISTINCT instrument_id FROM artist_instrument`
   - Add album and track Q-IDs for album_genre and track_album extraction
2. Unions and deduplicates into a `HashSet<String>`
3. During the scan, dynamically adds any P264 record-label Q-IDs discovered on album entities to the match set (so label entities encountered later in the dump get their labels extracted)
4. Streaming-parses `latest-all.json.gz`, extracting for any Q-ID in the set:
   - `(qid, en_label, en_description)` → `labels.parquet`
   - `(album_qid, P577 date, P264 label_qid)` → `enrichment.parquet`
   - `(track_qid, P2047 duration)` → `enrichment.parquet`
   - `(album_qid, genre_qid)` for P136 on album entities → `enrichment.parquet`
   - `(track_qid, album_qid)` for P361 on track entities → `enrichment.parquet`
5. Uses a substring pre-check for performance (skip deserialization for lines matching zero Q-IDs). A fuzzy `line.contains(qid)` check on the raw JSON line avoids deserializing ~99% of the dump. Unit tests verify the pre-check correctly matches and skips lines.
6. Writes results to Parquet files:
   - `labels.parquet` — single file (~73 MB, no rotation needed at this size)
   - `enrichment.parquet` — single file (similar size; no rotation needed)

**Parquet schemas:**

`labels.parquet`:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `qid` | TEXT | No | Wikidata Q-ID (e.g. "Q2831") |
| `label` | TEXT | Yes | English label, NULL if missing |
| `description` | TEXT | Yes | English description, NULL if missing |

`enrichment.parquet`:

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `entity_qid` | TEXT | No | The entity Q-ID this row belongs to |
| `entity_type` | TEXT | No | `"album"` or `"track"` |
| `release_date` | TEXT | Yes | Raw P577 date string (parsed to DATE during load) |
| `record_label_qid` | TEXT | Yes | Raw P264 Q-ID for record label |
| `duration_seconds` | TEXT | Yes | Raw P2047 value (parsed to INTEGER during load) |
| `genre_qid` | TEXT | Yes | Q-ID for album_genre link (P136 on album entity) |
| `parent_album_qid` | TEXT | Yes | Q-ID for track_album link (P361 on track entity) |

Both files use the same Arrow/Parquet writing infrastructure as the existing `MusicEntityBatchWriter` (same crate versions, same file I/O patterns).

**Data contract (ingestion → output boundary):**

- Labels must be non-empty UTF-8 strings; empty labels are stored as NULL
- Dates are parsed via Wikidata format (`+YYYY-MM-DDT…`); unparseable dates are logged at WARN and stored as NULL
- Durations are parsed to INTEGER (seconds); unparseable values are logged at WARN and stored as NULL
- Q-IDs without English labels or claims are silently omitted from output

**Intermediate storage:** Using Parquet files (not direct DuckDB writes) provides resumability, independent testability, and decouples extraction from loading — consistent with the main pipeline architecture.

**Commit:** `feat: implement Q-ID label and claims extraction from Wikidata dump`

### Step 3: Implement DuckDB loader for new Parquet files

Add functions to `src/db/load.rs` that load `labels.parquet` and `enrichment.parquet` into the new tables and backfill existing columns.

Load order:

1. `labels.parquet` → `qid_label` (INSERT OR IGNORE)
2. `enrichment.parquet` → populate `instrument`, `record_label`, `album_genre`, `track_album`
3. All backfill UPDATEs wrapped in a single DuckDB transaction:
   - `album.name` from qid_label
   - `track.name` from qid_label
   - `artist.name` from qid_label (where NULL or equals id)
   - `album.release_date` from enrichment
   - `album.record_label` from enrichment (resolved via qid_label)
   - `track.duration_seconds` from enrichment
4. After backfill completes, recreate FTS indexes on `album(name)` and `track(name)` since their values changed from Q-ID placeholders to English labels. The existing `create_fts_indexes()` function is idempotent and safe to call.

> **Note:** Steps 4-7 from the original plan (backfill album/track/artist names, backfill enrichment data, populate instrument/record_label tables, populate junction tables) are all implemented as a single commit within this loader step. They are not independent — all depend on the same `labels.parquet` and `enrichment.parquet` files written by Step 2, and each SQL operation is fast (sub-second except ~2s for artist.name). Splitting them into separate commits would require re-running the 30–45 minute dump scan for each commit's integration test.

**Commit:** `feat(db): add label and enrichment Parquet loader with backfill`

### Step 4: Add `populate` CLI subcommand

Add a new `populate` subcommand (separate from `bootstrap`):

```bash
cargo run -- populate --db music.duckdb --dump latest-all.json.gz
```

This runs the label extractor → Parquet writer → DuckDB loader → backfill pipeline. Supports `--force` to force a re-populate even if the database already has resolved names.

**Resume support:** Since `labels.parquet` and `enrichment.parquet` are each a single file, resume is all-or-nothing: if both files exist, the extraction step is skipped and the loader runs from existing Parquet files. If either file is missing or incomplete, extraction restarts from the beginning of the dump. (The files are small enough — ~73 MB each — that mid-file resume is unnecessary.)

```bash
# Re-run just the loading phase using existing Parquet files
cargo run -- populate --db music.duckdb --dump latest-all.json.gz --resume
```

**Commit:** `feat(cli): add populate subcommand for name/date/label backfill`

### Step 5: Update the incremental update pipeline

Modify the `update` subcommand to also fetch labels for new album/track/artist/instrument Q-IDs via the Wikimedia REST API (individual entity lookups). New entities from incremental updates are already inserted with Q-ID placeholder names — the update step should resolve these to English labels when available.

**Commit:** `feat(update): fetch labels for new entities during incremental update`

---

## 6. Testing Strategy

### Unit Tests (in `src/label_extractor.rs`)

| Test | What it verifies |
|---|---|
| `test_extract_label_valid` | Entity with English label → `(qid, label)` pair |
| `test_extract_label_no_en` | Entity without English label → NULL label (not rejected) |
| `test_extract_label_empty` | Entity with empty English label → NULL label |
| `test_extract_claim_p577` | Entity with P577 date → parsed NaiveDate |
| `test_extract_claim_p577_precision_9` | Entity with year-only P577 (precision 9, `+1970-00-00T…`) → parsed to YYYY-01-01, logged at DEBUG |
| `test_extract_claim_p577_precision_8` | Entity with decade precision P577 → NULL, logged at WARN |
| `test_extract_claim_p577_invalid` | Entity with unparseable P577 → NULL (logged at WARN) |
| `test_extract_claim_p264` | Entity with P264 record label → Q-ID string |
| `test_extract_claim_p2047` | Entity with P2047 duration → INTEGER seconds |
| `test_extract_claim_p2047_invalid` | Entity with unparseable P2047 → NULL (logged at WARN) |
| `test_extract_album_genre` | Album entity with P136 → `(album_id, genre_id)` pair |
| `test_extract_track_album` | Track entity with P361 → `(track_id, album_id)` pair |
| `test_qid_set_collection` | DB Q-ID collection produces correct union of all referenced IDs |
| `test_qid_set_collection_empty_db` | Empty database (no tables populated) returns empty set |
| `test_qid_set_collection_partial` | Database with only some tables returns correct subset |
| `test_substring_precheck_match` | Line containing a Q-ID is not skipped |
| `test_substring_precheck_skip` | Line without any target Q-ID is skipped (avoid deserialization) |
| `test_substring_precheck_false_positive` | Line containing Q-ID as substring of another string still deserializes correctly and is filtered by full Q-ID comparison |

### Integration Tests (in `tests/populate_test.rs`)

| Test | What it verifies |
|---|---|
| `test_populate_full_pipeline` | End-to-end: create DB with known Q-IDs → run populate → verify names/dates/labels resolved |
| `test_populate_idempotent` | Run populate twice → identical database state (row counts, column values) |
| `test_populate_preserves_existing` | Populate doesn't overwrite already-populated names (e.g., artists with real names) |
| `test_populate_missing_label` | Q-IDs without English labels keep their Q-ID placeholder |
| `test_populate_transaction_rollback` | Corrupt one Parquet row to trigger a mid-load failure → database unchanged (verify all prior inserts/updates were rolled back) |
| `test_populate_fts_after_backfill` | After populate, search for a resolved album/track name via FTS (or LIKE fallback) returns the correct result |

### Test Fixtures (in `tests/fixtures/`)

| Fixture | Content |
|---|---|
| `album_entity.json` | Album entity with en label, P577 date (precision 11), P264 label, P136 genre |
| `album_entity_year_only.json` | Album entity with P577 date precision 9 (year only) |
| `track_entity.json` | Track entity with en label, P2047 duration, P361 part-of |
| `mini_dump_with_labels.json.gz` | Small gzipped dump containing artist + album + track entities for integration testing |

---

## 7. Migration Strategy

Existing v1 databases need a migration path to v2. The approach is:

1. **Schema migration**: Bump `SCHEMA_VERSION` from 1 to 2 in `src/db/schema.rs`. Add `CREATE TABLE IF NOT EXISTS` for `qid_label`, `instrument`, and `record_label`. The `initialize()` function is already idempotent — calling it on a v1 database adds the new tables without affecting existing data. Since `initialize()` uses `INSERT OR IGNORE INTO schema_version (version) VALUES (?1)`, bumping `SCHEMA_VERSION` to 2 causes the new version row to be inserted alongside the existing row of 1. The `schema_version()` function returns `MAX(version)`, so it correctly reports 2 after migration. No explicit `UPDATE` of the version row is needed.

2. **Data migration**: The `populate` subcommand is the migration mechanism. Running `populate` on a v1 database resolves all Q-ID names and populates the new tables. On a v2 database, `populate` is a no-op for already-resolved names (INSERT OR IGNORE / COALESCE).

3. **Version check**: The `populate` subcommand checks `schema_version`. If the database is already v2 and all names are resolved, it skips the expensive dump scan. Users can force a re-populate with `--force`.

No separate SQL migration scripts are needed — `populate` is both the initial backfill and the v1→v2 upgrade path.

---

## 8. Data Quality Considerations

### 8.1 Missing Labels

Some Wikidata entities (especially very obscure tracks and albums) may not have English labels. For these, the `name` value remains as the Q-ID placeholder (the current behavior). The `qid_label` table stores NULL for missing labels, and the UPDATE query uses `COALESCE(label, existing_name)` — never `'Unknown'` or another invented placeholder.

### 8.2 Multiple Labels

Wikidata entities can have one label per language. The English label is the authoritative name for English-language queries. No deduplication is needed.

### 8.3 Stale Data

If the local dump is old (e.g., weeks old), some labels may have changed on Wikidata. The incremental update pipeline (Step 5) handles freshness going forward. `qid_label.updated_at` tracks when each label was last extracted/populated (via `DEFAULT CURRENT_TIMESTAMP` on INSERT), not the Wikidata dump's generation date. This gives a per-row audit trail of when the local data was refreshed.

### 8.4 Record Label Resolution

Record label Q-IDs are discovered during the dump scan (from P264 claims on album entities) and dynamically added to the match set. If the label entity appears later in the dump (higher Q-ID), its English label is extracted and stored in `qid_label`. The loader then resolves `album.record_label` via `qid_label` lookup, storing the resolved name directly in the TEXT column. If the label entity appeared earlier in the dump (lower Q-ID and already passed), the P264 Q-ID remains as a raw Q-ID placeholder — consistent with how album/track names are handled. A subsequent `populate --force` re-run or the incremental update pipeline resolves remaining Q-IDs. The `record_label` table stores the canonical `(id, name)` mapping for all successfully resolved labels.

### 8.5 Data Quality Contracts

At the extraction boundary:

- **Labels**: must be non-empty UTF-8 strings. NULL if missing or empty — never rejected.
- **Dates (P577)**: parsed from Wikidata format `+YYYY-MM-DDT…`. Full-precision dates (precision 11, day-level) parse normally. Year-only dates (precision 9, e.g. `+1970-00-00T00:00:00Z`) are parsed to `YYYY-01-01` and logged at DEBUG. Decade/century/millennium precisions (8, 7, 6) are logged at WARN and stored as NULL — they are too imprecise for meaningful DATE storage. Unparseable values logged at WARN, stored as NULL.
- **Durations (P2047)**: parsed to INTEGER seconds. Unparseable values logged at WARN, stored as NULL.
- **Genre links (P136 on album entities)**: Q-ID pairs. Both must be non-empty.
- **Track-album links (P361 on track entities)**: Q-ID pairs. Both must be non-empty.

---

## 9. Performance Estimates

| Operation | Estimated time | Notes |
|-----------|---------------|-------|
| Collect Q-ID set from DB | < 5 sec | UNION query across all relevant tables |
| Scan dump for label + claims extraction | 30–45 min | ~735K Q-IDs to match; substring pre-check avoids deserialization for ~99% of lines |
| Load `labels.parquet` → `qid_label` | < 10 sec | ~735K rows, INSERT OR IGNORE |
| Load `enrichment.parquet` → instrument, record_label, album_genre, track_album | < 10 sec | INSERT OR IGNORE |
| SQL UPDATE for album.name | < 1 sec | 109K rows, indexed |
| SQL UPDATE for track.name | < 1 sec | 46K rows, indexed |
| SQL UPDATE for artist.name | ~2 sec | 577K rows, indexed |
| SQL UPDATE for album.release_date | < 1 sec | 109K rows, indexed |
| SQL UPDATE for album.record_label | < 1 sec | 109K rows, indexed |
| SQL UPDATE for track.duration | < 1 sec | 46K rows, indexed |
| Recreate FTS indexes (album, track) | < 5 sec | Idempotent `create_fts_indexes()` |
| **Total (one-time)** | **~30–45 min** | Dominated by dump scan |

**Memory estimate**: ~735K Q-ID strings in `HashSet<String>` at ~100 bytes each ≈ 73 MB. Acceptable for a one-time bulk operation.

**Disk estimate**: `labels.parquet` ≈ 73 MB, `enrichment.parquet` ≈ similar. Both files are ephemeral — they can be deleted after successful load (consistent with `--cleanup-parquet` pattern).

---

## 10. Schema Changes Summary

### New Tables

```sql
CREATE TABLE IF NOT EXISTS qid_label (
    qid         TEXT PRIMARY KEY,
    label       TEXT,
    description TEXT,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS instrument (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS record_label (
    id   TEXT PRIMARY KEY,
    name TEXT NOT NULL
);
```

### Modified Tables (Data Only, No Schema Change)

- `album.name` — updated from QID to English label
- `track.name` — updated from QID to English label
- `artist.name` — updated from NULL/QID to English label
- `album.release_date` — populated from P577
- `album.record_label` — populated from P264 (resolved label name stored as TEXT; logical reference to `record_label(id)`)
- `track.duration_seconds` — populated from P2047

### Populated (Previously Empty) Tables

- `album_genre` — populated with album → genre links from album entity P136 claims
- `track_album` — populated with track → album links from track entity P361 claims

### Schema Version Bump

`schema_version` bumped from 1 to 2. The `populate` subcommand serves as the v1→v2 migration path.

---

## 11. Resolved Design Decisions

These were open questions during the initial plan; now resolved:

1. **Instruments** → Dedicated `instrument` table (normalized, consistent with `genre` pattern)
2. **Record labels** → Normalized `record_label` table as reference lookup (not enforced FK). `album.record_label` keeps TEXT type. Dynamic Q-ID collection during dump scan for resolution.
3. **Album genres & track-album links** → Included in populate phase (extracted during label dump scan)
4. **CLI command** → Separate `populate` subcommand (not integrated into `bootstrap`). `bootstrap` does not automatically run `populate`.
5. **NULL artist names** → Keep Q-ID placeholder when no English label exists (never invent "Unknown")
6. **Bootstrap integration** → The new label extraction replaces the existing `extract_genre_labels()` second pass. That function and its call site in `cmd_bootstrap` are deprecated and removed.

## 12. Review Findings

This plan was reviewed on 2026-08-02. Key issues identified and resolved:

- **SQL error fixed**: `COALESCE(…, 'Unknown')` changed to `COALESCE(…, artist.name)` — never invent placeholder text
- **SQL error fixed**: `LEFT JOIN` for instrument table changed to `INNER JOIN` — avoid NULL names in NOT NULL column
- **Merged schema steps**: All three new tables (`qid_label`, `instrument`, `record_label`) added in single Step 1
- **Reordered steps**: CLI subcommand moved after all backfill logic; steps 4-7 merged into Step 3 (not independent — all depend on same Parquet files)
- **Split investigation from implementation**: Junction table research replaced with concrete extraction plan
- **Added intermediate storage**: Labels and claims written to `labels.parquet` and `enrichment.parquet` (not direct DuckDB)
- **Added transaction wrapping**: All backfill UPDATEs run inside a single DuckDB transaction
- **Added migration strategy**: `populate` subcommand serves as v1→v2 upgrade path
- **Added testing strategy**: Unit tests for extractor, integration tests for pipeline, new fixtures
- **Added data quality contracts**: Explicit boundaries for label/date/duration extraction
- **Merged dump passes**: Label extraction replaces the existing genre label second pass (not a third scan); `extract_genre_labels()` deprecated
- **Documented root cause**: `album_genre` and `track_album` emptiness is an unimplemented feature, not a bug
- **Record label circular dependency**: Dynamic Q-ID collection during scan; unresolved Q-IDs remain as placeholders (resolvable on re-run)
- **Parquet schemas defined**: Explicit column definitions for `labels.parquet` and `enrichment.parquet`
- **FTS index refresh**: Recreate FTS indexes on `album` and `track` after name backfill
- **Imprecise Wikidata dates**: Year-only precision (9) mapped to YYYY-01-01; coarser precisions stored as NULL
- **`updated_at` semantics clarified**: Tracks extraction time, not dump date
- **Resume simplified**: Single-file Parquet — resume is all-or-nothing (skip extraction if both files exist)
