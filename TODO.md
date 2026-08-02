# Implementation Plan: Populate Album, Track, and Other Name Columns

Source: `docs/research/2026-08_populate_names_research.md`

## Context

This plan builds on the existing wiki_db pipeline (Phases 1–8 completed). The database currently stores Q-ID placeholders for album/track names, NULL artist names, empty `album_genre` and `track_album` tables, and NULL enrichment columns (release dates, record labels, durations). This plan resolves all of those via a re-scan of the Wikidata dump that extracts labels, claims, and cross-references for all referenced Q-IDs.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat(db): add qid_label, instrument, and record_label tables` | Schema migration | `src/db/schema.rs` — add 3 tables, bump SCHEMA_VERSION to 2, add migration path | Unit |
| 2 | `feat: implement Q-ID label and claims extraction from Wikidata dump` | Label extractor | `src/label_extractor.rs` — Q-ID collection, stream scan, substring pre-check, Parquet writer for `labels.parquet` and `enrichment.parquet` | Unit, property-based |
| 3 | `feat(db): add label and enrichment Parquet loader with backfill` | DuckDB loader | `src/db/load.rs` — load labels/enrichment, backfill UPDATEs in transaction, recreate FTS, deprecate `extract_genre_labels()` | Integration |
| 4 | `feat(cli): add populate subcommand for name/date/label backfill` | CLI subcommand | `src/cli/populate.rs` — `populate` subcommand with `--force`, `--resume`; `src/cli/mod.rs` — register; `src/main.rs` — wire | Unit, integration |
| 5 | `feat(update): fetch labels for new entities during incremental update` | Update pipeline | `src/sparql.rs` — extend `fetch_entity` response; `src/db/load.rs` — upsert labels; `src/cli/update.rs` — wire label resolution | Unit |

## Step details

### Step 1 — `feat(db): add qid_label, instrument, and record_label tables`

**Rationale:** The new tables store resolved English labels, instrument names, and record label names. They are referenced by the backfill UPDATEs in Step 3.

**Deliverables:**

- `src/db/schema.rs`:
  - Add `CREATE TABLE IF NOT EXISTS qid_label (qid TEXT PRIMARY KEY, label TEXT, description TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`
  - Add `CREATE TABLE IF NOT EXISTS instrument (id TEXT PRIMARY KEY, name TEXT NOT NULL)`
  - Add `CREATE TABLE IF NOT EXISTS record_label (id TEXT PRIMARY KEY, name TEXT NOT NULL)`
  - Bump `SCHEMA_VERSION` from 1 to 2
  - The `initialize()` function is already idempotent — calling it on a v1 database adds the new tables. The `schema_version()` function returns `MAX(version)`, so it correctly reports 2 after migration. No explicit UPDATE of the version row is needed.
  - Update `all_tables_exist()` to include the new table names
  - Update any existing tests that assert SCHEMA_VERSION

- **Migration strategy:** `populate` subcommand (Step 4) is the migration mechanism. No separate migration scripts.

- **Tests:**
  - `test_initialize_creates_new_tables` — verify `qid_label`, `instrument`, `record_label` exist after initialize
  - `test_schema_version_bumped` — verify SCHEMA_VERSION is 2
  - `test_v1_migration_path` — insert version 1, re-initialize, verify version 2 is returned (MAX)
  - `test_all_tables_exist_includes_new` — verify all_tables_exist succeeds with new tables

### Step 2 — `feat: implement Q-ID label and claims extraction from Wikidata dump`

**Rationale:** A new module `src/label_extractor.rs` that collects all referenced Q-IDs from the existing database, then streams the Wikidata dump to extract labels and claims, writing them to intermediate Parquet files.

**Deliverables:**

- `src/label_extractor.rs` — new module with:
  - `collect_qid_set(conn)` — queries the DuckDB database for all referenced Q-IDs:
    - `SELECT DISTINCT id FROM album`
    - `SELECT DISTINCT id FROM track`
    - `SELECT DISTINCT id FROM artist WHERE name IS NULL OR name = id`
    - `SELECT DISTINCT instrument_id FROM artist_instrument`
    - Adds album and track Q-IDs for album_genre and track_album extraction
    - Unions and deduplicates into a `HashSet<String>`
  - `extract_labels_and_claims(dump_path, qid_set, parquet_dir)` — stream the dump, extracting for any Q-ID in the set:
    - `(qid, en_label, en_description)` → `labels.parquet`
    - `(entity_qid, entity_type, release_date, record_label_qid, duration_seconds, genre_qid, parent_album_qid)` → `enrichment.parquet`
  - During the scan, dynamically adds any P264 record-label Q-IDs discovered on album entities to the match set (so label entities encountered later in the dump get their labels extracted)
  - Substring pre-check: `line.contains(qid)` check on the raw JSON line to avoid deserializing ~99% of the dump
  - Claims extraction:
    - `P577` (publication date) — parsed to DATE. Full precision (11) → normal parse. Year-only (9) → `YYYY-01-01`, logged at DEBUG. Coarser precisions → NULL, logged at WARN.
    - `P264` (record label) — Q-ID of the record label entity
    - `P2047` (duration) — parsed to INTEGER seconds
    - `P136` on album entities → `(album_id, genre_id)` pairs
    - `P361` on track entities → `(track_id, album_id)` pairs
  - Writes results to Parquet files using the same `ArrowWriter`/`WriterProperties` infrastructure as `MusicEntityBatchWriter`

- **Parquet schemas:**

  `labels.parquet`:

  | Column | Type | Nullable |
  |--------|------|----------|
  | `qid` | TEXT | No |
  | `label` | TEXT | Yes |
  | `description` | TEXT | Yes |

  `enrichment.parquet`:

  | Column | Type | Nullable |
  |--------|------|----------|
  | `entity_qid` | TEXT | No |
  | `entity_type` | TEXT | No |
  | `release_date` | TEXT | Yes |
  | `record_label_qid` | TEXT | Yes |
  | `duration_seconds` | TEXT | Yes |
  | `genre_qid` | TEXT | Yes |
  | `parent_album_qid` | TEXT | Yes |

- **Data contracts:**
  - Labels must be non-empty UTF-8 strings; empty labels stored as NULL
  - Dates parsed via Wikidata format; unparseable dates logged at WARN, stored as NULL
  - Durations parsed to INTEGER seconds; unparseable values logged at WARN, stored as NULL
  - Q-IDs without English labels or claims silently omitted from output

- **Tests (unit):**
  - `test_extract_label_valid` — entity with English label
  - `test_extract_label_no_en` — entity without English label → NULL
  - `test_extract_label_empty` — entity with empty English label → NULL
  - `test_extract_claim_p577` — entity with P577 date → parsed
  - `test_extract_claim_p577_precision_9` — year-only precision
  - `test_extract_claim_p577_precision_8` — decade precision → NULL
  - `test_extract_claim_p577_invalid` — unparseable → NULL
  - `test_extract_claim_p264` — record label Q-ID
  - `test_extract_claim_p2047` — duration in seconds
  - `test_extract_claim_p2047_invalid` — unparseable → NULL
  - `test_extract_album_genre` — P136 on album entity
  - `test_extract_track_album` — P361 on track entity
  - `test_qid_set_collection` — DB Q-ID collection produces correct union
  - `test_qid_set_collection_empty_db` — empty DB returns empty set
  - `test_substring_precheck_match` — line containing Q-ID is not skipped
  - `test_substring_precheck_skip` — line without target Q-ID is skipped
  - `test_substring_precheck_false_positive` — substring match still filters correctly

- **Tests (property-based):**
  - `test_roundtrip_labels_parquet` — write labels → read back → match
  - `test_roundtrip_enrichment_parquet` — write enrichment → read back → match

### Step 3 — `feat(db): add label and enrichment Parquet loader with backfill`

**Rationale:** Load the Parquet files produced by Step 2 into the DuckDB tables and backfill the existing columns. All backfill UPDATEs are wrapped in a single transaction. This step also removes the deprecated `extract_genre_labels()` function and its call site in `cmd_bootstrap`, since the new label extraction replaces it.

**Deliverables:**

- `src/db/load.rs`:
  - Add `load_labels(conn, parquet_dir)` — load `labels.parquet` → `qid_label` via `INSERT OR IGNORE`
  - Add `load_enrichment(conn, parquet_dir)` — load `enrichment.parquet`:
    - `INSERT OR IGNORE INTO instrument (id, name)` — from enrichment, joining with `qid_label`
    - `INSERT OR IGNORE INTO record_label (id, name)` — from enrichment, joining with `qid_label`
    - `INSERT OR IGNORE INTO album_genre (album_id, genre_id)` — from enrichment `genre_qid` rows
    - `INSERT OR IGNORE INTO track_album (track_id, album_id)` — from enrichment `parent_album_qid` rows
  - Add `backfill_names(conn)` — single transaction wrapping all UPDATEs:
    - `UPDATE album SET name = COALESCE((SELECT label FROM qid_label WHERE qid = album.id), album.name)`
    - `UPDATE track SET name = COALESCE((SELECT label FROM qid_label WHERE qid = track.id), track.name)`
    - `UPDATE artist SET name = COALESCE((SELECT label FROM qid_label WHERE qid = artist.id), artist.name) WHERE name IS NULL OR name = id`
    - `UPDATE album SET release_date = ...` — from enrichment
    - `UPDATE album SET record_label = ...` — from enrichment (resolved via qid_label)
    - `UPDATE track SET duration_seconds = ...` — from enrichment
  - After backfill, recreate FTS indexes via `create_fts_indexes()` (idempotent)
  - Add `load_label_and_enrichment(conn, parquet_dir)` — orchestrator for the full load pipeline

- `src/main.rs` — `cmd_bootstrap()`:
  - Remove the `extract_genre_labels()` call and the second-pass genre extraction block
  - Remove the `write_genres_parquet()` call for the second pass
  - The genre label extraction is now handled by the `populate` subcommand (Step 4)

- `src/extraction.rs`:
  - Remove `extract_genre_labels()`, `extract_genre_entity()`, `collect_genre_qids()`, `collect_all_genre_qids()`, `GenreEntry` struct
  - Keep `extract_music_entity()` and all other types/functions (they remain in use)

- `src/parquet_writer.rs`:
  - Keep `write_genres_parquet()` — it's still used during the main streaming pass for the genre table. Only the second-pass `extract_genre_labels()` call is removed.

- **Tests (integration):**
  - `test_load_labels_parquet` — write labels.parquet → load → verify qid_label table
  - `test_load_enrichment_parquet` — write enrichment.parquet → load → verify instrument, record_label, album_genre, track_album
  - `test_backfill_album_names` — album with QID placeholder → updated to English label
  - `test_backfill_track_names` — track with QID placeholder → updated to English label
  - `test_backfill_artist_names` — artist with NULL/QID name → updated to English label
  - `test_backfill_preserves_existing` — artist with real name left unchanged
  - `test_backfill_missing_label` — QID without English label keeps Q-ID placeholder
  - `test_backfill_transaction_rollback` — corrupt Parquet row → mid-load failure → database unchanged
  - `test_fts_after_backfill` — search for resolved name after backfill returns correct result
  - `test_backfill_idempotent` — run twice → identical state

### Step 4 — `feat(cli): add populate subcommand for name/date/label backfill`

**Rationale:** A new `populate` subcommand that runs the label extractor → Parquet writer → DuckDB loader → backfill pipeline. Separate from `bootstrap` — users run `populate` after `bootstrap` to resolve names.

**Deliverables:**

- `src/cli/populate.rs` — new module:

  ```rust
  #[derive(Parser, Debug)]
  pub struct PopulateArgs {
      /// Path to the Wikidata JSON dump (gzipped).
      #[arg(long, short)]
      pub dump: Option<String>,

      /// Path to the DuckDB database file.
      #[arg(long)]
      pub db: Option<String>,

      /// Directory for intermediate Parquet files.
      #[arg(long)]
      pub parquet_dir: Option<String>,

      /// Force re-populate even if names are already resolved.
      #[arg(long)]
      pub force: bool,

      /// Skip extraction if Parquet files already exist.
      #[arg(long)]
      pub resume: bool,
  }
  ```

- `src/cli/mod.rs`:
  - Add `Populate(PopulateArgs)` variant to `Command` enum
  - Add `pub mod populate;`

- `src/main.rs`:
  - Add `Command::Populate(args) => cmd_populate(args, config.as_ref())` dispatch
  - Implement `cmd_populate()`:
    1. Check schema version — if already v2 and all names resolved, skip (unless `--force`)
    2. Collect Q-ID set from database
    3. Check for existing Parquet files (resume support)
    4. Stream dump for label + claims extraction (Step 2)
    5. Load Parquet + backfill (Step 3)
    6. Cleanup Parquet files (optional)
    7. Print summary

- **Tests (unit):**
  - `test_populate_subcommand_parses` — basic CLI parsing
  - `test_populate_force_flag` — `--force` parsed correctly
  - `test_populate_resume_flag` — `--resume` parsed correctly

- **Tests (integration in `tests/populate_test.rs`):**
  - `test_populate_full_pipeline` — create DB with known Q-IDs → run populate → verify names resolved
  - `test_populate_idempotent` — run populate twice → identical state
  - `test_populate_preserves_existing` — doesn't overwrite already-populated names
  - `test_populate_missing_label` — Q-IDs without English labels keep Q-ID placeholder
  - `test_populate_transaction_rollback` — corrupt Parquet → mid-load failure → DB unchanged
  - `test_populate_fts_after_backfill` — search for resolved name via FTS

- **Test fixtures (in `tests/fixtures/`):**
  - `album_entity.json` — album entity with en label, P577 date, P264 label, P136 genre
  - `album_entity_year_only.json` — album entity with P577 year-only precision
  - `track_entity.json` — track entity with en label, P2047 duration, P361 part-of
  - `mini_dump_with_labels.json.gz` — small gzipped dump with artist + album + track entities

### Step 5 — `feat(update): fetch labels for new entities during incremental update`

**Rationale:** When new entities are added during incremental updates, their names are Q-ID placeholders. The update pipeline should also fetch English labels for these new entities and store them in `qid_label`.

**Deliverables:**

- `src/sparql.rs`:
  - Extend the entity fetch response to include the English label (already available in the REST API response's `labels` field)
  - No changes needed if the existing `Entity` model already captures labels

- `src/db/load.rs`:
  - Modify `upsert_entity_inner()` to also upsert into `qid_label` when the entity has an English label:
    - `INSERT OR IGNORE INTO qid_label (qid, label, description) VALUES (?1, ?2, ?3)`
  - When inserting album/track stubs during incremental update, use the English label from `qid_label` if available (instead of the Q-ID placeholder)

- `src/main.rs` — `cmd_update()`:
  - After the SPARQL fetch resolves entity data, extract the English label and store it in `qid_label`
  - This integrates naturally with the existing `upsert_entity_from_json()` flow

- **Tests (unit):**
  - `test_upsert_updates_qid_label` — upserting an entity with English label populates `qid_label`
  - `test_upsert_album_with_label` — album upsert uses English label from qid_label
  - `test_upsert_track_with_label` — track upsert uses English label from qid_label
