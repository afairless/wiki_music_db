# Architecture: wiki_db

## Overview

**wiki_db** is a Rust CLI tool that builds a local DuckDB database of musical acts and artists from the Wikidata entity dump. It streams and filters a ~35 GB gzipped JSON file, extracts music-relevant entities, normalizes them into a relational schema, and stores the result in an embedded DuckDB database — all in a single static binary.

**Audience**: End users who want a fast, offline queryable music database from Wikidata. Maintainers who contribute to the Rust codebase.

---

## Module Map

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            CLI Layer (clap)                               │
│  subcommands: download, bootstrap, update, query, populate, completion    │
│  src/cli/  ──  argument definitions and parsing                          │
└───────────────────────────────┬──────────────────────────────────────────┘
                                │
┌───────────────────────────────▼──────────────────────────────────────────┐
│                           Pipeline Layer                                  │
│                                                                           │
│  src/wikidata/stream.rs          src/extraction.rs                       │
│  ┌─────────────────────┐        ┌──────────────────────┐                 │
│  │ StreamReader         │───────▶│ extract_music_entity │                 │
│  │ (gzip JSON → events) │        │ (FilteredEntity →    │                 │
│  │ + music entity filter│        │  MusicEntity)        │                 │
│  └─────────────────────┘        └──────────┬───────────┘                 │
│                                            │                              │
│  src/parquet_writer.rs                     │                              │
│  ┌──────────────────────────┐              │                              │
│  │ MusicEntityBatchWriter   │◀─────────────┘                              │
│  │ (batch → part-*.parquet) │                                              │
│  └────────────┬─────────────┘                                              │
│               │                                                            │
│  src/db/load.rs              src/db/schema.rs                             │
│  ┌──────────────────────┐   ┌──────────────────────┐                     │
│  │ load_all()            │   │ schema::initialize() │                     │
│  │ (Parquet → DuckDB)    │   │ (CREATE TABLE IF     │                     │
│  │ dependency-ordered    │   │  NOT EXISTS + INDEX) │                     │
│  │ INSERT OR IGNORE      │   └──────────────────────┘                     │
│  └──────────┬────────────┘                                                │
│             │                                                              │
└─────────────┼──────────────────────────────────────────────────────────────┘
              │
┌─────────────▼──────────────────────────────────────────────────────────────┐
│                            Data Layer (DuckDB)                              │
│                                                                             │
│  music.duckdb  ──  embedded DuckDB file                                     │
│                                                                             │
│  Tables:  artist, genre, artist_genre, album, album_artist,                 │
│           album_genre, track, track_album, track_artist,                    │
│           artist_instrument, artist_member_of, sync_state,                  │
│           qid_label, instrument, record_label, schema_version               │
│                                                                             │
│  Indexes:  idx_artist_name, idx_album_name, idx_track_name, idx_genre_name  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Module Responsibilities

| Module | Path | Responsibility | Dependencies |
|---|---|---|---|
| **CLI** | `src/cli/` | Argument definitions for the six subcommands (`download`, `bootstrap`, `update`, `query`, `populate`, `completion`) | `clap` |
| **Config** | `src/config.rs` | Loads `wiki_db.toml`; `expand_tilde()` path expansion; `[download]` / `[update]` subsections | `serde`, `toml` |
| **Wikidata model** | `src/wikidata/model.rs` | Serde structs for Wikidata JSON entities (`Entity`, `Claim`, `Mainsnak`) | `serde`, `serde_json` |
| **Music filter** | `src/wikidata/filter.rs` | `is_music_entity()` — checks claims against Q-ID whitelists | `model` |
| **Streaming parser** | `src/wikidata/stream.rs` | `StreamReader` — gzipped line-by-line JSON parser + filter + counters | `model`, `filter` |
| **Extraction** | `src/extraction.rs` | `extract_music_entity()` — converts `FilteredEntity` → `MusicEntity` + genre label extraction | `model`, `stream` |
| **Parquet writer** | `src/parquet_writer.rs` | `MusicEntityBatchWriter` — batch-writes `MusicEntity` to rotating Parquet files | `extraction`, `parquet`, `arrow` |
| **Label extractor** | `src/label_extractor.rs` | `collect_qid_set()` (Q-IDs referenced in the DB) + `extract_labels_and_claims()` (dump rescan → labels/enrichment Parquet) — Aho-Corasick substring scan | `parquet`, `arrow`, `aho-corasick` |
| **SPARQL client** | `src/sparql.rs` | SPARQL query builder (`build_modified_query`) + HTTP client (async `reqwest` in a Tokio runtime) for `update` | `reqwest`, `tokio` |
| **Schema** | `src/db/schema.rs` | `initialize()` — creates all 16 tables (schema v2), indexes, and seeds `schema_version` | `duckdb` |
| **Loader** | `src/db/load.rs` | `load_all()` (bootstrap Parquet → DuckDB); `load_label_and_enrichment()` + `backfill_all_safe()` (populate backfill via FK-safe temp-table swap) | `duckdb`, `schema` |
| **Queries** | `src/db/query.rs` | Search + detail queries used by `cmd_query` (`search_artist`, `search_album`, `album_tracks`, `genre_artists`, …) | `duckdb` |
| **Error types** | `src/error.rs` | Domain-specific error enum (`thiserror`) | `thiserror`, `duckdb`, `chrono` |
| **Orchestration** | `src/main.rs` | `cmd_download()`, `cmd_bootstrap()`, `cmd_update()`, `cmd_query()`, `cmd_populate()` — wire each pipeline stage + progress bars | all modules |

---

## Data Flow: Bootstrap

```
latest-all.json.gz
  (~35 GB, weekly)
       │
       ▼
┌─────────────────┐
│  StreamReader    │  flate2::GzDecoder + BufReader, line-by-line
│  (ingestion)     │  strips trailing commas, deserializes Entity
└────────┬────────┘
         │ StreamEvent
         ▼
┌─────────────────┐
│  is_music_entity │  checks P106 (occupation) → P31 (group type) → catch-all props
│  (filter)        │  first match wins; records inclusion_reason
└────────┬────────┘
         │ FilteredEntity
         ▼
┌─────────────────┐
│  extract_music_  │  extracts labels, descriptions, dates, genres, instruments,
│  entity          │  member_of, albums, tracks; collects genre Q-IDs into HashSet
│  (transformation)│
└────────┬────────┘
         │ MusicEntity batch
         ▼
┌─────────────────┐
│  MusicEntity     │  writes flat VARCHAR schema to part-NNNNN.parquet
│  BatchWriter     │  rotates every 100K entities; pipe-delimited arrays,
│  (output)        │  JSON arrays for album/track refs
└────────┬────────┘
         │ (second pass for genre labels)
         ▼
┌─────────────────┐
│  extract_genre_  │  rescans dump for genre Q-IDs collected during first pass;
│  labels          │  writes genres.parquet with (id, name)
└────────┬────────┘
         │ part-*.parquet + genres.parquet
         ▼
┌─────────────────┐
│  schema::        │  CREATE TABLE IF NOT EXISTS for all 16 tables + indexes
│  initialize      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  load_all()      │  dependency-ordered INSERT OR IGNORE via DuckDB SQL:
│  (loading)       │  1. genres.parquet → genre
│                  │  2. part-*.parquet → artist (casts VARCHAR dates to DATE)
│                  │  3. string_split + unnest → artist_genre, artist_instrument,
│                  │     artist_member_of
│                  │  4. json_each + json_extract_string → album, album_artist,
│                  │     track, track_artist
└─────────────────┘
         │
         ▼
  music.duckdb
```

---

## Data Flow: Populate

```
              ┌───────────────────────────────┐
              │  collect_qid_set()             │  album/track/artist Q-IDs
              │  (src/label_extractor.rs)      │  currently referenced in the DB
              └───────────────┬───────────────┘
                              │ QidSets
                              ▼
              ┌───────────────────────────────┐
              │  extract_labels_and_claims()   │  re-scans the dump for English
              │  (dump rescan)                 │  labels + enrichment claims
              └───────────────┬───────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │  labels.parquet +              │  intermediate columnar output
              │  enrichment.parquet            │
              └───────────────┬───────────────┘
                              │
                              ▼
              ┌───────────────────────────────┐
              │  load_label_and_enrichment()   │  loads qid_label, instrument,
              │  (src/db/load.rs)              │  record_label + child tables
              └───────────────┬───────────────┘
                              │ calls
                              ▼
              ┌───────────────────────────────┐
              │  backfill_all_safe()           │  FK-safe temp-table swap across
              │  (src/db/load.rs)              │  all seven child tables
              └───────────────┬───────────────┘
                              │
                              ▼
                resolved names/dates/labels
                in album / track / artist_* tables
```

## Data Flow: Update

```
   sync_state.last_sync (key/value row)
        │
        ▼
   build_modified_query(since)     SPARQL query for music entities modified
   (src/sparql.rs)                 since the last sync (replicates the filter)
        │ modified Q-IDs (JSON results)
        ▼
   fetch_entity(qid)               Wikimedia REST API: full entity JSON per Q-ID
   (src/sparql.rs, async reqwest)
        │ Entity
        ▼
   upsert_entity_from_json()       INSERT OR IGNORE / UPDATE into DuckDB
   (src/db/load.rs)
        │
        ▼
   update_sync_state(timestamp)    records the new last-sync timestamp
   (src/db/load.rs)
```

---

## Key Design Decisions

### Decision: Parquet intermediate format between streaming and database

- **Context**: The bootstrap pipeline streams a 35 GB gzipped JSON file and produces a DuckDB database. Writing directly to DuckDB from the streaming parser would couple the extraction and loading phases.
- **Decision**: Write filtered entities to Parquet files first, then load into DuckDB in a separate step.
- **Alternatives considered**:
  - Direct DuckDB insertion during streaming: simpler but lost resumability and decoupling; harder to re-run loading logic independently.
  - JSON intermediate files: larger than Parquet, no schema enforcement.
- **Consequences**:
  - (+) Resumability: `--resume` flag skips already-written Parquet files.
  - (+) Decoupling: Parquet format is self-describing; loading logic can be tested independently.
  - (+) Columnar compression: ~5 GB Parquet vs. ~35 GB gzipped JSON.
  - (-) Extra disk space: both Parquet files and the final database coexist unless `--cleanup-parquet` is used.

### Decision: Flat VARCHAR schema for Parquet (v1)

- **Context**: The normalized DuckDB schema has 16 tables with foreign keys (12 at the time of this decision). Representing this directly in Parquet would require multiple Parquet schemas or nested structures.
- **Decision**: Use a single flat Parquet schema with 12 VARCHAR columns. Arrays are stored as pipe-delimited strings (genres, instruments, member_of) or JSON arrays (albums, tracks). DuckDB unpacks them during loading via `string_split`, `unnest`, and `json_each`.
- **Alternatives considered**:
  - Multiple Parquet schemas (one per table): more normalized but complex writer logic; harder to resume.
  - Nested Arrow structs: not supported by DuckDB's `read_parquet` for the join-table expansion patterns we need.
- **Consequences**:
  - (+) Single writer, single reader glob pattern (`part-*.parquet`).
  - (+) Simple resumability: one file per batch.
  - (-) Pipe-delimited format cannot represent Q-IDs containing `|` (not a concern in practice for Wikidata Q-IDs).
  - (-) JSON columns require DuckDB-level parsing during load (still fast since it happens in SQL).

### Decision: English-only labels

- **Context**: Wikidata entities have multilingual labels. Storing all languages would require a separate `label` table.
- **Decision**: Extract only English labels and descriptions. Store multilingual support as a future schema migration.
- **Consequences**:
  - (+) Simpler schema: `artist.name` is a single nullable TEXT column.
  - (+) Faster queries: no JOIN to a label table.
  - (-) Artists without English labels have NULL names. This is logged at WARN and stored as NULL — never rejected.

### Decision: Music entity filter with hardcoded Q-ID whitelists

- **Context**: The filter must identify music-related entities from millions of Wikidata items.
- **Decision**: Maintain hardcoded lists of Q-IDs for music occupations (19 entries) and music group types (12 entries), plus a catch-all heuristic requiring ≥1 of 4 music-related properties.
- **Alternatives considered**:
  - SPARQL subclass resolution at bootstrap time: more precise but adds an HTTP dependency and latency.
  - Machine learning classifier: overkill for the precision/recall trade-off.
- **Consequences**:
  - (+) Fast: pure string matching, no network I/O during filtering.
  - (+) Deterministic: same dump always produces the same filtered set.
  - (-) Stale Q-ID lists: Wikidata taxonomy evolves; lists should be reviewed every 6 months.
  - (-) No subclass resolution: entities with `P106:Q12345` where Q12345 is a subclass of musician (but not in our list) are missed.

### Decision: Album and track names use Q-ID placeholders (until populate)

- **Context**: The initial bootstrap pass only sees album/track Q-IDs from artist claims (P175, P658). Resolving actual names requires a second pass or SPARQL.
- **Decision**: Seed the `album` and `track` tables with Q-ID placeholders as names. **Superseded** — the populate phase (2026-08) resolves names, dates, labels, and durations from a dump rescan (`populate` subcommand). Kept for historical record.
- **Consequences**:
  - (+) Unblocks the initial load without a third scan of the dump.
  - (-) Album and track names are uninformative (e.g., "Q12345") until `populate` runs.

---

## Database Schema

### Tables (16 total)

```
artist               — core entity: person or group
genre                — genre taxonomy (id, name)
artist_genre         — many-to-many artist ↔ genre
artist_instrument    — many-to-many artist ↔ instrument Q-ID
artist_member_of     — many-to-many person → group
album                — albums, EPs, singles, compilations
album_artist         — many-to-many album ↔ artist (with role)
album_genre          — many-to-many album ↔ genre
track                — individual tracks/songs
track_album          — many-to-many track ↔ album
track_artist         — many-to-many track ↔ artist (with role)
sync_state           — key/value last-sync state for incremental updates (key, value)
qid_label            — resolved English labels by Q-ID (qid, label, description, updated_at)
instrument           — normalized instrument lookup (id, name)
record_label         — normalized record-label lookup (id, name)
schema_version       — migration version tracking (schema v2)
```

> `album.release_date`, `album.record_label`, and `track.duration_seconds` columns exist but are NULL after bootstrap; `populate` fills them in and resolves the Q-ID placeholders in `album.name` / `track.name`.

### Foreign Keys

All join tables (e.g., `artist_genre`, `album_artist`) use composite primary keys and foreign key references to their parent tables. DuckDB enforces foreign keys by default in version 1.x, so invalid references are rejected at INSERT time.

### Indexes

```
idx_artist_name  ON artist(name)
idx_album_name   ON album(name)
idx_track_name   ON track(name)
idx_genre_name   ON genre(name)
```

---

## Filtering Strategy

The filter (`is_music_entity()` in `src/wikidata/filter.rs`) tests each entity's claims in order:

1. **Occupation (P106)** — 19 hardcoded music occupation Q-IDs (musician, composer, singer, pianist, guitarist, conductor, record producer, songwriter, DJ, drummer, bassist, violinist, etc.).
2. **Group type (P31)** — 12 hardcoded music group Q-IDs (musical group, ensemble, rock band, orchestra, boy band, girl group, supergroup, duo, K-pop group, choir, pop group, vocal group).
3. **Catch-all properties** — ≥1 of P1303 (instrument), P175 (performer), P136 (genre), P358 (discography).

First match wins. The match reason is recorded in `artist.inclusion_reason` (e.g., `P106:Q639669`, `P31:Q215380`, `PROP:P1303,P136`).

---

## Error Handling Strategy

| Condition | Behavior |
|---|---|
| Entity has no English label | Logged at WARN; `name` stored as NULL |
| Entity has no English description | Stored as NULL (common, not an error) |
| Date string is unparseable | Logged at WARN with raw value; stored as NULL |
| JSON line is malformed | Logged at WARN; line counter incremented; processing continues |
| Entity has no `id` field | Returned as `Err(EntityMissingId)` |
| Parquet file missing | Propagated as `Error::Io` |
| Foreign key violation during load | Rejected by DuckDB (constraint) |

---

## Limitations

- **English-only labels**: Artists without English labels have NULL names (stored, not rejected). Multilingual support is deferred.
- **Q-ID placeholders until populate**: A fresh bootstrap database shows Q-ID placeholders in `album.name` / `track.name` and NULL dates/labels. Running `populate` resolves them; databases built before the populate phase may still show placeholders until it is run once.
- **Resume limitations**: `--resume` restarts streaming from the beginning of the dump (rather than mid-stream). Only Parquet files are skipped; the genre label extraction second pass is also repeated.
- **No subclass resolution**: The filter uses hardcoded Q-ID lists. Entities with subclass-of-musician occupations that aren't in the list are missed.
- **Single-node**: DuckDB is embedded and file-based. No concurrent access or replication.

## Completed Phases

- [x] Phase 5: Full-text search via DuckDB `fts` extension
- [x] Phase 6: Query subcommand (artist, genre, album, search by name)
- [x] Phase 7: Incremental updates via Wikidata SPARQL + Wikimedia REST API
- [x] Phase 8: CLI polish, CI pipeline, shell completions, `--verbose` / `--quiet` flags, documentation updates
- [x] Phase 9: Populate & name resolution — Q-ID label/claim extraction from the dump, `populate` subcommand, labels/enrichment Parquet loading, FK-safe backfill of names/dates/labels (2026-08)
- [x] Phase 10: FK-safety hardening — enrichment FK guards, FK-safe backfill via temp-table swap, artist child tables in backfill (2026-08)

## Future Work

- [ ] Multilingual label support (separate `label` table)
- [ ] SPARQL-based subclass resolution for filter Q-IDs at bootstrap time
- [ ] Mid-stream resume (save stream position for `--resume`)
- [ ] Record label normalization: the `record_label` lookup table exists, but `album.record_label` is still stored as bare TEXT and is not yet normalized against it (join by P264 Q-IDs)
