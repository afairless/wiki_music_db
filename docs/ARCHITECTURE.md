# Architecture: wiki_db

## Overview

**wiki_db** is a Rust CLI tool that builds a local DuckDB database of musical acts, albums, and tracks from the Wikidata entity dump. It streams and filters a ~145 GB gzipped JSON file, classifies each entity into a role (agent / album / track), normalizes them into a relational schema, and stores the result in an embedded DuckDB database — all in a single static binary.

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
| **Wikidata model** | `src/wikidata/model.rs` | Serde structs for Wikidata JSON entities (`Entity`, `Claim`, `Mainsnak`, `Sitelink`; quantity `amount`/`unit`) | `serde`, `serde_json` |
| **Music filter** | `src/wikidata/filter.rs` | `classify_entity()` — classifies claims into `EntityRole::Agent` / `Album` / `Track` with documented precedence; `is_music_entity()` kept as a thin wrapper for the update path | `model` |
| **Streaming parser** | `src/wikidata/stream.rs` | `StreamReader` — gzipped line-by-line JSON parser + filter + counters; `FilteredEntity` carries the classified `role` | `model`, `filter` |
| **Extraction** | `src/extraction.rs` | `extract_music_entity()` — converts `FilteredEntity` → `MusicEntity`; role-aware P175 (featured performers) / P361 (parent albums) mapping | `model`, `stream` |
| **Parquet writer** | `src/parquet_writer.rs` | `MusicEntityBatchWriter` — batch-writes `MusicEntity` to rotating Parquet files; `role` and `parents` columns | `extraction`, `parquet`, `arrow` |
| **Label extractor** | `src/label_extractor.rs` | `collect_qid_set()` (Q-IDs referenced in the DB) + `extract_labels_and_claims()` (dump rescan → labels/enrichment Parquet) — Aho-Corasick substring scan; en-label → `enwiki` sitelink-title fallback; P2047 duration from datavalue `amount` | `parquet`, `arrow`, `aho-corasick` |
| **SPARQL client** | `src/sparql.rs` | SPARQL query builder (`build_modified_query`) + HTTP client (async `reqwest` in a Tokio runtime) for `update` | `reqwest`, `tokio` |
| **Schema** | `src/db/schema.rs` | `initialize()` — creates all 16 tables (schema v2), indexes, and seeds `schema_version` | `duckdb` |
| **Loader** | `src/db/load.rs` | `load_all()` (bootstrap Parquet → DuckDB, role-routed: agents → artist tables, album/track works → their tables + junctions); `load_label_and_enrichment()` + `backfill_all_safe()` (populate backfill via FK-safe temp-table swap); `upsert_entity_from_json()` (role-routed incremental updates) | `duckdb`, `schema` |
| **Queries** | `src/db/query.rs` | Search + detail queries used by `cmd_query` (`search_artist`, `search_album`, `album_tracks`, `genre_artists`, …) | `duckdb` |
| **Error types** | `src/error.rs` | Domain-specific error enum (`thiserror`) | `thiserror`, `duckdb`, `chrono` |
| **Orchestration** | `src/main.rs` | `cmd_download()`, `cmd_bootstrap()`, `cmd_update()`, `cmd_query()`, `cmd_populate()` — wire each pipeline stage + progress bars | all modules |

---

## Data Flow: Bootstrap

```
latest-all.json.gz
  (~145 GB, weekly)
       │
       ▼
┌─────────────────┐
│  StreamReader    │  flate2::GzDecoder + BufReader, line-by-line
│  (ingestion)     │  strips trailing commas, deserializes Entity
└────────┬────────┘
         │ StreamEvent
         ▼
┌─────────────────┐
│  classify_entity │  P106 (occupation) → P31 (group) → P31 (album work class)
│  (filter)        │  → P31 (track work class) → catch-all props
│                  │  first match wins; records inclusion_reason + EntityRole
└────────┬────────┘
         │ FilteredEntity (with role)
         ▼
┌─────────────────┐
│  extract_music_  │  extracts labels, descriptions, dates, genres, instruments,
│  entity          │  member_of; role-aware P175 → performer refs, P361 → parents;
│  (transformation)│  collects genre Q-IDs into HashSet
└────────┬────────┘
         │ MusicEntity batch
         ▼
┌─────────────────┐
│  MusicEntity     │  writes flat VARCHAR schema to part-NNNNN.parquet
│  BatchWriter     │  rotates every 100K entities; pipe-delimited arrays;
│  (output)        │  role + parents columns; JSON arrays for performer refs
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
│                  │  2. part-*.parquet → artist (role = 'Agent' only)
│                  │  3. string_split + unnest → artist_genre, artist_instrument,
│                  │     artist_member_of (role = 'Agent' only)
│                  │  4. role = 'Album' → album + album_artist (P175 performers)
│                  │     role = 'Track' → track + track_artist (P175) +
│                  │     track_album (P361 parents)
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
              │  extract_labels_and_claims()   │  re-scans the dump for labels
              │  (dump rescan)                 │  (en → enwiki-sitelink fallback)
              │                               │  + enrichment claims
              │                               │  (P577/P264/P136/P361/P2047)
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
   upsert_entity_from_json()       classify_entity() → route by role:
   (src/db/load.rs)                agent → artist tables + qid_label;
                                   album/track works → their tables +
                                   performer/parent junctions
        │
        ▼
   update_sync_state(timestamp)    records the new last-sync timestamp
   (src/db/load.rs)
```

---

## Key Design Decisions

### Decision: Parquet intermediate format between streaming and database

- **Context**: The bootstrap pipeline streams a 145 GB gzipped JSON file and produces a DuckDB database. Writing directly to DuckDB from the streaming parser would couple the extraction and loading phases.
- **Decision**: Write filtered entities to Parquet files first, then load into DuckDB in a separate step.
- **Alternatives considered**:
  - Direct DuckDB insertion during streaming: simpler but lost resumability and decoupling; harder to re-run loading logic independently.
  - JSON intermediate files: larger than Parquet, no schema enforcement.
- **Consequences**:
  - (+) Resumability: `--resume` flag skips already-written Parquet files.
  - (+) Decoupling: Parquet format is self-describing; loading logic can be tested independently.
  - (+) Columnar compression: ~5 GB Parquet vs. ~145 GB gzipped JSON.
  - (-) Extra disk space: both Parquet files and the final database coexist unless `--cleanup-parquet` is used.

### Decision: Flat VARCHAR schema for Parquet (v1)

- **Context**: The normalized DuckDB schema has 16 tables with foreign keys (12 at the time of this decision). Representing this directly in Parquet would require multiple Parquet schemas or nested structures.
- **Decision**: Use a single flat VARCHAR schema (currently 14 columns). Arrays are stored as pipe-delimited strings (genres, instruments, member_of) or JSON arrays (albums — performer refs; parents — P361 album Q-IDs). DuckDB unpacks them during loading via `string_split`, `unnest`, and `json_each`. The `role` column (added 2026-09) records the entity role so the loader can route rows into the artist vs. album/track tables.
- **Alternatives considered**:
  - Multiple Parquet schemas (one per table): more normalized but complex writer logic; harder to resume.
  - Nested Arrow structs: not supported by DuckDB's `read_parquet` for the join-table expansion patterns we need.
- **Consequences**:
  - (+) Single writer, single reader glob pattern (`part-*.parquet`).
  - (+) Simple resumability: one file per batch.
  - (-) Pipe-delimited format cannot represent Q-IDs containing `|` (not a concern in practice for Wikidata Q-IDs).
  - (-) JSON columns require DuckDB-level parsing during load (still fast since it happens in SQL).

### Decision: English labels with an enwiki-sitelink fallback

- **Context**: Wikidata entities have multilingual labels; roughly 3,930 albums and 8,942 tracks in a dump-wide build lack an English label entirely. Storing all languages would require a separate `label` table.
- **Decision**: Extract English labels with a fallback chain: `en` label → sanitized `enwiki` sitelink title → NULL. Enwiki titles are sanitized before use: `_` → space (`The_Joshua_Tree` → `The Joshua Tree`), whitespace trimmed; `(album)`/`(song)`-style disambiguators are **kept** (name-resolution contract §4 #2). This decision supersedes the earlier English-only decision.
- **Consequences**:
  - (+) Simpler schema: `artist.name`/`album.name`/`track.name` are single nullable TEXT columns.
  - (+) Faster queries: no JOIN to a label table.
  - (+) Covers entities that have a Wikipedia article but no English label, shrinking Q-ID-mirror rows.
  - (-) Names may come from article titles (underscores/redirect names); sanitization is applied but disambiguators remain (accepted by contract).
  - (-) Entities with neither an English label nor an `enwiki` sitelink still get NULL names (logged at WARN, stored as NULL — never rejected).

### Decision: Role classifier with hardcoded Q-ID whitelists

- **Context**: The filter must identify music-related entities from millions of Wikidata items and route each into exactly one role: agent (people/groups), album work, or track work. Previously the catch-all admitted album/track works as artists, which inverted P175 into album stubs (the entity-role inversion fixed 2026-09).
- **Decision**: `classify_entity()` applies precedence: (1) P106 ∈ music occupations → agent; (2) P31 ∈ music group types → agent; (3) P31 ∈ `ALBUM_WORK_CLASS_IDS` → album; (4) P31 ∈ `TRACK_WORK_CLASS_IDS` → track; (5) catch-all properties (≥1 of P1303/P175/P136/P358) → agent **only** if no work-class P31 is present; (6) otherwise excluded. `is_music_entity()` remains as a thin wrapper for the update path. The work-class lists are curated, e.g. album — `Q482994` (album), `Q134556` (single), `Q208569` (studio album), `Q169930` (EP), `Q222910` (compilation), `Q209939` (live), `Q5610543` (demo), `Q963099` (remix), `Q723849` (greatest hits), `Q1892995` (mixtape), `Q217199` (soundtrack), `Q5049564` (cast recording); track — `Q7366` (song), `Q24887304` (instrumental composition), `Q639197` (instrumental music). Singles/EPs live in `album` (v1 schema intent).
- **Alternatives considered**:
  - SPARQL subclass resolution at bootstrap time: more precise but adds an HTTP dependency and latency.
  - Machine learning classifier: overkill for the precision/recall trade-off.
- **Consequences**:
  - (+) Fast: pure string matching, no network I/O during filtering.
  - (+) Deterministic: same dump always produces the same filtered set.
  - (+) Albums/tracks land in their own tables with performer/parent junctions — no more role inversion.
  - (-) Stale Q-ID lists: Wikidata taxonomy evolves; lists should be reviewed every 6 months.
  - (-) No subclass resolution (P279): an album class absent from the curated list silently falls to the catch-all agent path; the class lists must be audited against a golden corpus.

### Decision: Album and track names use Q-ID placeholders (until populate)

- **Context**: The initial bootstrap pass used to see album/track Q-IDs only from artist claims (P175, P658). Resolving actual names required a second pass or SPARQL.
- **Decision**: Seed the `album` and `track` tables with Q-ID placeholders as names. **Superseded twice** — the populate phase (2026-08) resolved names, dates, labels, and durations from a dump rescan; as of the role fix (2026-09) album/track rows are work entities that carry their own names, and bootstrap loads `COALESCE(name, id)`, so only label-less entities keep placeholders. `populate` resolves those via the en-label → enwiki-sitelink fallback. Kept for historical record.
- **Consequences**:
  - (+) Unblocks the initial load without a third scan of the dump.
  - (-) A small residual of label-less albums/tracks (no `en` label, no `enwiki` sitelink) retains Q-ID mirror names even after `populate`.

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

> Album and track rows come from *work* entities: `classify_entity` routes an entity to the `album` tables when its P31 is an album work class and to the `track` tables when P31 is a track work class. Agents (`role = 'Agent'`) populate `artist` and its join tables. Bootstrap names work rows via `COALESCE(name, id)` (the work's own label); `populate` fills `album.release_date`, `album.record_label`, and `track.duration_seconds` (P2047, seconds unit only) and resolves remaining Q-ID placeholder names via the en-label → enwiki-sitelink fallback.

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

`classify_entity()` in `src/wikidata/filter.rs` returns the role + inclusion reason for an entity's claims. `is_music_entity()` wraps it for the update path. The rules run in order, first match wins:

1. **Occupation (P106)** — 19 hardcoded music-occupation Q-IDs (musician, composer, singer, pianist, guitarist, conductor, record producer, songwriter, DJ, drummer, bassist, violinist, etc.) → **agent**
2. **Group type (P31)** — 12 hardcoded music-group Q-IDs (musical group, ensemble, rock band, orchestra, boy band, girl group, supergroup, duo, K-pop group, choir, pop group, vocal group) → **agent**
3. **Album work class (P31)** — `ALBUM_WORK_CLASS_IDS` (album, single, studio album, EP, compilation, live album, demo, remix, greatest hits, mixtape, soundtrack, cast recording) → **album**
4. **Track work class (P31)** — `TRACK_WORK_CLASS_IDS` (song, instrumental composition, instrumental music) → **track**
5. **Catch-all properties** — ≥1 of P1303 (instrument), P175 (performer), P136 (genre), P358 (discography) → **agent, only when no work-class P31 is present** (rules 3–4 win when they match)
6. Otherwise → excluded.

The match reason is recorded (e.g., `P106:Q639669`, `P31:Q215380`, `P31:Q482994`, `PROP:P1303,P136`) and the parsed role travels with the entity through extraction, the Parquet `role` column, and the loader, where it decides which tables receive the row. P106 therefore beats a work-class P31 (documented precedence — an entity that is both an occupation holder and an album work class is an agent); the album/track classes also outrank the catch-all, removing the inversion source where albums/songs previously passed as artists via P175/P136.

### Entity Roles & Data Contracts

The 2026-09 role fix established four data contracts (detail: `2026-09_fix_entity_role_inversion.md`, §4):

1. **Entity roles** — the pipeline distinguishes `agent`, `album`, and `track`. Each matching entity is emitted into exactly one role; `artist` holds agents only. A work's P175 lists its *featured performers* (→ `album_artist` / `track_artist`), and a track's P361 lists its *parent albums* (→ `track_album`).
2. **Name resolution** — label source precedence is `en` label → sanitized `enwiki` sitelink title → NULL. Enwiki sanitization replaces `_` with space and keeps `(album)`/`(song)` disambiguators.
3. **Duration** — `track.duration_seconds` accepts a P2047 amount only when the unit is absent or resolves to seconds (`Q11574`); other/unparseable units → NULL with WARN (log-and-store-NULL, never reject).
4. **Work classes** — curated hardcoded P31 lists (`ALBUM_WORK_CLASS_IDS`, `TRACK_WORK_CLASS_IDS`); subclass resolution (P279) is unsupported, so classes absent from the lists silently fall to the catch-all agent path.

---

## Error Handling Strategy

| Condition | Behavior |
|---|---|
| Entity has no English label / enwiki sitelink | Logged at WARN; `name` stored as NULL |
| Entity has no English description | Stored as NULL (common, not an error) |
| Date string is unparseable | Logged at WARN with raw value; stored as NULL |
| P2047 duration amount missing, non-numeric, or non-second unit | Logged at WARN; `duration_seconds` stored as NULL |
| JSON line is malformed | Logged at WARN; line counter incremented; processing continues |
| Entity has no `id` field | Returned as `Err(EntityMissingId)` |
| Parquet file missing | Propagated as `Error::Io` |
| Foreign key violation during load | Rejected by DuckDB (constraint) |

---

## Limitations

- **Sitelink-fallback labels**: Entities without an English label or `enwiki` sitelink get NULL names (stored, not rejected). Names that come from sitelinks can carry `(album)`/`(song)` disambiguators (kept by contract). Multilingual label support is deferred.
- **Q-ID placeholders until populate**: A fresh bootstrap database shows Q-ID placeholders in `album.name` / `track.name` only for label-less entities, and NULL dates/labels. Running `populate` resolves them (en-label → enwiki-sitelink fallback); databases built before the populate phase may still show placeholders until it is run once.
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
- [x] Phase 11: Entity-role inversion fix & enrichment completion — role classifier (agent/album/track), role-aware extraction/Parquet/loader, P2047 duration from datavalue amount, enwiki-sitelink label fallback, fresh `music-v2.duckdb` build (2026-09)

## Future Work

- [ ] Multilingual label support (separate `label` table)
- [ ] SPARQL-based subclass resolution for filter Q-IDs at bootstrap time
- [ ] Mid-stream resume (save stream position for `--resume`)
- [ ] Record label normalization: the `record_label` lookup table exists, but `album.record_label` is still stored as bare TEXT and is not yet normalized against it (join by P264 Q-IDs)
