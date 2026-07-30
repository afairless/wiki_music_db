# Architecture: wiki_db

## Overview

**wiki_db** is a Rust CLI tool that builds a local DuckDB database of musical acts and artists from the Wikidata entity dump. It streams and filters a ~35 GB gzipped JSON file, extracts music-relevant entities, normalizes them into a relational schema, and stores the result in an embedded DuckDB database — all in a single static binary.

**Audience**: End users who want a fast, offline queryable music database from Wikidata. Maintainers who contribute to the Rust codebase.

---

## Module Map

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            CLI Layer (clap)                               │
│  subcommands: bootstrap, update, query                                    │
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
│           artist_instrument, artist_member_of, schema_version               │
│                                                                             │
│  Indexes:  idx_artist_name, idx_album_name, idx_track_name, idx_genre_name  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Module Responsibilities

| Module | Path | Responsibility | Dependencies |
|---|---|---|---|
| **CLI** | `src/cli/` | Argument definitions for `bootstrap`, `update`, `query` subcommands | `clap` |
| **Wikidata model** | `src/wikidata/model.rs` | Serde structs for Wikidata JSON entities (`Entity`, `Claim`, `Mainsnak`) | `serde`, `serde_json` |
| **Music filter** | `src/wikidata/filter.rs` | `is_music_entity()` — checks claims against Q-ID whitelists | `model` |
| **Streaming parser** | `src/wikidata/stream.rs` | `StreamReader` — gzipped line-by-line JSON parser + filter + counters | `model`, `filter` |
| **Extraction** | `src/extraction.rs` | `extract_music_entity()` — converts `FilteredEntity` → `MusicEntity` + genre label extraction | `model`, `stream` |
| **Parquet writer** | `src/parquet_writer.rs` | `MusicEntityBatchWriter` — batch-writes `MusicEntity` to rotating Parquet files | `extraction`, `parquet`, `arrow` |
| **Schema** | `src/db/schema.rs` | `initialize()` — creates all 12 tables, indexes, and seeds `schema_version` | `duckdb` |
| **Loader** | `src/db/load.rs` | `load_all()` — orchestrates Parquet → DuckDB loading in dependency order | `duckdb`, `schema` |
| **Error types** | `src/error.rs` | Domain-specific error enum (`thiserror`) | `thiserror`, `duckdb`, `chrono` |
| **Orchestration** | `src/main.rs` | `cmd_bootstrap()` — wires the full pipeline: stream → filter → extract → Parquet → DuckDB + progress bars | all modules |

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
│  schema::        │  CREATE TABLE IF NOT EXISTS for all 12 tables + indexes
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

- **Context**: The normalized DuckDB schema has 12 tables with foreign keys. Representing this directly in Parquet would require multiple Parquet schemas or nested structures.
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

### Decision: Album and track names use Q-ID placeholders

- **Context**: The initial bootstrap pass only sees album/track Q-IDs from artist claims (P175, P658). Resolving actual names requires a second pass or SPARQL.
- **Decision**: Seed the `album` and `track` tables with Q-ID placeholders as names. A future phase can resolve actual names.
- **Consequences**:
  - (+) Unblocks the initial load without a third scan of the dump.
  - (-) Album and track names are uninformative (e.g., "Q12345") until a future resolution phase.

---

## Database Schema

### Tables (12 total)

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
schema_version       — migration tracking (currently v1)
```

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
- **No album/track names**: Album and track tables use Q-ID placeholders. Name resolution is deferred.
- **Resume limitations**: `--resume` restarts streaming from the beginning of the dump (rather than mid-stream). Only Parquet files are skipped; the genre label extraction second pass is also repeated.
- **No subclass resolution**: The filter uses hardcoded Q-ID lists. Entities with subclass-of-musician occupations that aren't in the list are missed.
- **Single-node**: DuckDB is embedded and file-based. No concurrent access or replication.

## Completed Phases

- [x] Phase 5: Full-text search via DuckDB `fts` extension
- [x] Phase 6: Query subcommand (artist, genre, album, search by name)
- [x] Phase 7: Incremental updates via Wikidata SPARQL + Wikimedia REST API
- [x] Phase 8: CLI polish, CI pipeline, shell completions, `--verbose` / `--quiet` flags, documentation updates

## Future Work

- [ ] Multilingual label support (separate `label` table)
- [ ] Album and track name resolution (second pass or SPARQL)
- [ ] SPARQL-based subclass resolution for filter Q-IDs at bootstrap time
- [ ] Mid-stream resume (save stream position for `--resume`)
- [ ] Record label reference table (v2: normalize labels via P264 Q-IDs)
