# Musical Acts & Artists Database: Rust Implementation Plan

**Date:** 2026-07-24  
**Status:** Implementation Plan  
**Status note (2026-09):** Phases 1–8 are complete and shipped. The populate/name-resolution phase was added after Phase 8 — see [2026-08_populate_names_research.md](./2026-08_populate_names_research.md).
**References:**

- [2026-07_music_db_options.md](./2026-07_music_db_options.md) — research & design options
- [2026-07_wiki_db_dump.md](./2026-07_wiki_db_dump.md) — dump format analysis

---

## 1. Overview

Implement a local, single-user, single-node music database by streaming the Wikidata JSON dump, filtering to musical acts & artists, normalizing into a relational schema, and storing in DuckDB — all in Rust.

The research document ([music_db_options.md](./2026-07_music_db_options.md)) selected **Option B (Revised): Custom Wikidata JSON → DuckDB** as the recommended design. This plan translates that design — originally sketched in Python — into a Rust-native implementation.

### Why Rust

| Factor | Rust | Python |
|--------|------|--------|
| **Stream throughput** | ~35 GB gzipped JSON parsed in ~10–20 min on a modern CPU (single binary, no GIL) | Slower: JSON parsing in pure Python is CPU-bound; `orjson`/`simdjson` help but still contend with GIL for downstream work |
| **Memory** | Fine-grained control; can bound per-entity allocation | Garbage collector can spike on large line batches |
| **Distribution** | Single static binary; `curl | sh` or `cargo install` | Requires Python runtime + venv + dependency install |
| **Correctness** | Type system catches schema mismatches at compile time; `serde` enforces structural contracts | Runtime errors from malformed JSON or unexpected nulls are common |
| **Maintenance** | `cargo update`, recompile; no runtime dependency drift | `pip freeze`, venv rot, OS Python version conflicts |

The trade-off is development speed — Rust takes longer to write initially but produces a faster, more reliable, and easier-to-distribute tool.

---

## 2. Technology Stack

### Core Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| `duckdb` | 1.x | Embedded DuckDB database (wraps libduckdb C API) |
| `serde` + `serde_json` | 1.x | Streaming deserialization of Wikidata JSON lines |
| `clap` | 4.x | CLI argument parsing, subcommands, shell completions |
| `tokio` | 1.x | Async runtime for concurrent HTTP requests (Phase 7 incremental updates only) |
| `reqwest` | 0.12.x | HTTP client for SPARQL endpoint & Wikimedia REST API (Phase 7 only) |

> **Note:** `tokio` and `reqwest` are listed here for completeness but are not used until Phase 7. They can be added to `Cargo.toml` in Phase 1 or deferred to Phase 7.
| `parquet` (arrow-rs) | 53.x | Write filtered entities to Parquet intermediate files |

> **Note:** Verify the latest stable `parquet` crate version at implementation time. Version 53.x is the target; pin a specific minor version in `Cargo.toml`.
| `tracing` + `tracing-subscriber` | 0.1.x / 0.3.x | Structured logging & progress reporting |
| `indicatif` | 0.17.x | Progress bars for long-running dump processing |
| `flate2` | 1.x | Gzip decompression streaming |
| `anyhow` + `thiserror` | 1.x | Error handling |
| `regex` | 1.x | Wikidata Q-ID / URL validation |
| `chrono` | 0.4.x | Date/time handling for birth dates, release dates |

### Dev Tooling

| Tool | Purpose |
|------|---------|
| `cargo` | Build, test, run |
| `clippy` | Linting |
| `rustfmt` | Formatting |
| `cargo-audit` | Security audit of dependencies |

> **Note on `tokio` + DuckDB:** DuckDB operations use a synchronous C API and can block the async runtime. For the current plan scope (sequential CLI commands), this is not an issue. If future versions add concurrent queries (e.g., a `serve` web API alongside incremental updates), wrap DuckDB calls in `tokio::task::spawn_blocking` to avoid starving worker threads.

---

## 3. Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                         CLI (clap)                                │
│  subcommands: bootstrap, update, query                           │
└──────────────────────────┬───────────────────────────────────────┘
                           │
┌──────────────────────────▼───────────────────────────────────────┐
│                     PIPELINE LAYER                                │
│                                                                   │
│  ┌─────────────────┐   ┌──────────────────┐   ┌───────────────┐  │
│  │ Wikidata Stream  │   │ SPARQL Updater   │   │ Query Engine  │  │
│  │ (gzip JSON →     │   │ (incremental     │   │ (SQL Builder  │  │
│  │  filtered        │   │  fetch + upsert) │   │  + FTS)       │  │
│  │  entities +      │   │                  │   │               │  │
│  │  rejected log)   │   │                  │   │               │  │
│  └────────┬─────────┘   └────────┬─────────┘   └───────┬───────┘  │
│           │                      │                     │          │
│  ┌────────▼──────────────────────▼─────────────────────▼───────┐  │
│  │                    DATA LAYER (duckdb crate)                 │  │
│  │  • Normalized tables (artist, album, track, genre, etc.)    │  │
│  │  • FTS index on artist/album names                          │  │
│  │  • Materialized aggregation views (optional)                │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                                                                   │
└───────────────────────────────────────────────────────────────────┘
```

### Data Flow: Bootstrap

```
latest-all.json.gz          Streaming             Parquet            DuckDB
  (~35 GB, weekly)  ──►  entity_filter  ──►  filtered_entities/  ──►  music.duckdb
                         (keep ~1-2%)          (intermediate)          (normalized schema)
                         │                                           
                         ▼                                           
                    rejected.jsonl                                    
                    (bad records log)                                 
  • flate2::GzDecoder    • Check claims.P106   • Batch write      • read_parquet() in SQL
  • serde_json stream    • Check claims.P31     • ~2-5 GB total    • INSERT INTO SELECT
  • line-by-line         • Extract labels,      • Columnar format  • CREATE INDEX
                           descriptions, etc.
```

### Data Flow: Incremental Update

```
Wikidata SPARQL          Changed Q-IDs       Wikimedia REST API      DuckDB
  endpoint         ──►   (JSON list)    ──►   (entity data)     ──►   UPSERT
  (modified since T)                                                  (INSERT OR REPLACE)
```

---

## 4. Database Schema

```sql
-- Schema version tracking (migration support)
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);
INSERT OR IGNORE INTO schema_version (version) VALUES (1);

-- Core entity: person or group
CREATE TABLE artist (
    id          TEXT PRIMARY KEY,      -- Wikidata Q-ID (e.g., "Q2831")
    name        TEXT NOT NULL,         -- English label
    description TEXT,                  -- English description
    artist_type    TEXT NOT NULL,         -- 'person' or 'group'
    inclusion_reason TEXT,              -- why the entity was included
                                        -- (e.g., 'P106:Q639669', 'P31:Q215380',
                                        --  'PROP:P1303,P136' for catch-all)
    birth_date     DATE,                -- NULL for groups
    death_date     DATE,                -- NULL for groups / living persons
    updated_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Genre taxonomy
CREATE TABLE genre (
    id   TEXT PRIMARY KEY,             -- Wikidata Q-ID
    name TEXT NOT NULL                 -- English label
);

-- Many-to-many artist ↔ genre
CREATE TABLE artist_genre (
    artist_id TEXT NOT NULL REFERENCES artist(id),
    genre_id  TEXT NOT NULL REFERENCES genre(id),
    PRIMARY KEY (artist_id, genre_id)
);

-- Albums, EPs, singles, and compilation albums.
-- These are distinguished by P31 subclasses of Q482994 (album).
-- v1 includes all of them (not just Q482994 proper).
CREATE TABLE album (
    id           TEXT PRIMARY KEY,     -- Wikidata Q-ID
    name         TEXT NOT NULL,
    release_date DATE,
    record_label TEXT,                 -- Label name as literal string (v1 simplification;
                                       -- Wikidata models labels via P264 with Q-IDs;
                                       -- a record_label reference table is deferred to v2)
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Many-to-many album ↔ artist
CREATE TABLE album_artist (
    album_id  TEXT NOT NULL REFERENCES album(id),
    artist_id TEXT NOT NULL REFERENCES artist(id),
    role      TEXT,                    -- 'performer', 'producer', etc.
    PRIMARY KEY (album_id, artist_id, role)
);

-- Many-to-many album ↔ genre
CREATE TABLE album_genre (
    album_id TEXT NOT NULL REFERENCES album(id),
    genre_id TEXT NOT NULL REFERENCES genre(id),
    PRIMARY KEY (album_id, genre_id)
);

-- Tracks (songs)
CREATE TABLE track (
    id               TEXT PRIMARY KEY, -- Wikidata Q-ID
    name             TEXT NOT NULL,
    duration_seconds INTEGER,
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Many-to-many track ↔ album
CREATE TABLE track_album (
    track_id     TEXT NOT NULL REFERENCES track(id),
    album_id     TEXT NOT NULL REFERENCES album(id),
    track_number INTEGER,
    PRIMARY KEY (track_id, album_id)
);

-- Many-to-many track ↔ artist
CREATE TABLE track_artist (
    track_id  TEXT NOT NULL REFERENCES track(id),
    artist_id TEXT NOT NULL REFERENCES artist(id),
    role      TEXT,                    -- 'performer', 'composer', etc.
    PRIMARY KEY (track_id, artist_id, role)
);

-- Instruments played by artists
CREATE TABLE artist_instrument (
    artist_id     TEXT NOT NULL REFERENCES artist(id),
    instrument_id TEXT NOT NULL,       -- Wikidata Q-ID
    PRIMARY KEY (artist_id, instrument_id)
);

-- Group membership (person → group)
CREATE TABLE artist_member_of (
    artist_id TEXT NOT NULL REFERENCES artist(id),   -- person
    group_id  TEXT NOT NULL REFERENCES artist(id),   -- group
    PRIMARY KEY (artist_id, group_id)
);

-- Indexes for common query patterns
CREATE INDEX idx_artist_name ON artist(name);
CREATE INDEX idx_album_name ON album(name);
CREATE INDEX idx_track_name ON track(name);
CREATE INDEX idx_genre_name ON genre(name);

-- Full-text search indexes (DuckDB fts extension)
-- Created via PRAGMA after table population:
--   INSTALL fts; LOAD fts;
--   PRAGMA create_fts_index('artist', 'id', 'name', 'description');
--   PRAGMA create_fts_index('album', 'id', 'name');
--   PRAGMA create_fts_index('track', 'id', 'name');
```

---

## 5. Filtering Strategy

Implemented in Rust as a predicate function operating on each deserialized Wikidata entity line.

### Music Occupation Q-IDs (P106)

Filter any entity whose `claims.P106` contains a value that is, or is a subclass of, any of:

> **Maintenance note:** The Q-ID lists below should be reviewed every 6 months against Wikidata's evolving taxonomy. A future enhancement (v2) could fetch subclass-of relationships from the Wikidata SPARQL endpoint at bootstrap time instead of maintaining hardcoded lists.

| Q-ID | Label |
|------|-------|
| Q639669 | musician |
| Q36834 | composer |
| Q177220 | singer |
| Q488205 | singer-songwriter |
| Q486748 | pianist |
| Q548274 | guitarist |
| Q158852 | conductor |
| Q15981151 | music artist |
| Q183945 | record producer |
| Q753110 | songwriter |
| Q1280273 | instrumentalist |
| Q1086813 | jazz musician |
| Q2252262 | rapper |
| Q2865816 | DJ |
| Q1075651 | drummer |
| Q855091 | organist |
| Q105543609 | electronic musician |
| Q793509 | bassist |
| Q2551014 | violinist |

### Music Group Q-IDs (P31)

Filter any entity whose `claims.P31` contains:

| Q-ID | Label |
|------|-------|
| Q215380 | musical group |
| Q2088357 | musical ensemble |
| Q5741069 | rock band |
| Q42998 | orchestra |
| Q1146754 | boy band |
| Q6185547 | girl group |
| Q2151147 | supergroup |
| Q1229826 | musical duo |
| Q114114601 | K-pop group |
| Q2539346 | choir |
| Q108421069 | pop group |
| Q1196129 | vocal group |

### Catch-All Heuristic (Property-Based)

An entity passing neither filter above is still included if it has **at least 1 of 4** of these properties:

- **P1303** (instrument) — plays an instrument
- **P175** (performer) — performed on recordings
- **P136** (genre) — genre association
- **P358** (discography) — has a discography

> **Note:** Requiring at least 1 property casts a wide net, prioritizing recall over precision. This may produce false positives (e.g., a non-musician with a single incidental music property). The threshold can be raised after measuring the false-positive rate on a sample of the dump.
>
> Every included entity records its inclusion reason in the `artist.inclusion_reason` column so queries can filter by match quality.

### Rust Implementation Sketch

```rust
/// Set of Q-IDs considered "music occupations" (P106).
const MUSIC_OCCUPATION_IDS: &[&str] = &[
    "Q639669", "Q36834", "Q177220", "Q488205", "Q486748",
    "Q548274", "Q158852", "Q15981151", "Q183945", "Q753110",
    "Q1280273", "Q1086813", "Q2252262", "Q2865816", "Q1075651",
    "Q855091", "Q105543609", "Q793509", "Q2551014",
];

/// Set of Q-IDs considered "music groups" (P31).
const MUSIC_GROUP_IDS: &[&str] = &[
    "Q215380", "Q2088357", "Q5741069", "Q42998", "Q1146754",
    "Q6185547", "Q2151147", "Q1229826", "Q114114601", "Q2539346",
    "Q108421069", "Q1196129",
];

/// Properties that indicate a music-relevant entity.
const MUSIC_PROPERTIES: &[&str] = &["P1303", "P175", "P136", "P358"];

/// Minimum number of catch-all properties required (1 = wide net, prioritize recall).
const MIN_CATCHALL_PROPERTIES: usize = 1;

fn is_music_entity(claims: &HashMap<String, Vec<Statement>>) -> bool {
    // Check occupation (P106)
    if let Some(stmts) = claims.get("P106") {
        if stmts.iter().any(|s| music_occupation_target(s)) {
            return true;
        }
    }
    // Check instance-of (P31)
    if let Some(stmts) = claims.get("P31") {
        if stmts.iter().any(|s| music_group_target(s)) {
            return true;
        }
    }
    // Check catch-all properties: require at least MIN_CATCHALL_PROPERTIES
    let matched: Vec<&str> = MUSIC_PROPERTIES
        .iter()
        .filter(|prop| claims.contains_key(*prop))
        .copied()
        .collect();
    matched.len() >= MIN_CATCHALL_PROPERTIES
}
```

---

## 6. Implementation Phases

Each phase is a self-contained, testable, committable unit of work. Phases are sequential; sub-steps within a phase can sometimes be parallelized.

---

### Phase 1: Project Scaffold & Schema

**Goal:** A compilable Rust project with CLI structure and database schema creation.

1. `cargo init --name music-db` in the project directory; verify `cargo build` succeeds on the empty template
2. Create `.gitignore` with entries: `music.duckdb`, `*.duckdb`, `parquet-dir/`, `.env`, `target/`
3. Set up `Cargo.toml` with all dependencies
4. Configure `clap` CLI with subcommands: `bootstrap`, `update`, `query`
5. Set up `tracing` + `tracing-subscriber` for structured logging
6. Implement database initialization: open/create `music.duckdb`, run `CREATE TABLE IF NOT EXISTS` statements for all tables and indexes from the schema (Section 4), including the `schema_version` table
7. Write integration test: database created, all tables exist, indexes exist, `schema_version` row present

**Deliverable:** `cargo run -- query --help` prints help text; `cargo run -- bootstrap --help` prints help text; `cargo test` passes with a temp database.

---

### Phase 2a: Wikidata Entity Model & Deserialization

**Goal:** Define the `serde` structs for Wikidata JSON entities and implement deserialization with tests.

1. Define `serde` structs for the Wikidata JSON entity model:
   - `Entity { id, labels, descriptions, claims, ... }`
   - `Claim { mainsnak, ... }` — use a **custom `Deserialize` implementation** rather than `serde(untagged)`. The Wikidata claim model has three `snaktype` variants (`value`, `novalue`, `somevalue`) that are ambiguous under untagged deserialization. A custom deserializer captures only `mainsnak.datavalue.value.id` (for Q-ID references) and `mainsnak.datavalue.value.time` (for dates), ignoring qualifiers and references entirely for v1.
2. Write unit tests for:
   - Deserializing a known musician entity (e.g., a tiny hand-crafted JSON fixture) — valid input
   - Deserializing a non-musician entity — valid input, should deserialize without error
   - Error-path tests:
     - Entity with valid JSON but missing `labels.en` → handled gracefully
     - Entity where `P106` exists but `mainsnak` is missing → handled gracefully
     - Date value that isn't valid ISO 8601 → logged and skipped
     - Gracefully handling malformed JSON lines: skip, log reason to `rejected.jsonl`, increment error counter

**Deliverable:** `cargo test` passes; entity deserialization from hand-crafted fixtures works.

---

### Phase 2b: Filter + Streaming Parser

**Goal:** Stream `latest-all.json.gz` line-by-line, apply the music filter, and write a `rejected.jsonl` log.

1. Implement `is_music_entity()` filter with the Q-ID sets from Section 5. Track the specific reason each entity passes the filter (which P106/P31 Q-ID matched, or which catch-all properties fired) so it can be written to the `artist.inclusion_reason` column.
2. Implement the streaming parser:
   - Open gzip file with `flate2::GzDecoder`
   - Wrap in `BufReader`
   - Read lines, skip `[` / `]` delimiters, **strip trailing commas** (`.trim_end_matches(',')`), deserialize each line as `Entity`
   - Filter with `is_music_entity()`
3. Write unit tests for:
   - `is_music_entity()` returning true/false per the **None-One-Many** principle:
     - None: empty claims map → `false`
     - One: single P106=Q639669 claim → `true`; single P31=Q215380 claim → `true`
     - Many: multiple claims across P106, P31, and catch-all properties → `true`; multiple claims with no music IDs → `false`
   - Catch-all with exactly 1 property → `true` (threshold is 1)
   - **Property-based test:** `is_music_entity()` never panics for any valid `HashMap<String, Vec<Statement>>` input
   - Gracefully handling malformed JSON lines: skip, log reason to `rejected.jsonl`, increment error counter

**Deliverable:** `cargo test` passes; can stream a test fixture file and print filtered entity counts with inclusion reasons.

---

### Phase 3a: Parquet Writer & MusicEntity Extraction

**Goal:** Define the flat `MusicEntity` struct, extract fields from filtered Wikidata entities, and batch-write to Parquet files.

1. Define a flat `MusicEntity` struct with all extracted fields and explicit data contracts (see below):

     ```rust
     /// Intermediate representation of a music-relevant Wikidata entity.
     ///
     /// Data contract (transformation → output stage boundary):
     /// - `id`: always present (entities missing a Q-ID are rejected at ingestion)
     /// - `name`: None means the entity has no English label (logged at WARN; stored as NULL)
     /// - `description`: None means no English description (common; stored as NULL, not an error)
     /// - `birth_date`, `death_date`: None means either no date property or an unparseable
     ///   date string (logged at WARN with the raw value; stored as NULL)
     /// - `inclusion_reason`: why the entity passed the music filter (e.g., "P106:Q639669",
     ///   "P31:Q215380", "PROP:P1303,P136") — records the match that triggered inclusion
     /// - All `Vec` fields default to empty (not None) — empty collections mean no data, not missing data
     struct MusicEntity {
         id: String,                    // Wikidata Q-ID
         name: Option<String>,          // English label (None if missing)
         description: Option<String>,   // English description
         artist_type: String,           // "person" or "group"
         inclusion_reason: String,      // e.g., "P106:Q639669", "P31:Q215380", "PROP:P1303,P136"
         birth_date: Option<NaiveDate>,
         death_date: Option<NaiveDate>,
         genres: Vec<String>,           // Q-IDs
         instruments: Vec<String>,      // Q-IDs
         member_of: Vec<String>,        // group Q-IDs (for persons)
         albums: Vec<AlbumRef>,         // album Q-IDs with roles
         tracks: Vec<TrackRef>,         // track Q-IDs with roles
     }
     ```

   - **Genre label collection:** During entity processing, when a claim references a genre Q-ID (P136 or entity P31), collect that Q-ID into a set for deferred label lookup. After the filtering pass, make a second pass over the dump (or iterate through already-seen genre entity IDs) to extract English labels for all referenced genre Q-IDs. These are written as a separate `genres.parquet` file that the loader reads before inserting into the `genre` table.
   - Batch-write `MusicEntity` records to `.parquet` files using `arrow-rs` + `parquet` crate
   - Rotate files every N entities (e.g., 100K) to keep memory bounded
   - **Rejection criteria:** Write an entity to `rejected.jsonl` (with reason) only if: (a) `serde_json` deserialization fails on the line, (b) the entity has no `id` field, or (c) a date string fails `chrono::NaiveDate` parsing after all known formats are attempted. Missing labels, missing descriptions, and missing optional fields are stored as NULL — never rejected.
2. Write unit tests for:
   - Round-tripping the `MusicEntity` struct through Parquet: write → read back → verify all fields match

**Deliverable:** `cargo test` passes; can produce `.parquet` files from a test fixture.

---

### Phase 3b: DuckDB Loader & Bootstrap CLI

**Goal:** Load Parquet files into DuckDB, wire up the `bootstrap` subcommand, and add progress reporting.

1. Implement DuckDB loader:
   - Use `duckdb` crate to `INSERT INTO ... SELECT * FROM read_parquet(...)`
   - **Parquet extension:** The `parquet` extension is built-in since DuckDB 1.0. Verify during the Phase 1 spike; if not available, `INSTALL parquet; LOAD parquet;`
   - Load multiple Parquet files via glob pattern: `read_parquet('parquet-dir/*.parquet')` or iterate files individually
   - Load in dependency order: genres → artists → artist_genre → albums → album_artist → album_genre → tracks → ...
   - Deduplicate: `INSERT OR IGNORE` to handle re-runs
   - **Keep Parquet files by default** for resumability. After successful load, delete intermediate Parquet files only with `--cleanup-parquet` flag
2. Wire up `clap` `bootstrap` subcommand:
   - Arg: `--dump <PATH>` (path to `latest-all.json.gz`)
   - Arg: `--db <PATH>` (path to output `.duckdb`, default `music.duckdb`)
   - Arg: `--parquet-dir <PATH>` (directory for intermediate files; needs ~5 GB free space)
   - Arg: `--cleanup-parquet` (delete intermediate Parquet files after successful load; off by default for resumability)
   - Arg: `--resume` (skip already-written Parquet files to resume interrupted runs)
3. Add progress reporting with `indicatif`:
   - Entity counter with spinner (N entities processed, M filtered, R rejected)
   - Note: byte-proportional progress bars on gzip streams are unreliable due to variable compression ratios
4. Integration test: run on a small (100-entity) gzipped Wikidata fixture, verify row counts in each table. Additional edge-case tests:
   - Run bootstrap twice on the same fixture → verify no duplicate rows (INSERT OR IGNORE)
   - Feed an entity with missing `name` → verify stored as NULL in the artist table, not rejected
   - Interrupt bootstrap mid-streaming, run with `--resume` → verify it picks up from the last completed Parquet file
   - Mock a genre entity without an English label → verify stored with NULL name and logged warning
   - SQL query builders (from Phase 6) with an in-memory DuckDB: test each query function returns expected columns and handles empty results

**Deliverable:** `cargo run -- bootstrap --dump test_data/fixture.json.gz --db /tmp/test.duckdb` populates a valid DuckDB database.

---

### Phase 5: Full-Text Search Setup

**Goal:** Enable text search on artist names, album names, and track names.

1. Add DuckDB `fts` extension loading during database initialization
2. Create FTS indexes via `PRAGMA create_fts_index('artist', 'id', 'name', 'description')` etc.
3. Implement query helper functions:

   ```sql
   SELECT * FROM artist WHERE fts_match_artist(?1);
   SELECT * FROM album WHERE fts_match_album(?1);
   SELECT * FROM track WHERE fts_match_track(?1);
   ```

4. Fallback logic: if `fts` extension fails to install/load during database initialization, set a flag and fall back to `LIKE '%term%'` with a logged warning. Detection is via runtime error handling on `INSTALL fts; LOAD fts;` — if either statement returns an error, the FTS feature is disabled for the session.

**Deliverable:** FTS indexes created; integration test searches for an artist by partial name match.

---

### Phase 6: Query Subcommand

**Goal:** CLI for answering the three core question types.

Implement `clap` `query` subcommand with sub-subcommands:

```
cargo run -- query artist --name "Miles Davis"
cargo run -- query genre --name "Jazz"
cargo run -- query album --name "Kind of Blue"
```

1. `artist` subcommand:
   - `--name <NAME>` — full-text search by name
   - Output: name, description, birth_date, artist_type, genres, albums, instruments
2. `genre` subcommand:
   - `--name <NAME>` — find genre by name
   - Output: genre info + paginated list of associated artists
   - `--limit <N>` / `--offset <N>` for pagination
3. `album` subcommand:
   - `--name <NAME>` — full-text search by album name
   - Output: album info, artists, genres, track listing
4. `search` subcommand (bonus):
   - `--term <TERM>` — search across artists, albums, and tracks simultaneously
5. Formatted output: colored terminal output with `colored` or `owo-colors` crate
6. Write integration tests following the **None-One-Many** principle:
   - None: query against an empty database → "no results" message, exit code 0
   - One: query for a single matching artist/album/genre → correct fields displayed
   - Many: query returning multiple results → pagination works (`--limit` / `--offset`)
   - FTS fallback: verify `LIKE` fallback produces results when FTS is disabled

**Deliverable:** All query subcommands work against a populated database.

---

### Phase 7: Incremental Updates

**Goal:** Daily/weekly updates via Wikidata SPARQL endpoint without re-downloading the full dump.

1. Implement SPARQL query builder:
   - Query: "all music-related entities modified since timestamp T"
   - Use the `schema:dateModified` property
   - Respect rate limits (User-Agent header, 1 request/second, exponential backoff)
   - **Timeout handling:** Wikidata's public Query Service has a 60-second timeout. Use paginated queries (`LIMIT 10000 OFFSET ...` in a loop) to avoid hitting the timeout on broad result sets. If a page fails, log the offset and retry with exponential backoff.
2. Implement entity fetcher via Wikimedia REST API:
   - `GET https://www.wikidata.org/wiki/Special:EntityData/{Q-ID}.json`
   - Parse response, extract the same fields as the streaming parser
3. Implement DuckDB upsert logic:
   - `INSERT OR REPLACE` for changed entities
   - Deletions: SPARQL cannot directly detect entities that stopped matching music criteria. Documented limitation — stale rows are reconciled during periodic full re-bootstraps (every 3–6 months per Strategy E). Alternatively, a reconciliation step can diff the local artist Q-IDs against the SPARQL result set.
4. Store sync state: `sync_state` table with `last_sync_timestamp`
5. Wire up `clap` `update` subcommand:
   - `--since <TIMESTAMP>` (optional, default: last sync)
   - `--dry-run` (print changes without writing)
6. Write integration test: mock SPARQL response, verify upsert behavior, verify sync state updates

**Deliverable:** `cargo run -- update` fetches recent changes and merges them into the database.

---

### Phase 8: Polish & Distribution

1. Error handling polish: replace any remaining `unwrap()` with proper `anyhow` contexts
2. Add `--verbose` / `--quiet` flags to control log level
3. Add `--version` flag
4. Shell completion generation: `clap_complete`
5. README with installation instructions and usage examples
6. `cargo build --release` benchmark on a real Wikidata dump slice
7. CI pipeline: `.github/workflows/ci.yml` — `cargo test`, `cargo clippy`, `cargo fmt --check`

---

## 7. Project File Structure

```
wiki_db/
├── Cargo.toml
├── Cargo.lock
├── README.md
├── .github/
│   └── workflows/
│       └── ci.yml
├── .gitignore              # Excludes music.duckdb, *.duckdb, parquet-dir/, .env
├── src/
│   ├── main.rs              # CLI entry point, clap setup
│   ├── cli/
│   │   ├── mod.rs
│   │   ├── bootstrap.rs     # bootstrap subcommand
│   │   ├── update.rs        # update subcommand
│   │   └── query.rs         # query subcommand
│   ├── wikidata/
│   │   ├── mod.rs
│   │   ├── model.rs         # serde structs for Wikidata JSON
│   │   ├── filter.rs        # is_music_entity() + Q-ID constants
│   │   └── stream.rs        # streaming parser (gzip → Entity iterator)
│   ├── db/
│   │   ├── mod.rs
│   │   ├── schema.rs        # CREATE TABLE statements, initialization
│   │   ├── load.rs          # Parquet → DuckDB loading
│   │   └── query.rs         # SQL query builders for each query type
│   ├── parquet_writer.rs    # Batch-write MusicEntity to .parquet files
│   ├── sparql.rs            # SPARQL query builder & HTTP client
│   └── error.rs             # Error types (thiserror)
├── tests/
│   ├── integration/
│   │   ├── mod.rs
│   │   ├── bootstrap_test.rs
│   │   ├── query_test.rs
│   │   └── update_test.rs
│   ├── property/
│   │   └── filter_tests.rs      # Property-based tests for is_music_entity()
│   └── fixtures/
│       ├── musician_entity.json    # Hand-crafted Wikidata entity (musician)
│       ├── band_entity.json        # Hand-crafted Wikidata entity (band)
│       ├── non_musician_entity.json # Non-music entity for filter tests
│       ├── malformed_entity.json   # Malformed entity for error-path tests
│       └── mini_dump.json.gz       # ~100-entity gzipped dump for integration tests
├── docs/
│   ├── research/
│   │   ├── 2026-07_wiki_db_dump.md
│   │   ├── 2026-07_music_db_options.md
│   │   └── 2026-07_music_db_rust_plan.md   # This document
│   └── ARCHITECTURE.md             # Architecture documentation (after implementation)
└── scripts/
    └── download_dump.sh            # Helper to download latest Wikidata dump
```

---

## 8. Risk Assessment

| Risk | Impact | Likelihood | Mitigation |
|------|--------|------------|------------|
| `duckdb-rs` crate is immature or missing features (e.g., `fts` extension loading, `read_parquet()`) | High — may need to switch to `rusqlite` | Medium | Spike the DuckDB integration in Phase 1; if it doesn't work, pivot to SQLite via `rusqlite` (which is battle-tested). The schema is identical either way. |
| Wikidata JSON dump format changes between releases | Medium — parser could break | Low | Wikidata's JSON format is versioned and stable. Pin to a specific dump date; add a format-version check on bootstrap. |
| ~35 GB gzip streaming overwhelms memory | Medium — OOM on constrained systems | Low | Line-by-line streaming with `BufReader` + `flate2::GzDecoder` keeps memory bounded. Batch Parquet writes with rotation prevent accumulation. |
| SPARQL endpoint rate-limiting or downtime blocks incremental updates | Low — updates delayed | Medium | Exponential backoff + retry. Fall back to re-downloading the weekly dump if SPARQL is unavailable for >24h. |
| Rust compile times slow iteration | Low — developer friction | High | Use `cargo check` for fast feedback; split code into small crates only if compile times exceed ~30s. Not expected with this crate count. |

---

## 9. SQLite Fallback Plan

If `duckdb-rs` proves insufficient, pivot to `rusqlite` (SQLite). The schema, query patterns, and pipeline architecture are identical. The differences:

| Area | DuckDB (plan) | SQLite (fallback) |
|------|--------------|-------------------|
| Crate | `duckdb` | `rusqlite` with `bundled` feature |
| FTS | `fts` extension via PRAGMA | FTS5 via `CREATE VIRTUAL TABLE ... USING fts5(...)` |
| Parquet ingestion | `read_parquet()` in SQL | Rust parses Parquet → iterates rows → `INSERT` via prepared statements |
| Aggregation performance | Vectorized (faster) | Row-based (still fast at this scale) |
| Compression | Columnar (automatic) | Page-level (still good with proper schema) |

The migration is a one-day refactor limited to `src/db/load.rs` and `src/db/schema.rs`.

---

## 10. Resolved Design Decisions

The following decisions were open during planning and have been resolved:

1. **Languages:** English-only. The schema uses a single `name` column on `artist`. A multilingual `label` table is deferred to a future schema migration.

2. **Discography depth:** Store all album types — albums, EPs, singles, and compilation albums. The album filter includes all P31 subclasses of Q482994 (album), not just Q482994 proper. The `album` table (Section 4) reflects this.

3. **Hosting / distribution:** No pre-built database downloads. Users run `bootstrap` locally. Evaluate demand after v1 release.

4. **Write-audit logging:** `updated_at` columns only. A change history table is deferred to a future schema migration.

---

## 11. Estimated Effort

| Phase | Description | Estimated Hours |
|-------|-------------|-----------------|
| 1 | Project scaffold & schema | 2–3 |
| 2a | Wikidata entity model & deserialization | 2–3 |
| 2b | Filter + streaming parser | 2–3 |
| 3a | Parquet writer & MusicEntity extraction | 2–3 |
| 3b | DuckDB loader & bootstrap CLI | 2–3 |
| 5 | Full-text search setup | 2–3 |
| 6 | Query subcommand | 3–5 |
| 7 | Incremental updates | 4–6 |
| 8 | Polish & distribution | 2–3 |
| **Total** | | **21–32 hours** |

These are conservative estimates for a developer familiar with Rust. The largest unknowns are the Wikidata JSON model deserialization (nested, optional, highly variable structure) and the `duckdb-rs` crate ergonomics.

---

## 12. Security Considerations

### Untrusted Input

The Wikidata JSON dump is treated as **untrusted external input**. Every field extracted from entity JSON is validated before insertion:

- **Null handling:** Missing labels, descriptions, and dates are stored as `NULL` — never filled with defaults that could mask data issues.
- **Type validation:** Date strings are parsed with `chrono::NaiveDate` and rejected if invalid.
- **SQL injection prevention:** All database operations use DuckDB prepared statements (parameterized queries via the `duckdb` crate). Entity data never participates in SQL string construction, so malicious entity names or descriptions cannot cause SQL injection.

### Rate Limiting & Identification

The SPARQL endpoint and Wikimedia REST API require:

- A proper `User-Agent` header identifying the tool (e.g., `music-db/1.0 (user@example.com)`). This is both a rate-limiting convention and an identification best practice per Wikimedia's API guidelines.
- Respect for rate limits: 1 request/second with exponential backoff.

### Secrets & Configuration

- No API keys, passwords, or credentials are hardcoded. SPARQL and Wikimedia REST API access is public and unauthenticated.
- Ensure `.gitignore` excludes `music.duckdb`, `*.duckdb`, `parquet-dir/`, and `.env` files before the first commit.

### Dependency Auditing

Run `cargo audit` before each release to verify no known vulnerabilities exist in the dependency tree.
