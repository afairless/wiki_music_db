# Musical Acts & Artists Database: Design Options

**Date:** 2026-07-24  
**Status:** Research / Options Analysis  
**References:** [2026-07_wiki_db_dump.md](./2026-07_wiki_db_dump.md)

## Research Questions

1. What is the best way (including Wikipedia dump format) to filter Wikipedia entries to only musical acts and artists?
2. What is the best database type to store the information?
3. What is the best way to keep the database updated?

---

## 1. Filtering Wikipedia to Musical Acts & Artists

### Research Findings: Wikidata Model

Wikidata classifies musical entities through a rich hierarchy of **instance-of (P31)** classes and **occupation (P106)** values. This is more precise and maintainable than regex-parsing Wikipedia infoboxes.

#### Person-Level Identification (via P106 occupation)

| Q-ID | Label | Scope |
|------|-------|-------|
| Q639669 | musician | Top-level: any person who creates or performs music |
| Q36834 | composer | Writes music |
| Q177220 | singer | Vocal performer |
| Q488205 | singer-songwriter | Writes and performs own songs |
| Q486748 | pianist | Keyboard instrumentalist |
| Q548274 | guitarist | String instrumentalist |
| Q158852 | conductor | Orchestra/choral director |
| Q15981151 | music artist | Broad category for modern music performers |
| Q183945 | record producer | Produces recordings |
| Q753110 | songwriter | Writes songs (lyrics and/or music) |
| Q1280273 | instrumentalist | Plays a musical instrument |
| Q1086813 | jazz musician | Sub-genre-specific musician |
| Q2252262 | rapper | Hip-hop vocal performer |
| Q2865816 | DJ | Disc jockey / electronic music performer |

**Filtering strategy for persons:** Stream the Wikidata JSON dump and check `claims.P106` for any occupation value that is a subclass of `Q639669` (musician) or `Q36834` (composer). In practice, checking a well-curated list of ~25-50 Q-IDs covers the vast majority of musicians.

#### Group-Level Identification (via P31 instance-of)

| Q-ID | Label | Scope |
|------|-------|-------|
| Q215380 | musical group | Top-level: any organized group creating music |
| Q2088357 | musical ensemble | Broader: any ensemble performing music |
| Q5741069 | rock band | Genre-specific group |
| Q42998 | orchestra | Classical orchestra |
| Q1146754 | boy band | Pop vocal group |
| Q6185547 | girl group | Female pop vocal group |
| Q2151147 | supergroup | Established musicians forming a new band |
| Q1229826 | musical duo | Two-person group |
| Q114114601 | K-pop group | Genre/culture-specific group |

**Filtering strategy for groups:** Check `claims.P31` for values in the musical group hierarchy under `Q215380` and `Q2088357`.

#### Alternative: Property-Based Heuristic

An entity is likely music-relevant if it has any of these properties:

- P1303 (instrument) — the person plays an instrument
- P175 (performer) — the entity performed on recordings (inverse usage)
- P136 (genre) — when used as a main value on a person/group (not just on works)
- P358 (discography) — has a discography page

This works well as a **secondary catch-all** after the primary occupation/instance-of filter.

#### Wikidata Statistics (Estimated for Music Domain)

- Total Wikidata entities: ~110 million
- Entities with P106 = Q639669 (musician) or subclasses: ~500K-1M
- Entities with P31 = Q215380 (musical group) or subclasses: ~200K-500K
- After filtering to music-only, the data is **~1-2% of the full dump**

### Research Findings: DBpedia Model

DBpedia maps Wikipedia infoboxes to a simpler ontology:

- `dbo:MusicalArtist` — class for individual musicians (solo performers)
- `dbo:Band` — class for musical groups
- Both are subclasses of `dbo:Artist` → `dbo:Person` / `dbo:Organisation`
- Properties include: `dbo:genre`, `dbo:instrument`, `dbo:associatedBand`, `dbo:associatedMusicalArtist`, `dbo:recordLabel`, `dbo:album`

DBpedia also provides **pre-extracted subsets**:

- `mappingbased_objects_en.ttl` — entity-to-entity links (e.g., artist → genre)
- `mappingbased_literals_en.ttl` — entity-to-literal links (e.g., artist → birth date)
- `instance_types_en.ttl` — entity type assignments
- File sizes: ~2-5 GB compressed total for the relevant subsets

**Filtering strategy for DBpedia:** Filter by `rdf:type dbo:MusicalArtist` or `rdf:type dbo:Band` in the instance-types file, or download the pre-filtered `persondata_en.ttl` subset.

---

## 2. Database Type Comparison

| Criterion | SQLite | DuckDB | PostgreSQL | Neo4j (Graph DB) |
|-----------|--------|--------|------------|------------------|
| **Storage model** | Row-based (B-tree) | Columnar (vectorized) | Row-based (heap) | Graph (nodes+edges) |
| **Embedded / serverless** | Yes; single file | Yes; single file | No; client-server | No; client-server |
| **Schema flexibility** | Rigid | Rigid; nested types | Rigid; JSONB columns | Schema-optional |
| **Graph traversal** | JOINs only | JOINs; efficient for moderate rows | JOINs + recursive CTEs | Native graph traversal |
| **Full-text search** | FTS5 extension | `fts` extension | Built-in `tsvector`/`tsquery` | Apache Lucene integration |
| **Query examples that matter** |||||
| "Find all artists in genre X" | 2-table JOIN | 2-table JOIN | 2-table JOIN | 1-hop traversal |
| "Find all albums by artist Y" | 2-table JOIN | 2-table JOIN | 2-table JOIN | 1-hop traversal |
| "What genres does artist Z work in?" | 2-table JOIN | 2-table JOIN | 2-table JOIN | 1-hop traversal |
| "Find songs on album W" | 2-3 table JOINs | 2-3 table JOINs | 2-3 table JOINs | 1-2 hop traversal |
| "Artists who played with artist V" | 3+ JOINs | 3+ JOINs | Recursive CTE | 2-hop traversal (trivial) |
| **Setup complexity** | Minimal; `pip install` | Minimal; `pip install duckdb` | Medium; server setup | High; JVM + server |
| **Operational cost** | Zero (embedded) | Zero (embedded) | Low (server process) | High (JVM memory) |
| **Concurrent writes** | Single-writer (WAL) | Single-writer (optimistic) | Excellent (MVCC) | Good |
| **Analytics / aggregation** | Moderate | Excellent (columnar, vectorized) | Good | Poor (traversal-focused) |
| **Data import path** | wd2sql automates | Direct Parquet/CSV/JSON read; Python-native | Custom ETL pipeline | Custom ETL + Cypher import |
| **Python integration** | Good (`sqlite3`) | First-class (native integration) | Good (`psycopg2`) | Good (`neo4j` driver) |
| **Disk size (est. music only)** | ~200-500 MB | ~150-400 MB (columnar compression) | ~300-800 MB | ~500 MB-1.5 GB |

### Key Insight: Our Query Pattern

The required queries are:

1. Artist name → genres, discography, song titles
2. Genre → associated artists
3. Album name → associated artists, song titles

These are all **1-2 hop traversals** — well within the comfortable range of any relational database with proper indexing. A graph database would be overkill.

### DuckDB vs PostgreSQL for Single-User / Single-Node

Given the constraint of **single user, single node**, DuckDB has compelling advantages:

| Factor | DuckDB | PostgreSQL | Winner |
|--------|--------|------------|--------|
| **Setup** | `pip install duckdb` — zero config | Install server, initdb, create user, configure | DuckDB |
| **Deployment** | Single `.duckdb` file; portable | Server process must be running | DuckDB |
| **Backup** | Copy the `.duckdb` file | `pg_dump` or WAL archiving | DuckDB |
| **Concurrent reads** | Multiple readers via same process | Excellent (MVCC) | PostgreSQL |
| **Point lookups** (name → artist) | Good (indexed, but columnar is not ideal) | Excellent (row-store, B-tree) | PostgreSQL |
| **Analytical queries** (genre stats, aggregation) | Excellent (columnar, vectorized) | Good | DuckDB |
| **ETL pipeline** | Read JSON/Parquet/CSV directly; transform in SQL | Requires external loader or `COPY` | DuckDB |
| **Full-text search** | `fts` extension works but is newer | `tsvector` is mature and battle-tested | PostgreSQL |
| **Maturity / stability** | ~5 years; rapid development | ~30 years; extremely stable | PostgreSQL |
| **Ecosystem** | Growing fast; Python/R/Node SDKs | Vast; every language, tool, and cloud | PostgreSQL |

**Verdict:** For this specific use case — single user, single node, read-heavy, a few million rows — **DuckDB is the better choice**. The setup simplicity and embedded deployment outweigh PostgreSQL's advantages in concurrent access and full-text search maturity. The columnar storage also compresses genre/discography data well, and the ability to query Parquet/JSON files directly simplifies the ETL pipeline dramatically.

---

## 3. Update Strategies

### Strategy A: Periodic Full Re-dump

- Wikidata publishes **weekly** JSON dumps at `https://dumps.wikimedia.org/wikidatawiki/entities/`
- Download the full dump, re-filter, rebuild the database from scratch
- **Pros:** Simple, always consistent, no drift
- **Cons:** 130 GB compressed per download; heavy CPU/IO for rebuilding; weekly latency

### Strategy B: SPARQL Incremental Updates (Recommended for Wikidata)

- Query the Wikidata SPARQL endpoint (`https://query.wikidata.org/sparql`) for entities modified since the last sync
- Example query to find recently modified musicians:

```sparql
SELECT ?item WHERE {
  ?item wdt:P106 wd:Q639669 .       # Occupation: Musician
  ?item schema:dateModified ?date .
  FILTER(?date > "2026-07-17T00:00:00Z"^^xsd:dateTime)
}
```

- After getting the changed Q-IDs, fetch full entity data via the Wikidata REST API or by re-extracting from the next weekly dump
- **Pros:** Efficient (only changed entities); can run daily or even hourly
- **Cons:** Requires stable internet; SPARQL endpoint has rate limits (~5 concurrent queries); may miss deletions
- **Rate limits:** Wikidata Query Service allows public access; heavy users should set a User-Agent header and respect rate limits

### Strategy C: Wikimedia EventStream (Real-time)

- Subscribe to the Wikimedia Recent Changes stream at `https://stream.wikimedia.org/v2/stream/recentchange`
- Filter for Wikidata edits (`wiki: "wikidatawiki"`) affecting music-related Q-IDs
- **Pros:** Real-time updates; no polling
- **Cons:** Complex to set up; requires persistent connection; need to filter noise

### Strategy D: DBpedia Live

- DBpedia Live provides continuous RDF updates from Wikipedia edits
- **Pros:** Designed for incremental updates
- **Cons:** DBpedia-specific; less frequent than Wikidata updates; DBpedia ontology coverage is narrower

### Strategy E: Hybrid (Bootstrap + Incremental)

1. **Bootstrap:** Download full Wikidata JSON dump once, build complete database
2. **Incremental:** Run weekly SPARQL queries for changed entities, patch the database
3. **Periodic full rebuild:** Every 3-6 months, rebuild from fresh dump to correct any drift

**Recommended approach:** Strategy E provides the best balance of freshness, cost, and correctness.

---

## 4. Proposed Design Options

### Option A: Wikidata JSON → SQLite via wd2sql (Simplest)

**Pipeline:**

1. Download `latest-all.json.gz` (weekly, 130 GB compressed)
2. Use [wd2sql](https://github.com/) (Rust tool) to automatically convert to indexed SQLite
3. Post-process: create application views/queries for music-specific lookups

**Schema (auto-generated by wd2sql, then queried via views):**

```
entities(id, type, data_json)
statements(subject, property, object, qualifiers_json)
labels(entity_id, language, text)
descriptions(entity_id, language, text)
```

**Pros:**

- Minimal development: wd2sql handles 90% of the work
- SQLite is zero-config, portable, embeddable
- Rust-based tool is fast and memory-efficient
- Good for prototyping and small-to-medium scale

**Cons:**

- SQLite not ideal for concurrent read/write access
- Full-text search requires FTS5 setup
- Schema is generic (not domain-optimized); queries may be verbose
- 130 GB download per week is expensive

**Best for:** Rapid prototyping, single-user tools, embedded applications

---

### Option B: Custom Wikidata JSON → PostgreSQL Pipeline (Production-Ready)

**Pipeline:**

1. Stream `latest-all.json.gz` line-by-line in Python
2. Filter: keep only entities where P106 ∈ music occupations OR P31 ∈ music group classes
3. Extract domain-specific fields into a normalized schema
4. Bulk-insert into PostgreSQL with appropriate indexes

**Target Schema:**

```sql
-- Core entities
artist (id TEXT PK, name TEXT, description TEXT, birth_date DATE, death_date DATE, 
        artist_type TEXT)  -- 'person' or 'group'

-- Genres (P136)
genre (id TEXT PK, name TEXT)
artist_genre (artist_id TEXT FK, genre_id TEXT FK)

-- Discography (P358) / Albums (P31=Q482994)
album (id TEXT PK, name TEXT, release_date DATE, record_label TEXT)
album_artist (album_id TEXT FK, artist_id TEXT FK, role TEXT)  -- 'performer' via P175
album_genre (album_id TEXT FK, genre_id TEXT FK)

-- Tracks (P31=Q7302866)
track (id TEXT PK, name TEXT, duration_seconds INT)
track_album (track_id TEXT FK, album_id TEXT FK, track_number INT)
track_artist (track_id TEXT FK, artist_id TEXT FK, role TEXT)

-- Enrichment tables
artist_instrument (artist_id TEXT FK, instrument_id TEXT FK)  -- P1303
artist_member_of (artist_id TEXT FK, group_id TEXT FK)         -- P463
```

**Pros:**

- PostgreSQL is production-grade: concurrent access, robust backup, replication
- Domain-specific schema means clean, fast queries
- Built-in full-text search via `tsvector`
- JSONB columns can store extra Wikidata fields without schema changes
- Indexes on `artist.name`, `album.name`, `genre.name`, plus GIN indexes on text search columns

**Cons:**

- Requires custom ETL development (estimated 500-1000 lines of Python)
- PostgreSQL server needs management
- Schema evolution requires migrations
- 130 GB download per week still expensive

**Best for:** Production applications, web APIs, multi-user access, long-term maintenance

---

### Option C: Wikidata JSON → Graph Database (Neo4j)

**Pipeline:**

1. Stream Wikidata dump, filter music entities
2. Transform entities into nodes and relationships
3. Batch-import using Neo4j's `neo4j-admin import` or Cypher `LOAD CSV`

**Graph Model:**

```
(:Artist {name, birthDate}) -[:PERFORMS_GENRE]-> (:Genre {name})
(:Artist) -[:RELEASED]-> (:Album {name, releaseDate})
(:Album) -[:HAS_TRACK]-> (:Track {name, duration})
(:Track) -[:PERFORMED_BY]-> (:Artist)
(:Artist) -[:MEMBER_OF]-> (:Group {name})
(:Artist) -[:PLAYS_INSTRUMENT]-> (:Instrument {name})
```

**Sample Cypher Queries:**

```cypher
// "Find all artists in genre 'Jazz'"
MATCH (a:Artist)-[:PERFORMS_GENRE]->(:Genre {name: 'jazz'})
RETURN a.name

// "Find all songs on album 'Thriller'"
MATCH (:Album {name: 'Thriller'})-[:HAS_TRACK]->(t:Track)
RETURN t.name

// "Artists who collaborated with Miles Davis" (complex traversal)
MATCH (miles:Artist {name: 'Miles Davis'})<-[:PERFORMED_BY]-(:Album)-[:PERFORMED_BY]->(other:Artist)
WHERE other <> miles
RETURN DISTINCT other.name
```

**Pros:**

- Natural fit for Wikidata's graph structure
- Complex multi-hop queries are clean and performant
- Schema-flexible: easy to add new relationship types
- Excellent for "collaboration" and "influence" graph queries

**Cons:**

- High operational complexity (JVM, memory management)
- Overkill for the stated 1-2 hop query patterns
- Smaller community/tooling ecosystem compared to relational
- Import tooling less mature; custom ETL required
- Higher hosting costs

**Best for:** If the project scope expands to include social graph queries (collaborations, influences, band membership history, "six degrees of Kevin Bacon"-style exploration)

---

### Option D: DBpedia RDF Subsets → PostgreSQL

**Pipeline:**

1. Download DBpedia subset files (~2-5 GB compressed total)
   - `instance_types_en.ttl` — entity types
   - `mappingbased_objects_en.ttl` — relationships (genre, instrument, etc.)
   - `mappingbased_literals_en.ttl` — literal values (names, dates)
   - `persondata_en.ttl` — person-specific data
2. Filter for `dbo:MusicalArtist` and `dbo:Band` types
3. Parse N-Triples into a normalized PostgreSQL schema

**Pros:**

- Much smaller download (2-5 GB vs 130 GB)
- Pre-cleaned data: no need to parse Wikidata's complex JSON structure
- DBpedia ontology is simpler and more consistent
- Good for quick SQL imports

**Cons:**

- DBpedia releases are less frequent (monthly or quarterly, vs Wikidata's weekly)
- Coverage is narrower: only entities with Wikipedia articles and parsed infoboxes
- Data quality depends on Wikipedia infobox consistency (varies by language)
- Ontology is less granular than Wikidata's class hierarchy
- DBpedia.org was unresponsive during research (reliability concern)
- Does not include all the cross-referenced identifiers Wikidata has (MusicBrainz, Discogs, etc.)

**Best for:** Quick start when you want a smaller, simpler dataset and can tolerate less frequent updates

---

### Option E: Hybrid — Wikidata for Entities + DBpedia for Enrichment

**Pipeline:**

1. Use Wikidata JSON dump to identify all music-related entities (widest coverage)
2. Use DBpedia subsets to enrich with cleaner literal values (names, dates, abstracts)
3. Store in PostgreSQL with schema from Option B

**Pros:**

- Best of both worlds: Wikidata's coverage + DBpedia's clean literal data
- DBpedia abstracts provide ready-made descriptions

**Cons:**

- Two data sources to maintain and reconcile
- Entity linking between Wikidata Q-IDs and DBpedia URIs required
- Higher development complexity

**Best for:** When data quality and comprehensiveness are both critical and development resources allow

---

## 5. Recommendation

### Recommended: **Option B (Revised) — Custom Wikidata JSON → DuckDB**

**Rationale:**

| Factor | Assessment |
|--------|------------|
| **Filtering precision** | Wikidata's P106/P31 class hierarchy is the most precise way to identify musical acts. Wikidata has ~25+ specific musician occupations and ~10+ group classes. |
| **Coverage** | Wikidata includes not just Wikipedia-notable entities but also entities from MusicBrainz, Discogs, and other databases — broader coverage than DBpedia |
| **Data freshness** | Weekly dumps + SPARQL incremental updates provide near-real-time freshness |
| **Query power** | DuckDB handles our 1-2 hop query patterns efficiently with proper indexes; full-text search via `fts` extension |
| **Operational simplicity** | DuckDB is embedded — `pip install duckdb`, no server, no config, single file backup |
| **ETL simplicity** | DuckDB reads gzipped JSON/CSV/Parquet directly via `read_json_auto()`, allowing much of the filtering to happen in SQL rather than Python |
| **Single-user fit** | No concurrent-write overhead; columnar compression keeps disk footprint small (~150-400 MB estimated) |

**Why not PostgreSQL?** PostgreSQL's main strengths — concurrent users, MVCC, replication, battle-tested full-text search — are unnecessary for a single-user, single-node, read-heavy music database. DuckDB gives us the same query power with zero operational overhead.

**Why not SQLite?** SQLite works, but DuckDB's columnar storage compresses music metadata better, its vectorized execution handles analytical queries (e.g., "how many artists per genre?") faster, and its native JSON/Parquet ingestion simplifies the ETL significantly.

### Implementation Sketch

```
┌─────────────────────────────────────────────────────────────┐
│                     WEEKLY / BOOTSTRAP                       │
│                                                              │
│  Wikidata Dump     Python stream filter    Parquet/CSV       │
│  (130 GB .json.gz) ──► (keep ~1-2%)   ──► intermediate      │
│                                              │               │
│                                     DuckDB read_parquet()    │
│                                     + SQL transforms         │
│                                              │               │
│                                        music.duckdb          │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                     DAILY / INCREMENTAL                      │
│                                                              │
│  Wikidata SPARQL       Changed Q-IDs       Fetch via REST    │
│  (modified since T) ──► (JSON list)    ──► API or dump      │
│                                                   │          │
│                                     DuckDB UPSERT            │
│                                     (INSERT OR REPLACE)      │
│                                                              │
├─────────────────────────────────────────────────────────────┤
│                     QUERY INTERFACE                          │
│                                                              │
│  Python CLI/API  ──► DuckDB connection ──► SQL queries       │
│                      (artist_search,                         │
│                       genre_artists,                         │
│                       album_tracks, etc.)                    │
│                                                              │
│  The .duckdb file is portable — copy it anywhere.            │
└─────────────────────────────────────────────────────────────┘
```

### Migration Path

1. **Phase 1:** Implement DuckDB pipeline directly — it's simple enough to skip a prototype phase
2. **Phase 2 (optional):** If concurrent access or multi-user needs arise later, migrate to PostgreSQL. The normalized schema is identical, so migration is a straightforward `pg_dump`-style export
3. **Phase 3 (optional):** Add Neo4j as a read replica only if complex graph traversal becomes a core feature

---

## 6. DuckDB: Evaluation for This Use Case

### Why DuckDB Fits Perfectly

DuckDB is an embedded, in-process OLAP database (like "SQLite for analytics"). It is uniquely well-suited here:

1. **Single-user, single-node:** DuckDB's main limitation — no multi-user concurrency — is irrelevant. It thrives in single-user analytical and data-engineering workloads.

2. **Embedded deployment:** The entire database is one file (`music.duckdb`). No server to install, no port to open, no auth to configure. Just `import duckdb` in Python and you have a fully capable SQL database.

3. **ETL-friendly:** DuckDB can query gzipped JSON, CSV, and Parquet files *directly* without loading them first:

   ```python
   import duckdb
   # Read filtered JSON directly into a table
   duckdb.sql("""
     CREATE TABLE artist AS
     SELECT * FROM read_json_auto('music_entities.json.gz')
   """)
   ```

   This means the Python filtering step can write intermediate Parquet files, and DuckDB ingests them via SQL — cleaner than imperative INSERT loops.

4. **Columnar compression:** Genre, instrument, and label data has low cardinality and compresses extremely well column-wise. DuckDB automatically applies lightweight compression, keeping the database small.

5. **Full-text search:** The `fts` extension provides a PRAGMA-driven FTS index:

   ```sql
   INSTALL fts; LOAD fts;
   PRAGMA create_fts_index('artist', 'id', 'name', 'description');
   SELECT * FROM artist WHERE fts_match_artist('jazz');
   ```

6. **Analytics bonus:** Want to answer "what are the top 10 genres by artist count?" or "how has the number of new rock bands changed over decades?" — DuckDB's vectorized, columnar engine handles these aggregations far faster than row-store databases.

### Potential Concerns (and Mitigations)

| Concern | Reality | Mitigation |
|---------|---------|------------|
| "DuckDB is OLAP, not OLTP" | True, but OLTP performance matters at scale. At ~1M artists, point lookups with an index are sub-millisecond. | Use covering indexes on frequently queried columns. |
| "FTS extension is immature" | It's newer than PostgreSQL's `tsvector`, but actively maintained and sufficient for artist/album name search. | If FTS proves insufficient, extract names into a separate SQLite FTS5 index (which DuckDB can attach and query). |
| "No concurrent writes" | Our write path is a single weekly/daily batch process. No concurrent writers. | N/A — single-user constraint obviates this. |
| "DuckDB version churn" | DuckDB releases frequently; API can change between versions. | Pin the version in `requirements.txt` / `pyproject.toml`. |
| "Limited tooling ecosystem" | Fewer GUI tools than PostgreSQL. | Use DuckDB's CLI (`duckdb music.duckdb`) for interactive exploration, or DBeaver (has DuckDB support). |

### DuckDB vs SQLite — Quick Comparison

Both are embedded, single-file databases. Why DuckDB over SQLite?

- **ETL:** DuckDB reads JSON/Parquet/CSV natively; SQLite requires Python loops to INSERT
- **Compression:** DuckDB's columnar storage compresses low-cardinality music metadata ~2-3x better
- **Analytics:** "Top N genres" queries run in columnar-optimized vectorized execution
- **SQL dialect:** DuckDB has a richer SQL dialect (list/struct types, `QUALIFY`, `EXCLUDE`, `COLUMNS()`)

SQLite is still a fine choice (especially via wd2sql for prototyping). But DuckDB matches SQLite's zero-config simplicity while adding analytical power and ETL convenience that directly benefit this project.

---

## 7. Open Questions

1. **Which languages?** Should the database include only English labels or also native-language names? Wikidata has multilingual labels; we should decide which to store.
2. **Depth of discography?** Should we include only albums linked via P358 (discography) and P175 (performer), or should we traverse the full album→track hierarchy?
3. **Hosting?** Where will the database be hosted? A local file (SQLite), a cloud PostgreSQL instance, or a self-managed server?
4. **API or direct query?** Will users query the database directly (SQL) or through a web API / CLI tool?
