# Implementation Plan: Phase 7 — Incremental Updates

Source: `docs/research/2026-07_music_db_rust_plan.md` (Section 6, Phase 7)

**Preceding phases (completed):** 1 (scaffold & schema), 2a (entity model & deserialization),
2b (filter + streaming parser), 3a (Parquet writer & MusicEntity extraction),
3b (DuckDB loader & bootstrap CLI), 5 (full-text search setup), 6 (query subcommand).

**What already exists (Phase 7 scaffolding):**

- `src/cli/update.rs` — CLI argument definitions for `--since` and `--dry-run`
- `src/config.rs` — `UpdateConfig` with `since` and `dry_run` fields
- `src/error.rs` — `Error::Http(String)` variant
- `Cargo.toml` — `tokio` and `reqwest` already in `[dependencies]`
- `src/main.rs` — `cmd_update()` stub that logs "not yet implemented"

**What is still missing:**

- `sync_state` table in the database schema for tracking the last sync timestamp
- SPARQL query builder for finding modified music entities
- SPARQL HTTP client for querying the Wikidata SPARQL endpoint (with pagination, rate limiting, exponential backoff)
- Wikimedia REST API entity fetcher for fetching individual entity data
- DuckDB upsert logic for single-entity INSERT OR REPLACE
- `cmd_update()` wiring in `src/main.rs` with progress reporting
- Integration tests with mocked HTTP responses

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat(db): add sync_state table for incremental update tracking` | Sync state table | `src/db/schema.rs` — add `sync_state` table, `get_last_sync_timestamp()`, `update_sync_timestamp()` | Unit |
| 2 | `feat(update): implement SPARQL query builder and HTTP client` | SPARQL client | `src/sparql.rs` — `build_modified_query()`, `SparqlClient` struct, `query_modified_entities()` with pagination and rate limiting | Unit |
| 3 | `feat(update): implement REST API entity fetcher for individual entities` | Entity fetcher | `src/sparql.rs` — `fetch_entity()` via Wikimedia REST API, deserialize into `Entity`, `EntityResponse` wrapper | Unit |
| 4 | `feat(update): implement DuckDB upsert logic for single entity updates` | Upsert logic | `src/db/load.rs` — `upsert_entity()`, `upsert_entity_from_json()` with INSERT OR REPLACE and sync state update | Unit |
| 5 | `feat(update): wire up cmd_update with CLI orchestration` | Update command | `src/main.rs` — implement `cmd_update()` with `--since`, `--dry-run`, progress reporting | Unit (CLI parse tests) |
| 6 | `test: add integration tests for update subcommand` | Integration tests | `tests/update_test.rs` — mock HTTP responses, verify upsert, verify sync state | Integration |

## Step details

### Step 1 — `feat(db): add sync_state table for incremental update tracking`

**Rationale:** The sync state table is the foundation of the incremental update system. It
records the timestamp of the last successful sync so subsequent updates only fetch entities
modified after that point. This must exist before any update logic is written.

**Deliverables:**

- `src/db/schema.rs`:
  - Add `sync_state` table to `CREATE_TABLE_STATEMENTS`:

    ```sql
    CREATE TABLE IF NOT EXISTS sync_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )
    ```

  - `get_last_sync_timestamp(conn) -> Result<Option<String>>` — reads the `last_sync` key
    from `sync_state`. Returns `None` if no sync has ever been performed.
  - `update_sync_timestamp(conn, timestamp: &str) -> Result<()>` — `INSERT OR REPLACE`
    into `sync_state` with key `'last_sync'` and the given timestamp.
  - Constants for the `sync_state` key name: `pub const SYNC_STATE_KEY: &str = "last_sync";`
  - Logging: `tracing::info!` on update, `tracing::debug!` on read.

- Unit tests:
  - Fresh database has no sync timestamp → `get_last_sync_timestamp()` returns `None`
  - After `update_sync_timestamp("2026-07-17T00:00:00Z")`, `get_last_sync_timestamp()`
    returns `Some("2026-07-17T00:00:00Z")`
  - `update_sync_timestamp` with a newer timestamp replaces the old one
  - Schema initialization includes the `sync_state` table (verify via `all_tables_exist` or
    direct query)

### Step 2 — `feat(update): implement SPARQL query builder and HTTP client`

**Rationale:** The SPARQL client is the data source for incremental updates. It queries the
Wikidata Query Service for entity Q-IDs that have been modified since the last sync timestamp
and match the music entity criteria. This is the first piece of the data-fetching pipeline.

**Deliverables:**

- `src/sparql.rs` — new module, registered in `src/lib.rs` and `src/main.rs`:
  - `build_modified_query(since: &str, limit: u64, offset: u64) -> String` — builds a SPARQL
    query string that selects distinct entity Q-IDs with `schema:dateModified >= since`,
    filtered to music-relevant entities. The SPARQL query replicates the music filter logic
    from `src/wikidata/filter.rs`:

    ```sparql
    SELECT DISTINCT ?item WHERE {
      ?item schema:dateModified ?modified .
      FILTER(?modified >= "2026-07-17T00:00:00Z"^^xsd:dateTime)
      {
        # Occupation filter (P106)
        ?item wdt:P106 wd:Q639669 .
      } UNION {
        ?item wdt:P106 wd:Q36834 .
      } UNION {
        # ... all MUSIC_OCCUPATION_IDS
        ?item wdt:P31 wd:Q215380 .
      } UNION {
        # ... all MUSIC_GROUP_IDS
      } UNION {
        # Catch-all: at least one of P1303, P175, P136, P358
        ?item wdt:P1303|wdt:P175|wdt:P136|wdt:P358 [] .
      }
    }
    ORDER BY ?item
    LIMIT 10000 OFFSET 0
    ```

    - The `since` parameter is interpolated as a SPARQL literal (since it's a trusted
      application-side value, not user input).
    - `limit` defaults to 10000 (Wikidata Query Service max per page).
    - `offset` is passed through for pagination.

  - `SparqlClient` struct:
    - `endpoint_url: String` — default `https://query.wikidata.org/sparql`
    - `user_agent: String` — `"wiki_db/0.1.0 (incremental-update)"`
    - `client: reqwest::Client` — shared HTTP client
    - `rate_limit_delay: Duration` — 1 second between requests
    - `max_retries: u32` — 3 retries on failure
    - `page_size: u64` — 10000

  - `SparqlClient::new() -> Result<Self>` — constructor with default settings
  - `SparqlClient::query_modified_entities(&self, since: &str) -> Result<Vec<String>>`:
    - Calls `build_modified_query()` in a paginated loop
    - Sends HTTP GET request to the SPARQL endpoint with `Accept: application/sparql-results+json`
    - Parses the JSON response to extract Q-ID values from the `results.bindings` array
    - Respects rate limiting: sleeps `rate_limit_delay` between requests
    - Handles 60-second timeout: if a page fails, logs the offset and retries with
      exponential backoff (1s, 2s, 4s)
    - Continues paginating until a page returns fewer results than `page_size`
    - Deduplicates Q-IDs across pages (in case of inconsistent ordering)
    - Returns the full list of modified music-entity Q-IDs

- Unit tests:
  - `build_modified_query` produces a valid SPARQL query string with the given `since`,
    `limit`, and `offset` values embedded
  - `build_modified_query` includes all music occupation Q-IDs from the filter
  - `build_modified_query` includes all music group Q-IDs from the filter
  - `build_modified_query` includes the catch-all property filter
  - `build_modified_query` includes the `ORDER BY` and `LIMIT`/`OFFSET` clauses
  - `SparqlClient` constructor sets expected defaults
  - Mock HTTP test: `SparqlClient::query_modified_entities` parses a
    `sparql-results+json` response correctly (use a local HTTP mock or test the
    response parsing logic directly)

### Step 3 — `feat(update): implement REST API entity fetcher for individual entities`

**Rationale:** Once we have a list of changed Q-IDs from the SPARQL query, we need to fetch
the full entity data for each Q-ID via the Wikimedia REST API. This step adds the entity
fetcher to `src/sparql.rs`.

**Deliverables:**

- `src/sparql.rs`:
  - `EntityResponse` struct — serde wrapper for the REST API response:

    ```rust
    #[derive(Deserialize)]
    struct EntityResponse {
        entities: HashMap<String, serde_json::Value>,
    }
    ```

    The `entities` map is keyed by Q-ID, and each value is the full entity JSON (same
    structure as the dump entity lines). We deserialize into `serde_json::Value` first
    to avoid partial-deserialization issues, then convert to our `Entity` type.

  - `fetch_entity(client: &reqwest::Client, qid: &str, user_agent: &str) -> Result<Entity>`:
    - Sends `GET https://www.wikidata.org/wiki/Special:EntityData/{QID}.json`
    - Sets `User-Agent` header
    - Parses the JSON response
    - Extracts the entity from `entities.{QID}` and deserializes it into our `Entity` type
      (reusing `src/wikidata/model.rs` which already handles all the serde logic)
    - 404 handling: if the entity has been deleted, log a warning and return an error
    - Rate limiting: 1 request/second (shared with the SPARQL client)

  - Reuse the existing `src/wikidata/model.rs` `Entity` struct for deserialization.
    The `Entity` struct already handles all the Wikidata JSON variations (snaktype
    variants, missing labels, etc.) — no changes needed.

  - Update `SparqlClient` to hold a shared `reqwest::Client` that can be used by both
    the SPARQL query method and the entity fetcher.

- Unit tests:
  - Deserialize a mock REST API response into `Entity` via `EntityResponse` wrapper
  - Entity with missing English label → `Entity` with `labels.en` as `None`
  - Entity with no claims → `Entity` with empty claims map
  - 404 response → error return (test with a mock HTTP endpoint)

### Step 4 — `feat(update): implement DuckDB upsert logic for single entity updates`

**Rationale:** After fetching entity data, we need to upsert it into the DuckDB database.
This step adds the upsert functions to `src/db/load.rs`, handling all the tables that a
single entity may touch: artist, genre, artist_genre, album, album_artist, album_genre,
track, track_album, track_artist, artist_instrument, artist_member_of.

**Deliverables:**

- `src/db/load.rs`:
  - `upsert_entity(conn: &Connection, music_entity: &MusicEntity) -> Result<()>`:
    - Wraps the operation in a DuckDB transaction (for atomicity)
    - `INSERT OR REPLACE INTO artist (...) VALUES (...)` for the entity itself
    - `INSERT OR IGNORE` for genre Q-IDs that don't exist yet (subject to label resolution)
    - `INSERT OR IGNORE` for artist_genre, artist_instrument, artist_member_of
    - `INSERT OR REPLACE` for album, track (if the entity has album/track data)
    - `INSERT OR IGNORE` for album_artist, album_genre, track_album, track_artist
    - **Genre label resolution:** When a genre Q-ID is referenced but doesn't exist in
      the `genre` table, insert a placeholder with the Q-ID as the name (the full dump
      bootstrap will have proper labels; for incremental updates, a future enhancement
      could fetch genre labels via the REST API).
    - Logging: `tracing::debug!` for each upserted entity, `tracing::info!` on completion
    - On failure, log the error and the entity Q-ID, then roll back the transaction

  - `upsert_entity_from_json(conn: &Connection, entity: &Entity) -> Result<()>`:
    - Takes a deserialized `Entity` (from the REST API fetcher)
    - Runs `is_music_entity()` from `src/wikidata/filter.rs` to check if it still matches
      music criteria (entities that no longer match are skipped with a warning)
    - Calls `extract_music_entity()` from `src/extraction.rs` to produce a `MusicEntity`
    - If extraction succeeds, calls `upsert_entity()` with the `MusicEntity`
    - If the entity no longer matches music criteria, log a warning and skip it
      (deletion of stale rows is a documented limitation — handled by periodic full
      re-bootstrap)

  - `update_sync_state(conn: &Connection, timestamp: &str) -> Result<()>`:
    - Wraps `schema::update_sync_timestamp()` with error context
    - Called after all entities in a batch are successfully upserted

- Unit tests:
  - Upsert a new artist → verify row exists in `artist` table
  - Upsert the same artist again with modified data → verify `INSERT OR REPLACE` updated
    the row (check `updated_at` or a changed field)
  - Upsert an artist with genres → verify rows in `artist_genre` and `genre` tables
  - Upsert an entity that no longer matches music criteria → verify it is skipped
    (no row in `artist` table)
  - Genre label placeholder: upsert an artist with a genre Q-ID that doesn't exist in
    `genre` → verify a placeholder row is inserted into `genre`
  - Transaction rollback: simulate a failure mid-upsert → verify no partial data

### Step 5 — `feat(update): wire up cmd_update with CLI orchestration`

**Rationale:** This step connects all the pieces — SPARQL client, entity fetcher, upsert
logic — into the `cmd_update()` function in `src/main.rs`. The function orchestrates the
full incremental update pipeline: determine the sync timestamp, query for modified entities,
fetch each entity's data, upsert into the database, and update the sync state.

**Deliverables:**

- `src/main.rs` — implement `cmd_update()`:
  - Parse the `--since` flag or fall back to `schema::get_last_sync_timestamp()` from the
    database. If neither is available, error with a message suggesting the user provide
    `--since` or run bootstrap first.
  - Open the DuckDB database (same path resolution logic as `cmd_bootstrap`)
  - Initialize the schema (ensures `sync_state` table exists)
  - Create a `SparqlClient` with default settings
  - Use `progess_bar` (like `cmd_bootstrap` does) with a spinner showing:
    - "Querying SPARQL endpoint for modified entities..."
    - "Fetching entity N of M..."
    - "Upserting into database..."
  - Call `SparqlClient::query_modified_entities(since)` to get the list of changed Q-IDs
  - For each Q-ID in the list:
    - Call `fetch_entity()` to get the entity data
    - Call `upsert_entity_from_json()` to upsert into DuckDB
    - If `--dry-run` is set, print the Q-ID and a summary of what would change without
      actually writing to the database
  - After all entities are processed, call `update_sync_timestamp()` with the current
    timestamp (or the `--since` value if explicitly provided by the user)
  - `--dry-run` mode: print counts of entities that would be added/updated, without
    modifying the database or sync state
  - Error handling: if any individual entity fetch or upsert fails, log the error with
    the Q-ID and continue with the next entity (don't abort the entire update). Track
    success/failure counts.
  - Summary output:

    ```
    === Update Complete ===
      Queried:      2026-07-17T00:00:00Z → 2026-07-24T00:00:00Z
      Entities:     42 updated, 3 failed, 0 skipped
      Sync state:   2026-07-24T00:00:00Z
    ```

- The `cmd_update` function signature is already `fn cmd_update(args: &cli::update::UpdateArgs,
  config: Option<&Config>) -> Result<()>` — no changes needed to the signature.

- Note: Since this uses `reqwest` (async HTTP), the update pipeline needs to be wrapped in
  a Tokio runtime. The existing `main()` function is synchronous, so we need to either:
  - Use `tokio::runtime::Runtime::block_on()` to run the async update code, or
  - Make `main()` async with `#[tokio::main]`

  **Option A (recommended):** Use `tokio::runtime::Runtime::block_on()` in `cmd_update()`
  to keep `main()` synchronous and avoid changing the existing command functions. This is
  the simplest approach and matches the existing pattern where `cmd_update` is called from
  a synchronous `main()`.

- Unit tests: CLI argument parsing tests already exist in `src/cli/update.rs`. Add:
  - `--since` with explicit timestamp
  - `--dry-run` flag
  - Default parsing (no args)

### Step 6 — `test: add integration tests for update subcommand`

**Rationale:** Integration tests verify the full update pipeline end-to-end with mocked HTTP
responses. This ensures the SPARQL client, entity fetcher, upsert logic, and sync state
tracking all work together correctly.

**Deliverables:**

- `tests/update_test.rs` — new integration test file:
  - **Test fixture:** Use a small local HTTP mock server (or embed JSON responses as
    strings and test the pipeline components in isolation, with the HTTP layer
    abstracted via a trait or test helper that provides canned responses).
    - For simplicity, test the SPARQL response parsing, REST API response parsing, and
      upsert logic separately, then add a combined test that wires them together with
      a mock HTTP server (e.g., `wiremock` or a simple `tiny_http` server).

  - **None:** No entities modified since the sync timestamp → SPARQL returns empty results
    → verify no database changes, sync state unchanged
    - Mock SPARQL response: `{"results":{"bindings":[]}}`

  - **One:** Single entity modified → fetch entity → upsert → verify database row and
    sync state update
    - Mock SPARQL response: returns `Q2831`
    - Mock REST API response: minimal entity JSON for `Q2831` (musician Ivy Queen)
    - Verify: artist table has `Q2831`, sync_state has `last_sync`

  - **Many:** Multiple entities modified → fetch all → upsert all → verify all rows
    and sync state
    - Also verify: partial failure (one entity's fetch fails) → successful entities are
      still upserted, failed entity is logged, sync state is updated
    - Also verify: `--dry-run` mode prints changes without modifying the database

  - **Entity no longer matches music criteria:** SPARQL returns a Q-ID that was previously
    a music entity but no longer has music properties → verify it is skipped (no upsert,
    logged at warn level)

  - **Sync state update:**
    - Initial state: no sync timestamp
    - After update with `--since 2026-07-17T00:00:00Z`: sync state is set to that timestamp
    - After subsequent update without `--since`: reads from sync state, uses it as the
      query parameter

  - **Genre placeholder insertion:** Entity references a genre Q-ID not in the `genre` table
    → verify placeholder row is inserted

## Risk notes

- **SPARQL query complexity:** The music filter in SPARQL requires a UNION of all
  occupation Q-IDs (19), group Q-IDs (12), and a catch-all property path. This is a large
  query that may approach the 60-second timeout. Pagination (LIMIT 10000) mitigates this.
  If the query still times out, log the error and suggest splitting the update into smaller
  time windows.

- **Rate limiting:** The Wikidata SPARQL endpoint allows ~1 request/second without a
  User-Agent. With a proper User-Agent, higher rates are permitted. We use 1 request/second
  to be conservative. The REST API has a similar rate limit.

- **Genre label resolution:** The plan does not explicitly address how to resolve genre
  labels for new genres encountered during incremental updates. The placeholder approach
  (storing the Q-ID as the name) is pragmatic for v1. A future enhancement could fetch
  genre labels via the REST API.

- **`tokio` runtime in synchronous context:** The existing `main()` is synchronous. Using
  `tokio::runtime::Runtime::block_on()` in `cmd_update()` is the recommended approach to
  avoid a larger refactor. If the `--since` flag or `--dry-run` mode is used, no async
  work is needed until the actual HTTP requests start.

- **DuckDB + async:** DuckDB operations use a synchronous C API. Wrapping them in
  `tokio::task::spawn_blocking` is not needed for the sequential CLI update path but
  should be noted for future concurrent use cases.
