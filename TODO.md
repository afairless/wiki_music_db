# Implementation Plan: Phase 5 — Full-Text Search Setup

**Source:** `docs/research/2026-07_music_db_rust_plan.md` (Phase 5 only)

**Project state:** Phases 1 (project scaffold & schema), 2a (Wikidata entity model & deserialization), 2b (filter + streaming parser), 3a (Parquet writer & MusicEntity extraction), and 3b (DuckDB loader & bootstrap CLI) are **complete**. This plan covers **Phase 5 only**. Phases 6, 7, and 8 are **out of scope**.

## Summary

Enable full-text search on artist names, album names, and track names via DuckDB's built-in `fts` extension. Load the extension during database initialization, create FTS indexes (with `PRAGMA create_fts_index`) after data loading, and implement Rust query helper functions that use FTS when available — with a graceful fallback to `LIKE '%term%'` if the extension cannot be loaded.

**Deliverable:** FTS indexes created during bootstrap; integration test searches for an artist by partial name match using both FTS and LIKE fallback paths.

## Steps

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat(db): load DuckDB fts extension during schema initialization with fallback` | FTS extension loading | `src/db/schema.rs` (extended) | Unit |
| 2 | `feat(db): create FTS indexes on artist, album, and track tables` | FTS index creation | `src/db/schema.rs` (extended), `src/db/mod.rs` | Unit |
| 3 | `feat(db): implement query helper functions with FTS/LIKE fallback` | Query helper module | `src/db/query.rs`, `src/db/mod.rs` (updated) | Unit |
| 4 | `feat: wire FTS index creation into bootstrap pipeline` | Bootstrap wiring | `src/main.rs` (updated) | Smoke |
| 5 | `test: add integration test for FTS search after full bootstrap` | FTS integration test | `tests/bootstrap_test.rs` (extended) | Integration |

### Step 1 — FTS extension loading during schema initialization with fallback

Extend `src/db/schema.rs` to load the DuckDB `fts` extension during database initialization, with graceful fallback.

**Changes:**

1. Add a public function `load_fts_extension(conn: &Connection) -> DuckDbResult<bool>` that:
   - Attempts `INSTALL fts; LOAD fts;` via `conn.execute_batch`.
   - On success, returns `true` and logs at `info` level.
   - On failure, logs a warning (e.g., "FTS extension unavailable, falling back to LIKE queries") and returns `false`. The error is **not** propagated — initialization succeeds regardless.
   - The function is idempotent: calling `LOAD fts` on an already-loaded extension is a no-op.

2. Replace the direct `INSTALL fts; LOAD fts;` in `initialize()` with a call to `load_fts_extension()`. The return value is currently unused (callers in later steps will check it).

3. Add a public function `fts_available(conn: &Connection) -> DuckDbResult<bool>` that queries DuckDB's `current_setting('access_mode')` or attempts `SELECT * FROM pragma_function_list WHERE name LIKE 'fts_%'` to detect whether FTS functions are available at runtime.

**Design decisions:**

- The fallback is silent at the `warning` level rather than failing. This is consistent with the plan's requirement that "if fts extension fails to install/load during database initialization, set a flag and fall back to `LIKE '%term%'` with a logged warning."
- Detection of FTS availability at query time uses a DuckDB introspection query (`pragma_function_list` or similar) so the flag survives across database reconnects.
- The `fts_available` check is a cheap query — suitable to call before every search in Phase 6.

**Unit tests (in `src/db/schema.rs` under `#[cfg(test)]`):**

- `test_load_fts_extension`: Initialize an in-memory database, call `load_fts_extension`, verify it returns `true` and has no error.
- `test_load_fts_extension_idempotent`: Call `load_fts_extension` twice on the same connection — second call returns `true` without error.
- `test_fts_available_after_load`: After `initialize()` (which now loads FTS), call `fts_available` — returns `true`.
- `test_fts_available_before_load`: On a fresh connection with no FTS loaded, `fts_available` returns `false`.

### Step 2 — FTS index creation on artist, album, and track tables

Extend `src/db/schema.rs` with a public function to create FTS indexes via `PRAGMA create_fts_index`.

**Changes:**

1. Add `pub fn create_fts_indexes(conn: &Connection) -> DuckDbResult<()>` that:

   ```rust
   pub fn create_fts_indexes(conn: &Connection) -> DuckDbResult<()> {
       // Create FTS index on artist table
       conn.execute_batch(
           "PRAGMA create_fts_index('artist', 'id', 'name', 'description');"
       )?;

       // Create FTS index on album table
       conn.execute_batch(
           "PRAGMA create_fts_index('album', 'id', 'name');"
       )?;

       // Create FTS index on track table
       conn.execute_batch(
           "PRAGMA create_fts_index('track', 'id', 'name');"
       )?;

       tracing::info!("FTS indexes created on artist, album, and track tables");
       Ok(())
   }
   ```

2. The function is idempotent for testing convenience: calling it twice on the same connection should not error. This can be achieved by checking if the FTS shadow tables already exist (`fts_main_artist`, `fts_main_album`, `fts_main_track`) before creating them, or by wrapping each PRAGMA in a try/catch and ignoring "already exists" errors.

3. The function is called **after** data loading (in Step 4), not during `initialize()`, because `create_fts_index` requires the target tables to already exist (they are created in `initialize()`).

**Design decisions:**

- `create_fts_index` creates shadow tables (`fts_main_artist`, `fts_main_album`, `fts_main_track`) and registers `fts_match_*` table functions. These functions are then queried via `SELECT * FROM artist WHERE fts_match_artist(?1)`, DuckDB's FTS query syntax.
- The `id` column is included in the index so results can be joined back to the main table.
- This function is separate from `initialize` so it can be called at the right point in the pipeline (after `load_all`).

**Unit tests:**

- `test_create_fts_indexes`: Initialize an in-memory database, insert one row each into `artist`, `album`, `track` tables, call `create_fts_indexes`, then verify that FTS queries return results:

  ```sql
  -- Verify via introspection
  SELECT COUNT(*) FROM pragma_function_list WHERE name = 'fts_main_artist'
  ```

  or directly:

  ```sql
  SELECT COUNT(*) FROM artist WHERE fts_match_artist('test')
  ```

- `test_create_fts_indexes_idempotent`: Call `create_fts_indexes` twice — no error on second call.
- `test_fts_index_search_artist`: Insert a row with a known name, create FTS indexes, search by a substring of that name, verify the row is returned.
- `test_fts_index_search_album`: Same pattern for album table.
- `test_fts_index_search_track`: Same pattern for track table.

### Step 3 — Query helper functions with FTS/LIKE fallback

Create `src/db/query.rs` with Rust functions that wrap FTS queries and fall back to `LIKE` when FTS is unavailable.

**New file: `src/db/query.rs`**

```rust
//! Query helper functions with FTS/LIKE fallback.
//!
//! All search functions check whether the DuckDB FTS extension is available
//! and use FTS queries when possible, falling back to `LIKE '%term%'`.

**Types and functions:**

```rust
/// A search result row from the artist table.
pub struct ArtistSearchResult {
    pub id: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub artist_type: String,
    pub birth_date: Option<NaiveDate>,
    pub death_date: Option<NaiveDate>,
}

/// A search result row from the album table.
pub struct AlbumSearchResult {
    pub id: String,
    pub name: String,
    pub release_date: Option<NaiveDate>,
}

/// A search result row from the track table.
pub struct TrackSearchResult {
    pub id: String,
    pub name: String,
    pub duration_seconds: Option<i32>,
}

/// Search artists by name using FTS when available, falling back to LIKE.
pub fn search_artist(conn: &Connection, term: &str) -> Result<Vec<ArtistSearchResult>>

/// Search albums by name using FTS when available, falling back to LIKE.
pub fn search_album(conn: &Connection, term: &str) -> Result<Vec<AlbumSearchResult>>

/// Search tracks by name using FTS when available, falling back to LIKE.
pub fn search_track(conn: &Connection, term: &str) -> Result<Vec<TrackSearchResult>>
```

**FTS query path (when `fts_available` returns `true`):**

```sql
SELECT id, name, description, artist_type, birth_date, death_date
FROM artist
WHERE fts_match_artist(?1)
```

**LIKE fallback path (when `fts_available` returns `false`):**

```sql
SELECT id, name, description, artist_type, birth_date, death_date
FROM artist
WHERE name LIKE '%' || ?1 || '%'
  OR description LIKE '%' || ?1 || '%'
ORDER BY name
LIMIT 100
```

**Key design decisions:**

- All search functions accept `&Connection` (borrow, not owned) — consistent with the rest of the `db` module.
- The LIKE fallback uses `%` wildcards on both sides for substring matching, bounded by `LIMIT 100` to prevent unbounded scans.
- The term parameter is passed as a DuckDB parameterized value (not string interpolation) — prevents SQL injection in the LIKE path.
- Search result structs derive `Debug, Clone, PartialEq` for test assertions.
- The FTS query is simpler (single `fts_match_artist(?1)` call) than the LIKE fallback, which searches both `name` and `description`.

**Unit tests (in `src/db/query.rs` under `#[cfg(test)]`):**

- **FTS path (fts available):**
  - `test_search_artist_fts`: Insert row with name "Miles Davis", create FTS indexes, search for "Miles", verify row returned.
  - `test_search_artist_fts_partial`: Search for "Mile" (partial match), verify row returned.
  - `test_search_artist_fts_no_match`: Search for "Nonexistent", verify zero results.
  - `test_search_album_fts`: Insert album row, create FTS index, search, verify.
  - `test_search_track_fts`: Insert track row, create FTS index, search, verify.
- **LIKE fallback path (fts unavailable):**
  - `test_search_artist_like_fallback`: On a connection without FTS loaded, search for "Miles", verify LIKE fallback returns results. Mock the FTS check by simply not calling `load_fts_extension`.
  - `test_search_artist_like_fallback_no_match`: LIKE fallback with no matches → empty vec.
- **Edge cases:**
  - `test_search_empty_term`: Both FTS and LIKE paths with empty string term — return empty results (no match).
  - `test_search_special_chars`: Term with SQL-special characters (single quote, percent) — no panic or SQL error.

### Step 4 — Wire FTS index creation into bootstrap pipeline

Update `src/main.rs` to call `create_fts_indexes` after `load_all` in the bootstrap command, and report FTS status in the summary.

**Changes to `cmd_bootstrap` in `src/main.rs`:**

1. Add a `use wiki_db::db::schema::create_fts_indexes;` import.
2. Immediately after `load_all(&conn,&parquet_dir)` succeeds, call `create_fts_indexes(&conn)`:

   ```rust
   // Create FTS indexes for full-text search
   pb.set_message("Creating FTS indexes...");
   pb.tick();

   schema::create_fts_indexes(&conn)
       .context("Failed to create FTS indexes")?;
   ```

3. After the FTS call, check `schema::fts_available(&conn)` and include the status in the summary output:

   ```
   FTS: enabled (DuckDB fts extension)
   ```

   or:

   ```
   FTS: disabled (using LIKE fallback)
   ```

4. Add a new summary line in the final `println!` block.

**Fallback at bootstrap time:** If `create_fts_indexes` fails (e.g., FTS extension wasn't loaded), the error must be handled gracefully. Follow the same pattern as `load_fts_extension`: log a warning and continue, rather than failing the entire bootstrap. Wrap the call:

```rust
if let Err(e) = schema::create_fts_indexes(&conn) {
    tracing::warn!(error = %e, "Failed to create FTS indexes; LIKE fallback will be used");
}
```

This ensures that a database created without FTS support still works (queries will use LIKE fallback via Step 3).

**Smoke tests (in `src/main.rs` under `#[cfg(test)]`):**

- `test_bootstrap_with_fts_indexes`: Run the existing `test_bootstrap_with_mini_fixture` test, then re-open the database, call `fts_available`, and verify FTS is available.
- The existing smoke tests (`test_bootstrap_with_mini_fixture`, `test_bootstrap_missing_dump_returns_error`, `test_bootstrap_cleanup_parquet`, `test_bootstrap_entity_missing_name_stored_as_null`) must still pass unchanged.

### Step 5 — Integration test for FTS search after full bootstrap

Add an integration test in `tests/bootstrap_test.rs` that performs a full bootstrap on the mini fixture and then runs FTS queries.

**Test cases:**

1. **`test_bootstrap_fts_search_artist`:**
   - Run bootstrap on the mini fixture.
   - Re-open the database (as a separate connection, like real usage).
   - Load the FTS extension: `conn.execute_batch("LOAD fts;")`.
   - Search for "Ivy Queen" using the FTS query helper.
   - Verify the result contains the expected artist row.

2. **`test_bootstrap_fts_search_album`:**
   - Run bootstrap on the mini fixture.
   - Search for an album name from the fixture (if fixture has albums). If none, add an album entity to a new fixture.

3. **`test_bootstrap_fts_search_no_match`:**
   - Search for a term that doesn't exist in the database.
   - Verify zero results are returned.

**Fixtures:** The existing `create_mini_fixture` helper in `src/main.rs` generates a mini gzipped fixture with "Ivy Queen" (musician, Q2831) and "jazz" (genre, Q35718). This fixture is sufficient for the artist FTS test. If album/track FTS tests need data, extend the fixture with entities that have album/track references, or use the already-extracted album/track data from the existing fixture (note: the mini fixture only has `P136` and `P569` claims, no albums/tracks).

For this step, focus on artist FTS search. Album and track FTS can be tested in a future extension when the fixture has album/track data.

## Notes for Implementation

### DuckDB FTS Details

- DuckDB's FTS extension requires explicit loading: `INSTALL fts; LOAD fts;`. These are idempotent and should be called once per connection.
- `PRAGMA create_fts_index('table', 'id', 'name')` creates shadow tables `fts_main_table` and registers the `fts_match_table` table function.
- The FTS query syntax is: `SELECT * FROM table WHERE fts_match_table('query')`.
- FTS query syntax uses DuckDB's built-in tokenizer (simple lowercased whitespace tokenization). Double-quoted strings match exact phrases.
- Shadow tables persist across connections (they are real tables in the database file), so FTS indexes only need to be created once.

### `fts_available` Detection Approach

The detection function should query DuckDB's catalog to determine if the FTS extension is loaded. Options (in order of preference):

1. **Probe a known FTS function:** `SELECT COUNT(*) FROM pragma_functions() WHERE name = 'fts_main_artist'` — but this function may not exist until an index is created.
2. **Check installed extensions:** `SELECT * FROM duckdb_extensions()` — DuckDB 1.x provides this system table listing installed/loaded extensions.
3. **Try-Load approach:** Attempt `LOAD fts;` — if it fails, FTS isn't available; if it succeeds, it was just loaded.

**Recommendation:** Use option 2 (`duckdb_extensions()`) for detection, and option 1 (`pragma_functions()`) only after index creation. For pre-index detection, option 2 is reliable and cheap.

### Idempotency of `create_fts_indexes`

DuckDB's `PRAGMA create_fts_index` will error if called twice for the same table (shadow tables already exist). To make `create_fts_indexes` idempotent, check for existing shadow tables before creating:

```rust
pub fn create_fts_indexes(conn: &Connection) -> DuckDbResult<()> {
    let existing: i64 = conn.query_row(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'fts\\_main\\_%' ESCAPE '\\'",
        [],
        |row| row.get(0),
    )?;

    if existing >= 3 {
        tracing::info!("FTS indexes already exist, skipping creation");
        return Ok(());
    }

    // ... create indexes ...
    Ok(())
}
```

Alternatively, wrap each PRAGMA in a try/catch and ignore `Catalog already contains` errors.
