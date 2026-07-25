# Implementation Plan: Phase 3b — DuckDB Loader & Bootstrap CLI

**Source:** `docs/research/2026-07_music_db_rust_plan.md` (Phase 3b only)

**Project state:** Phases 1 (project scaffold & schema), 2a (Wikidata entity model & deserialization), 2b (filter + streaming parser), and 3a (Parquet writer & MusicEntity extraction) are **complete**. This plan covers **Phase 3b only**. Phases 5, 6, 7, and 8 are **out of scope**. Do not implement any code belonging to those phases.

## Summary

Load the Parquet intermediate files (produced by Phase 3a) into DuckDB, wire up the `bootstrap` CLI subcommand with the complete streaming pipeline, and add progress reporting. The loader handles dependency-ordered insertion into all schema tables, parsing pipe-delimited columns (genres, instruments, member_of) and JSON arrays (albums, tracks) from the flat Parquet format.

**Deliverable:** `cargo run -- bootstrap --dump test_data/fixture.json.gz --db /tmp/test.duckdb` populates a valid DuckDB database; `cargo test` passes.

## Steps

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat: implement DuckDB loader for genre and artist tables from Parquet` | Genre + artist loader | `src/db/load.rs`, `src/db/mod.rs` (updated) | Unit |
| 2 | `feat: implement DuckDB loader for artist join tables from flat Parquet columns` | Join-table loader | `src/db/load.rs` (extended) | Unit |
| 3 | `feat: implement DuckDB loader for album and track stub tables from JSON columns` | Album + track loader | `src/db/load.rs` (extended) | Unit |
| 4 | `feat: wire up bootstrap CLI with streaming pipeline, genre extraction, and progress bars` | Bootstrap CLI wiring | `src/main.rs`, `src/cli/bootstrap.rs` (updated) | Smoke |
| 5 | `test: add integration tests for full bootstrap flow with mini dump fixture` | Bootstrap integration tests | `tests/bootstrap_test.rs` (extended), `tests/fixtures/` (updated) | Integration |

### Step 1 — DuckDB loader: genre and artist tables

Create `src/db/load.rs` with the core DuckDB loader logic. Update `src/db/mod.rs` to export `pub mod load;`.

**Design:**

- Define a public function `load_all(conn: &Connection, parquet_dir: &Path) -> Result<()>` that orchestrates all loading in dependency order. For Step 1, this function delegates to the genre and artist loaders.
- `load_genres(conn: &Connection, parquet_dir: &Path) -> Result<usize>` — loads `genres.parquet` into the `genre` table via DuckDB SQL:

  ```sql
  INSERT OR IGNORE INTO genre (id, name)
  SELECT id, name FROM read_parquet('<parquet_dir>/genres.parquet')
  ```

  Return the number of rows inserted.
- `load_artists(conn: &Connection, parquet_dir: &Path) -> Result<usize>` — loads artist Parquet files into the `artist` table. Since the flat Parquet has VARCHAR columns for dates, cast them:

  ```sql
  INSERT OR IGNORE INTO artist (id, name, description, artist_type, inclusion_reason, birth_date, death_date)
  SELECT id, name, description, artist_type, inclusion_reason,
         CASE WHEN birth_date IS NOT NULL AND birth_date != '' THEN birth_date::DATE ELSE NULL END,
         CASE WHEN death_date IS NOT NULL AND death_date != '' THEN death_date::DATE ELSE NULL END
  FROM read_parquet('<parquet_dir>/part-*.parquet')
  ```

  Return the number of rows inserted.

**Key design decisions:**

- Use `INSERT OR IGNORE` throughout for idempotent re-runs.
- Artist Parquet files are read via glob `part-*.parquet`; genre file is a single `genres.parquet`.
- The functions are idempotent: calling them multiple times with the same Parquet files produces the same database state.
- Errors from missing Parquet files (e.g., `genres.parquet` not found) are propagated as `Error::Io`.
- DuckDB's `read_parquet` function reads all files matching the glob pattern in a single query.

**Unit tests (in `src/db/load.rs` under `#[cfg(test)]`):**

- `test_load_genres`: Create an in-memory DuckDB with the schema, write a test `genres.parquet` via the Parquet writer, call `load_genres`, verify 2 genre rows inserted, verify `INSERT OR IGNORE` — calling twice doesn't error and rows remain at 2.
- `test_load_genres_empty_file`: Write an empty `genres.parquet` (0 rows), load → 0 rows inserted, no error.
- `test_load_artists`: Write a test artist Parquet file with 3 entities (including one with `name=NULL`, one with a birth_date, one with empty genres), call `load_artists`, verify 3 artist rows, verify NULL name is stored as NULL, verify date is parsed correctly.
- `test_load_artists_idempotent`: Load twice → same row count (INSERT OR IGNORE).
- `test_load_artists_missing_parquet_dir`: Call with nonexistent path → error.
- `test_load_all_orchestrator_empty_dir`: Call `load_all` on an empty parquet directory → error (no files to load).

### Step 2 — DuckDB loader: artist join tables from pipe-delimited columns

Extend `src/db/load.rs` with functions that parse the pipe-delimited columns from the flat Parquet format and insert into the artist join tables.

**Functions:**

- `load_artist_genre(conn, parquet_dir) -> Result<usize>` — parses the pipe-delimited `genres` column using DuckDB SQL:

  ```sql
  INSERT OR IGNORE INTO artist_genre (artist_id, genre_id)
  SELECT a.id, unnest(string_split(a.genres, '|'))
  FROM read_parquet('<parquet_dir>/part-*.parquet') a
  WHERE a.genres IS NOT NULL AND a.genres != ''
  ```

  Return row count.

- `load_artist_instrument(conn, parquet_dir) -> Result<usize>` — same pattern for `instruments` → `artist_instrument`.

- `load_artist_member_of(conn, parquet_dir) -> Result<usize>` — same pattern for `member_of` → `artist_member_of`.

- Update `load_all` to call these three in sequence after `load_artists`.

**Key design decisions:**

- DuckDB's `string_split` + `unnest` handles the pipe-delimited format efficiently in SQL.
- `WHERE` clause filters out empty strings to avoid inserting empty-string rows.
- `INSERT OR IGNORE` prevents duplicate (artist_id, genre_id) pairs.
- Foreign-key constraints require that `load_artists` and `load_genres` run first (so referenced IDs exist). The order in `load_all` enforces this.

**Unit tests:**

- `test_load_artist_genre`: Write a Parquet file with one entity having `genres="Q35718|Q57251"` and one with `genres=""`. Load genres and artists first, then artist_genre. Verify 2 rows inserted, verify correct artist_id/genre_id pairs, verify empty-string entity produces no rows.
- `test_load_artist_genre_no_genres`: Entity with `genres=""` → 0 rows inserted.
- `test_load_artist_instrument`: Entity with `instruments="Q171|Q197"` → 2 rows in `artist_instrument`.
- `test_load_artist_instrument_empty`: Entity with `instruments=""` → 0 rows.
- `test_load_artist_member_of`: Entity with `member_of="Q11649"` → 1 row.
- `test_load_all_join_tables_idempotent`: Call load_all twice → row counts unchanged after first call (INSERT OR IGNORE).

### Step 3 — DuckDB loader: album and track stubs from JSON columns

Extend `src/db/load.rs` with functions that parse the JSON array columns (`albums`, `tracks`) and create stub entries in the album/track tables plus join-table entries.

**Functions:**

- `load_albums_and_tracks(conn, parquet_dir) -> Result<(usize, usize)>` returning `(album_count, track_count)`.

  Parse the `albums` JSON column to seed the `album` table with stub records (Q-ID as both id and placeholder name since we don't have album names from artist data):

  ```sql
  INSERT OR IGNORE INTO album (id, name)
  SELECT DISTINCT
      json_extract_string(value, '$.album_id') as id,
      json_extract_string(value, '$.album_id') as name  -- placeholder: Q-ID as name
  FROM read_parquet('<parquet_dir>/part-*.parquet'),
  LATERAL json_each(albums)
  WHERE albums IS NOT NULL AND albums != '[]'
  ```

  Populate `album_artist`:

  ```sql
  INSERT OR IGNORE INTO album_artist (album_id, artist_id, role)
  SELECT
      json_extract_string(value, '$.album_id') as album_id,
      a.id as artist_id,
      json_extract_string(value, '$.role') as role
  FROM read_parquet('<parquet_dir>/part-*.parquet') a,
  LATERAL json_each(a.albums)
  WHERE a.albums IS NOT NULL AND a.albums != '[]'
  ```

  Same pattern for tracks → `track` table and `track_artist` table.

- Update `load_all` to call `load_albums_and_tracks` after the artist join tables.

**Key design decisions:**

- Album names use the Q-ID as a placeholder. A future phase can resolve actual album names via a second pass over the dump or a SPARQL query.
- `LATERAL json_each` is DuckDB syntax for expanding JSON arrays row-wise.
- `INSERT OR IGNORE` handles deduplication when multiple artists reference the same album.
- Track names also use Q-ID placeholders.

**Unit tests:**

- `test_load_albums`: Write a Parquet file with an entity having `albums='[{"album_id":"Q123","role":"performer"}]'`. Load albums. Verify 1 row in `album` with id="Q123" and name="Q123". Verify 1 row in `album_artist`.
- `test_load_albums_multiple_artists_same_album`: Two entities referencing the same album Q-ID → 1 album row, 2 album_artist rows.
- `test_load_albums_empty`: Entity with `albums='[]'` → no album rows inserted.
- `test_load_tracks`: Entity with `tracks='[{"track_id":"Q456","role":"performer"}]'` → 1 track row, 1 track_artist row.
- `test_load_tracks_and_albums_together`: Entity with both `albums` and `tracks` populated → correct counts in both tables.
- `test_load_all_complete_small_fixture`: Load a small Parquet fixture with all column types populated → verify row counts across all 12 tables.

### Step 4 — Wire up bootstrap CLI with pipeline and progress bars

Update `src/main.rs` to implement the `cmd_bootstrap` function with the full pipeline.

**Pipeline flow in `cmd_bootstrap`:**

```
1. Open dump file with StreamReader
2. Create MusicEntityBatchWriter (Parquet output)
3. Initialize genre_qids HashSet
4. For each StreamEvent:
   - Filtered: extract_music_entity → write_batch
   - Rejected: log warning, increment counter
   - Skipped: increment counter
5. Flush Parquet writer
6. Run second-pass genre label extraction (extract_genre_labels)
7. Write genre Parquet file
8. Open/create target DuckDB database
9. Initialize schema (schema::initialize)
10. Call load_all (DuckDB loader from Step 3)
11. If --cleanup-parquet: delete intermediate Parquet files
12. Print summary
```

**Progress reporting with `indicatif`:**

- Create a `ProgressBar` with a spinner style and message template:

  ```
  [{spinner}] Processed: {processed} | Filtered: {filtered} | Rejected: {rejected}
  ```

- Update the progress bar periodically (every 1000 events for performance).
- On completion, set the progress bar to `finish_with_message("Done")`.

**`--resume` support:**

- Before streaming, check if Parquet files already exist in `parquet_dir`.
- If `--resume` is set and files exist:
  1. Find the highest `part-*.parquet` file index.
  2. Remove that file (may be incomplete from an interrupted run).
  3. Check if `genres.parquet` exists — if so, loading can proceed directly.
  4. If all Parquet files exist and are complete, skip streaming entirely.
- Log a warning about the v1 limitation (streaming from the beginning rather than mid-stream).

**`--cleanup-parquet`:**

- After successful `load_all`, if `--cleanup-parquet` is set, recursively delete `parquet_dir` contents.
- Log the deletion.

**Genre Parquet writing:**

- After `extract_genre_labels` returns `Vec<GenreEntry>`, write these to `genres.parquet` in the same output directory.
- Use a simple helper:

  ```rust
  fn write_genres_parquet(genres: &[GenreEntry], path: &Path) -> Result<()>
  ```

- Parquet schema for genres: `id: VARCHAR, name: VARCHAR` (both non-nullable).

**Smoke test (in `src/main.rs` under `#[cfg(test)]`):**

- `test_bootstrap_with_mini_fixture`: Create a small test gzipped fixture (3-5 entities), run `cmd_bootstrap` with a temp database, verify the function completes without error.
- `test_bootstrap_cleanup_parquet`: Run bootstrap with `--cleanup-parquet`, verify Parquet directory is deleted after success.

### Step 5 — Integration tests for full bootstrap flow

Extend `tests/bootstrap_test.rs` with comprehensive integration tests using the existing `mini_dump.json.gz` fixture (or create a new small fixture if needed).

**Test cases:**

1. **`test_bootstrap_populates_all_tables`**:
   - Run full bootstrap with `--dump tests/fixtures/mini_dump.json.gz --db <temp> --parquet-dir <temp_parquet>`
   - Verify row counts in each table are greater than 0.
   - Verify core tables have data: `artist`, `genre`, `album`, `track`, `artist_genre`, `album_artist`, etc.

2. **`test_bootstrap_idempotent`**:
   - Run bootstrap twice with the same fixture.
   - Verify row counts are identical after each run (no duplicates from INSERT OR IGNORE).

3. **`test_bootstrap_entity_missing_name_stored_as_null`**:
   - Create a small fixture with an entity that has `id` and `inclusion_reason` but no English label.
   - Run bootstrap on this fixture.
   - Query the artist table: verify the row exists and `name` is `NULL`.

4. **`test_bootstrap_genre_without_label`**:
   - Create a mini dump where an artist references genre Q-ID "Q99999" but no genre entity "Q99999" exists in the dump.
   - Run bootstrap.
   - Verify `artist_genre` contains "Q99999" row but `genre` table does not contain "Q99999".

5. **`test_bootstrap_resume_skips_existing_parquet`**:
   - Create a parquet directory with one pre-existing valid `part-00001.parquet`.
   - Run bootstrap with `--resume`.
   - Verify the streaming phase is skipped (no new Parquet files created beyond those already present), and loading proceeds from existing files.

6. **Deferred** — SQL query builder integration tests (from Phase 6). These depend on query builder functions that do not yet exist. They will be added when Phase 6 is implemented.
