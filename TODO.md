# Implementation Plan: Phase 6 — Query Subcommand

Source: `docs/research/2026-07_music_db_rust_plan.md` (Section 6, Phase 6)

**Preceding phases (completed):** 1 (scaffold & schema), 2a (entity model & deserialization),
2b (filter + streaming parser), 3a (Parquet writer & MusicEntity extraction),
3b (DuckDB loader & bootstrap CLI), 5 (full-text search setup).

**What already exists (Phase 6 scaffolding):**

- `src/cli/query.rs` — CLI argument definitions for `artist`, `genre`, `album`, `search` subcommands
- `src/db/query.rs` — `search_artist()`, `search_album()`, `search_track()` with FTS/LIKE fallback
- `src/main.rs` — `cmd_query()` stub that logs "not yet implemented"

**What is still missing:**

- `search_genre()` function (with pagination — `LIMIT ? OFFSET ?`)
- Detail lookup functions for related data (genres/albums/instruments for an artist; artists/genres/tracks for an album; artists for a genre)
- Actual `cmd_query()` implementation with colored terminal output
- `colored` crate dependency
- Integration tests

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat(db): add search_genre with LIKE fallback and pagination` | Genre search | `src/db/query.rs` — add `GenreSearchResult` struct, `search_genre()` function with `LIMIT ? OFFSET ?` parameters, FTS/LIKE fallback, and unit tests | Unit |
| 2 | `feat(db): add detail lookup functions for artist, album, and genre relations` | Detail lookups | `src/db/query.rs` — add `artist_genres()`, `artist_albums()`, `artist_instruments()`, `album_artists()`, `album_genres()`, `album_tracks()`, `genre_artists()` with pagination, plus their result structs and unit tests | Unit |
| 3 | `feat(cli): implement query subcommand with colored output` | Query CLI wiring | `Cargo.toml` — add `colored` crate; `src/main.rs` — implement `cmd_query()` dispatching to `search_artist`/`search_genre`/`search_album`/`search_track` + detail lookups, with colored terminal output | Unit (CLI parse tests already exist) |
| 4 | `test: add integration tests for query subcommand` | Integration tests | `tests/` — new integration test file or module exercising the None-One-Many principle for all query subcommands, including FTS fallback verification | Integration |

## Step details

### Step 1 — `feat(db): add search_genre with LIKE fallback and pagination`

**Rationale:** The `genre` subcommand needs a search function analogous to `search_artist()`,
`search_album()`, and `search_track()`, but with pagination parameters (`limit`, `offset`)
that the others lack. Adding this first means the detail lookup functions in Step 2 can
reference `GenreSearchResult` without circular dependencies.

**Deliverables:**

- `src/db/query.rs`:
  - `GenreSearchResult` struct with `id`, `name`
  - `search_genre()` function with `Connection`, `term`, `limit`, `offset` parameters
  - FTS path when `fts_available()` and `fts_index_exists("genre")` are true
  - LIKE fallback path: `SELECT id, name FROM genre WHERE name LIKE '%' || ?1 || '%' ORDER BY name LIMIT ? OFFSET ?`
  - Empty-term guard returning empty vec
- Unit tests for `search_genre()`:
  - Like fallback finds genre by name
  - Like fallback partial match
  - No match returns empty vec
  - Empty term returns empty vec
  - `--limit` / `--offset` pagination works
  - SQL special characters don't cause errors

### Step 2 — `feat(db): add detail lookup functions for artist, album, and genre relations`

**Rationale:** The query output specifications require showing related data — e.g.
"artist output shows genres, albums, instruments". These functions are all structurally
similar (simple JOIN queries), so they can be implemented together with their tests.

**Deliverables:**

`src/db/query.rs` — new result structs and functions:

| Function | Returns | Query pattern |
|---|---|---|
| `artist_genres(conn, artist_id)` | `Vec<ArtistGenreResult>` | `SELECT g.id, g.name FROM genre g JOIN artist_genre ag ON g.id = ag.genre_id WHERE ag.artist_id = ?` |
| `artist_albums(conn, artist_id)` | `Vec<ArtistAlbumResult>` | `SELECT a.id, a.name, a.release_date, aa.role FROM album a JOIN album_artist aa ON a.id = aa.album_id WHERE aa.artist_id = ? ORDER BY a.name` |
| `artist_instruments(conn, artist_id)` | `Vec<ArtistInstrumentResult>` | `SELECT instrument_id FROM artist_instrument WHERE artist_id = ?` |
| `album_artists(conn, album_id)` | `Vec<AlbumArtistResult>` | `SELECT ar.id, ar.name, aa.role FROM artist ar JOIN album_artist aa ON ar.id = aa.artist_id WHERE aa.album_id = ?` |
| `album_genres(conn, album_id)` | `Vec<AlbumGenreResult>` | `SELECT g.id, g.name FROM genre g JOIN album_genre ag ON g.id = ag.genre_id WHERE ag.album_id = ?` |
| `album_tracks(conn, album_id)` | `Vec<AlbumTrackResult>` | `SELECT t.id, t.name, t.duration_seconds, ta.track_number FROM track t JOIN track_album ta ON t.id = ta.track_id WHERE ta.album_id = ? ORDER BY ta.track_number` |
| `genre_artists(conn, genre_id, limit, offset)` | `Vec<GenreArtistResult>` | `SELECT ar.id, ar.name, ar.description, ar.artist_type FROM artist ar JOIN artist_genre ag ON ar.id = ag.artist_id WHERE ag.genre_id = ? ORDER BY ar.name LIMIT ? OFFSET ?` |

- Unit tests for each function: insert test data, query back, verify correct fields and counts
- Edge cases: empty result sets, missing optional fields (NULL names, NULL dates)

### Step 3 — `feat(cli): implement query subcommand with colored output`

**Rationale:** This is the wiring step that connects the CLI argument parsing (already done
in `src/cli/query.rs`) to the query functions (Steps 1–2) and displays results to the user.

**Deliverables:**

- `Cargo.toml` — add `colored = "2"` to `[dependencies]`
- `src/main.rs` — implement `cmd_query()`:
  - `QueryCommand::Artist(args)`:
    - Call `search_artist()`, for each result call `artist_genres()`, `artist_albums()`, `artist_instruments()`
    - Display: `name`, `description`, `birth_date`, `artist_type`, `genres`, `albums`, `instruments`
    - Colored output: section headers in bold cyan, labels in yellow, values in white
  - `QueryCommand::Genre(args)`:
    - Call `search_genre()` with `args.limit`, `args.offset`
    - Display: genre info + associated artists via `genre_artists()`
  - `QueryCommand::Album(args)`:
    - Call `search_album()`, for each result call `album_artists()`, `album_genres()`, `album_tracks()`
    - Display: album info, artists, genres, track listing
  - `QueryCommand::Search(args)`:
    - Call `search_artist()`, `search_album()`, `search_track()` with the same term
    - Display combined results grouped by category
  - "No results" message when search returns empty
  - `anyhow` context on all query operations, `tracing::warn!` on non-fatal lookup failures
- Unit tests: verify CLI parsing still works (tests already exist in `src/cli/query.rs`)

### Step 4 — `test: add integration tests for query subcommand`

**Rationale:** Integration tests ensure the full pipeline (CLI args → query functions → formatted output)
works correctly against a real (in-memory) DuckDB with populated data.

**Deliverables:**

- `tests/query_test.rs` (or inline in `tests/bootstrap_test.rs`):
  - **None:** Query against an empty in-memory database → "no results" message, exit code 0
  - **One (artist):** Insert a single artist, query by name → correct fields displayed
  - **One (genre):** Insert a single genre, query by name → genre info + artist list
  - **One (album):** Insert a single album, query by name → album info + related data
  - **Many (genre):** Insert multiple genres, query with `--limit` / `--offset` → pagination works
  - **Search:** Insert across all entity types, query with `search --term` → combined results
  - **FTS fallback:** Verify LIKE fallback produces results when FTS is disabled (bundled DuckDB build)
  - **No data:** Database with no music entities → all queries return empty

## Risk notes

- **DuckDB FTS limitations:** The bundled DuckDB build loads the FTS extension but does not
  support actual index creation. All query functions already have LIKE fallback — this is
  transparent to callers. The integration tests should explicitly test the LIKE fallback path.
- **`colored` crate version:** Pin `colored = "2"` at implementation time; verify it compiles
  with edition 2024.
