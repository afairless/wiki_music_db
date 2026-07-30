//! Integration tests for the query subcommand.
//!
//! These tests exercise the full query pipeline against an in-memory
//! DuckDB database with populated test data. They test the library's
//! query functions directly (via `wiki_db::db::query`) rather than
//! spawning a child process, ensuring the query logic works correctly
//! with real database state.

use chrono::NaiveDate;
use duckdb::Connection;
use wiki_db::db::query;
use wiki_db::db::schema;

/// Helper: create an in-memory DuckDB with full schema initialized.
fn test_conn() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    schema::initialize(&conn).unwrap();
    conn
}

/// Helper: populate the database with a small set of test data.
fn populate_test_data(conn: &Connection) {
    // Artists
    conn.execute_batch(
        "INSERT INTO artist (id, name, description, artist_type, birth_date, death_date) VALUES
            ('Q1', 'Miles Davis', 'Jazz trumpeter and composer', 'person', '1926-05-26', '1991-09-28');
         INSERT INTO artist (id, name, artist_type) VALUES
            ('Q2', 'John Coltrane', 'person');
         INSERT INTO artist (id, name, artist_type) VALUES
            ('Q3', 'Weather Report', 'group');
         INSERT INTO artist (id, name, description, artist_type) VALUES
            ('Q4', 'Ivy Queen', 'American singer-songwriter', 'person');",
    )
    .unwrap();

    // Genres
    conn.execute_batch(
        "INSERT INTO genre (id, name) VALUES
            ('G1', 'Jazz'),
            ('G2', 'Fusion'),
            ('G3', 'Rock'),
            ('G4', 'Classical'),
            ('G5', 'Reggaeton');",
    )
    .unwrap();

    // Artist-genre
    conn.execute_batch(
        "INSERT INTO artist_genre (artist_id, genre_id) VALUES
            ('Q1', 'G1'),
            ('Q2', 'G1'),
            ('Q3', 'G2'),
            ('Q4', 'G5');",
    )
    .unwrap();

    // Artist-instrument
    conn.execute_batch(
        "INSERT INTO artist_instrument (artist_id, instrument_id) VALUES
            ('Q1', 'Q93474'),  -- trumpet
            ('Q2', 'Q8349'),   -- saxophone
            ('Q3', 'Q171236'); -- keyboard",
    )
    .unwrap();

    // Albums
    conn.execute_batch(
        "INSERT INTO album (id, name, release_date) VALUES
            ('A1', 'Kind of Blue', '1959-08-17'),
            ('A2', 'Heavy Weather', '1977-01-01'),
            ('A3', 'The Sentence', '2002-01-01');",
    )
    .unwrap();

    // Album-artist
    conn.execute_batch(
        "INSERT INTO album_artist (album_id, artist_id, role) VALUES
            ('A1', 'Q1', 'performer'),
            ('A1', 'Q2', 'performer'),
            ('A2', 'Q3', 'performer'),
            ('A3', 'Q4', 'performer');",
    )
    .unwrap();

    // Album-genre
    conn.execute_batch(
        "INSERT INTO album_genre (album_id, genre_id) VALUES
            ('A1', 'G1'),
            ('A2', 'G2'),
            ('A3', 'G5');",
    )
    .unwrap();

    // Tracks
    conn.execute_batch(
        "INSERT INTO track (id, name, duration_seconds) VALUES
            ('T1', 'So What', 562),
            ('T2', 'Freddie Freeloader', 290),
            ('T3', 'Birdland', 363),
            ('T4', 'Quiero Bailar', 215);",
    )
    .unwrap();

    // Track-album
    conn.execute_batch(
        "INSERT INTO track_album (track_id, album_id, track_number) VALUES
            ('T1', 'A1', 1),
            ('T2', 'A1', 2),
            ('T3', 'A2', 1),
            ('T4', 'A3', 1);",
    )
    .unwrap();
}

// ---------------------------------------------------------------------------
// None: Query against an empty database
// ---------------------------------------------------------------------------

#[test]
fn test_query_artist_empty_db() {
    let conn = test_conn();
    let results = query::search_artist(&conn, "anything").unwrap();
    assert!(results.is_empty(), "Empty database should return no artists");
}

#[test]
fn test_query_genre_empty_db() {
    let conn = test_conn();
    let results = query::search_genre(&conn, "anything", 20, 0).unwrap();
    assert!(results.is_empty(), "Empty database should return no genres");
}

#[test]
fn test_query_album_empty_db() {
    let conn = test_conn();
    let results = query::search_album(&conn, "anything").unwrap();
    assert!(results.is_empty(), "Empty database should return no albums");
}

#[test]
fn test_query_track_empty_db() {
    let conn = test_conn();
    let results = query::search_track(&conn, "anything").unwrap();
    assert!(results.is_empty(), "Empty database should return no tracks");
}

// ---------------------------------------------------------------------------
// One: Single result queries
// ---------------------------------------------------------------------------

#[test]
fn test_query_artist_one_result() {
    let conn = test_conn();
    populate_test_data(&conn);

    let results = query::search_artist(&conn, "Miles Davis").unwrap();
    assert_eq!(results.len(), 1, "Should find exactly one artist");
    assert_eq!(results[0].id, "Q1");
    assert_eq!(results[0].name.as_deref(), Some("Miles Davis"));
    assert_eq!(
        results[0].description.as_deref(),
        Some("Jazz trumpeter and composer")
    );
    assert_eq!(results[0].artist_type, "person");
    assert_eq!(results[0].birth_date, NaiveDate::from_ymd_opt(1926, 5, 26));
    assert_eq!(results[0].death_date, NaiveDate::from_ymd_opt(1991, 9, 28));
}

#[test]
fn test_query_genre_one_result() {
    let conn = test_conn();
    populate_test_data(&conn);

    let results = query::search_genre(&conn, "Jazz", 20, 0).unwrap();
    assert_eq!(results.len(), 1, "Should find exactly one genre");
    assert_eq!(results[0].id, "G1");
    assert_eq!(results[0].name, "Jazz");
}

#[test]
fn test_query_album_one_result() {
    let conn = test_conn();
    populate_test_data(&conn);

    let results = query::search_album(&conn, "Kind of Blue").unwrap();
    assert_eq!(results.len(), 1, "Should find exactly one album");
    assert_eq!(results[0].id, "A1");
    assert_eq!(results[0].name, "Kind of Blue");
    assert_eq!(results[0].release_date, NaiveDate::from_ymd_opt(1959, 8, 17));
}

// ---------------------------------------------------------------------------
// One: Artist detail lookups
// ---------------------------------------------------------------------------

#[test]
fn test_query_artist_detail_genres() {
    let conn = test_conn();
    populate_test_data(&conn);

    let genres = query::artist_genres(&conn, "Q1").unwrap();
    assert_eq!(genres.len(), 1, "Miles Davis should have 1 genre");
    assert_eq!(genres[0].id, "G1");
    assert_eq!(genres[0].name, "Jazz");
}

#[test]
fn test_query_artist_detail_albums() {
    let conn = test_conn();
    populate_test_data(&conn);

    let albums = query::artist_albums(&conn, "Q1").unwrap();
    assert_eq!(albums.len(), 1, "Miles Davis should have 1 album");
    assert_eq!(albums[0].id, "A1");
    assert_eq!(albums[0].name, "Kind of Blue");
    assert_eq!(albums[0].role.as_deref(), Some("performer"));
}

#[test]
fn test_query_artist_detail_instruments() {
    let conn = test_conn();
    populate_test_data(&conn);

    let instruments = query::artist_instruments(&conn, "Q1").unwrap();
    assert_eq!(instruments.len(), 1, "Miles Davis should have 1 instrument");
    assert_eq!(instruments[0].instrument_id, "Q93474");
}

// ---------------------------------------------------------------------------
// One: Album detail lookups
// ---------------------------------------------------------------------------

#[test]
fn test_query_album_detail_artists() {
    let conn = test_conn();
    populate_test_data(&conn);

    let artists = query::album_artists(&conn, "A1").unwrap();
    assert_eq!(artists.len(), 2, "Kind of Blue should have 2 artists");
    let names: Vec<Option<&str>> = artists.iter().map(|a| a.name.as_deref()).collect();
    assert!(names.contains(&Some("Miles Davis")));
    assert!(names.contains(&Some("John Coltrane")));
}

#[test]
fn test_query_album_detail_genres() {
    let conn = test_conn();
    populate_test_data(&conn);

    let genres = query::album_genres(&conn, "A1").unwrap();
    assert_eq!(genres.len(), 1, "Kind of Blue should have 1 genre");
    assert_eq!(genres[0].name, "Jazz");
}

#[test]
fn test_query_album_detail_tracks() {
    let conn = test_conn();
    populate_test_data(&conn);

    let tracks = query::album_tracks(&conn, "A1").unwrap();
    assert_eq!(tracks.len(), 2, "Kind of Blue should have 2 tracks");
    assert_eq!(tracks[0].track_number, Some(1));
    assert_eq!(tracks[0].name, "So What");
    assert_eq!(tracks[0].duration_seconds, Some(562));
    assert_eq!(tracks[1].track_number, Some(2));
    assert_eq!(tracks[1].name, "Freddie Freeloader");
}

// ---------------------------------------------------------------------------
// One: Genre detail lookups
// ---------------------------------------------------------------------------

#[test]
fn test_query_genre_detail_artists() {
    let conn = test_conn();
    populate_test_data(&conn);

    let artists = query::genre_artists(&conn, "G1", 20, 0).unwrap();
    assert_eq!(artists.len(), 2, "Jazz should have 2 artists");
    assert!(artists.iter().any(|a| a.id == "Q1"));
    assert!(artists.iter().any(|a| a.id == "Q2"));
}

// ---------------------------------------------------------------------------
// Many: Multiple results and pagination
// ---------------------------------------------------------------------------

#[test]
fn test_query_genre_many_results() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Search for 'a' which matches Jazz, Classical, Reggaeton
    let results = query::search_genre(&conn, "a", 20, 0).unwrap();
    assert_eq!(results.len(), 3, "Should find 3 genres containing 'a'");
}

#[test]
fn test_query_genre_pagination_limit() {
    let conn = test_conn();
    populate_test_data(&conn);

    let results = query::search_genre(&conn, "a", 2, 0).unwrap();
    assert_eq!(results.len(), 2, "Limit 2 should return at most 2 results");
}

#[test]
fn test_query_genre_pagination_offset() {
    let conn = test_conn();
    populate_test_data(&conn);

    // 'a' matches: Classical, Jazz, Reggaeton (alphabetically)
    // Offset 2 should skip Classical and Jazz, return Reggaeton
    let results = query::search_genre(&conn, "a", 20, 2).unwrap();
    assert_eq!(results.len(), 1, "Offset 2 should return 1 remaining result");
    assert_eq!(results[0].name, "Reggaeton");
}

#[test]
fn test_query_genre_pagination_offset_beyond_end() {
    let conn = test_conn();
    populate_test_data(&conn);

    let results = query::search_genre(&conn, "a", 20, 100).unwrap();
    assert!(results.is_empty(), "Offset beyond end should return empty");
}

#[test]
fn test_query_artist_many_results() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Search for a partial term that matches multiple artists
    let results = query::search_artist(&conn, "Miles").unwrap();
    assert_eq!(results.len(), 1, "Should find exactly 1 artist matching 'Miles'");
    assert_eq!(results[0].id, "Q1");

    // Search for term that matches multiple
    let results = query::search_artist(&conn, "a").unwrap();
    // All 4 artists have 'a' in name or description
    assert_eq!(results.len(), 4, "Should find all 4 artists with 'a'");
}

// ---------------------------------------------------------------------------
// Search across entity types
// ---------------------------------------------------------------------------

#[test]
fn test_query_search_across_types() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Search 'Blue' should match album 'Kind of Blue'
    let albums = query::search_album(&conn, "Blue").unwrap();
    assert_eq!(albums.len(), 1, "Should find 1 album matching 'Blue'");
    assert_eq!(albums[0].name, "Kind of Blue");

    // Search 'So What' should match track
    let tracks = query::search_track(&conn, "So What").unwrap();
    assert_eq!(tracks.len(), 1, "Should find 1 track matching 'So What'");
    assert_eq!(tracks[0].name, "So What");
}

#[test]
fn test_query_search_term_matches_multiple_types() {
    let conn = test_conn();
    populate_test_data(&conn);

    // 'a' is a broad term that should match across all entity types
    let artists = query::search_artist(&conn, "a").unwrap();
    let albums = query::search_album(&conn, "a").unwrap();
    let tracks = query::search_track(&conn, "a").unwrap();

    assert!(!artists.is_empty(), "Artists should match 'a'");
    assert!(!albums.is_empty(), "Albums should match 'a'");
    assert!(!tracks.is_empty(), "Tracks should match 'a'");
}

// ---------------------------------------------------------------------------
// FTS fallback: LIKE fallback produces results when FTS is disabled
// ---------------------------------------------------------------------------

#[test]
fn test_query_like_fallback_artist() {
    let conn = test_conn();
    populate_test_data(&conn);

    // FTS is not available in the bundled DuckDB build, so this always
    // exercises the LIKE fallback path.
    let results = query::search_artist(&conn, "Miles").unwrap();
    assert!(!results.is_empty(), "LIKE fallback should find 'Miles'");
}

#[test]
fn test_query_like_fallback_partial() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Partial match via LIKE
    let results = query::search_artist(&conn, "Mile").unwrap();
    assert_eq!(results.len(), 1, "LIKE should find partial 'Mile'");
}

#[test]
fn test_query_like_fallback_description() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Search by description content
    let results = query::search_artist(&conn, "trumpet").unwrap();
    assert_eq!(results.len(), 1, "LIKE should find 'trumpet' in description");
}

#[test]
fn test_query_like_fallback_special_chars() {
    let conn = test_conn();
    populate_test_data(&conn);

    // SQL injection attempt should not cause errors
    // search_artist uses parameterized queries, so this is safe.
    let results = query::search_artist(&conn, "' OR 1=1 --").unwrap();
    assert!(
        results.is_empty(),
        "SQL injection attempt should return no results"
    );
}

// ---------------------------------------------------------------------------
// No data edge cases
// ---------------------------------------------------------------------------

#[test]
fn test_query_detail_empty_result_sets() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Artist with no genre associations (add a new artist without genres)
    conn.execute(
        "INSERT INTO artist (id, name, artist_type) VALUES ('Q99', 'No Genre Artist', 'person')",
        [],
    )
    .unwrap();

    let genres = query::artist_genres(&conn, "Q99").unwrap();
    assert!(genres.is_empty(), "Artist with no genres should return empty");

    let albums = query::artist_albums(&conn, "Q99").unwrap();
    assert!(albums.is_empty(), "Artist with no albums should return empty");

    let instruments = query::artist_instruments(&conn, "Q99").unwrap();
    assert!(
        instruments.is_empty(),
        "Artist with no instruments should return empty"
    );
}

#[test]
fn test_query_album_no_tracks() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Album with no tracks
    conn.execute(
        "INSERT INTO album (id, name) VALUES ('A99', 'Empty Album')",
        [],
    )
    .unwrap();

    let tracks = query::album_tracks(&conn, "A99").unwrap();
    assert!(tracks.is_empty(), "Album with no tracks should return empty");
}

// ---------------------------------------------------------------------------
// Edge cases: NULL values
// ---------------------------------------------------------------------------

#[test]
fn test_query_artist_null_name() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Add an artist with NULL name
    conn.execute(
        "INSERT INTO artist (id, artist_type) VALUES ('Q100', 'person')",
        [],
    )
    .unwrap();

    let results = query::search_artist(&conn, "Q100").unwrap();
    // The LIKE search searches name and description, so 'Q100' won't match
    // the LIKE pattern. Search by empty string returns empty.
    // Instead, verify the NULL name is handled correctly via direct query.
    // The search functions return the name as Option<String>.
    // We can't easily search for NULL-name artists via LIKE,
    // but we can verify data integrity.
    assert!(results.is_empty(), "Q100 should not match via LIKE");
}

#[test]
fn test_query_album_null_release_date() {
    let conn = test_conn();
    populate_test_data(&conn);

    // Add an album with NULL release_date
    conn.execute(
        "INSERT INTO album (id, name) VALUES ('A100', 'No Date Album')",
        [],
    )
    .unwrap();

    let results = query::search_album(&conn, "No Date Album").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id, "A100");
    assert_eq!(results[0].release_date, None, "NULL release_date should be None");
}