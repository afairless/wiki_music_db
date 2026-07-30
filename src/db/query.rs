//! Query helper functions with FTS/LIKE fallback.
//!
//! All search functions check whether the DuckDB FTS extension is
//! available (via [`crate::db::schema::fts_available`]) and use FTS
//! queries when possible, falling back to `LIKE '%term%'` when FTS
//! is not available.
//!
//! # Current status
//!
//! The DuckDB bundled build (v1.5.x) loads the FTS extension but does
//! not support actual index creation. As a result, `fts_available()`
//! returns `false` after schema initialization, and all searches use
//! the LIKE fallback path. This is transparent to callers — the
//! fallback is automatic and logged at debug level.

use chrono::NaiveDate;
use duckdb::{params, Connection};

use crate::db::schema::{fts_available, fts_index_exists};
use anyhow::{Context, Result};

/// A search result row from the artist table.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtistSearchResult {
    /// Wikidata ID (e.g., "Q2831").
    pub id: String,
    /// English label, or `None` if unavailable.
    pub name: Option<String>,
    /// English description, or `None` if unavailable.
    pub description: Option<String>,
    /// Entity type ("person" or "group").
    pub artist_type: String,
    /// Birth date, or `None` if unavailable or unparseable.
    pub birth_date: Option<NaiveDate>,
    /// Death date, or `None` if unavailable or unparseable.
    pub death_date: Option<NaiveDate>,
}

/// A search result row from the album table.
#[derive(Debug, Clone, PartialEq)]
pub struct AlbumSearchResult {
    /// Wikidata ID (e.g., "Q12345").
    pub id: String,
    /// Album name (currently a Q-ID placeholder for unresolved albums).
    pub name: String,
    /// Release date, or `None` if unavailable.
    pub release_date: Option<NaiveDate>,
}

/// A search result row from the track table.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackSearchResult {
    /// Wikidata ID (e.g., "Q67890").
    pub id: String,
    /// Track name (currently a Q-ID placeholder for unresolved tracks).
    pub name: String,
    /// Duration in seconds, or `None` if unavailable.
    pub duration_seconds: Option<i32>,
}

// ---------------------------------------------------------------------------
// Artist search
// ---------------------------------------------------------------------------

const LIKE_SEARCH_ARTIST: &str = "\
SELECT id, name, description, artist_type, birth_date, death_date
FROM artist
WHERE name LIKE '%' || ?1 || '%'
   OR description LIKE '%' || ?1 || '%'
ORDER BY name
LIMIT 100";

/// Search artists by name using FTS when available, falling back to LIKE.
///
/// When the DuckDB FTS extension is available and indexes have been
/// created, this function uses `fts_match_artist()` for search.
/// Otherwise, it uses `LIKE '%term%'` on both `name` and `description`
/// columns, bounded by `LIMIT 100`.
///
/// # Arguments
///
/// * `conn` — A reference to an open DuckDB connection.
/// * `term` — The search term. An empty string returns no results.
///
/// # Returns
///
/// A vector of matching [`ArtistSearchResult`] rows, sorted by name.
/// Returns an empty vec when no matches are found.
pub fn search_artist(conn: &Connection, term: &str) -> Result<Vec<ArtistSearchResult>> {
    if term.is_empty() {
        return Ok(Vec::new());
    }

    if fts_available(conn)? && fts_index_exists(conn, "artist")? {
        // FTS query path
        let rows = search_artist_fts(conn, term)
            .context("FTS artist search failed")?;
        tracing::debug!(term, count = rows.len(), "FTS artist search");
        Ok(rows)
    } else {
        // LIKE fallback path
        let rows = search_artist_like(conn, term)
            .context("LIKE artist search failed")?;
        tracing::debug!(term, count = rows.len(), "LIKE artist search");
        Ok(rows)
    }
}

/// FTS-based artist search.
fn search_artist_fts(conn: &Connection, term: &str) -> Result<Vec<ArtistSearchResult>> {
    let mut stmt = conn.prepare(
        "SELECT id, name, description, artist_type, birth_date, death_date \
         FROM artist WHERE fts_match_artist(?1)",
    )?;

    let rows = stmt
        .query_map(params![term], |row| {
            Ok(ArtistSearchResult {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                artist_type: row.get(3)?,
                birth_date: row.get(4)?,
                death_date: row.get(5)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// LIKE-based artist search (fallback).
fn search_artist_like(conn: &Connection, term: &str) -> Result<Vec<ArtistSearchResult>> {
    let mut stmt = conn.prepare(LIKE_SEARCH_ARTIST)?;

    let rows = stmt
        .query_map(params![term], |row| {
            Ok(ArtistSearchResult {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                artist_type: row.get(3)?,
                birth_date: row.get(4)?,
                death_date: row.get(5)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Album search
// ---------------------------------------------------------------------------

const LIKE_SEARCH_ALBUM: &str = "\
SELECT id, name, release_date
FROM album
WHERE name LIKE '%' || ?1 || '%'
ORDER BY name
LIMIT 100";

/// Search albums by name using FTS when available, falling back to LIKE.
///
/// See [`search_artist`] for detailed behavior.
pub fn search_album(conn: &Connection, term: &str) -> Result<Vec<AlbumSearchResult>> {
    if term.is_empty() {
        return Ok(Vec::new());
    }

    if fts_available(conn)? && fts_index_exists(conn, "album")? {
        let mut stmt = conn.prepare(
            "SELECT id, name, release_date FROM album WHERE fts_match_album(?1)",
        )?;

        let rows = stmt
            .query_map(params![term], |row| {
                Ok(AlbumSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    release_date: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "FTS album search");
        Ok(rows)
    } else {
        let mut stmt = conn.prepare(LIKE_SEARCH_ALBUM)?;

        let rows = stmt
            .query_map(params![term], |row| {
                Ok(AlbumSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    release_date: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "LIKE album search");
        Ok(rows)
    }
}

// ---------------------------------------------------------------------------
// Track search
// ---------------------------------------------------------------------------

const LIKE_SEARCH_TRACK: &str = "\
SELECT id, name, duration_seconds
FROM track
WHERE name LIKE '%' || ?1 || '%'
ORDER BY name
LIMIT 100";

/// Search tracks by name using FTS when available, falling back to LIKE.
///
/// See [`search_artist`] for detailed behavior.
pub fn search_track(conn: &Connection, term: &str) -> Result<Vec<TrackSearchResult>> {
    if term.is_empty() {
        return Ok(Vec::new());
    }

    if fts_available(conn)? && fts_index_exists(conn, "track")? {
        let mut stmt = conn.prepare(
            "SELECT id, name, duration_seconds FROM track WHERE fts_match_track(?1)",
        )?;

        let rows = stmt
            .query_map(params![term], |row| {
                Ok(TrackSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    duration_seconds: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "FTS track search");
        Ok(rows)
    } else {
        let mut stmt = conn.prepare(LIKE_SEARCH_TRACK)?;

        let rows = stmt
            .query_map(params![term], |row| {
                Ok(TrackSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    duration_seconds: row.get(2)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "LIKE track search");
        Ok(rows)
    }
}

/// A search result row from the genre table.
#[derive(Debug, Clone, PartialEq)]
pub struct GenreSearchResult {
    /// Wikidata ID (e.g., "Q35718").
    pub id: String,
    /// Genre name (e.g., "jazz").
    pub name: String,
}

// ---------------------------------------------------------------------------
// Genre search
// ---------------------------------------------------------------------------

const LIKE_SEARCH_GENRE: &str = "\
SELECT id, name
FROM genre
WHERE name LIKE '%' || ?1 || '%'
ORDER BY name
LIMIT ?
OFFSET ?";

/// Search genres by name using FTS when available, falling back to LIKE.
///
/// Unlike [`search_artist`], this function accepts pagination parameters
/// (`limit` and `offset`) to support browsing large genre result sets.
///
/// # Arguments
///
/// * `conn` — A reference to an open DuckDB connection.
/// * `term` — The search term. An empty string returns no results.
/// * `limit` — Maximum number of results to return.
/// * `offset` — Number of results to skip (for pagination).
///
/// # Returns
///
/// A vector of matching [`GenreSearchResult`] rows, sorted by name.
/// Returns an empty vec when no matches are found.
pub fn search_genre(
    conn: &Connection,
    term: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<GenreSearchResult>> {
    if term.is_empty() {
        return Ok(Vec::new());
    }

    if fts_available(conn)? && fts_index_exists(conn, "genre")? {
        let mut stmt = conn.prepare(
            "SELECT id, name FROM genre WHERE fts_match_genre(?1) ORDER BY name LIMIT ? OFFSET ?",
        )?;

        let rows = stmt
            .query_map(params![term, limit as i64, offset as i64], |row| {
                Ok(GenreSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "FTS genre search");
        Ok(rows)
    } else {
        let mut stmt = conn.prepare(LIKE_SEARCH_GENRE)?;

        let rows = stmt
            .query_map(params![term, limit as i64, offset as i64], |row| {
                Ok(GenreSearchResult {
                    id: row.get(0)?,
                    name: row.get(1)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        tracing::debug!(term, count = rows.len(), "LIKE genre search");
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;

    /// Helper: create an in-memory DuckDB with full schema initialized.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize(&conn).unwrap();
        conn
    }

    /// Helper: insert a test artist.
    fn insert_test_artist(conn: &Connection) {
        conn.execute(
            "INSERT INTO artist (id, name, description, artist_type) \
             VALUES ('Q1', 'Miles Davis', 'Jazz trumpeter and composer', 'person')",
            [],
        )
        .unwrap();
    }

    /// Helper: insert a test album.
    fn insert_test_album(conn: &Connection) {
        conn.execute(
            "INSERT INTO album (id, name) VALUES ('A1', 'Kind of Blue')",
            [],
        )
        .unwrap();
    }

    /// Helper: insert a test track.
    fn insert_test_track(conn: &Connection) {
        conn.execute(
            "INSERT INTO track (id, name) VALUES ('T1', 'So What')",
            [],
        )
        .unwrap();
    }

    // -------------------------------------------------------------------
    // LIKE fallback path tests (FTS not available in bundled build)
    // -------------------------------------------------------------------

    #[test]
    fn test_search_artist_like_fallback() {
        let conn = test_conn();
        insert_test_artist(&conn);

        let results = search_artist(&conn, "Miles").unwrap();
        assert_eq!(results.len(), 1, "LIKE should find 'Miles'");
        assert_eq!(results[0].id, "Q1");
        assert_eq!(
            results[0].name.as_deref(),
            Some("Miles Davis")
        );
    }

    #[test]
    fn test_search_artist_like_fallback_partial() {
        let conn = test_conn();
        insert_test_artist(&conn);

        // Partial match via LIKE (substring)
        let results = search_artist(&conn, "Mile").unwrap();
        assert_eq!(results.len(), 1, "LIKE should find partial 'Mile'");
    }

    #[test]
    fn test_search_artist_like_fallback_description() {
        let conn = test_conn();
        insert_test_artist(&conn);

        // Search by description content
        let results = search_artist(&conn, "trumpet").unwrap();
        assert_eq!(results.len(), 1, "LIKE should find 'trumpet' in description");
    }

    #[test]
    fn test_search_artist_like_fallback_no_match() {
        let conn = test_conn();
        insert_test_artist(&conn);

        let results = search_artist(&conn, "Nonexistent").unwrap();
        assert!(results.is_empty(), "No match should return empty vec");
    }

    #[test]
    fn test_search_artist_empty_term() {
        let conn = test_conn();
        insert_test_artist(&conn);

        let results = search_artist(&conn, "").unwrap();
        assert!(results.is_empty(), "Empty term should return empty vec");
    }

    #[test]
    fn test_search_artist_special_chars() {
        let conn = test_conn();
        insert_test_artist(&conn);

        // SQL-special characters should not cause errors.
        let results = search_artist(&conn, "' OR 1=1 --").unwrap();
        assert!(
            results.is_empty(),
            "SQL injection attempt should return no results"
        );

        let results = search_artist(&conn, "100%").unwrap();
        assert!(
            results.is_empty(),
            "Percent char should not cause error"
        );
    }

    #[test]
    fn test_search_album_like_fallback() {
        let conn = test_conn();
        insert_test_album(&conn);

        let results = search_album(&conn, "Blue").unwrap();
        assert_eq!(results.len(), 1, "LIKE should find 'Blue' in album name");
        assert_eq!(results[0].id, "A1");
    }

    #[test]
    fn test_search_album_like_fallback_no_match() {
        let conn = test_conn();
        insert_test_album(&conn);

        let results = search_album(&conn, "Nonexistent").unwrap();
        assert!(results.is_empty(), "No match should return empty vec");
    }

    #[test]
    fn test_search_album_empty_term() {
        let conn = test_conn();
        insert_test_album(&conn);

        let results = search_album(&conn, "").unwrap();
        assert!(results.is_empty(), "Empty term should return empty vec");
    }

    #[test]
    fn test_search_track_like_fallback() {
        let conn = test_conn();
        insert_test_track(&conn);

        let results = search_track(&conn, "What").unwrap();
        assert_eq!(results.len(), 1, "LIKE should find 'What' in track name");
        assert_eq!(results[0].id, "T1");
    }

    #[test]
    fn test_search_track_like_fallback_no_match() {
        let conn = test_conn();
        insert_test_track(&conn);

        let results = search_track(&conn, "Nonexistent").unwrap();
        assert!(results.is_empty(), "No match should return empty vec");
    }

    #[test]
    fn test_search_track_empty_term() {
        let conn = test_conn();
        insert_test_track(&conn);

        let results = search_track(&conn, "").unwrap();
        assert!(results.is_empty(), "Empty term should return empty vec");
    }

    #[test]
    fn test_search_no_data() {
        let conn = test_conn();

        // No artist data — search should return empty.
        let results = search_artist(&conn, "anything").unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_result_fields() {
        let conn = test_conn();

        // Insert artist with all fields populated.
        conn.execute(
            "INSERT INTO artist (id, name, description, artist_type, birth_date, death_date) \
             VALUES ('Q100', 'John Coltrane', 'Jazz saxophonist', 'person', '1926-09-23', '1967-07-17')",
            [],
        )
        .unwrap();

        let results = search_artist(&conn, "Coltrane").unwrap();
        assert_eq!(results.len(), 1);

        let r = &results[0];
        assert_eq!(r.id, "Q100");
        assert_eq!(r.name.as_deref(), Some("John Coltrane"));
        assert_eq!(r.description.as_deref(), Some("Jazz saxophonist"));
        assert_eq!(r.artist_type, "person");
        assert_eq!(r.birth_date, NaiveDate::from_ymd_opt(1926, 9, 23));
        assert_eq!(r.death_date, NaiveDate::from_ymd_opt(1967, 7, 17));
    }

    // -------------------------------------------------------------------
    // Genre search tests
    // -------------------------------------------------------------------

    /// Helper: insert test genres.
    fn insert_test_genres(conn: &Connection) {
        conn.execute("INSERT INTO genre (id, name) VALUES ('G1', 'Jazz')", [])
            .unwrap();
        conn.execute("INSERT INTO genre (id, name) VALUES ('G2', 'Rock')", [])
            .unwrap();
        conn.execute(
            "INSERT INTO genre (id, name) VALUES ('G3', 'Classical')",
            [],
        )
        .unwrap();
    }

    #[test]
    fn test_search_genre_like_fallback() {
        let conn = test_conn();
        insert_test_genres(&conn);

        let results = search_genre(&conn, "Jazz", 20, 0).unwrap();
        assert_eq!(results.len(), 1, "LIKE should find 'Jazz'");
        assert_eq!(results[0].id, "G1");
        assert_eq!(results[0].name, "Jazz");
    }

    #[test]
    fn test_search_genre_partial_match() {
        let conn = test_conn();
        insert_test_genres(&conn);

        let results = search_genre(&conn, "Jaz", 20, 0).unwrap();
        assert_eq!(results.len(), 1, "LIKE should find partial 'Jaz'");
        assert_eq!(results[0].id, "G1");
    }

    #[test]
    fn test_search_genre_no_match() {
        let conn = test_conn();
        insert_test_genres(&conn);

        let results = search_genre(&conn, "Nonexistent", 20, 0).unwrap();
        assert!(results.is_empty(), "No match should return empty vec");
    }

    #[test]
    fn test_search_genre_empty_term() {
        let conn = test_conn();
        insert_test_genres(&conn);

        let results = search_genre(&conn, "", 20, 0).unwrap();
        assert!(results.is_empty(), "Empty term should return empty vec");
    }

    #[test]
    fn test_search_genre_pagination_limit() {
        let conn = test_conn();
        insert_test_genres(&conn);

        // Limit to 1 result
        let results = search_genre(&conn, "a", 1, 0).unwrap();
        assert_eq!(results.len(), 1, "Limit should cap results at 1");
        // Should return 'Classical' (alphabetically first among matches)
        assert_eq!(results[0].name, "Classical");
    }

    #[test]
    fn test_search_genre_pagination_offset() {
        let conn = test_conn();
        insert_test_genres(&conn);

        // 'a' matches 'Classical' and 'Jazz' (alphabetical order).
        // Offset 1 should skip 'Classical', return only 'Jazz'.
        let results = search_genre(&conn, "a", 20, 1).unwrap();
        assert_eq!(results.len(), 1, "Offset 1 should skip first result");
        assert_eq!(results[0].name, "Jazz");
    }

    #[test]
    fn test_search_genre_pagination_offset_beyond_end() {
        let conn = test_conn();
        insert_test_genres(&conn);

        // Offset beyond total count should return empty
        let results = search_genre(&conn, "a", 20, 100).unwrap();
        assert!(results.is_empty(), "Offset beyond end should return empty");
    }

    #[test]
    fn test_search_genre_special_chars() {
        let conn = test_conn();
        insert_test_genres(&conn);

        // SQL-special characters should not cause errors.
        let results = search_genre(&conn, "' OR 1=1 --", 20, 0).unwrap();
        assert!(
            results.is_empty(),
            "SQL injection attempt should return no results"
        );

        let results = search_genre(&conn, "100%", 20, 0).unwrap();
        assert!(
            results.is_empty(),
            "Percent char should not cause error"
        );
    }

    #[test]
    fn test_search_genre_no_data() {
        let conn = test_conn();

        // No genre data — search should return empty.
        let results = search_genre(&conn, "anything", 20, 0).unwrap();
        assert!(results.is_empty());
    }

    #[test]
    fn test_search_genre_result_fields() {
        let conn = test_conn();

        conn.execute(
            "INSERT INTO genre (id, name) VALUES ('G100', 'Blues')",
            [],
        )
        .unwrap();

        let results = search_genre(&conn, "Blues", 20, 0).unwrap();
        assert_eq!(results.len(), 1);

        let r = &results[0];
        assert_eq!(r.id, "G100");
        assert_eq!(r.name, "Blues");
    }
}