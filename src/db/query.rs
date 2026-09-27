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
use duckdb::{Connection, params};

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
    /// Album name (the work's own English label, or a Q-ID mirror when no label or sitelink exists).
    pub name: String,
    /// Release date, or `None` if unavailable.
    pub release_date: Option<NaiveDate>,
}

/// A search result row from the track table.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackSearchResult {
    /// Wikidata ID (e.g., "Q67890").
    pub id: String,
    /// Track name (the work's own English label, or a Q-ID mirror when no label or sitelink exists).
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
        let rows = search_artist_fts(conn, term).context("FTS artist search failed")?;
        tracing::debug!(term, count = rows.len(), "FTS artist search");
        Ok(rows)
    } else {
        // LIKE fallback path
        let rows = search_artist_like(conn, term).context("LIKE artist search failed")?;
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
        let mut stmt =
            conn.prepare("SELECT id, name, release_date FROM album WHERE fts_match_album(?1)")?;

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
        let mut stmt =
            conn.prepare("SELECT id, name, duration_seconds FROM track WHERE fts_match_track(?1)")?;

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

// ---------------------------------------------------------------------------
// Detail lookup: artist relations
// ---------------------------------------------------------------------------

/// A genre result associated with an artist.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtistGenreResult {
    /// Wikidata ID of the genre.
    pub id: String,
    /// Genre name.
    pub name: String,
}

/// An album result associated with an artist.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtistAlbumResult {
    /// Wikidata ID of the album.
    pub id: String,
    /// Album name.
    pub name: String,
    /// Release date, or `None` if unavailable.
    pub release_date: Option<NaiveDate>,
    /// Role of the artist on this album (e.g., "performer", "producer").
    pub role: Option<String>,
}

/// An instrument result associated with an artist.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtistInstrumentResult {
    /// Wikidata Q-ID of the instrument.
    pub instrument_id: String,
}

/// Look up genres for a given artist.
pub fn artist_genres(conn: &Connection, artist_id: &str) -> Result<Vec<ArtistGenreResult>> {
    let mut stmt = conn.prepare(
        "SELECT g.id, g.name \
         FROM genre g \
         JOIN artist_genre ag ON g.id = ag.genre_id \
         WHERE ag.artist_id = ? \
         ORDER BY g.name",
    )?;

    let rows = stmt
        .query_map(params![artist_id], |row| {
            Ok(ArtistGenreResult {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// Look up albums for a given artist.
pub fn artist_albums(conn: &Connection, artist_id: &str) -> Result<Vec<ArtistAlbumResult>> {
    let mut stmt = conn.prepare(
        "SELECT a.id, a.name, a.release_date, aa.role \
         FROM album a \
         JOIN album_artist aa ON a.id = aa.album_id \
         WHERE aa.artist_id = ? \
         ORDER BY a.name",
    )?;

    let rows = stmt
        .query_map(params![artist_id], |row| {
            Ok(ArtistAlbumResult {
                id: row.get(0)?,
                name: row.get(1)?,
                release_date: row.get(2)?,
                role: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// Look up instruments played by a given artist.
pub fn artist_instruments(
    conn: &Connection,
    artist_id: &str,
) -> Result<Vec<ArtistInstrumentResult>> {
    let mut stmt = conn.prepare(
        "SELECT instrument_id \
         FROM artist_instrument \
         WHERE artist_id = ? \
         ORDER BY instrument_id",
    )?;

    let rows = stmt
        .query_map(params![artist_id], |row| {
            Ok(ArtistInstrumentResult {
                instrument_id: row.get(0)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Detail lookup: album relations
// ---------------------------------------------------------------------------

/// An artist result associated with an album.
#[derive(Debug, Clone, PartialEq)]
pub struct AlbumArtistResult {
    /// Wikidata ID of the artist.
    pub id: String,
    /// Artist name.
    pub name: Option<String>,
    /// Role of the artist on this album (e.g., "performer", "producer").
    pub role: Option<String>,
}

/// A genre result associated with an album.
#[derive(Debug, Clone, PartialEq)]
pub struct AlbumGenreResult {
    /// Wikidata ID of the genre.
    pub id: String,
    /// Genre name.
    pub name: String,
}

/// A track result associated with an album.
#[derive(Debug, Clone, PartialEq)]
pub struct AlbumTrackResult {
    /// Wikidata ID of the track.
    pub id: String,
    /// Track name.
    pub name: String,
    /// Duration in seconds, or `None` if unavailable.
    pub duration_seconds: Option<i32>,
    /// Track number on the album, or `None` if unavailable.
    pub track_number: Option<i32>,
}

/// Look up artists for a given album.
pub fn album_artists(conn: &Connection, album_id: &str) -> Result<Vec<AlbumArtistResult>> {
    let mut stmt = conn.prepare(
        "SELECT ar.id, ar.name, aa.role \
         FROM artist ar \
         JOIN album_artist aa ON ar.id = aa.artist_id \
         WHERE aa.album_id = ? \
         ORDER BY ar.name",
    )?;

    let rows = stmt
        .query_map(params![album_id], |row| {
            Ok(AlbumArtistResult {
                id: row.get(0)?,
                name: row.get(1)?,
                role: row.get(2)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// Look up genres for a given album.
pub fn album_genres(conn: &Connection, album_id: &str) -> Result<Vec<AlbumGenreResult>> {
    let mut stmt = conn.prepare(
        "SELECT g.id, g.name \
         FROM genre g \
         JOIN album_genre ag ON g.id = ag.genre_id \
         WHERE ag.album_id = ? \
         ORDER BY g.name",
    )?;

    let rows = stmt
        .query_map(params![album_id], |row| {
            Ok(AlbumGenreResult {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

/// Look up tracks for a given album, ordered by track number.
pub fn album_tracks(conn: &Connection, album_id: &str) -> Result<Vec<AlbumTrackResult>> {
    let mut stmt = conn.prepare(
        "SELECT t.id, t.name, t.duration_seconds, ta.track_number \
         FROM track t \
         JOIN track_album ta ON t.id = ta.track_id \
         WHERE ta.album_id = ? \
         ORDER BY ta.track_number",
    )?;

    let rows = stmt
        .query_map(params![album_id], |row| {
            Ok(AlbumTrackResult {
                id: row.get(0)?,
                name: row.get(1)?,
                duration_seconds: row.get(2)?,
                track_number: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Detail lookup: genre relations
// ---------------------------------------------------------------------------

/// An artist result associated with a genre.
#[derive(Debug, Clone, PartialEq)]
pub struct GenreArtistResult {
    /// Wikidata ID of the artist.
    pub id: String,
    /// Artist name, or `None` if unavailable.
    pub name: Option<String>,
    /// Artist description, or `None` if unavailable.
    pub description: Option<String>,
    /// Entity type ("person" or "group").
    pub artist_type: String,
}

/// Look up artists associated with a given genre, with pagination.
pub fn genre_artists(
    conn: &Connection,
    genre_id: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<GenreArtistResult>> {
    let mut stmt = conn.prepare(
        "SELECT ar.id, ar.name, ar.description, ar.artist_type \
         FROM artist ar \
         JOIN artist_genre ag ON ar.id = ag.artist_id \
         WHERE ag.genre_id = ? \
         ORDER BY ar.name \
         LIMIT ? OFFSET ?",
    )?;

    let rows = stmt
        .query_map(params![genre_id, limit as i64, offset as i64], |row| {
            Ok(GenreArtistResult {
                id: row.get(0)?,
                name: row.get(1)?,
                description: row.get(2)?,
                artist_type: row.get(3)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;

    Ok(rows)
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
        conn.execute("INSERT INTO track (id, name) VALUES ('T1', 'So What')", [])
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
        assert_eq!(results[0].name.as_deref(), Some("Miles Davis"));
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
        assert_eq!(
            results.len(),
            1,
            "LIKE should find 'trumpet' in description"
        );
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
        assert!(results.is_empty(), "Percent char should not cause error");
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
        assert!(results.is_empty(), "Percent char should not cause error");
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

        conn.execute("INSERT INTO genre (id, name) VALUES ('G100', 'Blues')", [])
            .unwrap();

        let results = search_genre(&conn, "Blues", 20, 0).unwrap();
        assert_eq!(results.len(), 1);

        let r = &results[0];
        assert_eq!(r.id, "G100");
        assert_eq!(r.name, "Blues");
    }

    // -------------------------------------------------------------------
    // Detail lookup tests
    // -------------------------------------------------------------------

    /// Helper: insert related data for detail lookup tests.
    fn insert_test_related_data(conn: &Connection) {
        // Artists
        conn.execute(
            "INSERT INTO artist (id, name, artist_type) VALUES \
             ('Q1', 'Miles Davis', 'person'),
             ('Q2', 'John Coltrane', 'person'),
             ('Q3', 'Weather Report', 'group')",
            [],
        )
        .unwrap();

        // Genres
        conn.execute(
            "INSERT INTO genre (id, name) VALUES \
             ('G1', 'Jazz'),
             ('G2', 'Fusion')",
            [],
        )
        .unwrap();

        // Artist-genre
        conn.execute(
            "INSERT INTO artist_genre (artist_id, genre_id) VALUES \
             ('Q1', 'G1'),
             ('Q2', 'G1'),
             ('Q3', 'G2')",
            [],
        )
        .unwrap();

        // Artist-instrument
        conn.execute(
            "INSERT INTO artist_instrument (artist_id, instrument_id) VALUES \
             ('Q1', 'Q93474'),  -- trumpet
             ('Q2', 'Q8349'),   -- saxophone
             ('Q3', 'Q171236')  -- keyboard
             ",
            [],
        )
        .unwrap();

        // Albums
        conn.execute(
            "INSERT INTO album (id, name, release_date) VALUES \
             ('A1', 'Kind of Blue', '1959-08-17'),
             ('A2', 'Heavy Weather', '1977-01-01')",
            [],
        )
        .unwrap();

        // Album-artist
        conn.execute(
            "INSERT INTO album_artist (album_id, artist_id, role) VALUES \
             ('A1', 'Q1', 'performer'),
             ('A1', 'Q2', 'performer'),
             ('A2', 'Q3', 'performer')",
            [],
        )
        .unwrap();

        // Album-genre
        conn.execute(
            "INSERT INTO album_genre (album_id, genre_id) VALUES \
             ('A1', 'G1'),
             ('A2', 'G2')",
            [],
        )
        .unwrap();

        // Tracks
        conn.execute(
            "INSERT INTO track (id, name, duration_seconds) VALUES \
             ('T1', 'So What', 562),
             ('T2', 'Freddie Freeloader', 290),
             ('T3', 'Birdland', 363)",
            [],
        )
        .unwrap();

        // Track-album
        conn.execute(
            "INSERT INTO track_album (track_id, album_id, track_number) VALUES \
             ('T1', 'A1', 1),
             ('T2', 'A1', 2),
             ('T3', 'A2', 1)",
            [],
        )
        .unwrap();
    }

    // -------------------------------------------------------------------
    // Artist detail lookup tests
    // -------------------------------------------------------------------

    #[test]
    fn test_artist_genres_returns_genres() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = artist_genres(&conn, "Q1").unwrap();
        assert_eq!(results.len(), 1, "Miles Davis should have 1 genre");
        assert_eq!(results[0].id, "G1");
        assert_eq!(results[0].name, "Jazz");
    }

    #[test]
    fn test_artist_genres_multiple_genres() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // Check that John Coltrane also has Jazz
        let results = artist_genres(&conn, "Q2").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Jazz");
    }

    #[test]
    fn test_artist_genres_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // Artist with no genre associations
        conn.execute(
            "INSERT INTO artist (id, name, artist_type) VALUES ('Q99', 'No Genre Artist', 'person')",
            [],
        )
        .unwrap();

        let results = artist_genres(&conn, "Q99").unwrap();
        assert!(
            results.is_empty(),
            "No genre associations should return empty"
        );
    }

    #[test]
    fn test_artist_albums_returns_albums() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = artist_albums(&conn, "Q1").unwrap();
        assert_eq!(results.len(), 1, "Miles Davis should have 1 album");
        assert_eq!(results[0].id, "A1");
        assert_eq!(results[0].name, "Kind of Blue");
        assert_eq!(results[0].role.as_deref(), Some("performer"));
    }

    #[test]
    fn test_artist_albums_multiple_artists_same_album() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // John Coltrane also appears on Kind of Blue
        let results = artist_albums(&conn, "Q2").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "Kind of Blue");
    }

    #[test]
    fn test_artist_albums_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        conn.execute(
            "INSERT INTO artist (id, name, artist_type) VALUES ('Q99', 'No Album Artist', 'person')",
            [],
        )
        .unwrap();

        let results = artist_albums(&conn, "Q99").unwrap();
        assert!(
            results.is_empty(),
            "No album associations should return empty"
        );
    }

    #[test]
    fn test_artist_instruments_returns_instruments() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = artist_instruments(&conn, "Q1").unwrap();
        assert_eq!(results.len(), 1, "Miles Davis should have 1 instrument");
        assert_eq!(results[0].instrument_id, "Q93474");
    }

    #[test]
    fn test_artist_instruments_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // An artist with no instruments
        conn.execute(
            "INSERT INTO artist (id, name, artist_type) VALUES ('Q99', 'No Instrument Artist', 'person')",
            [],
        )
        .unwrap();

        let results = artist_instruments(&conn, "Q99").unwrap();
        assert!(results.is_empty(), "No instruments should return empty");
    }

    // -------------------------------------------------------------------
    // Album detail lookup tests
    // -------------------------------------------------------------------

    #[test]
    fn test_album_artists_returns_artists() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = album_artists(&conn, "A1").unwrap();
        assert_eq!(results.len(), 2, "Kind of Blue should have 2 artists");
        // Sorted by name alphabetically: John Coltrane (Q2) before Miles Davis (Q1)
        assert_eq!(results[0].id, "Q2");
        assert_eq!(results[0].name.as_deref(), Some("John Coltrane"));
        assert_eq!(results[0].role.as_deref(), Some("performer"));
        assert_eq!(results[1].id, "Q1");
        assert_eq!(results[1].name.as_deref(), Some("Miles Davis"));
    }

    #[test]
    fn test_album_artists_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        conn.execute(
            "INSERT INTO album (id, name) VALUES ('A99', 'Unknown Album')",
            [],
        )
        .unwrap();

        let results = album_artists(&conn, "A99").unwrap();
        assert!(
            results.is_empty(),
            "No artist associations should return empty"
        );
    }

    #[test]
    fn test_album_genres_returns_genres() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = album_genres(&conn, "A1").unwrap();
        assert_eq!(results.len(), 1, "Kind of Blue should have 1 genre");
        assert_eq!(results[0].id, "G1");
        assert_eq!(results[0].name, "Jazz");
    }

    #[test]
    fn test_album_genres_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        conn.execute(
            "INSERT INTO album (id, name) VALUES ('A99', 'Unknown Album')",
            [],
        )
        .unwrap();

        let results = album_genres(&conn, "A99").unwrap();
        assert!(
            results.is_empty(),
            "No genre associations should return empty"
        );
    }

    #[test]
    fn test_album_tracks_returns_tracks_ordered() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = album_tracks(&conn, "A1").unwrap();
        assert_eq!(results.len(), 2, "Kind of Blue should have 2 tracks");
        assert_eq!(results[0].id, "T1");
        assert_eq!(results[0].name, "So What");
        assert_eq!(results[0].duration_seconds, Some(562));
        assert_eq!(results[0].track_number, Some(1));
        assert_eq!(results[1].id, "T2");
        assert_eq!(results[1].name, "Freddie Freeloader");
        assert_eq!(results[1].track_number, Some(2));
    }

    #[test]
    fn test_album_tracks_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        conn.execute(
            "INSERT INTO album (id, name) VALUES ('A99', 'Unknown Album')",
            [],
        )
        .unwrap();

        let results = album_tracks(&conn, "A99").unwrap();
        assert!(
            results.is_empty(),
            "No track associations should return empty"
        );
    }

    // -------------------------------------------------------------------
    // Genre detail lookup tests
    // -------------------------------------------------------------------

    #[test]
    fn test_genre_artists_returns_artists() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = genre_artists(&conn, "G1", 20, 0).unwrap();
        assert_eq!(results.len(), 2, "Jazz should have 2 artists");
        // Sorted by name alphabetically: John Coltrane (Q2) before Miles Davis (Q1)
        assert_eq!(results[0].id, "Q2");
        assert_eq!(results[0].name.as_deref(), Some("John Coltrane"));
        assert_eq!(results[0].artist_type, "person");
        assert_eq!(results[1].id, "Q1");
        assert_eq!(results[1].name.as_deref(), Some("Miles Davis"));
    }

    #[test]
    fn test_genre_artists_pagination_limit() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        let results = genre_artists(&conn, "G1", 1, 0).unwrap();
        assert_eq!(results.len(), 1, "Limit should cap results at 1");
        assert_eq!(results[0].name.as_deref(), Some("John Coltrane"));
    }

    #[test]
    fn test_genre_artists_pagination_offset() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // Offset 1 should skip 'John Coltrane', return 'Miles Davis'
        let results = genre_artists(&conn, "G1", 20, 1).unwrap();
        assert_eq!(results.len(), 1, "Offset 1 should skip first result");
        assert_eq!(
            results[0].name.as_deref(),
            Some("Miles Davis"),
            "Miles Davis is alphabetically second"
        );
    }

    #[test]
    fn test_genre_artists_empty() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        conn.execute(
            "INSERT INTO genre (id, name) VALUES ('G99', 'Unknown Genre')",
            [],
        )
        .unwrap();

        let results = genre_artists(&conn, "G99", 20, 0).unwrap();
        assert!(
            results.is_empty(),
            "No artist associations should return empty"
        );
    }

    #[test]
    fn test_genre_artists_null_name() {
        let conn = test_conn();
        insert_test_related_data(&conn);

        // Add an artist with NULL name
        conn.execute(
            "INSERT INTO artist (id, artist_type) VALUES ('Q100', 'person')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO artist_genre (artist_id, genre_id) VALUES ('Q100', 'G1')",
            [],
        )
        .unwrap();

        let results = genre_artists(&conn, "G1", 20, 0).unwrap();
        assert_eq!(results.len(), 3, "Should include NULL-name artist");
        // NULL-name artist should be present
        assert!(results.iter().any(|r| r.id == "Q100"));
        assert!(results.iter().any(|r| r.name.is_none()));
    }
}
