#![allow(dead_code)]

use duckdb::{Connection, Result as DuckDbResult};

/// Expected schema version for this application.
pub const SCHEMA_VERSION: i32 = 1;

/// The SQL statements to create the full database schema.
///
/// Statements are ordered to respect foreign-key dependencies:
/// core entities first, then join tables, then indexes.
const CREATE_TABLE_STATEMENTS: &[&str] = &[
    // Schema version tracking (migration support)
    "CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY
    )",
    // Core entity: person or group
    "CREATE TABLE IF NOT EXISTS artist (
        id              TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        description     TEXT,
        artist_type     TEXT NOT NULL,
        inclusion_reason TEXT,
        birth_date      DATE,
        death_date      DATE,
        updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )",
    // Genre taxonomy
    "CREATE TABLE IF NOT EXISTS genre (
        id   TEXT PRIMARY KEY,
        name TEXT NOT NULL
    )",
    // Many-to-many artist ↔ genre
    "CREATE TABLE IF NOT EXISTS artist_genre (
        artist_id TEXT NOT NULL REFERENCES artist(id),
        genre_id  TEXT NOT NULL REFERENCES genre(id),
        PRIMARY KEY (artist_id, genre_id)
    )",
    // Albums, EPs, singles, compilation albums
    "CREATE TABLE IF NOT EXISTS album (
        id           TEXT PRIMARY KEY,
        name         TEXT NOT NULL,
        release_date DATE,
        record_label TEXT,
        updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )",
    // Many-to-many album ↔ artist
    "CREATE TABLE IF NOT EXISTS album_artist (
        album_id  TEXT NOT NULL REFERENCES album(id),
        artist_id TEXT NOT NULL REFERENCES artist(id),
        role      TEXT,
        PRIMARY KEY (album_id, artist_id, role)
    )",
    // Many-to-many album ↔ genre
    "CREATE TABLE IF NOT EXISTS album_genre (
        album_id TEXT NOT NULL REFERENCES album(id),
        genre_id TEXT NOT NULL REFERENCES genre(id),
        PRIMARY KEY (album_id, genre_id)
    )",
    // Tracks (songs)
    "CREATE TABLE IF NOT EXISTS track (
        id               TEXT PRIMARY KEY,
        name             TEXT NOT NULL,
        duration_seconds INTEGER,
        updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )",
    // Many-to-many track ↔ album
    "CREATE TABLE IF NOT EXISTS track_album (
        track_id     TEXT NOT NULL REFERENCES track(id),
        album_id     TEXT NOT NULL REFERENCES album(id),
        track_number INTEGER,
        PRIMARY KEY (track_id, album_id)
    )",
    // Many-to-many track ↔ artist
    "CREATE TABLE IF NOT EXISTS track_artist (
        track_id  TEXT NOT NULL REFERENCES track(id),
        artist_id TEXT NOT NULL REFERENCES artist(id),
        role      TEXT,
        PRIMARY KEY (track_id, artist_id, role)
    )",
    // Instruments played by artists
    "CREATE TABLE IF NOT EXISTS artist_instrument (
        artist_id     TEXT NOT NULL REFERENCES artist(id),
        instrument_id TEXT NOT NULL,
        PRIMARY KEY (artist_id, instrument_id)
    )",
    // Group membership (person → group)
    "CREATE TABLE IF NOT EXISTS artist_member_of (
        artist_id TEXT NOT NULL REFERENCES artist(id),
        group_id  TEXT NOT NULL REFERENCES artist(id),
        PRIMARY KEY (artist_id, group_id)
    )",
];

/// Index statements that follow table creation.
const CREATE_INDEX_STATEMENTS: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_artist_name ON artist(name)",
    "CREATE INDEX IF NOT EXISTS idx_album_name ON album(name)",
    "CREATE INDEX IF NOT EXISTS idx_track_name ON track(name)",
    "CREATE INDEX IF NOT EXISTS idx_genre_name ON genre(name)",
];

/// Initialize the database: create all tables, indexes, and seed
/// the schema version. This function is idempotent — safe to call
/// on every startup.
pub fn initialize(conn: &Connection) -> DuckDbResult<()> {
    // Create all tables. Foreign-key enforcement is enabled by
    // default in DuckDB 1.x, so no explicit PRAGMA is needed.
    for stmt in CREATE_TABLE_STATEMENTS {
        conn.execute_batch(stmt)?;
    }

    // Create all indexes.
    for stmt in CREATE_INDEX_STATEMENTS {
        conn.execute_batch(stmt)?;
    }

    // Seed schema version if not already present.
    conn.execute(
        "INSERT OR IGNORE INTO schema_version (version) VALUES (?1)",
        duckdb::params![SCHEMA_VERSION],
    )?;

    Ok(())
}

/// Check whether all expected tables exist in the database.
pub fn all_tables_exist(conn: &Connection) -> DuckDbResult<bool> {
    let table_names = [
        "schema_version",
        "artist",
        "genre",
        "artist_genre",
        "album",
        "album_artist",
        "album_genre",
        "track",
        "track_album",
        "track_artist",
        "artist_instrument",
        "artist_member_of",
    ];

    for name in &table_names {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?1 AND table_schema = 'main'",
            duckdb::params![name],
            |row| row.get(0),
        )?;
        if count == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Check whether all expected indexes exist in the database.
pub fn all_indexes_exist(conn: &Connection) -> DuckDbResult<bool> {
    let index_names = [
        "idx_artist_name",
        "idx_album_name",
        "idx_track_name",
        "idx_genre_name",
    ];

    for name in &index_names {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM pg_catalog.pg_indexes WHERE indexname = ?1 AND schemaname = 'main'",
            duckdb::params![name],
            |row| row.get(0),
        )?;
        if count == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Return the current schema version, or None if the version table
/// is empty (pre-migration database).
pub fn schema_version(conn: &Connection) -> DuckDbResult<Option<i32>> {
    let version: Option<i32> = conn
        .query_row(
            "SELECT version FROM schema_version ORDER BY version DESC LIMIT 1",
            [],
            |row| row.get(0),
        )
        .ok();
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create an in-memory DuckDB for testing.
    fn test_conn() -> DuckDbResult<Connection> {
        let conn = Connection::open_in_memory()?;
        Ok(conn)
    }

    #[test]
    fn test_initialize_creates_tables() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        assert!(all_tables_exist(&conn).unwrap());
    }

    #[test]
    fn test_initialize_creates_indexes() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        assert!(all_indexes_exist(&conn).unwrap());
    }

    #[test]
    fn test_initialize_is_idempotent() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        initialize(&conn).unwrap(); // second call should not error
        assert!(all_tables_exist(&conn).unwrap());
        assert!(all_indexes_exist(&conn).unwrap());
    }

    #[test]
    fn test_schema_version_seeded() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        let version = schema_version(&conn).unwrap();
        assert_eq!(version, Some(SCHEMA_VERSION));
    }

    #[test]
    fn test_schema_version_insert_or_ignore() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        initialize(&conn).unwrap(); // second init
        let version = schema_version(&conn).unwrap();
        assert_eq!(version, Some(SCHEMA_VERSION));
    }

    #[test]
    fn test_tables_exist_empty_db() {
        let conn = test_conn().unwrap();
        // No initialization — tables should not exist
        assert!(!all_tables_exist(&conn).unwrap());
    }

    #[test]
    fn test_indexes_exist_empty_db() {
        let conn = test_conn().unwrap();
        assert!(!all_indexes_exist(&conn).unwrap());
    }

    #[test]
    fn test_foreign_keys_enforced() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        // Verify that foreign-key violations are rejected.
        let result = conn.execute(
            "INSERT INTO artist_genre (artist_id, genre_id) VALUES ('nonexistent', 'also_nonexistent')",
            [],
        );
        assert!(result.is_err());
    }
}
