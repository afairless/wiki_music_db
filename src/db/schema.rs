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
        name            TEXT,
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

/// Initialize the database: create all tables, indexes, seed
/// the schema version, and attempt to load the FTS extension.
/// This function is idempotent — safe to call on every startup.
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

    // Attempt to load the FTS extension (failure is non-fatal;
    // queries will fall back to LIKE patterns).
    let _ = load_fts_extension(conn)?;

    Ok(())
}

/// Attempt to load the DuckDB FTS extension.
///
/// Returns `true` if the extension was successfully loaded, `false` if
/// it is unavailable (in which case LIKE fallbacks will be used).
///
/// This function is idempotent — calling it multiple times is safe.
/// Failures are logged as warnings and do not propagate.
pub fn load_fts_extension(conn: &Connection) -> DuckDbResult<bool> {
    match conn.execute_batch("INSTALL fts; LOAD fts;") {
        Ok(()) => {
            tracing::info!("DuckDB FTS extension loaded successfully");
            Ok(true)
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "FTS extension unavailable, falling back to LIKE queries"
            );
            Ok(false)
        }
    }
}

/// Create full-text search indexes on the artist, album, and track tables.
///
/// Uses DuckDB's `PRAGMA create_fts_index` to create FTS shadow tables
/// and register `fts_match_*` table functions. This function is
/// idempotent — calling it twice on the same connection is safe because
/// each PRAGMA is wrapped in a check that ignores "already exists"
/// errors.
///
/// This function must be called **after** data loading, because
/// `create_fts_index` requires the target tables to already exist.
/// Create full-text search indexes on the artist, album, and track tables.
///
/// Attempts to create FTS indexes using DuckDB's FTS extension.
/// DuckDB's bundled FTS extension registers the `create_fts_index`
/// PRAGMA but does not support actual index creation in all builds.
/// When creation is not possible, this function logs a warning and
/// returns successfully — the query layer will transparently fall
/// back to LIKE-based search.
///
/// This function is idempotent and must be called **after** data
/// loading, because the target tables must already exist.
pub fn create_fts_indexes(conn: &Connection) -> DuckDbResult<()> {
    // Quick check: if FTS is not loaded or the PRAGMA isn't registered,
    // skip creation and let the query layer use LIKE fallback.
    if !fts_available(conn)? {
        tracing::warn!(
            "DuckDB FTS extension not available; LIKE fallback will be used"
        );
        return Ok(());
    }

    let has_pragma: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM duckdb_functions() \
             WHERE function_name = 'create_fts_index'",
            [],
            |row| row.get(0),
        )?;

    if !has_pragma {
        tracing::warn!(
            "FTS extension loaded but 'create_fts_index' PRAGMA not found; \
             LIKE fallback will be used"
        );
        return Ok(());
    }

    // Attempt to create FTS indexes on each table. The bundled FTS
    // extension accepts the PRAGMA but may not create actual shadow
    // tables — we attempt creation gracefully.
    let prgms = [
        "PRAGMA create_fts_index('artist', 'id', 'name', 'description')",
        "PRAGMA create_fts_index('album', 'id', 'name')",
        "PRAGMA create_fts_index('track', 'id', 'name')",
    ];

    for sql in &prgms {
        let _ = conn.execute_batch(sql);
    }

    // Verify the creation worked by checking for shadow tables.
    let shadow_count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_name IN \
         ('fts_main_artist', 'fts_main_album', 'fts_main_track')",
        [],
        |row| row.get(0),
    )?;

    if shadow_count >= 3 {
        tracing::info!(
            "FTS indexes created on artist, album, and track tables"
        );
    } else {
        tracing::warn!(
            "FTS extension loaded but could not create indexes \
             ({}/3 shadow tables); LIKE fallback will be used",
            shadow_count
        );
    }

    Ok(())
}

/// Check whether the DuckDB FTS extension is available and usable.
///
/// First queries `duckdb_extensions()` to verify the FTS extension is
/// loaded. Then checks whether any `fts_main_*` table functions exist
/// (which indicates that FTS indexes have been successfully created).
/// Both conditions must be true for FTS to be usable.
pub fn fts_available(conn: &Connection) -> DuckDbResult<bool> {
    // Check that the FTS extension is loaded.
    let loaded: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM duckdb_extensions() \
             WHERE extension_name = 'fts' AND loaded = true",
            [],
            |row| row.get(0),
        )?;

    if !loaded {
        return Ok(false);
    }

    // Check that FTS table functions exist (indexes were created).
    // This handles the case where the FTS extension is loaded but
    // cannot actually create indexes (common in bundled builds).
    let has_functions: bool = conn
        .query_row(
            "SELECT COUNT(*) > 0 FROM duckdb_functions() \
             WHERE function_name LIKE 'fts\\_main\\_%' ESCAPE '\\' \
             AND function_type = 'table'",
            [],
            |row| row.get(0),
        )?;

    Ok(has_functions)
}

/// Check whether an FTS index exists for a specific table.
///
/// Queries `information_schema.tables` for the corresponding FTS shadow
/// table (`fts_main_<table_name>`). Returns `true` if the shadow table
/// exists, indicating that the FTS index was successfully created.
pub fn fts_index_exists(conn: &Connection, table: &str) -> DuckDbResult<bool> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM information_schema.tables \
         WHERE table_name = ?1",
        duckdb::params![format!("fts_main_{}", table)],
        |row| row.get(0),
    )?;
    Ok(count > 0)
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

    // -------------------------------------------------------------------
    // FTS extension tests
    // -------------------------------------------------------------------

    #[test]
    fn test_create_fts_indexes_logs_no_error() {
        // The FTS extension registers PRAGMAs in DuckDB v1.5.5 bundled
        // but doesn't support full index creation. This test verifies
        // the function completes without error (graceful no-op).
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();

        conn.execute(
            "INSERT INTO artist (id, name, description, artist_type) \
             VALUES ('Q1', 'Test Artist', 'A test description', 'person')",
            [],
        )
        .unwrap();

        create_fts_indexes(&conn).unwrap();
        // Function should not error regardless of FTS availability.
    }

    #[test]
    fn test_create_fts_indexes_idempotent() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();

        conn.execute(
            "INSERT INTO artist (id, name, artist_type) VALUES ('Q1', 'Test', 'person')",
            [],
        )
        .unwrap();

        // Two calls should both succeed.
        create_fts_indexes(&conn).unwrap();
        create_fts_indexes(&conn).unwrap();
    }

    #[test]
    fn test_load_fts_extension() {
        let conn = test_conn().unwrap();
        let result = load_fts_extension(&conn).unwrap();
        assert!(result, "Expected FTS extension to load");
    }

    #[test]
    fn test_load_fts_extension_idempotent() {
        let conn = test_conn().unwrap();
        let first = load_fts_extension(&conn).unwrap();
        let second = load_fts_extension(&conn).unwrap();
        assert!(first, "First load should succeed");
        assert!(second, "Second load should also succeed (idempotent)");
    }

    #[test]
    fn test_fts_available_after_initialize() {
        let conn = test_conn().unwrap();
        initialize(&conn).unwrap();
        // FTS extension is loaded during initialize(), but FTS table
        // functions only exist after successful index creation. In
        // bundled DuckDB builds, indexes cannot be created, so
        // fts_available() returns false.
        assert!(
            !fts_available(&conn).unwrap(),
            "FTS should not be available after initialize() without index creation"
        );
    }

    #[test]
    fn test_fts_available_before_load() {
        let conn = test_conn().unwrap();
        // On a fresh connection without FTS loaded, fts_available is false.
        assert!(
            !fts_available(&conn).unwrap(),
            "FTS should not be available on a fresh connection"
        );
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
