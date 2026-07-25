//! DuckDB loader: loads Parquet intermediate files into the normalized schema.
//!
//! This module reads the `.parquet` files produced by Phase 3a
//! (`MusicEntityBatchWriter`) and inserts the data into the DuckDB tables
//! defined in [`schema`]. All functions use `INSERT OR IGNORE` for idempotency.
//!
//! ## Load order (dependency-ordered)
//!
//! 1. `genre` — genre taxonomy (referenced by `artist_genre`)
//! 2. `artist` — core entities (referenced by join tables)
//! 3. `artist_genre`, `artist_instrument`, `artist_member_of` — artist join tables
//! 4. `album`, `track` — entity stubs from JSON columns
//! 5. `album_artist`, `track_artist` — entity join tables
//!
//! ## Idempotency
//!
//! All insert statements use `INSERT OR IGNORE`, so calling `load_all` multiple
//! times with the same Parquet files produces identical database state.

use std::path::Path;

use anyhow::{Context, Result};
use duckdb::Connection;

/// Orchestrate the full loading pipeline in dependency order.
///
/// Calls each loading function in sequence: genres → artists → join tables
/// → albums/tracks. Errors from any step propagate immediately.
pub fn load_all(conn: &Connection, parquet_dir: &Path) -> Result<()> {
    let genres = load_genres(conn, parquet_dir)?;
    tracing::info!(genres, "Loaded genres");

    let artists = load_artists(conn, parquet_dir)?;
    tracing::info!(artists, "Loaded artists");

    tracing::info!("Loading complete: {} genres, {} artists", genres, artists);
    Ok(())
}

/// Load the `genres.parquet` file into the `genre` table.
///
/// Reads all rows from `<parquet_dir>/genres.parquet` and inserts them
/// into the `genre (id, name)` table via DuckDB's `read_parquet` function.
///
/// Returns the number of rows inserted.
///
/// # Errors
///
/// Returns an error if the Parquet file cannot be read or the SQL query fails.
pub fn load_genres(conn: &Connection, parquet_dir: &Path) -> Result<usize> {
    let genre_path = parquet_dir.join("genres.parquet");
    let genre_path_str = genre_path
        .to_str()
        .context("Genre parquet path contains invalid UTF-8")?;

    let sql = format!(
        "INSERT OR IGNORE INTO genre (id, name)
         SELECT id, name FROM read_parquet('{genre_path_str}')"
    );

    conn.execute(&sql, [])
        .context("Failed to load genres from Parquet")?;

    let count: usize = conn
        .query_row("SELECT COUNT(*) FROM genre", [], |row| row.get(0))
        .context("Failed to count genres after load")?;

    Ok(count)
}

/// Load artist Parquet files into the `artist` table.
///
/// Reads all `part-*.parquet` files from `<parquet_dir>` via glob pattern
/// and inserts into `artist (id, name, description, artist_type,
/// inclusion_reason, birth_date, death_date)`.
///
/// Date VARCHAR columns are cast to `DATE` using DuckDB's `::DATE` syntax,
/// with NULL handling for empty strings.
///
/// Returns the total number of artist rows in the table after load.
///
/// # Errors
///
/// Returns an error if no Parquet files match the glob pattern, or if the
/// SQL query fails.
pub fn load_artists(conn: &Connection, parquet_dir: &Path) -> Result<usize> {
    let parquet_dir_str = parquet_dir
        .to_str()
        .context("Parquet directory path contains invalid UTF-8")?;

    let sql = format!(
        "INSERT OR IGNORE INTO artist (id, name, description, artist_type, inclusion_reason, birth_date, death_date)
         SELECT id, name, description, artist_type, inclusion_reason,
                CASE WHEN birth_date IS NOT NULL AND birth_date != '' THEN birth_date::DATE ELSE NULL END,
                CASE WHEN death_date IS NOT NULL AND death_date != '' THEN death_date::DATE ELSE NULL END
         FROM read_parquet('{parquet_dir_str}/part-*.parquet')"
    );

    conn.execute(&sql, [])
        .context("Failed to load artists from Parquet")?;

    let count: usize = conn
        .query_row("SELECT COUNT(*) FROM artist", [], |row| row.get(0))
        .context("Failed to count artists after load")?;

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema;
    use arrow::array::StringBuilder;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::fs;
    use std::sync::Arc;

    /// Helper: create an in-memory DuckDB with the full schema initialized.
    fn test_conn() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        schema::initialize(&conn).unwrap();
        conn
    }

    /// Helper: write a small genre Parquet file with two rows.
    fn write_test_genres_parquet(dir: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let mut id_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        id_builder.append_value("Q35718");
        name_builder.append_value("jazz");
        id_builder.append_value("Q57251");
        name_builder.append_value("rock music");

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
            ],
        )
        .unwrap();

        let path = dir.join("genres.parquet");
        let file = fs::File::create(&path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Helper: write a test artist Parquet file with three entities.
    fn write_test_artists_parquet(dir: &Path) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new("artist_type", DataType::Utf8, false),
            Field::new("inclusion_reason", DataType::Utf8, false),
            Field::new("birth_date", DataType::Utf8, true),
            Field::new("death_date", DataType::Utf8, true),
            Field::new("genres", DataType::Utf8, false),
            Field::new("instruments", DataType::Utf8, false),
            Field::new("member_of", DataType::Utf8, false),
            Field::new("albums", DataType::Utf8, false),
            Field::new("tracks", DataType::Utf8, false),
        ]));

        let mut id_builder = StringBuilder::new();
        let mut name_builder = StringBuilder::new();
        let mut desc_builder = StringBuilder::new();
        let mut type_builder = StringBuilder::new();
        let mut reason_builder = StringBuilder::new();
        let mut birth_builder = StringBuilder::new();
        let mut death_builder = StringBuilder::new();
        let mut genres_builder = StringBuilder::new();
        let mut instruments_builder = StringBuilder::new();
        let mut member_builder = StringBuilder::new();
        let mut albums_builder = StringBuilder::new();
        let mut tracks_builder = StringBuilder::new();

        // Entity 1: Ivy Queen — full data
        id_builder.append_value("Q2831");
        name_builder.append_value("Ivy Queen");
        desc_builder.append_value("American singer-songwriter");
        type_builder.append_value("person");
        reason_builder.append_value("P106:Q639669");
        birth_builder.append_value("1972-03-22");
        death_builder.append_null();
        genres_builder.append_value("Q35718|Q57251");
        instruments_builder.append_value("");
        member_builder.append_value("");
        albums_builder.append_value("[]");
        tracks_builder.append_value("[]");

        // Entity 2: name is NULL
        id_builder.append_value("Q99999");
        name_builder.append_null();
        desc_builder.append_null();
        type_builder.append_value("person");
        reason_builder.append_value("PROP:P136");
        birth_builder.append_null();
        death_builder.append_null();
        genres_builder.append_value("");
        instruments_builder.append_value("");
        member_builder.append_value("");
        albums_builder.append_value("[]");
        tracks_builder.append_value("[]");

        // Entity 3: with date
        id_builder.append_value("Q12345");
        name_builder.append_value("Test Artist");
        desc_builder.append_null();
        type_builder.append_value("group");
        reason_builder.append_value("P31:Q215380");
        birth_builder.append_null();
        death_builder.append_value("2000-01-15");
        genres_builder.append_value("");
        instruments_builder.append_value("");
        member_builder.append_value("");
        albums_builder.append_value("[]");
        tracks_builder.append_value("[]");

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(name_builder.finish()),
                Arc::new(desc_builder.finish()),
                Arc::new(type_builder.finish()),
                Arc::new(reason_builder.finish()),
                Arc::new(birth_builder.finish()),
                Arc::new(death_builder.finish()),
                Arc::new(genres_builder.finish()),
                Arc::new(instruments_builder.finish()),
                Arc::new(member_builder.finish()),
                Arc::new(albums_builder.finish()),
                Arc::new(tracks_builder.finish()),
            ],
        )
        .unwrap();

        let path = dir.join("part-00001.parquet");
        let file = fs::File::create(&path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    // -----------------------------------------------------------------------
    // load_genres tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_load_genres() {
        let dir = tempfile::tempdir().unwrap();
        write_test_genres_parquet(dir.path());
        let conn = test_conn();

        let count = load_genres(&conn, dir.path()).unwrap();
        assert_eq!(count, 2, "Expected 2 genre rows");

        // Verify content
        let names: Vec<String> = conn
            .prepare("SELECT name FROM genre ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(names, vec!["jazz", "rock music"]);
    }

    #[test]
    fn test_load_genres_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        write_test_genres_parquet(dir.path());
        let conn = test_conn();

        let count1 = load_genres(&conn, dir.path()).unwrap();
        let count2 = load_genres(&conn, dir.path()).unwrap();
        assert_eq!(count1, count2, "Row count should be identical after re-run");
        assert_eq!(count1, 2);
    }

    #[test]
    fn test_load_genres_empty_file() {
        let dir = tempfile::tempdir().unwrap();

        // Write an empty genres.parquet (0 rows)
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringBuilder::new().finish()),
                Arc::new(StringBuilder::new().finish()),
            ],
        )
        .unwrap();
        let path = dir.path().join("genres.parquet");
        let file = fs::File::create(&path).unwrap();
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let conn = test_conn();
        let count = load_genres(&conn, dir.path()).unwrap();
        assert_eq!(count, 0, "Expected 0 genre rows from empty file");
    }

    // -----------------------------------------------------------------------
    // load_artists tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_load_artists() {
        let dir = tempfile::tempdir().unwrap();
        write_test_artists_parquet(dir.path());
        let conn = test_conn();

        let count = load_artists(&conn, dir.path()).unwrap();
        assert_eq!(count, 3, "Expected 3 artist rows");

        // Verify NULL name is stored correctly
        let null_name_count: usize = conn
            .query_row(
                "SELECT COUNT(*) FROM artist WHERE name IS NULL AND id = 'Q99999'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(null_name_count, 1, "Expected 1 artist with NULL name");

        // Verify date parsing (cast to TEXT for comparison)
        let birth_date: Option<String> = conn
            .query_row(
                "SELECT birth_date::TEXT FROM artist WHERE id = 'Q2831'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(birth_date, Some("1972-03-22".to_string()));

        // Verify death date for entity 3
        let death_date: Option<String> = conn
            .query_row(
                "SELECT death_date::TEXT FROM artist WHERE id = 'Q12345'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(death_date, Some("2000-01-15".to_string()));
    }

    #[test]
    fn test_load_artists_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        write_test_artists_parquet(dir.path());
        let conn = test_conn();

        let count1 = load_artists(&conn, dir.path()).unwrap();
        let count2 = load_artists(&conn, dir.path()).unwrap();
        assert_eq!(count1, count2, "Row count should be identical after re-run");
        assert_eq!(count1, 3);
    }

    #[test]
    fn test_load_artists_missing_parquet_dir() {
        let conn = test_conn();
        let missing_dir = Path::new("/tmp/nonexistent_dir_that_does_not_exist_xyzzy");

        let result = load_artists(&conn, missing_dir);
        assert!(
            result.is_err(),
            "Expected error for missing parquet directory"
        );
    }

    // -----------------------------------------------------------------------
    // load_all orchestrator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_load_all_orchestrator_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let conn = test_conn();

        // No genres.parquet exists — should error from load_genres
        let result = load_all(&conn, dir.path());
        assert!(result.is_err(), "Expected error for missing genres.parquet");
    }
}
