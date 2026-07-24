use duckdb::Connection;
use wiki_db::db::schema;

/// Verify that the database can be initialized from scratch with
/// all expected tables, indexes, and the schema version row.
#[test]
fn test_database_initialization() {
    let conn = Connection::open_in_memory().unwrap();
    schema::initialize(&conn).unwrap();

    // All tables should exist.
    assert!(schema::all_tables_exist(&conn).unwrap());

    // All indexes should exist.
    assert!(schema::all_indexes_exist(&conn).unwrap());

    // Schema version should be seeded.
    assert_eq!(
        schema::schema_version(&conn).unwrap(),
        Some(schema::SCHEMA_VERSION)
    );
}

/// Verify that initialization is idempotent.
#[test]
fn test_initialization_idempotent() {
    let conn = Connection::open_in_memory().unwrap();
    schema::initialize(&conn).unwrap();
    schema::initialize(&conn).unwrap(); // second call

    assert!(schema::all_tables_exist(&conn).unwrap());
    assert_eq!(
        schema::schema_version(&conn).unwrap(),
        Some(schema::SCHEMA_VERSION)
    );
}

/// Verify that a fresh database has no data yet (empty tables).
#[test]
fn test_fresh_database_is_empty() {
    let conn = Connection::open_in_memory().unwrap();
    schema::initialize(&conn).unwrap();

    // Count rows in each core table.
    let artist_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM artist", [], |r| r.get(0))
        .unwrap();
    let genre_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM genre", [], |r| r.get(0))
        .unwrap();
    let album_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM album", [], |r| r.get(0))
        .unwrap();
    let track_count: i64 = conn
        .query_row("SELECT COUNT(*) FROM track", [], |r| r.get(0))
        .unwrap();

    assert_eq!(artist_count, 0);
    assert_eq!(genre_count, 0);
    assert_eq!(album_count, 0);
    assert_eq!(track_count, 0);
}

/// Verify that foreign-key constraints are enforced.
#[test]
fn test_foreign_key_enforcement() {
    let conn = Connection::open_in_memory().unwrap();
    schema::initialize(&conn).unwrap();

    // Inserting into a join table without the parent row should fail.
    let result = conn.execute(
        "INSERT INTO artist_genre (artist_id, genre_id) VALUES ('Q1', 'Q2')",
        [],
    );
    assert!(result.is_err(), "foreign key violation should be rejected");
}
