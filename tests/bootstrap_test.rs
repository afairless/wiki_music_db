//! Integration tests for the full bootstrap pipeline.
//!
//! These tests run the `bootstrap` subcommand as a child process
//! with programmatically-created fixtures, and verify the resulting
//! DuckDB database.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

/// Path to the built binary (set during build).
const BINARY_PATH: &str = env!("CARGO_BIN_EXE_wiki_db");

/// Create a gzipped fixture with a comprehensive set of Wikidata entities.
///
/// Includes:
/// - Q35718 (jazz) — a genre entity with English label
/// - Q2831 (Ivy Queen) — musician with P106:Q639669 and P136:Q35718
/// - Q11649 (The Beatles) — band with P31:Q215380
/// - Q42 (Douglas Adams) — non-musician (P31:Q5) → excluded
/// - Q23215 (Adele) — musician with P106 and P1303 instrument
/// - Q99901 (Fictional Genre Entity) — catch-all via P136 only
/// - Q99902 (Entity With All Catch-All) — P1303, P175, P136, P358 (catch-all → Agent)
/// - Q152873 (The Joshua Tree) — album work (P31:Q482994) with P175 performer and P136 genre
/// - Q123456 (Test Track) — song work (P31:Q7366) with P175 performer and P361 parent album
/// - A malformed line → rejected
/// - Q90 (Paris) — non-musician (P31:Q5) → excluded
fn create_fixture(dir: &Path) -> PathBuf {
    let path = dir.join("fixture.json.gz");
    let file = std::fs::File::create(&path).unwrap();
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());

    let content = br#"[
{"id":"Q35718","type":"item","labels":{"en":{"value":"jazz"}},"claims":{}},
{"id":"Q2831","type":"item","labels":{"en":{"value":"Ivy Queen"}},"descriptions":{"en":{"value":"American singer-songwriter"}},"claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}],"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q35718"}}}}],"P569":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"time":"+1972-03-22T00:00:00Z"}}}}]}},
{"id":"Q11649","type":"item","labels":{"en":{"value":"The Beatles"}},"descriptions":{"en":{"value":"English rock band"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q215380"}}}}]}},
{"id":"Q42","type":"item","labels":{"en":{"value":"Douglas Adams"}},"descriptions":{"en":{"value":"Author"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}},
{"id":"Q23215","type":"item","labels":{"en":{"value":"Adele"}},"descriptions":{"en":{"value":"English singer"}},"claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}],"P1303":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q171"}}}}]}},
{"id":"Q99901","type":"item","labels":{"en":{"value":"Fictional Genre Entity"}},"claims":{"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q35718"}}}}]}},
{"id":"Q99902","type":"item","labels":{"en":{"value":"Entity With All Catch-All Properties"}},"claims":{"P1303":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q171"}}}}],"P175":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q12345"}}}}],"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q35718"}}}}],"P358":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q67890"}}}}]}},
{"id":"Q152873","type":"item","labels":{"en":{"value":"The Joshua Tree"}},"descriptions":{"en":{"value":"1987 studio album by U2"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q482994"}}}}],"P175":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q2831"}}}}],"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q35718"}}}}]}},
{"id":"Q123456","type":"item","labels":{"en":{"value":"Test Track"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q7366"}}}}],"P175":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q2831"}}}}],"P361":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q152873"}}}}]}},
{"id":"Q99999","type":"item","claims": broken},
{"id":"Q90","type":"item","labels":{"en":{"value":"Paris"}},"descriptions":{"en":{"value":"Capital of France"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}}
]"#;

    encoder.write_all(content).unwrap();
    encoder.finish().unwrap();
    path
}

/// Create a fixture with an entity missing an English label.
fn create_fixture_missing_name(dir: &Path) -> PathBuf {
    let path = dir.join("fixture_no_name.json.gz");
    let file = std::fs::File::create(&path).unwrap();
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());

    let content = br#"[
{"id":"Q35718","type":"item","labels":{"en":{"value":"jazz"}},"claims":{}},
{"id":"Q99999","type":"item","claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}}
]"#;

    encoder.write_all(content).unwrap();
    encoder.finish().unwrap();
    path
}

/// Run `bootstrap` subcommand with a given fixture and return its output.
fn run_bootstrap(
    fixture: &Path,
    db_path: &Path,
    parquet_dir: &Path,
    extra_args: &[&str],
) -> std::process::Output {
    let mut cmd = Command::new(BINARY_PATH);
    cmd.arg("bootstrap")
        .arg("--dump")
        .arg(fixture)
        .arg("--db")
        .arg(db_path)
        .arg("--parquet-dir")
        .arg(parquet_dir);
    for arg in extra_args {
        cmd.arg(arg);
    }
    cmd.output().expect("failed to run bootstrap")
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "bootstrap failed:\nstderr: {}",
        String::from_utf8_lossy(&output.stderr),
    );
}

fn open_db(db_path: &Path) -> duckdb::Connection {
    duckdb::Connection::open(db_path).expect("open duckdb database")
}

fn table_count(conn: &duckdb::Connection, table: &str) -> usize {
    let sql = format!("SELECT COUNT(*) FROM {}", table);
    conn.query_row(&sql, [], |row| row.get::<_, usize>(0))
        .unwrap_or_else(|e| panic!("Failed to count table {}: {}", table, e))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Test that bootstrap populates all expected tables with correct data.
#[test]
fn test_bootstrap_populates_all_tables() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output);

    let conn = open_db(&db_path);

    // Expected filtered entities:
    // Q2831 (P106:Q639669), Q11649 (P31:Q215380),
    // Q23215 (P106:Q639669), Q99901 (P136 catch-all), Q99902 (catch-all)
    // Note: Q35718 has empty claims so it's not filtered
    // Q152873 (P31:Q482994) and Q123456 (P31:Q7366) are work entities —
    // they are filtered but routed to album/track, never to artist.
    // = 5 artists
    assert_eq!(table_count(&conn, "artist"), 5, "Expected 5 artists");

    // 1 genre with label: Q35718 (jazz)
    assert_eq!(table_count(&conn, "genre"), 1, "Expected 1 genre");

    // artist_genre: Q2831→Q35718, Q99901→Q35718, Q99902→Q35718 = 3 rows.
    // Q152873 (album work) also carries P136:Q35718 but the loader's
    // role='Agent' filter keeps work genres out of artist_genre.
    assert_eq!(
        table_count(&conn, "artist_genre"),
        3,
        "Expected 3 artist_genre rows"
    );

    // artist_instrument: Q23215→Q171, Q99902→Q171 = 2 rows
    assert_eq!(
        table_count(&conn, "artist_instrument"),
        2,
        "Expected 2 artist_instrument rows"
    );

    // artist_member_of: none in fixture
    assert_eq!(
        table_count(&conn, "artist_member_of"),
        0,
        "Expected 0 artist_member_of rows"
    );

    // Album/track rows come from work entities routed by role — never from
    // agent P175 refs (role-inversion fix). Q152873 is an Album-role work,
    // Q123456 a Track-role work.
    assert_eq!(
        table_count(&conn, "album"),
        1,
        "Expected 1 album (work entity)"
    );
    assert_eq!(
        table_count(&conn, "album_artist"),
        1,
        "Expected 1 album_artist row (album → performer)"
    );
    assert_eq!(
        table_count(&conn, "track"),
        1,
        "Expected 1 track (work entity)"
    );
    assert_eq!(
        table_count(&conn, "track_artist"),
        1,
        "Expected 1 track_artist row (track → performer)"
    );
    assert_eq!(
        table_count(&conn, "track_album"),
        1,
        "Expected 1 track_album row (track → P361 parent)"
    );

    // Work names come from the dump labels (COALESCE(name, id)):
    // queryable before `populate`.
    let album_name: String = conn
        .query_row("SELECT name FROM album WHERE id = 'Q152873'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(album_name, "The Joshua Tree");
    let track_name: String = conn
        .query_row("SELECT name FROM track WHERE id = 'Q123456'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(track_name, "Test Track");

    // Role separation: no Q-ID is both album and artist.
    let overlap: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM album a JOIN artist ar ON a.id = ar.id",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(overlap, 0, "No Q-ID may be both album and artist");

    // Parquet directory should still exist (no --cleanup-parquet)
    assert!(parquet_dir.exists(), "Parquet dir should exist");
}

/// Test that running bootstrap twice is idempotent.
#[test]
fn test_bootstrap_idempotent() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    // First run
    let output1 = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output1);

    let conn = open_db(&db_path);
    let tables = [
        "artist",
        "genre",
        "artist_genre",
        "artist_instrument",
        "album",
        "album_artist",
        "track",
        "track_artist",
    ];
    let count1: Vec<usize> = tables.iter().map(|t| table_count(&conn, t)).collect();
    drop(conn);

    // Second run with --resume (parquet files already exist)
    let output2 = run_bootstrap(&fixture, &db_path, &parquet_dir, &["--resume"]);
    assert_success(&output2);

    let conn2 = open_db(&db_path);
    let count2: Vec<usize> = tables.iter().map(|t| table_count(&conn2, t)).collect();

    for (i, table) in tables.iter().enumerate() {
        assert_eq!(
            count1[i], count2[i],
            "Table {}: counts differ after second run",
            table
        );
    }
}

/// Test that an entity with missing name is stored as NULL.
#[test]
fn test_bootstrap_entity_missing_name_stored_as_null() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture_missing_name(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output);

    let conn = open_db(&db_path);

    let name: Option<String> = conn
        .query_row("SELECT name FROM artist WHERE id = 'Q99999'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(
        name, None,
        "Entity without English label should have NULL name"
    );

    // No genres: Q99999 has no P136 claims so genre_qids is empty;
    // Q35718 exists in the dump but isn't found by extract_genre_labels
    // because no genre Q-IDs were collected during streaming.
    assert_eq!(
        table_count(&conn, "genre"),
        0,
        "Expected 0 genres (no genre Q-IDs collected)"
    );
}

/// Test: artist references genre Q-ID not present as a genre entity.
/// Currently covered by unit tests in db::load since FK constraints
/// prevent this from working at the integration level without
/// creating genre stubs.
#[test]
#[ignore = "Requires genre stubs for missing genre entities; covered by db::load unit tests"]
fn test_bootstrap_genre_without_label() {}

/// Test that --resume skips already-existing Parquet files.
#[test]
fn test_bootstrap_resume_skips_existing_parquet() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    // First run
    let output1 = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output1);

    // Second run with --resume
    let output2 = run_bootstrap(&fixture, &db_path, &parquet_dir, &["--resume"]);
    assert_success(&output2);

    // Parquet files should still exist after resume
    let parquet_files: Vec<_> = std::fs::read_dir(&parquet_dir)
        .expect("read parquet dir")
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "parquet"))
        .collect();
    assert!(
        !parquet_files.is_empty(),
        "Expected parquet files after resume"
    );

    // DB should have data
    let conn = open_db(&db_path);
    assert!(
        table_count(&conn, "artist") > 0,
        "Expected artists after resume"
    );
    assert_eq!(
        table_count(&conn, "genre"),
        1,
        "Expected genre after resume"
    );
}

/// Test that --cleanup-parquet deletes intermediate files.
#[test]
fn test_bootstrap_cleanup_parquet() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &["--cleanup-parquet"]);
    assert_success(&output);

    assert!(
        !parquet_dir.exists(),
        "Parquet dir should be deleted after --cleanup-parquet"
    );
    assert!(db_path.exists(), "Database should still exist");
}

/// Test that a missing dump file produces an error.
#[test]
fn test_bootstrap_missing_dump_errors() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");
    let missing_dump = dir.path().join("nonexistent.json.gz");

    let output = Command::new(BINARY_PATH)
        .arg("bootstrap")
        .arg("--dump")
        .arg(&missing_dump)
        .arg("--db")
        .arg(&db_path)
        .arg("--parquet-dir")
        .arg(&parquet_dir)
        .output()
        .expect("failed to run bootstrap");

    assert!(
        !output.status.success(),
        "Expected bootstrap to fail with missing dump"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Dump file not found") || stderr.contains("not found"),
        "stderr should mention missing dump: {}",
        stderr
    );
}

// ---------------------------------------------------------------------------
// Full-text search integration tests
// ---------------------------------------------------------------------------

/// Test that bootstrap populates data and FTS search (LIKE fallback) works
/// for artist search after a full bootstrap.
#[test]
fn test_bootstrap_fts_search_artist() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output);

    let conn = open_db(&db_path);

    let results = wiki_db::db::query::search_artist(&conn, "Ivy Queen").unwrap();
    assert_eq!(results.len(), 1, "Should find Ivy Queen");
    assert_eq!(results[0].id, "Q2831");
    assert_eq!(
        results[0].name.as_deref(),
        Some("Ivy Queen"),
        "Artist name should match"
    );
    assert_eq!(
        results[0].description.as_deref(),
        Some("American singer-songwriter"),
        "Artist description should match"
    );
}

/// Test FTS search for a partial name match.
#[test]
fn test_bootstrap_fts_search_artist_partial() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output);

    let conn = open_db(&db_path);

    // Partial match via LIKE
    let results = wiki_db::db::query::search_artist(&conn, "Ivy Qu").unwrap();
    assert_eq!(results.len(), 1, "Should find Ivy Queen with partial name");
    assert_eq!(results[0].id, "Q2831");

    // Also search The Beatles
    let beatles = wiki_db::db::query::search_artist(&conn, "Beatles").unwrap();
    assert_eq!(beatles.len(), 1, "Should find The Beatles");
    assert_eq!(beatles[0].id, "Q11649");
}

/// Test FTS search with a term that doesn't exist.
#[test]
fn test_bootstrap_fts_search_no_match() {
    let dir = tempfile::TempDir::new().expect("create temp dir");
    let fixture = create_fixture(dir.path());
    let db_path = dir.path().join("music.duckdb");
    let parquet_dir = dir.path().join("parquet");

    let output = run_bootstrap(&fixture, &db_path, &parquet_dir, &[]);
    assert_success(&output);

    let conn = open_db(&db_path);

    let results = wiki_db::db::query::search_artist(&conn, "NonexistentArtistXYZ").unwrap();
    assert!(
        results.is_empty(),
        "Search for nonexistent term should return empty"
    );
}
