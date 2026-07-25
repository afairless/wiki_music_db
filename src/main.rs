use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use tracing_subscriber::EnvFilter;
use wiki_db::cli::{self, Cli, Command};
use wiki_db::db::load::load_all;
use wiki_db::db::schema;
use wiki_db::extraction::{extract_genre_labels, extract_music_entity};
use wiki_db::parquet_writer::{MusicEntityBatchWriter, write_genres_parquet};
use wiki_db::wikidata::stream::StreamEvent;
use wiki_db::wikidata::stream::StreamReader;

fn main() -> Result<()> {
    // Initialize structured logging with sensible defaults.
    // Use RUST_LOG env var to control verbosity (e.g., RUST_LOG=debug).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match &cli.command {
        Command::Bootstrap(args) => cmd_bootstrap(args),
        Command::Update(args) => cmd_update(args),
        Command::Query(args) => cmd_query(args),
    }
}

/// Execute the `bootstrap` subcommand.
fn cmd_bootstrap(args: &cli::bootstrap::BootstrapArgs) -> Result<()> {
    let dump_path = Path::new(&args.dump);
    let db_path = Path::new(&args.db);
    let parquet_dir = Path::new(&args.parquet_dir);

    if !dump_path.exists() {
        anyhow::bail!("Dump file not found: {}", dump_path.display());
    }

    // --- Resume support: check existing Parquet files ---
    let mut skip_streaming = false;
    if args.resume && parquet_dir.exists() {
        let entries = find_parquet_files(parquet_dir);
        if !entries.is_empty() {
            // Find the highest part-* file index
            let max_index = entries
                .iter()
                .filter_map(|name| {
                    let name = name.file_name()?.to_str()?;
                    if name.starts_with("part-") && name.ends_with(".parquet") {
                        let num_part = name.strip_prefix("part-")?.strip_suffix(".parquet")?;
                        num_part.parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .max()
                .unwrap_or(0);

            // Remove the highest file (may be incomplete)
            let highest_path = parquet_dir.join(format!("part-{:05}.parquet", max_index));
            if highest_path.exists() {
                tracing::warn!(
                    "Removing potentially incomplete Parquet file: {}",
                    highest_path.display()
                );
                std::fs::remove_file(&highest_path)
                    .with_context(|| format!("Failed to remove {}", highest_path.display()))?;
            }

            // Check if genres.parquet and at least one complete part file exist
            let genres_exist = parquet_dir.join("genres.parquet").exists();
            let remaining_parts = find_parquet_files(parquet_dir)
                .iter()
                .any(|p| p.file_name().unwrap_or_default() != "genres.parquet");

            if genres_exist && remaining_parts {
                tracing::info!("Resume mode: all Parquet files exist, skipping streaming");
                skip_streaming = true;
            } else if !genres_exist {
                tracing::info!(
                    "Resume mode: genres.parquet missing, will re-extract genres after streaming"
                );
            }
        } else {
            tracing::info!("Resume mode: no existing Parquet files found, starting fresh");
        }
    }

    // --- Setup progress bar ---
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} Processed: {msg}")
            .context("Failed to set progress bar style")?,
    );
    pb.set_message("0 processed, 0 filtered, 0 rejected");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // --- Streaming phase ---
    let mut genre_qids: HashSet<String> = HashSet::new();

    if skip_streaming {
        pb.set_message("Resume: skipping streaming, using existing Parquet files");
        pb.tick();
    } else {
        tracing::info!("Opening dump file: {}", dump_path.display());
        let mut reader = StreamReader::new(dump_path)
            .with_context(|| format!("Failed to open dump file: {}", dump_path.display()))?;

        let mut writer =
            MusicEntityBatchWriter::new(parquet_dir).context("Failed to create Parquet writer")?;

        let mut event_count: u64 = 0;

        while let Some(event) = reader
            .next_event()
            .context("Failed to read next event from dump stream")?
        {
            event_count += 1;

            match event {
                StreamEvent::Filtered(filtered) => {
                    match extract_music_entity(&filtered, &mut genre_qids) {
                        Ok(entity) => {
                            writer
                                .write_batch(&[entity])
                                .context("Failed to write entity batch to Parquet")?;
                        }
                        Err(e) => {
                            tracing::warn!(
                                event_count,
                                entity_id = %filtered.entity.id,
                                reason = %e,
                                "Failed to extract music entity"
                            );
                        }
                    }
                }
                StreamEvent::Rejected {
                    line,
                    reason,
                    raw: _,
                } => {
                    tracing::warn!(line, reason, "Rejected malformed line");
                }
                StreamEvent::Skipped => {
                    // Delimiters and non-music entities are skipped silently
                }
            }

            // Update progress every 1000 events
            if event_count.is_multiple_of(1000) {
                pb.set_message(format!(
                    "{} processed, {} filtered, {} rejected",
                    reader.processed(),
                    reader.filtered(),
                    reader.rejected()
                ));
                pb.tick();
            }
        }

        // Final flush
        writer.flush().context("Failed to flush Parquet writer")?;

        pb.set_message(format!(
            "{} processed, {} filtered, {} rejected — Done streaming",
            reader.processed(),
            reader.filtered(),
            reader.rejected()
        ));
        pb.tick();

        tracing::info!(
            processed = reader.processed(),
            filtered = reader.filtered(),
            rejected = reader.rejected(),
            genre_qids = genre_qids.len(),
            "Streaming phase complete"
        );
    }

    // --- Genre label extraction (second pass) ---
    if !skip_streaming || !parquet_dir.join("genres.parquet").exists() {
        tracing::info!("Extracting genre labels from dump (second pass)...");

        let genre_entries = extract_genre_labels(dump_path, &genre_qids)
            .context("Failed to extract genre labels from dump")?;

        write_genres_parquet(&genre_entries, &parquet_dir.join("genres.parquet"))
            .context("Failed to write genre Parquet file")?;

        tracing::info!(
            genre_count = genre_entries.len(),
            "Genre label extraction complete"
        );
    } else {
        tracing::info!("Genre Parquet file already exists, skipping genre extraction");
    }
    pb.tick();

    // --- DuckDB loading phase ---
    tracing::info!("Opening DuckDB database: {}", db_path.display());

    // Ensure the parent directory exists
    if let Some(parent) = db_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create db parent directory: {}", parent.display())
        })?;
    }

    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open DuckDB database: {}", db_path.display()))?;

    schema::initialize(&conn).context("Failed to initialize database schema")?;

    pb.set_message("Loading data into DuckDB...");
    pb.tick();

    load_all(&conn, parquet_dir).context("Failed to load Parquet data into DuckDB")?;

    pb.set_message("DuckDB loading complete");
    pb.tick();

    // --- Create FTS indexes ---
    pb.set_message("Creating FTS indexes...");
    pb.tick();

    if let Err(e) = schema::create_fts_indexes(&conn) {
        tracing::warn!(error = %e, "Failed to create FTS indexes; LIKE fallback will be used");
    }

    pb.set_message("FTS indexes ready");
    pb.tick();

    // --- Cleanup Parquet files ---
    if args.cleanup_parquet {
        tracing::info!(
            "Cleaning up intermediate Parquet files: {}",
            parquet_dir.display()
        );
        std::fs::remove_dir_all(parquet_dir).with_context(|| {
            format!(
                "Failed to remove parquet directory: {}",
                parquet_dir.display()
            )
        })?;
        tracing::info!("Parquet files cleaned up");
    }

    // --- Summary ---
    let artist_count: usize = conn
        .query_row("SELECT COUNT(*) FROM artist", [], |row| row.get(0))
        .context("Failed to count artists")?;
    let genre_count: usize = conn
        .query_row("SELECT COUNT(*) FROM genre", [], |row| row.get(0))
        .context("Failed to count genres")?;
    let album_count: usize = conn
        .query_row("SELECT COUNT(*) FROM album", [], |row| row.get(0))
        .context("Failed to count albums")?;
    let track_count: usize = conn
        .query_row("SELECT COUNT(*) FROM track", [], |row| row.get(0))
        .context("Failed to count tracks")?;

    let fts_enabled = schema::fts_available(&conn).unwrap_or(false);

    pb.finish_with_message("Bootstrap complete");

    tracing::info!(
        artists = artist_count,
        genres = genre_count,
        albums = album_count,
        tracks = track_count,
        fts = fts_enabled,
        "Bootstrap summary"
    );

    println!();
    println!("=== Bootstrap Complete ===");
    println!("  Artists: {}", artist_count);
    println!("  Genres:  {}", genre_count);
    println!("  Albums:  {}", album_count);
    println!("  Tracks:  {}", track_count);

    if fts_enabled {
        println!("  FTS:      enabled (DuckDB fts extension)");
    } else {
        println!("  FTS:      disabled (using LIKE fallback)");
    }

    if args.cleanup_parquet {
        println!("  Parquet files: deleted");
    } else {
        println!("  Parquet files: {}", parquet_dir.display());
    }

    Ok(())
}

/// Find all Parquet files in a directory, sorted by name.
fn find_parquet_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let path = entry.path();
                if path.extension()?.to_str()? == "parquet" {
                    Some(path)
                } else {
                    None
                }
            })
            .collect(),
        Err(_) => return vec![],
    };
    files.sort();
    files
}

/// Execute the `update` subcommand.
fn cmd_update(_args: &cli::update::UpdateArgs) -> Result<()> {
    tracing::info!("update command not yet implemented");
    Ok(())
}

/// Execute the `query` subcommand.
fn cmd_query(args: &cli::query::QueryArgs) -> Result<()> {
    use cli::query::QueryCommand;
    match &args.command {
        QueryCommand::Artist(a) => {
            tracing::info!("query artist: {} (not yet implemented)", a.name);
        }
        QueryCommand::Genre(g) => {
            tracing::info!(
                "query genre: {} (limit={}, offset={}) (not yet implemented)",
                g.name,
                g.limit,
                g.offset
            );
        }
        QueryCommand::Album(a) => {
            tracing::info!("query album: {} (not yet implemented)", a.name);
        }
        QueryCommand::Search(s) => {
            tracing::info!("query search: {} (not yet implemented)", s.term);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;
    use wiki_db::cli::bootstrap::BootstrapArgs;
    use wiki_db::db::schema;

    /// Helper: create a mini gzipped fixture with a few music entities.
    fn create_mini_fixture(dir: &Path) -> PathBuf {
        let path = dir.join("fixture.json.gz");
        let file = fs::File::create(&path).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());

        // Two music entities and one non-music entity
        let content = r#"[
{"id":"Q2831","type":"item","labels":{"en":{"value":"Ivy Queen"}},"descriptions":{"en":{"value":"American singer-songwriter"}},"claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}],"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q35718"}}}}],"P569":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"time":"+1972-03-22T00:00:00Z"}}}}]}},
{"id":"Q35718","type":"item","labels":{"en":{"value":"jazz"}},"claims":{}},
{"id":"Q42","type":"item","labels":{"en":{"value":"Douglas Adams"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}}
]"#;

        use std::io::Write;
        encoder.write_all(content.as_bytes()).unwrap();
        encoder.finish().unwrap();
        path
    }

    /// Helper: create a mini fixture with an entity missing a name.
    fn create_mini_fixture_missing_name(dir: &Path) -> PathBuf {
        let path = dir.join("fixture_no_name.json.gz");
        let file = fs::File::create(&path).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());

        // Entity with no English label, but still a music entity via P106
        let content = r#"[
{"id":"Q99999","type":"item","claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}}
]"#;

        use std::io::Write;
        encoder.write_all(content.as_bytes()).unwrap();
        encoder.finish().unwrap();
        path
    }

    /// Helper: create a mini fixture with a missing-genre Q-ID.
    #[allow(dead_code)]
    fn create_mini_fixture_missing_genre(dir: &Path) -> PathBuf {
        let path = dir.join("fixture_missing_genre.json.gz");
        let file = fs::File::create(&path).unwrap();
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());

        // Entity references genre Q99999 which doesn't exist as a genre entity
        let content = r#"[
{"id":"Q2831","type":"item","labels":{"en":{"value":"Test Artist"}},"claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}],"P136":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q99999"}}}}]}}
]"#;

        use std::io::Write;
        encoder.write_all(content.as_bytes()).unwrap();
        encoder.finish().unwrap();
        path
    }

    /// Helper: set up BootstrapArgs from a fixture path and temp db.
    fn setup_bootstrap_args(dump: &Path, db: &Path, parquet_dir: &Path) -> BootstrapArgs {
        BootstrapArgs::try_parse_from([
            "bootstrap",
            "--dump",
            dump.to_str().unwrap(),
            "--db",
            db.to_str().unwrap(),
            "--parquet-dir",
            parquet_dir.to_str().unwrap(),
        ])
        .unwrap()
    }

    /// Helper: run cmd_bootstrap and verify it succeeds.
    fn run_bootstrap(dump: &Path, db: &Path, parquet_dir: &Path) {
        let args = setup_bootstrap_args(dump, db, parquet_dir);
        let result = cmd_bootstrap(&args);
        assert!(
            result.is_ok(),
            "cmd_bootstrap failed: {}",
            result.unwrap_err()
        );
    }

    /// Helper: count rows in a table.
    fn table_count(conn: &Connection, table: &str) -> usize {
        let sql = format!("SELECT COUNT(*) FROM {}", table);
        conn.query_row(&sql, [], |row| row.get(0)).unwrap()
    }

    // -----------------------------------------------------------------------
    // Smoke tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bootstrap_cmd_logs() {
        let args =
            BootstrapArgs::try_parse_from(["bootstrap", "--dump", "/tmp/test.json.gz"]).unwrap();
        // Just verify the command function runs without error.
        let result = cmd_bootstrap(&args);
        assert!(result.is_err()); // No such file
    }

    #[test]
    fn test_bootstrap_with_mini_fixture() {
        let dir = TempDir::new().unwrap();
        let fixture = create_mini_fixture(dir.path());
        let db_path = dir.path().join("test.duckdb");
        let parquet_dir = dir.path().join("parquet");

        run_bootstrap(&fixture, &db_path, &parquet_dir);

        // Verify database was created and has data
        let conn = Connection::open(&db_path).unwrap();
        assert!(schema::all_tables_exist(&conn).unwrap());

        let artists = table_count(&conn, "artist");
        assert_eq!(artists, 1, "Expected 1 artist (Q2831)");

        let genres = table_count(&conn, "genre");
        assert_eq!(genres, 1, "Expected 1 genre (jazz)");

        // Verify artist data
        let name: Option<String> = conn
            .query_row("SELECT name FROM artist WHERE id = 'Q2831'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(name, Some("Ivy Queen".to_string()));

        // Parquet files should still exist (no --cleanup-parquet)
        assert!(parquet_dir.exists());
    }

    #[test]
    fn test_bootstrap_missing_dump_returns_error() {
        let args =
            BootstrapArgs::try_parse_from(["bootstrap", "--dump", "/nonexistent/file.json.gz"])
                .unwrap();
        let result = cmd_bootstrap(&args);
        assert!(result.is_err(), "Expected error for missing dump file");
    }

    #[test]
    fn test_bootstrap_cleanup_parquet() {
        let dir = TempDir::new().unwrap();
        let fixture = create_mini_fixture(dir.path());
        let db_path = dir.path().join("test.duckdb");
        let parquet_dir = dir.path().join("parquet");

        let args = BootstrapArgs::try_parse_from([
            "bootstrap",
            "--dump",
            fixture.to_str().unwrap(),
            "--db",
            db_path.to_str().unwrap(),
            "--parquet-dir",
            parquet_dir.to_str().unwrap(),
            "--cleanup-parquet",
        ])
        .unwrap();

        let result = cmd_bootstrap(&args);
        assert!(
            result.is_ok(),
            "cmd_bootstrap failed: {}",
            result.unwrap_err()
        );

        // Parquet directory should be deleted
        assert!(
            !parquet_dir.exists(),
            "Parquet directory should be deleted after --cleanup-parquet"
        );

        // Database should still exist
        assert!(db_path.exists(), "Database should still exist");
    }

    #[test]
    fn test_bootstrap_entity_missing_name_stored_as_null() {
        let dir = TempDir::new().unwrap();
        let fixture = create_mini_fixture_missing_name(dir.path());
        let db_path = dir.path().join("test.duckdb");
        let parquet_dir = dir.path().join("parquet");

        run_bootstrap(&fixture, &db_path, &parquet_dir);

        let conn = Connection::open(&db_path).unwrap();
        let name: Option<String> = conn
            .query_row("SELECT name FROM artist WHERE id = 'Q99999'", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(
            name, None,
            "Entity without English label should have NULL name"
        );
    }

    // -----------------------------------------------------------------------
    // CLI parsing tests (moved from the original inline module)
    // -----------------------------------------------------------------------

    #[test]
    fn test_update_cmd_logs() {
        use wiki_db::cli::update::UpdateArgs;
        let args = UpdateArgs::try_parse_from(["update"]).unwrap();
        let result = cmd_update(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_artist_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "artist", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_genre_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "genre", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_album_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "album", "--name", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_search_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "search", "--term", "Test"]).unwrap();
        let result = cmd_query(&args);
        assert!(result.is_ok());
    }
}
