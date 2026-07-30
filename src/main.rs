use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use clap::CommandFactory;
use clap::Parser;
use clap_complete::generate_to;
use colored::*;
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};
use tracing_subscriber::EnvFilter;
use wiki_db::cli::{self, Cli, Command};
use wiki_db::config::Config;
use wiki_db::db::load::load_all;
use wiki_db::db::load::{update_sync_state, upsert_entity_from_json};
use wiki_db::db::query;
use wiki_db::db::schema;
use wiki_db::db::schema::get_last_sync_timestamp;
use wiki_db::extraction::{extract_genre_labels, extract_music_entity};
use wiki_db::parquet_writer::{MusicEntityBatchWriter, write_genres_parquet};
use wiki_db::sparql::SparqlClient;
use wiki_db::wikidata::stream::StreamEvent;
use wiki_db::wikidata::stream::StreamReader;

fn main() -> Result<()> {
    // Parse CLI args first so we can read the --config flag.
    let cli = Cli::parse();

    // Apply --verbose / --quiet flags to the RUST_LOG env var.
    // SAFETY: This is called at the very start of `main()`, before any threads
    // are spawned, so it cannot race with other readers of RUST_LOG. The single
    // thread ensures no data races — the canonical safe scenario for set_var.
    if std::env::var("RUST_LOG").is_err() {
        if cli.verbose {
            unsafe { std::env::set_var("RUST_LOG", "debug") };
        } else if cli.quiet {
            unsafe { std::env::set_var("RUST_LOG", "warn") };
        }
    }

    // Initialize structured logging: RUST_LOG env var > "info"
    // NOTE: logging is initialized BEFORE config loading so that config load
    // errors are visible (tracing::warn! needs an active subscriber).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    // Load optional config file.  Silently falls back if the file doesn't exist.
    let config = match Config::load(&cli.config) {
        Ok(cfg) => cfg,
        Err(e) => {
            // Config file exists but is malformed -- warn and continue with CLI defaults.
            tracing::warn!(error = %e, "Failed to load config file; using CLI defaults");
            None
        }
    };

    // Override log level from config if set (but only if RUST_LOG is not set,
    // since env var takes precedence per the EnvFilter semantics).
    if let Some(level) = config.as_ref().and_then(|c| c.log_level.as_deref())
        && std::env::var("RUST_LOG").is_err()
    {
        tracing::info!("Using log level from config: {}", level);
    }

    if let Some(_cfg) = &config {
        tracing::debug!("Loaded config file: {}", cli.config.display());
    }

    match &cli.command {
        Command::Download(args) => cmd_download(args, config.as_ref()),
        Command::Bootstrap(args) => cmd_bootstrap(args, config.as_ref()),
        Command::Update(args) => cmd_update(args, config.as_ref()),
        Command::Query(args) => cmd_query(args, config.as_ref()),
        Command::Completion(args) => cmd_completion(args, &cli),
    }
}

/// Execute the `download` subcommand.
///
/// Delegates to `scripts/download_dump.sh`, passing along the configured or
/// default output path and any user-supplied flags.
fn cmd_download(args: &cli::download::DownloadArgs, config: Option<&Config>) -> Result<()> {
    let script_path = find_download_script()?;

    // Resolve output path: CLI flag > config `dump` field > default
    let output = args
        .output
        .clone()
        .or_else(|| config.and_then(|c| c.dump_path()))
        .unwrap_or_else(|| "latest-all.json.gz".to_string());

    tracing::info!(
        "Starting Wikidata dump download via {}",
        script_path.display()
    );
    tracing::info!("Output path: {}", output);

    let mut cmd = std::process::Command::new(&script_path);
    cmd.arg("--output").arg(&output);

    if args.force {
        cmd.arg("--force");
    }
    if args.quiet {
        cmd.arg("--quiet");
    }

    let status = cmd.status().with_context(|| {
        format!(
            "Failed to execute download script: {}",
            script_path.display()
        )
    })?;

    if status.success() {
        tracing::info!("Download complete: {}", output);
        Ok(())
    } else {
        anyhow::bail!(
            "Download script exited with code {}; see output above for details.",
            status.code().unwrap_or(-1)
        );
    }
}

/// Find `scripts/download_dump.sh` relative to the binary or CWD.
fn find_download_script() -> Result<std::path::PathBuf> {
    // Check next to the binary (`cargo install` case)
    let exe_dir = std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|p| p.to_path_buf()));
    if let Some(ref dir) = exe_dir {
        let candidate = dir.join("download_dump.sh");
        if candidate.exists() {
            return Ok(candidate);
        }
        // Check ../scripts/ relative to binary (debug/release build dirs)
        let candidate = dir.join("scripts").join("download_dump.sh");
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    // Fallback: assume CWD is the project root
    let candidate = std::path::Path::new("scripts/download_dump.sh");
    if candidate.exists() {
        return Ok(candidate.to_path_buf());
    }
    anyhow::bail!(
        "Could not find scripts/download_dump.sh. Run this command from the project root, or install the script alongside the binary."
    );
}

/// Execute the `bootstrap` subcommand.
fn cmd_bootstrap(args: &cli::bootstrap::BootstrapArgs, config: Option<&Config>) -> Result<()> {
    // Resolve paths: CLI flag > config file > hardcoded default
    let dump_str = args
        .dump
        .clone()
        .or_else(|| config.and_then(|c| c.dump_path()))
        .context("No dump path specified. Use --dump <PATH> or set `dump` in wiki_db.toml.")?;

    let db_str = args
        .db
        .clone()
        .or_else(|| config.and_then(|c| c.db_path()))
        .unwrap_or_else(|| "music.duckdb".to_string());

    let parquet_dir_str = args
        .parquet_dir
        .clone()
        .or_else(|| config.and_then(|c| c.parquet_dir_path()))
        .unwrap_or_else(|| "parquet-dir".to_string());

    // Boolean flags: CLI true wins, else check config, else default false
    let cleanup = args.cleanup_parquet || config.and_then(|c| c.cleanup_parquet).unwrap_or(false);
    let resume = args.resume || config.and_then(|c| c.resume).unwrap_or(false);

    let dump_path = Path::new(&dump_str);
    let db_path = Path::new(&db_str);
    let parquet_dir = Path::new(&parquet_dir_str);

    if !dump_path.exists() {
        anyhow::bail!(
            "Dump file not found: {}. Use --dump <PATH> or set `dump` in wiki_db.toml.",
            dump_path.display()
        );
    }

    // --- Resume support: check existing Parquet files ---
    let mut skip_streaming = false;
    if resume && parquet_dir.exists() {
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
    if cleanup {
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

    if cleanup {
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
///
/// Orchestrates the full incremental update pipeline:
/// 1. Determine the sync timestamp (from `--since` flag or database)
/// 2. Query the SPARQL endpoint for modified entities
/// 3. Fetch each entity's full data via the Wikimedia REST API
/// 4. Upsert into the DuckDB database
/// 5. Update the sync state
///
/// Supports `--dry-run` mode that prints changes without writing.
fn cmd_update(args: &cli::update::UpdateArgs, config: Option<&Config>) -> Result<()> {
    // Resolve database path: config > default
    let db_str = config
        .and_then(|c| c.db_path())
        .unwrap_or_else(|| "music.duckdb".to_string());

    let db_path = Path::new(&db_str);

    // Open the database
    tracing::info!("Opening database: {}", db_path.display());
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

    // Initialize schema (ensures sync_state table exists)
    schema::initialize(&conn).context("Failed to initialize database schema")?;

    // Determine the sync timestamp
    let since = match &args.since {
        Some(ts) => {
            tracing::info!("Using explicit --since timestamp: {}", ts);
            ts.clone()
        }
        None => {
            match get_last_sync_timestamp(&conn).context("Failed to read last sync timestamp")? {
                Some(ts) => {
                    tracing::info!("Using last sync timestamp from database: {}", ts);
                    ts
                }
                None => {
                    anyhow::bail!(
                        "No sync timestamp available. Use --since <TIMESTAMP> to specify \
                         one (e.g., --since 2026-07-17T00:00:00Z), or run bootstrap first."
                    );
                }
            }
        }
    };

    // Create progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .context("Failed to set progress bar style")?,
    );
    pb.set_message("Querying SPARQL endpoint for modified entities...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Create SPARQL client
    let sparql_client = SparqlClient::new().context("Failed to create SPARQL client")?;

    // Query for modified entities
    pb.set_message("Querying SPARQL endpoint for modified entities...");
    pb.tick();

    let modified_qids = sparql_client
        .query_modified_entities(&since)
        .context("Failed to query SPARQL endpoint for modified entities")?;

    let total = modified_qids.len();
    tracing::info!(count = total, "Found modified entities");

    if total == 0 {
        pb.finish_with_message("No modified entities found");
        println!();
        println!("=== Update Complete ===");
        println!("  Queried:      {} → now", since);
        println!("  Entities:     0 updated, 0 failed, 0 skipped");
        println!("  Sync state:   unchanged");
        return Ok(());
    }

    // Determine the end timestamp for the summary
    let now = chrono::Utc::now();
    let end_timestamp = now.format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Process entities
    let mut updated: u64 = 0;
    let mut failed: u64 = 0;
    let mut skipped: u64 = 0;

    for (i, qid) in modified_qids.iter().enumerate() {
        if args.dry_run {
            pb.set_message(format!(
                "[DRY-RUN] Would process entity {} of {}: {}",
                i + 1,
                total,
                qid
            ));
            pb.tick();
            updated += 1;
            continue;
        }

        pb.set_message(format!("Fetching entity {} of {}: {}", i + 1, total, qid));
        pb.tick();

        match sparql_client.fetch_entity(qid) {
            Ok(entity) => match upsert_entity_from_json(&conn, &entity) {
                Ok(true) => {
                    updated += 1;
                }
                Ok(false) => {
                    skipped += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        entity_id = %qid,
                        error = %e,
                        "Failed to upsert entity"
                    );
                    failed += 1;
                }
            },
            Err(e) => {
                // Check if the entity was skipped because it no longer matches
                // music criteria (upsert_entity_from_json logs this internally)
                tracing::warn!(
                    entity_id = %qid,
                    error = %e,
                    "Failed to fetch entity"
                );
                failed += 1;
            }
        }
    }

    // Update sync state (only if not dry-run)
    if !args.dry_run {
        pb.set_message("Updating sync state...");
        pb.tick();

        update_sync_state(&conn, &end_timestamp).context("Failed to update sync state")?;
    }

    // Finish progress
    if args.dry_run {
        pb.finish_with_message(format!("[DRY-RUN] Would process {} entities", total));
    } else {
        pb.finish_with_message("Update complete");
    }

    // Print summary
    println!();
    println!("=== Update Complete ===");
    println!("  Queried:      {} → {}", since, end_timestamp);
    println!(
        "  Entities:     {} updated, {} failed, {} skipped",
        updated, failed, skipped
    );
    if args.dry_run {
        println!("  (dry-run — no changes written)");
        println!("  Sync state:   unchanged");
    } else {
        println!("  Sync state:   {}", end_timestamp);
    }

    Ok(())
}

/// Execute the `query` subcommand.
fn cmd_query(args: &cli::query::QueryArgs, config: Option<&Config>) -> Result<()> {
    use cli::query::QueryCommand;

    // Resolve database path: config > default
    let db_str = config
        .and_then(|c| c.db_path())
        .unwrap_or_else(|| "music.duckdb".to_string());

    let conn = Connection::open(&db_str)
        .with_context(|| format!("Failed to open database: {}", db_str))?;

    // Ensure schema is initialized (in case the database is new or empty)
    schema::initialize(&conn).context("Failed to initialize schema")?;

    match &args.command {
        QueryCommand::Artist(a) => {
            let results = query::search_artist(&conn, &a.name)
                .with_context(|| format!("Failed to search artist: {}", a.name))?;

            if results.is_empty() {
                println!(
                    "{}: No artists found matching '{}'",
                    "No Results".bold().yellow(),
                    a.name
                );
                return Ok(());
            }

            for artist in &results {
                println!("\n{}", "━━━ Artist ━━━".bold().cyan());
                println!("{}  {}", "ID:".yellow(), artist.id);
                if let Some(ref name) = artist.name {
                    println!("{}  {}", "Name:".yellow(), name.bold());
                }
                if let Some(ref desc) = artist.description {
                    println!("{}  {}", "Description:".yellow(), desc);
                }
                println!("{}  {}", "Type:".yellow(), artist.artist_type);
                if let Some(date) = artist.birth_date {
                    println!("{}  {}", "Born:".yellow(), date);
                }
                if let Some(date) = artist.death_date {
                    println!("{}  {}", "Died:".yellow(), date);
                }

                // Detail lookups
                match query::artist_genres(&conn, &artist.id) {
                    Ok(genres) if !genres.is_empty() => {
                        let names: Vec<&str> = genres.iter().map(|g| g.name.as_str()).collect();
                        println!("{}  {}", "Genres:".yellow(), names.join(", "));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, artist_id = %artist.id, "Failed to fetch genres")
                    }
                }

                match query::artist_albums(&conn, &artist.id) {
                    Ok(albums) if !albums.is_empty() => {
                        println!("{}  ", "Albums:".yellow());
                        for album in &albums {
                            let mut line = format!("    - {}", album.name);
                            if let Some(ref role) = album.role {
                                line.push_str(&format!(" ({})", role));
                            }
                            if let Some(date) = album.release_date {
                                line.push_str(&format!(" [{}]", date));
                            }
                            println!("{}", line);
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, artist_id = %artist.id, "Failed to fetch albums")
                    }
                }

                match query::artist_instruments(&conn, &artist.id) {
                    Ok(instruments) if !instruments.is_empty() => {
                        let ids: Vec<&str> = instruments
                            .iter()
                            .map(|i| i.instrument_id.as_str())
                            .collect();
                        println!("{}  {}", "Instruments:".yellow(), ids.join(", "));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, artist_id = %artist.id, "Failed to fetch instruments")
                    }
                }
            }
        }
        QueryCommand::Genre(g) => {
            let results = query::search_genre(&conn, &g.name, g.limit, g.offset)
                .with_context(|| format!("Failed to search genre: {}", g.name))?;

            if results.is_empty() {
                println!(
                    "{}: No genres found matching '{}'",
                    "No Results".bold().yellow(),
                    g.name
                );
                return Ok(());
            }

            for genre_result in &results {
                println!("\n{}", "━━━ Genre ━━━".bold().cyan());
                println!("{}  {}", "ID:".yellow(), genre_result.id);
                println!("{}  {}", "Name:".yellow(), genre_result.name.bold());

                // Associated artists
                match query::genre_artists(&conn, &genre_result.id, 20, 0) {
                    Ok(artists) if !artists.is_empty() => {
                        println!("{}  {} artists", "Artists:".yellow(), artists.len());
                        for artist in &artists {
                            let name = artist.name.as_deref().unwrap_or("(unknown)");
                            println!("    - {} ({})", name, artist.artist_type);
                        }
                        println!("      (use --limit/--offset to paginate)");
                    }
                    Ok(_) => println!("{}  (no artists)", "Artists:".yellow()),
                    Err(e) => {
                        tracing::warn!(error = %e, genre_id = %genre_result.id, "Failed to fetch artists")
                    }
                }
            }
        }
        QueryCommand::Album(a) => {
            let results = query::search_album(&conn, &a.name)
                .with_context(|| format!("Failed to search album: {}", a.name))?;

            if results.is_empty() {
                println!(
                    "{}: No albums found matching '{}'",
                    "No Results".bold().yellow(),
                    a.name
                );
                return Ok(());
            }

            for album in &results {
                println!("\n{}", "━━━ Album ━━━".bold().cyan());
                println!("{}  {}", "ID:".yellow(), album.id);
                println!("{}  {}", "Name:".yellow(), album.name.bold());
                if let Some(date) = album.release_date {
                    println!("{}  {}", "Released:".yellow(), date);
                }

                // Artists on this album
                match query::album_artists(&conn, &album.id) {
                    Ok(artists) if !artists.is_empty() => {
                        println!("{}  ", "Artists:".yellow());
                        for artist in &artists {
                            let name = artist.name.as_deref().unwrap_or("(unknown)");
                            let mut line = format!("    - {}", name);
                            if let Some(ref role) = artist.role {
                                line.push_str(&format!(" ({})", role));
                            }
                            println!("{}", line);
                        }
                    }
                    Ok(_) => println!("{}  (none)", "Artists:".yellow()),
                    Err(e) => {
                        tracing::warn!(error = %e, album_id = %album.id, "Failed to fetch artists")
                    }
                }

                // Genres on this album
                match query::album_genres(&conn, &album.id) {
                    Ok(genres) if !genres.is_empty() => {
                        let names: Vec<&str> = genres.iter().map(|g| g.name.as_str()).collect();
                        println!("{}  {}", "Genres:".yellow(), names.join(", "));
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(error = %e, album_id = %album.id, "Failed to fetch genres")
                    }
                }

                // Track listing
                match query::album_tracks(&conn, &album.id) {
                    Ok(tracks) if !tracks.is_empty() => {
                        println!("{}  ", "Tracks:".yellow());
                        for track in &tracks {
                            let num = track
                                .track_number
                                .map(|n| format!("{:02}.", n))
                                .unwrap_or_else(|| "  -".to_string());
                            let mut line = format!("    {} {}", num, track.name);
                            if let Some(dur) = track.duration_seconds {
                                let mins = dur / 60;
                                let secs = dur % 60;
                                line.push_str(&format!(" ({}:{:02})", mins, secs));
                            }
                            println!("{}", line);
                        }
                    }
                    Ok(_) => println!("{}  (no tracks)", "Tracks:".yellow()),
                    Err(e) => {
                        tracing::warn!(error = %e, album_id = %album.id, "Failed to fetch tracks")
                    }
                }
            }
        }
        QueryCommand::Search(s) => {
            let term = &s.term;
            println!("\n{}", format!("Searching for '{}'...", term).bold().cyan());

            // Search artists
            match query::search_artist(&conn, term) {
                Ok(artists) if !artists.is_empty() => {
                    println!("\n{}  ({} found)", "Artists".bold().cyan(), artists.len());
                    for artist in &artists {
                        let name = artist.name.as_deref().unwrap_or("(unknown)");
                        println!("  {}  {}", "•".yellow(), name);
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "Failed to search artists"),
            }

            // Search albums
            match query::search_album(&conn, term) {
                Ok(albums) if !albums.is_empty() => {
                    println!("\n{}  ({} found)", "Albums".bold().cyan(), albums.len());
                    for album in &albums {
                        println!("  {}  {}", "•".yellow(), album.name);
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "Failed to search albums"),
            }

            // Search tracks
            match query::search_track(&conn, term) {
                Ok(tracks) if !tracks.is_empty() => {
                    println!("\n{}  ({} found)", "Tracks".bold().cyan(), tracks.len());
                    for track in &tracks {
                        println!("  {}  {}", "•".yellow(), track.name);
                    }
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "Failed to search tracks"),
            }

            // Check if nothing was found at all
            let artist_count = query::search_artist(&conn, term).map_or(0, |v| v.len());
            let album_count = query::search_album(&conn, term).map_or(0, |v| v.len());
            let track_count = query::search_track(&conn, term).map_or(0, |v| v.len());
            if artist_count == 0 && album_count == 0 && track_count == 0 {
                println!(
                    "\n{}: No results found for '{}'",
                    "No Results".bold().yellow(),
                    term
                );
            }
        }
    }
    Ok(())
}

/// Execute the `completion` subcommand.
///
/// Generates shell completion scripts for the specified shell.
fn cmd_completion(args: &cli::CompletionArgs, _cli: &Cli) -> Result<()> {
    let mut cmd = Cli::command();
    let path = generate_to(args.shell, &mut cmd, "wiki_db", &args.output)?;
    println!("Completion script generated: {}", path.display());
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
        let result = cmd_bootstrap(&args, None);
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
        let result = cmd_bootstrap(&args, None);
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
        let result = cmd_bootstrap(&args, None);
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

        let result = cmd_bootstrap(&args, None);
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
        // Without a database, cmd_update should return an error about missing
        // sync timestamp or database. This test verifies it doesn't panic.
        let args = UpdateArgs::try_parse_from(["update"]).unwrap();
        let result = cmd_update(&args, None);
        // The function should either succeed (no-op) or fail gracefully
        // — it should not panic.
        let _ = result;
    }

    #[test]
    fn test_query_artist_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "artist", "--name", "Test"]).unwrap();
        let result = cmd_query(&args, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_genre_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "genre", "--name", "Test"]).unwrap();
        let result = cmd_query(&args, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_album_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "album", "--name", "Test"]).unwrap();
        let result = cmd_query(&args, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_query_search_cmd_logs() {
        use wiki_db::cli::query::QueryArgs;
        let args = QueryArgs::try_parse_from(["query", "search", "--term", "Test"]).unwrap();
        let result = cmd_query(&args, None);
        assert!(result.is_ok());
    }
}
