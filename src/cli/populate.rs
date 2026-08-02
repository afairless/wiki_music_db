//! CLI subcommand for populating name, date, and label columns.
//!
//! The `populate` subcommand resolves Q-ID placeholders in album, track, and
//! artist name columns by re-scanning the Wikidata dump for English labels
//! and enrichment claims. It is a separate step from `bootstrap`.
//!
//! ## Usage
//!
//! ```bash
//! # Full populate: extract labels from dump, load into DB, backfill
//! wiki_db populate --db music.duckdb --dump latest-all.json.gz
//!
//! # Resume mode: skip extraction if Parquet files already exist
//! wiki_db populate --db music.duckdb --dump latest-all.json.gz --resume
//!
//! # Force re-populate even if names are already resolved
//! wiki_db populate --db music.duckdb --dump latest-all.json.gz --force
//! ```

use std::path::Path;

use anyhow::{Context, Result};
use clap::Parser;
use colored::*;
use duckdb::Connection;
use indicatif::{ProgressBar, ProgressStyle};

use crate::config::Config;
use crate::db::load::load_label_and_enrichment;
use crate::db::schema;
use crate::label_extractor::{QidSets, collect_qid_set, extract_labels_and_claims};

/// Arguments for the `populate` subcommand.
#[derive(Parser, Debug)]
pub struct PopulateArgs {
    /// Path to the Wikidata JSON dump (gzipped).
    #[arg(long, short)]
    pub dump: Option<String>,

    /// Path to the DuckDB database file.
    #[arg(long)]
    pub db: Option<String>,

    /// Directory for intermediate Parquet files.
    #[arg(long)]
    pub parquet_dir: Option<String>,

    /// Force re-populate even if names are already resolved.
    #[arg(long)]
    pub force: bool,

    /// Skip extraction if Parquet files already exist.
    #[arg(long)]
    pub resume: bool,
}

/// Execute the `populate` subcommand.
///
/// Orchestrates the full label extraction → Parquet → DuckDB → backfill pipeline.
pub fn cmd_populate(args: &PopulateArgs, config: Option<&Config>) -> Result<()> {
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

    let dump_path = Path::new(&dump_str);
    let db_path = Path::new(&db_str);
    let parquet_dir = Path::new(&parquet_dir_str);

    // Open the database
    tracing::info!("Opening database: {}", db_path.display());
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open database: {}", db_path.display()))?;

    // Check schema version
    let version = schema::schema_version(&conn).context("Failed to read schema version")?;

    if version != Some(2) && !args.force {
        anyhow::bail!(
            "Database schema version is {:?}, expected 2. \
             Run `wiki_db bootstrap` first, then `wiki_db populate`. \
             Use --force to override this check.",
            version
        );
    }

    // Quick check: if all names are already resolved and --force is not set, skip
    if !args.force {
        let unresolved_albums: usize = conn
            .query_row("SELECT COUNT(*) FROM album WHERE name = id", [], |row| {
                row.get(0)
            })
            .context("Failed to check unresolved albums")?;

        if unresolved_albums == 0 {
            tracing::info!("All album names already resolved, skipping populate");
            println!(
                "{}: All names already resolved. Use --force to re-populate.",
                "Skipped".bold().yellow()
            );
            return Ok(());
        }
    }

    // Create progress bar
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::default_spinner()
            .template("{spinner} {msg}")
            .context("Failed to set progress bar style")?,
    );
    pb.set_message("Collecting Q-ID set from database...");
    pb.enable_steady_tick(std::time::Duration::from_millis(100));

    // Step 1: Collect Q-ID set
    pb.set_message("Collecting Q-ID set from database...");
    pb.tick();
    let mut qid_sets: QidSets =
        collect_qid_set(&conn).context("Failed to collect Q-ID set from database")?;

    let total_qids = qid_sets.all.len();
    tracing::info!(total_qids, "Collected Q-ID set");
    pb.set_message(format!("{} Q-IDs to resolve", total_qids));
    pb.tick();

    if total_qids == 0 {
        pb.finish_with_message("No Q-IDs need resolution");
        println!(
            "{}: All names already resolved.",
            "No work needed".bold().green()
        );
        return Ok(());
    }

    // Step 2: Check for existing Parquet files (resume support)
    let labels_path = parquet_dir.join("labels.parquet");
    let enrichment_path = parquet_dir.join("enrichment.parquet");
    let skip_extraction = args.resume && labels_path.exists() && enrichment_path.exists();

    if skip_extraction {
        tracing::info!(
            "Resume mode: labels.parquet and enrichment.parquet already exist, skipping extraction"
        );
        pb.set_message("Skipping extraction (resume mode)");
        pb.tick();
    } else {
        // Ensure the dump file exists
        if !dump_path.exists() {
            anyhow::bail!(
                "Dump file not found: {}. Use --dump <PATH> or set `dump` in wiki_db.toml.",
                dump_path.display()
            );
        }

        // Step 3: Extract labels and claims
        pb.set_message("Extracting labels and claims from dump...");
        pb.tick();

        extract_labels_and_claims(dump_path, &mut qid_sets, parquet_dir)
            .context("Failed to extract labels and claims from dump")?;

        pb.set_message("Extraction complete");
        pb.tick();
    }

    // Step 4: Load Parquet and backfill
    pb.set_message("Loading labels and enrichment into database...");
    pb.tick();

    load_label_and_enrichment(&conn, parquet_dir)
        .context("Failed to load label and enrichment data")?;

    pb.set_message("Backfill complete");
    pb.tick();

    // --- Summary ---
    let resolved_albums: usize = conn
        .query_row("SELECT COUNT(*) FROM album WHERE name != id", [], |row| {
            row.get(0)
        })
        .context("Failed to count resolved albums")?;
    let resolved_tracks: usize = conn
        .query_row("SELECT COUNT(*) FROM track WHERE name != id", [], |row| {
            row.get(0)
        })
        .context("Failed to count resolved tracks")?;
    let resolved_artists: usize = conn
        .query_row(
            "SELECT COUNT(*) FROM artist WHERE name IS NOT NULL AND name != id",
            [],
            |row| row.get(0),
        )
        .context("Failed to count resolved artists")?;
    let label_count: usize = conn
        .query_row("SELECT COUNT(*) FROM qid_label", [], |row| row.get(0))
        .context("Failed to count labels")?;

    pb.finish_with_message("Populate complete");

    println!();
    println!("=== Populate Complete ===");
    println!("  Q-IDs resolved: {}", label_count);
    println!("  Albums:         {} resolved", resolved_albums);
    println!("  Tracks:         {} resolved", resolved_tracks);
    println!("  Artists:        {} resolved", resolved_artists);

    Ok(())
}
