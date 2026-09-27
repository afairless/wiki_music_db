//! Q-ID label and claims extraction from the Wikidata JSON dump.
//!
//! This module provides functions to:
//!
//! 1. Collect all referenced Q-IDs from the DuckDB database (`collect_qid_set`)
//! 2. Stream the Wikidata dump extracting English labels, descriptions, and
//!    enrichment claims (publication dates, record labels, durations, album
//!    genres, track-album links) into intermediate Parquet files
//!
//! ## Data contract (ingestion → output boundary)
//!
//! - **Labels**: non-empty UTF-8 strings; empty labels stored as NULL.
//!   Source precedence: `en` label → sanitized `enwiki` sitelink title → NULL
//! - **Dates (P577)**: Wikidata full-precision dates parse normally; year-only
//!   dates (precision 9) produce YYYY-01-01; coarser precisions (8, 7, 6)
//!   produce NULL; unparseable dates produce NULL
//! - **Durations (P2047)**: parsed from the quantity `amount` to INTEGER
//!   seconds when the unit is absent or resolves to seconds (`Q11574`);
//!   otherwise NULL
//! - **Genre links (P136 on albums)**: Q-ID pairs; both must be non-empty
//! - **Track-album links (P361 on tracks)**: Q-ID pairs; both must be non-empty
//!
//! All failures are logged at WARN and stored as NULL — never rejected.

use aho_corasick::AhoCorasick;
use std::collections::HashSet;
use std::fs;
use std::io::BufRead;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use duckdb::Connection;
use flate2::read::MultiGzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::extraction::GenreEntry;
use crate::parquet_writer::write_genres_parquet;
use crate::wikidata::model::Entity;

// ---------------------------------------------------------------------------
// Q-ID set collection
// ---------------------------------------------------------------------------

/// Sets of Q-IDs collected from the database, categorized by origin table.
///
/// The `all` set is the union of all individual sets and is used for the
/// substring pre-check during dump scanning.
#[derive(Debug, Clone)]
pub struct QidSets {
    /// Union of all Q-IDs referenced anywhere (for substring pre-check).
    pub all: HashSet<String>,
    /// Q-IDs from `album.id` — used for album enrichment extraction.
    pub albums: HashSet<String>,
    /// Q-IDs from `track.id` — used for track enrichment extraction.
    pub tracks: HashSet<String>,
    /// Q-IDs from `artist.id WHERE name IS NULL OR name = id`.
    pub artists_with_null_names: HashSet<String>,
    /// Q-IDs from `artist_instrument.instrument_id`.
    pub instruments: HashSet<String>,
    /// Q-IDs from `genre.id` — genre Q-IDs that need label resolution.
    pub genres: HashSet<String>,
}

/// Collect all referenced Q-IDs from the DuckDB database.
///
/// Queries `album`, `track`, `artist`, `artist_instrument`, and `genre`
/// tables for Q-IDs whose names need resolution. For album and track Q-IDs,
/// also collects them as candidates for genre and track-album link extraction.
///
/// Returns an empty `QidSets` if the database is empty or no tables exist.
///
/// # Errors
///
/// Returns an error if any database query fails.
pub fn collect_qid_set(conn: &Connection) -> Result<QidSets> {
    // Helper: query a single column into a HashSet
    let query_ids = |sql: &str| -> Result<HashSet<String>> {
        let mut stmt = conn
            .prepare(sql)
            .with_context(|| format!("Failed to prepare query: {}", sql))?;
        let mut ids = HashSet::new();
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .with_context(|| format!("Failed to execute query: {}", sql))?;
        for row in rows {
            ids.insert(row.with_context(|| format!("Failed to read row from: {}", sql))?);
        }
        Ok(ids)
    };

    let albums = query_ids("SELECT DISTINCT id FROM album")?;
    let tracks = query_ids("SELECT DISTINCT id FROM track")?;
    let artists_with_null_names =
        query_ids("SELECT DISTINCT id FROM artist WHERE name IS NULL OR name = id")?;
    let instruments = query_ids("SELECT DISTINCT instrument_id FROM artist_instrument")?;
    let genres = query_ids("SELECT DISTINCT id FROM genre")?;

    // Build the union
    let mut all = HashSet::new();
    all.extend(albums.iter().cloned());
    all.extend(tracks.iter().cloned());
    all.extend(artists_with_null_names.iter().cloned());
    all.extend(instruments.iter().cloned());
    all.extend(genres.iter().cloned());

    tracing::info!(
        albums = albums.len(),
        tracks = tracks.len(),
        artists = artists_with_null_names.len(),
        instruments = instruments.len(),
        genres = genres.len(),
        total = all.len(),
        "Collected Q-ID set from database"
    );

    Ok(QidSets {
        all,
        albums,
        tracks,
        artists_with_null_names,
        instruments,
        genres,
    })
}

// ---------------------------------------------------------------------------
// Label and claims extraction
// ---------------------------------------------------------------------------

/// A label entry with optional description.
#[derive(Debug, Clone)]
struct LabelEntry {
    qid: String,
    label: Option<String>,
    description: Option<String>,
}

/// An enrichment row with optional claim values.
#[derive(Debug, Clone)]
struct EnrichmentRow {
    entity_qid: String,
    entity_type: String, // "album" or "track"
    release_date: Option<String>,
    record_label_qid: Option<String>,
    duration_seconds: Option<String>,
    genre_qid: Option<String>,
    parent_album_qid: Option<String>,
}

/// Extract English labels and enrichment claims from the Wikidata dump.
///
/// Opens the gzipped dump file and reads it line by line, performing a
/// substring pre-check on each line to avoid deserializing ~99% of entities.
/// For matching Q-IDs, extracts:
///
/// - English label and description → written to `labels.parquet`
/// - Album enrichment (P577 date, P264 record label, P136 genre) and track
///   enrichment (P2047 duration, P361 parent album) → written to
///   `enrichment.parquet`
///
/// During scanning, record label Q-IDs (P264) discovered on album entities
/// are dynamically added to the match set, so label entities appearing later
/// in the dump (higher Q-ID) will have their labels extracted.
///
/// The `parquet_dir` directory must exist; the two output files are created
/// inside it. Also writes genre entries to `genres.parquet` if the genre set
/// contains any Q-IDs.
///
/// # Errors
///
/// Returns an error if the dump file cannot be opened, a Parquet write
/// fails, or the stream encounters an I/O error.
pub fn extract_labels_and_claims(
    dump_path: &Path,
    qid_sets: &mut QidSets,
    parquet_dir: &Path,
) -> Result<()> {
    // Early return if no Q-IDs to match (also skips opening the dump file)
    if qid_sets.all.is_empty() {
        tracing::info!("No Q-IDs to match -- skipping label extraction");
        return Ok(());
    }

    let file = fs::File::open(dump_path)
        .with_context(|| format!("Failed to open dump file: {}", dump_path.display()))?;
    let decoder = MultiGzDecoder::new(file);
    let mut reader = std::io::BufReader::new(decoder);

    let mut labels: Vec<LabelEntry> = Vec::new();
    let mut enrichment: Vec<EnrichmentRow> = Vec::new();
    // Collect genre entries for genres.parquet
    let mut genre_entries: Vec<GenreEntry> = Vec::new();
    let mut line_number: u64 = 0;

    // Build an Aho-Corasick automaton from all known Q-IDs for O(L) single-pass
    // substring matching, replacing the O(K × L) HashSet linear scan. Built once
    // before the loop; never mutated during scanning.
    let qid_patterns: Vec<&str> = qid_sets.all.iter().map(|s| s.as_str()).collect();
    let ac = AhoCorasick::new(&qid_patterns).context("Failed to build Aho-Corasick automaton")?;
    // Q-IDs discovered mid-scan (e.g. P264 record labels) that are not in the
    // automaton; matched with a linear fallback since this set stays tiny.
    let mut discovered_qids: HashSet<String> = HashSet::new();

    let mut line_buf = String::new();

    // Track discovered P264 record label Q-IDs that should be added to the
    // match set so label entities appearing later in the dump are processed.
    let mut discovered_label_qids: Vec<String> = Vec::new();

    loop {
        line_buf.clear();
        let bytes_read = reader
            .read_line(&mut line_buf)
            .with_context(|| format!("Failed to read line {} from dump", line_number + 1))?;

        if bytes_read == 0 {
            break; // EOF
        }

        line_number += 1;
        let trimmed = line_buf.trim();

        // Skip JSON delimiters and empty lines
        if trimmed == "[" || trimmed == "]" || trimmed.is_empty() {
            continue;
        }

        // Strip trailing comma (Wikidata dump has comma-separated JSON objects)
        let line = trimmed.trim_end_matches(',');

        // Two-tier pre-check: automaton (bulk) + linear fallback (discovered)
        if !passes_precheck(line, &ac, &discovered_qids) {
            continue;
        }

        // Deserialize the entity
        let entity: Entity = match serde_json::from_str(line) {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(
                    line = line_number,
                    reason = %e,
                    "Failed to parse line during label extraction"
                );
                continue;
            }
        };

        // Check if this entity's Q-ID is in our set
        if !qid_sets.all.contains(&entity.id) {
            continue;
        }

        // 1. Extract label and description
        let (label, description) = extract_label(&entity);
        labels.push(LabelEntry {
            qid: entity.id.clone(),
            label,
            description,
        });

        // 2. Check if this is a genre entity — extract genre entry
        if qid_sets.genres.contains(&entity.id) {
            let name = entity
                .labels
                .as_ref()
                .and_then(|l| l.en())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            if let Some(name) = name {
                genre_entries.push(GenreEntry {
                    id: entity.id.clone(),
                    name,
                });
            }
        }

        // 3. Check if this is an album entity — extract enrichment claims
        if qid_sets.albums.contains(&entity.id) {
            let (release_date, record_label_qid, genre_qid) = extract_album_claims(&entity);

            // If we found a record label Q-ID, add it to the match set
            // so its label entity will be processed if encountered later
            if let Some(ref rl_qid) = record_label_qid
                && qid_sets.all.insert(rl_qid.clone())
            {
                discovered_qids.insert(rl_qid.clone());
                discovered_label_qids.push(rl_qid.clone());
                tracing::debug!(
                    entity_id = %entity.id,
                    label_qid = %rl_qid,
                    "Discovered record label Q-ID, added to match set"
                );
            }

            enrichment.push(EnrichmentRow {
                entity_qid: entity.id.clone(),
                entity_type: "album".to_string(),
                release_date,
                record_label_qid,
                duration_seconds: None,
                genre_qid,
                parent_album_qid: None,
            });
        }

        // 4. Check if this is a track entity — extract enrichment claims
        if qid_sets.tracks.contains(&entity.id) {
            let (duration_seconds, parent_album_qid) = extract_track_claims(&entity);

            enrichment.push(EnrichmentRow {
                entity_qid: entity.id.clone(),
                entity_type: "track".to_string(),
                release_date: None,
                record_label_qid: None,
                duration_seconds,
                genre_qid: None,
                parent_album_qid,
            });
        }
    }

    // Write Parquet files
    write_labels_parquet(&labels, &parquet_dir.join("labels.parquet"))?;
    write_enrichment_parquet(&enrichment, &parquet_dir.join("enrichment.parquet"))?;

    // Write genre entries if any were found
    if !genre_entries.is_empty() {
        write_genres_parquet(&genre_entries, &parquet_dir.join("genres.parquet"))?;
        tracing::info!(genre_count = genre_entries.len(), "Wrote genre labels");
    }

    tracing::info!(
        labels = labels.len(),
        enrichment = enrichment.len(),
        discovered_labels = discovered_label_qids.len(),
        "Extracted labels and claims from dump"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Pre-check helper
// ---------------------------------------------------------------------------

/// Two-tier substring pre-check for a dump line.
///
/// Returns `true` when the line contains any Q-ID known to the Aho-Corasick
/// automaton (bulk match, single pass over the text), or any Q-ID in the
/// dynamically-discovered set (linear fallback over the tiny discovered set).
///
/// This avoids deserializing ~99% of dump lines that reference no target
/// Q-IDs.
fn passes_precheck(line: &str, ac: &AhoCorasick, discovered_qids: &HashSet<String>) -> bool {
    if ac.find(line).is_some() {
        return true;
    }
    discovered_qids
        .iter()
        .any(|qid| line.contains(qid.as_str()))
}

// ---------------------------------------------------------------------------
// Low-level extraction helpers
// ---------------------------------------------------------------------------

/// Extract the English label and description from a Wikidata entity.
///
/// Label source precedence (name-resolution contract §4 #2): the `en` label
/// wins; otherwise a sanitized `enwiki` sitelink title is used as a fallback;
/// otherwise `None`. Empty labels are stored as NULL. The sitelink fallback
/// backfills entities that have a Wikipedia article but no English label in
/// the dump.
fn extract_label(entity: &Entity) -> (Option<String>, Option<String>) {
    let label = entity
        .labels
        .as_ref()
        .and_then(|l| l.en())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .or_else(|| {
            entity
                .sitelinks
                .as_ref()
                .and_then(|links| links.get("enwiki").map(|sl| sl.title.as_str()))
                .and_then(sanitize_sitelink_title)
        });

    let description = entity
        .descriptions
        .as_ref()
        .and_then(|d| d.en())
        .map(|s| s.to_string());

    (label, description)
}

/// Sanitize a sitelink title for use as a label.
///
/// Replaces underscores with spaces (`The_Joshua_Tree` → `The Joshua Tree`),
/// trims surrounding whitespace, and **keeps** `(album)`/`(song)`-style
/// disambiguators (name-resolution contract §4 #2 — decided: do not strip).
/// Returns `None` when the title is empty or whitespace-only after
/// sanitization (e.g. an underscore-only title).
fn sanitize_sitelink_title(title: &str) -> Option<String> {
    let spaced = title.replace("_", " ");
    let sanitized = spaced.trim();
    if sanitized.is_empty() {
        None
    } else {
        Some(sanitized.to_string())
    }
}

/// Extract claims for album enrichment from a Wikidata entity.
///
/// Extracts P577 (publication date), P264 (record label Q-ID), and
/// P136 (genre Q-ID). Returns the first value found for each property.
fn extract_album_claims(entity: &Entity) -> (Option<String>, Option<String>, Option<String>) {
    let mut release_date: Option<String> = None;
    let mut record_label_qid: Option<String> = None;
    let mut genre_qid: Option<String> = None;

    for (prop_id, claims) in &entity.claims {
        match prop_id.as_str() {
            "P577" => {
                if release_date.is_none() {
                    release_date = extract_p577_date(claims);
                }
            }
            "P264" => {
                if record_label_qid.is_none() {
                    record_label_qid = extract_first_qid(claims);
                }
            }
            "P136" => {
                if genre_qid.is_none() {
                    genre_qid = extract_first_qid(claims);
                }
            }
            _ => {}
        }
    }

    (release_date, record_label_qid, genre_qid)
}

/// Extract claims for track enrichment from a Wikidata entity.
///
/// Extracts P2047 (duration in seconds) and P361 (parent album Q-ID).
/// Returns the first value found for each property.
fn extract_track_claims(entity: &Entity) -> (Option<String>, Option<String>) {
    let mut duration_seconds: Option<String> = None;
    let mut parent_album_qid: Option<String> = None;

    for (prop_id, claims) in &entity.claims {
        match prop_id.as_str() {
            "P2047" => {
                if duration_seconds.is_none() {
                    duration_seconds = extract_p2047_duration(claims);
                }
            }
            "P361" => {
                if parent_album_qid.is_none() {
                    parent_album_qid = extract_first_qid(claims);
                }
            }
            _ => {}
        }
    }

    (duration_seconds, parent_album_qid)
}

/// Extract the first Q-ID from a list of claims' mainsnak datavalues.
fn extract_first_qid(claims: &[crate::wikidata::model::Claim]) -> Option<String> {
    claims
        .iter()
        .filter_map(|claim| {
            claim
                .mainsnak
                .as_ref()
                .and_then(|m| m.datavalue.as_ref())
                .and_then(|dv| dv.id.as_deref())
                .filter(|id| !id.is_empty())
                .map(|id| id.to_string())
        })
        .next()
}

/// Extract and parse a P577 publication date from claims.
///
/// Handles Wikidata date precision:
/// - 11 (day): parsed as full date
/// - 10 (month): parsed as YYYY-MM-01
/// - 9 (year): parsed as YYYY-01-01, logged at DEBUG
/// - 8, 7, 6 (decade/century/millennium): too imprecise → NULL, logged at WARN
/// - Other/unparseable → NULL, logged at WARN
fn extract_p577_date(claims: &[crate::wikidata::model::Claim]) -> Option<String> {
    for claim in claims {
        if let Some(time) = claim
            .mainsnak
            .as_ref()
            .and_then(|m| m.datavalue.as_ref())
            .and_then(|dv| dv.time.as_deref())
        {
            let precision = claim
                .mainsnak
                .as_ref()
                .and_then(|m| m.datavalue.as_ref())
                .and_then(|dv| dv.precision);

            return match parse_wikidata_date_with_precision(time, precision) {
                Some(date_str) => {
                    if precision == Some(9) {
                        tracing::debug!(
                            raw = time,
                            precision = ?precision,
                            parsed = %date_str,
                            "Parsed year-only P577 date"
                        );
                    }
                    Some(date_str)
                }
                None => {
                    tracing::warn!(
                        raw = time,
                        precision = ?precision,
                        "Could not parse P577 date"
                    );
                    None
                }
            };
        }
    }
    None
}

/// Parse a Wikidata time string with precision handling.
///
/// Wikidata format: `+YYYY-MM-DDT00:00:00Z` or `+YYYY-MM-DD`.
/// Precision codes: 11=day, 10=month, 9=year, 8=decade, 7=century, 6=millennium.
fn parse_wikidata_date_with_precision(raw: &str, precision: Option<i64>) -> Option<String> {
    let stripped = raw.strip_prefix('+').unwrap_or(raw);
    let date_str = stripped.split('T').next().unwrap_or(stripped);

    match precision {
        Some(11) | None => {
            // Full precision (day-level) or unknown — attempt normal parse
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .ok()
                .map(|d| d.format("%Y-%m-%d").to_string())
        }
        Some(10) => {
            // Month precision: YYYY-MM-00 → use YYYY-MM-01
            if let Ok(d) = NaiveDate::parse_from_str(&date_str.replace("-00", "-01"), "%Y-%m-%d") {
                Some(d.format("%Y-%m-%d").to_string())
            } else {
                None
            }
        }
        Some(9) => {
            // Year precision: YYYY-00-00 → use YYYY-01-01
            let year_only = date_str.split('-').next().unwrap_or(date_str);
            NaiveDate::parse_from_str(&format!("{}-01-01", year_only), "%Y-%m-%d")
                .ok()
                .map(|d| d.format("%Y-%m-%d").to_string())
        }
        Some(8) | Some(7) | Some(6) => {
            // Decade/century/millennium — too imprecise
            tracing::warn!(
                raw = raw,
                precision = precision.unwrap_or(0),
                "P577 date precision too coarse, storing as NULL"
            );
            None
        }
        Some(_) => {
            // Unknown precision — try normal parse
            NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
                .ok()
                .map(|d| d.format("%Y-%m-%d").to_string())
        }
    }
}

/// Extract and parse a P2047 duration (in seconds) from claims.
///
/// P2047 values are quantity-typed claims; the duration lives in the
/// `amount` field of `mainsnak.datavalue` (e.g. `"+240"`). The amount is
/// stripped of a leading `+`, parsed as an integer, and accepted only when
/// the unit is absent or resolves to seconds (`Q11574`). Returns the value
/// as a string. Claims that fail validation (missing/non-numeric amount or
/// non-second unit) are logged at WARN and stored as NULL.
fn extract_p2047_duration(claims: &[crate::wikidata::model::Claim]) -> Option<String> {
    for claim in claims {
        if let Some(dv) = claim.mainsnak.as_ref().and_then(|m| m.datavalue.as_ref()) {
            let amount = dv.amount.as_ref().map(|a| a.trim_start_matches('+'));
            if let Some(cleaned) = amount
                && let Ok(seconds) = cleaned.parse::<i64>()
            {
                // Accept bare amounts and explicit seconds (unit Q11574);
                // reject other units (e.g. milliseconds Q1186222).
                let unit_is_seconds = dv
                    .unit
                    .as_ref()
                    .map(|u| u.ends_with("Q11574"))
                    .unwrap_or(true);
                if unit_is_seconds {
                    return Some(seconds.to_string());
                }
            }
        }
    }
    tracing::warn!("P2047 duration missing, non-integer, or non-second unit; storing as NULL");
    None
}

// ---------------------------------------------------------------------------
// Parquet writer helpers
// ---------------------------------------------------------------------------

/// Write label entries to a Parquet file.
///
/// Schema: `(qid: TEXT NOT NULL, label: TEXT, description: TEXT)`
fn write_labels_parquet(labels: &[LabelEntry], path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("qid", DataType::Utf8, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
    ]));

    let mut qid_builder = StringBuilder::new();
    let mut label_builder = StringBuilder::new();
    let mut desc_builder = StringBuilder::new();

    for entry in labels {
        qid_builder.append_value(&entry.qid);
        label_builder.append_option(entry.label.as_deref());
        desc_builder.append_option(entry.description.as_deref());
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(qid_builder.finish()),
            Arc::new(label_builder.finish()),
            Arc::new(desc_builder.finish()),
        ],
    )
    .context("Failed to create labels RecordBatch")?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create parent directory for: {}", path.display())
        })?;
    }

    let file = fs::File::create(path)
        .with_context(|| format!("Failed to create labels parquet file: {}", path.display()))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .context("Failed to create ArrowWriter for labels")?;

    writer
        .write(&batch)
        .context("Failed to write labels RecordBatch to Parquet")?;
    writer
        .close()
        .context("Failed to close labels ArrowWriter")?;

    Ok(())
}

/// Write enrichment rows to a Parquet file.
///
/// Schema:
/// - `entity_qid`: TEXT NOT NULL
/// - `entity_type`: TEXT NOT NULL ("album" or "track")
/// - `release_date`: TEXT
/// - `record_label_qid`: TEXT
/// - `duration_seconds`: TEXT
/// - `genre_qid`: TEXT
/// - `parent_album_qid`: TEXT
fn write_enrichment_parquet(rows: &[EnrichmentRow], path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("entity_qid", DataType::Utf8, false),
        Field::new("entity_type", DataType::Utf8, false),
        Field::new("release_date", DataType::Utf8, true),
        Field::new("record_label_qid", DataType::Utf8, true),
        Field::new("duration_seconds", DataType::Utf8, true),
        Field::new("genre_qid", DataType::Utf8, true),
        Field::new("parent_album_qid", DataType::Utf8, true),
    ]));

    let mut entity_qid_builder = StringBuilder::new();
    let mut entity_type_builder = StringBuilder::new();
    let mut release_date_builder = StringBuilder::new();
    let mut record_label_qid_builder = StringBuilder::new();
    let mut duration_seconds_builder = StringBuilder::new();
    let mut genre_qid_builder = StringBuilder::new();
    let mut parent_album_qid_builder = StringBuilder::new();

    for row in rows {
        entity_qid_builder.append_value(&row.entity_qid);
        entity_type_builder.append_value(&row.entity_type);
        release_date_builder.append_option(row.release_date.as_deref());
        record_label_qid_builder.append_option(row.record_label_qid.as_deref());
        duration_seconds_builder.append_option(row.duration_seconds.as_deref());
        genre_qid_builder.append_option(row.genre_qid.as_deref());
        parent_album_qid_builder.append_option(row.parent_album_qid.as_deref());
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(entity_qid_builder.finish()),
            Arc::new(entity_type_builder.finish()),
            Arc::new(release_date_builder.finish()),
            Arc::new(record_label_qid_builder.finish()),
            Arc::new(duration_seconds_builder.finish()),
            Arc::new(genre_qid_builder.finish()),
            Arc::new(parent_album_qid_builder.finish()),
        ],
    )
    .context("Failed to create enrichment RecordBatch")?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("Failed to create parent directory for: {}", path.display())
        })?;
    }

    let file = fs::File::create(path).with_context(|| {
        format!(
            "Failed to create enrichment parquet file: {}",
            path.display()
        )
    })?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .context("Failed to create ArrowWriter for enrichment")?;

    writer
        .write(&batch)
        .context("Failed to write enrichment RecordBatch to Parquet")?;
    writer
        .close()
        .context("Failed to close enrichment ArrowWriter")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata::model::{
        Claim, DatavalueValue, Descriptions, Entity, Labels, LanguageValue, Mainsnak, Sitelink,
    };
    use proptest::prop_assert_eq;
    use std::collections::HashMap;

    // -------------------------------------------------------------------
    // Label extraction tests
    // -------------------------------------------------------------------

    fn make_entity_with_labels(id: &str, en_label: Option<&str>, en_desc: Option<&str>) -> Entity {
        let labels = en_label.map(|l| {
            Labels({
                let mut m = HashMap::new();
                m.insert("en".into(), LanguageValue { value: l.into() });
                m
            })
        });
        let descriptions = en_desc.map(|d| {
            Descriptions({
                let mut m = HashMap::new();
                m.insert("en".into(), LanguageValue { value: d.into() });
                m
            })
        });
        Entity {
            sitelinks: None,
            id: id.to_string(),
            entity_type: "item".to_string(),
            labels,
            descriptions,
            claims: HashMap::new(),
        }
    }

    #[test]
    fn test_extract_label_valid() {
        let entity = make_entity_with_labels("Q2831", Some("Ivy Queen"), Some("American singer"));
        let (label, desc) = extract_label(&entity);
        assert_eq!(label, Some("Ivy Queen".to_string()));
        assert_eq!(desc, Some("American singer".to_string()));
    }

    #[test]
    fn test_extract_label_no_en() {
        let entity = Entity {
            sitelinks: None,
            id: "Q42".to_string(),
            entity_type: "item".to_string(),
            labels: Some(Labels({
                let mut m = HashMap::new();
                m.insert(
                    "de".into(),
                    LanguageValue {
                        value: "Test".into(),
                    },
                );
                m
            })),
            descriptions: None,
            claims: HashMap::new(),
        };
        let (label, desc) = extract_label(&entity);
        assert_eq!(label, None);
        assert_eq!(desc, None);
    }

    #[test]
    fn test_extract_label_empty() {
        let entity = make_entity_with_labels("Q42", Some(""), None);
        let (label, desc) = extract_label(&entity);
        assert_eq!(label, None, "Empty English label should be NULL");
        assert_eq!(desc, None);
    }

    /// Helper: entity with no labels/descriptions but with an `enwiki` sitelink.
    fn make_entity_with_sitelink(en_title: &str) -> Entity {
        Entity {
            sitelinks: Some({
                let mut m = HashMap::new();
                m.insert(
                    "enwiki".into(),
                    Sitelink {
                        title: en_title.into(),
                    },
                );
                m
            }),
            id: "Q152873".to_string(),
            entity_type: "item".to_string(),
            labels: None,
            descriptions: None,
            claims: HashMap::new(),
        }
    }

    #[test]
    fn test_extract_label_sitelink_fallback() {
        // No English label → the sanitized enwiki title fills the label slot.
        let entity = make_entity_with_sitelink("The_Joshua_Tree");
        let (label, desc) = extract_label(&entity);
        assert_eq!(label, Some("The Joshua Tree".to_string()));
        assert_eq!(desc, None);
    }

    #[test]
    fn test_extract_label_en_wins_over_sitelink() {
        // The `en` label always wins over the sitelink fallback, even when the
        // sitelink title differs.
        let entity = Entity {
            sitelinks: Some({
                let mut m = HashMap::new();
                m.insert(
                    "enwiki".into(),
                    Sitelink {
                        title: "Ivy_Queen_(Puerto_Rican_singer)".into(),
                    },
                );
                m
            }),
            id: "Q2831".to_string(),
            entity_type: "item".to_string(),
            labels: Some(Labels({
                let mut m = HashMap::new();
                m.insert(
                    "en".into(),
                    LanguageValue {
                        value: "Ivy Queen".into(),
                    },
                );
                m
            })),
            descriptions: None,
            claims: HashMap::new(),
        };
        let (label, _) = extract_label(&entity);
        assert_eq!(label, Some("Ivy Queen".to_string()));
    }

    #[test]
    fn test_extract_label_underscore_only_sitelink() {
        // An underscore-only sitelink title sanitizes to empty → NULL.
        let entity = make_entity_with_sitelink("___");
        let (label, _) = extract_label(&entity);
        assert_eq!(label, None);
    }

    #[test]
    fn test_sanitize_sitelink_title_replaces_underscores() {
        assert_eq!(
            sanitize_sitelink_title("The_Joshua_Tree"),
            Some("The Joshua Tree".to_string())
        );
    }

    #[test]
    fn test_sanitize_sitelink_title_keeps_disambiguator() {
        // (album)/(song)-style disambiguators are kept, per contract §4 #2.
        assert_eq!(
            sanitize_sitelink_title("The_Joshua_Tree_(album)"),
            Some("The Joshua Tree (album)".to_string())
        );
    }

    #[test]
    fn test_sanitize_sitelink_title_empty_is_none() {
        // Empty, whitespace-only, and underscore-only titles yield NULL.
        assert_eq!(sanitize_sitelink_title(""), None);
        assert_eq!(sanitize_sitelink_title("  "), None);
        assert_eq!(sanitize_sitelink_title("____"), None);
    }

    // -------------------------------------------------------------------
    // P577 date extraction tests
    // -------------------------------------------------------------------

    /// Helper: build a claim with P577-like time and precision.
    fn make_time_claim(time: &str, precision: i64) -> Vec<Claim> {
        vec![Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    amount: None,
                    unit: None,
                    id: None,
                    time: Some(time.into()),
                    precision: Some(precision),
                }),
            }),
            extra: HashMap::new(),
        }]
    }

    #[test]
    fn test_extract_claim_p577() {
        let claims = make_time_claim("+2026-04-15T00:00:00Z", 11);
        let date = extract_p577_date(&claims);
        assert_eq!(date, Some("2026-04-15".to_string()));
    }

    #[test]
    fn test_extract_claim_p577_precision_9() {
        let claims = make_time_claim("+1970-00-00T00:00:00Z", 9);
        let date = extract_p577_date(&claims);
        assert_eq!(date, Some("1970-01-01".to_string()));
    }

    #[test]
    fn test_extract_claim_p577_precision_8() {
        let claims = make_time_claim("+1970-00-00T00:00:00Z", 8);
        let date = extract_p577_date(&claims);
        assert_eq!(date, None, "Decade precision should be NULL");
    }

    #[test]
    fn test_extract_claim_p577_invalid() {
        let claims = make_time_claim("not-a-date", 11);
        let date = extract_p577_date(&claims);
        assert_eq!(date, None, "Unparseable date should be NULL");
    }

    // -------------------------------------------------------------------
    // P264 record label extraction tests
    // -------------------------------------------------------------------

    /// Helper: build a claim with a Q-ID value.
    fn make_qid_claim(id: &str) -> Vec<Claim> {
        vec![Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    amount: None,
                    unit: None,
                    id: Some(id.into()),
                    time: None,
                    precision: None,
                }),
            }),
            extra: HashMap::new(),
        }]
    }

    #[test]
    fn test_extract_claim_p264() {
        let entity = Entity {
            sitelinks: None,
            id: "Q123".to_string(),
            entity_type: "item".to_string(),
            labels: None,
            descriptions: None,
            claims: {
                let mut m = HashMap::new();
                m.insert("P264".to_string(), make_qid_claim("Q12345"));
                m
            },
        };
        let (_, record_label_qid, _) = extract_album_claims(&entity);
        assert_eq!(record_label_qid, Some("Q12345".to_string()));
    }

    // -------------------------------------------------------------------
    // P2047 duration extraction tests
    // -------------------------------------------------------------------

    /// Helper: build a claim with a P2047-like quantity datavalue.
    fn make_quantity_claim(amount: Option<&str>, unit: Option<&str>) -> Vec<Claim> {
        vec![Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    amount: amount.map(|a| a.into()),
                    unit: unit.map(|u| u.into()),
                    id: None,
                    time: None,
                    precision: None,
                }),
            }),
            extra: HashMap::new(),
        }]
    }

    #[test]
    fn test_extract_claim_p2047() {
        // P2047 values are quantity-typed claims: the duration lives in
        // `mainsnak.datavalue.amount`, e.g. "+240" with a seconds unit.
        let claims =
            make_quantity_claim(Some("+240"), Some("http://www.wikidata.org/entity/Q11574"));
        let duration = extract_p2047_duration(&claims);
        assert_eq!(duration, Some("240".to_string()));
    }

    #[test]
    fn test_extract_claim_p2047_no_unit() {
        // A bare amount without a unit is accepted per the duration contract.
        let claims = make_quantity_claim(Some("+240"), None);
        let duration = extract_p2047_duration(&claims);
        assert_eq!(duration, Some("240".to_string()));
    }

    #[test]
    fn test_extract_claim_p2047_invalid() {
        // Non-numeric amounts are unparseable → NULL.
        let claims = make_quantity_claim(Some("not-a-number"), None);
        let duration = extract_p2047_duration(&claims);
        assert_eq!(duration, None);
    }

    #[test]
    fn test_extract_claim_p2047_missing_amount() {
        // A claim with no amount cannot yield a duration → NULL.
        let claims = make_quantity_claim(None, None);
        let duration = extract_p2047_duration(&claims);
        assert_eq!(duration, None);
    }

    #[test]
    fn test_extract_claim_p2047_non_second_unit() {
        // Milliseconds (Q1186222) and other non-second units → NULL.
        let claims = make_quantity_claim(
            Some("+240"),
            Some("http://www.wikidata.org/entity/Q1186222"),
        );
        let duration = extract_p2047_duration(&claims);
        assert_eq!(duration, None, "Non-second units should be stored as NULL");
    }

    // -------------------------------------------------------------------
    // Fixture-driven enrichment tests (tests/fixtures/*)
    // -------------------------------------------------------------------

    /// Load a fixture entity and return it deserialized.
    fn load_fixture_entity(json: &str) -> Entity {
        serde_json::from_str::<Entity>(json).expect("deserialize fixture")
    }

    #[test]
    fn test_song_work_fixture_duration_and_parent() {
        // With or Without You (Q155849): P2047 amount 240s + unit Q11574
        // yields a duration, and P361 names the parent album.
        let json = include_str!("../tests/fixtures/song_work_entity.json");
        let entity = load_fixture_entity(json);

        let duration = extract_p2047_duration(&entity.claims["P2047"]);
        assert_eq!(duration, Some("240".to_string()));

        let (track_duration, parent) = extract_track_claims(&entity);
        assert_eq!(track_duration, Some("240".to_string()));
        assert_eq!(parent, Some("Q152873".to_string()));
    }

    #[test]
    fn test_person_agent_sitelinks_fixture_label_fallback() {
        // Ivy Queen (Q2831): no en label, so the sanitized `enwiki` title
        // fills the label slot (contract §4 #2).
        let json = include_str!("../tests/fixtures/person_agent_sitelinks.json");
        let entity = load_fixture_entity(json);

        let (label, description) = extract_label(&entity);
        assert_eq!(label, Some("Ivy Queen".to_string()));
        assert_eq!(description, None);
    }

    #[test]
    fn test_album_work_fixture_album_claims() {
        // The Joshua Tree (Q152873): the work's P136 genre feeds
        // album_genre link extraction during populate.
        let json = include_str!("../tests/fixtures/album_work_entity.json");
        let entity = load_fixture_entity(json);

        let (_, _, genre_qid) = extract_album_claims(&entity);
        assert_eq!(genre_qid, Some("Q35718".to_string()));
    }

    // -------------------------------------------------------------------
    // Album genre extraction tests
    // -------------------------------------------------------------------

    #[test]
    fn test_extract_album_genre() {
        let entity = Entity {
            sitelinks: None,
            id: "Q123".to_string(),
            entity_type: "item".to_string(),
            labels: None,
            descriptions: None,
            claims: {
                let mut m = HashMap::new();
                m.insert("P136".to_string(), make_qid_claim("Q35718"));
                m
            },
        };
        let (_, _, genre_qid) = extract_album_claims(&entity);
        assert_eq!(genre_qid, Some("Q35718".to_string()));
    }

    // -------------------------------------------------------------------
    // Track-album extraction tests
    // -------------------------------------------------------------------

    #[test]
    fn test_extract_track_album() {
        let entity = Entity {
            sitelinks: None,
            id: "Q456".to_string(),
            entity_type: "item".to_string(),
            labels: None,
            descriptions: None,
            claims: {
                let mut m = HashMap::new();
                m.insert("P361".to_string(), make_qid_claim("Q789"));
                m
            },
        };
        let (_, parent_album_qid) = extract_track_claims(&entity);
        assert_eq!(parent_album_qid, Some("Q789".to_string()));
    }

    // -------------------------------------------------------------------
    // Q-ID set collection tests
    // -------------------------------------------------------------------

    #[test]
    fn test_collect_qid_set_empty_db() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        crate::db::schema::initialize(&conn).unwrap();
        let qid_sets = collect_qid_set(&conn).unwrap();
        assert!(qid_sets.all.is_empty());
        assert!(qid_sets.albums.is_empty());
        assert!(qid_sets.tracks.is_empty());
    }

    // -------------------------------------------------------------------
    // Pre-check helper for tests
    // -------------------------------------------------------------------

    /// Build an Aho-Corasick automaton from a set of Q-IDs for pre-check tests.
    fn build_ac(all: &HashSet<String>) -> AhoCorasick {
        let patterns: Vec<&str> = all.iter().map(|s| s.as_str()).collect();
        AhoCorasick::new(&patterns).unwrap()
    }

    #[test]
    fn test_substring_precheck_match() {
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        let line = r#"{"id":"Q2831","type":"item"}"#;
        assert!(
            passes_precheck(line, &ac, &discovered),
            "Line containing Q2831 should match"
        );
        assert!(
            ac.find(line).is_some(),
            "Aho-Corasick should also find the match"
        );
    }

    #[test]
    fn test_substring_precheck_skip() {
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        let line = r#"{"id":"Q42","type":"item"}"#;
        assert!(
            !passes_precheck(line, &ac, &discovered),
            "Line without Q2831 should not match"
        );
    }

    #[test]
    fn test_substring_precheck_false_positive() {
        // Q2831 is a substring of Q28310, but the full QID comparison
        // should filter correctly during entity processing.
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        // This line contains "Q2831" as a substring of "Q28310"
        let line = r#"{"id":"Q28310","type":"item"}"#;
        assert!(
            passes_precheck(line, &ac, &discovered),
            "Substring check should match false positive"
        );
        // But the entity id check should reject it
        assert!(!all.contains("Q28310"));
    }

    #[test]
    fn test_aho_corasick_precheck_match() {
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        let line = r#"{"id":"Q2831","type":"item"}"#;
        assert!(passes_precheck(line, &ac, &discovered));
    }

    #[test]
    fn test_aho_corasick_precheck_skip() {
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        let line = r#"{"id":"Q42","type":"item"}"#;
        assert!(!passes_precheck(line, &ac, &discovered));
    }

    #[test]
    fn test_aho_corasick_precheck_false_positive() {
        let all = HashSet::from(["Q2831".to_string()]);
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        // "Q2831" is a substring of "Q28310" — passes the automaton pre-check
        let line = r#"{"id":"Q28310","type":"item"}"#;
        assert!(
            passes_precheck(line, &ac, &discovered),
            "False positive at pre-check is expected"
        );
        // But the full entity Q-ID check (not tested here) correctly rejects Q28310
    }

    #[test]
    fn test_aho_corasick_many_patterns() {
        let all: HashSet<String> = (0..10_000).map(|i| format!("Q{}", i)).collect();
        let ac = build_ac(&all);
        let discovered = HashSet::new();
        // A line containing one of the 10K patterns matches
        let line = r#"{"id":"Q9999","type":"item"}"#;
        assert!(passes_precheck(line, &ac, &discovered));
        // A line without any pattern does not match
        let line2 = "{\"id\":\"X200000\",\"type\":\"item\"}";
        assert!(!passes_precheck(line2, &ac, &discovered));
    }

    #[test]
    fn test_empty_qid_set_returns_early() {
        // An empty set should cause extract_labels_and_claims to return early
        // without opening the dump file. Use a nonexistent path to verify.
        let mut all = QidSets {
            all: HashSet::new(),
            albums: HashSet::new(),
            tracks: HashSet::new(),
            artists_with_null_names: HashSet::new(),
            instruments: HashSet::new(),
            genres: HashSet::new(),
        };
        let result = extract_labels_and_claims(
            Path::new("/nonexistent/dump.json.gz"),
            &mut all,
            Path::new("/tmp"),
        );
        assert!(result.is_ok(), "Empty Q-ID set should return Ok(()) early");
    }

    #[test]
    fn test_discovered_qids_fallback() {
        // Build an automaton with a dummy pattern that won't match our test line
        let ac = AhoCorasick::new(["ZZZZ-NO-MATCH-ZZZZ"]).unwrap();
        let mut discovered: HashSet<String> = HashSet::new();
        let line = r#"{"id":"Q12345","type":"item"}"#;
        // No match initially
        assert!(!passes_precheck(line, &ac, &discovered));
        // After adding to discovered set mid-scan, the linear fallback matches
        discovered.insert("Q12345".to_string());
        assert!(passes_precheck(line, &ac, &discovered));
    }

    proptest::proptest! {
        #[test]
        fn test_aho_corasick_equivalent_to_hashset_contains(
            qids in proptest::collection::vec("[Q][0-9]{1,4}", 0..20),
            line in ".*"
        ) {
            let set: HashSet<String> = qids.iter().cloned().collect();
            let ac = if set.is_empty() {
                AhoCorasick::new(["ZZZZ-NO-MATCH-ZZZZ"]).unwrap()
            } else {
                let patterns: Vec<&str> = set.iter().map(|s| s.as_str()).collect();
                AhoCorasick::new(&patterns).unwrap()
            };
            let discovered = HashSet::new();
            let automaton_matches = passes_precheck(&line, &ac, &discovered);
            let hashset_matches = set.iter().any(|qid| line.contains(qid.as_str()));
            prop_assert_eq!(automaton_matches, hashset_matches,
                "Aho-Corasick and HashSet must agree on all lines");
        }
    }

    // -------------------------------------------------------------------
    // Parquet round-trip tests
    // -------------------------------------------------------------------

    #[test]
    fn test_roundtrip_labels_parquet() {
        let dir = tempfile::TempDir::new().unwrap();
        let labels = vec![
            LabelEntry {
                qid: "Q2831".to_string(),
                label: Some("Ivy Queen".to_string()),
                description: Some("Singer".to_string()),
            },
            LabelEntry {
                qid: "Q42".to_string(),
                label: None,
                description: None,
            },
        ];

        let path = dir.path().join("labels.parquet");
        write_labels_parquet(&labels, &path).unwrap();

        // Read back and verify
        let file = fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 2);

        let qid_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(qid_col.value(0), "Q2831");
        assert_eq!(qid_col.value(1), "Q42");
    }

    #[test]
    fn test_roundtrip_enrichment_parquet() {
        let dir = tempfile::TempDir::new().unwrap();
        let rows = vec![
            EnrichmentRow {
                entity_qid: "Q123".to_string(),
                entity_type: "album".to_string(),
                release_date: Some("2026-04-15".to_string()),
                record_label_qid: Some("Q12345".to_string()),
                duration_seconds: None,
                genre_qid: Some("Q35718".to_string()),
                parent_album_qid: None,
            },
            EnrichmentRow {
                entity_qid: "Q456".to_string(),
                entity_type: "track".to_string(),
                release_date: None,
                record_label_qid: None,
                duration_seconds: Some("240".to_string()),
                genre_qid: None,
                parent_album_qid: Some("Q123".to_string()),
            },
        ];

        let path = dir.path().join("enrichment.parquet");
        write_enrichment_parquet(&rows, &path).unwrap();

        // Read back and verify
        let file = fs::File::open(&path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches[0].num_rows(), 2);

        let type_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(type_col.value(0), "album");
        assert_eq!(type_col.value(1), "track");
    }
}
