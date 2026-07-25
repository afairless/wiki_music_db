# Implementation Plan: Phase 3a — Parquet Writer & MusicEntity Extraction

**Source:** `docs/research/2026-07_music_db_rust_plan.md` (Phase 3a only)

**Project state:** Phases 1 (project scaffold & schema), 2a (Wikidata entity model & deserialization), and 2b (filter + streaming parser) are **complete**. This plan covers **Phase 3a only**. Phases 3b, 5, 6, 7, and 8 are **out of scope**. Do not implement any code belonging to those phases.

## Summary

Define a flat `MusicEntity` struct with all extracted fields and explicit data contracts, extract those fields from filtered Wikidata entities (name, description, artist_type, inclusion_reason, birth_date, death_date, genres, instruments, member_of, albums, tracks), collect genre Q-ID references for deferred label extraction, and batch-write entities to `.parquet` files with rotation.

**Deliverable:** `cargo test` passes; can produce `.parquet` files from a test fixture; genre Q-IDs collected and label extraction working.

## Steps

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat: define MusicEntity struct and extraction from FilteredEntity` | MusicEntity extraction | `src/extraction.rs`, `src/lib.rs` (updated) | Unit |
| 2 | `feat: implement batch Parquet writer for MusicEntity with file rotation` | Batch Parquet writer | `src/parquet_writer.rs`, `src/lib.rs` (updated) | Unit |
| 3 | `feat: implement genre Q-ID collection and genre label extraction pass` | Genre label extraction | `src/extraction.rs` (updated) | Unit, Integration |

### Step 1 — Define `MusicEntity`, `AlbumRef`, `TrackRef` structs + extraction function

Create `src/extraction.rs` with the intermediate representation types and the extraction logic. Update `src/lib.rs` to export `pub mod extraction;`.

**Struct definitions:**

```rust
/// Intermediate representation of a music-relevant Wikidata entity.
///
/// Data contract (transformation → output stage boundary):
/// - `id`: always present (entities missing a Q-ID are rejected at ingestion)
/// - `name`: None means the entity has no English label (logged at WARN; stored as NULL)
/// - `description`: None means no English description (common; stored as NULL, not an error)
/// - `birth_date`, `death_date`: None means either no date property or an unparseable
///   date string (logged at WARN with the raw value; stored as NULL)
/// - `inclusion_reason`: why the entity passed the music filter (e.g., "P106:Q639669",
///   "P31:Q215380", "PROP:P1303,P136") — records the match that triggered inclusion
/// - All `Vec` fields default to empty (not None) — empty collections mean no data, not missing data
struct MusicEntity {
    id: String,
    name: Option<String>,
    description: Option<String>,
    artist_type: String,           // "person" or "group"
    inclusion_reason: String,
    birth_date: Option<NaiveDate>,
    death_date: Option<NaiveDate>,
    genres: Vec<String>,           // Q-IDs
    instruments: Vec<String>,      // Q-IDs
    member_of: Vec<String>,        // group Q-IDs (for persons)
    albums: Vec<AlbumRef>,         // album Q-IDs with roles
    tracks: Vec<TrackRef>,         // track Q-IDs with roles
}

struct AlbumRef {
    album_id: String,
    role: Option<String>,
}

struct TrackRef {
    track_id: String,
    role: Option<String>,
}
```

**Extraction logic (`extract_music_entity`):**

- Takes a `FilteredEntity` (from `stream.rs`) and returns `Result<MusicEntity>`
- Determines `artist_type`:
  - `"group"` if `inclusion_reason` starts with `P31:` (matched as a music group)
  - `"person"` otherwise (occupation or catch-all match)
- Extracts English label and description from `entity.labels.en()` and `entity.descriptions.en()`
- Extracts `birth_date` from P569 (date of birth) claims: parse the `time` string via `chrono::NaiveDate`. Wikidata date format is like `+1926-09-23T00:00:00Z`. Strip leading `+`, take the date prefix before `T`, and parse as `NaiveDate`. Log a `WARN` with the raw value on parse failure and return `None`.
- Extracts `death_date` from P570 (date of death) claims: same parsing logic as birth_date
- Extracts genre Q-IDs from P136 claims (genre) by collecting `datavalue.id` from each claim's mainsnak
- Extracts instruments from P1303 claims (instrument)
- Extracts `member_of` group Q-IDs from P463 claims (member of)
- Extracts albums from P4636 (album) or P658 (track) claims — for v1, collect album references from P361 (part of) or P175 (performer) where the entity is the performer
  - Simplification for v1: collect Q-IDs from P175 (performer) claims where the entity is linked as a performer on albums/tracks, and P361 (part of) for group membership
  - Actually, for simplicity in v1: extract albums from any claim property that references an album-like Q-ID and appears on the entity. The key point is to populate `albums: Vec<AlbumRef>` and `tracks: Vec<TrackRef>` from the available claim data.
  
  **Simplified extraction for v1:** Walk all claims, and for each claim whose `datavalue.id` is present, classify:
  - P136 → genre
  - P1303 → instrument
  - P463 → member_of
  - P175 → albums (entity is performer on an album — store album_id)
  - P658 → tracks (entity is performer on a track)
  
  For P569/P570, extract dates. All other properties' values are ignored.
  
  Note: The extraction is best-effort. Missing data results in empty Vecs or None, never rejection. Rejection only happens when: (a) `id` is empty/missing, or (b) a date string fails all parse attempts.
  
- Rejection: if `entity.id` is empty, return `Err(Error::EntityMissingId { line: 0 })`. If a date parse fails, log WARN and return `None` for that date (do not reject the entity).

**Date parsing helper:**

```rust
/// Parse a Wikidata time string into chrono::NaiveDate.
///
/// Wikidata format: "+1926-09-23T00:00:00Z" or "+1926-09-23"
/// Strips leading '+', splits on 'T', and parses the date portion.
fn parse_wikidata_date(raw: &str) -> Option<NaiveDate> {
    let stripped = raw.strip_prefix('+').unwrap_or(raw);
    let date_str = stripped.split('T').next().unwrap_or(stripped);
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
}
```

**Inline unit tests (`#[cfg(test)]` in `extraction.rs`):**

- `test_extract_musician`: Extract from a `FilteredEntity` matching the musician fixture → verify `artist_type == "person"`, `name == Some("Ivy Queen")`, `genres` contains P136 genre Q-IDs, `birth_date` parsed correctly
- `test_extract_band`: Extract from a band fixture → verify `artist_type == "group"`, `member_of` is empty (entity is the group itself)
- `test_extract_missing_label`: Create a `FilteredEntity` with no English label → verify `name == None`, not rejected
- `test_extract_missing_description`: No English description → `description == None`
- `test_extract_birth_date`: Entity with P569 claim containing a valid Wikidata time → `birth_date` is `Some`
- `test_extract_invalid_date`: Entity with P569 claim containing unparseable time string → date logged as WARN, `birth_date == None`, entity not rejected
- `test_extract_no_genres`: Entity with no P136 claims → `genres` is empty
- `test_extract_multiple_genres`: Entity with multiple P136 claims → all genre Q-IDs collected
- `test_extract_instruments`: Entity with P1303 claims → `instruments` populated
- `test_extract_member_of`: Person entity with P463 claims → `member_of` populated with group Q-IDs
- `test_parse_wikidata_date_valid`: `"+1926-09-23T00:00:00Z"` → `Some(1926-09-23)`
- `test_parse_wikidata_date_no_time`: `"+1926-09-23"` → `Some(1926-09-23)`
- `test_parse_wikidata_date_invalid`: `"not-a-date"` → `None`
- `test_parse_wikidata_date_no_plus`: `"1926-09-23"` → `Some(1926-09-23)`

### Step 2 — Batch Parquet writer for `MusicEntity`

Create `src/parquet_writer.rs` with a writer that serializes `MusicEntity` records to Parquet files. Update `src/lib.rs` to export `pub mod parquet_writer;`.

**Key design decisions:**

- Define a `MusicEntityBatchWriter` struct that:
  - Owns an output directory path
  - Tracks a current file index and per-file entity counter
  - Writes a Parquet schema derived from the `MusicEntity` struct
  - Rotates to a new file every `BATCH_SIZE` entities (e.g., 100_000)
  - Uses the `parquet` crate's `ArrowWriter` to write batches
- Parquet schema columns: `id` (UTF-8), `name` (UTF-8, optional), `description` (UTF-8, optional), `artist_type` (UTF-8), `inclusion_reason` (UTF-8), `birth_date` (Date64, optional), `death_date` (Date64, optional), `genres` (List of UTF-8), `instruments` (List of UTF-8), `member_of` (List of UTF-8), `albums` (List of Struct), `tracks` (List of Struct)
  
  **Simplification for v1:** Flatten arrays. Store `genres`, `instruments`, `member_of` as delimited strings separated by `|` (pipe), since DuckDB's Parquet reader handles VARCHAR columns easily. Store `albums` and `tracks` as JSON arrays of `{"id":"Q...", "role":"..."}` strings or pipe-delimited. This avoids complex nested Parquet schemas that are harder to map from `arrow-rs`.

  **Revised approach (v1):** Use a flat Parquet schema with VARCHAR columns:
  - `id`, `name`, `description`, `artist_type`, `inclusion_reason` — plain strings
  - `birth_date`, `death_date` — date strings (ISO 8601) stored as `Option<String>` for simplicity in v1
  - `genres` — pipe-delimited string: `"Q123|Q456"` or empty string
  - `instruments` — pipe-delimited string
  - `member_of` — pipe-delimited string
  - `albums` — JSON array string: `[{"album_id":"Q1","role":"performer"}]` or empty
  - `tracks` — JSON array string

- `MusicEntityBatchWriter::new(parquet_dir: &Path) -> Result<Self>` — creates the output directory if it doesn't exist
- `MusicEntityBatchWriter::write_batch(&mut self, entities: &[MusicEntity]) -> Result<()>` — writes a batch of entities, rotating if the current file exceeds the batch size
- `MusicEntityBatchWriter::flush(&mut self) -> Result<()>` — finalize the current file
- File naming: `part-00001.parquet`, `part-00002.parquet`, etc.
- Uses the `parquet` crate's `ArrowWriter<File>` with `SchemaRef` built from `arrow::datatypes::Schema`

**Inline unit tests (in `parquet_writer.rs` under `#[cfg(test)]`):**

- Round-trip a single `MusicEntity` through Parquet: write → read back using `parquet::reader::SerializedFileReader` → verify all fields match
- Round-trip multiple entities across a file rotation boundary (set a tiny BATCH_SIZE for testing, e.g. 2, write 3 entities → verify 2 files created, correct entities in each)
- Write empty entity list → no file created, no error
- Verify entity with all Option fields as None → round-trips correctly (name stored as NULL/empty)
- Verify entity with empty Vec fields → round-trips correctly (empty strings)
- Verify entity with multiple genres → round-trips correctly (pipe-delimited)

### Step 3 — Genre Q-ID collection + second-pass genre label extraction

Extend `src/extraction.rs` (or create a separate genre collection module) with:

1. **Genre Q-ID collection during extraction:** When `extract_music_entity()` processes a `FilteredEntity`, collect every Q-ID referenced in a P136 (genre) claim into a `HashSet<String>`.

2. **Second-pass genre label extraction:** After the initial filtering pass (Phase 2b streaming), make a second pass over the dump file to extract English labels for all collected genre Q-IDs. This reuses the `StreamReader` from Phase 2b but only processes lines whose entity ID is in the genre set.

3. **Genre entity extraction:** For each matching genre entity, extract its Q-ID and English label into a flat struct and write to a `genres.parquet` file using the same Parquet writer pattern.

**Implementation approach:**

```rust
/// Collect all genre Q-IDs referenced in a MusicEntity.
fn collect_genre_qids(entity: &MusicEntity) -> Vec<String> {
    entity.genres.clone()
}

/// Collect genre Q-IDs from all entities in a batch.
fn collect_all_genre_qids(entities: &[MusicEntity]) -> HashSet<String> {
    entities.iter().flat_map(|e| e.genres.iter().cloned()).collect()
}

/// Extract genre labels from a FilteredEntity (only for genre entities).
fn extract_genre_entity(entity: &Entity) -> Option<GenreEntry> {
    let id = entity.id.clone();
    let name = entity.labels.as_ref()?.en()?.to_string();
    Some(GenreEntry { id, name })
}

/// Flat struct for genre Parquet output.
struct GenreEntry {
    id: String,
    name: String,
}
```

**Second-pass logic:**

A function `extract_genre_labels(dump_path: &Path, genre_qids: &HashSet<String>, output_dir: &Path) -> Result<()>` that:

1. Opens the dump file with `StreamReader`
2. Reads each line, parses as `Entity`
3. If `entity.id` is in `genre_qids`, extracts the English label
4. Batches `GenreEntry` records and writes to `genres.parquet`

**Integration test (in `tests/extraction_test.rs` or `tests/genre_test.rs`):**

- Use the `mini_dump.json.gz` fixture:
  1. Stream all filtered entities from the mini dump
  2. Extract each into `MusicEntity`, collecting all genre Q-IDs
  3. Run second-pass genre label extraction on the same fixture
  4. Verify `genres.parquet` contains English labels for each referenced genre Q-ID
  5. Verify genres without English labels are omitted (or stored with NULL name)
