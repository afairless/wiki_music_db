//! Batch Parquet writer for `MusicEntity` records.
//!
//! Writes filtered Wikidata entities to Parquet files with automatic
//! file rotation at a configurable batch size. Uses a flat VARCHAR schema
//! for v1 simplicity: arrays are stored as pipe-delimited strings, and
//! performer references (`albums`) plus parent-album Q-IDs (`parents`)
//! are stored as JSON arrays. Each row also carries a non-null `role`
//! column (`Agent`/`Album`/`Track`) so the DuckDB loader can route rows
//! to the correct tables.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::extraction::{GenreEntry, MusicEntity};

/// Default number of entities per Parquet file before rotating.
const DEFAULT_BATCH_SIZE: usize = 100_000;

/// Write genre entries to a `genres.parquet` file.
///
/// The Parquet schema is `(id: VARCHAR, name: VARCHAR)` with both columns
/// non-nullable. This file is read by `load_genres` during the DuckDB loading phase.
///
/// # Errors
///
/// Returns an error if the file cannot be created or the Parquet write fails.
pub fn write_genres_parquet(genres: &[GenreEntry], path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let mut id_builder = StringBuilder::new();
    let mut name_builder = StringBuilder::new();

    for genre in genres {
        id_builder.append_value(&genre.id);
        name_builder.append_value(&genre.name);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .context("Failed to create genre RecordBatch")?;

    let file = fs::File::create(path)
        .with_context(|| format!("Failed to create genre parquet file: {}", path.display()))?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))
        .context("Failed to create ArrowWriter for genres")?;

    writer
        .write(&batch)
        .context("Failed to write genre RecordBatch to Parquet")?;
    writer
        .close()
        .context("Failed to close genre ArrowWriter")?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Batch writer
// ---------------------------------------------------------------------------

/// Writes `MusicEntity` records to partitioned Parquet files.
///
/// Files are named `part-NNNNN.parquet` in the configured output directory.
pub struct MusicEntityBatchWriter {
    output_dir: PathBuf,
    batch_size: usize,
    file_index: usize,
    current_count: usize,
    writer: Option<ArrowWriter<fs::File>>,
    accumulators: Option<BatchAccumulators>,
}

struct BatchAccumulators {
    id: StringBuilder,
    name: StringBuilder,
    description: StringBuilder,
    artist_type: StringBuilder,
    inclusion_reason: StringBuilder,
    birth_date: StringBuilder,
    death_date: StringBuilder,
    genres: StringBuilder,
    instruments: StringBuilder,
    member_of: StringBuilder,
    albums: StringBuilder,
    tracks: StringBuilder,
    role: StringBuilder,
    parents: StringBuilder,
}

impl MusicEntityBatchWriter {
    /// Create a new batch writer.
    pub fn new(output_dir: &Path) -> Result<Self> {
        fs::create_dir_all(output_dir).with_context(|| {
            format!(
                "Failed to create output directory: {}",
                output_dir.display()
            )
        })?;
        Ok(MusicEntityBatchWriter {
            output_dir: output_dir.to_path_buf(),
            batch_size: DEFAULT_BATCH_SIZE,
            file_index: 0,
            current_count: 0,
            writer: None,
            accumulators: None,
        })
    }

    /// Set a custom batch size (useful for testing).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Write entities, rotating files at batch_size.
    pub fn write_batch(&mut self, entities: &[MusicEntity]) -> Result<()> {
        if entities.is_empty() {
            return Ok(());
        }
        for entity in entities {
            if self.writer.is_none() {
                self.start_new_file()?;
            }
            self.append_entity(entity)?;
            self.current_count += 1;
            if self.current_count >= self.batch_size {
                self.flush_current()?;
            }
        }
        Ok(())
    }

    /// Finalize the current file.
    pub fn flush(&mut self) -> Result<()> {
        self.flush_current()?;
        Ok(())
    }

    /// Number of files written.
    pub fn file_count(&self) -> usize {
        self.file_index
    }

    fn start_new_file(&mut self) -> Result<()> {
        self.file_index += 1;
        self.current_count = 0;
        self.accumulators = Some(BatchAccumulators::new());
        let file_path = self
            .output_dir
            .join(format!("part-{:05}.parquet", self.file_index));
        let file = fs::File::create(&file_path)
            .with_context(|| format!("Failed to create parquet file: {}", file_path.display()))?;
        let schema = build_schema();
        let props = WriterProperties::builder().build();
        let writer = ArrowWriter::try_new(file, schema, Some(props))
            .context("Failed to create ArrowWriter")?;
        self.writer = Some(writer);
        Ok(())
    }

    fn append_entity(&mut self, entity: &MusicEntity) -> Result<()> {
        let acc = self
            .accumulators
            .as_mut()
            .expect("accumulators must be initialized");
        acc.id.append_value(&entity.id);
        acc.artist_type.append_value(&entity.artist_type);
        acc.inclusion_reason.append_value(&entity.inclusion_reason);
        if let Some(ref name) = entity.name {
            acc.name.append_value(name);
        } else {
            acc.name.append_null();
        }
        if let Some(ref desc) = entity.description {
            acc.description.append_value(desc);
        } else {
            acc.description.append_null();
        }
        if let Some(ref date) = entity.birth_date {
            acc.birth_date
                .append_value(date.format("%Y-%m-%d").to_string());
        } else {
            acc.birth_date.append_null();
        }
        if let Some(ref date) = entity.death_date {
            acc.death_date
                .append_value(date.format("%Y-%m-%d").to_string());
        } else {
            acc.death_date.append_null();
        }
        acc.genres.append_value(entity.genres.join("|"));
        acc.instruments.append_value(entity.instruments.join("|"));
        acc.member_of.append_value(entity.member_of.join("|"));
        let albums_json =
            serde_json::to_string(&entity.albums).unwrap_or_else(|_| "[]".to_string());
        acc.albums.append_value(&albums_json);
        let tracks_json =
            serde_json::to_string(&entity.tracks).unwrap_or_else(|_| "[]".to_string());
        acc.tracks.append_value(&tracks_json);
        acc.role.append_value(format!("{:?}", entity.role));
        let parents_json =
            serde_json::to_string(&entity.parent_album).unwrap_or_else(|_| "[]".to_string());
        acc.parents.append_value(&parents_json);
        Ok(())
    }

    fn flush_current(&mut self) -> Result<()> {
        if let Some(acc) = self.accumulators.take() {
            let batch = acc.into_record_batch()?;
            if let Some(ref mut writer) = self.writer {
                writer
                    .write(&batch)
                    .context("Failed to write RecordBatch to Parquet")?;
            }
        }
        if let Some(writer) = self.writer.take() {
            writer.close().context("Failed to close ArrowWriter")?;
        }
        Ok(())
    }
}

fn build_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
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
        Field::new("role", DataType::Utf8, false),
        Field::new("parents", DataType::Utf8, false),
    ]))
}

impl BatchAccumulators {
    fn new() -> Self {
        BatchAccumulators {
            id: StringBuilder::new(),
            name: StringBuilder::new(),
            description: StringBuilder::new(),
            artist_type: StringBuilder::new(),
            inclusion_reason: StringBuilder::new(),
            birth_date: StringBuilder::new(),
            death_date: StringBuilder::new(),
            genres: StringBuilder::new(),
            instruments: StringBuilder::new(),
            member_of: StringBuilder::new(),
            albums: StringBuilder::new(),
            tracks: StringBuilder::new(),
            role: StringBuilder::new(),
            parents: StringBuilder::new(),
        }
    }

    fn into_record_batch(mut self) -> Result<RecordBatch> {
        let schema = build_schema();
        let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(self.id.finish()),
            Arc::new(self.name.finish()),
            Arc::new(self.description.finish()),
            Arc::new(self.artist_type.finish()),
            Arc::new(self.inclusion_reason.finish()),
            Arc::new(self.birth_date.finish()),
            Arc::new(self.death_date.finish()),
            Arc::new(self.genres.finish()),
            Arc::new(self.instruments.finish()),
            Arc::new(self.member_of.finish()),
            Arc::new(self.albums.finish()),
            Arc::new(self.tracks.finish()),
            Arc::new(self.role.finish()),
            Arc::new(self.parents.finish()),
        ];
        let batch =
            RecordBatch::try_new(schema, columns).context("Failed to create RecordBatch")?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn test_entity(id: &str, name: Option<&str>, genres: Vec<&str>) -> MusicEntity {
        MusicEntity {
            id: id.to_string(),
            name: name.map(String::from),
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "P106:Q639669".to_string(),
            role: crate::wikidata::filter::EntityRole::Agent,
            birth_date: None,
            death_date: None,
            genres: genres.into_iter().map(String::from).collect(),
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            parent_album: vec![],
            tracks: vec![],
        }
    }

    fn read_parquet(path: &Path) -> Result<Vec<RecordBatch>> {
        let file =
            fs::File::open(path).with_context(|| format!("Failed to open: {}", path.display()))?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
            .build()?;
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>()?;
        Ok(batches)
    }

    use arrow::array::{Array, StringArray};

    fn col_to_strings(batch: &RecordBatch, col_idx: usize) -> Vec<Option<String>> {
        let array = batch.column(col_idx);
        let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
        (0..string_array.len())
            .map(|i| {
                if string_array.is_null(i) {
                    None
                } else {
                    Some(string_array.value(i).to_string())
                }
            })
            .collect()
    }

    #[test]
    fn test_round_trip_single_entity() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        writer
            .write_batch(&[test_entity("Q2831", Some("Ivy Queen"), vec!["Q35718"])])
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.file_count(), 1);

        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(col_to_strings(&batches[0], 0), vec![Some("Q2831".into())]);
        assert_eq!(
            col_to_strings(&batches[0], 1),
            vec![Some("Ivy Queen".into())]
        );
        assert_eq!(col_to_strings(&batches[0], 7), vec![Some("Q35718".into())]);
        // role column round-trips the entity role; parents defaults to "[]" for agents.
        assert_eq!(col_to_strings(&batches[0], 12), vec![Some("Agent".into())]);
        assert_eq!(col_to_strings(&batches[0], 13), vec![Some("[]".into())]);
    }

    #[test]
    fn test_round_trip_file_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(2);
        writer
            .write_batch(&[
                test_entity("Q1", Some("A"), vec![]),
                test_entity("Q2", Some("B"), vec![]),
                test_entity("Q3", Some("C"), vec![]),
            ])
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.file_count(), 2);

        let b1 = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(b1[0].num_rows(), 2);
        assert_eq!(
            col_to_strings(&b1[0], 1),
            vec![Some("A".into()), Some("B".into())]
        );

        let b2 = read_parquet(&dir.path().join("part-00002.parquet")).unwrap();
        assert_eq!(b2[0].num_rows(), 1);
        assert_eq!(col_to_strings(&b2[0], 1), vec![Some("C".into())]);
    }

    #[test]
    fn test_empty_entity_list() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path()).unwrap();
        writer.write_batch(&[]).unwrap();
        writer.flush().unwrap();
        assert_eq!(writer.file_count(), 0);
    }

    #[test]
    fn test_round_trip_all_option_fields_none() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        let entity = MusicEntity {
            id: "Q42".to_string(),
            name: None,
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "PROP:P136".to_string(),
            role: crate::wikidata::filter::EntityRole::Agent,
            birth_date: None,
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            parent_album: vec![],
            tracks: vec![],
        };
        writer.write_batch(&[entity]).unwrap();
        writer.flush().unwrap();
        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(col_to_strings(&batches[0], 1), vec![None::<String>]);
        assert_eq!(col_to_strings(&batches[0], 7), vec![Some("".to_string())]);
    }

    #[test]
    fn test_round_trip_with_multiple_genres() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        let entity = test_entity("Q1", Some("Multi"), vec!["Q35718", "Q57251"]);
        writer.write_batch(&[entity]).unwrap();
        writer.flush().unwrap();
        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(
            col_to_strings(&batches[0], 7),
            vec![Some("Q35718|Q57251".into())]
        );
    }

    #[test]
    fn test_round_trip_with_dates() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        let entity = MusicEntity {
            id: "Q1".to_string(),
            name: Some("Test".to_string()),
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "P106:Q639669".to_string(),
            role: crate::wikidata::filter::EntityRole::Agent,
            birth_date: NaiveDate::from_ymd_opt(1972, 3, 22),
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            parent_album: vec![],
            tracks: vec![],
        };
        writer.write_batch(&[entity]).unwrap();
        writer.flush().unwrap();
        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(
            col_to_strings(&batches[0], 5),
            vec![Some("1972-03-22".into())]
        );
        assert_eq!(col_to_strings(&batches[0], 6), vec![None::<String>]);
    }

    // -----------------------------------------------------------------------
    // write_genres_parquet tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_write_genres_parquet() {
        let dir = tempfile::tempdir().unwrap();
        let genres = vec![
            GenreEntry {
                id: "Q35718".into(),
                name: "jazz".into(),
            },
            GenreEntry {
                id: "Q57251".into(),
                name: "rock music".into(),
            },
        ];
        let path = dir.path().join("genres.parquet");
        write_genres_parquet(&genres, &path).unwrap();

        // Read back and verify
        let batches = read_parquet(&path).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        let ids = col_to_strings(&batches[0], 0);
        let names = col_to_strings(&batches[0], 1);
        assert_eq!(ids, vec![Some("Q35718".into()), Some("Q57251".into()),]);
        assert_eq!(names, vec![Some("jazz".into()), Some("rock music".into()),]);
    }

    #[test]
    fn test_write_genres_parquet_empty() {
        let dir = tempfile::tempdir().unwrap();
        let genres: Vec<GenreEntry> = vec![];
        let path = dir.path().join("genres.parquet");
        write_genres_parquet(&genres, &path).unwrap();

        let batches = read_parquet(&path).unwrap();
        // Empty parquet with 0 rows may produce 0 batches
        if !batches.is_empty() {
            assert_eq!(batches[0].num_rows(), 0);
        }
    }

    /// Album/track works carry a `role` column and a `parents` JSON array
    /// (P361 parent-album Q-IDs), so the loader can route rows by role.
    #[test]
    fn test_round_trip_role_and_parents() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        let album = MusicEntity {
            id: "Q152873".to_string(),
            name: Some("The Joshua Tree".to_string()),
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "P31:Q482994".to_string(),
            role: crate::wikidata::filter::EntityRole::Album,
            birth_date: None,
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            parent_album: vec![],
            tracks: vec![],
        };
        let track = MusicEntity {
            id: "Q1234".to_string(),
            name: Some("With or Without You".to_string()),
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "P31:Q7366".to_string(),
            role: crate::wikidata::filter::EntityRole::Track,
            birth_date: None,
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            parent_album: vec!["Q152873".into()],
            tracks: vec![],
        };
        writer.write_batch(&[album, track]).unwrap();
        writer.flush().unwrap();
        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(
            col_to_strings(&batches[0], 12),
            vec![Some("Album".into()), Some("Track".into())]
        );
        // Parents: empty JSON array for the album, the P361 parent for the track.
        assert_eq!(
            col_to_strings(&batches[0], 13),
            vec![Some("[]".into()), Some("[\"Q152873\"]".into())]
        );
    }

    #[test]
    fn test_round_trip_with_album_refs() {
        use crate::extraction::PerformerRef;
        let dir = tempfile::tempdir().unwrap();
        let mut writer = MusicEntityBatchWriter::new(dir.path())
            .unwrap()
            .with_batch_size(100);
        let entity = MusicEntity {
            id: "Q152873".to_string(),
            name: Some("The Joshua Tree".to_string()),
            description: None,
            artist_type: "person".to_string(),
            inclusion_reason: "P31:Q482994".to_string(),
            role: crate::wikidata::filter::EntityRole::Album,
            birth_date: None,
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![PerformerRef {
                qid: "Q396".into(),
                role: Some("performer".into()),
            }],
            parent_album: vec![],
            tracks: vec![],
        };
        writer.write_batch(&[entity]).unwrap();
        writer.flush().unwrap();
        let batches = read_parquet(&dir.path().join("part-00001.parquet")).unwrap();
        // The albums JSON column serializes PerformerRef with a `qid` key —
        // never the old inverted `album_id`/`track_id` key names.
        let albums_str = col_to_strings(&batches[0], 10);
        assert!(albums_str[0].as_ref().unwrap().contains(r#""qid":"Q396""#));
        assert!(
            albums_str[0]
                .as_ref()
                .unwrap()
                .contains(r#""role":"performer""#)
        );
        assert!(!albums_str[0].as_ref().unwrap().contains("album_id"));
        assert!(!albums_str[0].as_ref().unwrap().contains("track_id"));
        // role + parents columns round-trip on the same row.
        assert_eq!(col_to_strings(&batches[0], 12), vec![Some("Album".into())]);
        assert_eq!(col_to_strings(&batches[0], 13), vec![Some("[]".into())]);
    }
}
