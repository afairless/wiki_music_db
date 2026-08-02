//! Streaming parser for gzipped Wikidata JSON dump files.
//!
//! Reads `latest-all.json.gz` line-by-line, applies the music entity filter,
//! and yields [`StreamEvent`] values representing filtered entities, rejected
//! lines, and skipped delimiters.

use std::io::BufRead;
use std::path::Path;

use anyhow::{Context, Result};
use flate2::read::MultiGzDecoder;

use crate::wikidata::filter::{FilterResult, is_music_entity};
use crate::wikidata::model::Entity;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A deserialized entity that passed the music filter, along with the reason.
#[derive(Debug, Clone, PartialEq)]
pub struct FilteredEntity {
    pub entity: Entity,
    pub inclusion_reason: String,
}

/// Events produced by the streaming parser.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamEvent {
    /// A filtered entity was found.
    Filtered(FilteredEntity),
    /// A line could not be parsed as a valid entity.
    Rejected {
        /// The 1-based line number in the dump file.
        line: u64,
        /// The error message describing why parsing failed.
        reason: String,
        /// The raw line text, if available (may be truncated).
        raw: Option<String>,
    },
    /// A JSON delimiter line ('[' or ']') that was skipped.
    Skipped,
}

/// Streaming reader for gzipped Wikidata JSON dump files.
///
/// Reads the file lazily, one line at a time. Each line is trimmed, trailing
/// commas are removed, and the line is deserialised as an [`Entity`]. If the
/// entity passes the music filter, a [`StreamEvent::Filtered`] is yielded.
pub struct StreamReader {
    reader: std::io::BufReader<MultiGzDecoder<std::fs::File>>,
    line_buf: String,
    line_number: u64,
    /// Accumulated counters: (processed_lines, filtered_entities, rejected_lines).
    /// Processed lines include all non-delimiter lines (both valid and invalid).
    processed: u64,
    filtered: u64,
    rejected: u64,
}

impl StreamReader {
    /// Open a gzipped Wikidata dump file for streaming.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or if the gzip
    /// header is invalid.
    pub fn new(path: &Path) -> Result<Self> {
        let file = std::fs::File::open(path)
            .with_context(|| format!("Failed to open dump file: {}", path.display()))?;
        let decoder = MultiGzDecoder::new(file);
        let reader = std::io::BufReader::new(decoder);

        Ok(StreamReader {
            reader,
            line_buf: String::new(),
            line_number: 0,
            processed: 0,
            filtered: 0,
            rejected: 0,
        })
    }

    /// Read the next event from the dump stream.
    ///
    /// Returns `Ok(None)` when the stream is exhausted, `Ok(Some(event))`
    /// for each non-EOF line, or `Err` for I/O errors.
    pub fn next_event(&mut self) -> Result<Option<StreamEvent>> {
        self.line_buf.clear();
        let bytes_read = self
            .reader
            .read_line(&mut self.line_buf)
            .context("Failed to read line from dump file")?;

        if bytes_read == 0 {
            // EOF
            return Ok(None);
        }

        self.line_number += 1;
        let trimmed = self.line_buf.trim();

        // Skip JSON array delimiters
        if trimmed == "[" || trimmed == "]" {
            self.processed += 1;
            return Ok(Some(StreamEvent::Skipped));
        }

        // Skip empty lines
        if trimmed.is_empty() {
            return Ok(Some(StreamEvent::Skipped));
        }

        // Strip trailing comma (Wikidata dump has comma-separated JSON objects)
        let line = trimmed.trim_end_matches(',');

        match serde_json::from_str::<Entity>(line) {
            Ok(entity) => {
                self.processed += 1;
                match is_music_entity(&entity.claims) {
                    FilterResult::Included(reason) => {
                        self.filtered += 1;
                        Ok(Some(StreamEvent::Filtered(FilteredEntity {
                            entity,
                            inclusion_reason: reason,
                        })))
                    }
                    FilterResult::Excluded => {
                        // Valid entity but not music-related — skip silently
                        Ok(Some(StreamEvent::Skipped))
                    }
                }
            }
            Err(e) => {
                self.processed += 1;
                self.rejected += 1;
                let raw = Some(trimmed.to_string());
                tracing::warn!(
                    line = self.line_number,
                    reason = %e,
                    "Rejected malformed Wikidata line"
                );
                Ok(Some(StreamEvent::Rejected {
                    line: self.line_number,
                    reason: e.to_string(),
                    raw,
                }))
            }
        }
    }

    /// Drain the entire stream and return aggregate counters.
    ///
    /// Returns `(processed_lines, filtered_entities, rejected_lines)`.
    /// This is a convenience method for callers that only need totals.
    pub fn count_entities(&mut self) -> Result<(u64, u64, u64)> {
        while self.next_event()?.is_some() {
            // Drain
        }
        Ok((self.processed, self.filtered, self.rejected))
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    /// Returns the number of processed lines so far.
    pub fn processed(&self) -> u64 {
        self.processed
    }

    /// Returns the number of filtered entities so far.
    pub fn filtered(&self) -> u64 {
        self.filtered
    }

    /// Returns the number of rejected lines so far.
    pub fn rejected(&self) -> u64 {
        self.rejected
    }

    /// Returns the current 1-based line number.
    pub fn line_number(&self) -> u64 {
        self.line_number
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata::model::DatavalueValue;
    use std::io::Write;

    /// Helper: create a temporary gzipped file with the given content.
    fn write_gz(content: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("create temp dir");
        let path = dir.path().join("test_dump.json.gz");
        let file = std::fs::File::create(&path).expect("create temp file");
        let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::fast());
        encoder
            .write_all(content.as_bytes())
            .expect("write gz content");
        encoder.finish().expect("finish gz");
        (dir, path)
    }

    fn read_all(reader: &mut StreamReader) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        while let Ok(Some(event)) = reader.next_event() {
            events.push(event);
        }
        events
    }

    /// Build content for a test dump with the given lines.
    fn make_dump(lines: &[&str]) -> String {
        lines.join("\n")
    }

    // --- Basic streaming ---

    #[test]
    fn test_stream_single_musician() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q2831","type":"item","labels":{"en":{"value":"Ivy Queen"}},"claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}},"#,
            r#"{"id":"Q42","type":"item","labels":{"en":{"value":"Douglas Adams"}},"claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}}"#,
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");

        let events = read_all(&mut reader);
        // 2 entities + 2 delimiters = 4 events
        assert_eq!(events.len(), 4);

        // First event should be Skipped for '['
        assert_eq!(events[0], StreamEvent::Skipped);

        // Second event should be the musician
        match &events[1] {
            StreamEvent::Filtered(fe) => {
                assert_eq!(fe.entity.id, "Q2831");
                assert_eq!(fe.inclusion_reason, "P106:Q639669");
            }
            other => panic!("Expected Filtered, got {other:?}"),
        }

        // Third event should be Skipped for the non-musician (excluded)
        assert_eq!(events[2], StreamEvent::Skipped);

        // Fourth event should be Skipped for ']'
        assert_eq!(events[3], StreamEvent::Skipped);

        assert_eq!(reader.processed(), 4);
        assert_eq!(reader.filtered(), 1);
        assert_eq!(reader.rejected(), 0);
    }

    #[test]
    fn test_stream_empty_array() {
        let content = make_dump(&["[", "]"]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let events = read_all(&mut reader);
        // [ and ] — 2 skipped
        assert_eq!(events.len(), 2);
        assert!(events.iter().all(|e| e == &StreamEvent::Skipped));
    }

    #[test]
    fn test_stream_all_excluded() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q42","type":"item","claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}},"#,
            r#"{"id":"Q90","type":"item","claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5107"}}}}]}}"#,
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let events = read_all(&mut reader);
        // 2 delimiters + 2 excluded entities (skipped) = 4 Skipped
        assert_eq!(events.len(), 4);
        assert!(events.iter().all(|e| e == &StreamEvent::Skipped));
        assert_eq!(reader.filtered(), 0);
    }

    // --- Malformed lines ---

    #[test]
    fn test_stream_malformed_line() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q2831","type":"item","claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}},"#,
            "{truncated",
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let events = read_all(&mut reader);

        // Event sequence: Skipped([) -> Filtered(Q2831) -> Rejected -> Skipped(])
        assert_eq!(events.len(), 4);

        // Validate the Rejected event
        match &events[2] {
            StreamEvent::Rejected {
                line,
                reason: _,
                raw,
            } => {
                assert_eq!(*line, 3);
                assert!(raw.as_deref().unwrap().contains("truncated"));
            }
            other => panic!("Expected Rejected, got {other:?}"),
        }

        // processed: [ (1) + Q2831 (2) + {truncated (3) + ] (4) = 4
        assert_eq!(reader.processed(), 4);
        assert_eq!(reader.filtered(), 1);
        assert_eq!(reader.rejected(), 1);
    }

    // --- Trailing comma handling ---

    #[test]
    fn test_stream_strips_trailing_comma() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q2831","type":"item","claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}},"#,
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let events = read_all(&mut reader);

        // Should parse the entity despite the trailing comma before ]
        let filtered: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Filtered(fe) => Some(fe.entity.id.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(filtered, vec!["Q2831"]);
    }

    // --- count_entities ---

    #[test]
    fn test_count_entities() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q2831","type":"item","claims":{"P106":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q639669"}}}}]}},"#,
            r#"{"id":"Q42","type":"item","claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5"}}}}]}},"#,
            "{broken",
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let (processed, filtered, rejected) = reader.count_entities().expect("count");
        // processed = 5 ([ + musician + non-musician + broken + ]), filtered = 1, rejected = 1
        assert_eq!((processed, filtered, rejected), (5, 1, 1));
    }

    // --- Streaming with no music claims ---

    #[test]
    fn test_stream_non_music_entity_skipped() {
        let content = make_dump(&[
            "[",
            r#"{"id":"Q90","type":"item","claims":{"P31":[{"mainsnak":{"snaktype":"value","datavalue":{"value":{"id":"Q5107"}}}}]}}"#,
            "]",
        ]);
        let (_dir, path) = write_gz(&content);
        let mut reader = StreamReader::new(&path).expect("open stream");
        let events = read_all(&mut reader);
        // [ , excluded entity (skipped), ] => all Skipped
        assert_eq!(events.len(), 3);
        assert!(events.iter().all(|e| e == &StreamEvent::Skipped));
    }

    // --- FilteredEntity fields ---

    #[test]
    fn test_filtered_entity_round_trip() {
        let entity = Entity {
            id: "Q2831".into(),
            entity_type: "item".into(),
            labels: None,
            descriptions: None,
            claims: {
                let mut map = std::collections::HashMap::new();
                map.insert(
                    "P106".into(),
                    vec![crate::wikidata::model::Claim {
                        mainsnak: Some(crate::wikidata::model::Mainsnak {
                            snaktype: "value".into(),
                            datavalue: Some(DatavalueValue {
                                precision: None,
                                id: Some("Q639669".into()),
                                time: None,
                            }),
                        }),
                        extra: std::collections::HashMap::new(),
                    }],
                );
                map
            },
        };

        let fe = FilteredEntity {
            inclusion_reason: "P106:Q639669".into(),
            entity,
        };

        assert_eq!(fe.inclusion_reason, "P106:Q639669");
        assert_eq!(fe.entity.id, "Q2831");
    }
}
