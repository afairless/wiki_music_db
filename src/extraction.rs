//! MusicEntity extraction from filtered Wikidata entities.
//!
//! This module defines the intermediate representation for music-relevant
//! Wikidata entities and the logic to extract fields from a [`FilteredEntity`]
//! (produced by the streaming parser and filter in Phase 2b).
//!
//! Data contract (transformation → output stage boundary):
//!
//! - `id`: always present (entities missing a Q-ID are rejected at ingestion)
//! - `name`: None means the entity has no English label (logged at WARN; stored as NULL)
//! - `description`: None means no English description (common; stored as NULL, not an error)
//! - `birth_date`, `death_date`: None means either no date property or an unparseable
//!   date string (logged at WARN with the raw value; stored as NULL)
//! - `inclusion_reason`: why the entity passed the music filter
//! - All `Vec` fields default to empty (not None) — empty collections mean no data

use std::collections::HashSet;

use chrono::NaiveDate;

use crate::wikidata::model::Entity;
use crate::wikidata::stream::FilteredEntity;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Intermediate representation of a music-relevant Wikidata entity.
#[derive(Debug, Clone, PartialEq)]
pub struct MusicEntity {
    /// Wikidata Q-ID (e.g. "Q2831").
    pub id: String,
    /// English label, or None if missing.
    pub name: Option<String>,
    /// English description, or None if missing.
    pub description: Option<String>,
    /// "person" or "group".
    pub artist_type: String,
    /// Why the entity passed the music filter (e.g. "P106:Q639669").
    pub inclusion_reason: String,
    /// Date of birth, or None (parsed from P569).
    pub birth_date: Option<NaiveDate>,
    /// Date of death, or None (parsed from P570).
    pub death_date: Option<NaiveDate>,
    /// Genre Q-IDs collected from P136 claims.
    pub genres: Vec<String>,
    /// Instrument Q-IDs collected from P1303 claims.
    pub instruments: Vec<String>,
    /// Group Q-IDs the entity is a member of (P463).
    pub member_of: Vec<String>,
    /// Album references (P175 — performer on an album).
    pub albums: Vec<AlbumRef>,
    /// Track references (P658 — performer on a track).
    pub tracks: Vec<TrackRef>,
}

/// Reference to an album with an optional role.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct AlbumRef {
    /// The album's Wikidata Q-ID.
    pub album_id: String,
    /// Optional role (e.g. "performer", "producer").
    pub role: Option<String>,
}

/// Reference to a track with an optional role.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct TrackRef {
    /// The track's Wikidata Q-ID.
    pub track_id: String,
    /// Optional role (e.g. "performer", "composer").
    pub role: Option<String>,
}

/// A genre entry with Q-ID and English label, used for the second-pass
/// genre label extraction (Step 3).
#[derive(Debug, Clone, PartialEq)]
pub struct GenreEntry {
    /// Wikidata Q-ID.
    pub id: String,
    /// English label.
    pub name: String,
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

/// Extract a `MusicEntity` from a filtered Wikidata entity.
///
/// Returns `Err` only when `entity.id` is empty/missing. All other
/// missing or unparseable data is represented as `None` or an empty `Vec`.
///
/// # Errors
///
/// Returns [`crate::error::Error::EntityMissingId`] if the entity has
/// no valid Wikidata ID.
pub fn extract_music_entity(
    filtered: &FilteredEntity,
    genre_qids: &mut HashSet<String>,
) -> Result<MusicEntity, crate::error::Error> {
    let entity = &filtered.entity;
    let inclusion_reason = &filtered.inclusion_reason;

    if entity.id.is_empty() {
        return Err(crate::error::Error::EntityMissingId { line: 0 });
    }

    // Determine artist_type:
    // "group" if inclusion_reason starts with "P31:" (matched as a music group)
    // "person" otherwise (occupation or catch-all match)
    let artist_type = if inclusion_reason.starts_with("P31:") {
        "group".to_string()
    } else {
        "person".to_string()
    };

    // Extract English label
    let name = entity.labels.as_ref().and_then(|l| l.en()).map(|s| {
        if s.is_empty() {
            tracing::warn!(
                entity_id = %entity.id,
                "Entity has empty English label"
            );
        }
        s.to_string()
    });

    if name.is_none() {
        tracing::warn!(
            entity_id = %entity.id,
            "Entity has no English label"
        );
    }

    // Extract English description
    let description = entity
        .descriptions
        .as_ref()
        .and_then(|d| d.en())
        .map(|s| s.to_string());

    // Extract dates and genre/instrument/member/album/track claims
    let mut birth_date: Option<NaiveDate> = None;
    let mut death_date: Option<NaiveDate> = None;
    let mut genres: Vec<String> = Vec::new();
    let mut instruments: Vec<String> = Vec::new();
    let mut member_of: Vec<String> = Vec::new();
    let mut albums: Vec<AlbumRef> = Vec::new();
    let mut tracks: Vec<TrackRef> = Vec::new();

    for (prop_id, claims) in &entity.claims {
        match prop_id.as_str() {
            "P569" => {
                // Date of birth
                if birth_date.is_none() {
                    birth_date = extract_date(claims, &entity.id);
                }
            }
            "P570" => {
                // Date of death
                if death_date.is_none() {
                    death_date = extract_date(claims, &entity.id);
                }
            }
            "P136" => {
                // Genre
                for qid in extract_qids(claims) {
                    genres.push(qid.clone());
                    genre_qids.insert(qid);
                }
            }
            "P1303" => {
                // Instrument
                instruments.extend(extract_qids(claims));
            }
            "P463" => {
                // Member of (group membership)
                member_of.extend(extract_qids(claims));
            }
            "P175" => {
                // Performer (album reference)
                for qid in extract_qids(claims) {
                    albums.push(AlbumRef {
                        album_id: qid,
                        role: Some("performer".to_string()),
                    });
                }
            }
            "P658" => {
                // Track (performer on a track)
                for qid in extract_qids(claims) {
                    tracks.push(TrackRef {
                        track_id: qid,
                        role: Some("performer".to_string()),
                    });
                }
            }
            _ => {
                // All other properties are ignored
            }
        }
    }

    Ok(MusicEntity {
        id: entity.id.clone(),
        name,
        description,
        artist_type,
        inclusion_reason: inclusion_reason.clone(),
        birth_date,
        death_date,
        genres,
        instruments,
        member_of,
        albums,
        tracks,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the target Q-ID from a list of claims' mainsnak datavalues.
///
/// Returns all non-empty IDs found.
fn extract_qids(claims: &[crate::wikidata::model::Claim]) -> Vec<String> {
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
        .collect()
}

/// Extract a date from the first claim with a valid time string.
///
/// Logs a WARN and returns `None` if parsing fails.
fn extract_date(claims: &[crate::wikidata::model::Claim], entity_id: &str) -> Option<NaiveDate> {
    for claim in claims {
        if let Some(time) = claim
            .mainsnak
            .as_ref()
            .and_then(|m| m.datavalue.as_ref())
            .and_then(|dv| dv.time.as_deref())
        {
            match parse_wikidata_date(time) {
                Some(date) => return Some(date),
                None => {
                    tracing::warn!(
                        entity_id = entity_id,
                        raw = time,
                        "Failed to parse date value"
                    );
                }
            }
        }
    }
    None
}

/// Parse a Wikidata time string into `chrono::NaiveDate`.
///
/// Wikidata format: `+1926-09-23T00:00:00Z` or `+1926-09-23`.
/// Strips leading `+`, splits on `T`, and parses the date portion.
fn parse_wikidata_date(raw: &str) -> Option<NaiveDate> {
    let stripped = raw.strip_prefix('+').unwrap_or(raw);
    let date_str = stripped.split('T').next().unwrap_or(stripped);
    NaiveDate::parse_from_str(date_str, "%Y-%m-%d").ok()
}

// ---------------------------------------------------------------------------
// Genre label extraction (Step 3)
// ---------------------------------------------------------------------------

/// Collect all genre Q-IDs referenced in a MusicEntity.
pub fn collect_genre_qids(entity: &MusicEntity) -> Vec<String> {
    entity.genres.clone()
}

/// Collect genre Q-IDs from all entities in a batch.
pub fn collect_all_genre_qids(entities: &[MusicEntity]) -> HashSet<String> {
    entities
        .iter()
        .flat_map(|e| e.genres.iter().cloned())
        .collect()
}

/// Extract genre labels from a Wikidata `Entity`.
///
/// Returns `Some(GenreEntry)` if the entity has a valid ID and English label.
pub fn extract_genre_entity(entity: &Entity) -> Option<GenreEntry> {
    let id = entity.id.clone();
    if id.is_empty() {
        return None;
    }
    let name = entity.labels.as_ref()?.en()?.to_string();
    if name.is_empty() {
        return None;
    }
    Some(GenreEntry { id, name })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata::model::{
        Claim, DatavalueValue, Descriptions, Entity, Labels, LanguageValue, Mainsnak,
    };
    use crate::wikidata::stream::FilteredEntity;
    use std::collections::HashMap;

    // -----------------------------------------------------------------------
    // Fixture helpers
    // -----------------------------------------------------------------------

    fn make_claim_with_id(id: &str) -> Claim {
        Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some(id.into()),
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        }
    }

    fn make_claim_with_time(time: &str) -> Claim {
        Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: None,
                    time: Some(time.into()),
                }),
            }),
            extra: HashMap::new(),
        }
    }

    fn filtered_entity(
        id: &str,
        labels: Option<HashMap<String, LanguageValue>>,
        descriptions: Option<HashMap<String, LanguageValue>>,
        claims: HashMap<String, Vec<Claim>>,
        inclusion_reason: &str,
    ) -> FilteredEntity {
        FilteredEntity {
            entity: Entity {
                id: id.into(),
                entity_type: "item".into(),
                labels: labels.map(Labels),
                descriptions: descriptions.map(Descriptions),
                claims,
            },
            inclusion_reason: inclusion_reason.into(),
        }
    }

    fn en_label(value: &str) -> HashMap<String, LanguageValue> {
        let mut m = HashMap::new();
        m.insert(
            "en".into(),
            LanguageValue {
                value: value.into(),
            },
        );
        m
    }

    fn en_description(value: &str) -> HashMap<String, LanguageValue> {
        let mut m = HashMap::new();
        m.insert(
            "en".into(),
            LanguageValue {
                value: value.into(),
            },
        );
        m
    }

    // -----------------------------------------------------------------------
    // Parse helper tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_parse_wikidata_date_valid() {
        let date = parse_wikidata_date("+1926-09-23T00:00:00Z");
        assert_eq!(date, NaiveDate::from_ymd_opt(1926, 9, 23));
    }

    #[test]
    fn test_parse_wikidata_date_no_time() {
        let date = parse_wikidata_date("+1926-09-23");
        assert_eq!(date, NaiveDate::from_ymd_opt(1926, 9, 23));
    }

    #[test]
    fn test_parse_wikidata_date_invalid() {
        let date = parse_wikidata_date("not-a-date");
        assert_eq!(date, None);
    }

    #[test]
    fn test_parse_wikidata_date_no_plus() {
        let date = parse_wikidata_date("1926-09-23");
        assert_eq!(date, NaiveDate::from_ymd_opt(1926, 9, 23));
    }

    // -----------------------------------------------------------------------
    // Extraction tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_extract_musician() {
        // Build a FilteredEntity matching the musician fixture
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P106".into(), vec![make_claim_with_id("Q639669")]);
        claims.insert("P31".into(), vec![make_claim_with_id("Q5")]); // human, not group
        claims.insert("P136".into(), vec![make_claim_with_id("Q35718")]); // genre
        claims.insert(
            "P569".into(),
            vec![make_claim_with_time("+1972-03-22T00:00:00Z")],
        );

        let fe = filtered_entity(
            "Q2831",
            Some(en_label("Ivy Queen")),
            Some(en_description("American singer-songwriter and musician")),
            claims,
            "P106:Q639669",
        );

        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract musician");

        assert_eq!(me.id, "Q2831");
        assert_eq!(me.name, Some("Ivy Queen".to_string()));
        assert_eq!(me.artist_type, "person");
        assert_eq!(me.inclusion_reason, "P106:Q639669");
        assert_eq!(me.birth_date, NaiveDate::from_ymd_opt(1972, 3, 22));
        assert!(me.death_date.is_none());
        assert_eq!(me.genres, vec!["Q35718"]);
        assert!(me.instruments.is_empty());
        assert!(me.member_of.is_empty());
        assert!(me.albums.is_empty());
        assert!(me.tracks.is_empty());

        // Genre Q-ID should be collected
        assert!(genre_qids.contains("Q35718"));
    }

    #[test]
    fn test_extract_band() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P31".into(), vec![make_claim_with_id("Q215380")]); // musical group
        claims.insert("P136".into(), vec![make_claim_with_id("Q57251")]); // rock music

        let fe = filtered_entity(
            "Q11649",
            Some(en_label("The Beatles")),
            Some(en_description("English rock band")),
            claims,
            "P31:Q215380",
        );

        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract band");

        assert_eq!(me.id, "Q11649");
        assert_eq!(me.name, Some("The Beatles".to_string()));
        assert_eq!(me.artist_type, "group");
        assert_eq!(me.inclusion_reason, "P31:Q215380");
        assert!(me.birth_date.is_none());
        assert!(me.death_date.is_none());
        assert_eq!(me.genres, vec!["Q57251"]);
        assert!(me.member_of.is_empty());
    }

    #[test]
    fn test_extract_missing_label() {
        let fe = filtered_entity(
            "Q42",
            None, // no labels at all
            None,
            HashMap::new(),
            "PROP:P136",
        );

        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract with missing label");

        assert_eq!(me.id, "Q42");
        assert!(me.name.is_none(), "Expected name to be None");
    }

    #[test]
    fn test_extract_missing_description() {
        let fe = filtered_entity(
            "Q42",
            Some(en_label("Test")),
            None,
            HashMap::new(),
            "PROP:P136",
        );

        let mut genre_qids = HashSet::new();
        let me =
            extract_music_entity(&fe, &mut genre_qids).expect("extract with missing description");

        assert_eq!(me.id, "Q42");
        assert!(me.description.is_none(), "Expected description to be None");
    }

    #[test]
    fn test_extract_birth_date() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert(
            "P569".into(),
            vec![make_claim_with_time("+1985-06-21T00:00:00Z")],
        );

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "P106:Q639669");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract with birth date");

        assert_eq!(me.birth_date, NaiveDate::from_ymd_opt(1985, 6, 21));
        assert!(me.death_date.is_none());
    }

    #[test]
    fn test_extract_invalid_date() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert(
            "P569".into(),
            vec![make_claim_with_time("not-a-valid-date")],
        );

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "P106:Q639669");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract with invalid date");

        // Invalid date should result in None, not rejection
        assert_eq!(me.birth_date, None);
    }

    #[test]
    fn test_extract_no_genres() {
        let fe = filtered_entity(
            "Q123",
            Some(en_label("Test")),
            None,
            HashMap::new(),
            "P106:Q639669",
        );

        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract with no genres");

        assert!(me.genres.is_empty());
        assert!(genre_qids.is_empty());
    }

    #[test]
    fn test_extract_multiple_genres() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert(
            "P136".into(),
            vec![make_claim_with_id("Q35718"), make_claim_with_id("Q57251")],
        );

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "PROP:P136");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract multiple genres");

        assert_eq!(me.genres.len(), 2);
        assert!(me.genres.contains(&"Q35718".to_string()));
        assert!(me.genres.contains(&"Q57251".to_string()));
        assert!(genre_qids.contains("Q35718"));
        assert!(genre_qids.contains("Q57251"));
    }

    #[test]
    fn test_extract_instruments() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P1303".into(), vec![make_claim_with_id("Q171")]);

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "PROP:P1303");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract instruments");

        assert_eq!(me.instruments, vec!["Q171"]);
    }

    #[test]
    fn test_extract_member_of() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P463".into(), vec![make_claim_with_id("Q11649")]);

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "P106:Q639669");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract member_of");

        assert_eq!(me.member_of, vec!["Q11649"]);
    }

    #[test]
    fn test_extract_albums() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P175".into(), vec![make_claim_with_id("Q12345")]);

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "P106:Q639669");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract albums");

        assert_eq!(me.albums.len(), 1);
        assert_eq!(me.albums[0].album_id, "Q12345");
        assert_eq!(me.albums[0].role, Some("performer".to_string()));
    }

    #[test]
    fn test_extract_tracks() {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.insert("P658".into(), vec![make_claim_with_id("Q54321")]);

        let fe = filtered_entity("Q123", Some(en_label("Test")), None, claims, "P106:Q639669");
        let mut genre_qids = HashSet::new();
        let me = extract_music_entity(&fe, &mut genre_qids).expect("extract tracks");

        assert_eq!(me.tracks.len(), 1);
        assert_eq!(me.tracks[0].track_id, "Q54321");
        assert_eq!(me.tracks[0].role, Some("performer".to_string()));
    }

    #[test]
    fn test_extract_empty_id_rejected() {
        let fe = filtered_entity("", None, None, HashMap::new(), "PROP:P136");
        let mut genre_qids = HashSet::new();
        let result = extract_music_entity(&fe, &mut genre_qids);

        assert!(result.is_err(), "Empty ID should be rejected");
        match result {
            Err(crate::error::Error::EntityMissingId { line: 0 }) => { /* expected */ }
            other => panic!("Expected EntityMissingId, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // Genre collection tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_collect_genre_qids() {
        let entity = MusicEntity {
            id: "Q1".into(),
            name: None,
            description: None,
            artist_type: "person".into(),
            inclusion_reason: "test".into(),
            birth_date: None,
            death_date: None,
            genres: vec!["Q35718".into(), "Q57251".into()],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            tracks: vec![],
        };

        let qids = collect_genre_qids(&entity);
        assert_eq!(qids, vec!["Q35718", "Q57251"]);
    }

    #[test]
    fn test_collect_all_genre_qids() {
        let entities = vec![
            MusicEntity {
                genres: vec!["Q35718".into()],
                ..create_dummy("Q1")
            },
            MusicEntity {
                genres: vec!["Q57251".into()],
                ..create_dummy("Q2")
            },
            MusicEntity {
                genres: vec!["Q35718".into()],
                ..create_dummy("Q3")
            },
        ];

        let all = collect_all_genre_qids(&entities);
        assert_eq!(all.len(), 2);
        assert!(all.contains("Q35718"));
        assert!(all.contains("Q57251"));
    }

    #[test]
    fn test_extract_genre_entity() {
        let entity = Entity {
            id: "Q35718".into(),
            entity_type: "item".into(),
            labels: Some(Labels({
                let mut m = HashMap::new();
                m.insert(
                    "en".into(),
                    LanguageValue {
                        value: "jazz".into(),
                    },
                );
                m
            })),
            descriptions: None,
            claims: HashMap::new(),
        };

        let entry = extract_genre_entity(&entity);
        assert!(entry.is_some());
        assert_eq!(
            entry.unwrap(),
            GenreEntry {
                id: "Q35718".into(),
                name: "jazz".into(),
            }
        );
    }

    #[test]
    fn test_extract_genre_entity_no_label() {
        let entity = Entity {
            id: "Q35718".into(),
            entity_type: "item".into(),
            labels: None,
            descriptions: None,
            claims: HashMap::new(),
        };

        let entry = extract_genre_entity(&entity);
        assert!(entry.is_none());
    }

    #[test]
    fn test_extract_genre_entity_empty_id() {
        let entity = Entity {
            id: "".into(),
            entity_type: "item".into(),
            labels: Some(Labels({
                let mut m = HashMap::new();
                m.insert(
                    "en".into(),
                    LanguageValue {
                        value: "jazz".into(),
                    },
                );
                m
            })),
            descriptions: None,
            claims: HashMap::new(),
        };

        let entry = extract_genre_entity(&entity);
        assert!(entry.is_none());
    }

    fn create_dummy(id: &str) -> MusicEntity {
        MusicEntity {
            id: id.into(),
            name: None,
            description: None,
            artist_type: "person".into(),
            inclusion_reason: "test".into(),
            birth_date: None,
            death_date: None,
            genres: vec![],
            instruments: vec![],
            member_of: vec![],
            albums: vec![],
            tracks: vec![],
        }
    }
}
