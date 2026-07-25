//! Serde structs for the Wikidata JSON entity model.
//!
//! These types represent the top-level structure of a Wikidata entity as
//! returned by the Wikidata API and the JSON dump format. Only the fields
//! needed for music-relevant filtering are captured; qualifiers and
//! references are ignored entirely for v1.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A top-level Wikidata entity.
///
/// Corresponds to a single JSON object in the `latest-all.json.gz` dump.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entity {
    /// The Q-ID (e.g. `"Q2831"`).
    pub id: String,

    /// The entity type (e.g. `"item"`).
    #[serde(rename = "type")]
    pub entity_type: String,

    /// Language-keyed labels. `None` when the entity has no labels at all.
    pub labels: Option<Labels>,

    /// Language-keyed descriptions. `None` when the entity has no descriptions.
    pub descriptions: Option<Descriptions>,

    /// Claims keyed by property ID (e.g. `"P106"`, `"P31"`).
    /// Defaults to an empty map when absent.
    #[serde(default)]
    pub claims: HashMap<String, Vec<Claim>>,
}

/// Language-keyed labels.
///
/// A newtype over `HashMap<String, LanguageValue>` where the key is a
/// language code (e.g. `"en"`, `"de"`, `"fr"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Labels(pub HashMap<String, LanguageValue>);

impl Labels {
    /// Returns the English label value, if present.
    pub fn en(&self) -> Option<&str> {
        self.0.get("en").map(|lv| lv.value.as_str())
    }
}

/// Language-keyed descriptions.
///
/// A newtype over `HashMap<String, LanguageValue>` where the key is a
/// language code (e.g. `"en"`, `"de"`, `"fr"`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Descriptions(pub HashMap<String, LanguageValue>);

impl Descriptions {
    /// Returns the English description value, if present.
    pub fn en(&self) -> Option<&str> {
        self.0.get("en").map(|lv| lv.value.as_str())
    }
}

/// A value in a specific language (used inside `Labels` and `Descriptions`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LanguageValue {
    pub value: String,
}

/// A single claim (statement) on a Wikidata entity.
///
/// The only field we explicitly capture is `mainsnak`. All other fields
/// (`id`, `rank`, `qualifiers`, `references`, `hash`, etc.) are absorbed
/// via `#[serde(flatten)]` and silently ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Claim {
    pub mainsnak: Mainsnak,

    /// Catch-all for extra fields we don't explicitly model.
    #[serde(flatten)]
    #[serde(default)]
    pub extra: HashMap<String, serde_json::Value>,
}

/// A mainsnak object within a claim.
///
/// Uses serde-derive for now. Step 3 will replace this with a custom
/// `Deserialize` implementation that handles the three `snaktype`
/// variants (`value`, `novalue`, `somevalue`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Mainsnak {
    /// The snak type: `"value"`, `"novalue"`, or `"somevalue"`.
    pub snaktype: String,

    /// The optional data value. `None` for `novalue` and `somevalue`.
    pub datavalue: Option<Datavalue>,
}

/// A typed data value inside a mainsnak.
///
/// The `datavalue_type` field records the type of value (e.g.
/// `"wikibase-entityid"`, `"time"`, `"string"`), and `value` holds
/// the raw JSON payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Datavalue {
    /// The type discriminator (e.g. `"wikibase-entityid"`, `"time"`).
    #[serde(rename = "type")]
    pub datavalue_type: String,

    /// The raw value payload. Contains the nested object from which
    /// `id` and `time` are extracted in the custom deserializer.
    pub value: serde_json::Value,
}

/// Pre-defined value extraction type (used by the Step 3 custom deserializer).
///
/// Only `id` (for Q-ID references) and `time` (for dates) are captured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EntityValue {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Round-trip: serialize a known `Entity` to JSON, deserialize back,
    /// and verify fields match.
    #[test]
    fn test_entity_round_trip() {
        let entity = Entity {
            id: "Q2831".into(),
            entity_type: "item".into(),
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
            descriptions: Some(Descriptions({
                let mut m = HashMap::new();
                m.insert(
                    "en".into(),
                    LanguageValue {
                        value: "American singer-songwriter and musician".into(),
                    },
                );
                m
            })),
            claims: {
                let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
                claims.insert(
                    "P106".into(),
                    vec![Claim {
                        mainsnak: Mainsnak {
                            snaktype: "value".into(),
                            datavalue: Some(Datavalue {
                                datavalue_type: "wikibase-entityid".into(),
                                value: serde_json::json!({
                                    "id": "Q639669"
                                }),
                            }),
                        },
                        extra: HashMap::new(),
                    }],
                );
                claims
            },
        };

        let json = serde_json::to_string(&entity).expect("serialize");
        let deserialized: Entity = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(deserialized.id, "Q2831");
        assert_eq!(deserialized.entity_type, "item");
        assert_eq!(
            deserialized.labels.as_ref().and_then(|l| l.en()),
            Some("Ivy Queen")
        );
        assert_eq!(
            deserialized.descriptions.as_ref().and_then(|d| d.en()),
            Some("American singer-songwriter and musician")
        );
        assert_eq!(deserialized.claims.len(), 1);
        let p106 = &deserialized.claims["P106"];
        assert_eq!(p106.len(), 1);
        assert_eq!(p106[0].mainsnak.snaktype, "value");
    }

    /// Empty claims: deserialize an entity with no `claims` field — verify
    /// it defaults to an empty map.
    #[test]
    fn test_empty_claims_defaults_to_empty_map() {
        let json = r#"{
            "id": "Q42",
            "type": "item",
            "labels": { "en": { "value": "Test" } },
            "descriptions": { "en": { "value": "Test entity" } }
        }"#;
        let entity: Entity = serde_json::from_str(json).expect("deserialize");
        assert!(entity.claims.is_empty());
    }

    /// Missing labels: deserialize JSON with no `labels` field — verify
    /// `labels` is `None`.
    #[test]
    fn test_missing_labels_is_none() {
        let json = r#"{
            "id": "Q42",
            "type": "item",
            "claims": {}
        }"#;
        let entity: Entity = serde_json::from_str(json).expect("deserialize");
        assert!(entity.labels.is_none());
        assert!(entity.descriptions.is_none());
    }

    /// Entity with `type` field: verify the `type` field maps to `entity_type`.
    #[test]
    fn test_type_field_maps_to_entity_type() {
        let json = r#"{
            "id": "Q42",
            "type": "item"
        }"#;
        let entity: Entity = serde_json::from_str(json).expect("deserialize");
        assert_eq!(entity.entity_type, "item");
    }

    /// Accessor: Labels.en() returns None when "en" key is missing.
    #[test]
    fn test_labels_en_returns_none_when_missing() {
        let labels = Labels({
            let mut m = HashMap::new();
            m.insert(
                "de".into(),
                LanguageValue {
                    value: "Deutscher Name".into(),
                },
            );
            m
        });
        assert_eq!(labels.en(), None);
    }

    /// Accessor: Labels.en() returns Some when "en" key is present.
    #[test]
    fn test_labels_en_returns_value_when_present() {
        let labels = Labels({
            let mut m = HashMap::new();
            m.insert(
                "en".into(),
                LanguageValue {
                    value: "English Name".into(),
                },
            );
            m
        });
        assert_eq!(labels.en(), Some("English Name"));
    }

    /// Accessor: Descriptions.en() returns None when "en" key is missing.
    #[test]
    fn test_descriptions_en_returns_none_when_missing() {
        let descriptions = Descriptions({
            let mut m = HashMap::new();
            m.insert(
                "fr".into(),
                LanguageValue {
                    value: "Description française".into(),
                },
            );
            m
        });
        assert_eq!(descriptions.en(), None);
    }

    /// Accessor: Descriptions.en() returns Some when "en" key is present.
    #[test]
    fn test_descriptions_en_returns_value_when_present() {
        let descriptions = Descriptions({
            let mut m = HashMap::new();
            m.insert(
                "en".into(),
                LanguageValue {
                    value: "English Description".into(),
                },
            );
            m
        });
        assert_eq!(descriptions.en(), Some("English Description"));
    }
}
