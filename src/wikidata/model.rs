//! Serde structs for the Wikidata JSON entity model.
//!
//! These types represent the top-level structure of a Wikidata entity as
//! returned by the Wikidata API and the JSON dump format. Only the fields
//! needed for music-relevant filtering are captured; qualifiers and
//! references are ignored entirely for v1.
//!
//! The `Mainsnak` type uses a custom `Deserialize` implementation to
//! handle the three `snaktype` variants (`value`, `novalue`, `somevalue`).

use std::collections::HashMap;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use std::fmt;

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
/// Uses a custom `Deserialize` implementation to handle the three
/// `snaktype` variants:
///
/// - `"value"` — the `datavalue` field is parsed and the nested `value`
///   object is extracted into `DatavalueValue`.
/// - `"novalue"` — no value exists; `datavalue` is `None`.
/// - `"somevalue"` — unknown value; `datavalue` is `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct Mainsnak {
    /// The snak type: `"value"`, `"novalue"`, or `"somevalue"`.
    pub snaktype: String,

    /// The extracted value. `None` for `novalue` and `somevalue`.
    pub datavalue: Option<DatavalueValue>,
}

/// The extracted value from a mainsnak's datavalue.
///
/// Only `id` (for Q-ID references) and `time` (for dates) are captured.
/// All other fields in the nested value object are silently ignored.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct DatavalueValue {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub time: Option<String>,
}

// --- Custom Serialize + Deserialize for Mainsnak ---

impl Serialize for Mainsnak {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("snaktype", &self.snaktype)?;

        if let Some(ref dv) = self.datavalue {
            // Build the datavalue object: { "value": { "id": ..., "time": ... } }
            let inner = serde_json::json!(dv);
            let datavalue_obj = serde_json::json!({"value": inner});
            map.serialize_entry("datavalue", &datavalue_obj)?;
        }

        map.end()
    }
}

impl<'de> Deserialize<'de> for Mainsnak {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(MainsnakVisitor)
    }
}

struct MainsnakVisitor;

impl<'de> Visitor<'de> for MainsnakVisitor {
    type Value = Mainsnak;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a Wikidata mainsnak object")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut snaktype: Option<String> = None;
        let mut datavalue_raw: Option<serde_json::Value> = None;

        while let Some(key) = map.next_key::<String>()? {
            match key.as_str() {
                "snaktype" => {
                    snaktype = Some(map.next_value()?);
                }
                "datavalue" => {
                    datavalue_raw = map.next_value::<Option<serde_json::Value>>()?;
                }
                // Silently ignore all other fields (id, rank, qualifiers, references, hash, etc.)
                _ => {
                    map.next_value::<serde_json::Value>()?;
                }
            }
        }

        let snaktype = snaktype.unwrap_or_default();

        let datavalue = if snaktype == "value" {
            match datavalue_raw {
                Some(val) => {
                    // Extract the nested "value" object and try to parse it as DatavalueValue
                    let inner = val.get("value");
                    match inner {
                        Some(v) => serde_json::from_value(v.clone()).ok(),
                        None => None,
                    }
                }
                None => None,
            }
        } else {
            // "novalue" or "somevalue" — datavalue is always None
            None
        };

        Ok(Mainsnak {
            snaktype,
            datavalue,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Custom deserializer tests (Step 3) ---

    /// Value snaktype with entity ID.
    #[test]
    fn test_value_snaktype_with_entity_id() {
        let json = r#"{
            "snaktype": "value",
            "datavalue": {
                "value": { "id": "Q639669" }
            }
        }"#;
        let ms: Mainsnak = serde_json::from_str(json).expect("deserialize");
        assert_eq!(ms.snaktype, "value");
        assert_eq!(
            ms.datavalue,
            Some(DatavalueValue {
                id: Some("Q639669".into()),
                time: None
            })
        );
    }

    /// Value snaktype with time.
    #[test]
    fn test_value_snaktype_with_time() {
        let json = r#"{
            "snaktype": "value",
            "datavalue": {
                "value": { "time": "+1926-09-23T00:00:00Z" }
            }
        }"#;
        let ms: Mainsnak = serde_json::from_str(json).expect("deserialize");
        assert_eq!(ms.snaktype, "value");
        assert_eq!(
            ms.datavalue,
            Some(DatavalueValue {
                id: None,
                time: Some("+1926-09-23T00:00:00Z".into())
            })
        );
    }

    /// Novalue snaktype.
    #[test]
    fn test_novalue_snaktype() {
        let json = r#"{
            "snaktype": "novalue"
        }"#;
        let ms: Mainsnak = serde_json::from_str(json).expect("deserialize");
        assert_eq!(ms.snaktype, "novalue");
        assert_eq!(ms.datavalue, None);
    }

    /// Somevalue snaktype.
    #[test]
    fn test_somevalue_snaktype() {
        let json = r#"{
            "snaktype": "somevalue"
        }"#;
        let ms: Mainsnak = serde_json::from_str(json).expect("deserialize");
        assert_eq!(ms.snaktype, "somevalue");
        assert_eq!(ms.datavalue, None);
    }

    /// Full claim with ignored fields (id, rank, qualifiers, references).
    #[test]
    fn test_full_claim_with_ignored_fields() {
        let json = r#"{
            "id": "Q2831$B1C2D3E4-F5A6-7890-ABCD-EF1234567890",
            "mainsnak": {
                "snaktype": "value",
                "datavalue": {
                    "value": { "id": "Q639669" }
                }
            },
            "rank": "normal",
            "qualifiers": {
                "P580": []
            },
            "references": [
                { "P143": [] }
            ],
            "hash": "abc123def456"
        }"#;
        let claim: Claim = serde_json::from_str(json).expect("deserialize");
        assert_eq!(claim.mainsnak.snaktype, "value");
        assert_eq!(
            claim.mainsnak.datavalue,
            Some(DatavalueValue {
                id: Some("Q639669".into()),
                time: None
            })
        );
        // Extra fields should be captured in the catch-all
        assert!(claim.extra.contains_key("id"));
        assert!(claim.extra.contains_key("rank"));
        assert!(claim.extra.contains_key("qualifiers"));
        assert!(claim.extra.contains_key("references"));
        assert!(claim.extra.contains_key("hash"));
    }

    /// Entity with P106 claim.
    #[test]
    fn test_entity_with_p106_claim() {
        let json = r#"{
            "id": "Q2831",
            "type": "item",
            "labels": { "en": { "value": "Ivy Queen" } },
            "descriptions": { "en": { "value": "American singer-songwriter and musician" } },
            "claims": {
                "P106": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "datavalue": {
                                "value": { "id": "Q639669" }
                            }
                        }
                    }
                ]
            }
        }"#;
        let entity: Entity = serde_json::from_str(json).expect("deserialize");
        let p106 = entity.claims.get("P106").expect("P106 claim");
        assert_eq!(p106.len(), 1);
        assert_eq!(
            p106[0].mainsnak.datavalue,
            Some(DatavalueValue {
                id: Some("Q639669".into()),
                time: None
            })
        );
    }

    // --- Model tests (Step 2) ---

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
                            datavalue: Some(DatavalueValue {
                                id: Some("Q639669".into()),
                                time: None,
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
