//! Music entity filter for Wikidata entities.
//!
//! Provides `is_music_entity()` which checks a Wikidata entity's claims
//! against three criteria, in order of precision:
//!
//! 1. **Occupation (P106)** — does the entity have a music occupation?
//! 2. **Group type (P31)** — is the entity a music group?
//! 3. **Catch-all properties** — does the entity have music-related properties?
//!
//! The first match wins. Every included entity records why it passed.

use std::collections::HashMap;

use crate::wikidata::model::Claim;

// ---------------------------------------------------------------------------
// Q-ID sets
// ---------------------------------------------------------------------------

/// Q-IDs representing music-related occupations (P106).
pub(crate) const MUSIC_OCCUPATION_IDS: &[&str] = &[
    "Q639669",    // musician
    "Q36834",     // composer
    "Q177220",    // singer
    "Q488205",    // singer-songwriter
    "Q486748",    // pianist
    "Q548274",    // guitarist
    "Q158852",    // conductor
    "Q15981151",  // music artist
    "Q183945",    // record producer
    "Q753110",    // songwriter
    "Q1280273",   // instrumentalist
    "Q1086813",   // jazz musician
    "Q2252262",   // rapper
    "Q2865816",   // DJ
    "Q1075651",   // drummer
    "Q855091",    // organist
    "Q105543609", // electronic musician
    "Q793509",    // bassist
    "Q2551014",   // violinist
];

/// Q-IDs representing music groups (P31).
pub(crate) const MUSIC_GROUP_IDS: &[&str] = &[
    "Q215380",    // musical group
    "Q2088357",   // musical ensemble
    "Q5741069",   // rock band
    "Q42998",     // orchestra
    "Q1146754",   // boy band
    "Q6185547",   // girl group
    "Q2151147",   // supergroup
    "Q1229826",   // musical duo
    "Q114114601", // K-pop group
    "Q2539346",   // choir
    "Q108421069", // pop group
    "Q1196129",   // vocal group
];

/// Properties that indicate a music-relevant entity (catch-all heuristic).
pub(crate) const MUSIC_PROPERTIES: &[&str] = &["P1303", "P175", "P136", "P358"];

/// Minimum number of catch-all properties required for inclusion.
/// Set to 1 for a wide net (prioritises recall over precision).
const MIN_CATCHALL_PROPERTIES: usize = 1;

// ---------------------------------------------------------------------------
// Filter result type
// ---------------------------------------------------------------------------

/// The result of checking whether a Wikidata entity is music-related.
#[derive(Debug, Clone, PartialEq)]
pub enum FilterResult {
    /// The entity is music-related, with the reason string explaining why.
    Included(String),
    /// The entity is not music-related.
    Excluded,
}

impl FilterResult {
    /// Returns `true` if this result is `Included`.
    pub fn is_included(&self) -> bool {
        matches!(self, FilterResult::Included(_))
    }

    /// Returns the inclusion reason, if present.
    pub fn reason(&self) -> Option<&str> {
        match self {
            FilterResult::Included(reason) => Some(reason.as_str()),
            FilterResult::Excluded => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Determine whether a Wikidata entity is music-related based on its claims.
///
/// Checks three criteria in order of precision:
/// 1. Music occupation (P106)
/// 2. Music group type (P31)
/// 3. Catch-all music-related properties
///
/// The first match wins. Returns `FilterResult::Included(reason)` with a
/// structured reason string, or `FilterResult::Excluded`.
pub fn is_music_entity(claims: &HashMap<String, Vec<Claim>>) -> FilterResult {
    // Check 1: Music occupation (P106)
    if let Some(stmts) = claims.get("P106") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && MUSIC_OCCUPATION_IDS.contains(&target)
            {
                return FilterResult::Included(format!("P106:{target}"));
            }
        }
    }

    // Check 2: Music group type (P31)
    if let Some(stmts) = claims.get("P31") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && MUSIC_GROUP_IDS.contains(&target)
            {
                return FilterResult::Included(format!("P31:{target}"));
            }
        }
    }

    // Check 3: Catch-all properties
    let matched: Vec<&str> = MUSIC_PROPERTIES
        .iter()
        .filter(|prop| claims.contains_key(**prop))
        .copied()
        .collect();

    if matched.len() >= MIN_CATCHALL_PROPERTIES {
        // Sort for deterministic ordering (the plan says sorted order)
        let mut sorted = matched.clone();
        sorted.sort_unstable();
        return FilterResult::Included(format!("PROP:{}", sorted.join(",")));
    }

    FilterResult::Excluded
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

/// Extract the target Q-ID from a claim's mainsnak datavalue.
///
/// Drills through `claim.mainsnak.as_ref()?.datavalue.as_ref()?.id.as_deref()`.
fn claim_target_id(claim: &Claim) -> Option<&str> {
    claim
        .mainsnak
        .as_ref()
        .and_then(|m| m.datavalue.as_ref())
        .and_then(|dv| dv.id.as_deref())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wikidata::model::{Claim, DatavalueValue, Mainsnak};

    /// Helper: create a claim with the given target Q-ID.
    fn claim_with_id(id: &str) -> Claim {
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

    /// Helper: create a claim with no mainsnak (None).
    fn claim_no_mainsnak() -> Claim {
        Claim {
            mainsnak: None,
            extra: HashMap::new(),
        }
    }

    /// Helper: create a claim with a mainsnak that has no datavalue id.
    fn claim_no_datavalue_id() -> Claim {
        Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "novalue".into(),
                datavalue: None,
            }),
            extra: HashMap::new(),
        }
    }

    fn make_claims(map: Vec<(&str, Vec<Claim>)>) -> HashMap<String, Vec<Claim>> {
        map.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    // --- None: empty / non-music cases ---

    #[test]
    fn test_empty_claims_excluded() {
        let claims = HashMap::new();
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    #[test]
    fn test_no_claims_field() {
        let claims = HashMap::new();
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    // --- One: single matching claim ---

    #[test]
    fn test_single_occupation_included() {
        let claims = make_claims(vec![("P106", vec![claim_with_id("Q639669")])]);
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("P106:Q639669".into())
        );
    }

    #[test]
    fn test_single_group_included() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q215380")])]);
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("P31:Q215380".into())
        );
    }

    // --- Many: multiple claims ---

    #[test]
    fn test_multiple_music_claims() {
        let claims = make_claims(vec![
            (
                "P106",
                vec![claim_with_id("Q639669"), claim_with_id("Q177220")],
            ),
            ("P31", vec![claim_with_id("Q215380")]),
        ]);
        let result = is_music_entity(&claims);
        assert!(result.is_included());
        // Occupation check fires first
        assert_eq!(result.reason(), Some("P106:Q639669"));
    }

    #[test]
    fn test_multiple_non_music_claims() {
        // P31 but with non-music Q-IDs, no P106, no catch-all properties
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q5")]),       // human
            ("P106", vec![claim_with_id("Q123456")]), // unknown occupation
            ("P569", vec![claim_with_id("Q42")]),     // not a music property
        ]);
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    // --- Catch-all tests ---

    #[test]
    fn test_catchall_single_property_included() {
        let claims = make_claims(vec![("P136", vec![claim_with_id("Q35718")])]);
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("PROP:P136".into())
        );
    }

    #[test]
    fn test_catchall_no_match() {
        // No P106, no P31, and 0 out of 4 catch-all properties
        let claims = make_claims(vec![("P569", vec![claim_with_id("Q99")])]);
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    #[test]
    fn test_catchall_multiple_properties() {
        let claims = make_claims(vec![
            ("P1303", vec![claim_with_id("Q171")]),
            ("P136", vec![claim_with_id("Q35718")]),
            ("P358", vec![claim_with_id("Q1")]),
        ]);
        // Sorted order: P1303, P136, P358
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("PROP:P1303,P136,P358".into())
        );
    }

    // --- Edge cases ---

    #[test]
    fn test_claim_without_mainsnak() {
        let claims = make_claims(vec![
            ("P106", vec![claim_no_mainsnak()]),
            ("P31", vec![claim_with_id("Q5")]),
        ]);
        // P106 claim with no mainsnak is ignored; P31 doesn't match music group
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    #[test]
    fn test_claim_no_datavalue_id() {
        let claims = make_claims(vec![("P106", vec![claim_no_datavalue_id()])]);
        // P106 claim exists but datavalue has no id → ignored
        assert_eq!(is_music_entity(&claims), FilterResult::Excluded);
    }

    #[test]
    fn test_occupation_precedence_over_group() {
        // Entity has both a music occupation and a music group — occupation wins
        let claims = make_claims(vec![
            ("P106", vec![claim_with_id("Q177220")]), // singer
            ("P31", vec![claim_with_id("Q215380")]),  // musical group
        ]);
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("P106:Q177220".into())
        );
    }

    #[test]
    fn test_group_precedence_over_catchall() {
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q215380")]),
            ("P136", vec![claim_with_id("Q35718")]),
        ]);
        assert_eq!(
            is_music_entity(&claims),
            FilterResult::Included("P31:Q215380".into())
        );
    }

    #[test]
    fn test_all_catchall_properties() {
        let claims = make_claims(vec![
            ("P1303", vec![claim_with_id("Q171")]),
            ("P175", vec![claim_with_id("Q11649")]),
            ("P136", vec![claim_with_id("Q35718")]),
            ("P358", vec![claim_with_id("Q1")]),
        ]);
        let result = is_music_entity(&claims);
        assert!(result.is_included());
        let reason = result.reason().unwrap();
        assert!(reason.starts_with("PROP:"));
        // Should contain all 4 in sorted order
        assert!(reason.contains("P1303"));
        assert!(reason.contains("P136"));
        assert!(reason.contains("P175"));
        assert!(reason.contains("P358"));
    }

    #[test]
    fn test_filterresult_api() {
        assert!(FilterResult::Included("test".into()).is_included());
        assert!(!FilterResult::Excluded.is_included());
        assert_eq!(
            FilterResult::Included("reason".into()).reason(),
            Some("reason")
        );
        assert_eq!(FilterResult::Excluded.reason(), None);
    }
}
