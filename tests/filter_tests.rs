//! Property-based tests for the music entity filter.
//!
//! Uses `proptest` to verify that `is_music_entity()` satisfies key invariants
//! across a wide range of randomly generated claim inputs.

use std::collections::HashMap;

use proptest::collection::{hash_map, vec};
use proptest::prelude::*;
use proptest::sample::select;
use proptest::strategy::Just;

use wiki_db::wikidata::filter::{FilterResult, is_music_entity};
use wiki_db::wikidata::model::{Claim, DatavalueValue, Mainsnak};

// ---------------------------------------------------------------------------
// Strategy: generate a single Claim with random mainsnak configuration
// ---------------------------------------------------------------------------

/// Shorthand for a constant strategy.
fn just<T: std::fmt::Debug + Clone + 'static>(v: T) -> impl Strategy<Value = T> {
    Just(v)
}

/// Generate a random `Mainsnak`.
///
/// About 60% of the time it's a "value" snak (the common case), 20% it's
/// "novalue"/"somevalue" (no datavalue), and 20% it's None (missing mainsnak).
fn arb_mainsnak() -> impl Strategy<Value = Option<Mainsnak>> {
    prop_oneof![
        // 60% — snaktype "value" with a datavalue
        3 => arb_value_mainsnak(),
        // 20% — snaktype "novalue" or "somevalue" with datavalue=None
        1 => proptest::bool::ANY.prop_map(|novalue| {
            Some(Mainsnak {
                snaktype: if novalue { "novalue".into() } else { "somevalue".into() },
                datavalue: None,
            })
        }),
        // 20% — no mainsnak at all
        1 => just(None),
    ]
}

/// Generate a `Mainsnak` with snaktype="value" and a random DatavalueValue.
fn arb_value_mainsnak() -> impl Strategy<Value = Option<Mainsnak>> {
    arb_datavalue_value().prop_map(|dv| {
        Some(Mainsnak {
            snaktype: "value".into(),
            datavalue: Some(dv),
        })
    })
}

/// Generate a random `DatavalueValue`.
///
/// About 70% have an `id` set (Q-ID), 30% have `id: None`.
fn arb_datavalue_value() -> impl Strategy<Value = DatavalueValue> {
    prop_oneof![
        // 70% — has an id
        7 => "[Qq][0-9]{1,8}".prop_map(|id| DatavalueValue {
            id: Some(id),
            time: None,
        }),
        // 30% — no id
        3 => Just(DatavalueValue {
            id: None,
            time: None,
        }),
    ]
}

/// Generate a random `Claim` with a random mainsnak.
fn arb_claim() -> impl Strategy<Value = Claim> {
    arb_mainsnak().prop_map(|mainsnak| Claim {
        mainsnak,
        extra: HashMap::new(),
    })
}

// ---------------------------------------------------------------------------
// Strategy: generate claim maps
// ---------------------------------------------------------------------------

/// The set of property IDs that can appear in generated claims.
/// Includes music-related properties (P106, P31, P1303, P175, P136, P358)
/// plus non-music properties (P569, P1234) to test the catch-all threshold.
const PROPERTY_IDS: &[&str] = &[
    "P106",    // occupation
    "P31",     // instance of
    "P1303",   // instrument
    "P175",    // performer
    "P136",    // genre
    "P358",    // discography
    "P569",    // date of birth
    "P1234",   // some unknown property
];

/// Strategy: generate a random `HashMap<String, Vec<Claim>>`.
fn arb_claims() -> impl Strategy<Value = HashMap<String, Vec<Claim>>> {
    // Draw 0-8 property IDs, each with 0-5 claims
    hash_map(
        select(PROPERTY_IDS.to_vec()).prop_map(String::from),
        vec(arb_claim(), 0..=5),
        0..=8,
    )
}

// ---------------------------------------------------------------------------
// Property tests
// ---------------------------------------------------------------------------

proptest! {
    /// Property: `is_music_entity()` never panics for any valid input.
    #[test]
    fn filter_never_panics(claims in arb_claims()) {
        let result = is_music_entity(&claims);
        // Must always return a FilterResult (no panics)
        match result {
            FilterResult::Included(_) | FilterResult::Excluded => { /* ok */ }
        }
    }
}

proptest! {
    /// Property: If P106 has a claim with a music occupation Q-ID, the result
    /// is `Included` and the reason starts with `"P106:"`.
    ///
    /// The occupation check fires first, so even if P31 also matches,
    /// the reason should reference P106.
    #[test]
    fn filter_occupation_takes_precedence(
        occupation_qid in select(vec!["Q639669", "Q36834", "Q177220", "Q488205", "Q486748"]),
        group_qid in select(vec!["Q215380", "Q2088357", "Q5741069"]),
        extra_claims in arb_claims(),
    ) {
        let mut claims = extra_claims;
        // Insert a P106 claim with a known music occupation
        let occ_claim = Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some(occupation_qid.to_string()),
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        };
        claims.entry("P106".into()).or_default().push(occ_claim);

        // Also insert a P31 claim with a music group (to verify precedence)
        let group_claim = Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some(group_qid.to_string()),
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        };
        claims.entry("P31".into()).or_default().push(group_claim);

        let result = is_music_entity(&claims);
        match result {
            FilterResult::Included(reason) => {
                assert!(
                    reason.starts_with("P106:"),
                    "Expected reason to start with 'P106:', got '{reason}'"
                );
            }
            FilterResult::Excluded => {
                panic!(
                    "Expected Included for claims with music occupation P106={} and music group P31={}",
                    occupation_qid, group_qid,
                );
            }
        }
    }
}

proptest! {
    /// Property: When the claims contain `N` catch-all properties where
    /// `N >= 1` (the threshold), and there are no matching P106/P31 claims,
    /// the result is `Included` with a reason starting with `"PROP:"`.
    #[test]
    fn filter_catchall_includes_when_threshold_met(
        // 1..=4 catch-all properties
        catchall_keys in select(vec![
            vec!["P1303"],
            vec!["P175"],
            vec!["P136"],
            vec!["P358"],
            vec!["P1303", "P175"],
            vec!["P1303", "P136", "P358"],
            vec!["P175", "P136", "P358", "P1303"],
        ]),
    ) {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        // Add the catch-all properties — each with a dummy claim
        for key in catchall_keys {
            claims.entry(key.to_string()).or_default().push(Claim {
                mainsnak: Some(Mainsnak {
                    snaktype: "value".into(),
                    datavalue: Some(DatavalueValue {
                        id: Some("Q1".into()),
                        time: None,
                    }),
                }),
                extra: HashMap::new(),
            });
        }

        // Add some non-music claims to ensure P106/P31 don't match
        claims.entry("P106".into()).or_default().push(Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some("Q5".into()), // "human" — not a music occupation
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        });
        claims.entry("P31".into()).or_default().push(Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some("Q5".into()), // "human" — not a music group
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        });

        let result = is_music_entity(&claims);
        match result {
            FilterResult::Included(reason) => {
                assert!(
                    reason.starts_with("PROP:"),
                    "Expected catch-all reason to start with 'PROP:', got '{reason}'"
                );
            }
            FilterResult::Excluded => {
                panic!("Expected Included for claims with 1+ catch-all properties and no P106/P31 match");
            }
        }
    }
}

proptest! {
    /// Property: When there are no P106/P31 matches AND no catch-all
    /// properties, the result is `Excluded`.
    #[test]
    fn filter_catchall_excludes_below_threshold(
        keys in select(vec![
            vec!["P569"],
            vec!["P1234"],
            vec!["P569", "P1234"],
            vec![],
        ]),
    ) {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        for key in keys {
            claims.entry(key.to_string()).or_default().push(Claim {
                mainsnak: Some(Mainsnak {
                    snaktype: "value".into(),
                    datavalue: Some(DatavalueValue {
                        id: Some("Q1".into()),
                        time: None,
                    }),
                }),
                extra: HashMap::new(),
            });
        }

        // No P106 or P31 at all (or only with non-music Q-IDs)
        // Already the case from the keys above.

        let result = is_music_entity(&claims);
        assert_eq!(result, FilterResult::Excluded);
    }
}

proptest! {
    /// Property: When P31 matches a music group (without a matching P106),
    /// the result is `Included` with a reason starting with `"P31:"`.
    #[test]
    fn filter_group_included_when_no_occupation(
        group_qid in select(vec!["Q215380", "Q2088357", "Q5741069", "Q42998", "Q215380"]),
    ) {
        let mut claims: HashMap<String, Vec<Claim>> = HashMap::new();
        claims.entry("P31".into()).or_default().push(Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some(group_qid.to_string()),
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        });

        // Add some non-music P106 claims
        claims.entry("P106".into()).or_default().push(Claim {
            mainsnak: Some(Mainsnak {
                snaktype: "value".into(),
                datavalue: Some(DatavalueValue {
                    id: Some("Q5".into()),
                    time: None,
                }),
            }),
            extra: HashMap::new(),
        });

        let result = is_music_entity(&claims);
        match result {
            FilterResult::Included(reason) => {
                assert!(
                    reason.starts_with("P31:"),
                    "Expected reason to start with 'P31:', got '{reason}'"
                );
            }
            FilterResult::Excluded => {
                panic!("Expected Included for P31={}", group_qid);
            }
        }
    }
}