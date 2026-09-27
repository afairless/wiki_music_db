//! Music entity filter and role classifier for Wikidata entities.
//!
//! [`classify_entity`] assigns every matching entity exactly one of three
//! roles — [`EntityRole::Agent`] (people/groups), [`EntityRole::Album`]
//! (album-class works) or [`EntityRole::Track`] (song-class works) — by
//! checking claims in order of precision:
//!
//! 1. **Occupation (P106)** — music occupation → Agent.
//! 2. **Group type (P31)** — music group class → Agent.
//! 3. **Album work class (P31)** — album-class P31 → Album.
//! 4. **Track work class (P31)** — song-class P31 → Track.
//! 5. **Catch-all properties** — P1303/P175/P136/P358, only reached when no
//!    work-class P31 is present → Agent.
//!
//! The first match wins. Every included entity records why it passed
//! ([`classify_entity`] returns the role alongside the reason).
//! [`is_music_entity`] is a thin wrapper that discards the role, kept for
//! callers and tests that only need the yes/no verdict.

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

/// P31 classes that identify *album-like works* (as opposed to agents).
///
/// A work in one of these classes carries `P175` (featured performers) and
/// belongs in the `album` table, never in `artist`. Singles and EPs are
/// classified as albums per the v1 schema intent ("Albums, EPs, singles,
/// and compilation albums").
///
/// Curation (2026-09): every Q-ID below was verified by its English label
/// against the local `qid_label` table and/or the Wikidata API; class labels
/// are role-independent, so the pre-fix dump is a valid label source. The
/// list is intentionally minimal: uncovered classes fall through to the
/// catch-all (agent) path and are auditable via the `inclusion_reason`
/// distribution.
pub(crate) const ALBUM_WORK_CLASS_IDS: &[&str] = &[
    "Q482994",  // album
    "Q134556",  // single
    "Q208569",  // studio album
    "Q169930",  // extended play
    "Q222910",  // compilation album
    "Q209939",  // live album
    "Q5610543", // demo
    "Q963099",  // remix album
    "Q723849",  // greatest hits album
    "Q1892995", // mixtape
    "Q217199",  // soundtrack
    "Q5049564", // cast recording
];

/// P31 classes that identify *track-like works* (songs / instrumental pieces).
///
/// A work in one of these classes belongs in the `track` table; `P361` on the
/// entity names its parent albums. Curation method is the same as
/// [`ALBUM_WORK_CLASS_IDS`].
pub(crate) const TRACK_WORK_CLASS_IDS: &[&str] = &[
    "Q7366",     // song
    "Q24887304", // instrumental composition
    "Q639197",   // instrumental music
];

/// Properties that indicate a music-relevant entity (catch-all heuristic).
pub(crate) const MUSIC_PROPERTIES: &[&str] = &["P1303", "P175", "P136", "P358"];

/// Minimum number of catch-all properties required for inclusion.
/// Set to 1 for a wide net (prioritises recall over precision).
const MIN_CATCHALL_PROPERTIES: usize = 1;

// ---------------------------------------------------------------------------
// Entity roles
// ---------------------------------------------------------------------------

/// The role a music entity plays in the database.
///
/// Bootstrap and update routing use this to decide which tables an entity
/// contributes rows to: agents → `artist` (and artist join tables), album-
/// class works → `album` / `album_artist`, track-class works → `track` /
/// `track_artist` / `track_album`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntityRole {
    /// A person or group: the subject of the `artist` table.
    Agent,
    /// An album-class work (album, single, EP, compilation, …).
    Album,
    /// A track-class work (song, instrumental piece, …).
    Track,
}

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

/// Classify a Wikidata entity into its music role, if it is music-related.
///
/// Checks five criteria in order of precision:
/// 1. Music occupation (P106)      → `Agent`
/// 2. Music group type (P31)       → `Agent`
/// 3. Album work class (P31)       → `Album`
/// 4. Track work class (P31)       → `Track`
/// 5. Catch-all music properties   → `Agent`
///
/// The first match wins. Rules 3–4 run before the catch-all so album/song
/// works — which carry `P175` (performers) and `P136` (genre) — are never
/// admitted as agents, removing the source of the historical role inversion.
///
/// Returns `Some((role, reason))` with a structured reason string, or `None`
/// if the entity is not music-related.
pub fn classify_entity(claims: &HashMap<String, Vec<Claim>>) -> Option<(EntityRole, String)> {
    // Rule 1: Music occupation (P106)
    if let Some(stmts) = claims.get("P106") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && MUSIC_OCCUPATION_IDS.contains(&target)
            {
                return Some((EntityRole::Agent, format!("P106:{target}")));
            }
        }
    }

    // Rule 2: Music group type (P31)
    if let Some(stmts) = claims.get("P31") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && MUSIC_GROUP_IDS.contains(&target)
            {
                return Some((EntityRole::Agent, format!("P31:{target}")));
            }
        }
    }

    // Rule 3: Album work class (P31)
    if let Some(stmts) = claims.get("P31") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && ALBUM_WORK_CLASS_IDS.contains(&target)
            {
                return Some((EntityRole::Album, format!("P31:{target}")));
            }
        }
    }

    // Rule 4: Track work class (P31)
    if let Some(stmts) = claims.get("P31") {
        for claim in stmts {
            if let Some(target) = claim_target_id(claim)
                && TRACK_WORK_CLASS_IDS.contains(&target)
            {
                return Some((EntityRole::Track, format!("P31:{target}")));
            }
        }
    }

    // Rule 5: Catch-all properties. Only reached when no work-class P31 is
    // present (rules 3–4 would have matched it), so the entity is an agent.
    let matched: Vec<&str> = MUSIC_PROPERTIES
        .iter()
        .filter(|prop| claims.contains_key(**prop))
        .copied()
        .collect();

    if matched.len() >= MIN_CATCHALL_PROPERTIES {
        // Sort for deterministic ordering
        let mut sorted = matched.clone();
        sorted.sort_unstable();
        return Some((EntityRole::Agent, format!("PROP:{}", sorted.join(","))));
    }

    None
}

/// Determine whether a Wikidata entity is music-related based on its claims.
///
/// Thin wrapper over [`classify_entity`] that discards the role and keeps the
/// historical `FilterResult` verdict, for callers and tests that only need the
/// yes/no answer (e.g. the incremental-update path).
pub fn is_music_entity(claims: &HashMap<String, Vec<Claim>>) -> FilterResult {
    match classify_entity(claims) {
        Some((_, reason)) => FilterResult::Included(reason),
        None => FilterResult::Excluded,
    }
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
                    amount: None,
                    unit: None,
                    precision: None,
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

    // --- Role classification (classify_entity / EntityRole) ---

    #[test]
    fn test_classify_empty_claims_excluded() {
        let claims = HashMap::new();
        assert_eq!(classify_entity(&claims), None);
    }

    #[test]
    fn test_classify_musician_agent() {
        let claims = make_claims(vec![("P106", vec![claim_with_id("Q639669")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "P106:Q639669".into()))
        );
    }

    #[test]
    fn test_classify_group_agent() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q215380")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "P31:Q215380".into()))
        );
    }

    #[test]
    fn test_classify_album_not_artist() {
        // An album work carries P175 (its performers) — it must be Album, not Agent.
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q482994")]), // album
            ("P175", vec![claim_with_id("Q396")]),   // performer: U2
        ]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q482994".into()))
        );
    }

    #[test]
    fn test_classify_studio_album() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q208569")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q208569".into()))
        );
    }

    #[test]
    fn test_classify_single_album() {
        // Singles/EPs live in `album` per the v1 schema intent (plan question 5).
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q134556")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q134556".into()))
        );
    }

    #[test]
    fn test_classify_ep_album() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q169930")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q169930".into()))
        );
    }

    #[test]
    fn test_classify_compilation_album() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q222910")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q222910".into()))
        );
    }

    #[test]
    fn test_classify_live_album() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q209939")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q209939".into()))
        );
    }

    #[test]
    fn test_classify_song_track() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q7366")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Track, "P31:Q7366".into()))
        );
    }

    #[test]
    fn test_classify_instrumental_track() {
        let claims = make_claims(vec![("P31", vec![claim_with_id("Q24887304")])]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Track, "P31:Q24887304".into()))
        );
    }

    #[test]
    fn test_classify_work_class_beats_catchall() {
        // Album with a genre (P136): the work class wins over the catch-all,
        // so the entity is not misclassified as an agent.
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q482994")]),
            ("P136", vec![claim_with_id("Q35718")]), // rock music
        ]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q482994".into()))
        );
    }

    #[test]
    fn test_classify_occupation_precedence_over_work_class() {
        // Documented precedence: a music occupation (P106) beats any work-class
        // P31 — such an entity is an agent.
        let claims = make_claims(vec![
            ("P106", vec![claim_with_id("Q639669")]),
            ("P31", vec![claim_with_id("Q482994")]),
        ]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "P106:Q639669".into()))
        );
    }

    #[test]
    fn test_classify_group_precedence_over_work_class() {
        // Documented precedence: a music group P31 beats album/song classes
        // when the same entity carries both (defensive ordering).
        let claims = make_claims(vec![(
            "P31",
            vec![claim_with_id("Q215380"), claim_with_id("Q482994")],
        )]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "P31:Q215380".into()))
        );
    }

    #[test]
    fn test_classify_album_class_beats_song_class() {
        let claims = make_claims(vec![(
            "P31",
            vec![claim_with_id("Q7366"), claim_with_id("Q482994")],
        )]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q482994".into()))
        );
    }

    #[test]
    fn test_classify_catchall_agent() {
        // No occupation/group/work class — the catch-all admits an agent.
        let claims = make_claims(vec![
            ("P175", vec![claim_with_id("Q396")]),
            ("P136", vec![claim_with_id("Q35718")]),
        ]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "PROP:P136,P175".into()))
        );
    }

    #[test]
    fn test_classify_human_with_genre_is_agent() {
        // A person without a music occupation but with a genre stays an agent
        // (non-work P31 like Q5 does not beat the catch-all).
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q5")]),
            ("P136", vec![claim_with_id("Q35718")]),
        ]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Agent, "PROP:P136".into()))
        );
    }

    #[test]
    fn test_classify_non_music_excluded() {
        let claims = make_claims(vec![
            ("P31", vec![claim_with_id("Q5")]),
            ("P569", vec![claim_with_id("Q42")]),
        ]);
        assert_eq!(classify_entity(&claims), None);
    }

    #[test]
    fn test_classify_p31_claim_without_datavalue_id_skipped() {
        // A P31 claim with no target is ignored; a later album class still wins.
        let claims = make_claims(vec![(
            "P31",
            vec![claim_no_datavalue_id(), claim_with_id("Q482994")],
        )]);
        assert_eq!(
            classify_entity(&claims),
            Some((EntityRole::Album, "P31:Q482994".into()))
        );
    }

    #[test]
    fn test_is_music_entity_wraps_classify() {
        // The wrapper keeps the historical verdict shape for every role.
        let album = make_claims(vec![("P31", vec![claim_with_id("Q482994")])]);
        assert_eq!(
            is_music_entity(&album),
            FilterResult::Included("P31:Q482994".into())
        );
        let non_music = make_claims(vec![("P31", vec![claim_with_id("Q5")])]);
        assert_eq!(is_music_entity(&non_music), FilterResult::Excluded);
    }

    #[test]
    fn test_entity_role_derives() {
        // EntityRole must be Copy + Eq for cheap role routing.
        let roles = [EntityRole::Agent, EntityRole::Album, EntityRole::Track];
        assert_eq!(roles[0], EntityRole::Agent);
        assert_ne!(roles[1], EntityRole::Agent);
        assert_eq!(format!("{:?}", EntityRole::Album), "Album");
    }

    // -------------------------------------------------------------------
    // Fixture-driven role classification (tests/fixtures/*)
    // -------------------------------------------------------------------

    /// Helper: classify the claims of a deserialized fixture entity.
    fn classify_fixture(json: &str) -> Option<(EntityRole, String)> {
        let entity: crate::wikidata::model::Entity =
            serde_json::from_str(json).expect("deserialize fixture");
        classify_entity(&entity.claims)
    }

    #[test]
    fn test_classify_album_work_fixture() {
        // The Joshua Tree (Q152873): album class beats the catch-all, so the
        // work is Album even though it carries P175 (its performers).
        let json = include_str!("../../tests/fixtures/album_work_entity.json");
        assert_eq!(
            classify_fixture(json),
            Some((EntityRole::Album, "P31:Q482994".into()))
        );
    }

    #[test]
    fn test_classify_song_work_fixture() {
        // With or Without You (Q155849): the song class routes to Track.
        let json = include_str!("../../tests/fixtures/song_work_entity.json");
        assert_eq!(
            classify_fixture(json),
            Some((EntityRole::Track, "P31:Q7366".into()))
        );
    }

    #[test]
    fn test_classify_person_agent_sitelinks_fixture() {
        // Ivy Queen (Q2831): P106 occupation wins even without an en label.
        let json = include_str!("../../tests/fixtures/person_agent_sitelinks.json");
        assert_eq!(
            classify_fixture(json),
            Some((EntityRole::Agent, "P106:Q639669".into()))
        );
    }
}
