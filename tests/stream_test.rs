//! Integration tests for the streaming parser using the mini dump fixture.
//!
//! The fixture `tests/fixtures/mini_dump.json.gz` is a hand-crafted gzipped
//! Wikidata dump containing:
//!
//! | # | Entity | Q-ID | Expected | Reason |
//! |---|--------|------|----------|--------|
//! | 1 | Ivy Queen | Q2831 | Included | P106:Q639669 (musician) |
//! | 2 | The Beatles | Q11649 | Included | P31:Q215380 (musical group) |
//! | 3 | Douglas Adams | Q42 | Excluded | P31:Q5 (human) |
//! | 4 | Adele | Q23215 | Included | P106:Q177220 (singer) |
//! | 5 | Fictional Genre Entity | Q99901 | Included | PROP:P136 (catch-all) |
//! | 6 | Entity With All Catch-All | Q99902 | Included | PROP:P1303,P136,P175,P358 |
//! | 7 | Paris | Q90 | Excluded | no music properties |
//! | 8 | (malformed) | — | Rejected | broken JSON |

use std::path::Path;

use wiki_db::wikidata::stream::{StreamEvent, StreamReader};

/// Path to the mini dump fixture, relative to the crate root.
const MINI_DUMP_PATH: &str = "tests/fixtures/mini_dump.json.gz";

#[test]
fn test_stream_mini_dump() {
    let path = Path::new(MINI_DUMP_PATH);
    assert!(
        path.exists(),
        "Fixture not found at {}. Did you generate it?",
        MINI_DUMP_PATH
    );

    let mut reader = StreamReader::new(path).expect("Failed to open mini dump fixture");
    let mut events: Vec<StreamEvent> = Vec::new();
    while let Ok(Some(event)) = reader.next_event() {
        events.push(event);
    }

    // Total lines: [ (1) + 7 entities + 1 malformed + ] (1) = 10 lines
    // Events emitted: Skipped([) + 7 entity events + 1 Rejected + Skipped(]) = 10 events
    assert_eq!(
        events.len(),
        10,
        "Expected 10 events (2 delimiters + 7 entities + 1 rejected), got {}",
        events.len()
    );

    // Event 0: Skipped (opening '[')
    assert!(
        matches!(events[0], StreamEvent::Skipped),
        "Expected Skipped for '[', got {:?}",
        events[0]
    );

    // Event 1: Filtered — Ivy Queen (Q2831)
    match &events[1] {
        StreamEvent::Filtered(fe) => {
            assert_eq!(fe.entity.id, "Q2831");
            assert_eq!(fe.inclusion_reason, "P106:Q639669");
        }
        other => panic!("Expected Filtered(Q2831), got {:?}", other),
    }

    // Event 2: Filtered — The Beatles (Q11649)
    match &events[2] {
        StreamEvent::Filtered(fe) => {
            assert_eq!(fe.entity.id, "Q11649");
            assert_eq!(fe.inclusion_reason, "P31:Q215380");
        }
        other => panic!("Expected Filtered(Q11649), got {:?}", other),
    }

    // Event 3: Skipped — Douglas Adams (Q42) excluded
    assert!(
        matches!(events[3], StreamEvent::Skipped),
        "Expected Skipped for Douglas Adams, got {:?}",
        events[3]
    );

    // Event 4: Filtered — Adele (Q23215)
    match &events[4] {
        StreamEvent::Filtered(fe) => {
            assert_eq!(fe.entity.id, "Q23215");
            assert_eq!(fe.inclusion_reason, "P106:Q177220");
        }
        other => panic!("Expected Filtered(Q23215), got {:?}", other),
    }

    // Event 5: Filtered — Fictional Genre Entity (Q99901)
    match &events[5] {
        StreamEvent::Filtered(fe) => {
            assert_eq!(fe.entity.id, "Q99901");
            assert_eq!(fe.inclusion_reason, "PROP:P136");
        }
        other => panic!("Expected Filtered(Q99901), got {:?}", other),
    }

    // Event 6: Filtered — Entity With All Catch-All (Q99902)
    match &events[6] {
        StreamEvent::Filtered(fe) => {
            assert_eq!(fe.entity.id, "Q99902");
            assert!(
                fe.inclusion_reason.starts_with("PROP:"),
                "Expected inclusion_reason to start with 'PROP:', got '{}'",
                fe.inclusion_reason
            );
            // All four catch-all properties in sorted order
            assert!(
                fe.inclusion_reason.contains("P1303"),
                "Reason missing P1303: {}",
                fe.inclusion_reason
            );
            assert!(
                fe.inclusion_reason.contains("P136"),
                "Reason missing P136: {}",
                fe.inclusion_reason
            );
            assert!(
                fe.inclusion_reason.contains("P175"),
                "Reason missing P175: {}",
                fe.inclusion_reason
            );
            assert!(
                fe.inclusion_reason.contains("P358"),
                "Reason missing P358: {}",
                fe.inclusion_reason
            );
        }
        other => panic!("Expected Filtered(Q99902), got {:?}", other),
    }

    // Event 7: Skipped — Paris (Q90) excluded
    assert!(
        matches!(events[7], StreamEvent::Skipped),
        "Expected Skipped for Paris, got {:?}",
        events[7]
    );

    // Event 8: Rejected — malformed line
    match &events[8] {
        StreamEvent::Rejected {
            line,
            reason: _,
            raw,
        } => {
            assert_eq!(*line, 9, "Expected rejected line 9, got {}", line);
            assert!(
                raw.as_deref().unwrap().contains("broken"),
                "Expected raw content to contain 'broken', got {:?}",
                raw
            );
        }
        other => panic!("Expected Rejected, got {:?}", other),
    }

    // Event 9: Skipped (closing ']')
    assert!(
        matches!(events[9], StreamEvent::Skipped),
        "Expected Skipped for ']', got {:?}",
        events[9]
    );

    // Verify counters
    assert_eq!(
        reader.processed(),
        10,
        "Expected 10 processed lines, got {}",
        reader.processed()
    );
    assert_eq!(
        reader.filtered(),
        5,
        "Expected 5 filtered entities, got {}",
        reader.filtered()
    );
    assert_eq!(
        reader.rejected(),
        1,
        "Expected 1 rejected line, got {}",
        reader.rejected()
    );
}

#[test]
fn test_stream_mini_dump_count_entities() {
    let path = Path::new(MINI_DUMP_PATH);
    let mut reader = StreamReader::new(path).expect("Failed to open mini dump fixture");
    let (processed, filtered, rejected) = reader.count_entities().expect("count_entities failed");

    assert_eq!(processed, 10, "Expected 10 processed");
    assert_eq!(filtered, 5, "Expected 5 filtered");
    assert_eq!(rejected, 1, "Expected 1 rejected");
}
