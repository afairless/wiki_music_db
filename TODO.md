# Implementation Plan: Phase 2b — Filter & Streaming Parser

Source: `docs/research/2026-07_music_db_rust_plan.md` (Phase 2b only)

This plan covers **Phase 2b only**. Phases 1, 2a, 3a, 3b, 5, 6, 7, and 8 are **out of scope**. Do not implement any code belonging to those phases.

## Summary

Stream `latest-all.json.gz` line-by-line, apply the music entity filter with inclusion-reason tracking, and expose a streaming parser that yields filtered entities. The filter checks three criteria in order: music occupation (P106), music group type (P31), and a catch-all heuristic over music-related properties (P1303, P175, P136, P358). Every included entity records why it passed the filter so `artist.inclusion_reason` can be populated during loading.

**Deliverable:** `cargo test` passes; can stream a test fixture file and print filtered entity counts with inclusion reasons.

## Steps

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat: implement music entity filter with inclusion reason tracking` | Music entity filter | `src/wikidata/filter.rs`, `src/wikidata/mod.rs` (updated) | Unit |
| 2 | `feat: implement streaming gzip parser for Wikidata dump` | Streaming parser | `src/wikidata/stream.rs`, `src/wikidata/mod.rs` (updated) | Unit |
| 3 | `test: add property-based and integration tests for filter and stream` | Extended tests | `tests/property/filter_tests.rs`, `tests/fixtures/mini_dump.json.gz` | Property-based, Integration |

### Step 1 — Implement music entity filter

Create `src/wikidata/filter.rs` containing the `is_music_entity()` predicate and inclusion-reason tracking. Update `src/wikidata/mod.rs` to export `pub mod filter;`.

**Key design decisions:**

- The filter returns a `FilterResult` enum — either `Included(String)` with the reason string, or `Excluded`. This captures why an entity was included so the reason can be stored in the `artist.inclusion_reason` column during Phase 3b loading.
- Reason strings follow the conventions from the plan: `"P106:Q639669"` (matched a specific occupation), `"P31:Q215380"` (matched a specific group type), `"PROP:P1303,P136"` (matched catch-all properties).
- The three checks are ordered by precision: occupation → group type → catch-all. The first match wins.
- A private helper `fn claim_target_id(claim: &Claim) -> Option<&str>` extracts the target Q-ID from a claim's mainsnak datavalue, drilling through `claim.mainsnak.as_ref()?.datavalue.as_ref()?.id.as_deref()`.

**Inline unit tests (directly in `filter.rs` under `#[cfg(test)]`):**

- `test_empty_claims_excluded`: empty claims map → `FilterResult::Excluded`
- `test_single_occupation_included`: single `P106` claim with `Q639669` → `FilterResult::Included("P106:Q639669")`
- `test_single_group_included`: single `P31` claim with `Q215380` → `FilterResult::Included("P31:Q215380")`
- `test_multiple_music_claims`: multiple P106 and P31 claims with music Q-IDs → `Included`
- `test_multiple_non_music_claims`: claims present but no music Q-IDs, no catch-all properties → `Excluded`
- `test_catchall_single_property_included`: exactly 1 catch-all property (e.g. `P136`) → `Included` (threshold is 1)
- `test_catchall_no_match`: no P106, no P31, and 0 out of 4 catch-all properties → `Excluded`
- `test_catchall_multiple_properties`: 3 catch-all properties present → `Included("PROP:P1303,P136,P358")` (sorted order)
- `test_no_claims_field`: claims HashMap is empty → `Excluded`
- `test_claim_without_mainsnak`: claim exists but mainsnak is `None` → ignored for occupation check, falls through to other checks

### Step 2 — Implement streaming gzip parser

Create `src/wikidata/stream.rs` containing the streaming parser. Update `src/wikidata/mod.rs` to export `pub mod stream;`.

**Key design decisions:**

- Define a `StreamReader` struct that owns a `BufReader<GzDecoder<File>>`, an internal line buffer (`String`), and a `u64` line counter.
- A `StreamEvent` enum models each parsed line:

  ```rust
  pub enum StreamEvent {
      Filtered(FilteredEntity),
      Rejected { line: u64, reason: String, raw: Option<String> },
      Skipped, // JSON delimiters [ , ]
  }
  ```

- A `FilteredEntity` struct pairs the deserialized `Entity` with its inclusion reason:

  ```rust
  pub struct FilteredEntity {
      pub entity: Entity,
      pub inclusion_reason: String,
  }
  ```

- `StreamReader::new(path: &Path) -> Result<Self>` opens the gzip file:
  - `File::open(path)` → `GzDecoder::new(file)` → `BufReader::new(decoder)`
- `StreamReader::next_event(&mut self) -> Result<Option<StreamEvent>>` reads one line at a time:
  1. Read a line into the internal buffer
  2. Check for EOF → return `Ok(None)`
  3. Increment line counter
  4. Trim whitespace; skip `[` and `]` delimiter lines → return `Ok(Some(StreamEvent::Skipped))`
  5. Strip trailing comma: `line.trim_end_matches(',')`
  6. Try `serde_json::from_str::<Entity>(&line)`:
     - On success: call `is_music_entity(&entity.claims)`. If `Included(reason)`, yield `StreamEvent::Filtered(FilteredEntity { entity, inclusion_reason: reason })`. If `Excluded`, skip silently (caller may want counters — tracked via a separate counter).
     - On error: yield `StreamEvent::Rejected { line, reason: error.to_string(), raw: Some(line.clone()) }`
- A helper `StreamReader::count_entities(&mut self) -> Result<(u64, u64, u64)>` provides a convenience method that drains the stream and returns `(processed, filtered, rejected)` counts.
- Use `tracing::warn!` for rejected lines (with the reason and line number) so callers can optionally write to `rejected.jsonl`.

**Inline unit tests (in `stream.rs` under `#[cfg(test)]`):**

- Use a temporary gzip file created in the test via `flate2::write::GzEncoder`:
  - `test_stream_single_musician`: Write a 2-line JSON array (1 musician, 1 non-musician), stream it → verify 1 filtered result, correct Q-ID and inclusion reason
  - `test_stream_empty_array`: Write `[ ]` → zero events
  - `test_stream_all_excluded`: Write entities with no music claims → zero filtered events
  - `test_stream_malformed_line`: Write one valid entity + one truncated JSON line → verify the valid entity is yielded and a `Rejected` event is emitted for the malformed line
  - `test_stream_strips_trailing_comma`: Write `[ { ... },` (entity with trailing comma between array elements) → deserialization succeeds

### Step 3 — Add property-based and integration tests

#### Property-based tests (`tests/property/filter_tests.rs`)

- The test module **must not** require `proptest` or `quickcheck` as public dependencies. Add `proptest = "1"` to `[dev-dependencies]` in `Cargo.toml`.
- Strategy: generate a random `HashMap<String, Vec<Claim>>` where:
  - Keys are drawn from a small set of property IDs (`"P106"`, `"P31"`, `"P1303"`, `"P175"`, `"P136"`, `"P358"`, `"P569"`, `"P1234"`)
  - Values are random-length `Vec<Claim>` with random `mainsnak` configurations (some with `Some(Q-ID)` matching music Q-IDs, some with non-music Q-IDs, some with `None`)
  - Use a custom `Arbitrary` impl or compose strategies directly with `prop::collection::hash_map` and `prop::collection::vec`
- **Properties to test:**
  - `filter_never_panics`: For any generated input, `is_music_entity(&claims)` never panics (always returns a `FilterResult`)
  - `filter_consistent_with_precision_order`: If P106 matches a music occupation and P31 matches a music group, the reason starts with `"P106:"` (occupation check fires first)
  - `filter_catchall_includes_when_threshold_met`: When the claims contain `N` catch-all properties where `N >= MIN_CATCHALL_PROPERTIES` (and no P106/P31 match), result is `Included` with reason starting with `"PROP:"`
  - `filter_catchall_excludes_below_threshold`: When the claims contain `N` catch-all properties where `N < MIN_CATCHALL_PROPERTIES` (and no P106/P31 match), result is `Excluded`

#### Integration test — mini dump fixture (`tests/fixtures/mini_dump.json.gz`)

Create `tests/fixtures/mini_dump.json.gz` — a small hand-crafted gzipped Wikidata dump containing ~8–12 entities:

1. **Musician** (Ivy Queen, `Q2831`) — P106:musician, P136:reggaeton → included
2. **Band** (The Beatles, `Q11649`) — P31:musical group → included
3. **Non-musician** (Douglas Adams, `Q42`) — P31:human only → excluded
4. **Singer with instrument** (Adele, `Q23215`) — P106:singer + P1303:vocalist → included (occupation reason)
5. **Entity with only catch-all** (a fictional entity with P136 genre but no P106/P31) → included via catch-all
6. **Entity with all 4 catch-all properties** (P1303 + P175 + P136 + P358, but no P106/P31) → included
7. **Entity with no music properties at all** (a location like `Q90` Paris) → excluded
8. **Malformed last line** (truncated JSON as the final entry) → rejected

The fixture is in Wikidata dump format: `[` on first line, comma-separated JSON objects, `]` on last line. Each entity is a proper `Entity`-compatible JSON object.

**Integration test(s)** (add to `tests/bootstrap_test.rs` or create `tests/stream_test.rs`):

- `test_stream_mini_dump`: Open `tests/fixtures/mini_dump.json.gz` via `StreamReader`, drain the stream, verify:
  - Total processed lines = 11 (8 entities + 2 delimiters + 1 malformed line? actually it depends on format — let the fixture define it)
  - Filtered count = 5 (musician, band, singer, catch-all entity, all-4-props entity)
  - Rejected count = 1 (malformed last line)
  - Check specific filtered Q-IDs: `Q2831`, `Q11649`, `Q23215`, and the two catch-all entities
  - Check inclusion reasons: musician → `"P106:Q639669"`, band → `"P31:Q215380"`, catch-all entity → starts with `"PROP:"`
  - The `StreamEvent::Skipped` events for `[` and `]` delimiters are emitted but not counted in filtered/rejected totals
