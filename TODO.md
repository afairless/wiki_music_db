# Implementation Plan: Populate Album, Track, and Other Name Columns

Source: `docs/research/2026-08_populate_names_research.md`

## Context

This plan builds on the existing wiki_db pipeline (Phases 1–8 completed). The database currently stores Q-ID placeholders for album/track names, NULL artist names, empty `album_genre` and `track_album` tables, and NULL enrichment columns (release dates, record labels, durations). This plan resolves all of those via a re-scan of the Wikidata dump that extracts labels, claims, and cross-references for all referenced Q-IDs.

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `feat(db): add qid_label, instrument, and record_label tables` | Schema migration | `src/db/schema.rs` — add 3 tables, bump SCHEMA_VERSION to 2, add migration path | Unit |
| 2 | `feat: implement Q-ID label and claims extraction from Wikidata dump` | Label extractor | `src/label_extractor.rs` — Q-ID collection, stream scan, substring pre-check, Parquet writer for `labels.parquet` and `enrichment.parquet` | Unit, property-based |
| 3 | `feat(db): add label and enrichment Parquet loader with backfill` | DuckDB loader | `src/db/load.rs` — load labels/enrichment, backfill UPDATEs in transaction, recreate FTS, deprecate `extract_genre_labels()` | Integration |
| 4 | `feat(cli): add populate subcommand for name/date/label backfill` | CLI subcommand | `src/cli/populate.rs` — `populate` subcommand with `--force`, `--resume`; `src/cli/mod.rs` — register; `src/main.rs` — wire | Unit, integration |
| 5 | `feat(update): fetch labels for new entities during incremental update` | Update pipeline | `src/sparql.rs` — extend `fetch_entity` response; `src/db/load.rs` — upsert labels; `src/cli/update.rs` — wire label resolution | Unit |

---

# Populate Substring Pre-Check Performance Optimization

Source: `docs/research/2026-08_populate_performance_optimization.md`

## Context

The `populate` subcommand's `extract_labels_and_claims()` in `src/label_extractor.rs` has a severe performance bottleneck. The substring pre-check iterates through every Q-ID in a ~2.6M-element HashSet and performs a `String::contains()` scan for each one, achieving only ~13 KB/s throughput — projecting a **~134 day runtime** for the full 155 GB dump.

This plan replaces the O(K × L) pre-check with an **Aho-Corasick automaton** that finds all pattern matches in a single pass over the text, regardless of the number of patterns. Expected speedup: **1,500–3,700×** (from 134 days to ~25–35 minutes).

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `build(deps): add aho-corasick for multi-pattern substring matching` | Dependency | `Cargo.toml` — add `aho-corasick = "1"` | — |
| 2 | `perf(label_extractor): replace O(n) substring scan with Aho-Corasick automaton` | Pre-check optimization | `src/label_extractor.rs` — automaton build, two-tier filter (automaton + discovered_qids fallback), P264 insertion site update | Unit, property-based |

## Step details

### Step 1 — `build(deps): add aho-corasick for multi-pattern substring matching`

**Rationale:** The `aho-corasick` crate provides the multi-pattern string matching automaton needed to replace the O(K × L) `HashSet::iter().any()` pre-check loop.

**Deliverables:**

- `Cargo.toml`:
  - Add `aho-corasick = "1"` to `[dependencies]`

**Tests:** None — purely a build configuration change.

---

### Step 2 — `perf(label_extractor): replace O(n) substring scan with Aho-Corasick automaton`

**Rationale:** Replace the per-line `HashSet::iter().any(|qid| line.contains(qid))` loop (O(K × L) where K ≈ 2.6M) with a two-tier filter: an Aho-Corasick automaton for the bulk Q-IDs (O(L) single pass), plus a linear fallback over dynamically discovered Q-IDs (tiny set, typically hundreds).

**Deliverables:**

- `src/label_extractor.rs`:
  1. Add `use aho_corasick::AhoCorasick;` to the top-level imports.
  2. Add early return guard at the top of `extract_labels_and_claims()`:

     ```rust
     if qid_sets.all.is_empty() {
         tracing::info!("No Q-IDs to match — skipping label extraction");
         return Ok(());
     }
     ```

  3. Build the automaton once from all Q-IDs:

     ```rust
     let qid_patterns: Vec<&str> = qid_sets.all.iter().map(|s| s.as_str()).collect();
     let ac = AhoCorasick::new(&qid_patterns);
     let mut discovered_qids: HashSet<String> = HashSet::new();
     ```

  4. Replace the substring pre-check with a two-tier filter:

     ```rust
     // Before:
     if !qid_sets.all.iter().any(|qid| line.contains(qid.as_str())) {
         continue;
     }
     // After:
     if ac.find(&line).is_none()
         && !discovered_qids.iter().any(|qid| line.contains(qid.as_str()))
     {
         continue;
     }
     ```

  5. Update the P264 dynamic-insertion site to also insert into `discovered_qids`:

     ```rust
     // Before:
     if let Some(ref rl_qid) = record_label_qid
         && qid_sets.all.insert(rl_qid.clone())
     {
         discovered_label_qids.push(rl_qid.clone());
     }
     // After:
     if let Some(ref rl_qid) = record_label_qid
         && qid_sets.all.insert(rl_qid.clone())
     {
         discovered_qids.insert(rl_qid.clone());
         discovered_label_qids.push(rl_qid.clone());
     }
     ```

- **Tests (unit in `src/label_extractor.rs`):**
  - `test_aho_corasick_precheck_match` — Line containing a Q-ID is matched by the automaton
  - `test_aho_corasick_precheck_skip` — Line without any Q-ID is correctly skipped
  - `test_aho_corasick_precheck_false_positive` — Substring false positive (e.g., Q2831 in Q28310) passes pre-check but is correctly filtered by full Q-ID comparison
  - `test_aho_corasick_many_patterns` — Building from 10K+ Q-IDs works correctly and finds matches
  - `test_empty_qid_set_returns_early` — Calling with an empty `qid_sets.all` returns `Ok(())` without error
  - `test_discovered_qids_fallback` — A Q-ID added to `discovered_qids` mid-scan matches a line via the linear fallback
  - `test_aho_corasick_equivalent_to_hashset_contains` — Property-based test with random Q-ID sets and random lines, verifying automaton matches exactly the same lines as the original `HashSet::iter().any()` approach

- **Test updates:**
  - Update existing `test_substring_precheck_match`, `test_substring_precheck_skip`, `test_substring_precheck_false_positive` to exercise the Aho-Corasick path via a helper function

---

### Step 3 — Integration validation

**Rationale:** Run the full test suite and linters to confirm the optimization preserves correctness. No code changes.

**Deliverables:**

- `cargo test` — all existing tests pass without modification
- `cargo clippy -- -D warnings` — no warnings
- `cargo fmt --check` — formatting is clean
- `cargo audit` — no known vulnerabilities

**Commit:** None — verification step only.
