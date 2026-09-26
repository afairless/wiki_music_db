# Research: Populate Substring Pre-Check Performance Optimization

**Date:** 2026-08-05
**Status:** Implemented
**Implemented:** 2026-08 — closes with commits `041c9a0` (aho-corasick dep) + `0392d4b` (Aho-Corasick substring scan) — see git log.

*Status updated per the [documentation conventions in AGENTS.md](../../AGENTS.md) — implemented plans carry `Status: Implemented` with their closing commits.*
**References:**

- [2026-08_populate_names_research.md](./2026-08_populate_names_research.md) — original populate design & implementation
- [ARCHITECTURE.md](../ARCHITECTURE.md) — system design & module map
- [`src/label_extractor.rs`](../../src/label_extractor.rs) — current implementation with bottleneck
- [`src/wikidata/filter.rs`](../../src/wikidata/filter.rs) — bootstrap filter for throughput comparison
- [aho-corasick crate](https://crates.io/crates/aho-corasick) — multi-pattern substring search

---

## 1. Problem Statement

The `populate` subcommand (`src/cli/populate.rs` → `src/label_extractor.rs`) resolves Q-ID placeholders in album, track, artist, genre, and instrument columns by re-scanning the full Wikidata JSON dump for English labels. The current implementation is pathologically slow — **~134 days projected runtime** for a 155 GB gzip dump — because of an O(K × L) substring pre-check performed on every line of the dump.

### Performance Measurements (Production Run)

A `populate --force` run was started on 2026-08-02 and observed on 2026-08-05 (after ~66 hours):

| Metric | Value |
|---|---|
| Gzip position | 2.86 GB / 155 GB |
| Progress | 1.85% |
| Read rate | 13 KB/s (compressed) |
| CPU utilization | 99.8% |
| Memory (RSS) | 230 MB |
| ETA at current rate | ~134 days |

### Bootstrap Comparison

The `bootstrap` subcommand processes the **same** 155 GB dump and writes 26 Parquet files in ~4.5 hours — a throughput of **9.1 MB/s**. Bootstrap does **more** work per entity (full JSON deserialization + claim inspection via HashMap lookups) yet is **700× faster** than populate. Populate should be faster, not slower, because it only parses JSON for matched entities (~1-2% of the dump).

| Phase | Entities/Lines | Work per entity | Throughput |
|---|---|---|---|
| Bootstrap | ~90M lines | Full JSON parse + HashMap-based claim filter | **9.1 MB/s** |
| Populate (current) | ~90M lines | Substring pre-check only (no JSON parse for most) | **0.013 MB/s** |
| Populate (expected) | ~90M lines | Substring pre-check + JSON parse for ~1-2% | Should exceed 9.1 MB/s |

---

## 2. Root Cause Analysis

### 2.1 The Bottleneck

The bottleneck is a single line in `src/label_extractor.rs` (line 176):

```rust
if !qid_sets.all.iter().any(|qid| line.contains(qid.as_str())) {
    continue;
}
```

For each of the ~90 million lines in the Wikidata dump, this code iterates through **every Q-ID** in the `all` HashSet and performs a `String::contains()` substring scan. The `contains()` method itself scans the full line text for each Q-ID.

### 2.2 Quantified Cost

The database contains **~2.6 million unique entity IDs** (confirmed via DuckDB query on the bootstrap Parquet files). After deduplication across album, track, artist, genre, and instrument tables, the `all` set contains ~2.6M Q-IDs.

Per-line work:

- **Current:** 2.6M × (average `contains()` scan of ~5 KB) = **~13 GB of character comparisons per line**
- **Total for 90M lines:** ~1.2 exabytes of character comparisons

This is purely CPU-bound (99.8% CPU utilization confirms it). The gzip decompression rate of 13 KB/s is simply a consequence of spending nearly all CPU time in the pre-check — only 13 KB of compressed data is consumed per second because that's all the CPU can process while doing the substring scan.

### 2.3 Why Bootstrap Is Fast

Bootstrap's `is_music_entity()` filter uses `HashMap::get()` lookups on specific claim property IDs (P106, P31, P136, etc.) — O(1) per entity regardless of how many entity types exist. There is no linear iteration over a set of candidate Q-IDs. The filter is:

```rust
// O(1) HashMap lookup
if let Some(stmts) = claims.get("P106") {
    for claim in stmts {
        if let Some(target) = claim_target_id(claim)
            && MUSIC_OCCUPATION_IDS.contains(&target)  // small constant array (~19 items)
        {
            return FilterResult::Included(...);
        }
    }
}
```

---

## 3. Proposed Solution: Aho-Corasick Automaton

### 3.1 Algorithm

Replace the `HashSet::iter() + String::contains()` loop with a single **Aho-Corasick automaton** built from all Q-IDs upfront. Aho-Corasick is a multi-pattern string matching algorithm that finds all occurrences of any pattern from a set in a single pass over the text — regardless of how many patterns exist in the set.

| Aspect | Current | With Aho-Corasick |
|---|---|---|
| **Build time** | None (per-line) | One-time: O(total pattern length) ≈ O(2.6M × 10 bytes) ≈ ~26 MB scanned |
| **Per-line scan** | O(K × L) where K ≈ 2.6M | O(L) — single pass regardless of K |
| **Memory** | ~230 MB (HashSet of Q-ID strings) | ~230 MB + automaton state (~100-200 MB depending on Q-ID overlap) |
| **False positives** | Yes (e.g., Q2831 matches Q28310) | Yes (same substring behavior, but same correctness guarantee via post-filter) |

### 3.2 The `aho-corasick` Crate

The Rust [`aho-corasick`](https://crates.io/crates/aho-corasick) crate (by Andrew Gallant / BurntSushi) is the gold standard for multi-pattern matching in Rust. It is used in:

- **ripgrep** — for literal pattern matching
- **regex** crate — for alternation optimization
- **fd** — for filename matching

Key features relevant to this use case:

- `AhoCorasick::new(patterns)` — builds the automaton from `&[&str]`
- `.find(haystack)` — returns the first match, or `None`
- `AhoCorasickBuilder` — supports case-insensitive matching, DFA construction for throughput
- Mature, well-maintained, zero-unsafe by default

### 3.3 Integration Point

The change is localized to `extract_labels_and_claims()` in `src/label_extractor.rs`:

**Before (current):**

```rust
pub fn extract_labels_and_claims(
    dump_path: &Path,
    qid_sets: &mut QidSets,
    parquet_dir: &Path,
) -> Result<()> {
    let file = fs::File::open(dump_path)?;
    let decoder = MultiGzDecoder::new(file);
    let mut reader = std::io::BufReader::new(decoder);
    // ... no automaton ...

    loop {
        line_buf.clear();
        let bytes_read = reader.read_line(&mut line_buf)?;
        if bytes_read == 0 { break; }
        // ...

        // BOTTLENECK: O(K × L) substring scan
        if !qid_sets.all.iter().any(|qid| line.contains(qid.as_str())) {
            continue;
        }
        // ... deserialize, extract ...
    }
}
```

**After (optimized):**

```rust
// At top of file (not inside function):
use aho_corasick::AhoCorasick;

pub fn extract_labels_and_claims(
    dump_path: &Path,
    qid_sets: &mut QidSets,
    parquet_dir: &Path,
) -> Result<()> {
    // Build automaton once from all Q-IDs (AhoCorasick::new() accepts empty
    // input and returns a no-match automaton, but an early return avoids
    // unnecessary file I/O when there are no Q-IDs to resolve).
    if qid_sets.all.is_empty() {
        tracing::info!("No Q-IDs to match — skipping label extraction");
        return Ok(());
    }
    let qid_patterns: Vec<&str> = qid_sets.all.iter().map(|s| s.as_str()).collect();
    let ac = AhoCorasick::new(&qid_patterns);

    // Secondary set for dynamically discovered Q-IDs during scan (see §3.3.1).
    let mut discovered_qids: HashSet<String> = HashSet::new();

    let file = fs::File::open(dump_path)?;
    let decoder = MultiGzDecoder::new(file);
    let mut reader = std::io::BufReader::new(decoder);
    // ...

    loop {
        line_buf.clear();
        let bytes_read = reader.read_line(&mut line_buf)?;
        if bytes_read == 0 { break; }
        // ...

        // OPTIMIZED: O(L) single-pass scan via automaton,
        // with fallback linear scan over dynamically discovered Q-IDs
        if ac.find(&line).is_none()
            && !discovered_qids.iter().any(|qid| line.contains(qid.as_str()))
        {
            continue;
        }
        // ... deserialize, extract ...
    }
}
```

### 3.3.1 Dynamic Q-ID Discovery During Scan

The existing `extract_labels_and_claims()` dynamically discovers P264 record-label Q-IDs on album entities during the scan and inserts them into `qid_sets.all` so label entities appearing later in the dump (higher Q-ID) can be matched. However, the pre-built Aho-Corasick automaton does not know about Q-IDs added after construction.

**Solution: secondary `HashSet` for discovered Q-IDs.** Maintain a separate `HashSet<String>` (initially empty) that accumulates dynamically discovered P264 Q-IDs. The pre-check becomes a two-tier filter:

```rust
if ac.find(&line).is_none()
    && !discovered_qids.iter().any(|qid| line.contains(qid.as_str()))
{
    continue;
}
```

The automaton handles the initial ~2.6M Q-IDs in O(L) time. The fallback linear scan over `discovered_qids` is negligible because the set is tiny (hundreds to low thousands of record-label Q-IDs per full dump scan). The linear scan only executes on lines that already failed the automaton check — ~99% of lines are rejected by the automaton alone.

Newly discovered P264 Q-IDs are inserted into **both** `qid_sets.all` (for the post-deserialization `contains()` check, which verifies the entity's Q-ID is actually in our set) **and** `discovered_qids` (for the line-level pre-check).

This preserves the existing behavior: record labels discovered during the scan are resolved in the same pass when their label entity appears later in the dump.

The `discovered_qids` set is bounded: even in the worst case (10,000+ record labels), the linear fallback contributes negligible overhead — it only runs on lines that already failed the automaton check (~1-2% of the dump, and each of those ~1-2M lines checks a few thousand Q-IDs at most). The total cost of the fallback is dwarfed by gzip decompression.

### 3.4 Pre-filter Semantics

The Aho-Corasick pre-filter preserves the same **substring match** semantics as the current `contains()` approach:

- **True positives:** A line containing `"id":"Q2831"` matches (correct)
- **False positives:** A line containing `"id":"Q28310"` matches because `Q2831` is a substring of `Q28310` (same as current behavior)
- **False positives are harmless:** The full entity deserialization + `qid_sets.all.contains(&entity.id)` check correctly filters false positives

The false-positive rate is bounded: Q-IDs have fixed numeric suffixes, so substring collisions only occur when one Q-ID is a prefix of another (e.g., Q28 vs Q283, Q283 vs Q2831). This is rare enough that it doesn't meaningfully affect throughput.

---

## 4. Performance Estimates

### 4.1 Post-Optimization Throughput

With the pre-check effectively free, the bottleneck shifts to gzip decompression:

| Scenario | Decompress rate | Time for 155 GB | Speedup |
|---|---|---|---|
| **Conservative** (miniz_oxide, current `rust_backend` config) | ~30 MB/s | ~86 minutes | **2,200×** |
| **Realistic** (miniz_oxide, modern CPU) | ~50 MB/s | ~52 minutes | **3,700×** |

> **Note:** The project's `Cargo.toml` uses `flate2` with `features = ["rust_backend"]` (pure-Rust miniz_oxide). Switching to the `zlib-ng` backend could yield higher throughput, but the Aho-Corasick optimization makes that unnecessary — even the conservative scenario reduces runtime from 134 days to ~86 minutes.

Lower bound for comparison: even if decompression only reaches 20 MB/s, that's still a **1,500× speedup** (90 minutes vs 134 days).

### 4.2 Wall-Clock Estimate

The bootstrap phase achieved 9.1 MB/s while doing full JSON deserialization for every entity. Populate with Aho-Corasick will:

1. Decompress lines at 50-200 MB/s (bottleneck)
2. Scan each line through the automaton (negligible cost)
3. Deserialize ~1-2% of entities (minor cost)

A **conservative estimate** of 20-30 minutes for the full dump scan is reasonable. Total populate runtime (scan + Parquet write + DuckDB load + backfill):

| Step | Estimated time |
|---|---|
| Collect Q-ID set from DB | < 5 seconds |
| Build Aho-Corasick automaton | < 1 second |
| Scan dump for labels + claims | 20-30 minutes |
| Write `labels.parquet` / `enrichment.parquet` | < 5 seconds |
| Load Parquet → DuckDB | < 30 seconds |
| SQL backfill (UPDATEs) | < 10 seconds |
| **Total** | **~25-35 minutes** |

### 4.3 Memory Estimate

| Component | Size |
|---|---|
| Q-ID HashSet (existing) | ~230 MB |
| Aho-Corasick automaton (patterns as `&str` references to HashSet entries) | ~100-200 MB |
| Line buffer + working memory | < 10 MB |
| **Total** | **~400-450 MB** |

Well within acceptable bounds for a batch operation. `AhoCorasick::new()` copies the pattern bytes internally to build the automaton state (no lifetime coupling to the input slice after construction). Using `Vec<&str>` (rather than `Vec<String>`) avoids allocating duplicate strings — the `&str` references borrow from `qid_sets.all` temporarily during construction. The secondary `discovered_qids` HashSet (see §3.3.1) adds negligible memory (~50 KB for 1,000 Q-IDs).

---

## 5. Implementation Plan

### Step 1: Add `aho-corasick` dependency

**File:** `Cargo.toml`

Add `aho-corasick = "1"` to `[dependencies]`.

**Commit:** `build(deps): add aho-corasick for multi-pattern substring matching`

### Step 2: Replace pre-check with Aho-Corasick automaton

**File:** `src/label_extractor.rs`

Changes to `extract_labels_and_claims()`:

1. Add `use aho_corasick::AhoCorasick;` to the top-level imports of `src/label_extractor.rs` (not inside the function body).

2. Build the automaton at the top of the function, with an early return for empty Q-ID sets (avoids unnecessary file I/O when no Q-IDs need resolution):

   ```rust
   if qid_sets.all.is_empty() {
       tracing::info!("No Q-IDs to match — skipping label extraction");
       return Ok(());
   }
   let qid_patterns: Vec<&str> = qid_sets.all.iter().map(|s| s.as_str()).collect();
   let ac = AhoCorasick::new(&qid_patterns);
   let mut discovered_qids: HashSet<String> = HashSet::new();
   ```

3. Replace the substring pre-check loop (two-tier filter — automaton for bulk Q-IDs, linear fallback for dynamically discovered Q-IDs; see §3.3.1):

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

4. Update the P264 dynamic-insertion site to insert into **both** `qid_sets.all` (for the post-deserialization Q-ID check) and `discovered_qids` (for the line-level pre-check):

   ```rust
   // Before:
   if let Some(ref rl_qid) = record_label_qid
       && qid_sets.all.insert(rl_qid.clone())
   {
       discovered_label_qids.push(rl_qid.clone());
       // ...
   }
   // After: also track in discovered_qids for the line-level pre-check
   if let Some(ref rl_qid) = record_label_qid
       && qid_sets.all.insert(rl_qid.clone())
   {
       discovered_qids.insert(rl_qid.clone());
       discovered_label_qids.push(rl_qid.clone());
       // ...
   }
   ```

**Commit:** `perf(label_extractor): replace O(n) substring scan with Aho-Corasick automaton`

Tests included in this step:

| Test | What it verifies |
|---|---|
| `test_aho_corasick_precheck_match` | Line containing a Q-ID is matched by the automaton |
| `test_aho_corasick_precheck_skip` | Line without any Q-ID is correctly skipped |
| `test_aho_corasick_precheck_false_positive` | Substring false positive (e.g., Q2831 in Q28310) passes pre-check but is correctly filtered by full Q-ID comparison |
| `test_aho_corasick_many_patterns` | Building from 10K+ Q-IDs works correctly and finds matches |
| `test_empty_qid_set_returns_early` | Calling `extract_labels_and_claims()` with an empty `qid_sets.all` returns `Ok(())` without error (early-return path; `AhoCorasick::new(&[])` returns a valid no-match automaton) |
| `test_discovered_qids_fallback` | A Q-ID added to `discovered_qids` mid-scan matches a line via the linear fallback, even though it's not in the automaton |
| `test_aho_corasick_equivalent_to_hashset_contains` | Property-based: random Q-ID sets and random lines — verifies automaton matches exactly the same lines as the original `HashSet::iter().any()` approach |

Update existing tests (`test_substring_precheck_match`, `test_substring_precheck_skip`, `test_substring_precheck_false_positive`) to exercise the Aho-Corasick path by calling a helper that reflects the optimized pre-check logic.

### Step 3: Integration validation

Run the full test suite:

```bash
cargo test
cargo clippy -- -D warnings
cargo fmt --check
cargo audit
```

Verify existing integration tests pass without modification, confirming the optimization preserves correctness.

**Commit:** None (verification step only)

---

## 6. Testing Strategy

### 6.1 Correctness Guarantee

The Aho-Corasick optimization is **functionally identical** to the current pre-check:

- Both use substring matching (not exact Q-ID matching)
- Both produce false positives (handled by the full entity ID check after deserialization)
- Both produce exactly the same set of entities that pass the pre-check → deserialize → extract path

The only difference is performance. No existing tests should need to change their assertions.

### 6.2 Property-Based Test

A property-based test using `proptest` (already a dev-dependency) verifies equivalence between the Aho-Corasick pre-check and the original `HashSet::iter().any()` approach:

```rust
#[test]
fn test_aho_corasick_equivalent_to_hashset_contains() {
    // Generate random Q-ID sets and random lines
    // Verify: ac.find(line).is_some() == qids.iter().any(|q| line.contains(q))
}
```

This test is included in Step 2 alongside the other unit tests. While the equivalence is guaranteed by the algorithm, a property-based test provides defense against regressions and crate bugs.

### 6.3 Regression Tests

All existing tests in `src/label_extractor.rs` and `tests/` must pass without modification. The pre-check is an internal optimization with no behavioral change.

---

## 7. Risk Assessment

### 7.1 Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Automaton build time excessive for 2.6M patterns | Low | High (delays startup) | `AhoCorasick::new()` builds in O(total pattern length) — ~26 MB of pattern data, expected < 1 second. If slow, use `AhoCorasickBuilder::build()` to select DFA vs NFA |
| Automaton memory too large (2.6M patterns) | Low | Medium (OOM on constrained systems) | Estimated 100-200 MB overhead. If excessive, chunk patterns into multiple automata or sample most-frequent Q-IDs |
| `aho-corasick` version conflicts with transitive deps | Very low | Low | `regex` already depends on `aho-corasick` internally; no version conflict expected |
| `aho-corasick` unsoundness / bug | Very low | High | Crate is mature (v1.x), widely used in production (ripgrep, fd, regex), zero-unsafe default |

### 7.2 Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Regex alternation `(Q2831\|Q42\|...)` | Building a single 2.6M-alternation regex would be extremely slow to compile and memory-intensive. The `regex` crate uses Aho-Corasick internally for alternations anyway. |
| Bloom filter | Does not provide match positions; would still need the substring scan for confirmed matches. Adds complexity without proportional gain. |
| Multi-threaded dump scanning | Requires work-partitioning the gzip stream (non-trivial). Aho-Corasick solves the root cause without added concurrency complexity. |
| Pre-extract Q-ID from line via byte scan | Locating `"id":"` and extracting the Q-ID would be very fast, but Wikidata JSON lines contain Q-IDs in many positions (claims, qualifiers, references), not just the entity ID. The pre-check must match Q-IDs appearing anywhere in the line. Aho-Corasick handles this naturally. |
| Skip pre-check entirely, parse all JSON | Parsing full JSON for all 90M entities would be slower than bootstrap (which already takes 4.5 hours). The Aho-Corasick pre-check is a strict improvement over both the current approach and the no-pre-check alternative. |

### 7.3 Rollback

If the optimization causes issues, reverting to the current `HashSet::iter().any()` pre-check is a single-line change. No schema or file format changes are involved. The Parquet output is identical regardless of which pre-check algorithm is used.

---

## 8. References

- [aho-corasick crate documentation](https://docs.rs/aho-corasick)
- [Aho-Corasick algorithm (Wikipedia)](https://en.wikipedia.org/wiki/Aho%E2%80%93Corasick_algorithm)
- Current populate research: [2026-08_populate_names_research.md](./2026-08_populate_names_research.md)
- Bottleneck source: [`src/label_extractor.rs`](../../src/label_extractor.rs), `extract_labels_and_claims()`
- Bootstrap filter (fast reference): [`src/wikidata/filter.rs`](../../src/wikidata/filter.rs), `is_music_entity()`
