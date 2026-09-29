# Research: Verification Battery Fixes & Rebuild

**Date:** 2026-09-29
**Status:** Plan (ready to execute)
**References:**

- [2026-09_bootstrap_genre_scan_rebuild.md](./2026-09_bootstrap_genre_scan_rebuild.md) — the v2 build plan (executed 2026-09-27/28)
- [2026-09_fix_entity_role_inversion.md](./2026-09_fix_entity_role_inversion.md) — the role fix whose §7 DoD battery this plan gates on

---

## 1. Purpose

The §7 DoD battery against the freshly built `music-v2.duckdb` (2026-09-28)
passed the core role-separation gate but surfaced **three defects** and
**two divergence findings**. The swap was frozen by decision on 2026-09-29.
This plan:

1. Fixes the three defects (SPARQL property-path bug, artist-search
   ranking, work-class list gap) in code with unit tests — a commit series
   on `main`-based `agent/fix-sparql-search-role-lists`.
2. Re-runs the fresh bootstrap + populate into `music-v2.duckdb` (v3),
   re-runs the full battery, and only then performs the swap.

## 2. Battery results (v2, 2026-09-28) — what the swap freeze is based on

Build: bootstrap 14:11→08:34 UTC (pass 1 **~4 h 30 m**, genre pass
**~4 h 24 m**, load ~37 s), populate 14:11→23:47 UTC (**~9 h 36 m**,
measured 220 MB/min consumed — the plan's 4-5 h populate estimate was
stale; the 970,737-QID Aho-Corasick pre-check dominates over
decompression).

| # | Check | Expect | Measured | Verdict |
|---|---|---|---|---|
| 1 | Role separation `album.id ∩ artist.id` | ≈ 0 | **0** | ✅ |
| 2 | "Joshua Tree" album searchable | includes it | "The Joshua Tree" (Q152873) + "Live from Joshua Tree" | ✅ |
| 3 | U2 → albums | non-empty | **128** album_artist rows | ✅ |
| 4 | `track_album` count | ≥ 7,000 | **1,395** | ❌ divergence |
| 5 | durations populated | ≥ 100 | **271** | ✅ (v1 was 0) |
| 6 | `album.release_date` populated | ≥ 1,000 | **401,290** | ✅ |
| 7 | QID-mirror album names | < 3,930 | **77,940** | ❌ divergence |
| 8a | `query artist --name "U2"` | hits agent | band U2 (Q396) **absent from top-100** | ❌ defect B |
| 8b | `query album --name "Joshua Tree"` | hits | both albums hit (with `--config` v2) | ✅ |
| 9 | `update --dry-run` | no inverted rows | **SPARQL HTTP 400** | ❌ defect A |
| 10 | test/clippy/fmt | clean | clean (step-0 state, no code change since) | ✅ |

`inclusion_reason` audit (top): `PROP:P136` 1,522,679 · `P106:Q177220`
116,691 · `PROP:P175` 108,263 · `P31:Q215380` 98,985 · `PROP:P136,P175`
53,883 · `PROP:P1303` 21,062 — property catch-all dominates by design.

### 2.1 Divergence triage (checks #4, #7) — accepted with root cause

- **#7 (77,940 QID-mirror album names):** `LEFT JOIN qid_label` shows
  **77,939 of them have NULL/empty labels** (1 has a label the backfill
  left alone) — the resolver and FK-safe backfill are intact; these albums
  genuinely lack English labels/sitelinks in the dump. The <3,930 baseline
  was v1's 109,742-row *inverted*-population number; v2 holds 443,626 real
  albums (4.05×), so an absolute-count threshold was stale. Residual
  shrink must come from label coverage, not this battery.
- **#4 (1,395 track_album rows):** every track_album row derives from a
  P361 parent claim in the dump (8,380 tracks total). The ≥7,000 figure
  assumed P361 class coverage this dump does not realize; §7 itself marks
  the check *conditional on work-class coverage* (see gate). Fix C may
  raise this; measure and record, do not tune the code to the number.

### 2.2 Defect A — SPARQL catch-all property path is malformed (#9)

`build_modified_query` (src/sparql.rs) emits:

```sparql
?item wdt:P1303|P175|P136|P358 [] .
```

SPARQL tokenizes the `|`-joined alternatives as separate IRIs, so `P175`
parses as an **unbound prefixed name** (`P` prefix, `175` local) →
endpoint HTTP 400. Confirmed by minimal repro 2026-09-29:

```
?s wdt:P1303|P175 []   → HTTP 400
?s wdt:P1303|wdt:P175 [] → HTTP 200
```

The update-path unit/integration tests mock the endpoint, which is why this
survived. `update --dry-run` is the first real-endpoint caller and now
finds it. **Fix:** qualify every alternative `wdt:{p}` before joining with
`|`.

### 2.3 Defect B — artist search hides exact-name matches (#8a)

`LIKE_SEARCH_ARTIST` (src/db/query.rs) matches `name LIKE` **or**
`description LIKE`, ordered by `name`, `LIMIT 100`. Searching "U2"
therefore surfaces description-substring rows ("vocal track by U2…")
ahead of the band; with 226 name/description matches the exact "U2"
(Q396) row ranks past the limit. The band exists and is correctly an
agent — the presenter cannot surface it. **Fix:** rank exact name
equality first: `ORDER BY (name = ?1) DESC, name`. Always run CLI
battery checks with `--config /tmp/battery.toml` (db → music-v2.duckdb);
the shipped `wiki_db.toml` still points at `~/wiki_db/music.duckdb`
(v1-inverted), which silently served the wrong DB during the v2 battery.

### 2.4 Gate — work-class list gap (triggered §7 condition) → Fix C

The U2 single **"40" (Q113111952)** landed in `artist` with
`inclusion_reason = PROP:P136,P175`. Live Wikidata shows its
`P31 = Q55850593` ("music track with vocals", sub-class of Q7302866) —
present in **neither** `TRACK_WORK_CLASS_IDS` nor `ALBUM_WORK_CLASS_IDS`
(grep: 0 hits), while `Q134556` ("single") *is* listed. Result: a common
P31 for vocal singles falls to the agent catch-all. This triggers §7's
gate condition: *"If a material share of known albums still falls to the
catch-all/agent path, extend the lists and re-run bootstrap before
treating the battery as valid."* The gate is stated for *albums*; the
observed miss is a *track* class, so this plan extends the condition's
intent to music works generally (album and track classes alike — the
same curation discipline applies both ways), and the close-out (Step 8)
updates the fix doc's §7 wording to say so. The swap is frozen until
the lists are audited and a fresh build re-validated.

## 3. Fix decisions

| Fix | File | Change | Verification |
|---|---|---|---|
| A | `src/sparql.rs` | Map each of `MUSIC_PROPERTIES` to `wdt:{p}` before `.join("|")`; **also** add P31 work-class UNION blocks (`ALBUM_WORK_CLASS_IDS`, `TRACK_WORK_CLASS_IDS`) so the update path can see work-class-only entities (it currently replicates only occupations, groups, and the catch-all) | Unit test asserts the qualified catch-all line and absence of the unqualified form, plus the work-class UNIONs; Step-1 live curl smoke check of the built query returns HTTP 200; live `update --dry-run` returns 200 |
| B | `src/db/query.rs` | `ORDER BY (name = ?1) DESC, name` in `LIKE_SEARCH_ARTIST`; mirror exact-match-first ordering in `search_artist_fts` or comment the divergence (FTS is inert in the bundled build) | Unit test asserts exact-name row first, incl. a >100-row spillover regression |
| C | `src/wikidata/filter.rs` | Golden-corpus audit (§4) **plus a frequency count per candidate class**; add missing common work classes to `TRACK_WORK_CLASS_IDS` / `ALBUM_WORK_CLASS_IDS`; update the class-list enumerations in ARCHITECTURE.md (and README if it enumerates) in the same commit | Audit + frequency tables recorded; post-rebuild spot-checks (U2's "40" → track/album, not artist) |

**Fix C classification rule** (from the fix doc): `P31 ∈ ALBUM_WORK_CLASS_IDS`
→ `album` (+ `album_artist` via P175); `P31 ∈ TRACK_WORK_CLASS_IDS` →
`track` (+ `track_artist` / `track_album` via P361). Q55850593 ("a track
on a music release that features vocals") is **track-class** by its own
definition; it goes to `TRACK_WORK_CLASS_IDS`. Only add classes that are
common, unambiguous music-work types; annotate each addition with its
audit evidence. Do **not** add catch-all "musical work" parents that would
resurrect role pollution — reuse the exact curation discipline of §7 Step 2.

## 4. Fix C golden-corpus audit (method)

1. Curate a golden list of ~30-40 famous albums/singles/EPs/songs across
   eras and genres (including at least U2 "40"/Q113111952, "Sunday Bloody
   Sunday", "Thriller" the album and the song, "Abbey Road", "Nevermind",
   "Kind of Blue", "OK Computer", "Born to Run", "Dark Side of the Moon",
   "Blue" (Joni Mitchell), "Back in Black", "Rumours", classic singles of
   differing P31 conventions).
2. For each golden QID, fetch `P31` via
   `https://www.wikidata.org/wiki/Special:EntityData/<Q>.json` (live API,
   as used on 2026-09-29; ~0.4 s per request, 40 requests ≈ 1 min).
3. Diff each P31 against `ALBUM_WORK_CLASS_IDS` + `TRACK_WORK_CLASS_IDS`;
   tabulate misses with the gradient (famous-album misses = material,
   obscure ones = marginal).
4. Frequency check: for each candidate miss, count instances **before**
   adding the class — a SPARQL `COUNT` (`{ ?s wdt:P31 wd:<class> }`) or a
   grep over a dump sample. Only classes with material, unambiguous
   counts are added. Review-time baseline for the flagship class:
   `P31:Q55850593` ≈ **32 K** live instances (dump ≈ same magnitude) —
   do not project beyond the measured count.
5. Add every **common** miss to the correct list per §3's rule. Record the
   full audit table in this doc's run notes (and update the fix doc §7
   figures if measured values diverge further).
6. Quantify expected deltas (not gates): for each added class, record its
   step-4 frequency count and projected row movement (e.g. Q55850593 →
   ~30 K tracks; `artist` PROP-catch-all rows shrink by the moved
   classes; `track_album` rises toward the P361 envelope).

## 5. Execution

### Step 0 — Pre-work (incl. committing this plan)

```bash
git status                 # expected: modified TODO.md + untracked plan doc
cargo test && cargo clippy -- -D warnings && cargo fmt --check
git checkout -b agent/fix-sparql-search-role-lists
git add docs/research/2026-09_verify_rebuild_fixes.md TODO.md
git commit -m "docs(research): plan verification battery fixes and rebuild"
git status                 # now clean: fix commits land on the branch surface
df -h /home/tr             # expect ≥ 60 GB free (battery state: ~91-94 GB)
```

### Step 1 — Fix A (commit)

`fix(sparql): qualify every alternative and replicate work classes in modified query`

- Edit `src/sparql.rs`: (1) map each `MUSIC_PROPERTIES` item to `wdt:{p}`
  before `.join("|")`; (2) add two UNION blocks replicating
  `ALBUM_WORK_CLASS_IDS` / `TRACK_WORK_CLASS_IDS` (same pattern as the
  group block), so the update path can see work-class-only entities.
- Unit tests: the qualified form
  `wdt:P1303|wdt:P175|wdt:P136|wdt:P358` appears and the unqualified
  `wdt:P1303|P175` does not (derive the expected alternatives from
  `filter::MUSIC_PROPERTIES`, don't hardcode); each album/track work
  class appears as `wdt:P31 wd:<qid>`.
- Live smoke check in the gate (~2 requests, seconds — not the 20 h-later
  battery): re-run the §2.2 repro against the built query and assert HTTP
  200 for both the property-path and the work-class blocks.
- `cargo test` (all green) → `cargo clippy -- -D warnings` →
  `cargo fmt --check` → `cargo build --release` → commit.

### Step 2 — Fix B (commit)

`fix(query): rank exact artist-name matches before LIKE matches`

- Edit `src/db/query.rs`: `ORDER BY (name = ?1) DESC, name` in
  `LIKE_SEARCH_ARTIST`; mirror exact-match-first ordering in
  `search_artist_fts` or add a comment that FTS must keep it (FTS is
  inert in the bundled build; a future activation must not silently
  resurrect Defect B).
- Unit tests: a row with exact `name = term` sorts before description-
  substring rows; a >100-row spillover regression (only the exact-name
  row may survive the `LIMIT 100`).
- Same verify gate → commit.

### Step 3 — Fix C (commit, with audit)

`fix(extraction): classify vocal singles and audited work classes as tracks`

- Run the §4 golden-corpus audit **with the frequency check**; append
  results to this doc's run notes and to
  `2026-09_fix_entity_role_inversion.md` §7 (`Step 2` curation evidence),
  then extend the class lists in `src/wikidata/filter.rs`. Update the
  class-list enumerations in ARCHITECTURE.md (filter-strategy decisions,
  ≈lines 251/318–319) — and README if it enumerates — in the same
  commit series (AGENTS.md doc conventions).
- Update/extend the role-classifier unit tests for any new class.
- Same verify gate → commit. (Doc updates ride the same commit series per
  AGENTS.md doc conventions.)

### Step 4 — Archive v2 artifact + release build

```bash
mv -n /home/tr/wiki_db/music-v2.duckdb /home/tr/wiki_db/music-v2-battery-20260928.duckdb
cargo audit        # AGENTS.md release hygiene (no new deps expected, but verify)
cargo build --release
```

### Step 5 — Bootstrap v3 (fresh parquet, ~10 h)

```bash
rm -rf /tmp/tag_agent_parquet_v3
nohup cargo run --release -- bootstrap \
    --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb \
    --parquet-dir /tmp/tag_agent_parquet_v3 \
    > /tmp/verify_bootstrap3.log 2>&1 &
```

Watch (`sed -e 's/\x1b\[[0-9;]*m//g'` first):

1. `Streaming phase complete processed=~121000000 filtered=~2680000` (~4.5 h)
2. `Genre label extraction complete genre_count=~13K` (~4-5 h)
3. `DuckDB loading complete` → summary counts

### Step 6 — Populate v3 (~9-10 h; 970 K-QID pre-check dominates, not 4-5 h)

```bash
nohup cargo run --release -- populate \
    --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb \
    --parquet-dir /tmp/tag_agent_parquet_v3 \
    > /tmp/verify_populate3.log 2>&1 &
```

Watch for `Extracted labels and claims from dump` + `FK-safe backfill
committed successfully` + `=== Populate Complete ===`. Progress can be
measured via `/proc/<pid>/fdinfo/<fd>.pos` against the 154,996,350,508-byte
dump (~220 MB/min measured; don't trust wall-clock for estimates).

### Step 7 — DoD battery v3 (against the rebuilt `music-v2.duckdb`)

Re-run the full §7 table (§2.0 above) **with fixes in place**:

| # | This run must show |
|---|---|
| 1,2,3,5,6,10 | unchanged passes |
| 4 | measured (P361 envelope; record, gate-conditioned) |
| 7 | measured (77,939 genuine label-less residual expected to persist — record as accepted divergence) |
| 8a | band U2 (Q396) is the first hit |
| 8b | both Joshua Tree albums via `--config /tmp/battery.toml` |
| 9 | `update --dry-run --since 2026-09-01T00:00:00Z --config /tmp/battery.toml` returns HTTP 200 (fresh DB has no `sync_state`; `--since` is required) |
| Gate | "40" (Q113111952) no longer in `artist`; lives in `track` (or `album`) with `track_artist`/`album_artist` → U2 |

Also record the `inclusion_reason` audit again.

### Step 8 — Swap (user decision) + close-out

```bash
mv -n /home/tr/wiki_db/music.duckdb /home/tr/wiki_db/music-v1-inverted.duckdb
mv -n /home/tr/wiki_db/music-v2.duckdb /home/tr/wiki_db/music.duckdb
```

(`-n` keeps the swap idempotent on a re-run.) `wiki_db.toml`
`db = "~/wiki_db/music.duckdb"` then points at the fixed DB.

Close-out: record battery numbers in this doc's run notes; update
`2026-09_fix_entity_role_inversion.md` §7 where figures diverge **and
reword its gate condition to cover music works (album and track classes)
rather than albums only** (§2.4); mark this doc `Status: Implemented`
with closing commit refs; no further code changes expected. If the
battery still fails, investigate before swapping.

## 6. Pitfalls

- **Do not** reuse `/tmp/tag_agent_parquet_v2` or the v1 `/home/tr/wiki_db/parquet-dir`
  for v3 — fresh dir only; orphan part files feed the loader wrongly.
- A 0-byte current `part-NNNNN.parquet` is normal (in-memory ~100 K batch).
- **CLI battery checks silently hit the wrong DB** unless
  `--config /tmp/battery.toml` (db → `music-v2.duckdb`) is passed — the
  default config still points at v1 `music.duckdb`. This confused the v2
  battery's #8; do not repeat.
- Fresh DBs have no `sync_state` → `update` requires `--since`.
- The dump is not Q-ID-sorted; estimate progress from stage markers (or
  the fd-offset rate), never from Q-IDs.
- Populate is **~9-10 h**, not the old 4-5 h figure; plan accordingly.
- Logs are ANSI-colored; strip before grepping.
- Archive (do not delete) `music-v2.duckdb` at Step 4 — it is the v2
  battery artifact and the tag_agent comparison baseline.
- The incremental-update SPARQL query only replicated occupations, groups,
  and catch-all properties in v2 — after Fix A it must also replicate the
  work-class lists, or `update` can never retrieve work-class-only works.
- Swap `mv`s use `-n` (no-clobber): a re-run must not overwrite an
  existing `music-v1-inverted.duckdb`.
- Re-run tripwire: if the genre pass ever projects days again, measure the
  rate and stop before letting it run.