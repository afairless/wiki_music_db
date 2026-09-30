# Research: Fix Album/Track Entity-Role Inversion and Finish Enrichment Coverage

**Date:** 2026-09
**Status:** Implemented
**Implemented:** 2026-09 — closes with commits `45f892d` (model: amount/unit + sitelinks), `a599281` (filter: role classifier), `ddc5363` (extraction: performers/parents), `5d1ca0f` (parquet: role/parents columns), `ee86c07` (loader: bootstrap role routing), `41a0bac` (loader: update-path role routing), `27b0309` (label_extractor: P2047 durations), `34cd953` (label_extractor: sitelink fallback), `800f7b4` (query: corrected-role searches), `8895109` (test: work-entity fixtures) — see git log. The Step 10 real-dump verification battery is recorded in the TODO step 12 run notes.

*Status updated per the [documentation conventions in AGENTS.md](../../AGENTS.md) — implemented plans carry `Status: Implemented` with their closing commits.*
**References:**

- [ARCHITECTURE.md](../ARCHITECTURE.md) — schema, pipeline, filtering strategy, decision log
- [2026-08_populate_names_research.md](./2026-08_populate_names_research.md) — origin of the `populate` phase and its coverage claims
- [2026-07_music_db_rust_plan.md](./2026-07_music_db_rust_plan.md) — original schema intent (album = P31 subclasses of `Q482994`)
- [2026-08_fix_enrichment_fk_album_track_guards.md](./2026-08_fix_enrichment_fk_album_track_guards.md) — the FK guards that silently drop track→album parents
- [2026-08_populate_performance_optimization.md](./2026-08_populate_performance_optimization.md) — Aho-Corasick extraction path amended by this plan

---

## 1. Problem Statement

The `populate` phase (implemented 2026-08, run on `/home/tr/wiki_db/music.duckdb` 2026-09-26) resolved names, but a **structural role inversion introduced at bootstrap** means the database still cannot answer the questions tag_agent needs from it: *"is this album/track real, and does this tag match?"* Re-running `populate` cannot fix any of this — the defects are in the extraction and loading layers.

Verified against `/home/tr/wiki_db/music.duckdb` and the working tree (2026-09-26):

| # | Failure | Evidence (verified) |
|---|---------|---------------------|
| 1 | **Album/track tables are populated from the wrong Q-ID side** | **87,577 of 109,742** album IDs are *also* IDs in `artist`. "The Joshua Tree" (`Q152873`, desc "1987 studio album by U2") and "Kind of Blue" (`Q1741708`) are rows in **`artist`**, not `album`. `album.name ILIKE '%joshua tree%'` → 0 rows. |
| 2 | **Artist→album / artist→track links are inverted or dead** | Canonical U2 (`Q396`, "Irish rock band") has **0** rows in `album_artist` and **0** in `track_artist`. The link direction is performer→work stored as album→artist. |
| 3 | **`track_album` junction stuck at 4 rows** | `enrichment.parquet` carries **7,194** P361 track→parent claims (2,107 distinct parents), but the loader's FK guard requires `parent_album_qid IN (SELECT id FROM album)`; **2,007 of 2,107 distinct parents live in `artist` instead**, so 7,190 rows are silently dropped. |
| 4 | **`track.duration_seconds` stays 0 — a dead-code bug, not a data limit** | `DatavalueValue` (`src/wikidata/model.rs`) captures only `id`/`time`/`precision`; the P2047 quantity's **`amount`** is dropped during deserialization. `extract_p2047_duration` then reads `claim.extra.get("amount")`, which can never exist (extra holds claim-level id/rank/qualifiers/references/hash). **No code path can ever extract a duration.** |
| 5 | **`album.release_date` coverage (197 rows) is a symptom, not the cause** | P577 extraction works (time+precision parsed); the reason only 197 albums have dates is that most "album" stubs are actually *agents*, which rarely carry P577. |
| 6 | **~3,930 albums / ~8,942 tracks remain QID mirrors** | These entities have no English label *in the dump*. Only fix within scope: sitelink fallback (e.g. `enwiki` title), which is a documented contract change. |

Root cause: **`is_music_entity` admits works via its catch-all properties**, and `extract_music_entity` then treats them as artists.

- `filter.rs` catch-all: `≥1 of P1303 (instrument), P175 (performer), P136 (genre), P358 (discography)`.
- Albums and songs carry **P175** (their performers) and **P136** (genre) → any album/song passes the filter as if it were an artist.
- `extract_music_entity` on such a work reads `P175` (which on a work points *to the performers*) and stores the **performer Q-IDs** into its `albums` JSON.
- `load_albums_and_tracks` then creates album stubs from those values → **performer Q-IDs become album stubs**, and the work itself is stored as an "artist".

The correct Wikidata direction (used by the original schema intent in `2026-07_music_db_rust_plan.md`) is: a *work* (album/song) has `P31 ∈ work classes`, `P175` → *featured performers* (artists), `P361` → *parent album* (tracks). The current code reads these relationships backwards.

---

## 2. Goal and Non-Goals

### Goals

1. Real albums/tracks become first-class rows in `album`/`track`; real artists in `artist`; the same Q-ID never appears in both for the same role.
2. `album_artist` / `track_artist` encode work→performer correctly ("albums of U2" works).
3. `track_album` loads from the existing P361 enrichment (target ≥7,000 of the 7,194 rows).
4. `track.duration_seconds` actually populates wherever the dump has a parseable P2047 (seconds).
5. `album.release_date`/`record_label`/`album_genre` coverage improves because the album table now contains real albums.
6. QID-mirror residual shrinks via an **enwiki-sitelink label fallback** (contract change, see §4).

### Non-Goals

- Supplementing data from external sources (MusicBrainz/Spotify/Deezer APIs) — decided out of scope; dump-only.
- Internationalized labels beyond the `enwiki` sitelink fallback (multilingual label sets stay deferred).
- Fixing every contaminated entity inherited from Wikidata itself (e.g. the "U2/Negativland EP" row `Q1186863`) — that remains the tag_agent LLM's *judgment* job.
- Track number (`track_album.track_number`) population — no source claim is currently extracted; deferred.

---

## 3. Root-Cause Anatomy (code paths)

| Stage | File | Problem |
|---|---|---|
| Filter | `src/wikidata/filter.rs` | Catch-all props admit works as music entities; no work-vs-agent separation; `MUSIC_GROUP_IDS` is the only P31 use. |
| Extraction | `src/extraction.rs` | `P175` values blindly pushed into `MusicEntity.albums` (assumes subject is an artist). Works are emitted as artists. |
| Parquet | `src/parquet_writer.rs` | Artist-centric row schema; no work identifier/role column. |
| Loader | `src/db/load.rs` `load_albums_and_tracks` | Creates album/track stubs from `albums`/`tracks` JSON (i.e. from performer Q-IDs) and pairs them with row.id as the artist. |
| Enrichment | `src/label_extractor.rs` `extract_track_claims` / `extract_p2047_duration` | P361 captured, but parents dropped by loader FK guard; P2047 reads a field that cannot exist. |
| Model | `src/wikidata/model.rs` `DatavalueValue` | Drops `amount`/`unit` of quantity datavalues. |
| Model | `src/wikidata/model.rs` `Entity` | No `sitelinks` captured → no label fallback possible. |

---

## 4. Data-Contract Changes (documented here; must land in ARCHITECTURE/README/AGENTS in the docs step)

1. **Entity roles.** The pipeline now distinguishes three roles: `agent` (people/groups), `album` (works in album classes), `track` (works in song classes). Bootstrap emits each matching entity into exactly one role. `artist` holds agents only.
2. **Name resolution contract.** Label source precedence becomes: `en` label → `enwiki` sitelink title → NULL. `enwiki` titles are **sanitized** before use: `_` → space (`The_Joshua_Tree` → `The Joshua Tree`); `(album)`/`(song)`-style disambiguators are **kept** (decision: do not strip). This replaces "English-only labels" in the Limitations section of ARCHITECTURE.md and amends the "Decision: English-only labels" key-design-decision section.
3. **Duration contract.** `track.duration_seconds` accepts a P2047 amount whose unit resolves to seconds (unit `Q11574`); other/unparseable units → NULL (WARN), matching existing "log and store NULL, never reject" policy.
4. **Work classes.** A curated hardcoded P31 work-class list (same style as `MUSIC_GROUP_IDS`), because subclass resolution (P279) is documented as unsupported. **Only two class IDs are verified against the local DB's `qid_label` today**: `Q482994` (album) and `Q134556` (single). The remainder of the list (song, EP, compilation album, live album, instrumental, …) must be **curated and unit-tested during Step 2** — the IDs in this plan body are placeholders, not verified:
   - Album candidates: `Q482994` (album — verified), `Q134556` (single — verified), plus compilation/EP/live classes to be confirmed
   - Track candidates: song/instrumental classes to be confirmed
   - Decision to confirm at implementation: whether singles live in `album` (v1 schema intent says "Albums, EPs, singles, and compilation albums ... P31 subclasses of Q482994" → **yes**, singles/EPs → `album`).

---

## 5. Implementation Plan

Branch: `agent/fix-entity-role-inversion`

| # | Commit message (conventional) | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `fix(model): capture amount/unit and sitelinks in Wikidata entity model` | Model extension | `DatavalueValue.amount/unit`; `Entity.sitelinks` (map code→struct with `title`) | Unit tests: quantity datavalue round-trip; sitelinks deserialization |
| 2 | `fix(filter): classify entities into agent/album/track roles` | Role classifier | Work-class P31 lists; `is_music_entity` **kept as a thin wrapper over `classify_entity`** (returns role + reason) so all callers — incl. the update path — keep compiling; existing occupation/group logic unchanged for agents | Unit tests: album → not artist; group → agent; song → track; non-music → excluded; entity with both P106 and work-class P31 → agent (documented precedence) |
| 3 | `fix(extraction): emit works with performers and parents instead of inverting` | Extraction | `MusicEntity` gains `role` field; on works, P175→`album_artist`/`track_artist` source refs and P361→`parent_album`; agents no longer absorb P175 as "albums". `AlbumRef`/`TrackRef` are merged into `PerformerRef { qid, role }` so the parquet JSON columns no longer claim to hold album/track IDs on work rows | Unit tests: work with P175/P361 emits performer/parent refs, not inverted refs |
| 4 | `fix(loader): route works into album/track and load performer/parent junctions` | Loader | `load_albums_and_tracks` splits by role; album/track rows come from work entities; `album_artist`/`track_artist` from P175 of works; `track_album` from P361; **`load_artists` + all three artist join loaders filter `WHERE role = 'Agent'`** (else work rows with P136 genres violate the artist_* FKs); **same role routing applied to the incremental-update upsert path** (`upsert_entity`/`upsert_entity_from_json`) | Integration: mini dump fixture → correct rows in all tables; FK smiles; update-path tests: a work upserted from the REST-entity shape lands in album/track, not artist |
| 5 | `fix(label_extractor): parse P2047 duration from datavalue amount` | Duration extraction | `extract_p2047_duration` reads `mainsnak.datavalue.amount`; unit-validated to seconds; delete the dead `extra["amount"]` path AND rewrite the existing `test_extract_claim_p2047` (it asserts the dead path) | Unit tests: `+240`/unit Q11574 → 240; amount without unit → 240 (accepted per contract); missing amount / non-numeric / non-second unit → NULL |
| 6 | `fix(label_extractor): add sitelink fallback to label extraction` | Label fallback | `extract_label` uses `en` label, else sanitized `enwiki` title (`_` → space; disambiguators kept per §4 contract #2) | Unit tests: en label wins; no en label + enwiki → sanitized title; neither → NULL; underscore title sanitized |
| 7 | `fix(query): reaffirm artist/album searches over corrected roles` | Query layer | Verify `query artist/album/track` precedence; adjust SQL only if role semantics require | Query tests: "The Joshua Tree" in album; "U2" in artist with album_artist links |
| 8 | `test(db): fixtures for corrected role wiring and durations` | Test fixtures | `tests/fixtures/` additions: album work entity (P31+P175), song work (P361+P2047), group agent, sitelink label; **update existing integration expectations that encode the old inversion** (`bootstrap_test.rs` P175→album stub, `query_test.rs`, `update_test.rs`) | Fixture-driven integration tests incl. P2047 and track_album; update-path role tests |
| 9 | `docs: document entity roles, sitelink fallback, and the fixed enrichment contract` | Docs sync | ARCHITECTURE.md (roles, limitations), README.md (pipeline semantics), AGENTS.md (structure), research status headers | Grep spot-checks; mark this plan `Status: Implemented` with closing commits |
| 10 | *verification (no commit)* | Re-run on real dump | Fresh `music-v2.duckdb` bootstrap + populate; verification battery (§7) | Full battery below |

### Step 0 — Pre-work

```bash
git status            # clean
cargo test && cargo clippy -- -D warnings && cargo fmt --check
git checkout -b agent/fix-entity-role-inversion
```

Commit this plan doc (`docs(research): plan entity-role inversion fix and enrichment completion`) before Step 1.

### Step 1 — `fix(model)` — quantity and sitelinks in the entity model

Extend `DatavalueValue`:

```rust
pub struct DatavalueValue {
    #[serde(default)] pub id: Option<String>,
    #[serde(default)] pub time: Option<String>,
    #[serde(default)] pub precision: Option<i64>,
    #[serde(default)] pub amount: Option<String>,   // NEW — P2047 quantity
    #[serde(default)] pub unit: Option<String>,     // NEW — e.g. http://www.wikidata.org/entity/Q11574
}
```

Extend `Entity`:

```rust
#[serde(default)]
pub sitelinks: Option<HashMap<String, Sitelink>>,   // NEW — code -> {title, ...}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sitelink { pub title: String }
```

Tests: deserialize a real quantity datavalue from the dump shape; deserialize an entity with `sitelinks: {"enwiki": {..., "title": "The Joshua Tree"}}`.

### Step 2 — `fix(filter)` — role classifier

Fork `is_music_entity` into a role classifier:

```rust
pub enum EntityRole { Agent, Album, Track }

pub fn classify_entity(claims) -> Option<(EntityRole, &'static str /* reason */)>
// precedence:
//   1. P106 ∈ MUSIC_OCCUPATION_IDS                    → Agent
//   2. P31  ∈ MUSIC_GROUP_IDS                         → Agent
//   3. P31  ∈ ALBUM_WORK_CLASS_IDS                   → Album
//   4. P31  ∈ TRACK_WORK_CLASS_IDS                   → Track
//   5. catch-all (P1303/P175/P136/P358 ≥1)           → Agent ONLY IF no work-class P31 present;
//     if a work-class P31 is present it wins (3/4)    (removes the inversion source)
//   6. otherwise                                      → excluded
```

Keep `inclusion_reason` semantics (now also `ROLE:Album` etc.). **Keep `is_music_entity(claims) -> FilterResult` as a thin wrapper over `classify_entity`** — it is called by `upsert_entity_from_json` and by tests; removing it outright breaks compilation at this step. Document and test the precedence choice: an entity with both P106 ∈ `MUSIC_OCCUPATION_IDS` and a work-class P31 is an **agent** (P106 wins) — rare, intentional, covered by a unit test. All existing filter unit tests must stay green; add album/song/EP/single cases.

### Step 3 — `fix(extraction)` — works emit performers and parents, not inverted refs

`MusicEntity` gains a `role: EntityRole`. `AlbumRef`/`TrackRef` are merged into a single `PerformerRef { qid, role }` — the old `album_id`/`track_id` field names would now hold performer Q-IDs on work rows, a lie in the JSON schema. Claim mapping becomes role-aware:

| Role | P175 | P361 | Emitted artifact |
|---|---|---|---|
| Agent | — | — | artist row (as today); no album stubs |
| Album | featured performers | — | album row + `album_artist(album=me.id, artist=performer)` |
| Track | featured performers | parent album | track row + `track_artist` + `track_album(track=me.id, album=parent)` |

Agent rows keep their existing `member_of`/`instrument`/`genre` extraction. Works carry `genre` (P136) for `album_genre`/`track_genre`-style linkage inside enrichment.

### Step 4 — `fix(loader)` — route by role

`load_albums_and_tracks` splits by `role` column (add `role` to the parquet writer schema, back-compatible via default). Album/track stubs come from work rows only; performer/parent junctions come from the work's P175/P361 JSON. Because works now carry names (labels) with sitelink fallback at populate time, QID-mirror names shrink.

**Role filter on the artist loaders (required, or bootstrap breaks):** `part-*.parquet` now also contains Album/Track rows with non-empty P136 `genres`. `load_artists` and the three artist join loaders (`load_artist_genre`, `load_artist_instrument`, `load_artist_member_of`) must filter `WHERE role = 'Agent'` — otherwise work IDs are inserted as `artist_id`/`group_id` and violate the `REFERENCES artist(id)` FKs in `schema.rs`, failing `load_all` outright.

**Update path (incremental):** apply the same role routing to `upsert_entity` / `upsert_entity_from_json` in `src/db/load.rs` — they currently `INSERT OR REPLACE` every entity into `artist` and read P175/P658 with the old inverted semantics. Without this, `update` re-introduces inverted rows into a fixed database.

Remove the `take-first-P175-on-any-entity` path entirely — this is the inversion we are deleting.

### Step 5 — `fix(label_extractor)` — P2047 durations

```rust
fn extract_p2047_duration(claims) -> Option<String> {
    for claim in claims {
        let dv = claim.mainsnak?.datavalue?;
        let amount = dv.amount?.trim_start_matches('+');
        if amount parses as i64 && (dv.unit is None || dv.unit ends with "Q11574") {
            return Some(amount);
        }
    }
    None  // WARN + NULL per contract
}
```

Delete the `claim.extra.get("amount")` path (impossible by construction). The existing `src/label_extractor.rs` `test_extract_claim_p2047` asserts exactly that dead path — rewrite it in this step to build the claim from the `mainsnak.datavalue` shape (`amount`/`unit` per Step 1) and cover: amount + unit `Q11574` → seconds; amount with no unit → seconds (contract accepts unit-less); non-numeric amount or non-second unit → NULL with WARN.

### Step 6 — `fix(label_extractor)` — sitelink fallback

```rust
fn extract_label(entity) -> (Option<String>, Option<String>) {
    let label = entity.labels?.en() ?? sanitize_sitelink_title(entity.sitelinks?["enwiki"]?.title?);
    // sanitize_sitelink_title: '_' → ' '; keeps disambiguators (per §4 contract #2)
    ...
}
```

### Steps 7–8 — query regression + fixtures

- Add fixture JSON entities: album work (`P31=Q482994`, `P175=[artist]`), song work (`P31=Q736917`, `P361=[album]`, `P2047` quantity), group agent, person agent with `sitelinks`.
- Query tests: `query album --name "Joshua Tree"` returns the album; `query artist --name "U2"` returns the agent; album_artist join resolves U2's albums.
- Update-path tests (`update_test.rs`): a work entity upserted from the REST-entity shape lands in `album`/`track` with performer junctions — never in `artist`.
- **Update the existing integration expectations that encode the old inversion**: `tests/bootstrap_test.rs` (P175 → album stub + album_artist row), `query_test.rs`, and `update_test.rs` change their expected row counts/tables once works stop being agents.

### Step 9 — docs

Update ARCHITECTURE.md (roles, filter precedence, contracts, Limitations, **"Decision: English-only labels" → sitelink-fallback decision**, data-flow diagrams), README.md (pipeline semantics stay instance-agnostic; **correct the dump-size figures — the weekly dump is now ~155 GB, not ~35 GB**), AGENTS.md (module map/commands, dump size), and set this plan to `Status: Implemented` with closing commit hashes.

### Step 10 — real-dump verification run (no commit)

```bash
# Fresh DB — do NOT re-bootstrap into the contaminated file (INSERT OR IGNORE would keep bad rows)
cargo run --release -- bootstrap --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb --parquet-dir /tmp/tag_agent_parquet_v2
cargo run --release -- populate --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb --parquet-dir /tmp/tag_agent_parquet_v2
```

Timing note: bootstrap re-streams the 155 GB dump (~30–60 min). Do **not** reuse `/home/tr/wiki_db/parquet-dir` (it encodes the inverted schema) unless a fresh regeneration is intended; use a new dir. A backfill/FK-safe rebuild on the existing file is explored in §6 and rejected.

---

## 6. Migration Strategy (decided: fresh build, symlink swap)

- The old `music.duckdb` is contaminated at the row level (87,577 wrong ids in `album`, agents inside `artist`, dead junctions). Do **not** attempt an in-place repair — untangling roles retroactively is more error-prone than re-bootstrapping, and bootstrap is already resumable + idempotent.
- Build `music-v2.duckdb` fresh (§5 Step 10). On verification pass: `mv music.duckdb music-v1-inverted.duckdb && mv music-v2.duckdb music.duckdb` (or update `wiki_db.toml` `db` path — whichever the operator prefers; keep v1 file for audit).
- tag_agent integration note: `DbSearch` consumes only the DB; no repo changes needed on the tag_agent side beyond its existing "name-first" design. The whole point is that name-first queries now hit *real* albums/tracks.

---

## 7. Verification / Definition of Done

Run against `music-v2.duckdb`:

| # | Check | Command | Expect |
|---|-------|---------|--------|
| 1 | Role separation | `SELECT count(*) FROM album a JOIN artist ar ON a.id=ar.id` | ≈ 0 (allow tiny Wikidata oddities, <0.1%) |
| 2 | Real album searchable | `SELECT name FROM album WHERE name ILIKE '%joshua tree%'` | includes "The Joshua Tree" |
| 3 | Artist→albums | `SELECT al.name FROM album_artist aa JOIN artist ar ON ar.id=aa.artist_id JOIN album al ON al.id=aa.album_id WHERE ar.name='U2'` | non-empty, real U2 albums |
| 4 | track_album junction | `SELECT count(*) FROM track_album` | ≥ 7,000 (of 7,194 P361 rows) — **conditional** on track/album class coverage (see gate below). v3 measured **1,992** (kernel/agent rows moved out by Fix C); measured, accepted divergence — record, do not tune |
| 5 | Durations | `SELECT count(*) FROM track WHERE duration_seconds IS NOT NULL` | ≥ 100 (before: 0); spot-check a known song's duration — v3: **13,037** |
| 6 | release_date | `SELECT count(*) FROM album WHERE release_date IS NOT NULL` | ≥ 1,000 (≈5× the old 197); v3: **401,290** |
| 7 | Labels | `SELECT count(*) FROM album WHERE name ~ '^Q[0-9]+$'` | below the old 3,930 (sitelink fallback) — v3: **77,940** (77,939 label-less, 4.05× the v1 row count; accepted divergence, see verify doc §2.1) |
| 8 | Queries | `cargo run --release -- query album --name "Joshua Tree"` | hits; `query artist --name "U2"` → agent |
| 9 | Update path | `cargo run --release -- update --dry-run`; upsert a work entity via the REST-entity shape | lands in `album`/`track` with performer junctions, never `artist` (no inverted `album_artist` rows) |
| 10 | Quality | `cargo test && cargo clippy -- -D warnings && cargo fmt --check` | clean |

**Gate:** checks 2, 4, 5, 6 are only reachable once `ALBUM_WORK_CLASS_IDS` / `TRACK_WORK_CLASS_IDS` curation (Step 2, golden-corpus audit) is complete. If a material share of **known music works (albums or tracks)** still falls to the catch-all/agent path, extend the corresponding class list and re-run bootstrap before treating the battery as valid. Record the `inclusion_reason` distribution audit in the Step 10 run notes. (v3: gate passed — Q113111952 "40" lands in `track`, not `artist`, after the Q55850593 class fix.)

**Step 2 curation evidence (2026-09-29, `2026-09_verify_rebuild_fixes.md` §7 run notes):** the 51-work golden-corpus audit found zero album-class misses (all 25 albums → `Q482994`/`Q169930`) and one material song-class miss, `Q55850593` (music track with vocals, live COUNT ≈ 32 K) — added to `TRACK_WORK_CLASS_IDS` (U2 "40" Q113111952 is the §7 gate entity for the vox rebuild). The generic musical-work parent `Q105543609` (~208 K live) was **not** added: it is the catch-all that would re-unify the roles (see gate wording extension in `2026-09_verify_rebuild_fixes.md` §2.4/§8 — the condition covers album **and** track classes alike).

## 8. Risks / Pitfalls

- **P31 work-class coverage**: without P279 subclass resolution, any album class absent from the curated list silently falls to the catch-all (agent) path. Mitigation: unit-test the classifier against the known golden corpus (U2, Vivaldi, Ellington directories in `/home/tr/mp3_files`) and audit `inclusion_reason` distribution during Step 10.
- **Single location**: singles/EPs placement (album vs track) affects tag counts — must be decided and documented in Step 2 per the v1 schema intent (→ album).
- **`update` pipeline**: the incremental update path (`sparql.rs` → `upsert_entity_from_json`/`upsert_entity`) inserts single entities per-feature; it now reuses the same role classifier and role routing (Step 4) so increments don't re-introduce inverted rows. Covered by Step 8 update-path tests and DoD check 9.
- **Duration unit**: some P2047 claims may use non-second units (e.g. milliseconds Q1186222); contract says seconds-only → NULL + WARN. Revisit only if the dump shows material ms usage (log-driven).
- **Sitelink fallback changes name semantics**: names can now come from enwiki titles (underscores/redirect names). Sanitization is **decided** in §4 contract #2 (`_` → space, disambiguators kept) and unit-tested in Step 6.

## 9. Open Questions (raised during planning, answered 2026-09-26)

| # | Question | Decision |
|---|----------|----------|
| 1 | Scope of fix | **Full: code fixes + re-bootstrap** (fresh DB, symlink swap) — not just quick bugfixes |
| 2 | Plan location | Repo `docs/research/` (this file, house conventions) — user-confirmed over data-dir copy |
| 3 | Name fallback | **Add `enwiki` sitelink fallback** (contract change §4) |
| 4 | Duration source | **Dump P2047 only**, seconds unit; no external API |
| 5 | Singles/EPs role | → **album** (v1 schema intent); confirm during Step 2 with fixtures |