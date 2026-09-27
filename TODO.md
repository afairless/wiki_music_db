# Implementation Plan: Fix Album/Track Entity-Role Inversion and Finish Enrichment Coverage

Source: `docs/research/2026-09_fix_entity_role_inversion.md`

## Context

The `populate` phase resolved names but a **structural role inversion from bootstrap** makes the DB unable to answer "is this album/track real, and does this tag match?". Works (albums/songs) pass the catch-all filter via P175/P136, are emitted as artists, and `load_albums_and_tracks` creates album stubs from the P175 *performer* Q-IDs — so performer IDs land in `album` and works land in `artist`. Re-running `populate` cannot fix this; the defects are in model, filter, extraction, parquet, loader, and label-extractor layers. This plan fixes all layers, then does a fresh `music-v2.duckdb` build (no in-place repair: the old file is row-contaminated).

All plan claims verified against the working tree at `ce17737` (HEAD) before writing this TODO (model.rs, filter.rs, extraction.rs, parquet_writer.rs, db/load.rs, label_extractor.rs, schema.rs, bootstrap/query/update tests).

**Branch:** `agent/fix-entity-role-inversion`

### Design decisions resolved during planning (deviations/refinements vs. the source plan)

1. **Role transport (refines plan §5 Step 2).** The plan asks to "keep `is_music_entity` as a thin wrapper" *and* note roles in `inclusion_reason` ("ROLE:Album etc."), *and* keep all existing filter unit tests green. Existing tests assert exact reason strings (`"P106:Q639669"`, `"PROP:P1303,P136,P358"`), so encoding the role in the reason string is contradictory and pollutes the stored `inclusion_reason` column. **Decision:** `classify_entity(claims) -> Option<(EntityRole, String reason)>`; agent reasons stay byte-identical to today; `FilteredEntity` and `MusicEntity` each gain an explicit `role: EntityRole` field (plumbed in `stream.rs` and `upsert_entity_from_json`). `is_music_entity` stays as a thin `FilterResult` wrapper returning `Included(reason)` — all existing filter tests stay green; works match with reasons like `"P31:Q482994"`.
2. **P658 fate (underspecified in plan).** Today `extract_music_entity` maps P658 (an album's *tracklist*) into `tracks`. Under the corrected model, track rows come only from Track-role *works* (P31 ∈ `TRACK_WORK_CLASS_IDS`); P361 gives their parents. **Decision (default):** delete the P658 extraction path and the `TrackRef` type entirely — album tracklists are not a track source (deferred with track-number population). *Open for user veto:* keep P658 as a secondary track-stub source via `track_album`. See the approval question.
3. **Bootstrap naming (detail).** Old loader always writes stub `name = id`. **Decision:** Album/Track rows from work entities use `COALESCE(name, id)` from the parquet row (the work's own English label when present in the dump), so queries work pre-`populate`; the sitelink fallback still backfills the ~3,930 label-less residuals via `qid_label`.
4. **Fixed columns.** Parquet schema gets a new `role` column and the `tracks` JSON column is repurposed/renamed `parents` (JSON array of P361 album Q-IDs; empty for non-track rows). Writer + loader change together per step; old-format parquet is abandoned (fresh build), so no read-back-compat needed beyond a sane default for `role` (see pitfalls).
5. **Test distribution (refines plan §5 Step 8).** Tests broken by a behavior change are updated *in the step that breaks them* (incremental-development rule) — e.g. `bootstrap_test.rs` P175→album-stub expectations land with the loader step, `test_upsert_entity_with_albums_and_tracks` with the upsert step. Step 10 then adds the new work-entity fixtures and remaining expectation fixes.

## Step details

### Step 0 — Pre-work

```bash
git status            # clean except untracked docs/research/2026-09_fix_entity_role_inversion.md
cargo test && cargo clippy -- -D warnings && cargo fmt --check
git checkout -b agent/fix-entity-role-inversion
```

Commit the plan doc + this TODO before Step 1: `docs(research): plan entity-role inversion fix and enrichment completion`

### Step 1 — `fix(model)` — quantity and sitelinks in the entity model

`DatavalueValue` gains `#[serde(default)] amount: Option<String>` and `#[serde(default)] unit: Option<String>` (Wikidata quantity shape: `{ "amount": "+240", "unit": "http://www.wikidata.org/entity/Q11574" }`). `Entity` gains `#[serde(default)] pub sitelinks: Option<HashMap<String, Sitelink>>` + `pub struct Sitelink { pub title: String }`.

Tests: deserialize a quantity datavalue from the dump shape (`amount`/`unit` captured; existing id/time/precision paths unaffected); entity with `sitelinks: {"enwiki": {"title": "The Joshua Tree"}}`. All existing model tests stay green (serde `default` is backward compatible).

### Step 2 — `fix(filter)` — role classifier

Fork `is_music_entity` into `classify_entity`:

```rust
pub enum EntityRole { Agent, Album, Track }   // Debug, Clone, Copy, PartialEq, Eq

pub fn classify_entity(claims) -> Option<(EntityRole, String /* reason */)>
// precedence:
//   1. P106 ∈ MUSIC_OCCUPATION_IDS                    → (Agent, "P106:<qid>")
//   2. P31  ∈ MUSIC_GROUP_IDS                         → (Agent, "P31:<qid>")
//   3. P31  ∈ ALBUM_WORK_CLASS_IDS                    → (Album, "P31:<qid>")
//   4. P31  ∈ TRACK_WORK_CLASS_IDS                    → (Track, "P31:<qid>")
//   5. catch-all (P1303/P175/P136/P358 ≥1)            → Agent ONLY IF no work-class P31 present
//   6. otherwise                                      → None (excluded)
```

`is_music_entity(claims) -> FilterResult` becomes a thin wrapper (kept for the update path + tests). `FilteredEntity` gains `pub role: EntityRole`; wire it in `stream.rs` (line ~123) and the stream test helper. Existing agent reason strings are byte-identical → **all existing filter unit tests stay green**.

**Class curation (plan gate):** `ALBUM_WORK_CLASS_IDS` / `TRACK_WORK_CLASS_IDS` start from the verified `Q482994` (album) + `Q134556` (single); curate the rest (song, EP, compilation, live album, instrumental, …) by label lookups in the local DB's `qid_label` (old `music.duckdb` is fine — labels are role-independent) and spot-check known albums/songs from the golden corpus (`/home/tr/mp3_files` U2/Vivaldi/Ellington dirs). Document the final lists in code comments. Singles/EPs → `album` (v1 schema intent, question 5 answered).

Tests: album (P31=Q482994 + P175) → Album not artist; group → Agent; song → Track; non-music → excluded; P106 + work-class P31 → Agent (precedence); catch-all-only → Agent; work-class beats catch-all.

### Step 3 — `fix(extraction)` — works emit performers and parents, not inverted refs

`MusicEntity` gains `pub role: EntityRole` and `pub parent_album: Vec<String>` (P361 album Q-IDs, Track role only). `AlbumRef`/`TrackRef` merge into `PerformerRef { qid, role }` — the `albums` JSON column now holds performer refs (never work IDs on work rows). Role-aware claim mapping for `extract_music_entity`:

|Role|P175|P361|Emitted artifact|
|---|---|---|---|
|Agent|—|—|artist row (as today); no album stubs; existing member_of/instrument/genre extraction unchanged|
|Album|`PerformerRef` list|—|album row + `album_artist(album=me.id, artist=performer)`|
|Track|`PerformerRef` list|`parent_album` list|track row + `track_artist` + `track_album(track=me.id, album=parent)`|

P658 extraction path and `TrackRef` deleted (per decision #2 — vetoable). Rewrite `test_extract_tracks` (P658) and `test_extract_albums` accordingly; add unit tests: work with P175/P361 emits performer/parent refs — never inverted refs; agent does not absorb P175 as albums.

### Step 4 — `fix(parquet_writer)` — emit role and parents columns

`build_schema()` + `BatchAccumulators` + `append_entity` + `into_record_batch` gain `role` (Utf8, non-null, value from `entity.role`) and `parents` (Utf8 JSON array of entity.parent_album strings; back-compat default `"[]"`). `albums` JSON now serializes `PerformerRef` (key `qid`, not `album_id`/`track_id`). Old loader is untouched in this step and must still work on files the new writer emits (it reads named columns) — the writer step is independently testable via round-trip tests (including role/parents columns and album-ref JSON shape change in `test_round_trip_with_album_refs`).

### Step 5 — `fix(loader)` — route bootstrap rows by role

- `load_albums_and_tracks` splits on `role`: Album rows → `INSERT OR IGNORE INTO album (id, name)` from `role='Album'` rows with `COALESCE(name, id)` (decision #3); their `albums` JSON → `album_artist`; Track rows → `track` + `track_artist` from `albums` JSON + `track_album` (`track=me.id`, `album=parents` element, one row per parent). No more stubs from agent P175.
- `load_artists`, `load_artist_genre`, `load_artist_instrument`, `load_artist_member_of` gain `WHERE role = 'Agent'` — **required**, else Album/Track rows with P136 genres violate `REFERENCES artist(id)` in the FKs and `load_all` fails outright.
- Update `write_test_albums_tracks_parquet`-based / `bootstrap_test.rs` expectations broken by the change (P175 → album stub becomes performer junction; "Entity With All Catch-All Properties" roles).

Tests: mini parquet with role-mixed rows → correct tables; FK smiles; album/track count checks; `load_all` with albums+tracks still idempotent.

### Step 6 — `fix(loader)` — route update-path upserts by role

`upsert_entity_inner` branches on `entity.role`:
- Agent: existing artist + qid_label + genre/instrument/member_of upserts **plus** `INSERT OR IGNORE` on `artist_genre` (unchanged today).
- Album: `INSERT OR REPLACE INTO album (id, name)` from `entity.name` (or qid_label COALESCE) + `album_artist` from `PerformerRef`s — never into `artist`.
- Track: `track` row + `track_artist` + `track_album` (parents).

`upsert_entity_from_json` switches from `is_music_entity` to `classify_entity` and threads the role into the `FilteredEntity` it builds (main.rs:584 calls it unchanged). Rewrite `test_upsert_entity_with_albums_and_tracks` (old inverted semantics) and the upsert-path album/track label tests to the new routing; add an update-path test: a work entity upserted from the REST-entity shape lands in `album`/`track` with performer junctions, never `artist`.

### Step 7 — `fix(label_extractor)` — parse P2047 duration from datavalue amount

Rewrite `extract_p2047_duration` to read `claim.mainsnak?.datavalue?.amount` (trim leading `+`), validate the value parses as `i64`, and accept only when `unit` is `None` or ends with `Q11574` (seconds); otherwise WARN + NULL (contract §4 #3). Delete the dead `claim.extra.get("amount")` path **and** the bogus `dv.time` fallback. Rewrite `test_extract_claim_p2047` (asserts the dead path) and `test_extract_claim_p2047_invalid` (asserts the time fallback) to the `mainsnak.datavalue` shape: `+240`/unit Q11574 → 240; unitless → 240; missing amount / non-numeric / non-second unit → NULL.

### Step 8 — `fix(label_extractor)` — sitelink fallback to label extraction

`extract_label` falls back from `en` label to sanitized `enwiki` sitelink title. `sanitize_sitelink_title`: `_` → space; `(album)`/`(song)`-style disambiguators kept (decision: do not strip) — post-sanitization trim/empty → None. Contract §4 #2 (replaces the "English-only labels" limitation).

Tests: en label wins; no en label + enwiki → sanitized title (`The_Joshua_Tree` → `The Joshua Tree`); neither → None; underscore-only title → None.

### Step 9 — `fix(query)` — reaffirm artist/album searches over corrected roles

Verify `query artist/album/search` SQL against the corrected semantics; adjust only if role routing requires. Query tests (`query_test.rs`): "The Joshua Tree" resolvable via `query album`; `query artist --name "U2"` → agent with `album_artist` links; search surfaces tracks. Update any expectation encoding the old inversion.

### Step 10 — `test(db)` — fixtures for corrected role wiring and durations

Add `tests/fixtures/` entities (mirroring plan §5 Step 8): album work (`P31=Q482994` + `P175=[artist]`), song work (`P31` track class + `P361=[album]` + P2047 quantity datavalue), group agent, person agent with `sitelinks`; extend `mini_dump.json.gz` / bootstrap fixture JSON if needed. Update remaining integration expectations that encode the old inversion (`update_test.rs`, `query_test.rs`, `bootstrap_test.rs` album/track assertions). Add fixture-driven tests: P2047 → `track.duration_seconds`; P361 → `track_album`; role separation (`album.id` ∩ `artist.id` empty).

### Step 11 — Docs sync

ARCHITECTURE.md (entity roles + filter precedence, data-flow diagrams, Limitations, "Decision: English-only labels" → sitelink-fallback decision, duration contract), README.md (pipeline semantics; correct dump-size figure — weekly dump now ~155 GB, verify against `/home/tr/wiki_db/latest-all.json.gz` at implementation time), AGENTS.md (module map/commands, dump size). Set this plan's `Status:` header to `Implemented` with closing commit hashes.

### Step 12 — Real-dump verification (no commit)

```bash
cargo run --release -- bootstrap --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb --parquet-dir /tmp/tag_agent_parquet_v2
cargo run --release -- populate --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb --parquet-dir /tmp/tag_agent_parquet_v2
```

Run the plan §7 DoD battery: role separation ≈0; "The Joshua Tree" in album; U2 album_artist non-empty; `track_album` ≥ 7,000; `duration_seconds` ≥ 100; `release_date` ≥ 1,000; QID-mirror names below 3,930; `update --dry-run` + upsert sanity; `cargo test && cargo clippy && cargo fmt --check`. Record the `inclusion_reason` distribution audit. On pass: `mv music.duckdb music-v1-inverted.duckdb && mv music-v2.duckdb music.duckdb` (or point `wiki_db.toml` at v2).

## Steps

|#|Commit message|Logical unit|Key deliverables|Tests|
|---|---|---|---|---|
|0|`docs(research): plan entity-role inversion fix and enrichment completion`|Plan + TODO docs|`docs/research/2026-09_fix_entity_role_inversion.md`, `TODO.md`|—|
|1|`fix(model): capture amount/unit and sitelinks in Wikidata entity model`|Model extension|`src/wikidata/model.rs` (`DatavalueValue.amount/unit`, `Entity.sitelinks`, `Sitelink`)|Unit|
|2|`fix(filter): classify entities into agent/album/track roles`|Role classifier|`src/wikidata/filter.rs` (`EntityRole`, `classify_entity`, class-ID lists), `src/wikidata/stream.rs` (`FilteredEntity.role` wiring)|Unit|
|3|`fix(extraction): emit works with performers and parents instead of inverting`|Extraction|`src/extraction.rs` (`MusicEntity.role/parent_album`, `PerformerRef`, role-aware P175/P361, P658 removal)|Unit|
|4|`fix(parquet_writer): add role and parents columns to batch writer`|Parquet schema|`src/parquet_writer.rs` (`role`, `parents` columns; PerformerRef JSON)|Unit|
|5|`fix(loader): route bootstrap rows into album/track by role`|Bootstrap loader|`src/db/load.rs` (`load_albums_and_tracks`, `load_artists` + 3 join loaders role filter), `tests/bootstrap_test.rs`|Unit, Integration|
|6|`fix(loader): route update-path upserts by role`|Update upsert routing|`src/db/load.rs` (`upsert_entity_inner`, `upsert_entity_from_json`)|Unit, Integration|
|7|`fix(label_extractor): parse P2047 duration from datavalue amount`|Duration extraction|`src/label_extractor.rs` (`extract_p2047_duration`) + P2047 tests|Unit|
|8|`fix(label_extractor): add sitelink fallback to label extraction`|Label fallback|`src/label_extractor.rs` (`extract_label`, `sanitize_sitelink_title`)|Unit|
|9|`fix(query): reaffirm artist/album searches over corrected roles`|Query layer|`src/db/query.rs` (verify only), `tests/query_test.rs`|Integration|
|10|`test(db): fixtures for corrected role wiring and durations`|Test fixtures|`tests/fixtures/*`, `tests/bootstrap_test.rs`, `tests/update_test.rs`, `tests/query_test.rs`|Integration|
|11|`docs: document entity roles, sitelink fallback, and the fixed enrichment contract`|Docs sync|`docs/ARCHITECTURE.md`, `README.md`, `AGENTS.md`, research status headers|—|
|12|*(verification — no commit)*|Real-dump re-run|`music-v2.duckdb` + plan §7 battery, symlink swap|—|

## Pitfalls to watch

- **Role column read-back:** old-format parquet (no `role`) must not be fed to the new loader; fresh `--parquet-dir` (`/tmp/tag_agent_parquet_v2`) — do **not** reuse `/home/tr/wiki_db/parquet-dir` (encodes the inverted schema).
- **Artist FK cascade:** forgetting the `WHERE role='Agent'` filter on any artist-side loader breaks `load_all` on the schema FKs — verify the full suite per step.
- **Class coverage gate:** a real album whose P31 class is missing from `ALBUM_WORK_CLASS_IDS` silently falls to the catch-all (agent) path — DoD checks 2/4/5/6 are only valid once class curation is complete.
- **update re-introduction:** without Step 6 routing, `update` re-inserts inverted rows into a fixed database — DoD check 9 covers it.
- **Enrichment-only tables are untouched:** `extract_labels_and_claims` selects album/track enrichment via `qid_sets` from the DB tables — once bootstrap holds real works, P577/P264/P136/P361/P2047 enrichment automatically applies to real rows (FK guards now resolve); no code change expected, confirm in Step 5/12.