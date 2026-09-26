# Research: Sync README, ARCHITECTURE, and AGENTS with Current Codebase State

**Date:** 2026-09
**Status:** Plan
**References:**

- [ARCHITECTURE.md](../ARCHITECTURE.md) — architecture document in need of refresh
- [README.md](../../README.md) — end-user documentation in need of refresh
- [AGENTS.md](../../AGENTS.md) — agent instructions in need of refresh
- [2026-08_populate_names_research.md](./2026-08_populate_names_research.md) — origin of the `populate` phase

---

## 1. Problem Statement

The codebase has grown substantially beyond what the documentation describes. The `populate` subcommand (name resolution) and the incremental-update pipeline (Phase 7) were implemented after the last documentation pass, and several maintenance fixes landed afterward. As a result:

- **`README.md`** never mentions the `populate` subcommand; its "Limitations" section still claims album/track name resolution is *deferred*.
- **`docs/ARCHITECTURE.md`** documents a 12-table, schema-v1 database; the code now creates **16 tables** at **schema v2**, defines six CLI subcommands (not three), and has two additional pipeline stages (populate, update) that are absent from the module map and data-flow diagrams.
- **`AGENTS.md`** states Tokio is "not yet used in production code" (it is, in `src/sparql.rs`), lists an incomplete command table, and its project-structure/test listings omit several modules and test files.
- **Research docs** all carry `Status: Plan` even though their plans have been committed; a stale status header makes it impossible to tell what has actually been implemented.

Root cause: a process gap — document-update steps in previous plans were either skipped or never enshrined in a plan. This plan fixes both the drift and the process (research docs get an explicit status update when implemented).

The documentation must also be **instance-agnostic**: it should describe the pipeline and its stage semantics — not the state of any particular database file or path — so that someone can take the code in this repo and create a database from scratch following the docs, or consult the docs to understand how an existing database was built.

**Verified evidence** (all checked against the working tree at commit `a8495b2`):

| Artifact | Value |
|---|---|
| `SCHEMA_VERSION` (`src/db/schema.rs:7`) | `2` |
| Tables created (`CREATE_TABLE_STATEMENTS`) | 16: `schema_version`, `artist`, `genre`, `artist_genre`, `album`, `album_artist`, `album_genre`, `track`, `track_album`, `track_artist`, `artist_instrument`, `artist_member_of`, `sync_state`, `qid_label`, `instrument`, `record_label` |
| CLI subcommands (`Command` enum, `src/cli/mod.rs`) | `download`, `bootstrap`, `update`, `query`, `populate`, `completion` |
| Production use of Tokio | Yes — `src/sparql.rs` wraps an async `reqwest::Client` in a Tokio runtime |
| Production modules (`src/lib.rs`) | `cli`, `config`, `db`, `error`, `extraction`, `label_extractor`, `parquet_writer`, `sparql`, `wikidata` |
| Test files (`tests/`) | `bootstrap_test.rs`, `stream_test.rs`, `filter_tests.rs`, `query_test.rs`, `update_test.rs` |
| `populate` flags | `--dump -d`, `--db`, `--parquet-dir`, `--resume`, `--force`, plus globals |
| `download` flags | `--output`, `--force`, `--quiet` |
| `update` flags | `--since`, `--dry-run` |
| Config subsections (`src/config.rs`) | `[download]` (`url`, `output`, `user_agent`, `quiet`), `[update]` (`since`, `dry_run`) |

---

## 2. Discrepancy Inventory

### 2.1 `README.md`

| # | Location | Current claim | Reality / fix |
|---|---|---|---|
| R1 | Features → "Normalized schema" **and** the `## Database Schema` section | "12-table relational database" bullet + 12-table listing | 16 tables at schema v2. Fix **both** spots: update the Features bullet, and add `instrument`, `record_label`, `qid_label`, `sync_state` to the `## Database Schema` listing. |
| R2 | Pipeline, Steps 1–4 | No mention of `populate`; "That's it" after bootstrap | Insert a `populate` step (name resolution) between bootstrap and query. Fresh bootstrap DBs have Q-ID placeholders in `album.name`/`track.name` until `populate` runs. |
| R3 | Step 3 — Query | Album output described as showing "full track listing with durations" | True only *after* `populate`; otherwise track names are Q-IDs and `duration_seconds` is NULL. Add a note. |
| R4 | Command Reference | No `populate` block | Add `populate` usage: `--dump`, `--db`, `--parquet-dir`, `--resume`, `--force`; note it requires the dump on disk and re-scans it (~tens of minutes). |
| R5 | Configuration → "Full config reference" table | `dump` used by `download`/`bootstrap`; `db` by `bootstrap`; no `[download]` / `[update]` subsections | `dump`: add `populate`. `db`: add `query`, `update`, `populate`. `parquet_dir`/`resume`: add `populate`. Document the `[download]` (`url`, `output`, `user_agent`, `quiet`) and `[update]` (`since`, `dry_run`) subsections that `wiki_db.toml` already ships. |
| R6 | Limitations → "Album/track names" | "Use Wikidata Q-ID placeholders — actual name resolution is deferred" | Resolution is *implemented* (`populate`); reframe as "names are Q-IDs until `populate` is run". |
| R7 | Feature list (whole bullet list) | Doesn't mention FTS search or name resolution | Mention FTS search, incremental updates, and name resolution as features. |
| R8 | Dependencies → `tokio` + `reqwest` row | "Async runtime (Phase 7, not yet used)" | Tokio is used by `src/sparql.rs` (update pipeline SPARQL client); reword to match. |

### 2.2 `docs/ARCHITECTURE.md`

| # | Location | Current claim | Reality / fix |
|---|---|---|---|
| A1 | Module map — CLI layer box | "subcommands: bootstrap, update, query" | Six: `download`, `bootstrap`, `update`, `query`, `populate`, `completion` |
| A2 | Module map — Data Layer box | 12-table list ending at `schema_version` | 16 tables; add `sync_state`, `qid_label`, `instrument`, `record_label`. |
| A3 | Module responsibilities table | Lists 9 modules; orchestration = `cmd_bootstrap` only | Add `config.rs`, `sparql.rs`, `label_extractor.rs`, `cli/download.rs`, `cli/populate.rs`, `cli/update.rs`, `db/query.rs`. Orchestration row: `cmd_bootstrap`, `cmd_update`, `cmd_populate`, `cmd_query`, `cmd_download`. |
| A4 | "Data Flow: Bootstrap" section | Only bootstrap documented | Add "Data Flow: Populate" (collect Q-IDs from DB → scan dump for labels/claims → `labels.parquet` + `enrichment.parquet` → FK-safe backfill via `backfill_all_safe`) and "Data Flow: Update" (SPARQL modified-entities query → REST entity fetch → `upsert_entity_from_json` → `sync_state`). |
| A5 | Decision: "Album and track names use Q-ID placeholders" | Listed as active; "future phase can resolve" | Mark as superseded; the `populate` phase (2026-08) resolves names. |
| A6 | Database Schema section | "Tables (12 total)", "schema_version (currently v1)" | 16 tables; `SCHEMA_VERSION = 2`. Add new tables with columns: `sync_state(key,value)`, `qid_label(qid,label,description,updated_at)`, `instrument(id,name)`, `record_label(id,name)`. Note `album.release_date`/`record_label` and `track.duration_seconds` exist but are NULL until `populate`. |
| A7 | Completed Phases | Only Phases 5–8 listed | Add the populate/name-resolution phase and the FK-safety maintenance phase (see §2.4). |
| A8 | Future Work | "Album and track name resolution (second pass or SPARQL)" (done), "Record label reference table (v2…)" (table now exists) | Remove name resolution. Rephrase record-label item: `record_label` table exists but `album.record_label` is stored as bare TEXT and is not yet normalized against it. Keep: multilingual labels, mid-stream resume, subclass resolution. |
| A9 | Limitations | "No album/track names … deferred" | Reframe under A6/A5: names available after `populate`; fresh bootstrap DBs show Q-IDs. |
| A10 | Module map / schema "12 tables" wording in any other spot | — | Grep for `12 tables` / `v1` and fix every *stale schema* occurrence. Do **not** touch legitimate ones: "12 VARCHAR columns" (the flat Parquet schema still has 12 columns), "12 hardcoded music group Q-IDs" (`filter.rs`), the historical heading "Decision: Flat VARCHAR schema for Parquet (v1)", or README's "Resume v1" limitation label. |

### 2.3 `AGENTS.md`

| # | Location | Current claim | Reality / fix |
|---|---|---|---|
| G1 | Stack → Async runtime | "Tokio 1.x (…; not yet used in production code — deferred to Phase 7 incremental updates)" | Used in `src/sparql.rs` (async `reqwest` client wrapped in a Tokio runtime) for `update`. |
| G2 | Project Structure | `src/cli/` "(bootstrap, update, query)"; no `config.rs`, `sparql.rs`, `label_extractor.rs`; tests list only 3 files | Add `download.rs`, `populate.rs` to `src/cli/`; add `config.rs`, `sparql.rs`, `label_extractor.rs`; add `query_test.rs`, `update_test.rs` to tests. |
| G3 | Commands table | `bootstrap`, `update`, `query` only | Add `download` (resumable dump downloader), `populate` (resolve names/dates/labels from dump), `completion` (shell completions). |
| G4 | Current Implementation State | Ends at "Phase 8: Polish & distribution" | Add the name-resolution phase (post-Phase 8) and the FK-safety/backfill maintenance phase; update the schema description to 16 tables / v2. |
| G5 | Documentation bullet | "TODO.md — the current implementation plan (Phase 8 polish & distribution)" | TODO.md currently holds the (completed) FK-backfill fix plan; reword to "current implementation plan (see TODO.md); completed plans are archived in docs/research/". |

### 2.4 Research docs — stale status headers

All fixed by appending an `Implemented` status line with the closing commits. Optional detail: git log --oneline --reverse output to cite closes.

| Doc | Current status | Closing commits (verified in `git log`) |
|---|---|---|
| `2026-08_populate_names_research.md` | Reviewed / Plan | populate subcommand landed; perf commit `0392d4b` |
| `2026-08_populate_performance_optimization.md` | Plan | `0392d4b` (Aho-Corasick) |
| `2026-08_fix_artist_backfill_fk_violation.md` | Plan | `1793ec0`, `a8495b2` |
| `2026-08_fix_backfill_album_name_fk_violation.md` | Plan | `b870d31`, `1094283` |
| `2026-08_fix_backfill_fk_safe_emptying.md` | Plan | `989d133`, `1094283` |
| `2026-08_fix_enrichment_fk_album_track_guards.md` | Plan | `33eef88`, `ea66274` |
| `2026-08_fix_enrichment_fk_constraints.md` | Plan | `0f67175`, `34207d4` |

> Commit-hint correction: the constraints work (`645007f` plan → `0f67175` + `34207d4` fixes) was closed before the guards follow-up (`a69a87b` plan → `33eef88` fix + `ea66274` tests). `a69a87b` is the *plan-doc* commit for the guards feature, not a closing commit of the constraints doc; do not cite plan-doc commits as closers. Verify against `git log` at implementation time.

Historical design docs (`2026-07_music_db_options.md`, `2026-07_music_db_rust_plan.md`, `2026-07_wiki_db_dump.md`) keep their original statuses, but `2026-07_wiki_db_dump.md` (Option-3 analysis) should gain a one-line "Adopted → see rust plan" pointer. `2026-07_music_db_rust_plan.md` gets a status note that Phases 1–8 are complete and the populate phase was added after Phase 8.

### 2.5 Process improvement

To prevent recurrence, add to the plan a convention note (documented in `AGENTS.md` and the new research docs):

- Every research doc that gets implemented must have its `Status:` header updated to `Implemented` (with commit references) in the same commit series that closes the plan.
- Doc-update steps (`README`, `ARCHITECTURE`, `AGENTS`) are mandatory plan steps, not optional follow-ups.
- Documentation is **instance-agnostic**: describe pipeline stage semantics (bootstrap seeds Q-ID placeholders → populate resolves names → query surfaces them), never the state of a particular database file or path. A fresh bootstrap always produces Q-ID placeholders; `populate` always resolves them. A reader must be able to (a) build a database from scratch following the docs, or (b) understand how an existing database was created.

---

## 3. Approach

Straightforward: four doc-only commits, each touching one document body, plus a final verification pass. No code changes. Follows the repo's conventional-commit style (`docs(scope): subject`).

Branch: `agent/docs-sync-codebase`.

Files touched (all under `docs/` or project root — within AGENTS.md edit policy):

- `README.md`
- `docs/ARCHITECTURE.md`
- `AGENTS.md`
- `docs/research/*.md` (status headers)

## 4. Steps

| # | Commit message | Logical unit | Key deliverables |
|---|---|---|---|
| 1 | `docs(readme): document populate subcommand, 16-table schema, and updated limitations` | README refresh | R1–R8 resolved |
| 2 | `docs(architecture): refresh module map, schema, data flow, phases, and limitations` | ARCHITECTURE refresh | A1–A10 resolved |
| 3 | `docs(agents): sync project structure, commands, stack, and implementation state` | AGENTS refresh | G1–G5 resolved |
| 4 | `docs(research): mark implemented plans with status and closing commits` | Research status headers | §2.4 closed; add process convention note |
| 5 | *(verification)* Grep-based stale-claim scan + render/link check | Verify | §6 passes |

**Step order rationale:** README first (user-facing delta), then ARCHITECTURE (structural), then AGENTS (agent instructions), then research statuses last (they reference commits already in history).

## 5. Step details

### Step 1 — `docs(readme): document populate subcommand, 16-table schema, and updated limitations`

**Rationale:** README is the primary user-facing doc and currently omits an entire subcommand.

1. **Features** (R1, R7): change "12-table relational database" to "16-table relational database"; extend the bullet list with full-text search, incremental updates, and `populate` name resolution.
2. **Pipeline** (R2, R3): retitle the section heading (currently "Pipeline: Download → Import") to reflect the full chain (e.g., "Pipeline: Download → Bootstrap → Populate → Query"); reword the intro note "The next two commands are all you need for a complete build" to cover three commands (download, bootstrap, populate). Insert a step after bootstrap:

   ```bash
   # Step 3 — Resolve album/track names (optional but recommended)
   cargo run --release -- populate
   ```

   Renumber Query → Step 4, Update → Step 5. Add a short note: before `populate`, `album.name` and `track.name` are Q-ID placeholders (e.g., `Q1636124`); after it, real English labels, plus `release_date`, `record_label`, and `duration_seconds`.
3. **Command Reference** (R4): add a `populate` block mirroring the existing ones:

   ```text
   cargo run --release -- populate [OPTIONS]

   Options:
     --dump <PATH>          Path to the Wikidata JSON dump (gzipped) [from wiki_db.toml]
     --db <PATH>            Path to the DuckDB database (default: music.duckdb)
     --parquet-dir <PATH>   Directory for intermediate Parquet files
     --resume               Skip dump re-scan when Parquet files already exist
     --force                Re-populate even if names are already resolved
   ```

   Note: requires `bootstrap` output at schema v2; skips with a message when all names are already resolved.
4. **Config reference** (R5): update the "Used by" column — `dump`: download, bootstrap, populate; `db`: bootstrap, query, update, populate; `parquet_dir`, `resume`: bootstrap, populate. Add `[download]` and `[update]` subsection tables matching `src/config.rs` (`url`, `output`, `user_agent`, `quiet`; `since`, `dry_run`).
5. **Limitations** (R6): replace the "Q-ID placeholders / deferred" bullet with "Album/track names are Q-ID placeholders until `populate` is run; duration/dates are NULL until then."
6. **Dependencies** (R8): reword the `tokio` + `reqwest` row ("Async runtime (Phase 7, not yet used)") to reflect that `src/sparql.rs` wraps the async `reqwest` client in a Tokio runtime for the update pipeline.

**Commit:** `docs(readme): document populate subcommand, 16-table schema, and updated limitations`

### Step 2 — `docs(architecture): refresh module map, schema, data flow, phases, and limitations`

**Rationale:** ARCHITECTURE is the structural reference and is ~5 phases stale.

1. **Module map** (A1, A2): update the CLI-layer box to the six subcommands; update the Data-Layer box to the 16 tables.
2. **Module responsibilities table** (A3): add rows for `config.rs` (TOML config load/expand-tilde), `sparql.rs` (SPARQL query builder + HTTP client via async reqwest), `label_extractor.rs` (Aho-Corasick label/claim scan during populate), `cli/populate.rs` and `cli/download.rs` (argument definitions), `db/query.rs` (search + detail queries), `db/schema.rs` (16 tables, v2). Update the Orchestration row.
3. **Data flow** (A4): add the Populate and Update flow diagrams (keep ASCII style):
   - Populate: `album/track/artist Q-IDs from DB` (`collect_qid_set`) → `extract_labels_and_claims (dump rescan)` → `labels.parquet` + `enrichment.parquet` → `load_label_and_enrichment` (loads `qid_label`, `instrument`, `record_label`, and child tables, then **calls** `backfill_all_safe` — FK-safe temp-table swap across all seven child tables) → resolved names/dates/labels in `album`/`track`/etc.
   - Update: `sync_state.last_sync` → `build_modified_query (SPARQL)` → parse Q-IDs → `fetch_entity (REST)` → `upsert_entity_from_json` → `update_sync_state`.
4. **Decision A5**: retitle to "Album and track names use Q-ID placeholders (until populate)" and mark the decision superseded by the populate phase; keep the original rationale for history.
5. **Schema section** (A6): "Tables (16 total)"; `schema_version` = v2; add the four new tables; note `release_date`/`record_label`/`duration_seconds` columns exist and are filled by `populate`.
6. **Phases & future work** (A7, A8): add "Phase 9: Populate & name resolution" and "Phase 10: FK-safety hardening" as completed; move name resolution out of Future Work; reword the record-label item to note the lookup table exists but `album.record_label` is not yet normalized against it.
7. **Limitations** (A9): reframe per Step 1.
8. **Scan** (A10): `grep -n "12 tables\|12-table\|currently v1" docs/ARCHITECTURE.md` and fix only the DB-schema count / schema-version claims. Leave legitimate "12"s alone: "12 VARCHAR columns" (flat Parquet schema still has 12 columns — see `parquet_writer.rs`), "12 hardcoded music group Q-IDs" (true in `filter.rs`), and the historical "(v1)" heading on the Parquet decision.

**Commit:** `docs(architecture): refresh module map, schema, data flow, phases, and limitations`

### Step 3 — `docs(agents): sync project structure, commands, stack, and implementation state`

**Rationale:** AGENTS is the working contract for agents; stale claims here cause wrong work.

1. **Stack** (G1): change the Tokio bullet to "Tokio 1.x — used by `src/sparql.rs` (update pipeline) to wrap the async `reqwest` client in a runtime".
2. **Project structure** (G2): update `src/cli/` to `(bootstrap, download, populate, query, update)`; add `config.rs`, `label_extractor.rs`, `sparql.rs` lines; add `src/db/query.rs`; add `tests/query_test.rs`, `tests/update_test.rs`.
3. **Commands table** (G3): add rows:
   | `download` | Resumable dump downloader with MD5 verification |
   | `populate` | Resolve Q-ID placeholders: album/track names, dates, labels |
   | `completion` | Generate shell completion scripts |
4. **Implementation state** (G4): add the populate and FK-safety phases; state schema v2 / 16 tables; mark Phase 7 complete (already listed but now reflects Tokio use).
5. **Documentation section** (G5): reword the TODO.md bullet; add the documentation-process convention from §2.5 as a short "Documentation conventions" subsection. This is the **single canonical location** for the convention — Step 4 only references it and must not restate it.

**Commit:** `docs(agents): sync project structure, commands, stack, and implementation state`

### Step 4 — `docs(research): mark implemented plans with status and closing commits`

**Rationale:** Status headers are the index by which future agents distinguish done plans from open ones.

1. For the seven docs in §2.4, change `Status:` to `Implemented` and append a line:

   `**Implemented:** 2026-08 — closes with commits <hash…> (see git log).`
   Pull the exact closing commit hashes from `git log` at implementation time (table in §2.4 gives per-doc hints).
2. Add an "Adopted" pointer to `2026-07_wiki_db_dump.md` and a "Phases 1–8 complete; populate phase added after Phase 8" note to `2026-07_music_db_rust_plan.md`.
3. Reference the "Documentation conventions" subsection added to `AGENTS.md` in Step 3 (§2.5) — do not restate the convention here. Append a one-line note that this doc's own status update follows that convention.

**Commit:** `docs(research): mark implemented plans with status and closing commits`

### Step 5 — Verification (no commit)

Run the following; fix anything that fails **in the relevant earlier step's commit** (or a follow-up `docs:` fix commit) before considering the plan complete:

```bash
# 1. Stale-claim scan — all must return no matches
grep -rn "12-table\|12 tables" README.md docs/    || true
grep -n "currently v1" docs/ARCHITECTURE.md       || true
grep -rn "not yet used in production" AGENTS.md   || true
grep -rn "not yet used" README.md                 || true
grep -rn "two commands" README.md docs/           || true
grep -rn "name resolution is deferred\|resolution is deferred" README.md docs/  || true

# 2. Positive coverage — all must match
grep -n "populate" README.md | head
grep -n "Populate\|populate" docs/ARCHITECTURE.md | head
grep -n "populate" AGENTS.md | head
grep -c "Status: Implemented" docs/research/*.md

# 3. Markdown sanity — links resolve, headers/ASCII diagrams intact
cargo run --release -- completion bash --output /tmp/compcheck   # binary unaffected, still builds
git diff --stat   # docs-only changes expected: no src/, tests/, scripts/ files
```

Also verify the documented pipeline invariant on a **scratch** database — never on an existing database file. The docs must describe the pipeline's stage semantics, not the state of any particular DB instance:

```bash
# Build a throwaway DB and confirm the documented before/after behavior
cargo run --release -- bootstrap --dump <small-dump> --db /tmp/wiki_db_verify.duckdb
duckdb /tmp/wiki_db_verify.duckdb "SELECT COUNT(*) FROM album WHERE name = id;"
#   expect > 0 → bootstrap seeds Q-ID placeholders (album.name = album.id)
cargo run --release -- populate --dump <small-dump> --db /tmp/wiki_db_verify.duckdb
duckdb /tmp/wiki_db_verify.duckdb "SELECT COUNT(*) FROM album WHERE name = id;"
#   expect 0 → populate resolves real names
rm -f /tmp/wiki_db_verify.duckdb
```

`<small-dump>` may be `tests/fixtures/mini_dump.json.gz` if it contains label entities for the album/track Q-IDs the fixture references; otherwise use any small gzipped slice of the real dump containing those entities. This check is **optional** — the hard gate is `cargo test` (bootstrap/load tests build and verify scratch databases programmatically); it exists to keep the docs' stage-semantics claims honest without ever depending on a particular existing database file.

## 6. Definition of Done

- No stale claims from the inventory in §2 remain (`grep` scans above pass).
- `populate` is discoverable from README (command reference), ARCHITECTURE (data flow + schema + decisions), and AGENTS (commands table + structure).
- All researched-and-implemented plans carry `Status: Implemented` with closing commits.
- `cargo test`, `cargo clippy -- -D warnings`, and `cargo fmt --check` still pass (no code touched, but run anyway).
- One commit per step in §4, each self-contained and following conventional commits.

## 7. Out of scope

- Operating on any particular database file — e.g., deciding whether an existing local DB is up to date, or running `populate` against one. The docs describe the pipeline stages; database instances are incidental and intentionally not pinned in the documentation.
- Adding a `query track` subcommand or improving query output (separate feature; note in README that track titles are reached via `query search`).
- Extending the README config examples beyond the reference tables (cosmetic, optional follow-up).
