# Implementation Plan: Sync README, ARCHITECTURE, and AGENTS with Current Codebase State

Source: `docs/research/2026-09_docs_sync_with_codebase.md`

## Context

The codebase has grown beyond what the docs describe: the `populate` subcommand (name resolution), the Phase 7 incremental-update pipeline (`src/sparql.rs` uses Tokio), and a series of FK-safety/backfill maintenance fixes all landed after the last documentation pass. The docs still claim a 12-table schema-v1 database, three CLI subcommands, deferred name resolution, and unused Tokio. This plan fixes the drift in four doc-only commits and enshrines a process convention: research docs get `Status: Implemented` + closing commits, doc-update steps are mandatory, and documentation stays instance-agnostic (pipeline stage semantics, never the state of a particular database file).

All evidence claims verified against the working tree at `a8495b2` (HEAD). **No code changes** — this plan touches only `README.md`, `docs/ARCHITECTURE.md`, `AGENTS.md`, and `docs/research/*.md`.

**Branch:** `agent/docs-sync-codebase`

| # | Commit message | Logical unit | Key deliverables | Tests |
|---|---|---|---|---|
| 1 | `docs(readme): document populate subcommand, 16-table schema, and updated limitations` | README refresh | `README.md` (R1–R8) | — (grep spot-checks) |
| 2 | `docs(architecture): refresh module map, schema, data flow, phases, and limitations` | ARCHITECTURE refresh | `docs/ARCHITECTURE.md` (A1–A10) | — (grep spot-checks) |
| 3 | `docs(agents): sync project structure, commands, stack, and implementation state` | AGENTS refresh | `AGENTS.md` (G1–G5) + documentation-conventions subsection | — (grep spot-checks) |
| 4 | `docs(research): mark implemented plans with status and closing commits` | Research status headers | `docs/research/2026-08_*.md` (×7), `2026-07_wiki_db_dump.md`, `2026-07_music_db_rust_plan.md` | — (grep count) |
| 5 | *(verification — no commit)* | Stale-claim scan + full checks | §6 of plan: grep scans, `cargo test`, clippy, fmt, `git diff --stat` | — |

## Step details

### Step 0 — Pre-work

Verify workspace health, then create the feature branch.

```bash
git status            # clean except untracked docs/research/2026-09_docs_sync_with_codebase.md
cargo test
cargo clippy -- -D warnings
cargo fmt --check
git checkout -b agent/docs-sync-codebase
```

The plan document `docs/research/2026-09_docs_sync_with_codebase.md` is untracked — it is committed together with `TODO.md` as the plan-doc commit before Step 1 (branch `agent/docs-sync-codebase`).

### Step 1 — `docs(readme): document populate subcommand, 16-table schema, and updated limitations`

**Rationale:** README is the primary user-facing doc and omits an entire subcommand; its Limitations claim name resolution is deferred.

Kill all of R1–R8 from plan §2.1 in `README.md`:

1. **Features** (R1, R7): "12-table relational database" → "16-table"; extend bullets with full-text search, incremental updates, and `populate` name resolution.
2. **Pipeline** (R2, R3): retitle "Pipeline: Download → Import" → "Pipeline: Download → Bootstrap → Populate → Query"; reword the intro note from "two commands" to three; insert the `populate` step after bootstrap; renumber Query → Step 4, Update → Step 5; add the Q-ID-placeholder note (`album.name`/`track.name` are Q-IDs, `release_date`/`record_label`/`duration_seconds` NULL until `populate`).
3. **Command Reference** (R4): add a `populate` block (`--dump`, `--db`, `--parquet-dir`, `--resume`, `--force`) mirroring existing blocks; note it needs a bootstrap'd schema-v2 DB, requires the dump on disk, re-scans it (~tens of minutes), skips when names already resolved.
4. **Config reference** (R5): "Used by" column — `dump`: download, bootstrap, populate; `db`: bootstrap, query, update, populate; `parquet_dir`/`resume`: bootstrap, populate. Add `[download]` (`url`, `output`, `user_agent`, `quiet`) and `[update]` (`since`, `dry_run`) subsection tables matching `src/config.rs`.
5. **Limitations** (R6): replace the "deferred" bullet with "names are Q-ID placeholders until `populate` is run".
6. **Dependencies** (R8): reword the `tokio` + `reqwest` row — Tokio wraps the async `reqwest` client in `src/sparql.rs` for the update pipeline.

**Do NOT touch the legitimate "Resume v1" limitation label** (A10 carve-out).

**Commit:** `docs(readme): document populate subcommand, 16-table schema, and updated limitations`

### Step 2 — `docs(architecture): refresh module map, schema, data flow, phases, and limitations`

**Rationale:** ARCHITECTURE is the structural reference and is ~5 phases stale.

Kill all of A1–A10 from plan §2.2 in `docs/ARCHITECTURE.md`:

1. **Module map** (A1, A2): CLI-layer box → six subcommands (`download`, `bootstrap`, `update`, `query`, `populate`, `completion`); Data-Layer box → 16 tables (add `sync_state`, `qid_label`, `instrument`, `record_label`).
2. **Module responsibilities table** (A3): add `config.rs`, `sparql.rs`, `label_extractor.rs`, `cli/download.rs`, `cli/populate.rs`, `cli/update.rs`, `db/query.rs`; orchestration row → `cmd_bootstrap`, `cmd_update`, `cmd_populate`, `cmd_query`, `cmd_download`.
3. **Data flow** (A4): add "Data Flow: Populate" (`collect_qid_set` → dump rescan → `labels.parquet` + `enrichment.parquet` → `load_label_and_enrichment` → FK-safe `backfill_all_safe` across all seven child tables) and "Data Flow: Update" (`sync_state.last_sync` → SPARQL modified-entities query → REST entity fetch → `upsert_entity_from_json` → `update_sync_state`), ASCII style.
4. **Decision A5**: retitle to "(until populate)", mark superseded by the populate phase, keep rationale for history.
5. **Schema section** (A6): "Tables (16 total)"; `schema_version` v2; add the four new tables with columns; note `release_date`/`record_label`/`duration_seconds` exist but are NULL until `populate`.
6. **Phases & future work** (A7, A8): add "Phase 9: Populate & name resolution" and "Phase 10: FK-safety hardening" as completed; remove name resolution from Future Work; reword record-label item (table exists, `album.record_label` not yet normalized against it); keep multilingual labels, mid-stream resume, subclass resolution.
7. **Limitations** (A9): reframe — names available after `populate`; fresh bootstrap DBs show Q-IDs.
8. **Scan** (A10): `grep -n "12 tables\|12-table\|currently v1" docs/ARCHITECTURE.md`; fix only stale schema counts/version claims.

**Leave legitimate "12"s untouched:** "12 VARCHAR columns" (flat Parquet schema in `parquet_writer.rs`), "12 hardcoded music group Q-IDs" (`filter.rs`), historical "(v1)" heading on the Parquet decision.

**Commit:** `docs(architecture): refresh module map, schema, data flow, phases, and limitations`

### Step 3 — `docs(agents): sync project structure, commands, stack, and implementation state`

**Rationale:** AGENTS is the working contract for agents; stale claims cause wrong work.

Kill all of G1–G5 from plan §2.3 in `AGENTS.md`:

1. **Stack** (G1): Tokio bullet → "used by `src/sparql.rs` (update pipeline) to wrap the async `reqwest` client in a runtime".
2. **Project structure** (G2): `src/cli/` → (bootstrap, download, populate, query, update); add `config.rs`, `label_extractor.rs`, `sparql.rs`; add `src/db/query.rs`; add `tests/query_test.rs`, `tests/update_test.rs`.
3. **Commands table** (G3): add `download` (resumable dump downloader with MD5 verification), `populate` (resolve Q-ID placeholders), `completion` (shell completions).
4. **Implementation state** (G4): add the populate and FK-safety phases; schema v2 / 16 tables; Phase 7 now reflects Tokio use.
5. **Documentation** (G5): reword the TODO.md bullet; add a **"Documentation conventions"** subsection — the single canonical location for the §2.5 process convention (research docs get `Status: Implemented` + closing commits; doc-update steps mandatory; instance-agnostic pipeline semantics). Steps 4 only references it; must not restate it.

**Commit:** `docs(agents): sync project structure, commands, stack, and implementation state`

### Step 4 — `docs(research): mark implemented plans with status and closing commits`

**Rationale:** Status headers are the index by which future agents distinguish done plans from open ones.

Per plan §2.4:

1. For the seven 2026-08 docs, change `Status:` → `Implemented` and append `**Implemented:** 2026-08 — closes with commits <hash…> (see git log).` Pull exact hashes from `git log` at implementation time (plan §2.4 gives per-doc hints; do NOT cite plan-doc commits like `a69a87b` as closers — verify).
   - `2026-08_populate_names_research.md` (perf commit `0392d4b`)
   - `2026-08_populate_performance_optimization.md` (`0392d4b`)
   - `2026-08_fix_artist_backfill_fk_violation.md` (`1793ec0`, `a8495b2`)
   - `2026-08_fix_backfill_album_name_fk_violation.md` (`b870d31`, `1094283`)
   - `2026-08_fix_backfill_fk_safe_emptying.md` (`989d133`, `1094283`)
   - `2026-08_fix_enrichment_fk_album_track_guards.md` (`33eef88`, `ea66274`)
   - `2026-08_fix_enrichment_fk_constraints.md` (`0f67175`, `34207d4`)
2. `2026-07_wiki_db_dump.md`: add one-line "Adopted → see rust plan" pointer. `2026-07_music_db_rust_plan.md`: status note that Phases 1–8 are complete and the populate phase was added after Phase 8. `2026-07_music_db_options.md`: keep original status.
3. Reference the AGENTS.md "Documentation conventions" subsection (Step 3) — do not restate it; append a one-line note that this doc's own status update follows that convention.

**Commit:** `docs(research): mark implemented plans with status and closing commits`

### Step 5 — Verification (no commit)

Run the plan §6 Definition of Done; fix anything that fails in the relevant earlier step's commit (or a follow-up `docs:` fix commit):

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

# 3. Markdown sanity + repo hygiene
cargo test
cargo clippy -- -D warnings
cargo fmt --check
git diff --stat   # docs-only: no src/, tests/, scripts/ changes
```

**Optional:** verify the stage-semantics invariant on a scratch DB (`/tmp/wiki_db_verify.duckdb`): bootstrap seeds `album.name = album.id` (COUNT > 0), `populate` resolves them (COUNT = 0). Never on an existing database file. The hard gate is `cargo test`.

**Acceptance criteria (DoD):**
- No stale claims from §2 inventory remain (grep scans above pass).
- `populate` discoverable from README, ARCHITECTURE, and AGENTS.
- All researched-and-implemented plans carry `Status: Implemented` with closing commits.
- `cargo test`, `cargo clippy -- -D warnings`, `cargo fmt --check` pass.
- One commit per step, each self-contained, conventional commits, all on `agent/docs-sync-codebase`.

## Out of scope

- Operating on any particular database file (the docs stay instance-agnostic).
- Adding a `query track` subcommand or improving query output.
- Extending README config examples beyond the reference tables.
