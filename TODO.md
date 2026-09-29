# Implementation Plan: Verification Battery Fixes & Rebuild

Source: `docs/research/2026-09_verify_rebuild_fixes.md`
Supporting docs: `docs/research/2026-09_fix_entity_role_inversion.md` (§7 DoD battery, Step 2 curation discipline), `docs/research/2026-09_bootstrap_genre_scan_rebuild.md` (v2 build plan it supersedes)

## Context

The §7 battery against the 2026-09-28 `music-v2.duckdb` passed the core role-separation gate but surfaced three defects and two divergences (§2). The swap is frozen. This plan fixes the defects in code with unit tests (Fix A — malformed SPARQL catch-all path; Fix B — artist search hides exact-name matches; Fix C — work-class list gap for vocal singles), then re-runs bootstrap + populate into `music-v2.duckdb`, re-runs the full battery, and only then swaps. Divergences #4/#7 are accepted with root cause and are to be *measured*, not tuned to (see §2.1).

**Branch:** `agent/fix-sparql-search-role-lists` (from `main`)
**Expected runtime (code):** ~2-3 check/commit cycles; **(runtime):** bootstrap ~10 h + populate ~9-10 h, run under `nohup`

## Steps

|#|Commit message|Logical unit|Key deliverables|Tests|
|---|---|---|---|---|
|0|*(pre-work, no commit)*|Repo health + feature branch + plan-doc commit|Clean(able) tree on `main` (modified TODO.md + untracked plan doc are expected); `cargo test && cargo clippy -- -D warnings && cargo fmt --check` clean; `git checkout -b agent/fix-sparql-search-role-lists`; commit plan doc (`git add docs/research/2026-09_verify_rebuild_fixes.md TODO.md` → `docs(research): plan verification battery fixes and rebuild`); `df -h /home/tr` ≥ 60 GB free|—|
|1|fix(sparql): qualify every alternative and replicate work classes in modified query|SPARQL catch-all property path + work-class replication (Fix A)|`src/sparql.rs` — (1) map each of `MUSIC_PROPERTIES` to `wdt:{p}` before joining with `\|`; (2) add `ALBUM_WORK_CLASS_IDS`/`TRACK_WORK_CLASS_IDS` UNION blocks so the update path sees work-class-only entities. Unit tests derived from `filter::MUSIC_PROPERTIES` (no hardcoded string): qualified alternatives `wdt:P1303\|wdt:P175\|wdt:P136\|wdt:P358` present, unqualified `wdt:P1303\|P175` absent; work-class Q-IDs appear as `wdt:P31 wd:<qid>`. Live curl smoke check of the built query returns HTTP 200 (in gate, not deferred to the battery)|Unit|
|2|fix(query): rank exact artist-name matches before LIKE matches|Artist search exact-match ranking (Fix B)|`src/db/query.rs` — `ORDER BY (name = ?1) DESC, name` in `LIKE_SEARCH_ARTIST`; mirror exact-match-first ordering in `search_artist_fts` or comment the divergence (FTS is inert in the bundled build). Unit tests: exact-`name` row sorts before description-substring rows; >100-row spillover regression survives `LIMIT 100`|Unit|
|3|fix(extraction): classify vocal singles and audited work classes as tracks|Work-class list extension + audit (Fix C)|Runtime §4 golden-corpus audit **with a per-class frequency count** (~30-40 works via `Special:EntityData` + SPARQL `COUNT`/dump grep before adding any class; live baseline: Q55850593 ≈ 32 K instances); extend `TRACK_WORK_CLASS_IDS` / `ALBUM_WORK_CLASS_IDS` in `src/wikidata/filter.rs` per §3 rule (Q55850593 → track); update ARCHITECTURE.md class-list enumerations (and README if it enumerates) in the same series; extend role-classifier unit tests; record audit + frequency tables in plan run notes + fix doc §7 curation evidence|Unit, integration spot-check post-rebuild|
|4|*(no commit)*|Archive v2 + release build|`mv -n /home/tr/wiki_db/music-v2.duckdb music-v2-battery-20260928.duckdb`; `cargo audit`; `cargo build --release`|—|
|5|*(no commit)*|Bootstrap v3 run|Fresh parquet dir `/tmp/tag_agent_parquet_v3` (never v2/old dirs); nohup bootstrap (dump `/home/tr/wiki_db/latest-all.json.gz`, db `music-v2.duckdb`); watch 3 stage markers: streaming ~4.5 h (~121 M processed / ~2.68 M filtered), genre pass ~4-5 h, DuckDB loading|Log markers: Streaming / Genre label extraction / DuckDB loading complete|
|6|*(no commit)*|Populate v3 run|nohup populate (same dump/db/parquet); watch `Extracted labels and claims from dump` + FK-safe backfill + `=== Populate Complete ===`; ~9-10 h (970 K-QID pre-check dominates; rate ≈ 220 MB/min against the 155 GB dump)|—|
|7|*(no commit)*|DoD battery v3|Re-run all §7 checks **with** `--config /tmp/battery.toml` (default config silently points at v1 `music.duckdb`); pass #1-3,5,6,10; measure #4/#7 (record, accept divergence); #8a band U2 (Q396) is first hit; #8b both Joshua Tree albums; #9 `update --dry-run --since 2026-09-01T00:00:00Z` → HTTP 200 (fresh DB has no `sync_state`); gate: Q113111952 ("40") in `track`/`album`, not `artist`; record `inclusion_reason` audit|Battery + audit recorded|
|8|docs(research): record fix-rebuild battery results and mark plan implemented|Swap + close-out (swap is user decision)|`mv -n` swap (`music.duckdb` → `music-v1-inverted.duckdb`, `music-v2.duckdb` → `music.duckdb`); record battery numbers in plan run notes; update fix doc §7 where figures diverge **and reword its gate condition to cover album and track classes**; mark this doc `Status: Implemented` with closing commit refs; no further code changes expected|—|

## Verify gate (steps 1-3)

`cargo test` all green → `cargo clippy -- -D warnings` zero warnings → `cargo fmt --check` clean → `cargo build --release` succeeds → commit (conventional message from the table; doc updates ride the same series per AGENTS.md).

## Pitfalls

- **Do not** reuse `/tmp/tag_agent_parquet_v2` or `/home/tr/wiki_db/parquet-dir` for v3 — fresh dir only; orphan part files feed the loader wrongly. A 0-byte current `part-NNNNN.parquet` is normal.
- **CLI battery checks silently hit the wrong DB** (v1 `music.duckdb`) unless `--config /tmp/battery.toml` is passed — check #8 confusion in v2; the shipped `wiki_db.toml` still points at the v1 DB.
- Fresh DBs have no `sync_state` → `update` requires `--since`.
- The dump is **not Q-ID-sorted** — estimate progress from stage markers or fd-offset rate (~220 MB/min), never from Q-IDs.
- Fix C curation discipline: only add common, unambiguous music-work classes with audit evidence; do **not** add catch-all "musical work" parents that would resurrect role pollution.
- The update-path SPARQL query must replicate the work-class lists after Fix A (Fix A scope) — otherwise `update` can never retrieve work-class-only works.
- Archive (do not delete) `music-v2.duckdb` at step 4 — it is the v2 battery artifact.
- Swap `mv`s use `-n` (no-clobber) for idempotent re-runs.
- Logs are ANSI-colored; strip with `sed -e 's/\x1b\[[0-9;]*m//g'` before grepping.
- Populate is ~9-10 h, not the stale 4-5 h figure; plan wall-clock accordingly.
- Swap happens only if the battery passes — otherwise investigate before swapping.