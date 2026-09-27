# Implementation Plan: Full-Dump Rebuild for Entity-Role Verification

Source: `docs/research/2026-09_bootstrap_genre_scan_rebuild.md`

## Context

The entity-role inversion fix is fully implemented and committed on `agent/fix-entity-role-inversion` (commits `45f892d`…`8895109`, plus `7f7715d`). The remaining work is the **verification run**: a fresh full-dump bootstrap + populate into `music-v2.duckdb`, the plan §7 DoD battery, and the rename swap. An attempt on 2026-09-27 was aborted when the bootstrap genre-label second pass projected ~52 h (naive linear scan); that defect is fixed in `7f7715d` (Aho-Corasick). This plan re-runs the build and completes the battery.

Verified facts (2026-09-27): dump = `/home/tr/wiki_db/latest-all.json.gz`, **145 GB compressed** (plan's ~155 GB figure stale); pass-1 measured 4 h 36 m for 120,986,270 events / 2,680,421 filtered entities; 96 GB disk free at run time.

**Branch:** `agent/fix-entity-role-inversion`
**Expected runtime:** ~14-15 h (pass 1 ~4.5 h, genre pass ~4-5 h post-fix, populate ~4-5 h, load minutes). Run under `nohup` with log files.

## Steps

|#|Commit message|Logical unit|Key deliverables|Tests|
|---|---|---|---|---|
|0|*(pre-work, no commit)*|Repo health + fix presence|Clean tree; `7f7715d` in log; `cargo test && cargo clippy -- -D warnings && cargo fmt --check` clean; `df -h` ≥ 60 GB free|—|
|1|*(no commit)*|Release build|`cargo build --release` for the fresh entry point|—|
|2|*(no commit)*|Bootstrap run|`rm -rf /tmp/tag_agent_parquet_v2`; nohup bootstrap into `music-v2.duckdb` + fresh parquet dir; watch for the 3 stage markers|Log markers: Streaming phase complete / Genre label extraction complete / DuckDB loading complete|
|3|*(no commit)*|Populate run|nohup populate same dump/db/parquet dir; watch `Extracted labels and claims from dump` + backfill summaries|—|
|4|*(no commit)*|DoD battery (§7)|11 checks on `music-v2.duckdb` (role separation ≈0, Joshua Tree album, U2 album_artist, track_album ≥7 K, durations ≥100, release_date ≥1 K, QID mirrors <3,930, query hits, update --dry-run, quality gate) + `inclusion_reason` distribution audit|Battery + audit recorded|
|5|*(no commit)*|Swap on pass|`mv music.duckdb music-v1-inverted.duckdb && mv music-v2.duckdb music.duckdb` (config `db` already = `~/wiki_db/music.duckdb`)|`query artist --name "U2"` hits agent; `query album --name "Joshua Tree"` hits album|
|6|*(no commit)*|Close-out|Record battery figures in the research doc; no code changes expected|Working tree clean|

## Pitfalls

- Do **not** reuse `/home/tr/wiki_db/parquet-dir` (inverted v1 schema) or the aborted `/tmp/tag_agent_parquet_v2` (orphan part files); always start from a fresh parquet dir.
- The dump is **not Q-ID-sorted** — never estimate progress from Q-IDs; use the stage markers in the log.
- A 0-byte current `part-NNNNN.parquet` is normal (in-memory batch fills ~100 K entities).
- The genre pass must run the `7f7715d` binary — if it again projects days, stop and re-measure before letting it run.
- Logs are ANSI-colored; strip with `sed -e 's/\x1b\[[0-9;]*m//g'` before grepping for markers.