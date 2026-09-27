# Research: Full-Dump Rebuild for Entity-Role Verification

**Date:** 2026-09-27
**Status:** Plan (ready to execute)
**References:**

- [2026-09_fix_entity_role_inversion.md](./2026-09_fix_entity_role_inversion.md) — the role fix whose §7 DoD battery this plan runs
- [2026-08_populate_performance_optimization.md](./2026-08_populate_performance_optimization.md) — Aho-Corasick label scan precedent
- [ARCHITECTURE.md](../ARCHITECTURE.md) — pipeline stages and role routing

---

## 1. Purpose

The entity-role fix code is landed (`agent/fix-entity-role-inversion`, commits
`45f892d`…`7f7715d`). Its verification step — a **fresh** full-dump bootstrap +
populate into `music-v2.duckdb` plus the §7 Definition-of-Done battery — was
started on 2026-09-27 and aborted. This plan records exactly what was learned,
what is preserved, and the step-by-step re-run for a later session to execute
(an overnight job: ~14-15 h wall clock).

## 2. What the aborted run established

| Item | Value |
|---|---|
| Dump | `/home/tr/wiki_db/latest-all.json.gz` — **145 GB compressed** (2026-09-26 dump; plan estimate of ~155 GB is stale) |
| Lines (events) | 120,986,270 processed in pass 1 |
| Pass-1 duration | ~4 h 36 m (release build, 1 thread @ 99.9% CPU) |
| Pass-1 yield | 2,680,421 filtered music entities; 13,507 genre Q-IDs; 27 part files (~192 MB) |
| Pass-2 bottleneck | `extract_genre_labels` pre-check was a naive linear scan over every genre Q-ID (`genre_qids.iter().any(\|qid\| line.contains(qid))`) → measured ~36 K lines/min → **~52 h projected** for the whole dump. Aborted. |
| Fix | Commit `7f7715d` — `extract_genre_labels` now builds an `AhoCorasick` automaton from the genre Q-IDs (one `O(L)` single-pass match per line, same technique `label_extractor.rs` already used) + empty-set early return. Verified by unit tests; the second pass becomes decompression-bound (~4-5 h). |
| Disk free at run time | 96 GB (145 G dump + ~5 G parquet + ~2 G DuckDB fits) |
| DB state | `music-v2.duckdb` was never created (run aborted before the load phase) |
| Stale artifacts | `/tmp/tag_agent_parquet_v2` holds the aborted run's pass-1 parquet — **abandon, do not feed to the loader** |

## 3. Data-contract / routing recap (what the battery must confirm)

1. **Role separation** — the same Q-ID never appears as both `album.id` and `artist.id` (DoD #1 expects ≈0, allow tiny Wikidata oddities <0.1%).
2. **Work routing** — `P31∈album classes` → `album` + `album_artist` (P175 performers); `P31∈track classes` → `track` + `track_artist` (P175) + `track_album` (P361 parents). `artist` holds `role='Agent'` rows only.
3. **Name resolution** — `en` label → sanitized `enwiki` sitelink title → NULL; bootstrap names work rows `COALESCE(name, id)`.
4. **Duration** — `track.duration_seconds` = P2047 `mainsnak.datavalue.amount`, seconds unit (`Q11574`) or unit-less, else NULL+WARN.

## 4. Execution

### Step 0 — Pre-work

```bash
git status                 # expect clean, on agent/fix-entity-role-inversion
git log --oneline -3       # expect ... 7f7715d fix(extraction): build genre-label scan ...
cargo test && cargo clippy -- -D warnings && cargo fmt --check
df -h /home/tr/wiki_db    # expect ≥ 60 GB free
```

### Step 1 — Fresh release build

```bash
cargo build --release      # ~5 min
```

### Step 2 — Bootstrap (fresh parquet dir, ~10 h)

```bash
rm -rf /tmp/tag_agent_parquet_v2   # aborted pass-1 parquet: do NOT reuse
nohup cargo run --release -- bootstrap \
    --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb \
    --parquet-dir /tmp/tag_agent_parquet_v2 \
    > /tmp/verify_bootstrap2.log 2>&1 &
```

Progress markers to watch (log is ANSI-colored — strip with
`sed -e 's/\x1b\[[0-9;]*m//g'`):

1. `Streaming phase complete processed=~121000000 filtered=~2680000 genre_qids=~13500` (~4.5 h)
2. `Extracting genre labels from dump (second pass)...` then `Genre label extraction complete genre_count=~13K` (~4-5 h — previously ~52 h, now decompression-bound)
3. `Opening DuckDB database` → `DuckDB loading complete` → `FTS indexes ready` (minutes)
4. Summary counts line (artist/genre/album/track)

**Regression tripwire:** if the second pass again projects days, measure the rate
(`grep -a "line=" log | tail`) before letting it run — the fix must be present.

### Step 3 — Populate (~4-5 h)

```bash
nohup cargo run --release -- populate \
    --dump /home/tr/wiki_db/latest-all.json.gz \
    --db /home/tr/wiki_db/music-v2.duckdb \
    --parquet-dir /tmp/tag_agent_parquet_v2 \
    > /tmp/verify_populate.log 2>&1 &
```

Watch for `Extracted labels and claims from dump` + the backfill summaries.

### Step 4 — DoD battery (plan §7, against `music-v2.duckdb`)

Use `duckdb music-v2.duckdb` (or the CLI `cargo run --release -- query …`):

| # | Check | Expect |
|---|---|---|
| 1 | `SELECT count(*) FROM album a JOIN artist ar ON a.id=ar.id` | ≈ 0 (<0.1%) |
| 2 | `SELECT name FROM album WHERE name ILIKE '%joshua tree%'` | includes "The Joshua Tree" |
| 3 | `SELECT al.name FROM album_artist aa JOIN artist ar ON ar.id=aa.artist_id JOIN album al ON al.id=aa.album_id WHERE ar.name='U2'` | non-empty, real albums |
| 4 | `SELECT count(*) FROM track_album` | ≥ 7,000 |
| 5 | `SELECT count(*) FROM track WHERE duration_seconds IS NOT NULL` | ≥ 100 |
| 6 | `SELECT count(*) FROM album WHERE release_date IS NOT NULL` | ≥ 1,000 |
| 7 | `SELECT count(*) FROM album WHERE name ~ '^Q[0-9]+$'` | < 3,930 |
| 8 | `cargo run --release -- query album --name "Joshua Tree"` and `query artist --name "U2"` | hit album / agent |
| 9 | `cargo run --release -- update --dry-run` | no inverted rows; work entity upsert lands in album/track |
| 10 | `cargo test && cargo clippy -- -D warnings && cargo fmt --check` | clean |

Also record the inclusion-reason distribution audit:

```sql
SELECT inclusion_reason, count(*) FROM artist GROUP BY 1 ORDER BY 2 DESC LIMIT 20;
```

### Step 5 — Swap (user decision: rename swap)

```bash
mv /home/tr/wiki_db/music.duckdb /home/tr/wiki_db/music-v1-inverted.duckdb
mv /home/tr/wiki_db/music-v2.duckdb /home/tr/wiki_db/music.duckdb
# wiki_db.toml `db = "~/wiki_db/music.duckdb"` now points at the fixed DB.
```

### Step 6 — Close-out (no code commit)

- Record the battery numbers + `inclusion_reason` audit in this doc's run notes (or TODO).
- Update `2026-09_fix_entity_role_inversion.md` §7 if figures diverge from expectations.
- No repository code changes expected; if the battery fails, investigate before swapping.

## 5. Pitfalls

- **Do not** reuse `/home/tr/wiki_db/parquet-dir` (encodes the inverted v1 schema) or the aborted `/tmp/tag_agent_parquet_v2` (missing the genres/load steps and would feed the loader orphan part files). Fresh dir only.
- **The dump is not Q-ID-sorted** — Q-IDs observed out of order in the log; do not use them to estimate progress. Use the stage markers.
- **Batcher cadence:** part files flush at ~100 K entities; a 0-byte current part file is normal (in-memory batch).
- **Timing:** pass 1 ~4.5 h, pass 2 ~4-5 h (post-fix), populate ~4-5 h. Plan for overnight; use `nohup` + log files; a 1200 s sleep-and-poll loop works well.
- If the machine resumes from suspend / the network is irrelevant (dump is local), no state is lost until the load phase; killed runs leave partial parquet that a fresh dir discards.