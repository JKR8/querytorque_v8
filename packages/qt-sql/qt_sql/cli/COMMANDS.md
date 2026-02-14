# QueryTorque CLI Command Reference

Entry point: `qt` (or `python -m qt_sql.cli`)

Global options: `--verbose` / `--quiet`

---

## qt status

**Benchmark readiness check** (no LLM)

```
qt status <benchmark>
qt status postgres_dsb_76
qt status duckdb_tpcds
```

Shows: config.json, queries count, explains coverage, knowledge, plan_scanner (PG), gold examples, semantic_intents, strategy_leaderboard, leaderboard summary.

---

## qt prepare

**Generate analyst prompts deterministically** (no LLM, Phases 1-3)

```
qt prepare <benchmark>                            # all queries, swarm mode
qt prepare postgres_dsb_76 -q query001_multi_i1   # single query
qt prepare postgres_dsb_76 --mode oneshot          # oneshot mode prompt
qt prepare duckdb_tpcds -q query088               # prefix match (all Q88 variants)
qt prepare postgres_dsb_76 -o /tmp/prompts        # custom output dir
```

Options: `--query/-q` (repeatable), `--mode` (swarm/oneshot), `--force`, `--output-dir/-o`

Output: `benchmark/prepared/<timestamp>/` with `prompts/`, `context/`, `metadata/`, `original/`, `summary.json`

Wraps: `Pipeline._parse_logical_tree()` → `Pipeline.gather_analyst_context()` → `build_analyst_briefing_prompt()`

---

## qt run

**Full optimization pipeline** (LLM required, Phases 1-7)

```
qt run <benchmark>                                  # all queries
qt run postgres_dsb_76 -q query001 --mode oneshot   # single query, oneshot mode
qt run duckdb_tpcds --fan-out-only                  # state 0 only
qt run postgres_dsb_76 --resume                     # resume from checkpoint
```

Options: `--query/-q`, `--mode`, `--max-iterations`, `--target-speedup`, `--fan-out-only`, `--resume`, `--output-dir/-o`

Output: `benchmark/runs/run_<timestamp>/` with per-query results + `summary.json` + `checkpoint.json`

Wraps: `Pipeline.run_optimization_session()`

---

## qt validate

**Validate candidate SQL** (no LLM, Phase 6)

```
qt validate <benchmark> -q <query_id> -f <candidate.sql>
qt validate postgres_dsb_76 -q query065_multi -f optimized.sql
qt validate postgres_dsb_76 -q query102_spj_spj -f opt.sql --config-commands "SET LOCAL work_mem = '256MB'"
```

Options: `--query/-q` (required), `--sql-file/-f` (required), `--config-commands` (repeatable)

Wraps: `Validator.validate()` or `Validator.validate_with_config()`

---

## qt scan

**Plan-space scanner** (no LLM, PostgreSQL only)

```
qt scan <benchmark>                               # full scan
qt scan postgres_dsb_76 --explain-only             # cost-based only (~30s)
qt scan postgres_dsb_76 --explore                  # plan exploration with ANALYZE
qt scan postgres_dsb_76 -q query001 --timeout-ms 60000
```

Options: `--query/-q`, `--explore`, `--explain-only`, `--timeout-ms`

Output: `benchmark/plan_scanner/`, `benchmark/plan_explore/`

Wraps: `scan_corpus()`, `scan_corpus_explain_only()`, `explore_corpus()`

---

## qt blackboard

**Knowledge collation chain** (no LLM)

```
qt blackboard <benchmark>                          # full chain from latest batch
qt blackboard postgres_dsb_76 --from <batch_dir>   # specific source
qt blackboard --global                             # global best-of-all-sources
qt blackboard duckdb_tpcds --promote-only          # just phase 4
qt blackboard duckdb_tpcds --dry-run               # preview promotions
qt blackboard postgres_dsb_76 --min-speedup 1.5    # custom promotion threshold
```

Phases: Extract → Collate → GlobalKnowledge → Promote → Reindex

Options: `--from`, `--global`, `--promote-only`, `--dry-run`, `--min-speedup`

Wraps: `phase1_extract()` → `phase2_collate()` → `phase3_global()` → `phase4_promote_winners()` → `rebuild_index()`

---

## qt findings

**Scanner findings extraction** (LLM required, PostgreSQL only)

```
qt findings <benchmark>                            # full extraction
qt findings postgres_dsb_76 --prompt-only          # print LLM prompt, don't call
qt findings postgres_dsb_76 --force                # re-extract
qt findings postgres_dsb_76 --provider openai --model gpt-4
```

Options: `--prompt-only`, `--force`, `--provider`, `--model`

Wraps: `populate_blackboard()` → `extract_findings()`

---

## qt index

**Tag-based example index** (no LLM)

```
qt index --stats                                   # show statistics
qt index --rebuild                                 # rebuild from gold examples
```

Options: `--rebuild`, `--stats`

Wraps: `rebuild_index()`, `show_index_stats()`

---

## qt leaderboard

**Show/build leaderboards** (no LLM)

```
qt leaderboard <benchmark>                         # Rich table
qt leaderboard postgres_dsb --format json           # JSON output
qt leaderboard duckdb_tpcds --format csv --top 20  # CSV, top 20
qt leaderboard duckdb_tpcds --build-strategy       # build strategy leaderboard
```

Options: `--format` (table/json/csv), `--top N`, `--build-strategy`

Wraps: `leaderboard.json` reading, `build_strategy_leaderboard.py`
