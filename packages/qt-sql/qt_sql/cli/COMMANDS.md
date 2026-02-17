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
qt prepare <benchmark>                            # all queries, oneshot prompt context
qt prepare postgres_dsb_76 -q query001_multi_i1   # single query
qt prepare duckdb_tpcds -q query088               # prefix match (all Q88 variants)
qt prepare postgres_dsb_76 -o /tmp/prompts        # custom output dir
qt prepare duckdb_tpcds --scenario duckdb_embedded # with scenario card
qt prepare duckdb_tpcds --bootstrap               # first-run mode (no gold examples needed)
qt prepare duckdb_tpcds --evidence                # include evidence bundle
```

Options: `--query/-q` (repeatable), `--force`, `--output-dir/-o`, `--bootstrap`, `--scenario`, `--evidence`

Output: `benchmark/prepared/<timestamp>/` with `prompts/`, `context/`, `metadata/`, `original/`, `summary.json`

Wraps: `Pipeline._parse_logical_tree()` → `Pipeline.gather_analyst_context()` → `build_analyst_briefing_prompt()`

---

## qt run

**Full optimization pipeline** (LLM required, Phases 1-7)

```
qt run <benchmark>                                  # all queries
qt run postgres_dsb_76 -q query001 --mode oneshot   # single query, tiered analyst/worker flow
qt run duckdb_tpcds --mode fleet                    # fleet survey/triage/execute/scorecard
qt run duckdb_tpcds --single-iteration              # one analyst/worker/snipe round
qt run postgres_dsb_76 --resume                     # resume from checkpoint
qt run duckdb_tpcds --scenario duckdb_embedded      # with scenario card
qt run postgres_dsb_76 --engine-version 17          # explicit engine version
qt run duckdb_tpcds --output-contract               # emit QueryOutputContract JSON
qt run duckdb_tpcds --concurrency 25 --benchmark-concurrency 4  # parallel queries + 4 benchmark lanes
qt run postgres_dsb_76 --config-boost               # SET LOCAL tuning on winners
```

Options: `--query/-q`, `--mode` (`oneshot`/`fleet`), `--max-iterations`, `--target-speedup`, `--single-iteration`, `--resume`, `--output-dir/-o`, `--concurrency`, `--benchmark-concurrency`, `--launch-interval-seconds`, `--config-boost`, `--bootstrap`, `--scenario`, `--engine-version`, `--output-contract`, `--dry-run` (fleet only)

Output: `benchmark/runs/run_<mode>_<timestamp>/` with per-query `result.json` (includes `api_call_costs`, `beam_cost_usd`), `progress.json` (live total beam cost), `summary.json` (final total beam cost), and `checkpoint.json`. Per-query beam sessions also write `llm_calls.jsonl` + `llm_cost_summary.json`.

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

---

## qt config-boost

**SET LOCAL tuning on validated winners** (no LLM, PostgreSQL only)

```
qt config-boost <benchmark>                        # boost all winners
qt config-boost postgres_dsb_76 --min-speedup 1.1  # custom threshold
```

Options: `--min-speedup`

Wraps: `config_boost.boost_benchmark()`

---

## qt refresh-explains

**Re-collect EXPLAIN ANALYZE plans** (no LLM)

```
qt refresh-explains <benchmark>                    # refresh all
qt refresh-explains postgres_dsb_76 -q query001    # single query
```

Options: `--query/-q`

---

## qt collect-explains

**Collect EXPLAIN plans for benchmark queries** (no LLM)

```
qt collect-explains <benchmark>
```

---

## qt config-coach

**Interactive config tuning advisor** (LLM required, PostgreSQL only)

```
qt config-coach <benchmark>
```

---

## qt workload

**Fleet-level workload optimization** (LLM required for full run)

```
qt workload <benchmark>                                        # full workload optimization
qt workload duckdb_tpcds --dry-run                             # triage + fleet detection only
qt workload duckdb_tpcds --target-size Small                   # target warehouse size
qt workload duckdb_tpcds --scenario duckdb_embedded            # with scenario card
qt workload postgres_dsb_76 --max-tier3 10                     # limit deep optimization
qt workload duckdb_tpcds -o scorecard.md                       # save scorecard to file
```

Options: `--target-size/-t`, `--scenario/-s`, `--max-tier3/-m` (default 20), `--output/-o`, `--dry-run`

Stages:
1. **Triage**: Score all queries by pain x frequency x tractability → classify SKIP/TIER_2/TIER_3
2. **Fleet Detection**: Shared scans → index recommendations, config opportunities, statistics staleness
3. **Quick-Win Fast Path**: Top 3 queries with >80% total pain → direct to Tier 3
4. **Tier 2 (Light)**: Single-pass oneshot optimization (~5K tokens)
5. **Tier 3 (Deep)**: Tiered analyst/worker/snipe pipeline (~40-50K tokens)
6. **Scorecard**: Business case with estimated savings, residuals, recommendations

Output: Markdown scorecard (stdout or file)

Wraps: `WorkloadSession.run()` → `triage_workload()` → `detect_fleet_patterns()` → `compile_scorecard()`

---

## qt dashboard

**Web dashboard for swarm sessions** (no LLM)

```
qt dashboard <benchmark>                           # open on port 8765
qt dashboard duckdb_tpcds --port 9000              # custom port
```

Options: `--port` (default 8765)
