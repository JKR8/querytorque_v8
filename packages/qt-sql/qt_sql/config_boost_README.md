# Config Boost — Post-Rewrite SET LOCAL Tuning

## Overview

The Config Boost module applies **post-rewrite SET LOCAL parameter tuning** to winning SQL rewrites by analyzing their EXPLAIN ANALYZE plans. This provides additive performance gains on top of SQL rewrites.

**Architecture:**
- **Phase**: Runs after swarm fan-out selects a winning rewrite (speedup >= 1.05x threshold)
- **Input**: Leaderboard.json (optimized SQL + original SQL + EXPLAIN plans from any source)
- **Output**: config_boost_results.json (SET LOCAL configs + 3-variant benchmark timings)
- **Validation**: 3-variant benchmark (original vs rewrite vs rewrite+config)

## How It Works

### 1. Rule-Based Config Proposal (Production)

**Function:** `propose_config_from_explain(explain_text, current_work_mem_mb)`

**6 Regex-Based Rules:**

| Rule | Trigger | Proposes | Rationale |
|------|---------|----------|-----------|
| 1: work_mem | Hash peak memory >=50% work_mem OR Batches>1 | work_mem = 4x peak (max 2048MB) | Eliminate disk spills in sorts/hashes |
| 2: enable_nestloop | Nested Loop with >10K rows | enable_nestloop = off | Force hash/merge join on large joins |
| 3: max_parallel_workers | No parallel nodes despite seq scans >100K rows | max_parallel_workers_per_gather = 4 | Enable parallelism on large scans |
| 4: jit | JIT compilation on queries <500ms | jit = off | Eliminate JIT overhead on short queries |
| 5: random_page_cost | Seq scans on fact tables (store_sales, etc.) | random_page_cost = 1.1 | Favor index scans (SSD hint) |
| 6: join_collapse_limit | >6 join nodes in plan | join_collapse_limit = 12 | Allow larger join reordering search space |

**Whitelist:** All proposals validated against `PG_TUNABLE_PARAMS` (16 safe session-scoped params)

### 2. LLM-Driven Config Tuning (Research Track)

**Module:** `prompts/pg_tuner.py`

**Function:** `build_pg_tuner_prompt(query_sql, explain_plan, current_settings, engine_profile, baseline_ms)`

**LLM Prompt Includes:**
- SQL query + baseline execution time
- EXPLAIN ANALYZE plan (text format, truncated to 200 lines)
- Current PostgreSQL settings
- System constraints (max_workers, shared_buffers, connections)
- Engine profile (optimizer strengths/gaps)
- Tunable parameter catalog (16 params with types/ranges/descriptions)
- 8 detailed analysis rules (work_mem, parallelism, JIT, join strategies, etc.)

**Output:** JSON `{"params": {...}, "reasoning": "..."}`

**Status:** Integrated via `--use-llm` flag, falls back to rules on failure

## Usage

### CLI Command

```bash
# Boost all winning rewrites in a benchmark (reads leaderboard.json)
qt config-boost postgres_dsb_76

# Boost specific queries only
qt config-boost postgres_dsb_76 -q query001 -q query002

# Dry run — show proposed configs without benchmarking
qt config-boost postgres_dsb_76 --dry-run

# Adjust min speedup threshold
qt config-boost postgres_dsb_76 --min-speedup 1.10

# Use LLM for config analysis instead of rules
qt config-boost postgres_dsb_76 --use-llm --dry-run
```

### Programmatic API

```python
from qt_sql.config_boost import boost_from_leaderboard

# From leaderboard.json
results = boost_from_leaderboard(
    benchmark_dir=Path("benchmarks/postgres_dsb"),
    dsn="postgresql://user:pass@host:5432/db",
    min_speedup=1.05,
    dry_run=False,
    use_llm=False,
    query_ids=["query001", "query002"]  # optional filter
)

# Legacy: from swarm_sessions directly
from qt_sql.config_boost import boost_session, boost_benchmark
result = boost_session(session_dir, dsn, min_speedup=1.05)
results = boost_benchmark(benchmark_dir, dsn)
```

## Output Files

**Per-benchmark:**
- `config_boost_results.json`

**Schema:**
```json
{
  "benchmark": "postgres_dsb_76",
  "config_method": "rules",
  "results": [
    {
      "query_id": "query001_multi",
      "status": "BOOSTED",
      "rewrite_speedup": 1.45,
      "config_proposed": {
        "work_mem": "512MB",
        "jit": "off"
      },
      "config_commands": [
        "SET LOCAL jit = 'off'",
        "SET LOCAL work_mem = '512MB'"
      ],
      "rules_fired": ["increase_work_mem", "jit_off_short_query"],
      "reasons": {
        "work_mem": "Peak hash memory 128MB + disk spill (8 batches) vs current 4MB work_mem",
        "jit": "JIT active on 350ms query (JIT overhead ~12ms)"
      },
      "benchmark": {
        "original_ms": 5000.0,
        "rewrite_ms": 3448.3,
        "config_ms": 2857.1,
        "rewrite_speedup": 1.45,
        "config_speedup": 1.75,
        "config_additive": 1.21,
        "best_variant": "rewrite+config"
      }
    }
  ]
}
```

**Status values:**
- `BOOSTED`: Config improved speedup by >=2% (config_additive > 1.02)
- `NO_GAIN`: Config proposed but no measurable improvement
- `NO_RULES`: No rules matched the EXPLAIN plan
- `SKIPPED`: Rewrite speedup below threshold or SQL not found
- `ERROR` / `BENCHMARK_ERROR`: Exception during processing
- `DRY_RUN`: Config proposed without benchmarking

## Standalone Operation

Config boost is **NOT** integrated into `qt run`. It's a separate post-processing tool:

```bash
# 1. Run swarm to generate leaderboard
qt run postgres_dsb_76
qt leaderboard postgres_dsb_76  # Creates leaderboard.json

# 2. Run config boost separately (reads leaderboard.json)
qt config-boost postgres_dsb_76

# Output: config_boost_results.json
```

**Why separate:**
- Different teams can work on SQL rewrites vs config tuning independently
- Clear attribution: leaderboard.json = rewrite gains, config_boost_results.json = config gains
- No mixing of optimization sources (LLM rewrites vs config params)

## Validation Method

**3-Variant Interleaved Benchmark:**

Pattern: 1-2-3-1-2-3-1-2-3 (3 rounds: warmup, measure1, measure2)

```
Round 1 (warmup):   original -> rewrite -> rewrite+config
Round 2 (measure1): original -> rewrite -> rewrite+config
Round 3 (measure2): original -> rewrite -> rewrite+config
Result: Average of measure1 and measure2
```

**Total:** 9 executions (3 per variant)

**Rationale:** Interleaving controls for cache warming and system drift. Averaging two measurement rounds reduces noise from system variance. This follows the project's "never single-run" validation rule.

## Files

**Core:**
- `config_boost.py` — Rule-based EXPLAIN parser + session/benchmark runners + LLM integration
- `cli/cmd_config_boost.py` — CLI command
- `pg_tuning.py` — Parameter whitelist + validation + resource envelope builder
- `prompts/pg_tuner.py` — LLM prompt builder (research track)

**Tests:**
- `tests/test_config_boost.py` — Unit tests for 6 rules (26 tests)
- `tests/test_pg_tuning.py` — Parameter validation tests
- `tests/test_pg_tuner_llm.py` — LLM integration test

**Validation:**
- `validate.py::PostgresValidatorWrapper.benchmark_three_variants()` — 3-variant benchmark

## Known Limitations

1. **Regex fragility** — EXPLAIN output format varies across PostgreSQL versions (tested on PG 14.3)
2. **No cross-node pattern matching** — Rules analyze operators independently, may miss multi-node bottlenecks
3. **Static thresholds** — Fixed cutoffs (10K rows for nestloop, 500ms for JIT, etc.) may not generalize across workloads
4. **No config coordination** — If worker-level SET LOCAL was used, config_boost may propose conflicting configs
5. **Single-round proposals** — Rules fire once; no iterative refinement based on benchmark results
