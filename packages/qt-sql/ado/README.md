# ADO — Autonomous Data Optimization

Iterative SQL optimization engine: parse query structure, retrieve similar examples via FAISS, generate rewrites with LLM, validate with real execution timing.

## Architecture (ASCII)

```text
+---------------------- Inputs -----------------------+
| Benchmark config | SQL queries | Examples/Constraints|
| FAISS index | LLM provider | History/Learnings      |
+-------------------------------+---------------------+
                                |
                                v
+------------------------- Core Pipeline ----------------------+
| 1 Parse -> DAG + cost (pipeline.py)                           |
| 2 FAISS retrieval: examples + regressions (knowledge.py)      |
| 3 Prompt + N candidates (node_prompter.py, generate.py,       |
|    sql_rewriter.py)                                           |
| 4 Syntax gate (sqlglot)                                       |
| 5 Validate + score (validate.py; DuckDB/PG validators)        |
+-------------------------------+------------------------------+
                                |
                 +--------------+--------------+
                 |                             |
                 v                             v
      +---------------------+        +------------------------+
      | Analyst (Expert)    |        | Swarm (fan-out + snipe)|
      | analyst.py +        |        | swarm_prep/run.py      |
      | analyst_session.py  |        | 4 workers -> snipe     |
      | iterates on same    |        | batch or single query  |
      | original SQL        |        |                        |
      +---------------------+        +------------------------+
                 |                             |
                 +--------------+--------------+
                                |
                                v
+------------------------ Outputs -----------------------------+
| Artifacts: prompt/response/sql/validation (store.py)          |
| Learnings + leaderboards + history (learn.py)                 |
| Optional promotion to next state (pipeline.promote)           |
+--------------------------------------------------------------+
```

## Environment Setup

LLM config lives in the **project root** `.env` file (`QueryTorque_V8/.env`).
All env vars use the `QT_` prefix. Settings are loaded by `qt_shared.config.get_settings()`.

```
QT_LLM_PROVIDER=deepseek
QT_LLM_MODEL=deepseek-reasoner
QT_DEEPSEEK_API_KEY=sk-...
QT_OPENROUTER_API_KEY=sk-or-...
```

**Critical**: scripts must run from the project root (where `.env` lives) so pydantic-settings
can find it. Set PYTHONPATH accordingly:

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.runner ...
```

If `provider`/`model` are passed explicitly to `ADOConfig`, they override the `.env` values.
If omitted, the pipeline reads from `QT_LLM_PROVIDER` / `QT_LLM_MODEL` automatically.

## Databases

| Engine     | Benchmark | Location                                          | Notes                          |
|------------|-----------|---------------------------------------------------|--------------------------------|
| DuckDB     | TPC-DS    | `/mnt/d/TPC-DS/tpcds_sf10.duckdb`                | SF10, 99 queries               |
| PostgreSQL | DSB       | `postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10` | SF10 (explains), SF5 (bench)   |
| PostgreSQL | start     | `/usr/lib/postgresql/16/bin/pg_ctl -D /mnt/d/pgdata -l /mnt/d/pgdata/logfile start` | |

## Optimization Modes

All modes run from the **project root**. Common preamble:

```python
from ado.pipeline import Pipeline
from ado.schemas import OptimizationMode

p = Pipeline(
    benchmark_dir="packages/qt-sql/ado/benchmarks/duckdb_tpcds",
    provider="deepseek",
    model="deepseek-reasoner",
)
sql = open("packages/qt-sql/ado/benchmarks/duckdb_tpcds/queries/query_67.sql").read()
```

### Standard (fast, no analyst)

Single iteration: FAISS retrieval, prompt, generate, validate. No analyst call, no retry.

```python
result = p.run_optimization_session(
    query_id="query_67", sql=sql,
    mode=OptimizationMode.STANDARD, n_workers=3,
)
```

### Expert (iterative, 1 worker, analyst-steered)

Up to N iterations with 1 worker per round. On failure, the analyst LLM analyzes why it
failed (with full error messages) and steers the next attempt. Retry preamble shows raw
validation errors + expert analysis.

```python
result = p.run_optimization_session(
    query_id="query_67", sql=sql,
    mode=OptimizationMode.EXPERT,
    max_iterations=3, target_speedup=2.0,
)
# n_workers is always 1 in expert mode (hardcoded)
```

### Swarm (4-worker fan-out + snipe)

4 parallel workers with different strategies, then single-worker snipe refinement.

```python
result = p.run_optimization_session(
    query_id="query_67", sql=sql,
    mode=OptimizationMode.SWARM,
    max_iterations=3, target_speedup=2.0,
)
# n_workers is always 4 in swarm mode (hardcoded)
```

### Single query (no session, no retry)

```python
result = p.run_query("query_67", sql, n_workers=5)
```

## Swarm Pipeline (batch scripts)

Large-scale swarm optimization across all queries in a benchmark. Three scripts run
sequentially: prep (zero API calls), run (LLM + benchmark), validate (standalone timing).

All scripts accept `--benchmark-dir` to select the target benchmark. Default is `duckdb_tpcds`.

### 1. Prep — build all prompts (zero API calls)

```bash
# DuckDB TPC-DS (default)
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_prep

# PostgreSQL DSB
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_prep \
    --benchmark-dir packages/qt-sql/ado/benchmarks/postgres_dsb
```

Phase 0 caches EXPLAIN ANALYZE (DuckDB: runs live; PG: loads from `explains/sf10/`).
Phase 1 builds fan-out prompts (DAG + FAISS + regression warnings).

### 2. Run — LLM generation + benchmarking

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.swarm_run \
    --benchmark-dir packages/qt-sql/ado/benchmarks/postgres_dsb
```

Resume-safe: every LLM response and benchmark result is saved to disk immediately.
Three iterations: fan-out (4 workers) → snipe → re-analyze + final worker.
DuckDB uses 2 parallel DB slots; PostgreSQL uses 2 connections to the same DSN.

### 3. Validate — standalone batch timing

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.batch_validate \
    --benchmark-dir packages/qt-sql/ado/benchmarks/postgres_dsb \
    packages/qt-sql/ado/benchmarks/postgres_dsb/swarm_batch_XXXXXXXX_XXXXXX
```

3-run validation: discard 1st (warmup), average last 2.

## Running from CLI

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -c "
from dotenv import load_dotenv; load_dotenv('.env')
from ado.pipeline import Pipeline
from ado.schemas import OptimizationMode
p = Pipeline('packages/qt-sql/ado/benchmarks/duckdb_tpcds', provider='deepseek', model='deepseek-reasoner')
sql = open('packages/qt-sql/ado/benchmarks/duckdb_tpcds/queries/query_67.sql').read()
result = p.run_optimization_session('query_67', sql, mode=OptimizationMode.EXPERT, max_iterations=3)
print(f'{result.status} {result.best_speedup:.2f}x')
"
```

## Rebuilding the FAISS Index

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m ado.faiss_builder
```

## Validation Rules

Only 2 valid ways to validate query speedup:
1. **3-run**: Run 3 times, discard 1st (warmup), average last 2
2. **5-run trimmed mean**: Run 5 times, remove min/max, average remaining 3

Never use single-run comparisons.

## Documentation

Full architecture and runtime workflow: **[docs/ADO_WORKFLOW.md](docs/ADO_WORKFLOW.md)**
Cleanup safety verification: **[docs/ADO_DELETE_SAFETY_MANIFEST.md](docs/ADO_DELETE_SAFETY_MANIFEST.md)**
Product refactor plan (qt-sql centered): **[docs/QT_SQL_ADO_PRODUCT_REFACTOR_PLAN.md](docs/QT_SQL_ADO_PRODUCT_REFACTOR_PLAN.md)**

## Structure

```
ado/
├── pipeline.py       # 5-phase orchestrator (parse → FAISS → rewrite → syntax → validate)
├── node_prompter.py  # Prompt builder (attention-optimized sections)
├── analyst.py        # LLM-guided structural analysis
├── analyst_session.py # Expert mode session driver
├── runner.py         # ADORunner entry point
├── generate.py       # Parallel LLM candidate generation
├── sql_rewriter.py   # Response → SQL extraction + AST transform inference
├── validate.py       # Timing + correctness validation
├── schemas.py        # Data structures (BenchmarkConfig, SessionResult, etc.)
├── learn.py          # Learning records + analytics
├── knowledge.py      # FAISS recommender (example retrieval)
├── faiss_builder.py  # Build/rebuild FAISS similarity index
├── context.py        # Context management
├── store.py          # Result storage
├── session_logging.py # Session log utilities
├── smoke_test.py     # End-to-end smoke test
├── swarm_prep.py     # Swarm batch prep (Phase 0-1, zero API calls)
├── swarm_run.py      # Swarm batch run (LLM + benchmark, resume-safe)
├── batch_swarm.py    # Batch swarm generation (LLM only, no benchmark)
├── batch_validate.py # Batch validation (3-run timing)
├── constraints/      # 11 optimization constraint rules (JSON)
├── examples/         # 21 gold examples (16 DuckDB + 5 PostgreSQL)
│   ├── duckdb/       # Gold examples with principle fields
│   └── postgres/     # PostgreSQL-specific gold examples
├── models/           # FAISS similarity index + metadata
├── prompts/          # Swarm prompt builders (fan-out, snipe, parsers)
├── sessions/         # Session drivers (standard, expert, swarm)
├── learnings/        # Learning journal data
├── benchmarks/       # Per-engine benchmark configs, queries, results
│   ├── duckdb_tpcds/ # Config, 99 queries, explains, leaderboard, semantic_intents
│   └── postgres_dsb/ # Config, 52 queries, explains/sf10 + explains/sf5
└── docs/             # Architecture and reference documentation
```
