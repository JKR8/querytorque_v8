# ADO — Autonomous Data Optimization

Iterative SQL optimization engine: parse query structure, retrieve similar examples via FAISS, generate rewrites with LLM, validate with real execution timing.

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
| PostgreSQL | DSB       | `postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10` | SF10, 52 queries               |
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
├── node_prompter.py  # Prompt builder (attention-optimized sections)
├── pipeline.py       # 5-phase orchestrator (parse → FAISS → rewrite → syntax → validate)
├── analyst.py        # LLM-guided structural analysis
├── analyst_session.py # Expert mode session driver
├── runner.py         # ADORunner entry point
├── generate.py       # Parallel LLM candidate generation
├── validate.py       # Timing + correctness validation
├── faiss_builder.py  # Build/rebuild FAISS similarity index
├── knowledge.py      # FAISS recommender (example retrieval)
├── learn.py          # Learning records + analytics
├── schemas.py        # Data structures (ValidationResult, SessionResult, etc.)
├── constraints/      # 11 optimization constraint rules (JSON)
├── examples/         # 21 gold examples (16 DuckDB + 5 PostgreSQL)
│   ├── duckdb/       # Gold examples with principle fields
│   └── postgres/     # PostgreSQL-specific gold examples
├── models/           # FAISS similarity index (95 vectors) + metadata
├── sessions/         # Session drivers (standard, expert, swarm)
├── prompts/          # Swarm-specific prompt builders
├── benchmarks/       # Per-engine benchmark configs, queries, results
│   ├── duckdb_tpcds/ # Config, 99 queries, pairs, leaderboard, semantic_intents.json
│   └── postgres_dsb/ # Config, 52 queries, explains
└── docs/             # Architecture and reference documentation
```
