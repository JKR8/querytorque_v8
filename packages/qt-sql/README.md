# qt-sql

SQL optimization and analysis engine for QueryTorque.

## Optimization Modes

Three modes for LLM-powered query optimization:

### Oneshot
1 LLM call per iteration — the analyst analyzes the query AND produces optimized SQL in a single prompt. Cheapest mode. Optional retry with failure context on subsequent iterations.

```python
result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.ONESHOT)
```

### Expert (default)
Iterative with analyst failure analysis. Each iteration: analyst briefing → 1 worker rewrite → validate → failure analysis if below target. History accumulates across iterations so the LLM learns from prior failures.

```python
result = p.run_optimization_session("query_1", sql, mode=OptimizationMode.EXPERT)
```

### Swarm
Multi-worker fan-out: analyst distributes 4 diverse strategies across 4 workers. If none hit target, a snipe round synthesizes failures into a refined attempt. Best for stubborn queries.

```python
result = p.run_optimization_session("query_88", sql, mode=OptimizationMode.SWARM)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  5-PHASE PIPELINE                                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Parse:     SQL → DAG (CTE topology, join contracts)        │
│  2. Retrieve:  Tag-based example matching (engine-specific)     │
│  3. Rewrite:   V2 analyst briefing → worker prompt → LLM       │
│  4. Syntax:    sqlglot parse gate (deterministic)              │
│  5. Validate:  Timing + row count + checksum (3-run or 5-run)  │
│                                                                 │
│  Sessions: OneshotSession | ExpertSession | SwarmSession       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  EXECUTION                                                      │
├─────────────────────────────────────────────────────────────────┤
│  DuckDB Executor  │  PostgreSQL Executor  │  (Snowflake stub)  │
│  3-run / 5-run trimmed mean benchmarking                       │
│  Row count + MD5 checksum equivalence checking                 │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  KNOWLEDGE                                                      │
├─────────────────────────────────────────────────────────────────┤
│  Engine profiles (DuckDB/PG optimizer gaps + strengths)        │
│  Gold examples (19 verified transforms, up to 5.25x)           │
│  Tag-based similarity matching (108 examples, no FAISS)        │
│  Regression warnings (anti-pattern avoidance)                  │
│  PG system introspection (SET LOCAL tuning, pg_hint_plan)      │
└─────────────────────────────────────────────────────────────────┘
```

## CLI

```bash
qt-sql audit <file.sql>              # Static analysis (119 rules)
qt-sql optimize <file.sql>           # LLM-powered optimization
qt-sql validate <orig.sql> <opt.sql> # Validate optimization
```

## Usage

```python
from qt_sql.pipeline import Pipeline
from qt_sql.schemas import OptimizationMode

p = Pipeline("ado/benchmarks/duckdb_tpcds")

# Run in any mode
result = p.run_optimization_session(
    "query_88", sql,
    mode=OptimizationMode.SWARM,
    max_iterations=3,
    target_speedup=2.0,
)
print(f"{result.status}: {result.best_speedup:.2f}x")

# Or via ADORunner wrapper
from qt_sql import ADORunner, ADOConfig
runner = ADORunner(ADOConfig(benchmark_dir="ado/benchmarks/duckdb_tpcds"))
result = runner.run_analyst("query_88", sql, mode=OptimizationMode.SWARM)
```
