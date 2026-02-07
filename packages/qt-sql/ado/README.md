# ADO — Autonomous Data Optimization

Iterative SQL optimization engine: parse query structure, retrieve similar examples via FAISS, generate rewrites with LLM, validate with real execution timing.

## Quick Start

```python
from ado import ADORunner, ADOConfig

runner = ADORunner(ADOConfig(
    benchmark_dir="benchmarks/duckdb_tpcds",
))
result = runner.run_query("query_1", sql)
print(f"{result.query_id}: {result.status} {result.speedup:.2f}x")
```

## Documentation

Full architecture, module reference, and configuration: **[docs/ADO_WORKFLOW.md](docs/ADO_WORKFLOW.md)**

## Structure

```
ado/
├── core/           # Python modules (pipeline, prompter, validator, etc.)
├── constraints/    # 11 optimization constraint rules (JSON)
├── examples/       # 31 gold examples + regression anti-patterns (JSON)
├── models/         # FAISS similarity index + metadata
├── benchmarks/     # Per-engine benchmark configs, queries, results
│   ├── duckdb_tpcds/    → leaderboard.json + leaderboard.md (auto-generated)
│   ├── postgres_dsb/    → leaderboard.json + leaderboard.md (auto-generated)
│   └── snowflake_tpcds/ → stub
├── learnings/      # Cross-benchmark learning records
└── docs/           # Architecture and reference documentation
```
