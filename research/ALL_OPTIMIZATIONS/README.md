# ALL_OPTIMIZATIONS: Complete History of Every Optimization Attempt

> Built: 2026-02-11 18:47
> Total: 1743 optimization attempts

## Purpose

This folder contains EVERY optimization attempt we ever made, not just the best.
For the best-per-query gold collection, see `research/GOLD/`.

## Structure

```
ALL_OPTIMIZATIONS/
├── duckdb_tpcds/
│   ├── index.json
│   ├── q1/
│   │   ├── original.sql          # Base query
│   │   ├── attempts.json         # Index of all attempts
│   │   ├── kimi/
│   │   │   ├── optimized.sql
│   │   │   └── meta.json
│   │   ├── kimi_extended/
│   │   ├── v2_standard/
│   │   ├── swarm_w1/
│   │   ├── swarm_w2/
│   │   ├── retry_neutrals_w1/
│   │   └── ...
├── postgres_dsb/
│   └── (same structure)
└── README.md
```

## Sources Collected

### DuckDB TPC-DS
| Source | Description | Queries |
|--------|------------|---------|
| kimi | Kimi K2.5 full benchmark | 99 |
| kimi_extended | Kimi with DAG prompts | 99 |
| v1_standard | V1 pipeline standard | 17 |
| v2_standard | V2 pipeline standard | 88 |
| swarm_w[1-4] | Swarm batch workers | ~101 each |
| swarm_final | Swarm best-of selection | ~101 |
| swarm_snipe | Swarm targeted improvement | ~101 |
| retry_neutrals_w[1-4] | 4-worker retry on neutrals | ~43 each |
| retry_collect_w[1-4] | 3-worker retry on regressions | ~25 each |
| retry_under1_3x_w[1-4] | Retry on <1.3x queries | ~44 each |
| retry_sf10_winners_w[1-4] | Retry SF10 validated wins | ~17 each |
| analyst_v1 | V1 analyst mode | 5 |
| analyst_winner | Analyst validated winners | 2 |
| analyst_session_iter* | Multi-iteration analyst | varies |
| session_* | Swarm session iterations | varies |

### PostgreSQL DSB
| Source | Description | Queries |
|--------|------------|---------|
| swarm1_w[1-4] | Swarm batch 1 workers | ~52 each |
| swarm2_w[1-4] | Swarm batch 2 workers | ~52 each |
| session_* | Swarm session iterations | 7 |
