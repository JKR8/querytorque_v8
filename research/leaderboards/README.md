# Leaderboards — Single Source of Truth

All benchmark leaderboards live here. **Never create leaderboard data outside `research/leaderboards/`.**

## Structure

```
research/leaderboards/
├── *.html                    # Self-contained viewable leaderboards (double-click to open)
├── data/                     # JSON + CSV machine-readable data
├── prompts/                  # Prompt snapshots keyed by run name
│   └── {run_name}/
│       ├── MANIFEST.md
│       ├── knowledge/
│       ├── constraints/
│       └── examples/
└── scripts/                  # Build/snapshot scripts
```

## Naming Convention

`YYYYMMDD_{benchmark}_{label}.html` at root. Matching JSON/CSV in `data/`, matching prompt snapshot in `prompts/`.

## Current Leaderboards

| File | Benchmark | Engine | Queries | Wins |
|------|-----------|--------|---------|------|
| `20260209_duckdb_tpcds_v3_swarm.html` | TPC-DS SF10 | DuckDB | 101 | 92 |
| `20260212_pg_dsb_v2_combined.html` | DSB SF10 | PostgreSQL 14.3 | 52 | 29 |

## Scripts

- **`scripts/snapshot_prompts.py`** — Snapshot knowledge/constraints/examples for a given run
- **`scripts/build_production_pg_leaderboard.py`** — Build PG DSB production leaderboard from combined results
