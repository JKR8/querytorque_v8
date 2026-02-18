# Benchmark Artifact Index

Use this file to quickly find benchmark artifacts and the engine-specific README.

## Benchmark Roots

- DuckDB TPCDS: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds`
- PostgreSQL DSB-76: `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76`
- Snowflake DSB-76: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76`

## Engine READMEs

- DuckDB: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/README.md`
- PostgreSQL: `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/README.md`
- Snowflake: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/README.md`

## Common Artifact Paths (inside each benchmark root)

- `config.json`:
  runtime benchmark configuration
- `queries/`:
  canonical query corpus
- `explains/`:
  cached explain / explain analyze artifacts
- `prepared/`:
  prepared prompt snapshots and preparation summaries
- `beam_sessions/`:
  saved API call prompts/responses and stage artifacts
- `runs/`:
  benchmark outputs and final result JSONs
- `resume_bundles/` (if present):
  handoff checkpoints with manifest/timing index

## Current Slot Policy

- Use `api_call_slots` as the default query-level API concurrency knob.
- `qt run` uses `api_call_slots` when `--concurrency` is omitted.
- Use `benchmark_slots` as the single benchmark slot knob.
- Engine defaults:
  - PostgreSQL DSB-76: `api_call_slots=400`
  - Snowflake DSB-76: `api_call_slots=400`
- Benchmark lane defaults (`benchmark_slots`):
  - DuckDB: `4`
  - PostgreSQL: `4`
  - Snowflake: `8`
