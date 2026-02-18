# Postgres DSB-76 Resume Bundle

Created: 2026-02-18T08:22:14.149048

## Purpose
Freeze a restart-safe handoff of cached EXPLAIN timing for `postgres_dsb_76` without re-running EXPLAIN ANALYZE.

## Included
- `manifest.json`: bundle metadata and pickup pointers
- `explain_timing_index.json`: per-query timing index (76 queries)
- `explain_timing_index.csv`: same index in CSV

## Canonical Sources
- EXPLAIN cache: `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/explains`
- Latest full prepared prompts: `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/prepared/20260215_012857`
- Latest run dir: `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/runs/run_e2e_20260217_090602`

## Resume Guidance
1. Start PostgreSQL only when you're ready to run benchmark sessions.
2. Reuse this explain cache; do not refresh explains unless explicitly needed.
3. Run optimization as usual (for example: `qt run postgres_dsb_76 ...`).
