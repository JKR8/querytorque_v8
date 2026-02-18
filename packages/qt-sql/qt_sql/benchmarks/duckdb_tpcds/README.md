# DuckDB TPCDS Benchmark (Beam)

Canonical benchmark root:
`packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds`

## What To Use

- Config: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/config.json`
- Query corpus (101): `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries`
- Explain cache (101): `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/explains`
- Prepared prompt bundles: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/prepared`
- Beam API traces: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/beam_sessions`
- Run results: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/runs`

Latest known artifacts (as of 2026-02-18):
- Latest run: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/runs/run_beam_20260217_200502`
- Latest beam session: `packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/beam_sessions/query_35_20260216_132452`

## Current Pipeline Settings

- `validation_method`: `race`
- `semantic_validation_enabled`: `false`
- `snipe_rounds`: `2`
- `beam_edit_mode`: `tree`
- `wide_max_probes`: `32`
- `wide_worker_parallelism`: `32`
- `benchmark_slots`: `4` (single benchmark slot knob)
- `beam_qwen_workers`: `15`
- `beam_reasoner_workers`: `1`

## Stage Order (Current)

1. Analyst + worker API calls (save all prompts/responses).
2. Worker validation gates.
3. Worker benchmark batch.
4. Compiler API calls.
5. Compiler validation gates.
6. Compiler benchmark batch.

## Basic Commands

Status:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli status duckdb_tpcds
```

Collect explains:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli collect-explains duckdb_tpcds --parallel 10 --timeout 300
```

Beam single query:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run duckdb_tpcds -q query_35 --mode beam --single-iteration --concurrency 1
```

Beam batch:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run duckdb_tpcds --mode beam --concurrency 4 --benchmark-concurrency 4
```
