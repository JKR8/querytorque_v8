# Snowflake DSB-76 Benchmark (Beam)

Canonical benchmark root:
`packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76`

## What To Use

- Config: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/config.json`
- Query corpus (76): `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/queries`
- Explain cache (76): `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/explains`
- Prepared prompt bundles: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/prepared`
- Beam API traces: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/beam_sessions`
- Run results: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/runs`
- Resume handoff bundle: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/resume_bundles`

Latest known artifacts (as of 2026-02-18):
- Latest run: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/runs/run_beam_20260218_102451`
- Latest beam session: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/beam_sessions/query054_multi_i1_20260218_102452`
- Latest prepared set: `packages/qt-sql/qt_sql/benchmarks/snowflake_dsb_76/prepared/20260218_091423`

## Current Pipeline Settings

- `validation_method`: `race`
- `semantic_validation_enabled`: `false`
- `snipe_rounds`: `2`
- `beam_edit_mode`: `tree`
- `wide_max_probes`: `32`
- `wide_worker_parallelism`: `32`
- `api_call_slots`: `400` (default query-level API concurrency; never serial by default)
- `benchmark_slots`: `8` (single benchmark slot knob)
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
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli status snowflake_dsb_76
```

Collect explains:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli collect-explains snowflake_dsb_76 --parallel 10 --timeout 300
```

Beam single query:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run snowflake_dsb_76 -q query054_multi_i1 --mode beam --single-iteration --bootstrap
```

Beam batch:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run snowflake_dsb_76 --mode beam --benchmark-concurrency 8 --bootstrap
```

Notes:
- If `--concurrency` is omitted, `qt run` uses `api_call_slots` from `config.json`.
- Current default for this benchmark is `api_call_slots=400`.
