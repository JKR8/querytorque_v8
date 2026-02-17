# Snowflake DSB-76 Benchmark (Beam)

This benchmark targets the DSB-76 query corpus on Snowflake using the current Beam pipeline.

## What is included

- `queries/`: 76 DSB queries (copied from `postgres_dsb_76`)
- `config.json`: Snowflake engine + Beam defaults
- `knowledge/global_knowledge.json`: Snowflake global knowledge seed
- `manifest.json`: DSB-76 query manifest

## 1) Configure DSN

Edit `config.json` and replace both `dsn` and `benchmark_dsn`.

Format:

```text
snowflake://<user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>&role=<role>
```

## 2) Validate benchmark wiring

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli status snowflake_dsb_76
```

## 3) Collect explains (recommended before Beam)

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli collect-explains snowflake_dsb_76 --parallel 10 --timeout 300
```

## 4) Run new Beam

`snowflake` currently has a small gold-example set in this repo. For DSB-76 first runs,
use `--bootstrap` so Beam can run even when no tag-matched snowflake examples are found.

Single query smoke test:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run snowflake_dsb_76 -q query069_multi_i1 --mode beam --single-iteration --concurrency 1 --bootstrap
```

Batch run:

```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql python3 -m qt_sql.cli run snowflake_dsb_76 --mode beam --concurrency 8 --benchmark-concurrency 4 --bootstrap
```

## Notes

- DSB SQL is preserved from PostgreSQL corpus; if your Snowflake DSB schema differs, adjust queries or schema aliases first.
- `--config-boost` is PostgreSQL-only and should not be used for this benchmark.
