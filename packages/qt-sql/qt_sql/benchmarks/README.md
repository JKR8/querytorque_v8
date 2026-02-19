# Benchmark Artifact Index

Use this file to quickly find benchmark artifacts and the engine-specific README.

## Benchmark Roots

| Engine | Benchmark | Directory | Queries |
|--------|-----------|-----------|---------|
| DuckDB | TPC-DS SF10 | `duckdb_tpcds/` | 99 |
| PostgreSQL | DSB-76 SF10 | `postgres_dsb_76/` | 76 |
| Snowflake | DSB-76 SF10 | `snowflake_dsb_76/` | 76 |
| MySQL | DSB-76 SF10 | `mysql_dsb_76/` | 76 |

## Connection Details

### PostgreSQL 14.3 (compiled from source, no Docker)

```
DSN:    postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10
Host:   127.0.0.1:5434
User:   jakc9 / jakc9
DB:     dsb_sf10
Data:   /home/jakc9/pgdata14
Binary: /mnt/d/pg14.3/bin/
Start:  /mnt/d/pg14.3/bin/pg_ctl -D /home/jakc9/pgdata14 -l /home/jakc9/pgdata14/logfile start
Stop:   /mnt/d/pg14.3/bin/pg_ctl -D /home/jakc9/pgdata14 stop
Shell:  psql -h 127.0.0.1 -p 5434 -U jakc9 dsb_sf10
```

### MySQL 8.0.45 (Docker container)

```
DSN:       mysql://root:dsb2026@127.0.0.1:3306/dsb_sf10
Container: mysql-dsb
Host:      127.0.0.1:3306
User:      root / dsb2026
DB:        dsb_sf10
Data:      /mnt/d/mysqldata (bind-mounted into container)
Start:     docker start mysql-dsb
Stop:      docker stop mysql-dsb
Shell:     docker exec -it mysql-dsb mysql -uroot -pdsb2026 dsb_sf10
Config:    8GB innodb_buffer_pool_size, 1GB innodb_log_file_size, SSL disabled
```

### DuckDB (embedded, file-based)

```
Path:   /mnt/d/TPC-DS/tpcds_sf10.duckdb  (TPC-DS SF10)
        /mnt/d/dsb/dsb_sf10.duckdb       (DSB SF10)
Shell:  duckdb /mnt/d/TPC-DS/tpcds_sf10.duckdb
```

### Snowflake (cloud)

```
Account:    CVRYJTF-AW47074
User:       jkdl
Warehouse:  COMPUTE_WH (X-Small)
Database:   SNOWFLAKE_SAMPLE_DATA
Schema:     TPCDS_SF10TCL
DSN:        snowflake://jkdl@CVRYJTF-AW47074/SNOWFLAKE_SAMPLE_DATA/TPCDS_SF10TCL
```

## DSB Data

All engines use the same DSB SF10 dataset (12GB raw):

```
Source:     /mnt/d/dsb_sf10/*.dat (25 tables, pipe-delimited)
Schema:    /mnt/d/dsb/scripts/create_tables.sql
PG Index:  /mnt/d/dsb/scripts/dsb_index_pg.sql
MySQL Idx: scripts/create_mysql_indexes.sql
```

Key table sizes (SF10):
- store_sales: 28.8M rows
- catalog_sales: 14.4M rows
- web_sales: 7.2M rows
- inventory: 146M rows

## Common Artifact Paths (inside each benchmark root)

- `config.json`: runtime benchmark configuration
- `queries/`: canonical query corpus (.sql files)
- `explains/`: cached EXPLAIN ANALYZE artifacts
- `prepared/`: prepared prompt snapshots
- `beam_sessions/`: API call prompts/responses and stage artifacts
- `runs/`: benchmark outputs and final result JSONs
- `baseline_timing.json`: per-query baseline timings (warmup + measured)

## Current Slot Policy

- `api_call_slots`: query-level API concurrency (default for `qt run`)
- `benchmark_slots`: benchmark lane concurrency
- Engine defaults:
  - PostgreSQL DSB-76: `api_call_slots=400`, `benchmark_slots=4`
  - MySQL DSB-76: `api_call_slots=400`, `benchmark_slots=4`
  - Snowflake DSB-76: `api_call_slots=400`, `benchmark_slots=8`
  - DuckDB TPC-DS: `benchmark_slots=4`
