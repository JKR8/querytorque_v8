# ADO Swarm Pipeline Smoke Test Report

**Query**: `query_42`
**Engine**: DuckDB SF10
**Date**: 2026-02-08 10:07:45
**Duration**: 0.4s
**Status**: FAIL (9/9 checks)

## Step Summary

| Step | Name | Time | Checks | Status |
|-----:|------|-----:|-------:|:------:|
| 01 | Load Configuration | 0.1s | 6/6 | PASS |
| 02 | Load Query SQL | 0.1s | 3/3 | PASS |
| 03 | Parse SQL into DAG + EXPLAIN costs. | 0.3s | 0/0 | **FAIL** |

## Code Review Finding Coverage

| Finding | Description | Checks | Status |
|---------|-------------|-------:|:------:|
| F5 | Structured logging (logger exists) | 1/1 | PASS |

## Step Details

### Step 01: Load Configuration

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| engine_duckdb | PASS |  | duckdb |
| db_exists | PASS |  | /mnt/d/TPC-DS/tpcds_sf10.duckdb |
| timeout_positive | PASS |  | 300 |
| validation_method_valid | PASS |  | 3-run |
| scale_factor_10 | PASS |  | 10 |
| pipeline_has_logger | PASS | F5 | Logger |

### Step 02: Load Query SQL

| Check | Result | Finding | Detail |
|-------|:------:|:-------:|--------|
| sql_non_empty | PASS |  |  |
| sql_has_select | PASS |  |  |
| sql_parses | PASS |  |  |

### Step 03: Parse SQL into DAG + EXPLAIN costs.

**ERROR**: `AttributeError: 'str' object has no attribute 'id'`


## Artifact Inventory

- `01_config/` (2 files)
  - `contract.json`
  - `output.json`
- `02_query/` (3 files)
  - `contract.json`
  - `input.json`
  - `output.sql`
- `03_03_dag/` (1 files)
  - `traceback.txt`
- `03_dag/` (1 files)
  - `input.sql`
