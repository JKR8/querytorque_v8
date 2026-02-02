# Benchmark Methodology

## Overview

QueryTorque benchmarks measure SQL optimization effectiveness using the industry-standard TPC-DS benchmark suite.

---

## Test Environment

| Component | Specification |
|-----------|---------------|
| **Database** | DuckDB |
| **Scale Factor** | SF100 (100GB) |
| **Data Location** | `D:\TPC-DS\tpcds_sf100.duckdb` |
| **Query Source** | `D:\TPC-DS\queries_duckdb_converted\` |
| **Queries** | 99 TPC-DS standard queries |

---

## Benchmark Protocol

### Execution

1. **Warmup**: 1 run (discarded)
2. **Benchmark**: 3 runs (median taken)
3. **Timing**: Wall clock time from query submission to result

### Validation

Two-phase validation ensures semantic correctness:

1. **Syntax Validation**: Query parses and executes without error
2. **Semantic Validation**: Results match original query
   - Row count comparison
   - Checksum comparison (1-1-2-2 sample)
   - Full result comparison (optional)

---

## Metrics

| Metric | Formula | Interpretation |
|--------|---------|----------------|
| **Speedup** | `original_time / optimized_time` | >1.0 = faster |
| **Win** | Speedup >= 1.2x | Meaningful improvement |
| **Regression** | Speedup < 1.0x | Optimization made it slower |
| **Validated** | Passes semantic check | Results are correct |

---

## Pattern Codes

| Code | Pattern | Description |
|------|---------|-------------|
| `UNION` | UNION ALL Decomposition | Split OR conditions into separate branches |
| `CTE_DATE` | Early Date Filtering | Filter date_dim in CTE before joining |
| `MAT_CTE` | Materialized CTE | Force early materialization |
| `CTE` | Generic CTE | Restructure with CTEs |
| `PRED` | Predicate Pushdown | Move filters closer to base tables |
| `EXISTS` | EXISTS Conversion | Convert IN to EXISTS |
| `JOIN_ORD` | Join Reordering | Optimize join order |
| `SCAN` | Scan Consolidation | Combine multiple scans |
| `ORIG` | Keep Original | Original was optimal |

---

## Adding New Benchmark Results

1. Copy `_template.md` to `{provider}/{date}.md`
2. Fill in the results table
3. Update `../../BENCHMARKS.md` with summary
4. Commit both files

---

## Result File Format

Each result file should include:

- **Header**: Provider, date, model version
- **Summary table**: Key metrics
- **Full results table**: All 99 queries
- **Failed validations**: Queries with semantic errors
- **Top wins**: Queries with >=1.2x speedup
- **Run configuration**: Model settings, parameters

See `_template.md` for the standard format.
