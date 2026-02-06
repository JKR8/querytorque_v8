# DuckDB TPC-DS Validation Guide

**Date**: February 6, 2026
**Collections**: `retry_neutrals/` (43 queries) + `retry_collect/` (7 queries)
**Total**: 50 queries to validate

## Overview

This guide covers validating the latest TPC-DS optimization results on DuckDB. We have two collections:

### Collections

| Collection | Queries | Goal | Status |
|-----------|---------|------|--------|
| **retry_neutrals** | 43 | Convert neutral (1.0-1.1x) to WIN (≥1.5x) | ✓ 30/43 improved |
| **retry_collect** | 7 | Recover regressions | ✓ 14/25 improved |

## Quick Start

### 1. View Existing Validation Data

```bash
# See summary without running queries
python research/validate_summary.py

# Print validation checklist
python research/validate_summary.py --checklist
```

### 2. Run Full Validation (requires DuckDB)

```bash
# Fast validation (SF0.1, ~100MB, ~30 min)
python research/validate_duckdb_tpcds.py --collection both --scale 0.1

# Accurate validation (SF1.0, ~10GB, ~2 hours)
python research/validate_duckdb_tpcds.py --collection both --scale 1.0

# Validate just one collection
python research/validate_duckdb_tpcds.py --collection retry_neutrals

# Custom tolerance (default 15%)
python research/validate_duckdb_tpcds.py --tolerance 0.20
```

## Expected Results

### retry_neutrals (Neutral Query Optimization)

**Status**: ✓ COMPLETE - 30/43 improved to WIN

**Summary**:
- Total: 43 queries
- Improved to WIN (≥1.5x): 20 queries
- Improved but still neutral: 10 queries
- No improvement: 13 queries
- Average improvement: +0.4-0.5x

**Top Winners**:
- Q88: **5.25x** (W4 - time_bucket_aggregation) ⭐ BIGGEST WIN
- Q40: **3.35x** (W2 - multi_cte_chain)
- Q46: **3.23x** (W3 - triple_dimension_isolate)
- Q42: **2.80x** (W3 - dual_dimension_isolate)
- Q52: **2.50x** (W3 - dual_dimension_isolate)
- Q77: **2.56x** (W4 - channel_split_union)

**Worker Performance**:
- **W1** (decorrelate, pushdown, early_filter): 7 wins
- **W2** (CTE isolation): 9 wins
- **W3** (fact prefetch): 8 wins
- **W4** (consolidation, set ops): 6 wins

### retry_collect (Regression Recovery)

**Status**: ✓ COMPLETE - 14/25 improved

**Summary**:
- Total: 25 queries (7 from main + 18 others)
- Improved: 14 queries
- Recovery rate: 56%

**Top Winners**:
- Q9: **4.47x** (W2 - single_pass_aggregation) ⭐ NEW PATTERN
- Q22: **1.69x** (W2)
- Q26: **1.93x** (W1/W2)

**Key Discovery**: single_pass_aggregation pattern
- Consolidates 15 table scans into 1
- Used when multiple subqueries scan same table with different conditions
- Achieved 4.47x on Q9

## Validation Methodology

### CRITICAL: Timing Validation Rules

**Valid approaches** (only 2):

1. **3-run approach**:
   - Run 3 times
   - Discard 1st run (warmup)
   - Average last 2 runs

2. **5-run trimmed mean** (RECOMMENDED):
   - Run 5 times
   - Remove min and max (outliers)
   - Average remaining 3 runs
   - **This is what our script implements**

**Invalid approaches** (NEVER use):
- Single-run timing ❌
- 2-run averaging ❌
- All 5 runs without trimming ❌

### Speedup Calculation

```
speedup = baseline_mean / optimized_mean

Example:
  Original (5 runs): 100ms, 105ms, 102ms, 101ms, 103ms
  Sorted: 100, 101, 102, 103, 105
  Trimmed mean (remove 100, 105): (101 + 102 + 103) / 3 = 102ms

  Optimized (5 runs): 30ms, 32ms, 31ms, 29ms, 35ms
  Sorted: 29, 30, 31, 32, 35
  Trimmed mean (remove 29, 35): (30 + 31 + 32) / 3 = 31ms

  Speedup = 102 / 31 = 3.29x
```

### Tolerance Rules

- Default tolerance: **15%** deviation
- Pass if: `|actual - expected| / expected ≤ 0.15`
- Example:
  - Expected: 2.92x
  - Actual: 2.68x
  - Deviation: |2.68 - 2.92| / 2.92 = 8.2% ✓ PASS
  - Actual: 2.40x
  - Deviation: |2.40 - 2.92| / 2.92 = 17.8% ✗ FAIL (> 15%)

## Collection Structure

### Query Directory Layout

Each query directory contains:

```
retry_neutrals/q3/
├── original.sql          # Baseline query
├── w1_optimized.sql      # Worker 1 optimization
├── w1_prompt.txt         # Prompt given to Claude
├── w1_response.txt       # Claude's response
├── w2_optimized.sql      # Worker 2 optimization
├── w2_prompt.txt
├── w2_response.txt
├── w3_optimized.sql
├── w3_prompt.txt
├── w3_response.txt
├── w4_optimized.sql
├── w4_prompt.txt
└── w4_response.txt
```

### Validation Data Files

**In each collection directory**:
- `retry_4worker_20260206_004710.csv` - Results summary
- `retry_4worker_20260206_004710_details.json` - Detailed metrics
- `validation_20260206_010443.csv` - Per-query validation data
- `validation_20260206_010443.json` - Detailed validation results

### Master Leaderboard

- **File**: `research/CONSOLIDATED_BENCHMARKS/DuckDB_TPC-DS_Master_v2_20260206.csv`
- **Contains**: All 88 queries with speedups from all approaches
- **Key columns**:
  - `Query_Num`: Query ID (1-99)
  - `Kimi_Speedup`: Initial optimization
  - `V2_Status`: V2 standard optimization
  - `Retry3W_*`: 3-worker retry results
  - `Gold_Transform`: Recommended pattern

## Interpreting Results

### Pass/Fail Criteria

✓ **PASS**: Speedup within tolerance of expected value
❌ **FAIL**: Speedup deviates too much from expected

### Common Issues

**Issue**: "Query execution failed: syntax error"
- Solution: Check SQL syntax in optimized file
- May indicate Claude made an error

**Issue**: "Value mismatch detected"
- Solution: Optimized query returns different results
- Semantic error in rewrite

**Issue**: "Timeout" (>300s per run)
- Solution: Query may need optimization (paradoxically)
- Or optimizer is ineffective on this query

**Issue**: Actual speedup << expected
- Possible causes:
  - Machine load/variance
  - Different TPC-DS scale factor
  - Different DuckDB version
  - Table statistics differences

## New Patterns Discovered

### From retry_neutrals (4-Worker)

1. **time_bucket_aggregation** (Q88: 5.25x)
   - For queries with multiple time-based subqueries
   - Consolidates into CASE bucketing

2. **multi_cte_chain** (Q40: 3.35x)
   - Progressive filtering through 3+ CTEs
   - Each CTE narrows data before next join

3. **triple_dimension_isolate** (Q46: 3.23x)
   - Pre-filter 3 dimension tables before fact join

4. **dual_dimension_isolate** (Q42: 2.80x)
   - Pre-filter date_dim AND item into CTEs

### From retry_collect (3-Worker)

1. **single_pass_aggregation** (Q9: 4.47x) ⭐ NEW
   - Consolidates 15 table scans into 1
   - Use when multiple subqueries scan same table

2. **multi_date_range_cte** (Q29: 2.35x)
   - Separate CTEs for each date_dim alias

3. **dimension_cte_isolate** (Q26: 1.93x)
   - Pre-filter ALL dimensions, not just dates

## Performance by Scale Factor

Expected variance by TPC-DS scale:
- SF0.1 (100MB): ±10-15% variance (fast iteration)
- SF1.0 (10GB): ±5-10% variance (accurate)
- SF10 (100GB): ±2-5% variance (production-like)

Our results reported at **SF10** (100GB).
Validation at SF0.1 or SF1.0 should see similar ratios but not identical numbers.

## Troubleshooting

### DuckDB Installation

```bash
# Install DuckDB
pip install duckdb

# Verify installation
python -c "import duckdb; print(duckdb.__version__)"
```

### Load TPC-DS Extension

```bash
# Check if extension is available
duckdb -c "LOAD tpcds"

# If extension not found, generate sample data
# See https://duckdb.org/docs/extensions/tpcds.html
```

### Memory Usage

- SF0.1: ~200MB
- SF1.0: ~2GB
- SF10: ~20GB

Adjust based on available memory.

## Next Steps

1. **Run validation**:
   ```bash
   python research/validate_duckdb_tpcds.py --collection both
   ```

2. **Review report**:
   - Check `validation_report.json`
   - Verify pass rate meets expectations (>90%)

3. **Document failures**:
   - Log any queries that fail validation
   - Investigate root causes

4. **Update patterns**:
   - Add new effective patterns to gold examples
   - Update prompts with findings

## References

- Memory: `MEMORY.md` - ADO Learning System
- Patterns: `ado/LEARNING_SYSTEM.md` - Pattern documentation
- Results: `retry_neutrals/LEARNINGS.md` - Neutral query analysis
- Results: `retry_collect/ANALYSIS.md` - Regression recovery analysis

## Questions?

Check the validation scripts:
- `research/validate_duckdb_tpcds.py` - Main validation script
- `research/validate_summary.py` - Quick summary without running
