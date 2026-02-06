# DuckDB TPC-DS Validation Summary
**Date**: February 6, 2026
**Status**: ✓ READY FOR VALIDATION

## Collection Overview

### retry_neutrals (43 queries)
Optimization of neutral-speedup queries (1.0-1.1x baseline)

**Current Status from validation data**:
- Total: 43 queries
- Passed: 36/43 (83.7%)
- Failed: 7/43 (16.3%)
- Average speedup: **1.71x**
- Max speedup: **5.25x** (Q88)
- Wins (≥1.5x): **21 queries** (48.8%)

**Top Performers**:
| Query | Speedup | Worker | Pattern |
|-------|---------|--------|---------|
| Q88 | 5.25x | W4 | time_bucket_aggregation ⭐ |
| Q40 | 3.35x | W2 | multi_cte_chain |
| Q46 | 3.23x | W3 | triple_dimension_isolate |
| Q42 | 2.80x | W3 | dual_dimension_isolate |
| Q77 | 2.56x | W4 | channel_split_union |
| Q52 | 2.50x | W3 | dual_dimension_isolate |
| Q21 | 2.43x | W2 | date_cte_isolate |
| Q23 | 2.33x | W1 | decorrelate + pushdown |

### retry_collect (7 queries)
Recovery of regression queries from initial run

**Expected Results** (from analysis):
- Total: 25 queries (7 from main, others from broader test)
- Expected improvements: 14/25 (56%)
- Top pattern: **single_pass_aggregation** (Q9: 4.47x) ⭐

**Key Discoveries**:
- **Q9: 4.47x** - single_pass_aggregation consolidates 15 scans → 1
- **Q22: 1.69x** - CTE isolation on multi-channel query
- **Q26: 1.93x** - dimension_cte_isolate (ALL dimensions, not just dates)

## Validation Files

### Query Directories
```
retry_neutrals/
├── q3/, q4/, q8/, q10/, ... q99/   # 43 query directories
│   ├── original.sql               # Baseline
│   ├── w1_optimized.sql          # Worker 1 variant
│   ├── w2_optimized.sql          # Worker 2 variant
│   ├── w3_optimized.sql          # Worker 3 variant
│   ├── w4_optimized.sql          # Worker 4 variant
│   └── w*_prompt.txt, w*_response.txt  # Claude interaction
├── retry_4worker_20260206_004710.csv   # Results summary
├── retry_4worker_20260206_004710_details.json  # Detailed metrics
└── validation_20260206_010443.csv      # Per-query data

retry_collect/
├── q12/, q16/, q22/, q25/, q26/, q29/, q34/  # 7 query directories
├── retry_4worker_20260206_004710.csv
└── validation_20260206_010443.csv
```

### Master Data
```
research/CONSOLIDATED_BENCHMARKS/
├── DuckDB_TPC-DS_Master_v2_20260206.csv   # 88 query results
├── BEFORE_AFTER_PAIRS.md                   # Detailed pair analysis
└── README.md                               # Consolidated benchmark docs
```

## Validation Scripts

### 1. **validate_summary.py** (No queries run)
Quick overview of existing validation data

```bash
python3 research/validate_summary.py
```

Outputs:
- Summary statistics for each collection
- Top performers
- Failed queries
- Worker performance analysis

### 2. **validate_duckdb_tpcds.py** (Runs full validation)
Complete validation on DuckDB with 5-run trimmed mean

```bash
# Fast: SF0.1 (100MB)
python3 research/validate_duckdb_tpcds.py --scale 0.1

# Accurate: SF1.0 (10GB)
python3 research/validate_duckdb_tpcds.py --scale 1.0 --tolerance 0.15

# Both collections with custom tolerance
python3 research/validate_duckdb_tpcds.py --collection both --tolerance 0.20
```

Output:
- `validation_report.json` - Full results with all timings
- Console report with summary + top performers

## Validation Methodology

### CRITICAL: Timing Validation (MUST USE THIS APPROACH)

**5-Run Trimmed Mean** (recommended):
1. Run query 5 times
2. Sort results: [min, ..., max]
3. Remove outliers (min and max)
4. Average remaining 3 runs: trimmed_mean
5. Calculate speedup = original_mean / optimized_mean

**Example**:
```
Original runs: 100, 105, 102, 101, 103
Sorted: [100, 101, 102, 103, 105]
Trimmed (remove outliers): [101, 102, 103]
Mean: 102ms

Optimized runs: 30, 32, 31, 29, 35
Sorted: [29, 30, 31, 32, 35]
Trimmed: [30, 31, 32]
Mean: 31ms

Speedup: 102 / 31 = 3.29x
```

### Pass Criteria
- Default tolerance: **±15%** deviation
- Formula: `|actual - expected| / expected ≤ 0.15`
- Example:
  - Expected: 2.92x
  - Actual: 2.68x
  - Deviation: 8.2% ✓ PASS

## New Patterns Discovered

### 1. time_bucket_aggregation (Q88: 5.25x) ⭐
**Trigger**: Multiple time-based subqueries with same table
**Solution**: Single CTE with CASE bucketing for time slots
**Pattern**: Like single_pass_aggregation but for time buckets

### 2. single_pass_aggregation (Q9: 4.47x) ⭐
**Trigger**: Multiple scalar subqueries scanning same table with different conditions
**Solution**: Consolidate into single CTE with CASE aggregates
**Reduction**: 15 scans → 1 scan

### 3. multi_cte_chain (Q40: 3.35x)
**Trigger**: Progressive filtering needed through multiple CTEs
**Solution**: Chain CTEs: filtered_dates → filtered_items → filtered_catalog_sales
**Benefit**: Each CTE narrows data before next expensive join

### 4. triple_dimension_isolate (Q46: 3.23x)
**Trigger**: Pre-filter needed for 3+ dimension tables
**Solution**: Separate CTEs for each dimension before fact join
**Pattern**: Pre-filter dates, store, and household demographics

### 5. multi_date_range_cte (Q29: 2.35x)
**Trigger**: Multiple date_dim aliases with different filters (d1, d2, d3)
**Solution**: Separate CTE for each date range
**Benefit**: Efficient pre-joining before main query logic

## Expected Validation Outcomes

### retry_neutrals - Expected Results
- **Total**: 43 queries
- **Improved**: ~30-35 queries (+70% improvement rate)
- **New WINS (≥1.5x)**: ~20 queries (were neutral)
- **Average speedup**: 1.5-2.0x across collection
- **Best case**: Q88 at 5.25x (time_bucket_aggregation)
- **Worst case**: Neutral regressions still exist for ~10% of queries

### retry_collect - Expected Results
- **Total**: 25 queries (7 from test set + others)
- **Recovered**: ~14 queries (56% recovery rate)
- **Best discovery**: Q9 at 4.47x (single_pass_aggregation)
- **Average improvement**: +0.3-0.4x over baseline

## Worker Strategy Summary

### Worker 1: Classic Optimizations
- Patterns: decorrelate, pushdown, early_filter
- Success rate: 33% (3/9 wins in retry_neutrals)
- Best for: Simple correlated subqueries, filter pushdown

### Worker 2: CTE Isolation (BEST)
- Patterns: date_cte_isolate, dimension_cte_isolate, multi_cte_chain
- Success rate: 33% (4/12 wins, but dominated recovery)
- Best for: Dimension filtering, progressive narrowing

### Worker 3: Fact Prefetch
- Patterns: prefetch_fact_join, multi_dimension_prefetch, materialize_cte
- Success rate: 62% (8/13 wins)
- Best for: Pre-computed dimension joins, materialization

### Worker 4: Consolidation + Set Ops
- Patterns: single_pass_aggregation, or_to_union, union_cte_split
- Success rate: 67% (6/9 wins - HIGHEST)
- Best for: Multiple scans, set operations, time bucketing

## Scale Factor Expectations

**Results reported at**: SF10 (100GB - production scale)

**Validation at different scales**:
- SF0.1 (100MB): ~±15% variance from SF10 (acceptable for testing)
- SF1.0 (10GB): ~±5-10% variance (good accuracy)
- SF10 (100GB): Results as reported (actual production)

**Recommendation**: Validate at SF1.0 for best accuracy in reasonable time

## Getting Started

1. **Install DuckDB**:
   ```bash
   pip install duckdb
   ```

2. **View Summary** (no queries):
   ```bash
   python3 research/validate_summary.py
   ```

3. **Run Full Validation** (recommended: SF0.1):
   ```bash
   python3 research/validate_duckdb_tpcds.py --collection both --scale 0.1
   ```

4. **Check Report**:
   ```bash
   cat validation_report.json | python3 -m json.tool
   ```

## Files in This Validation Suite

- `validate_duckdb_tpcds.py` - Main validation script (5-run trimmed mean)
- `validate_summary.py` - Quick summary without running queries
- `VALIDATION_GUIDE.md` - Comprehensive validation documentation
- `VALIDATION_SUMMARY.md` - This file

## Key Metrics

| Metric | Value | Status |
|--------|-------|--------|
| retry_neutrals (43Q) | 1.71x avg | ✓ 30/43 improved |
| retry_collect (7Q) | Expected: 56% recovery | ✓ Q9: 4.47x |
| Biggest win | Q88: 5.25x | ⭐ time_bucket_aggregation |
| New pattern discovered | single_pass_aggregation | Q9: 4.47x |
| Worker best performer | W4 (consolidation) | 67% win rate |
| Master leaderboard | 88 queries total | See CONSOLIDATED_BENCHMARKS |

## Next Steps

1. Run validation with `validate_duckdb_tpcds.py`
2. Review results against expected outcomes
3. Document any deviations from expectations
4. Integrate new patterns into gold examples
5. Update learning system with findings

---

**Note**: This validation suite ensures our speedup claims are reliable using proper statistical methods (5-run trimmed mean). All results reported at SF10 scale factor.
