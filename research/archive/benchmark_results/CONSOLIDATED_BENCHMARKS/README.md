# DuckDB TPC-DS Consolidated Benchmark Knowledge Base

**Version**: v1
**Created**: 2026-02-05
**Coverage**: 256 before/after SQL pairs from 99 TPC-DS queries
**Total Size**: 2.47 MB

---

## BENCHMARK CONSOLIDATION POLICY

⚠️ **ALL BENCHMARKS MUST GO IN THIS DIRECTORY**

If you are running benchmarks (DuckDB, PostgreSQL, MySQL, Deepseek, Claude, etc.):
1. Create directory: `CONSOLIDATED_BENCHMARKS/{system}_benchmark_{date}`
2. Store all result files, queries, logs here
3. Add metadata file describing the run
4. Update this README with the new benchmark source

**Current Benchmarks in This Directory**:
- `benchmark_output_v2/` - Latest comprehensive V2 Standard (88 queries)
- `benchmark_output_v1_standard/` - V1 Standard iterations (17 queries × 5 iterations)
- `kimi_q1-q30_optimization/` - Kimi K2.5 optimization attempt Q1-Q30 (30 queries)
- `kimi_q31-q99_optimization/` - Kimi K2.5 optimization attempt Q31-Q99 (69 queries)

---

## Overview

This is the **consolidated knowledge base** for understanding query optimization across all databases and models. It contains 256 before/after SQL pairs and the full history of optimization attempts, failures, and successes.

**Purpose**: Mine patterns to understand WHAT WORKS and WHY, enabling better prompt injection and optimization strategies across all systems.

### Quick Facts
- **Total Queries**: 99 (Q1-Q99)
- **Success Rate**: 13.1% (13/99 queries show improvement ≥1.2x)
- **Gold Examples**: 6 verified high-value optimizations
- **Failure Rate**: 47.5% (47/99 show regression or validation failure)
- **Coverage**: 100% metrics, 89% SQL artifacts

---

## Files in This Knowledge Base

### Data Files
| File | Purpose | Size | Notes |
|------|---------|------|-------|
| `DuckDB_TPC-DS_Master_v1_20260205.csv` | Core metrics (21 columns) | 50 KB | Main analysis file |
| `DuckDB_TPC-DS_SQL_v1_20260205.csv` | Full SQL source code | 0.3 MB | Original + optimized SQL |
| `DuckDB_TPC-DS_Master_v1_METADATA.json` | Version & source info | 5 KB | Data lineage tracking |

### Analysis Documents
| File | Purpose |
|------|---------|
| `SCHEMA.md` | Column definitions and data sources |
| `PATTERNS.md` | Transform effectiveness analysis |
| `GOLD_EXAMPLES.md` | High-value optimizations (Q1, Q15, Q39, etc.) |
| `FAILURES.md` | Failure patterns and regressions |
| `README.md` | This file |

---

## Data Sources

### 1. Kimi K2.5 Full Benchmark (Feb 2, 2026)
- **Coverage**: All 99 queries
- **Validation**: Full SF100 dataset (100% coverage)
- **Speedup**: Verified with actual execution
- **Status**: Gold standard - this is our source of truth

### 2. V2 Standard Mode (Feb 5, 2026)
- **Coverage**: 88 queries (missing Q3, 4, 5, 6, 8, 9, 11, 12, 14, 15, 17)
- **Approach**: Direct LLM optimization with DAG v3 prompts
- **SQL Artifacts**: Complete (original + optimized)
- **Time**: ~3 hours total (12:24-12:52 UTC)

### 3. V2 Evolutionary Mode (Feb 5, 2026)
- **Coverage**: 15 queries (Q2-Q16) using MCTS search
- **Approach**: Multiple iterations seeking better solutions
- **Overlap**: Q2, Q7, Q10, Q13, Q16 also in standard mode
- **Value**: Different optimization approaches for comparison

---

## Classification System

Each query is classified for pattern mining:

| Classification | Count | Meaning |
|---|---|---|
| **GOLD_EXAMPLE** | 6 | Verified high-value (Q1, Q15, Q39, Q74, Q90, Q93) |
| **MODERATE_WIN** | 7 | Speedup 1.2-1.5x (meaningful improvement) |
| **NEUTRAL** | 39 | Speedup 1.0-1.2x (no improvement but valid) |
| **REGRESSION** | 35 | Slower than original (< 1.0x) |
| **FAILS_VALIDATION** | 9 | Wrong results (semantic error) |
| **ERROR** | 3 | Generation/execution failure |

**Key Insight**: Only 13 of 99 queries (13.1%) showed actual improvement.

---

## Quick Start

### 1. Find Gold Examples
```python
import pandas as pd

df = pd.read_csv('DuckDB_TPC-DS_Master_v1_20260205.csv')
gold = df[df['Classification'] == 'GOLD_EXAMPLE']

# Result: 6 queries with proven high speedups
print(gold[['Query_Num', 'Transform_Recommended', 'Kimi_Speedup']])
```

**Output**:
```
Query_Num  Transform_Recommended  Kimi_Speedup
1          decorrelate            2.92
15         or_to_union            2.78
39         pushdown               2.44
93         early_filter           2.73
90         early_filter           1.84
74         pushdown               1.42
```

### 2. Get Full SQL for a Query
```python
sql_df = pd.read_csv('DuckDB_TPC-DS_SQL_v1_20260205.csv')
q1 = sql_df[sql_df['Query_Num'] == 1].iloc[0]

print("=== ORIGINAL ===")
print(q1['SQL_Original'])

print("\n=== OPTIMIZED ===")
print(q1['SQL_Optimized'])
```

### 3. Analyze Transform Effectiveness
```python
# Which transforms work best?
transform_wins = df.groupby('Transform_Recommended').apply(
    lambda x: len(x[x['Classification'].isin(['MODERATE_WIN', 'GOLD_EXAMPLE'])]) / len(x) * 100
)
print(transform_wins.sort_values(ascending=False))
```

### 4. Find Problematic Patterns
```python
# What patterns cause regressions?
regressions = df[df['Classification'] == 'REGRESSION']
print(f"Regression transforms: {regressions['Transform_Recommended'].value_counts()}")

# What validation failures tell us?
failures = df[df['Classification'] == 'FAILS_VALIDATION']
print(f"Validation failure count: {len(failures)}")
```

---

## Pattern Mining Guide

### For Prompt Injection (Few-Shot Learning)

Use these 6 gold examples in your optimization prompts:

1. **Q1 (decorrelate)** - Correlated subquery with GROUP BY → 2.92x
2. **Q93 (early_filter)** - Dimension table filtering → 2.73x  
3. **Q15 (or_to_union)** - OR condition decomposition → 2.78x
4. **Q39 (pushdown)** - Filter pushdown → 2.44x
5. **Q90 (early_filter)** - Another early filter case → 1.84x
6. **Q74 (pushdown)** - Another pushdown case → 1.42x

### For Constraint Learning

Patterns that FAIL (avoid or fix):
- **OR-to-UNION with >3 branches**: Causes 9x scans (Q13, Q48)
- **Multi-scan rewrites**: 2x regressions on window functions
- **Deep CTE chains**: Timeout on large scales

### For Model Improvement

High-value research areas:
- **Why 47.5% fail?** - Constraints not learned
- **Why only 13% improve?** - Optimization search space too large
- **Evolutionary vs Standard**: Does MCTS find better solutions for Q2-Q16?

---

## Column Reference

### Core Metrics
- `Query_Num`: 1-99
- `Classification`: Pattern category
- `Kimi_Status`: Pass/Fail/Error
- `Kimi_Speedup`: Actual speedup ratio
- `V2_Status`: Syntax validation status

### Optimization Details
- `Transform_Recommended`: Type of optimization attempted
- `Expected_Speedup`: Predicted improvement
- `Risk_Level`: Low/Medium/High risk assessment

### SQL Data
- `SQL_Original`: Original query (full text in SQL CSV)
- `SQL_Optimized`: Optimized query (full text in SQL CSV)

### Comparison Data
- `Evo_Best_Speedup`: Evolutionary search result (for Q2-Q16)
- `Evo_Status`: Evolutionary validation status

See `SCHEMA.md` for complete definitions.

---

## Version History

### v1 (2026-02-05) - Initial Release
- 99 queries consolidated
- 88 with SQL artifacts
- ML pattern classification
- 4 analysis documents
- Full data lineage tracking

### Future Versions
- PostgreSQL TPC-DS benchmark
- MySQL TPC-DS benchmark  
- Alternative ML models (Deepseek, other LLMs)

---

## Known Limitations

1. **Incomplete SQL Coverage**: 11 queries missing from V2 standard run
2. **Evolutionary Limited**: Only Q2-Q16 in evolutionary mode
3. **No Full DB Validation**: Kimi only validated 47/99 on full SF100
4. **Pattern Weights Sparse**: Only 5-6 gold examples to learn from

---

## Usage Examples

### For ML Model Training
```python
# Get winning queries to study patterns
wins = df[df['Classification'].isin(['MODERATE_WIN', 'GOLD_EXAMPLE'])]

# Extract transform types that work
successful_transforms = wins['Transform_Recommended'].value_counts()

# Correlate with query characteristics
# (join with query complexity metrics)
```

### For Prompt Engineering
```python
# Build few-shot examples from gold queries
gold = df[df['Classification'] == 'GOLD_EXAMPLE'].sort_values('Kimi_Speedup', ascending=False)

for idx, row in gold.iterrows():
    print(f"Example {idx}: Q{row['Query_Num']} - {row['Transform_Recommended']} ({row['Kimi_Speedup']}x)")
```

### For Failure Analysis
```python
# Understand what's not working
failures_by_type = df.groupby('Classification').size()
print(failures_by_type)

# Deep dive into regressions
regressions = df[df['Classification'] == 'REGRESSION'].sort_values('Kimi_Speedup')
print(f"Worst regression: Q{regressions.iloc[0]['Query_Num']} at {regressions.iloc[0]['Kimi_Speedup']}x")
```

---

## How This Knowledge Base Will Be Used

1. **Pattern Discovery**: Mine for patterns that predict optimization success
2. **Constraint Learning**: Identify hard constraints that cause failures
3. **Few-Shot Injection**: Use gold examples in future optimization prompts
4. **Model Evaluation**: Benchmark future optimizers against this baseline
5. **Competitive Analysis**: Compare other models/systems against Kimi results

---

## Contributing to This Knowledge Base

When adding new benchmark data:

1. Create new version: `DuckDB_TPC-DS_Master_v2_YYYYMMDD.csv`
2. Update `METADATA.json` with data lineage
3. Re-run analysis scripts to update documentation
4. Add entry to version history
5. Compare against v1 baseline

---

## Support & Questions

- **Schema Questions**: See `SCHEMA.md`
- **Pattern Analysis**: See `PATTERNS.md`
- **Gold Examples**: See `GOLD_EXAMPLES.md`
- **Failure Analysis**: See `FAILURES.md`
- **Data Lineage**: See `DuckDB_TPC-DS_Master_v1_METADATA.json`

---

**This knowledge base is the foundation for understanding DuckDB TPC-DS optimization.**  
**All future optimization work should reference and extend it.**

Last updated: 2026-02-05
