# DuckDB TPC-DS Consolidated Benchmark Knowledge Base

## Overview

This is a consolidated knowledge base for TPC-DS query optimization on DuckDB. It combines:
- **Kimi K2.5** full validation (99 queries, Feb 2, 2026)
- **V2 Standard Mode** direct optimization (88 queries, Feb 5, 2026)  
- **V2 Evolutionary Mode** MCTS search (15 queries, Feb 5, 2026)
- **ML Pattern Classifier** (6 gold patterns with confidence weights)
- **SQL Artifacts** (full original + optimized for each query)

**Purpose**: Mine patterns for success/failure to guide prompt injection and identify winning optimization strategies.

---

## Files in This Knowledge Base

### Main Files
- **DuckDB_TPC-DS_Master_v1_20260205.csv** - Core benchmark metrics (21 columns)
- **DuckDB_TPC-DS_SQL_v1_20260205.csv** - Full SQL source (original + optimized)
- **DuckDB_TPC-DS_Master_v1_METADATA.json** - Version info and data source references

### Analysis Files
- **SCHEMA.md** - This file. Column definitions and data sources.
- **PATTERNS.md** - ML pattern analysis and effectiveness
- **GOLD_EXAMPLES.md** - Gold example performance
- **FAILURES.md** - Failure analysis and error classification

### Version History
- **v1** (2026-02-05) - Initial consolidation with 99 queries + 88 SQL files

---

## Column Definitions

### Identity
- **Query_Num** (1-99): TPC-DS query identifier
- **Classification**: Pattern classification category (see below)

### Kimi K2.5 Benchmark (Feb 2, 2026)
- **Kimi_Status**: Pass/Fail/Error status on full SF100 sample
- **Kimi_Speedup**: Measured speedup ratio (optimized/original)
- **Kimi_Original_ms**: Baseline execution time
- **Kimi_Optimized_ms**: Optimized execution time
- **Kimi_Error**: Error message if failed

### V2 Standard Mode (Feb 5, 2026)
- **V2_Status**: Success/Error status (syntax validation)
- **V2_Elapsed_s**: Time to generate optimization
- **V2_Attempts**: Number of attempts needed
- **V2_Syntax_Valid**: Whether SQL is syntactically valid
- **V2_Error**: Error message if generation failed

### V2 Evolutionary Mode (Feb 5, 2026)
MCTS-based search for queries Q2-Q16:
- **Evo_Status**: Status (success/failed/below_target)
- **Evo_Best_Speedup**: Best speedup found by evolutionary search
- **Evo_Valid_Count**: Number of valid iterations
- **Evo_Sample_Speedups**: Semicolon-separated speedup values for each iteration

### Optimization Metadata
- **Transform_Recommended**: Type of optimization recommended (decorrelate/or_to_union/pushdown/etc)
- **Expected_Speedup**: Predicted speedup (from LLM response)
- **Risk_Level**: Risk assessment (low/medium/high)

### Gold Examples
- **Gold_Transform**: Type of gold example (if applicable)
- **Gold_Expected_Speedup**: Expected speedup for gold example

---

## Classifications

Query success/failure patterns:

- **GOLD_EXAMPLE** (6): Known high-value examples (Q1, Q15, Q39, Q74, Q90, Q93)
- **MAJOR_WIN** (0): Speedup ≥ 2.0x
- **SIGNIFICANT_WIN** (0): Speedup 1.5-2.0x
- **MODERATE_WIN** (7): Speedup 1.2-1.5x
- **NEUTRAL** (39): Speedup 1.0-1.2x (no improvement but valid)
- **REGRESSION** (35): Speedup < 1.0x (slower than original)
- **FAILS_VALIDATION** (9): Failed result validation (wrong results)
- **ERROR** (3): Execution/generation error

**Key Finding**: Only 13/99 queries show improvement (13.1% win rate)

---

## Data Quality Notes

### Coverage
- **99 queries total**: All TPC-DS queries Q1-Q99
- **88 with SQL**: V2 Standard run generated SQL for 88 queries
- **11 missing**: Q3, Q4, Q5, Q6, Q8, Q9, Q11, Q12, Q14, Q15, Q17 (not in V2 standard)
- **15 with evolutionary**: Q2-Q16 have evolutionary search results (subset)
- **5 with both modes**: Q2, Q7, Q10, Q13, Q16 (can compare approaches)

### Confidence Levels
- **High Confidence**: Kimi full validation (100 queries tested)
- **Medium Confidence**: V2 standard syntax validation (88 queries)
- **Medium Confidence**: V2 evolutionary MCTS (15 queries, potentially better)

### Known Issues
- Evolutionary mode focuses on queries Q2-Q16 only (not comprehensive)
- V2 standard missing 11 queries (possibly timeout or skip)
- Some queries have NULL SQL (generation may have failed)

---

## Usage Examples

### Find all gold examples
```python
import pandas as pd
df = pd.read_csv('DuckDB_TPC-DS_Master_v1_20260205.csv')
gold = df[df['Classification'] == 'GOLD_EXAMPLE']
# Result: 6 queries with known high-value patterns
```

### Find regressions (where we made queries slower)
```python
regressions = df[df['Classification'] == 'REGRESSION']
print(f"Regressions: {len(regressions)} queries made slower")
```

### Compare optimization approaches for Q10
```python
q10 = df[df['Query_Num'] == 10]
print(f"V2 Standard: {q10['Transform_Recommended'].values[0]}")
print(f"Evo Result: {q10['Evo_Best_Speedup'].values[0]}")
```

### Get full SQL for a query
```python
sql_df = pd.read_csv('DuckDB_TPC-DS_SQL_v1_20260205.csv')
q1_sql = sql_df[sql_df['Query_Num'] == 1]
original = q1_sql['SQL_Original'].values[0]
optimized = q1_sql['SQL_Optimized'].values[0]
```

---

## Next Steps

1. **Pattern Mining**: Use CLASSIFICATION column to identify winning patterns
2. **Transform Analysis**: Map TRANSFORM_RECOMMENDED → success rate
3. **Evolutionary Learning**: Compare V2 Standard vs Evo for overlapping queries
4. **Gold Example Injection**: Use Q1, Q15, Q39, Q74, Q90, Q93 in few-shot prompts
5. **Failure Analysis**: Examine REGRESSION and FAILS_VALIDATION cases for constraints

---

**Knowledge Base Version**: v1  
**Last Updated**: 2026-02-05  
**Total Queries**: 99  
**Coverage**: 100% metrics, 89% SQL artifacts
