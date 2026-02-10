# Optimized Queries - Results

## Verified Full DB Wins (SF100)

| Query | Speedup | Pattern | Model | File |
|-------|---------|---------|-------|------|
| **Q1** | **2.44x** | Predicate pushdown + window function | Kimi K2.5 | `q1_kimi.sql` |
| **Q23** | **2.18x** | Join elimination | Manual | `q23_optimized.sql` |
| **Q15** | **1.37x** | UNION ALL decomposition | Gemini 3 Pro | `q15_optimized.sql` |

## Sample DB Results (need full DB verification)

| Query | Sample | Pattern | Notes |
|-------|--------|---------|-------|
| Q18 | 1.63x | Date filter pushdown | Push d_year filter into CTE |
| Q6 | 1.14x | Correlated subquery -> window | Needs testing |

## No Improvement

DuckDB already optimizes these well:
- Q3, Q7, Q12, Q20: Item/date filter pushdown provides <1.1x

## Patterns That Work

1. **Predicate pushdown + window function** (Q1: 2.44x)
   - Push dimension filters into CTE
   - Replace correlated subquery with window function
   - Turns O(n²) into O(n)

2. **Join elimination** (Q23: 2.18x)
   - Remove table joined only for FK validation
   - Add `IS NOT NULL` on FK column

3. **UNION ALL decomposition** (Q15: 1.37x)
   - Split complex OR conditions into separate scans
   - UNION ALL the results

## Files

- `q1_kimi.sql` - 2.44x full DB (Kimi K2.5)
- `q15_optimized.sql` - 1.37x full DB (Gemini UNION ALL)
- `q23_optimized.sql` - 2.18x full DB (join elimination)

## LLM Comparison

| Model | Quality | Cost |
|-------|---------|------|
| **Kimi K2.5** | Excellent - complete operations | ~$0.70/99 queries |
| Gemini CLI | Poor - broken operations | Free but useless |
| DeepSeek R1 | Not tested yet | ~$0.35/99 queries |
