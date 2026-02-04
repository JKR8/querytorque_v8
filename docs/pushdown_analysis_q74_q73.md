# Analysis: Why QT-OPT-004 Doesn't Detect Q74 and Q73

## Summary

**Finding:** Q74 and Q73 "pushdown" wins (1.42x, 1.24x) are **NOT** the pattern that QT-OPT-004 detects.

The transform attribution in `analyze_rule_gaps.py` is **INCORRECT**:
```python
74: {"transform": "pushdown", "rule": "QT-OPT-004", "speedup": 1.42},  # WRONG
73: {"transform": "pushdown", "rule": "QT-OPT-004", "speedup": 1.24},  # WRONG
```

## Q74 Analysis

### Original Structure
```sql
WITH year_total AS (
  SELECT ... FROM customer, store_sales, date_dim
  WHERE ... AND d_year IN (1999, 2000)  -- Filter INSIDE CTE
  UNION ALL
  SELECT ... FROM customer, web_sales, date_dim
  WHERE ... AND d_year IN (1999, 2000)  -- Filter INSIDE CTE
)
SELECT * FROM year_total t1, year_total t2, year_total t3, year_total t4
WHERE t1.sale_type = 's' AND t2.sale_type = 's' ...
```

### Optimized Structure
```sql
WITH year_total_store AS (  -- Specialized CTE for store sales
  SELECT ... FROM customer, store_sales, date_dim
  WHERE ... AND d_year IN (1999, 2000)
),
year_total_web AS (  -- Specialized CTE for web sales
  SELECT ... FROM customer, web_sales, date_dim
  WHERE ... AND d_year IN (1999, 2000)
)
SELECT * FROM year_total_store t1, year_total_store t2,
              year_total_web t3, year_total_web t4
WHERE ...
```

### Transform Type
**CTE Specialization / Scan Deduplication**
- Split one generic CTE into two specialized CTEs
- Eliminated UNION ALL by creating separate CTEs
- NOT a predicate pushdown (filter already inside CTE)

### Why QT-OPT-004 Doesn't Detect
```python
# QT-OPT-004 requires:
1. ✓ WITH clause (CTEs)
2. ✓ CTE with fact table and aggregation
3. ✗ Dimension table in MAIN query FROM  # Q74 fails here
```

**Q74 main query FROM:**
- `year_total` (CTE reference)
- `year_total` (CTE reference)
- `year_total` (CTE reference)
- `year_total` (CTE reference)

**No real dimension tables in main query** → QT-OPT-004 returns early.

## Q73 Analysis

### Structure
```sql
SELECT ... FROM
  (SELECT ... FROM store_sales, date_dim, store, household_demographics
   WHERE ... AND d_year IN (2000, 2001, 2002)  -- Filter in subquery
   GROUP BY ...) dj,
  customer
WHERE ...
```

### Why QT-OPT-004 Doesn't Detect
```python
# Line 389-391 of opportunity_rules.py:
with_clause = node.find(exp.With)
if not with_clause:
    return  # Q73 has NO CTE, immediate return
```

**Q73 has NO WITH clause** → QT-OPT-004 requires CTEs.

## Conclusion

### False Attribution
The winning "pushdown" transforms on Q74 and Q73 are **NOT** detected by QT-OPT-004 because:
- Q74: Different pattern (CTE specialization, not predicate pushdown)
- Q73: No CTE at all (QT-OPT-004 requires CTEs)

### Actual Coverage Status
From the 7 missing transforms:
- **early_filter** (4 wins): Now detected by GLD-003 ✓
- **pushdown** (2 wins on Q74, Q73): NOT QT-OPT-004 pattern, still missing ✗
- **projection_prune** (1 win): Detected by GLD-004 ✓

**Real coverage: 5/7 (71%)** - excluding incorrectly attributed pushdown wins

### Recommendation
1. Remove Q74 and Q73 from WINNING_TRANSFORMS or mark them as different transform type
2. Investigate what the LLM actually did to Q74 and Q73
3. Create new detector for "CTE specialization" pattern if it's a consistent win
