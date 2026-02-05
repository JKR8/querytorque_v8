# SQL Error Pattern Analysis
**Date**: 2026-02-05
**Retry Results**: RETRY_RESULTS_DETAILED.json

---

## Summary

**Retry Scope**: 16 queries (32 pairs)
**Results**:
- ✅ Kimi Q1-Q30: 16/16 pairs OK (100% pass)
- ✅ Kimi Q31-Q99: 16/16 pairs OK (100% pass)
- ❌ benchmark_v2: 16/16 pairs ERROR (0% pass)
- ❌ Kimi Q31-Q99 (Q44 only): 1/1 ERROR

**Key Finding**: benchmark_v2 optimizer generates syntactically invalid SQL 100% of the time on retried queries. Kimi optimizer passes 100% of the same queries.

---

## Error Categories

### Category 1: Missing Columns in FROM Clause (7 occurrences)
Referenced columns are not available in the FROM clause scope.

| Query | Error | Notes |
|-------|-------|-------|
| q30 | `Referenced column "c_current_addr_sk" not found in FROM clause` | Ambiguous source - candidates show partial matches |
| q31 | `Referenced column "d_qoy" not found in FROM clause` | Column from dimension table not properly joined |
| q57 | `Referenced column "d_year" not found in FROM clause` | Dimension column lost in CTE transformation |
| q59 | `Referenced column "d_day_name" not found in FROM clause` | Date dimension attribute missing |
| q78 | `Referenced column "d_year" not found in FROM clause` | Repeated pattern - date columns dropped |
| q89 | `Referenced column "d_moy" not found in FROM clause` | Date aggregate column missing |

**Root Cause**: CTE rewrites drop column selections from intermediate results. The optimizer likely generates simplified CTEs that don't include all necessary columns through the pipeline.

---

### Category 2: Ambiguous Column References (5 occurrences)
Multiple tables have the same column name; need explicit table qualification.

| Query | Error | Notes |
|-------|-------|-------|
| q32 | `Ambiguous reference to column name "cs_item_sk" (use: "catalog_sales.cs_item_sk" or "iad.cs_item_sk")` | Two sources exist; not disambiguated |
| q45 | `Ambiguous reference to column name "i_item_id" (use: "item.i_item_id" or "item_list.i_item_id")` | CTE aliasing creates ambiguity |
| q54 | `Ambiguous reference to column name "d_month_seq" (use: "date_dim.d_month_seq" or "base_month.d_month_seq")` | Date table self-join creates conflict |
| q81 | `Ambiguous reference to column name "ca_state" (use: "ctr1.ca_state" or "customer_address.ca_state")` | CTE alias vs original table conflict |

**Root Cause**: When generating CTEs or temporary results, the optimizer doesn't properly qualify column names. This happens when joining intermediate results with original tables that have overlapping columns.

---

### Category 3: Window Functions in Invalid Clauses (2 occurrences)
Window functions placed in WHERE/HAVING where they're disallowed in SQL.

| Query | Error | Notes |
|-------|-------|-------|
| q44 (benchmark_v2) | `WHERE clause cannot contain window functions!` | RANK() used directly in WHERE |
| q70 | `HAVING clause cannot contain window functions!` | RANK() used directly in HAVING |

**Root Cause**: Aggressive optimization tried to move filtering logic but violated SQL syntax rules. Window functions must be evaluated in SELECT or subquery scope, then filtered in WHERE/HAVING.

---

### Category 4: Column Not in GROUP BY (2 occurrences)
Selected columns must be aggregated or included in GROUP BY.

| Query | Error | Notes |
|-------|-------|-------|
| q36 | `column "total_net_profit" must appear in GROUP BY clause or be used in aggregate function` | Calculated column not handled |
| q44 (Kimi Q31-Q99) | `column profit_threshold must appear in GROUP BY clause or be used in aggregate function` | CTE parameter not properly scoped |

**Root Cause**: When extracting expressions to CTEs, the optimizer doesn't maintain GROUP BY coherence. Calculated/alias columns become orphaned.

---

### Category 5: Missing Table/Column Definitions (2 occurrences)
Referenced objects don't exist in the generated SQL.

| Query | Error | Notes |
|-------|-------|-------|
| q72 | `Referenced table "sold_dates" not found! Candidate tables: "filtered_sales"` | Table alias created but not defined |
| q74 | `Table with name store_sales_aggregated does not exist! Did you mean "store_sales"?` | Incorrect materialized view name |

**Root Cause**: CTE naming is inconsistent - table is referenced by one name but defined with another. Or optimizer assumes materialized views exist that don't.

---

### Category 6: Column Type/Selection Error (1 occurrence)
CTE projection doesn't include expected columns.

| Query | Error | Notes |
|-------|-------|-------|
| q40 | `Values list "cs" does not have a column named "cs_sold_date_sk"` | CTE aliasing drops column from projection |

**Root Cause**: When creating filtered CTEs, the optimizer doesn't preserve the full column list needed by downstream operations.

---

## Success Pattern: Kimi vs benchmark_v2

### Kimi Q1-Q30: 16/16 OK (100%)
### Kimi Q31-Q99: 15/16 OK (94%)
- Exception: Q44 has GROUP BY coherence issue (likely same optimization strategy)

### benchmark_v2: 0/16 OK (0%)
- Every single query has a fundamental SQL generation error
- Errors span all 6 categories
- Suggests systematic issue in generation approach, not query-specific

---

## Implications

### 1. **Source Reliability**
- **Kimi**: Production-ready (96-100% success rate)
- **benchmark_v2**: Unusable for this retry set (0% success rate)

### 2. **Optimization Strategy Issues**
- benchmark_v2 optimizer is too aggressive with CTE extraction
- Doesn't properly preserve column lineage through transformations
- Makes assumptions about table/column availability that don't hold

### 3. **Prompt Recommendations**
To avoid these errors, prompts should include:
- **Explicit column preservation rule**: "Always include all referenced columns in CTE SELECT"
- **Window function placement rule**: "Window functions can only appear in SELECT or subqueries, use intermediate results for filtering"
- **Column qualification rule**: "Explicitly qualify all columns with table/alias when joining multiple sources"
- **GROUP BY coherence rule**: "All non-aggregated SELECT columns must appear in GROUP BY"

### 4. **Immediate Actions**
- Deprioritize benchmark_v2 optimizations (0% success on complex queries)
- Focus on Kimi patterns for knowledge base
- Update prompts to enforce SQL syntax constraints
- Consider runtime validation before applying any optimization

---

## Next Steps

1. **Investigate Q44 Kimi failure** - Only Kimi failure, should understand why
2. **Regenerate benchmark_v2 SQL** - Current generation is broken, needs different approach
3. **Update prompt examples** - Add examples that show correct CTE column preservation
4. **Add runtime validation gates** - Check for ambiguous columns, undefined tables before execution
5. **Test prompt changes** - Rerun with updated prompt to verify error rate improves

