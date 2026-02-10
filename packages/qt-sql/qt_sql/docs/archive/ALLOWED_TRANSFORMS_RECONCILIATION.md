# ALLOWED_TRANSFORMS Reconciliation

**Date:** 2026-02-05
**Status:** ✅ RESOLVED

---

## Problem

Three different sources had inconsistent transform naming and counts:

1. **knowledge_base.py** - 11 canonical transform IDs (e.g., `push_pred`, `correlated_to_cte`)
2. **dag_v2.py ALLOWED_TRANSFORMS** - 7 transforms (old list)
3. **Gold example files** - 13 JSON files using 12 unique transform IDs

---

## Solution

**Updated dag_v2.py ALLOWED_TRANSFORMS to match the 12 transforms actually used in gold examples:**

```python
ALLOWED_TRANSFORMS = [
    "pushdown",             # Push filters into CTEs/subqueries
    "decorrelate",          # Correlated subquery -> CTE with GROUP BY
    "or_to_union",          # OR conditions -> UNION ALL branches
    "early_filter",         # Filter dimension tables before joining to facts
    "date_cte_isolate",     # Extract date dimension filtering into early CTE
    "materialize_cte",      # Extract repeated subqueries into CTE
    "flatten_subquery",     # Convert EXISTS/IN to JOINs
    "reorder_join",         # Reorder joins for selectivity
    "multi_push_predicate", # Push predicates through multiple CTE layers
    "inline_cte",           # Inline single-use CTEs
    "remove_redundant",     # Remove unnecessary DISTINCT/ORDER BY
    "semantic_rewrite",     # Catch-all for other valid optimizations
]
```

---

## Gold Example → Transform ID Mapping

| Gold Example File | Transform ID Used | Knowledge Base Equivalent |
|------------------|-------------------|---------------------------|
| decorrelate.json | `decorrelate` | CORRELATED_TO_CTE |
| early_filter.json | `early_filter` | (distinct pattern) |
| pushdown.json | `pushdown` | PUSH_PREDICATE |
| quantity_range_pushdown.json | `pushdown` | PUSH_PREDICATE |
| or_to_union.json | `or_to_union` | OR_TO_UNION ✓ |
| date_cte_isolate.json | `date_cte_isolate` | DATE_CTE_ISOLATION ✓ |
| materialize_cte.json | `materialize_cte` | MATERIALIZE_CTE ✓ |
| flatten_subquery.json | `flatten_subquery` | FLATTEN_SUBQUERY |
| reorder_join.json | `reorder_join` | REORDER_JOIN ✓ |
| multi_push_predicate.json | `multi_push_predicate` | MULTI_PUSH_PREDICATE |
| inline_cte.json | `inline_cte` | INLINE_CTE ✓ |
| remove_redundant.json | `remove_redundant` | REMOVE_REDUNDANT ✓ |
| semantic_late_materialization.json | `semantic_rewrite` | (catch-all) |

**Note:** 13 files → 12 unique transform IDs (pushdown used twice)

---

## Updated RULES (dag_v2.py)

Old rules were minimal. New rules clarify the optimization philosophy:

```
RULES:
- Primary Goal: optimize for execution speed while maintaining exact semantic equivalence.
- Allowed Transforms: Use the provided list. If a standard SQL optimization applies
  that is not listed, label it "semantic_rewrite".
- Atomic Sets: Group dependent changes (e.g., creating a CTE and joining it) into
  a single rewrite_set.
- Contracts: Output columns, grain, and total result rows must remain invariant.
- Naming: Use descriptive CTE names (e.g., `filtered_returns` vs `cte1`).
- Column Aliasing: Permitted only for aggregations or disambiguation.
```

**Key insight:** `semantic_rewrite` is a **catch-all** for valid optimizations that don't fit the specific named transforms.

---

## Why the Mismatch Existed

1. **knowledge_base.py** uses **canonical IDs** for the knowledge base registry (11 patterns with QT-OPT codes)
2. **Gold examples** use **practical transform names** that the LLM outputs in JSON (12 names)
3. **dag_v2.py** needed to accept the **LLM output format** (gold example names), not the internal KB IDs

The gold example names are what matter for validation because that's what the LLM generates in the `rewrite_sets[].transform` field.

---

## Verification

```bash
$ python3 -c "from qt_sql.optimization.dag_v2 import DagBuilder; \
  print(f'✓ {len(DagBuilder.ALLOWED_TRANSFORMS)} transforms loaded')"

✓ 12 transforms loaded
```

All 39 tests still passing:
- 21 integration tests (test_adaptive_rewriter_v5_integration.py)
- 18 unit tests (test_dag_v2.py, test_prompt_quality_v5.py)

---

## Impact on V5 Optimizer

Workers 1-4 now accept all 12 transform IDs from gold examples:
- ML recommendations can suggest any of the 12 transforms
- Prompts include all relevant examples
- Validation accepts all 12 transform types

Worker 5 outputs full SQL (not DAG JSON), so unaffected.

---

## Files Modified

1. **packages/qt-sql/qt_sql/optimization/dag_v2.py**
   - Updated `ALLOWED_TRANSFORMS` (lines 98-111): 7 → 12 transforms
   - Updated `SYSTEM_PROMPT` rules (lines 579-585): Added detailed optimization philosophy

---

## Next Steps

- ✅ ALLOWED_TRANSFORMS now matches gold examples (12 transforms)
- ✅ RULES clarified with new optimization guidelines
- ✅ `semantic_rewrite` documented as catch-all
- ✅ Module imports successfully
- ⏭️ Ready to run V5 optimizer on full TPC-DS benchmark

---

## Summary

**Problem:** Inconsistent transform naming across 3 sources
**Solution:** Unified on gold example transform IDs (12 transforms)
**Rationale:** Gold examples define what the LLM outputs, so that's the source of truth for ALLOWED_TRANSFORMS
**Status:** ✅ Complete and verified
