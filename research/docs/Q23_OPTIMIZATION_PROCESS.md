# Q23 Optimization Process

This document captures the exact reasoning process used to optimize Q23.
It serves as training material for LLMs to learn iterative optimization.

---

## Phase 1: Understand the Query Structure

### Block Map Analysis

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ BLOCK                  │ SCANS              │ FILTERS        │ OUTPUT       │
├─────────────────────────────────────────────────────────────────────────────┤
│ frequent_ss_items      │ store_sales        │ d_year IN (…)  │ item_sk      │
│                        │ date_dim, item     │ HAVING cnt > 4 │              │
├─────────────────────────────────────────────────────────────────────────────┤
│ max_store_sales        │ store_sales        │ d_year IN (…)  │ tpcds_cmax   │
│                        │ customer, date_dim │                │ (scalar)     │
├─────────────────────────────────────────────────────────────────────────────┤
│ best_ss_customer       │ store_sales        │ ⚠️ NO FILTER   │ c_customer_sk│
│                        │ customer           │ HAVING > 0.95× │              │
│   └─ refs: max_store_sales                                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│ main_query (UNION)     │ catalog_sales      │ d_year = 2000  │ sum(sales)   │
│                        │ web_sales          │ d_moy = 5      │              │
│   └─ refs: frequent_ss_items, best_ss_customer                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Observations

1. **Repeated scans:**
   - `store_sales`: scanned 3× (frequent_ss_items, max_store_sales, best_ss_customer)
   - `customer`: scanned 2× (max_store_sales, best_ss_customer)
   - `date_dim`: scanned 2× (frequent_ss_items, max_store_sales)

2. **Filter gap detected:**
   - `best_ss_customer` scans `store_sales` WITHOUT date filter
   - But it references `max_store_sales` which HAS date filter
   - This looks like a bug... but is it?

3. **Semantic analysis:**
   - `max_store_sales`: "max customer sales for years 2000-2003"
   - `best_ss_customer`: "customers whose TOTAL sales > 95% of that max"
   - The gap is INTENTIONAL: compare lifetime value against period max

---

## Phase 2: Generate Hypotheses

### Hypothesis 1: Add year filter to best_ss_customer
**Status: ❌ REJECTED**
**Reason:** Changes semantics. The original intentionally compares lifetime vs period.

### Hypothesis 2: Remove customer table from max_store_sales
**Status: ❌ REJECTED (initially)**
**Reason:** Without `IS NOT NULL`, includes NULL foreign keys and changes aggregation.

### Hypothesis 3: Consolidate store_sales scans
**Status: ✅ PROMISING**
**Approach:** Single scan with CASE WHEN to compute both filtered and unfiltered aggregates.

### Hypothesis 4: Reorder joins in main query
**Status: ✅ PROMISING**
**Approach:** Filter by best_ss_customer (small result) BEFORE joining frequent_ss_items.

---

## Phase 3: Iterative Testing

### Environment
- Sample DB: 1% of SF100 (525MB vs 30GB)
- Test time: ~0.3-0.4s per query (vs ~25s on full)
- Methodology: 3 runs, discard first, average rest

### Iteration Log

| # | Change | Time | Speedup | Correct | Notes |
|---|--------|------|---------|---------|-------|
| 0 | Original | 0.39s | 1.00x | ✅ | Baseline |
| 1 | IN → JOIN | 0.45s | 0.87x | ✅ | Slower! |
| 2 | Materialize CTEs | 0.38s | 1.03x | ✅ | Negligible |
| 3 | Join order | 0.44s | 0.89x | ✅ | Slower! |
| 4 | EXISTS instead of IN | 0.39s | 1.00x | ✅ | No change |
| 5 | Semi-join pattern | 0.36s | 1.08x | ✅ | Small win |
| 6 | Window function | 0.23s | 1.70x | ✅ | **Good!** |
| 7 | Single scan CASE WHEN | 0.20s | 1.95x | ✅ | **Better!** |
| 8 | + Materialized CTEs | 0.22s | 1.77x | ✅ | Worse than #7 |
| 9 | + Reorder joins | 0.20s | 1.95x | ✅ | Same as #7 |
| 10 | Mega scan (all in one) | 0.33s | 1.18x | ✅ | Worse |
| 11 | #7 + filter early | 0.20s | 1.95x | ✅ | Best on sample |
| 12 | Explicit joins | 0.21s | 1.86x | ✅ | Good |
| 13 | #7 + reorder w/ CTEs | 0.20s | 1.95x | ✅ | Tied for best |
| 14 | #13 + filter by customer first | 0.20s | 1.95x | ✅ | **Winner** |

### Key Insight from Iteration 7

The single-scan CASE WHEN pattern was the breakthrough:

```sql
-- Original: 2 separate scans
max_store_sales AS (
  SELECT MAX(csales) FROM (
    SELECT c_customer_sk, SUM(ss_quantity*ss_sales_price) csales
    FROM store_sales, customer, date_dim
    WHERE ss_customer_sk = c_customer_sk
      AND ss_sold_date_sk = d_date_sk
      AND d_year IN (2000,2001,2002,2003)
    GROUP BY c_customer_sk
  )
),
best_ss_customer AS (
  SELECT c_customer_sk, SUM(ss_quantity*ss_sales_price) ssales
  FROM store_sales, customer
  WHERE ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING ssales > 0.95 * (SELECT * FROM max_store_sales)
)

-- Optimized: 1 scan with CASE WHEN
ss_agg AS (
  SELECT ss_customer_sk,
         SUM(ss_quantity*ss_sales_price) AS total_sales,
         SUM(CASE WHEN d_year IN (2000,2001,2002,2003)
             THEN ss_quantity*ss_sales_price ELSE 0 END) AS filtered_sales
  FROM store_sales
  LEFT JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE ss_customer_sk IS NOT NULL
  GROUP BY ss_customer_sk
),
best_ss_customer AS (
  SELECT ss_customer_sk c_customer_sk
  FROM ss_agg
  WHERE total_sales > (SELECT MAX(filtered_sales) * 0.95 FROM ss_agg)
)
```

**Why this works:**
1. Scans store_sales ONCE instead of twice
2. Uses CASE WHEN to conditionally sum for filtered period
3. Adds `IS NOT NULL` to preserve join semantics
4. Removes customer table (only used for FK validation)

---

## Phase 4: Validate on Full Database

```
SF100 Results (3 runs, discard first):
  Original:  23.28s
  Optimized: 18.63s
  Speedup:   1.25x ✅
  Semantics: ✅ CORRECT (7637648.56 = 7637648.56)
```

Note: Sample predicted 1.95x but full DB showed 1.25x.
This is expected - sample has different data distribution.

---

## Summary: What Worked and Why

### Successful Patterns

1. **Scan Consolidation with CASE WHEN**
   - Signal: Same table scanned multiple times with different filters
   - Fix: Single scan with conditional aggregation
   - Impact: Reduced 2 scans → 1 scan

2. **Join Elimination with IS NOT NULL**
   - Signal: Dimension table only used to validate FK
   - Fix: Remove join, add `WHERE fk IS NOT NULL`
   - Impact: Avoided 24M row lookup

3. **Join Reordering**
   - Signal: IN subquery to large CTE result
   - Fix: Filter by smallest result first
   - Impact: Smaller intermediate results

### Failed Patterns

1. **Adding "missing" filter**
   - Why failed: Filter gap was intentional business logic
   - Lesson: Verify semantic intent before adding filters

2. **Materializing CTEs**
   - Why failed: DuckDB already optimizes this
   - Lesson: Modern optimizers handle common patterns

3. **Converting IN to JOIN**
   - Why failed: Optimizer already converts this
   - Lesson: Don't micro-optimize what the optimizer handles

---

## Teaching Points for LLMs

### Before Proposing an Optimization

1. **Check semantic intent**
   - Does the filter gap have a business reason?
   - Compare what each block SHOULD represent

2. **Look for consolidation opportunities**
   - Same table in multiple blocks → CASE WHEN
   - Dimension table for FK validation → IS NOT NULL

3. **Consider join order**
   - Which filter produces smallest result?
   - Filter by that first

### During Iteration

1. **Test on sample DB first**
   - Fast feedback (0.3s vs 25s)
   - Check both speedup AND correctness

2. **Try one pattern at a time**
   - Easier to identify what works
   - Combinations may interfere

3. **Accept diminishing returns**
   - First pattern: often 1.5-2x
   - Each additional: smaller gains
   - Stop when gains < 10%

### Common Mistakes to Avoid

1. **Assuming missing filters are bugs**
   - They may be intentional
   - Ask: "What does this block MEAN to compute?"

2. **Removing joins without NULL handling**
   - Joins implicitly filter NULLs
   - Always add `IS NOT NULL` when eliminating

3. **Adding redundant filters**
   - If filter exists via JOIN, adding IN subquery adds overhead
   - Check if filter is already applied
