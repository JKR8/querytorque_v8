# Optimization Patterns That Work

Proven patterns from TPC-DS SF100 benchmarks (Kimi K2.5 + DeepSeek V3).

## Quick Reference

| # | Pattern | Best Speedup | Verified Query |
|---|---------|--------------|----------------|
| 1 | Predicate Pushdown | 2.71x | Q93 |
| 2 | Scan Consolidation | 1.84x | Q90 |
| 3 | Join Elimination | 2.18x | Q23 |
| 4 | Correlated → Pre-computed CTE | **2.81x** | Q1 |
| 5 | Date CTE Isolation | 2.67x | Q15 |
| 6 | Correlated → Window | 2.5x | - |
| 7 | UNION ALL Decomposition | **2.98x** | Q15 |

---

## 1. Predicate Pushdown (2.1-2.5x)

**Signal**: Small filtered dimension table joined AFTER large fact table aggregation.

**Fix**: Join the dimension INSIDE the CTE, before GROUP BY.

```sql
-- BEFORE
WITH agg AS (
  SELECT key, sum(value) FROM fact_table, date_dim
  WHERE fact.date_sk = date_dim.date_sk AND year = 2000
  GROUP BY key
)
SELECT * FROM agg, dimension
WHERE agg.key = dimension.key AND dimension.filter = 'X'

-- AFTER
WITH agg AS (
  SELECT key, sum(value) FROM fact_table, date_dim, dimension
  WHERE fact.date_sk = date_dim.date_sk AND year = 2000
    AND fact.key = dimension.key AND dimension.filter = 'X'  -- pushed in
  GROUP BY key
)
SELECT * FROM agg
```

---

## 2. Scan Consolidation (1.25x)

**Signal**: Same table scanned multiple times with different filters.

**Fix**: Single scan with CASE WHEN for conditional aggregates.

```sql
-- BEFORE
cte_filtered AS (SELECT key, sum(val) FROM t WHERE year = 2000 GROUP BY key),
cte_all AS (SELECT key, sum(val) FROM t GROUP BY key)

-- AFTER
cte_combined AS (
  SELECT key,
         sum(CASE WHEN year = 2000 THEN val ELSE 0 END) AS filtered_sum,
         sum(val) AS total_sum
  FROM t
  GROUP BY key
)
```

---

## 3. Join Elimination (2.18x)

**Signal**: Table joined only to validate FK exists, no columns used from it.

**Fix**: Remove join, add `WHERE fk IS NOT NULL`.

```sql
-- BEFORE
SELECT a.id, sum(a.value)
FROM fact a JOIN dim d ON a.dim_key = d.id
GROUP BY a.id

-- AFTER
SELECT id, sum(value)
FROM fact
WHERE dim_key IS NOT NULL
GROUP BY id
```

**Critical**: The join implicitly filters NULLs. You must add IS NOT NULL.

**Proven Result (Q23)**: Removed joins to `item` and `customer` tables in 3 CTEs. **2.18x speedup** (24.5s → 11.3s) with exact semantic match.

---

## 4. Correlated Subquery → Pre-computed CTE (2.81x)

**Signal**: Correlated subquery computes aggregate per group, then compares.

**Fix**: Pre-compute the aggregate as a separate CTE with GROUP BY, then JOIN.

```sql
-- BEFORE (Q1 original)
WITH ctr AS (SELECT store_sk, customer_sk, SUM(fee) AS total FROM returns GROUP BY ...)
SELECT * FROM ctr c1
WHERE c1.total > (SELECT AVG(total) * 1.2 FROM ctr c2 WHERE c1.store_sk = c2.store_sk)

-- AFTER (Q1 optimized - 2.81x speedup)
WITH ctr AS (...),
     store_avg AS (SELECT store_sk, AVG(total) * 1.2 AS threshold FROM ctr GROUP BY store_sk)
SELECT * FROM ctr c1
JOIN store_avg sa ON c1.store_sk = sa.store_sk
WHERE c1.total > sa.threshold
```

**Why it works**: Eliminates O(n²) correlated execution. Pre-computes thresholds once.

**Proven Result (Q1)**: 2.81x speedup (241ms → 86ms) on TPC-DS SF100.

---

## 5. Date CTE Isolation (1.2-2.7x)

**Signal**: Date dimension filter repeated in multiple joins or complex WHERE clause.

**Fix**: Extract date filtering into a small CTE, join to it.

```sql
-- BEFORE
SELECT ... FROM fact, date_dim
WHERE fact.date_sk = date_dim.d_date_sk AND d_year = 2001 AND d_qoy = 1

-- AFTER
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy = 1)
SELECT ... FROM fact JOIN filtered_dates ON fact.date_sk = d_date_sk
```

**Why it works**: Small CTE materializes early, enables partition pruning, reduces repeated filter evaluation.

**Proven Results**: Q6 (1.21x), Q15 (2.67x), Q27 (1.23x)

---

## 6. Correlated Subquery → Window Function (2.5x)

**Signal**: Correlated subquery computes aggregate per group, used in SELECT list.

**Fix**: Window function in the CTE.

```sql
-- BEFORE
SELECT *, (SELECT avg(value) FROM t t2 WHERE t.group = t2.group) AS group_avg FROM t

-- AFTER
SELECT *, avg(value) OVER (PARTITION BY group) AS group_avg FROM t
```

**When to use**: When you need the aggregate alongside each row. For threshold comparisons, prefer pre-computed CTE (pattern #4).

---

## 7. UNION ALL Decomposition (2.67x)

**Signal**: Complex OR condition spanning different columns or value types.

**Fix**: Split into separate queries with simple filters, UNION ALL the results.

```sql
-- BEFORE (Q15 original)
SELECT ca_zip, SUM(cs_sales_price) FROM catalog_sales, customer, customer_address, date_dim
WHERE ... AND (
    substr(ca_zip,1,5) IN ('85669','86197',...)
    OR ca_state IN ('CA','WA','GA')
    OR cs_sales_price > 500
) ...

-- AFTER (Q15 optimized - 2.67x speedup)
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001),
filtered_sales AS (
    SELECT cs_sales_price, ca_zip FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk ...
    WHERE substr(ca_zip,1,5) IN ('85669','86197',...)
    UNION ALL
    SELECT cs_sales_price, ca_zip FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk ...
    WHERE ca_state IN ('CA','WA','GA')
    UNION ALL
    SELECT cs_sales_price, ca_zip FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk ...
    WHERE cs_sales_price > 500
)
SELECT ca_zip, SUM(cs_sales_price) FROM filtered_sales GROUP BY ca_zip
```

**Why it works**: Each branch uses optimal access path. Enables parallel execution.

**Proven Result (Q15)**: 2.67x speedup (142ms → 53ms) on TPC-DS SF100.

**Caution**: May produce duplicates if OR branches overlap. Add DISTINCT if needed.

---

## Anti-Patterns (Don't Do These)

| Mistake | Why It Fails |
|---------|--------------|
| Add filter to "all-time" CTE | May be intentional (comparing periods) |
| Remove join without IS NOT NULL | Changes results (NULLs included) |
| Add redundant IN subquery | Filter already exists via join |
| UNION ALL without checking overlap | May create duplicates |
| Over-decompose simple queries | CTE overhead > benefit on small queries |

---

## MCTS Transform Scorecard

Performance analysis from TPC-DS benchmark runs (Feb 2026).

### Current Transform Library (11 transforms)

| Transform | Max Speedup | Source | Status |
|-----------|------------|--------|--------|
| **or_to_union** | **2.98x** | Q15 benchmark | ✅ NEW |
| **correlated_to_cte** | **2.81x** | Q1 benchmark | ✅ NEW |
| **date_cte_isolate** | **2.67x** | Q15 benchmark | ✅ NEW |
| **consolidate_scans** | **1.84x** | Q90 benchmark | ✅ NEW |
| reorder_join | 1.16x | MCTS Q67 | ✅ Working |
| materialize_cte | 1.08x | MCTS Q67 | ✅ Working |
| remove_redundant | 1.08x | MCTS Q67 | ✅ Working |
| multi_push_pred | 1.00x | MCTS Q67 | ✅ Foundational |
| push_pred | 0.96x | MCTS Q67 | ✅ Foundational |
| inline_cte | 0.89x | MCTS Q67 | ✅ Working |
| flatten_subq | 1.00x | MCTS Q67 | ✅ Working |

### Removed Transforms (Feb 2026)

| Transform | Reason |
|-----------|--------|
| ~~opt_agg~~ | 0% validation success, no evidence in winning patterns |
| ~~opt_window~~ | 0% validation success, no evidence in winning patterns |

### Transform Priority Order

Based on proven speedups:
1. `or_to_union` - 2.98x (OR decomposition for better access paths)
2. `correlated_to_cte` - 2.81x (decorrelation with pre-computation)
3. `date_cte_isolate` - 2.67x (partition pruning enabler)
4. `consolidate_scans` - 1.84x (I/O reduction)
5. `reorder_join` - 1.16x (selectivity optimization)
6. `multi_push_pred` - enables other transforms
7. `push_pred` - foundational predicate movement
