# Working Optimizations

Patterns that produced verified speedups on TPC-DS SF100.

---

## Summary

| # | Pattern | Speedup | Source |
|---|---------|---------|--------|
| 1 | Predicate pushdown | 2.1-2.5x | Q1, Q2 |
| 2 | Scan consolidation | 1.25x | Q23 (manual) |
| 3 | **Join elimination** | **2.18x** | Q23 (Gemini) |
| 4 | Correlated subquery → window function | 2.54x | Q1 |

---

## 1. Predicate Pushdown

**Signal**: A small dimension table with a selective filter is joined AFTER a large fact table aggregation.

**Fix**: Join the dimension table INSIDE the CTE, before GROUP BY.

**Example** (Q1):

```sql
-- BEFORE: store filtered in main query, after aggregation
WITH customer_total_return AS (
  SELECT sr_customer_sk, sr_store_sk, sum(sr_return_amt)
  FROM store_returns, date_dim
  WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT ... FROM customer_total_return, store, customer
WHERE s_state = 'TN' ...  -- filter applied AFTER aggregation

-- AFTER: store filter pushed INTO CTE
WITH customer_total_return AS (
  SELECT sr_customer_sk, sr_store_sk, sum(sr_return_amt)
  FROM store_returns, date_dim, store  -- joined here
  WHERE sr_returned_date_sk = d_date_sk
    AND d_year = 2000
    AND sr_store_sk = s_store_sk
    AND s_state = 'TN'  -- filter applied BEFORE aggregation
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT ... FROM customer_total_return, customer ...
```

**Result**: 345M rows → ~35M rows before aggregation. **2.1x speedup**.

---

## 2. Scan Consolidation

**Signal**: Same table scanned multiple times with different WHERE filters.

**Fix**: Single scan with CASE WHEN expressions to compute conditional aggregates.

**Example** (Q23):

```sql
-- BEFORE: Two separate scans of store_sales
max_store_sales AS (
  SELECT c_customer_sk, sum(ss_quantity*ss_sales_price)
  FROM store_sales, customer, date_dim
  WHERE d_year IN (2000,2001,2002,2003)  -- filtered
  GROUP BY c_customer_sk
),
best_ss_customer AS (
  SELECT c_customer_sk, sum(ss_quantity*ss_sales_price)
  FROM store_sales, customer  -- no year filter (all-time)
  GROUP BY c_customer_sk
)

-- AFTER: Single scan with CASE WHEN
ss_agg AS (
  SELECT ss_customer_sk,
         sum(ss_quantity*ss_sales_price) AS total_sales,
         sum(CASE WHEN d_year IN (2000,2001,2002,2003)
             THEN ss_quantity*ss_sales_price ELSE 0 END) AS filtered_sales
  FROM store_sales
  LEFT JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE ss_customer_sk IS NOT NULL
  GROUP BY ss_customer_sk
)
```

**Result**: 2 scans → 1 scan. **1.25x speedup** (combined with join elimination).

---

## 3. Join Elimination

**Signal**: A table is joined only to validate a foreign key exists, no columns from it are used.

**Fix**: Remove the join, add `WHERE fk_column IS NOT NULL`.

**Example** (Q23 - Gemini 2025-02-01):

```sql
-- BEFORE: Join to customer just to validate FK
SELECT c_customer_sk, sum(ss_quantity*ss_sales_price)
FROM store_sales, customer
WHERE ss_customer_sk = c_customer_sk  -- validates FK exists
GROUP BY c_customer_sk

-- AFTER: IS NOT NULL replaces the FK validation
SELECT ss_customer_sk, sum(ss_quantity*ss_sales_price)
FROM store_sales
WHERE ss_customer_sk IS NOT NULL  -- same filtering effect
GROUP BY ss_customer_sk
```

**Why**: The join `ss_customer_sk = c_customer_sk` implicitly filters out NULL foreign keys. If you only need the FK value (not any customer columns), use IS NOT NULL instead.

**Q23 Result**: Applied to 3 CTEs (`frequent_ss_items`, `max_store_sales`, `best_ss_customer`), eliminating joins to `item` (204K rows) and `customer` (12M rows × 2). **2.18x speedup** (24.5s → 11.3s).

---

## 4. Correlated Subquery → Window Function

**Signal**: A correlated subquery computes an aggregate per group (e.g., average per store).

**Fix**: Replace with a window function in the CTE.

**Example** (Q1):

```sql
-- BEFORE: Correlated subquery calculates avg per store
SELECT c_customer_id
FROM customer_total_return ctr1, store, customer
WHERE ctr1.ctr_total_return > (
    SELECT avg(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk  -- correlated
  )
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'

-- AFTER: Window function computes avg inline
WITH customer_total_return AS (
  SELECT sr_customer_sk, sr_store_sk,
         sum(sr_return_amt) AS ctr_total_return,
         avg(sum(sr_return_amt)) OVER (PARTITION BY sr_store_sk) AS ctr_store_avg
  FROM store_returns, date_dim, store
  WHERE ... AND s_state = 'TN'
  GROUP BY sr_customer_sk, sr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1, customer
WHERE ctr1.ctr_total_return > (ctr1.ctr_store_avg * 1.2)
```

**Result**: Eliminates correlated subquery. **2.54x speedup** (combined with predicate pushdown).

---

## Verification Notes

- All optimizations tested on TPC-DS SF100 (30GB DuckDB)
- Semantic correctness verified: original result == optimized result
- Timings: 3 runs, discard first, average remaining 2
