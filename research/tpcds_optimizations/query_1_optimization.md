# TPC-DS Query 1 Optimization

## Benchmark Results

| Run | Original | Optimized |
|-----|----------|-----------|
| Run 1 (cold) | 0.896s | 0.111s |
| Run 2 (warm) | 0.267s | 0.122s |
| **Average** | **0.581s** | **0.116s** |

**Speedup: 5x**
**Results Match: Yes (verified identical output)**

Database: TPC-DS SF100 (~28GB DuckDB)

---

## Original Query

```sql
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
LIMIT 100;
```

---

## Optimized Query

```sql
WITH sd_store_returns AS (
    SELECT
        sr_customer_sk,
        sr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN store ON sr_store_sk = s_store_sk
    WHERE d_year = 2000
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
),
high_return_candidates AS (
    SELECT
        sr_customer_sk,
        ctr_total_return
    FROM (
        SELECT
            sr_customer_sk,
            ctr_total_return,
            AVG(ctr_total_return) OVER (PARTITION BY sr_store_sk) as store_avg
        FROM sd_store_returns
    )
    WHERE ctr_total_return > (store_avg * 1.2)
)
SELECT
    c_customer_id
FROM high_return_candidates
JOIN customer ON c_customer_sk = sr_customer_sk
ORDER BY c_customer_id
LIMIT 100;
```

---

## Optimization Techniques Applied

### 1. Predicate Pushdown
**Before:** Filter `s_state = 'SD'` applied after full aggregation of all stores
**After:** Filter applied before aggregation, reducing data volume immediately

```sql
-- Original: Aggregates ALL stores, then filters
FROM customer_total_return ctr1, store
WHERE s_state = 'SD'

-- Optimized: Only aggregates SD stores
FROM store_returns
JOIN store ON sr_store_sk = s_store_sk
WHERE s_state = 'SD'
GROUP BY ...
```

**Impact:** Reduces rows processed in aggregation from ~29M to only SD-state returns

### 2. Window Function Replaces Correlated Subquery
**Before:** Correlated subquery recalculates average for each row
**After:** Single-pass window function computes average per partition

```sql
-- Original: O(n^2) - subquery runs for each row
WHERE ctr1.ctr_total_return > (
    SELECT avg(ctr_total_return)*1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk
)

-- Optimized: O(n) - single pass with window
AVG(ctr_total_return) OVER (PARTITION BY sr_store_sk) as store_avg
WHERE ctr_total_return > (store_avg * 1.2)
```

**Impact:** Eliminates repeated scans of the CTE

### 3. Early Join for Filter Enablement
**Before:** Store table joined late, after aggregation
**After:** Store table joined early to enable predicate pushdown

```sql
-- Original: Late join
FROM store_returns, date_dim
...
FROM customer_total_return, store  -- store joined after aggregation

-- Optimized: Early join
FROM store_returns
JOIN date_dim ON sr_returned_date_sk = d_date_sk
JOIN store ON sr_store_sk = s_store_sk  -- joined before aggregation
WHERE s_state = 'SD'  -- filter can now be pushed down
```

### 4. Late Materialization
**Before:** Customer table joined early with full result set
**After:** Customer table joined only after filtering to candidates

**Impact:** Reduces customer lookups to only qualifying rows

---

## Failed Optimization Attempt

Initially tried adding `LIMIT 100` inside the `high_return_candidates` CTE:

```sql
high_return_candidates AS (
    ...
    WHERE ctr_total_return > (store_avg * 1.2)
    LIMIT 100  -- WRONG: limits before ORDER BY
)
```

**Problem:** This produced different results because it limited rows before the final `ORDER BY c_customer_id`. The original query orders first, then limits.

**Lesson:** LIMIT placement matters for semantic equivalence.

---

## Summary

| Technique | Impact |
|-----------|--------|
| Predicate Pushdown | Reduces aggregation input by filtering early |
| Window Function | Eliminates O(n^2) correlated subquery |
| Early Join | Enables predicate pushdown on store filter |
| Late Materialization | Defers expensive customer lookup |

Combined effect: **5x speedup** with verified correctness.
