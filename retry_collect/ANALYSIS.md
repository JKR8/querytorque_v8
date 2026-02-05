# 3-Worker Retry Analysis - Feb 6, 2026

## Summary
- **14/25 queries improved** on SF10
- **W2 (CTE optimizations) dominated** - won 9/14 improvements
- **New pattern discovered**: single_pass_aggregation (Q9: 4.47x)

## Transform Patterns Identified

### 1. NEW: single_pass_aggregation (Q9: 4.47x) ⭐
**Original Problem**: Query has multiple subqueries that each scan the same large table
```sql
SELECT
  CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) > X
       THEN (SELECT avg(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20)
       ELSE (SELECT avg(ss_net_paid) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) END,
  CASE WHEN (SELECT count(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) > Y
       ... -- 15 total scans of store_sales!
```

**Solution**: Consolidate into single CTE computing all metrics in one pass
```sql
WITH store_sales_aggregates AS (
  SELECT
    SUM(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 ELSE 0 END) AS cnt1,
    AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_discount_amt END) AS avg_disc1,
    AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_paid END) AS avg_paid1,
    -- ... all 5 buckets in one scan
  FROM store_sales
)
SELECT CASE WHEN cnt1 > X THEN avg_disc1 ELSE avg_paid1 END AS bucket1, ...
FROM store_sales_aggregates
```

**Why it works**: Reduces 15 table scans to 1. Each scan was filtering by quantity range - now done with CASE expressions in a single pass.

### 2. dimension_cte_isolate (Q26: 1.93x, Q73: 1.57x, Q96: 1.64x)
**Pattern**: Pre-filter ALL dimension tables into CTEs, not just dates
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000),
     filtered_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'M' ...),
     filtered_promotions AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' ...)
SELECT ... FROM fact_table
JOIN filtered_dates ON ...
JOIN filtered_demographics ON ...
```

### 3. date_cte_isolate + early_filter (Q63: 3.77x, Q43: 2.71x)
**Pattern**: Pre-filter date_dim, then pre-join with fact table
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_moy FROM date_dim WHERE d_month_seq IN (...)),
     filtered_sales AS (SELECT ... FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk)
SELECT ... FROM filtered_sales JOIN item ...
```

### 4. multi_date_range_cte (Q29: 2.35x)
**Pattern**: When query uses multiple date_dim aliases with different filters, create separate CTEs
```sql
WITH d1_dates AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 9 AND d_year = 1999),
     d2_dates AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 9 AND 12 AND d_year = 1999),
     d3_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000, 2001)),
     filtered_store_sales AS (SELECT ... FROM store_sales JOIN d1_dates ON ...),
     filtered_store_returns AS (SELECT ... FROM store_returns JOIN d2_dates ON ...),
     filtered_catalog_sales AS (SELECT ... FROM catalog_sales JOIN d3_dates ON ...)
```

## New Gold Examples to Add

### 1. single_pass_aggregation (from Q9)
- **Speedup**: 4.47x on SF10
- **Trigger**: Multiple scalar subqueries scanning same table
- **Transform**: Consolidate into single CTE with conditional aggregates

### 2. dimension_cte_isolate (from Q26)
- **Speedup**: 1.93x on SF10
- **Trigger**: Multiple dimension table filters in WHERE clause
- **Transform**: Pre-filter each dimension into CTE before joining

### 3. multi_date_range_cte (from Q29)
- **Speedup**: 2.35x on SF10
- **Trigger**: Multiple date_dim aliases with different filters (d1, d2, d3)
- **Transform**: Separate CTE for each date range, pre-join with fact tables

## Worker Performance

| Worker | Strategy | Wins | Top Result |
|--------|----------|------|------------|
| W2 | CTE optimizations | 9 | Q9: 4.47x |
| W1 | decorrelate, pushdown, early_filter | 4 | Q29: 2.35x |
| W3 | or_to_union, intersect_to_exists | 1 | Q12: 1.23x |

**Conclusion**: CTE isolation strategies (W2) outperformed subquery decorrelation (W1) on this batch.
