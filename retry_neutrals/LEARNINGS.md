# 4-Worker Neutral Query Learnings - Feb 6, 2026

## Summary
- **30/43 neutrals improved** (70%)
- Converted 20 queries from NEUTRAL to WIN (≥1.5x)
- New leaderboard: 34 WIN, 25 IMPROVED, 14 NEUTRAL, 15 REGRESSION

## Top Discoveries

### Q88: 5.25x (W4) - Time Bucket Aggregation ⭐
**Pattern**: Like single_pass_aggregation but for time-based buckets
- Original: 8 separate subqueries for each 30-min time slot
- Optimized: Single CTE with time_slot CASE bucketing + single-pass aggregation
```sql
WITH time_slots AS (
  SELECT t_time_sk,
    CASE WHEN t_hour = 8 AND t_minute >= 30 THEN 1
         WHEN t_hour = 9 AND t_minute < 30 THEN 2 ... END AS time_slot
  FROM time_dim WHERE ...
),
qualified_sales AS (
  SELECT ss.*, ts.time_slot FROM store_sales ss
  JOIN time_slots ts ON ...
)
SELECT SUM(CASE WHEN time_slot = 1 THEN ... END) AS h8_30_to_9, ...
FROM qualified_sales
```

### Q40: 3.35x (W2) - Multi-CTE Chain
**Pattern**: Progressive filtering through multiple CTEs
- filtered_dates → filtered_items → filtered_catalog_sales → sales_with_warehouse
- Each CTE narrows data before next join

### Q46: 3.23x (W3) - Triple Dimension Isolation
**Pattern**: Pre-filter 3 dimensions before fact join
```sql
WITH filtered_dates AS (...),
     filtered_store AS (...),
     filtered_hd AS (...)
SELECT ... FROM store_sales
JOIN filtered_dates ON ...
JOIN filtered_store ON ...
JOIN filtered_hd ON ...
```

### Q42, Q52: 2.80x, 2.50x (W3) - Dual Dimension Isolate
**Pattern**: Pre-filter both date_dim AND item into CTEs
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE ...),
     filtered_items AS (SELECT i_item_sk, i_brand FROM item WHERE ...)
SELECT ... FROM store_sales
JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
JOIN filtered_items ON ss_item_sk = i_item_sk
```

### Q77: 2.56x (W4) - Channel Split with Union
**Pattern**: Separate CTEs per sales channel, then UNION ALL
```sql
WITH ss AS (SELECT ... FROM store_sales JOIN filtered_dates ...),
     sr AS (SELECT ... FROM store_returns JOIN filtered_dates ...),
     cs AS (SELECT ... FROM catalog_sales ...),
     ...
SELECT ... FROM (
  SELECT 'store channel' AS channel, ... FROM ss LEFT JOIN sr ...
  UNION ALL
  SELECT 'catalog channel' AS channel, ... FROM cs, cr
  UNION ALL
  SELECT 'web channel' AS channel, ... FROM ws LEFT JOIN wr ...
)
```

## Worker Performance Analysis

| Worker | Strategy | Wins | Best Result |
|--------|----------|------|-------------|
| W1 | decorrelate, pushdown, early_filter | 7 | Q23: 2.33x |
| W2 | CTE isolation (date, dimension, multi_date_range) | 9 | Q40: 3.35x |
| W3 | fact prefetch (prefetch_fact_join, multi_dimension_prefetch) | 8 | Q46: 3.23x |
| W4 | consolidation + set ops (single_pass_aggregation, or_to_union) | 6 | Q88: 5.25x |

## New Patterns to Codify

1. **time_bucket_aggregation** - For queries with multiple time-based subqueries
2. **multi_cte_chain** - Progressive filtering through 3+ CTEs
3. **triple_dimension_isolate** - Pre-filter 3 dimension tables
4. **dual_dimension_isolate** - Pre-filter date + item dimensions

## Queries Still Neutral/Regression (13)

Need different approaches:
- Q45, Q79, Q33: Semantic failures on best attempts
- Q8, Q31, Q20: W2 CTE isolation didn't help
- Q25, Q71, Q64, Q57, Q68: Best ~1.0x, no clear win

## Conclusion

The 4-worker strategy with expanded gold examples is highly effective:
- 70% of neutrals improved
- W4 (consolidation patterns) produced the biggest win (Q88: 5.25x)
- W3 (fact prefetch) consistently delivers 2-3x improvements
- CTE isolation patterns remain the most broadly applicable
