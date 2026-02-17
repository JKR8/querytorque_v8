You are coordinating a swarm of 4 optimization specialists. Each specialist will attempt to optimize the same query using a DIFFERENT strategy and set of examples.

Your job: analyze the query structure, identify 4 diverse optimization angles, and assign each specialist a unique strategy with 3 relevant examples. Maximize diversity to cover the optimization space.

## Query: query_88
## Dialect: duckdb

```sql
-- start query 88 in stream 0 using template query88.tpl
select  *
from
 (select count(*) h8_30_to_9
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk   
     and ss_hdemo_sk = household_demographics.hd_demo_sk 
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 8
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2)) 
     and store.s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 9 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s2,
 (select count(*) h9_30_to_10 
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 9
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s3,
 (select count(*) h10_to_10_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s4,
 (select count(*) h10_30_to_11
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 10 
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s5,
 (select count(*) h11_to_11_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk 
     and time_dim.t_hour = 11
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s6,
 (select count(*) h11_30_to_12
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 11
     and time_dim.t_minute >= 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s7,
 (select count(*) h12_to_12_30
 from store_sales, household_demographics , time_dim, store
 where ss_sold_time_sk = time_dim.t_time_sk
     and ss_hdemo_sk = household_demographics.hd_demo_sk
     and ss_store_sk = s_store_sk
     and time_dim.t_hour = 12
     and time_dim.t_minute < 30
     and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
          (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
          (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
     and store.s_store_name = 'ese') s8
;

-- end query 88 in stream 0 using template query88.tpl
```

## Logical Tree Structure & Bottlenecks

| Node | Role | Cost % | Key Operations |
|------|------|-------:|----------------|
| main_query |  | 0.0% | — |

## Top 16 Matched Examples (by structural similarity)

1. **channel_bitmap_aggregation** (6.24x) — Consolidate repeated scans of the same fact table (one per time/channel bucket) into a single scan with CASE WHEN labels
2. **prefetch_fact_join** (3.77x) — Pre-filter dimension table into CTE, then pre-join with fact table in second CTE before joining other dimensions
3. **intersect_to_exists** (1.83x) — Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning
4. **multi_date_range_cte** (2.35x) — When query uses multiple date_dim aliases with different filters (d1, d2, d3), create separate CTEs for each date range 
5. **multi_intersect_exists_cte** (2.39x) — Convert cascading INTERSECT operations into correlated EXISTS subqueries with pre-materialized date and channel CTEs
6. **rollup_to_union_windowing** (2.47x) — Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each hierarchy level, combined with window fun
7. **shared_dimension_multi_channel** (1.30x) — Extract shared dimension filters (date, item, promotion) into CTEs when multiple channel CTEs (store/catalog/web) apply 
8. **or_to_union** (3.17x) — Split OR conditions on different columns into UNION ALL branches for better index usage
9. **composite_decorrelate_union** (2.42x) — Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT customer CTEs with a shared date filter
10. **date_cte_isolate** (4.00x) — Extract date filtering into a separate CTE to enable predicate pushdown and reduce scans
11. **decorrelate** (2.92x) — Convert correlated subquery to separate CTE with GROUP BY, then JOIN
12. **deferred_window_aggregation** (1.36x) — When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joined with FULL OUTER JOIN followed by ano
13. **early_filter** (4.00x) — Filter dimension tables FIRST, then join to fact tables to reduce expensive joins
14. **materialize_cte** (1.37x) — Extract repeated subquery patterns into a CTE to avoid recomputation
15. **multi_dimension_prefetch** (2.71x) — Pre-filter multiple dimension tables (date + store) into separate CTEs before joining with fact table
16. **union_cte_split** (1.36x) — Split a generic UNION ALL CTE into specialized CTEs when the main query filters by year or discriminator - eliminates re

## All Available Examples (full catalog — can swap if needed)

- **channel_bitmap_aggregation** (6.24x) — Consolidate repeated scans of the same fact table (one per time/channel bucket) 
- **composite_decorrelate_union** (2.42x) — Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT
- **date_cte_isolate** (4.00x) — Extract date filtering into a separate CTE to enable predicate pushdown and redu
- **decorrelate** (2.92x) — Convert correlated subquery to separate CTE with GROUP BY, then JOIN
- **deferred_window_aggregation** (1.36x) — When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joi
- **dimension_cte_isolate** (1.93x) — Pre-filter ALL dimension tables into CTEs before joining with fact table, not ju
- **early_filter** (4.00x) — Filter dimension tables FIRST, then join to fact tables to reduce expensive join
- **intersect_to_exists** (1.83x) — Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join pl
- **materialize_cte** (1.37x) — Extract repeated subquery patterns into a CTE to avoid recomputation
- **multi_date_range_cte** (2.35x) — When query uses multiple date_dim aliases with different filters (d1, d2, d3), c
- **multi_dimension_prefetch** (2.71x) — Pre-filter multiple dimension tables (date + store) into separate CTEs before jo
- **multi_intersect_exists_cte** (2.39x) — Convert cascading INTERSECT operations into correlated EXISTS subqueries with pr
- **or_to_union** (3.17x) — Split OR conditions on different columns into UNION ALL branches for better inde
- **prefetch_fact_join** (3.77x) — Pre-filter dimension table into CTE, then pre-join with fact table in second CTE
- **pushdown** (2.11x) — Push filters from outer query into CTEs/subqueries to reduce intermediate result
- **rollup_to_union_windowing** (2.47x) — Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each h
- **shared_dimension_multi_channel** (1.30x) — Extract shared dimension filters (date, item, promotion) into CTEs when multiple
- **single_pass_aggregation** (4.47x) — Consolidate multiple subqueries scanning the same table into a single CTE with c
- **union_cte_split** (1.36x) — Split a generic UNION ALL CTE into specialized CTEs when the main query filters 

## Regression Warnings (review relevance to THIS query)

These transforms caused regressions on structurally similar queries. Review each — if relevant to this query, AVOID the listed transform. If not relevant (different structure/bottleneck), you may ignore.

- **regression_q67_date_cte_isolate** (0.85x) — Materialized date, store, and item dimension filters into CTEs before a ROLLUP aggregation with window functions (RANK()
- **regression_q90_materialize_cte** (0.59x) — Split a simple OR condition (t_hour BETWEEN 10 AND 11 OR t_hour BETWEEN 16 AND 17) into UNION ALL of two separate web_sa
- **regression_q1_decorrelate** (0.71x) — Pre-computed customer_total_return (GROUP BY customer, store) and store_avg_return (GROUP BY store) as separate CTEs. Th

## Your Task

Design 4 DIFFERENT optimization strategies exploring diverse approaches. You may keep the matched recommendations OR swap examples from the catalog.

**Constraints**:
- Each worker gets exactly 3 examples
- No duplicate examples across workers (12 total, 3 per worker)
- If fewer than 12 unique examples are available, reuse is allowed

**Diversity guidelines**:
- Worker 1: Conservative — proven patterns, low risk (e.g., pushdown, early filter)
- Worker 2: Moderate — date/dimension isolation, CTE restructuring
- Worker 3: Aggressive — multi-CTE restructuring, prefetch patterns
- Worker 4: Novel — OR-to-UNION, structural transforms, intersect-to-exists

For each worker, specify:
1. **Strategy name** (e.g., `aggressive_date_prefetch`)
2. **3 examples** to use (from matched picks or catalog)
3. **Strategy hint** (1-2 sentences guiding the optimization approach)

### Output Format (follow EXACTLY)

```
WORKER_1:
STRATEGY: <strategy_name>
EXAMPLES: <ex1>, <ex2>, <ex3>
HINT: <strategy guidance>

WORKER_2:
STRATEGY: <strategy_name>
EXAMPLES: <ex4>, <ex5>, <ex6>
HINT: <strategy guidance>

WORKER_3:
STRATEGY: <strategy_name>
EXAMPLES: <ex7>, <ex8>, <ex9>
HINT: <strategy guidance>

WORKER_4:
STRATEGY: <strategy_name>
EXAMPLES: <ex10>, <ex11>, <ex12>
HINT: <strategy guidance>
```