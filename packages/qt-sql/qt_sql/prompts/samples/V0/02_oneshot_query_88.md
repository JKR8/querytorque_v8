You are a senior query optimization architect. Your job is to deeply analyze a SQL query, determine the single best optimization strategy, and then produce the optimized SQL directly.

You have all the data: EXPLAIN plans, DAG costs, full constraint list, global knowledge, and the complete example catalog. Analyze thoroughly, then implement the best strategy as working SQL.

## Query: query_88
## Dialect: duckdb v1.4.3

```sql
 1 | select  *
 2 | from
 3 |  (select count(*) h8_30_to_9
 4 |  from store_sales, household_demographics , time_dim, store
 5 |  where ss_sold_time_sk = time_dim.t_time_sk   
 6 |      and ss_hdemo_sk = household_demographics.hd_demo_sk 
 7 |      and ss_store_sk = s_store_sk
 8 |      and time_dim.t_hour = 8
 9 |      and time_dim.t_minute >= 30
10 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
11 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
12 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2)) 
13 |      and store.s_store_name = 'ese') s1,
14 |  (select count(*) h9_to_9_30 
15 |  from store_sales, household_demographics , time_dim, store
16 |  where ss_sold_time_sk = time_dim.t_time_sk
17 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
18 |      and ss_store_sk = s_store_sk 
19 |      and time_dim.t_hour = 9 
20 |      and time_dim.t_minute < 30
21 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
22 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
23 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
24 |      and store.s_store_name = 'ese') s2,
25 |  (select count(*) h9_30_to_10 
26 |  from store_sales, household_demographics , time_dim, store
27 |  where ss_sold_time_sk = time_dim.t_time_sk
28 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
29 |      and ss_store_sk = s_store_sk
30 |      and time_dim.t_hour = 9
31 |      and time_dim.t_minute >= 30
32 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
33 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
34 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
35 |      and store.s_store_name = 'ese') s3,
36 |  (select count(*) h10_to_10_30
37 |  from store_sales, household_demographics , time_dim, store
38 |  where ss_sold_time_sk = time_dim.t_time_sk
39 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
40 |      and ss_store_sk = s_store_sk
41 |      and time_dim.t_hour = 10 
42 |      and time_dim.t_minute < 30
43 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
44 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
45 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
46 |      and store.s_store_name = 'ese') s4,
47 |  (select count(*) h10_30_to_11
48 |  from store_sales, household_demographics , time_dim, store
49 |  where ss_sold_time_sk = time_dim.t_time_sk
50 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
51 |      and ss_store_sk = s_store_sk
52 |      and time_dim.t_hour = 10 
53 |      and time_dim.t_minute >= 30
54 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
55 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
56 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
57 |      and store.s_store_name = 'ese') s5,
58 |  (select count(*) h11_to_11_30
59 |  from store_sales, household_demographics , time_dim, store
60 |  where ss_sold_time_sk = time_dim.t_time_sk
61 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
62 |      and ss_store_sk = s_store_sk 
63 |      and time_dim.t_hour = 11
64 |      and time_dim.t_minute < 30
65 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
66 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
67 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
68 |      and store.s_store_name = 'ese') s6,
69 |  (select count(*) h11_30_to_12
70 |  from store_sales, household_demographics , time_dim, store
71 |  where ss_sold_time_sk = time_dim.t_time_sk
72 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
73 |      and ss_store_sk = s_store_sk
74 |      and time_dim.t_hour = 11
75 |      and time_dim.t_minute >= 30
76 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
77 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
78 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
79 |      and store.s_store_name = 'ese') s7,
80 |  (select count(*) h12_to_12_30
81 |  from store_sales, household_demographics , time_dim, store
82 |  where ss_sold_time_sk = time_dim.t_time_sk
83 |      and ss_hdemo_sk = household_demographics.hd_demo_sk
84 |      and ss_store_sk = s_store_sk
85 |      and time_dim.t_hour = 12
86 |      and time_dim.t_minute < 30
87 |      and ((household_demographics.hd_dep_count = -1 and household_demographics.hd_vehicle_count<=-1+2) or
88 |           (household_demographics.hd_dep_count = 4 and household_demographics.hd_vehicle_count<=4+2) or
89 |           (household_demographics.hd_dep_count = 3 and household_demographics.hd_vehicle_count<=3+2))
90 |      and store.s_store_name = 'ese') s8
91 | ;
```

## EXPLAIN ANALYZE Plan

```
Total execution time: 2254ms

CROSS_PRODUCT [1 rows]
  CROSS_PRODUCT [1 rows]
    CROSS_PRODUCT [1 rows]
      CROSS_PRODUCT [1 rows]
        CROSS_PRODUCT [1 rows]
          CROSS_PRODUCT [1 rows]
            CROSS_PRODUCT [1 rows]
              UNGROUPED_AGGREGATE [1 rows, 0.1ms]
                Aggregates: count_star()
                HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [25K rows, 1.1ms]
                  HASH_JOIN INNER on ss_store_sk = s_store_sk [110K rows, 8.7ms]
                    HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [719K rows, 38.3ms, 2%]
                      SEQ_SCAN  store_sales [719K of 345.6M rows, 231.2ms, 10%]
                      FILTER [1,800 rows]
                        Expression: (t_time_sk BETWEEN 28800 AND 75599)
                        SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=11, t_minute>=30
                    FILTER [14 rows]
                      Expression: (s_store_sk <= 100)
                      SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
                  FILTER [1,440 rows]
                    Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
                    SEQ_SCAN  household_demographics [7,200 rows]
              UNGROUPED_AGGREGATE [1 rows, 0.1ms]
                Aggregates: count_star()
                HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [28K rows, 1.1ms]
                  HASH_JOIN INNER on ss_store_sk = s_store_sk [126K rows, 12.0ms]
                    HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [831K rows, 34.8ms, 2%]
                      SEQ_SCAN  store_sales [831K of 345.6M rows, 229.8ms, 10%]
                      FILTER [1,800 rows]
                        Expression: (t_time_sk BETWEEN 28800 AND 75599)
                        SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=12, t_minute<30
                    FILTER [14 rows]
                      Expression: (s_store_sk <= 100)
                      SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
                  FILTER [1,440 rows]
                    Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
                    SEQ_SCAN  household_demographics [7,200 rows]
            UNGROUPED_AGGREGATE [1 rows, 0.1ms]
              Aggregates: count_star()
              HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [26K rows, 1.1ms]
                HASH_JOIN INNER on ss_store_sk = s_store_sk [111K rows, 8.7ms]
                  HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [721K rows, 35.6ms, 2%]
                    SEQ_SCAN  store_sales [721K of 345.6M rows, 226.1ms, 10%]
                    FILTER [1,800 rows]
                      Expression: (t_time_sk BETWEEN 28800 AND 75599)
                      SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=11, t_minute<30
                  FILTER [14 rows]
                    Expression: (s_store_sk <= 100)
                    SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
                FILTER [1,440 rows]
                  Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
                  SEQ_SCAN  household_demographics [7,200 rows]
          UNGROUPED_AGGREGATE [1 rows]
            Aggregates: count_star()
            HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [45K rows, 1.1ms]
              HASH_JOIN INNER on ss_store_sk = s_store_sk [192K rows, 22.4ms]
                HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [1.3M rows, 26.9ms, 1%]
                  SEQ_SCAN  store_sales [1.3M of 345.6M rows, 234.8ms, 10%]
                  FILTER [1,800 rows]
                    Expression: (t_time_sk BETWEEN 28800 AND 75599)
                    SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=10, t_minute>=30
                FILTER [14 rows]
                  Expression: (s_store_sk <= 100)
                  SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
              FILTER [1,440 rows]
                Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
                SEQ_SCAN  household_demographics [7,200 rows]
        UNGROUPED_AGGREGATE [1 rows]
          Aggregates: count_star()
          HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [42K rows, 1.1ms]
            HASH_JOIN INNER on ss_store_sk = s_store_sk [188K rows, 21.8ms]
              HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [1.2M rows, 28.1ms, 1%]
                SEQ_SCAN  store_sales [1.2M of 345.6M rows, 239.1ms, 11%]
                FILTER [1,800 rows]
                  Expression: (t_time_sk BETWEEN 28800 AND 75599)
                  SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=10, t_minute<30
              FILTER [14 rows]
                Expression: (s_store_sk <= 100)
                SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
            FILTER [1,440 rows]
              Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
              SEQ_SCAN  household_demographics [7,200 rows]
      UNGROUPED_AGGREGATE [1 rows, 0.1ms]
        Aggregates: count_star()
        HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [29K rows, 1.0ms]
          HASH_JOIN INNER on ss_store_sk = s_store_sk [126K rows, 11.9ms]
            HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [832K rows, 35.6ms, 2%]
              SEQ_SCAN  store_sales [832K of 345.6M rows, 233.2ms, 10%]
              FILTER [1,800 rows]
                Expression: (t_time_sk BETWEEN 28800 AND 75599)
                SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=9, t_minute>=30
            FILTER [14 rows]
              Expression: (s_store_sk <= 100)
              SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
          FILTER [1,440 rows]
            Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
            SEQ_SCAN  household_demographics [7,200 rows]
    UNGROUPED_AGGREGATE [1 rows, 0.1ms]
      Aggregates: count_star()
      HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [29K rows, 1.2ms]
        HASH_JOIN INNER on ss_store_sk = s_store_sk [127K rows, 12.1ms]
          HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [834K rows, 36.1ms, 2%]
            SEQ_SCAN  store_sales [834K of 345.6M rows, 240.8ms, 11%]
            FILTER [1,800 rows]
              Expression: (t_time_sk BETWEEN 28800 AND 75599)
              SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=9, t_minute<30
          FILTER [14 rows]
            Expression: (s_store_sk <= 100)
            SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
        FILTER [1,440 rows]
          Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
          SEQ_SCAN  household_demographics [7,200 rows]
  UNGROUPED_AGGREGATE [1 rows]
    Aggregates: count_star()
    HASH_JOIN INNER on ss_hdemo_sk = hd_demo_sk [15K rows, 1.0ms]
      HASH_JOIN INNER on ss_store_sk = s_store_sk [64K rows, 2.5ms]
        HASH_JOIN INNER on ss_sold_time_sk = t_time_sk [419K rows, 36.4ms, 2%]
          SEQ_SCAN  store_sales [419K of 345.6M rows, 236.3ms, 10%]
          FILTER [1,800 rows]
            Expression: (t_time_sk BETWEEN 28800 AND 75599)
            SEQ_SCAN  time_dim [1,800 of 86K rows]  Filters: t_hour=8, t_minute>=30
        FILTER [14 rows]
          Expression: (s_store_sk <= 100)
          SEQ_SCAN  store [14 of 102 rows]  Filters: s_store_name='ese'
      FILTER [1,440 rows]
        Expression: ((hd_dep_count = 4) OR (hd_dep_count = 3))
        SEQ_SCAN  household_demographics [7,200 rows]
```

**NOTE:** The EXPLAIN plan shows the PHYSICAL execution structure, which may differ significantly from the LOGICAL DAG below. The optimizer may have already split CTEs, reordered joins, or pushed predicates. When the EXPLAIN and DAG disagree, the EXPLAIN is ground truth for what the optimizer is already doing.

DuckDB EXPLAIN ANALYZE reports **operator-exclusive** wall-clock time per node (children's time is NOT included in the parent's reported time). The percentage annotations are also exclusive. You can sum sibling nodes to get pipeline cost. DAG cost percentages are derived metrics that may not reflect actual execution time — use EXPLAIN timings as ground truth.

## Query Structure (Logic Tree)

```
QUERY: (single statement)
└── [MAIN] main_query  [=]  Cost: 86%  Rows: ~1.3M  — Compute eight independent time-slice counts under identical household/store filters and return them in one cross-joined row for side-by-side comparison.
    ├── SCAN (store_sales, household_demographics, time_dim, store)
    ├── JOIN (ss_sold_time_sk = time_dim.t_time_sk)
    ├── JOIN (ss_hdemo_sk = household_demographics.hd_demo_sk)
    ├── JOIN (+1 more)
    ├── FILTER (time_dim.t_hour = 8)
    ├── FILTER (time_dim.t_minute >= 30)
    ├── FILTER (+1 more)
    └── OUTPUT (*)
```

## Node Details

### 1. main_query
**Role**: Root / Output (Definition Order: 0)
**Intent**: Compute eight independent time-slice counts under identical household/store filters and return them in one cross-joined row for side-by-side comparison.
**Stats**: 86% Cost | ~1.3M rows
**Outputs**: [*]
**Dependencies**: store_sales, household_demographics, time_dim, store
**Joins**: ss_sold_time_sk = time_dim.t_time_sk | ss_hdemo_sk = household_demographics.hd_demo_sk | ss_store_sk = s_store_sk
**Filters**: time_dim.t_hour = 8 | time_dim.t_minute >= 30 | store.s_store_name = 'ese'
**Operators**: SEQ_SCAN[store_sales], SEQ_SCAN[time_dim], SEQ_SCAN[store], SEQ_SCAN[household_demographics], SEQ_SCAN[store_sales]
**Key Logic (SQL)**:
```sql
SELECT
  *
FROM (
  SELECT
    COUNT(*) AS h8_30_to_9
  FROM store_sales, household_demographics, time_dim, store
  WHERE
    ss_sold_time_sk = time_dim.t_time_sk
    AND ss_hdemo_sk = household_demographics.hd_demo_sk
    AND ss_store_sk = s_store_sk
    AND time_dim.t_hour = 8
    AND time_dim.t_minute >= 30
    AND (
      (
        household_demographics.hd_dep_count = -1
        AND household_demographics.hd_vehicle_count <= -1 + 2
      )
      OR (
        household_demographics.hd_dep_count = 4
        AND household_demographics.hd_vehicle_count <= 4 + 2
...
```


## Pre-Computed Semantic Intent

**Query intent:** Count store sales by consecutive half-hour windows from 8:30 to 12:30 at store "ese" for households matching specific dependent/vehicle constraints.

START from this pre-computed intent. In your SEMANTIC_CONTRACT output, ENRICH it with: intersection/union semantics from JOIN types, aggregation function traps, NULL propagation paths, and filter dependencies. Do NOT re-derive what is already stated above.

## Aggregation Semantics Check

You MUST verify aggregation equivalence for any proposed restructuring:

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL for 0-1 values. Changing group membership changes the result.
- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — FILTER still uses the combined group's membership for the stddev denominator.
- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row duplication, the aggregate result changes.
- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split branch must preserve the exact GROUP BY columns and filter to the exact same row set as the original.
- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column (e.g., d_year), each group is already partitioned. STDDEV_SAMP computed per-group is correct. You can then pivot using `MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` because the GROUP BY guarantees exactly one row per (customer, year) — the MAX is just a row selector, not a real aggregation.

## Top 16 Tag-Matched Examples

### channel_bitmap_aggregation (6.24x)
**Description:** Consolidate repeated scans of the same fact table (one per time/channel bucket) into a single scan with CASE WHEN labels and conditional aggregation
**When NOT to apply:** Do not use when the number of distinct buckets exceeds 8 (diminishing returns from CASE evaluation overhead). Also not applicable when each subquery has structurally different joins or table references.

### prefetch_fact_join (3.77x)
**Description:** Pre-filter dimension table into CTE, then pre-join with fact table in second CTE before joining other dimensions
**Principle:** Staged Join Pipeline: build a CTE chain that progressively reduces data — first CTE filters the dimension, second CTE pre-joins filtered dimension keys with the fact table, subsequent CTEs join remaining dimensions against the already-reduced fact set.
**When NOT to apply:** Do not use on queries with baseline runtime under 50ms — CTE materialization overhead dominates on fast queries. Do not use on window-function-dominated queries where filtering is not the bottleneck. Avoid on queries with 5+ table joins and complex inter-table predicates where forcing join order via CTEs prevents the optimizer from choosing a better plan. Caused 0.50x on Q25 (fast baseline query), 0.87x on Q51 (window-function bottleneck), and 0.77x on Q72 (complex multi-table join reordering).

### intersect_to_exists (1.83x)
**Description:** Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning
**Principle:** Semi-Join Short-Circuit: replace INTERSECT with EXISTS to avoid full materialization and sorting. INTERSECT must compute complete result sets before intersecting; EXISTS stops at the first match per row, enabling semi-join optimizations.

### multi_date_range_cte (2.35x)
**Description:** When query uses multiple date_dim aliases with different filters (d1, d2, d3), create separate CTEs for each date range and pre-join with fact tables
**Principle:** Early Selection per Alias: when a query joins the same dimension table multiple times with different filters (d1, d2, d3), create separate CTEs for each filter and pre-join with fact tables to reduce rows entering the main join.

### multi_intersect_exists_cte (2.39x)
**Description:** Convert cascading INTERSECT operations into correlated EXISTS subqueries with pre-materialized date and channel CTEs
**When NOT to apply:** Do not use when the INTERSECT operates on small result sets (< 1000 rows) where materialization cost is negligible. Also not applicable when the EXISTS correlation would be on non-indexed columns, as the correlated probe could be slower than the hash-based INTERSECT.

### rollup_to_union_windowing (2.47x)
**Description:** Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each hierarchy level, combined with window functions for ranking
**When NOT to apply:** Do not use when ROLLUP generates all levels efficiently (small dimension tables, few groups) or when the query genuinely needs all possible grouping set combinations. Only beneficial when specific levels need different optimization paths.

### shared_dimension_multi_channel (1.30x)
**Description:** Extract shared dimension filters (date, item, promotion) into CTEs when multiple channel CTEs (store/catalog/web) apply identical filters independently
**Principle:** Shared Dimension Extraction: when multiple channel CTEs (store/catalog/web) apply identical dimension filters, extract those shared filters into one CTE and reference it from each channel. Avoids redundant dimension scans.

### or_to_union (3.17x)
**Description:** Split OR conditions on different columns into UNION ALL branches for better index usage
**Principle:** OR-to-UNION Decomposition: split OR conditions on different columns into separate UNION ALL branches, each with a focused predicate. The optimizer can use different access paths per branch instead of a single scan with a complex filter.
**When NOT to apply:** Do not split OR when all branches filter the SAME column on the same table (e.g., t_hour >= 8 OR t_hour <= 17). This duplicates the entire fact table scan for each branch with no selectivity benefit. Only apply when OR conditions span DIFFERENT tables or fundamentally different column families. Also never split into more than 3 UNION branches — each branch rescans the fact table. Caused 0.59x on Q90 (same-column time range split doubled fact scans) and historically 0.23x-0.41x on queries with 9+ UNION branches.

### composite_decorrelate_union (2.42x)
**Description:** Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT customer CTEs with a shared date filter, and replace OR(EXISTS a, EXISTS b) with UNION of key sets
**Principle:** Composite Decorrelation: when multiple correlated EXISTS share common filters, extract shared dimensions into a single CTE and decorrelate the EXISTS checks into pre-materialized key sets joined via UNION.

### date_cte_isolate (4.00x)
**Description:** Extract date filtering into a separate CTE to enable predicate pushdown and reduce scans
**Principle:** Dimension Isolation: extract small dimension lookups into CTEs so they materialize once and subsequent joins probe a tiny hash table instead of rescanning.
**When NOT to apply:** Do not use when the optimizer already pushes date predicates effectively (e.g., simple equality filters on date columns in self-joins). Do not decompose an already-efficient existing CTE into sub-CTEs — this adds materialization overhead without reducing scans. Caused 0.49x regression on Q31 (DuckDB already optimized the date pushdown) and 0.71x on Q1 (decomposed a well-structured CTE into slower pieces).

### decorrelate (2.92x)
**Description:** Convert correlated subquery to separate CTE with GROUP BY, then JOIN
**Principle:** Decorrelation: convert correlated subqueries to standalone CTEs with GROUP BY, then JOIN. Correlated subqueries re-execute per outer row; a pre-computed CTE executes once.

### deferred_window_aggregation (1.36x)
**Description:** When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joined with FULL OUTER JOIN followed by another WINDOW pass for NULL carry-forward: defer the WINDOW out of the CTEs, join daily totals, then compute cumulative sums once on the joined result. SUM() OVER() naturally skips NULLs, eliminating the need for a separate MAX() carry-forward window.
**Principle:** Deferred Aggregation: delay expensive operations (window functions) until after joins reduce the dataset. Computing window functions inside individual CTEs then joining is more expensive than joining first and computing windows once on the combined result.
**When NOT to apply:** Do not use when the CTE window function is referenced by other consumers besides the final join (the cumulative value is needed elsewhere). Do not use when the window function is not a monotonically accumulating SUM - e.g., AVG, COUNT, or non-monotonic window functions require separate computation. Only applies when the join is FULL OUTER and the carry-forward window is MAX/LAST_VALUE over a cumulative sum.

### early_filter (4.00x)
**Description:** Filter dimension tables FIRST, then join to fact tables to reduce expensive joins
**Principle:** Early Selection: filter small dimension tables first, then join to large fact tables. This reduces the fact table scan to only rows matching the filter, rather than scanning all rows and filtering after the join.

### materialize_cte (1.37x)
**Description:** Extract repeated subquery patterns into a CTE to avoid recomputation
**Principle:** Shared Materialization: extract repeated subquery patterns into CTEs to avoid recomputation. When the same logical check appears multiple times, compute it once and reference the result.
**When NOT to apply:** NEVER convert EXISTS or NOT EXISTS subqueries into materialized CTEs when the EXISTS is used as a filter (not a data source). EXISTS uses semi-join short-circuiting — the database stops scanning as soon as one match is found. Materializing into a CTE forces a full scan of the subquery table, destroying this optimization. Caused 0.14x on Q16 (7x slowdown — EXISTS on catalog_sales materialized into full CTE scan) and 0.54x on Q95 (EXISTS on web_sales forced full materialization).

### multi_dimension_prefetch (2.71x)
**Description:** Pre-filter multiple dimension tables (date + store) into separate CTEs before joining with fact table
**Principle:** Multi-Dimension Prefetch: when multiple dimension tables have selective filters, pre-filter ALL of them into CTEs before the fact table join. Combined selectivity compounds — each dimension CTE reduces the fact scan further.
**When NOT to apply:** Do not create dimension CTEs without a WHERE clause that actually reduces rows — an unfiltered dimension CTE is pure overhead (full scan + materialization for zero selectivity benefit). Avoid on queries with 5+ tables and complex inter-table predicates where forcing join order via CTEs prevents the optimizer from choosing a better plan. Caused 0.85x on Q67 (unfiltered dimension CTEs added overhead) and 0.77x on Q72 (forced suboptimal join ordering on complex multi-table query).

### union_cte_split (1.36x)
**Description:** Split a generic UNION ALL CTE into specialized CTEs when the main query filters by year or discriminator - eliminates redundant scans
**Principle:** CTE Specialization: when a generic CTE is scanned multiple times with different filters (e.g., by year), split it into specialized CTEs that embed the filter in their definition. Each specialized CTE processes only its relevant subset, eliminating redundant scans.

## Additional Examples (not tag-matched to this query)

- **dimension_cte_isolate** (1.93x) — Pre-filter ALL dimension tables into CTEs before joining with fact table, not ju
- **pushdown** (2.11x) — Push filters from outer query into CTEs/subqueries to reduce intermediate result
- **single_pass_aggregation** (4.47x) — Consolidate multiple subqueries scanning the same table into a single CTE with c

## Optimization Principles (from benchmark history)

**Or To Union** (2.5x avg, 19 wins)
  Why: Converting OR to UNION ALL lets optimizer choose independent index paths per branch
  When: WHERE clause has OR conditions over different dimension keys (≤3 branches)
**Single Pass Aggregation** (2.5x avg, 5 wins)
  Why: Consolidating repeated scans into CASE aggregates reduces I/O from N scans to 1; Pre-joining filtered dimensions with fact table before aggregation reduces join input; Separate CTEs for each date alias avoids ambiguous multi-way date joins
  When: Query has repeated scans of the same fact table with different WHERE filters
**Dimension Cte Isolate** (2.1x avg, 7 wins)
  Why: Pre-filtering all dimension tables into CTEs avoids repeated full-table scans; Pre-filtering date dimension into CTE reduces hash join probe table from 73K to ~365 rows
  When: Query joins 2+ dimension tables that could each be pre-filtered independently
**Prefetch Fact Join** (1.8x avg, 18 wins)
  Why: Pre-joining filtered dimensions with fact table before aggregation reduces join input; Pre-filtering multiple dimension tables in parallel reduces join fan-out; Consolidating repeated scans into CASE aggregates reduces I/O from N scans to 1
  When: Query joins filtered dates/dims with large fact table; pre-join reduces probe size
**Date Cte Isolate** (1.8x avg, 48 wins)
  Why: Pre-filtering date dimension into CTE reduces hash join probe table from 73K to ~365 rows
  When: Query joins date_dim on multiple conditions (year, month, etc.) with fact tables

## Regression Examples

### regression_q67_date_cte_isolate: date_cte_isolate on q67 (0.85x)
**Anti-pattern:** Do not materialize dimension filters into CTEs before complex aggregations (ROLLUP, CUBE, GROUPING SETS) with window functions. The optimizer needs to push aggregation through joins; CTEs create materialization barriers.
**Mechanism:** Materialized date, store, and item dimension filters into CTEs before a ROLLUP aggregation with window functions (RANK() OVER). CTE materialization prevents the optimizer from pushing the ROLLUP and window computation down through the join tree, forcing a full materialized intermediate before the expensive aggregation.

### regression_q90_materialize_cte: materialize_cte on q90 (0.59x)
**Anti-pattern:** Never convert OR conditions on the SAME column (e.g., range conditions on t_hour) into UNION ALL. The optimizer already handles same-column ORs as a single scan. UNION ALL only helps when branches access fundamentally different tables or columns.
**Mechanism:** Split a simple OR condition (t_hour BETWEEN 10 AND 11 OR t_hour BETWEEN 16 AND 17) into UNION ALL of two separate web_sales scans. This doubles the fact table scan. DuckDB handles same-column OR ranges efficiently in a single scan — the UNION ALL adds materialization overhead with zero selectivity benefit.

### regression_q1_decorrelate: decorrelate on q1 (0.71x)
**Anti-pattern:** Do not pre-aggregate GROUP BY results into CTEs when the query uses them in a correlated comparison (e.g., customer return > 1.2x store average). The optimizer can compute aggregates incrementally with filter pushdown; materialization loses this.
**Mechanism:** Pre-computed customer_total_return (GROUP BY customer, store) and store_avg_return (GROUP BY store) as separate CTEs. The original correlated subquery computed the per-store average incrementally during the customer scan, filtering as it goes. Materializing forces full aggregation of ALL stores before any filtering.

## Engine Profile: Field Intelligence Briefing

*This is field intelligence gathered from 88 TPC-DS queries at SF1-SF10. Use it to guide your analysis but apply your own judgment — every query is different. Add to this knowledge if you observe something new.*

### Optimizer Strengths (DO NOT fight these)

- **INTRA_SCAN_PREDICATE_PUSHDOWN**: Pushes WHERE filters directly into SEQ_SCAN. Single-table predicates are applied at scan time, zero overhead.
  *Field note:* If EXPLAIN shows the filter already inside the scan node, don't create a CTE to push it — the engine already did it.
- **SAME_COLUMN_OR**: OR on the SAME column (e.g., t_hour BETWEEN 8 AND 11 OR t_hour BETWEEN 16 AND 17) is handled in a single scan with range checks.
  *Field note:* Splitting same-column ORs into UNION ALL doubled the fact scan on Q90 (0.59x) and 9-branch expansion on Q13 hit 0.23x. The engine handles these natively — leave them alone.
- **HASH_JOIN_SELECTION**: Selects hash joins automatically. Join ordering is generally sound for 2-4 table joins.
  *Field note:* Restructuring simple join orders rarely helps. Focus on what happens BEFORE the join (input reduction), not the join itself. See CROSS_CTE_PREDICATE_BLINDNESS for pre-filtering strategies that reduce join input.
- **CTE_INLINING**: CTEs referenced once are typically inlined (treated as subquery). Multi-referenced CTEs may be materialized.
  *Field note:* Single-reference CTEs are free — use for clarity. Multi-referenced CTEs: the engine decides whether to materialize. This makes CTE-based strategies (pre-filtering, isolation) low-cost on DuckDB compared to PostgreSQL where multi-referenced CTEs are materialized and create optimization fences.
- **COLUMNAR_PROJECTION**: Only reads columns actually referenced. Unused columns have zero I/O cost.
  *Field note:* SELECT * isn't as bad as in row-stores, but explicit columns still matter for intermediate CTEs — fewer columns = less memory for materialization. When creating pre-filter CTEs, only SELECT the columns downstream nodes actually need.
- **PARALLEL_AGGREGATION**: Scans and aggregations parallelized across threads. PERFECT_HASH_GROUP_BY is highly efficient.
  *Field note:* Simple aggregation queries are already fast. Restructuring them rarely helps unless you're reducing input rows. See REDUNDANT_SCAN_ELIMINATION for consolidating repeated scans, and CROSS_CTE_PREDICATE_BLINDNESS for reducing rows entering the aggregation.
- **EXISTS_SEMI_JOIN**: EXISTS/NOT EXISTS uses semi-join with early termination — stops after first match per outer row.
  *Field note:* Converting EXISTS to a materialized CTE with SELECT DISTINCT forces a full scan. Semi-join is usually faster. We saw 0.14x on Q16 and 0.54x on Q95 from this mistake. However, if the optimizer fails to decorrelate the EXISTS (check EXPLAIN for nested-loop re-execution), materializing may help.

### Optimizer Gaps (hunt for these)

**CROSS_CTE_PREDICATE_BLINDNESS** [HIGH]
  What: Cannot push predicates from the outer query backward into CTE definitions.
  Why: CTEs are planned as independent subplans. The optimizer does not trace data lineage through CTE boundaries.
  Opportunity: Move selective predicates INTO the CTE definition. Pre-filter dimensions/facts before they get materialized.
  What worked:
    + Q6/Q11: 4.00x — date filter moved into CTE
    + Q63: 3.77x — pre-joined filtered dates with fact table before other dims
    + Q93: 2.97x — dimension filter applied before LEFT JOIN chain
    + Q26: 1.93x — all dimensions pre-filtered into separate CTEs
  What didn't work:
    - Q25: 0.50x — query was 31ms baseline, CTE overhead exceeded filter savings
    - Q31: 0.49x — over-decomposed an already-efficient query into too many sub-CTEs
  Field notes:
    * Check EXPLAIN: if the filter appears AFTER a large scan or join, there's opportunity to push it earlier via CTE.
    * On fast queries (<100ms), CTE materialization overhead can negate the savings. Assess whether it's worth it.
    * This is our most productive gap — ~35% of all wins exploit it. Most reliable on star-join queries with late dim filters.
    * Don't create unfiltered CTEs — a CTE without a WHERE clause just adds overhead.
    * NEVER CROSS JOIN 3+ pre-filtered dimension CTEs together — Cartesian explosion creates massive intermediate results. Q80 hit 0.0076x (132x slower) when 3 dims (30x200x20=120K rows) were cross-joined. Instead, join each filtered dimension directly to the fact table.
    * Limit cascading fact-table CTE chains to 2 levels. A 3rd cascading CTE causes excessive materialization — Q4 hit 0.78x from triple-chained fact CTEs. Each level should be highly selective (<5% rows) to justify the overhead.
    * Orphaned CTEs that are defined but not referenced in the final query still get materialized. Always remove dead CTEs when restructuring.

**REDUNDANT_SCAN_ELIMINATION** [HIGH]
  What: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
  Why: Common Subexpression Elimination doesn't cross scalar subquery boundaries. Each subquery is planned independently.
  Opportunity: Consolidate N subqueries on the same table into 1 scan with CASE WHEN / FILTER() inside aggregates.
  What worked:
    + Q88: 6.28x — 8 time-bucket subqueries consolidated into 1 scan with 8 CASE branches
    + Q9: 4.47x — 15 separate store_sales scans consolidated into 1 scan with 5 CASE buckets
  Field notes:
    * Count scans per base table in the EXPLAIN. If a fact table appears 3+ times, this gap is active.
    * DuckDB supports native FILTER clause: COUNT(*) FILTER (WHERE cond) — use it instead of CASE WHEN for cleaner SQL.
    * STDDEV_SAMP, VARIANCE, PERCENTILE_CONT are grouping-sensitive. CASE branches compute per-group differently than separate per-branch queries. Verify equivalence when these appear.
    * We tested up to 8 CASE branches successfully. Beyond that is untested territory, not proven harmful — use judgment.
    * This produced our single biggest win (Q88 6.28x). Always check for multi-scan patterns.

**CORRELATED_SUBQUERY_PARALYSIS** [HIGH]
  What: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
  Why: Decorrelation requires recognizing that the correlated predicate = GROUP BY + JOIN equivalence. The optimizer doesn't do this for complex correlation patterns.
  Opportunity: Convert correlated WHERE to CTE with GROUP BY on the correlation column, then JOIN back.
  What worked:
    + Q1: 2.92x — correlated AVG with store_sk correlation converted to GROUP BY store_sk + JOIN
  Field notes:
    * Look for WHERE col > (SELECT AGG FROM ... WHERE outer.key = inner.key) patterns.
    * EXPLAIN will show nested-loop with repeated subquery execution if the optimizer failed to decorrelate.
    * CRITICAL: when decorrelating, you MUST preserve all WHERE filters from the original subquery in the new CTE. Dropping a filter changes which rows participate in the aggregate — this produces wrong results, not just a regression.
    * Only applies to correlated scalar subqueries with aggregates. EXISTS correlation is handled by semi-join (see strengths).

**CROSS_COLUMN_OR_DECOMPOSITION** [MEDIUM]
  What: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
  Why: The optimizer evaluates OR as a single filter. It can't recognize that each branch targets a different column with different selectivity.
  Opportunity: Split cross-column ORs into UNION ALL branches, each with a targeted single-column filter.
  What worked:
    + Q88: 6.28x — 8 time-bucket subqueries with distinct hour ranges (distinct access paths)
    + Q15: 3.17x — (zip OR state OR price) split to 3 targeted branches
    + Q10: 1.49x, Q45: 1.35x, Q41: 1.89x
  What didn't work:
    - Q13: 0.23x — nested OR expansion (3×3=9 branches = 9 fact scans). Cartesian OR explosion.
    - Q48: 0.41x — same nested OR explosion pattern
    - Q90: 0.59x — same-column OR (the engine already handles this, see strengths)
  Field notes:
    * The key distinction is CROSS-COLUMN (good candidate) vs SAME-COLUMN (engine handles it).
    * Count the resulting branches before committing. If nested ORs expand to 6+ branches, it's almost certainly harmful.
    * Self-joins (same table aliased twice) are risky to split — each branch re-does the self-join independently.
    * Each UNION branch rescans the fact table. The math: if selectivity per branch is S, benefit = N×(1-S) saved rows vs cost = N extra scans. Only works when S is very small (<20% per branch).
    * This is high-variance: our biggest win (6.28x) and worst regressions (0.23x) both come from or_to_union. Context matters enormously.

**LEFT_JOIN_FILTER_ORDER_RIGIDITY** [MEDIUM]
  What: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins.
  Why: LEFT JOIN must preserve all rows from the left table. The optimizer can't move a dimension filter before the LEFT JOIN.
  Opportunity: Pre-filter the selective dimension into a CTE, then use the filtered result as the JOIN partner.
  What worked:
    + Q93: 2.97x — filtered reason dimension FIRST, then LEFT JOIN to returns then fact
    + Q80: 1.40x — dimension isolation before fact join
  Field notes:
    * Only applies to LEFT JOINs. INNER JOINs are freely reordered by the optimizer.
    * The dimension filter needs to be highly selective (>50% row reduction) to justify the CTE overhead.
    * Check EXPLAIN: if a dimension filter appears AFTER a large join, you can restructure to apply it before.

**UNION_CTE_SELF_JOIN_DECOMPOSITION** [LOW]
  What: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.
  Why: The optimizer can't recognize that each probe only needs a partition of the UNION result.
  Opportunity: Split the UNION ALL into N separate CTEs (one per discriminator value).
  What worked:
    + Q74: 1.36x — UNION of store/web sales split into separate year-partitioned CTEs
  Field notes:
    * Only applies when the UNION ALL CTE is self-joined 2+ times with different discriminator filters.
    * When splitting, you MUST remove the original UNION CTE and redirect all references to the new split CTEs. Leaving dead CTEs causes materialization overhead (Q31 hit 0.49x, Q74 hit 0.68x from this mistake).

## Correctness Constraints (4 — NEVER violate)

**[CRITICAL] COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.

**[CRITICAL] CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
  - Failure: Q21 — prefetched_inventory CTE omits i_item_id but main query references it in SELECT and GROUP BY
  - Failure: Q76 — filtered_store_dates CTE omits d_year and d_qoy but aggregation CTE uses them in GROUP BY

**[CRITICAL] LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.

**[CRITICAL] SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## Your Task

First, use a `<reasoning>` block for your internal analysis. This will be stripped before parsing. Work through these steps IN ORDER:

1. **CLASSIFY**: What structural archetype is this query?
   (channel-comparison self-join / correlated-aggregate filter / star-join with late dim filter / repeated fact scan / multi-channel UNION ALL / EXISTS-set operations / other)

2. **EXPLAIN PLAN ANALYSIS**: From the EXPLAIN ANALYZE output, identify:
   - Compute wall-clock ms per EXPLAIN node. Sum repeated operations (e.g., 2x store_sales joins = total cost). The EXPLAIN is ground truth, not the DAG cost percentages.
   - Which nodes consume >10% of runtime and WHY
   - Where row counts drop sharply (existing selectivity)
   - Where row counts DON'T drop (missed optimization opportunity)
   - Whether the optimizer already splits CTEs, pushes predicates, or performs transforms you might otherwise assign
   - Count scans per base table. If a fact table is scanned N times, a restructuring that reduces it to 1 scan saves (N-1)/N of that table's I/O cost. Prioritize transforms that reduce scan count on the largest tables.
   - Whether the CTE is materialized once and probed multiple times, or re-executed per reference

3. **GAP MATCHING**: Compare the EXPLAIN analysis to the Engine Profile gaps above. For each gap:
   - Does this query exhibit the gap? (e.g., is a predicate NOT pushed into a CTE? Is the same fact table scanned multiple times?)
   - Check the 'opportunity' — does this query's structure match?
   - Check 'what_didnt_work' and 'field_notes' — any disqualifiers for this query?
   - Also verify: is the optimizer ALREADY handling this well? (Check the Optimizer Strengths above — if the engine already does it, your transform adds overhead, not value.)

4. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, verify: does my proposed restructuring change which rows participate in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive (modulo duplicates). If the query uses FILTER clauses or conditional aggregation, verify equivalence explicitly.

5. **TRANSFORM SELECTION**: From the matched engine gaps, select the single best transform (or compound strategy) that maximizes expected value (rows affected × historical speedup from evidence) for THIS query.
   REJECT tag-matched examples whose primary technique requires a structural feature this query lacks. Tag matching is approximate — always verify structural applicability.

6. **DAG DESIGN**: Define the target DAG topology for your chosen strategy. Verify that every node contract has exhaustive output columns by checking downstream references.
   CTE materialization matters: a CTE referenced by 2+ consumers will likely be materialized. A CTE referenced once may be inlined.

7. **WRITE REWRITE**: Implement your strategy as a JSON rewrite_set. Each changed or added CTE is a node. Produce per-node SQL matching your DAG design from step 6. Declare output columns for every node in `node_contracts`. The rewrite must be semantically equivalent to the original.

Then produce the structured briefing in EXACTLY this format:

```
=== SHARED BRIEFING ===

SEMANTIC_CONTRACT: (80-150 tokens, cover ONLY:)
(a) One sentence of business intent (start from pre-computed intent if available).
(b) JOIN type semantics that constrain rewrites (INNER = intersection = all sides must match).
(c) Any aggregation function traps specific to THIS query.
(d) Any filter dependencies that a rewrite could break.
Do NOT repeat information already in ACTIVE_CONSTRAINTS or REGRESSION_WARNINGS.

BOTTLENECK_DIAGNOSIS:
[Which operation dominates cost and WHY (not just '50% cost').
Scan-bound vs join-bound vs aggregation-bound.
Cardinality flow (how many rows at each stage).
What the optimizer already handles well (don't re-optimize).
Whether DAG cost percentages are misleading.]

ACTIVE_CONSTRAINTS:
- [CORRECTNESS_CONSTRAINT_ID]: [Why it applies to this query, 1 line]
- [ENGINE_GAP_ID]: [Evidence from EXPLAIN that this gap is active]
(List all 4 correctness constraints + the 1-3 engine gaps that
are active for THIS query based on your EXPLAIN analysis.)

REGRESSION_WARNINGS:
1. [Pattern name] ([observed regression]):
   CAUSE: [What happened mechanistically]
   RULE: [Actionable avoidance rule for THIS query]
(If no regression warnings are relevant, write 'None applicable.')

=== REWRITE ===

First output a **Modified Logic Tree** showing what changed:
- `[+]` new  `[-]` removed  `[~]` modified  `[=]` unchanged  `[!]` structural

Then output a **Component Payload JSON**:

```json
{
  "spec_version": "1.0",
  "dialect": "<dialect>",
  "rewrite_rules": [{"id": "R1", "type": "<transform>", "description": "<what>", "applied_to": ["<comp_id>"]}],
  "statements": [{
    "target_table": null,
    "change": "modified",
    "components": {
      "<cte_name>": {"type": "cte", "change": "modified", "sql": "<CTE body>", "interfaces": {"outputs": ["col1"], "consumes": []}},
      "main_query": {"type": "main_query", "change": "modified", "sql": "<final SELECT>", "interfaces": {"outputs": ["col1"], "consumes": ["<cte_name>"]}}
    },
    "reconstruction_order": ["<cte_name>", "main_query"],
    "assembly_template": "WITH <cte_name> AS ({<cte_name>}) {main_query}"
  }],
  "macros": {},
  "frozen_blocks": [],
  "validation_checks": []
}
```

Rules:
- Tree first, always — generate Logic Tree before writing SQL
- One component at a time — treat other components as opaque interfaces
- No ellipsis — every `sql` value must be complete, executable SQL
- Only include changed/added components; set unchanged to `"change": "unchanged"` with `"sql": ""`
- `main_query` output columns must match original exactly

After the JSON, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
Expected speedup: <estimate>
```
```

## Section Validation Checklist (MUST pass before final output)

Use this checklist to verify content quality, not just section presence:

### SHARED BRIEFING
- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.
- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.
- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.
- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.

### REWRITE
- Modified Logic Tree is present with change markers ([+]/[-]/[~]/[=]).
- Component Payload JSON has `spec_version`, `statements`, and `rewrite_rules`.
- Each component has complete, executable `sql` (no ellipsis).
- `reconstruction_order` lists components in dependency order.
- `interfaces.outputs` matches actual SELECT columns in each component.
- `main_query` output columns match original query exactly (same names, same order).
- All literals preserved exactly (numbers, strings, date values).
- Semantically equivalent to the original query.

## Transform Catalog

Select the best transform (or compound strategy of 2-3 transforms) that maximizes expected speedup for THIS query.

### Predicate Movement
- **global_predicate_pushdown**: Trace selective predicates from late in the CTE chain back to the earliest scan via join equivalences. Biggest win when a dimension filter is applied after a large intermediate materialization.
  Maps to examples: pushdown, early_filter, date_cte_isolate
- **transitive_predicate_propagation**: Infer predicates through join equivalence chains (A.key = B.key AND B.key = 5 -> A.key = 5). Especially across CTE boundaries where optimizers stop propagating.
  Maps to examples: early_filter, dimension_cte_isolate
- **null_rejecting_join_simplification**: When downstream WHERE rejects NULLs from the outer side of a LEFT JOIN, convert to INNER. Enables reordering and predicate pushdown. CHECK: does the query actually have LEFT/OUTER joins before assigning this.
  Maps to examples: (no direct gold example — novel transform)

### Join Restructuring
- **self_join_elimination**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, split into N pre-partitioned CTEs. Eliminates discriminator filtering and repeated hash probes on rows that don't match.
  Maps to examples: union_cte_split, shared_dimension_multi_channel
- **decorrelation**: Convert correlated EXISTS/IN/scalar subqueries to CTE + JOIN. CHECK: does the query actually have correlated subqueries before assigning this.
  Maps to examples: decorrelate, composite_decorrelate_union
- **aggregate_pushdown**: When GROUP BY follows a multi-table join but aggregation only uses columns from one side, push the GROUP BY below the join. CHECK: verify the join doesn't change row multiplicity for the aggregate (one-to-many breaks AVG/STDDEV).
  Maps to examples: (no direct gold example — novel transform)
- **late_attribute_binding**: When a dimension table is joined only to resolve display columns (names, descriptions) that aren't used in filters, aggregations, or join conditions, defer that join until after all filtering and aggregation is complete. Join on the surrogate key once against the final reduced result set. This eliminates N-1 dimension scans when the CTE references the dimension N times. CHECK: verify the deferred columns aren't used in WHERE, GROUP BY, or JOIN ON — only in the final SELECT.
  Maps to examples: dimension_cte_isolate (partial pattern), early_filter

### Scan Optimization
- **star_join_prefetch**: Pre-filter ALL dimension tables into CTEs, then probe fact table with the combined key intersection.
  Maps to examples: dimension_cte_isolate, multi_dimension_prefetch, prefetch_fact_join, date_cte_isolate
- **single_pass_aggregation**: Merge N subqueries on the same fact table into 1 scan with CASE/FILTER inside aggregates. CHECK: STDDEV_SAMP/VARIANCE are grouping-sensitive — FILTER over a combined group != separate per-group computation.
  Maps to examples: single_pass_aggregation, channel_bitmap_aggregation
- **scan_consolidation_pivot**: When a CTE is self-joined N times with each reference filtering to a different discriminator (e.g., year, channel), consolidate into fewer scans that GROUP BY the discriminator, then pivot rows to columns using MAX(CASE WHEN discriminator = X THEN agg_value END). This halves the fact scans and dimension joins. SAFE when GROUP BY includes the discriminator — each group is naturally partitioned, so aggregates like STDDEV_SAMP are computed correctly per-partition. The pivot MAX is just a row selector (one row per group), not a real aggregation.
  Maps to examples: single_pass_aggregation, union_cte_split

### Structural Transforms
- **union_consolidation**: Share dimension lookups across UNION ALL branches that scan different fact tables with the same dim joins.
  Maps to examples: shared_dimension_multi_channel
- **window_optimization**: Push filters before window functions when they don't affect the frame. Convert ROW_NUMBER + filter to LATERAL + LIMIT. Merge same-PARTITION windows into one sort pass.
  Maps to examples: deferred_window_aggregation
- **exists_restructuring**: Convert INTERSECT to EXISTS for semi-join short-circuit, or restructure complex EXISTS with shared CTEs. CHECK: does the query actually have INTERSECT or complex EXISTS.
  Maps to examples: intersect_to_exists, multi_intersect_exists_cte

## Strategy Leaderboard (observed success rates)

Archetype: **general** (40 queries in pool, 347 total attempts)

| Transform | Attempts | Win Rate | Avg Speedup | Avoid? |
|-----------|----------|----------|-------------|--------|
| decorrelate | 40 | 38% | 0.95x |  |
| intersect_to_exists | 30 | 30% | 0.90x |  |
| date_cte_isolate | 127 | 28% | 1.02x |  |
| multi_dimension_prefetch | 51 | 28% | 0.99x |  |
| or_to_union | 89 | 26% | 1.12x |  |
| early_filter | 72 | 24% | 1.05x |  |
| prefetch_fact_join | 55 | 22% | 1.06x |  |
| pushdown | 53 | 21% | 1.02x |  |
| single_pass_aggregation | 34 | 21% | 1.11x |  |
| dimension_cte_isolate | 65 | 20% | 1.03x |  |
| multi_date_range_cte | 21 | 19% | 1.09x |  |
| materialize_cte | 43 | 19% | 1.02x |  |

## Strategy Selection Rules

1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite (correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the query actually has the prerequisite before assigning a transform. DO NOT assign decorrelation if there are no correlated subqueries.
2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer already performs a transform (e.g., already splits a UNION CTE, already pushes a predicate), that transform will have marginal benefit. Note this in your reasoning and prefer transforms the optimizer is NOT already doing.
3. **MAXIMIZE EXPECTED VALUE**: Select the single strategy with the highest expected speedup, considering both the magnitude of the bottleneck it addresses and the historical success rate.
4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x query complexity), not an inherent property of the transform. Decorrelation is low-risk on a simple EXISTS and high-risk on nested correlation inside a CTE. Assess per-assignment.
5. **COMPOSITION IS ALLOWED AND ENCOURAGED**: A strategy can combine 2-3 transforms from different categories (e.g., star_join_prefetch + scan_consolidation_pivot, or date_cte_isolate + early_filter + decorrelate). The TARGET_DAG should reflect the combined structure. Compound strategies are often the source of the biggest wins.

Select 1-3 examples that genuinely match the strategy. Do NOT pad with irrelevant examples — an irrelevant example is worse than no example. Use example IDs from the catalog above.

For TARGET_DAG: Define the CTE structure you want produced. For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns cause semantic breaks.
