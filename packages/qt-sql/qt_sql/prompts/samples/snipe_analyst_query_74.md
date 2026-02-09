You are the diagnostic analyst for query query_74. You've seen 4 parallel attempts at >=2.0x speedup on this query. Your job: diagnose what worked, what didn't, and WHY — then design a strategy the sniper couldn't have known without these empirical results.

## Target: >=2.0x speedup
Anything below 2.0x is a miss. The sniper you deploy must be given a strategy with genuine headroom to reach this bar.

## Previous Optimization Attempts
Target: **>=2.0x** | 4 workers tried | none reached target

### W1: decorrelate + early_filter → 1.21x ★ BEST [PASS, below target (1.21x)]
- **Examples**: decorrelate, early_filter
- **Transforms**: decorrelate, early_filter
- **Approach**: Decorrelated subquery and pushed filters early
- **Optimized SQL:**
```sql
-- W1: decorrelate attempt (mild win)
WITH ...
```

### W2: date_cte_isolate + late_attribute_binding → 1.18x [PASS, below target (1.18x)]
- **Examples**: date_cte_isolate, shared_dimension_multi_channel
- **Transforms**: date_cte_isolate, late_attribute_binding
- **Approach**: Isolated date filter into CTE, deferred customer join
- **Optimized SQL:**
```sql
-- W2: date CTE + late binding (mild win)
WITH ...
```

### W3: prefetch_fact_join + materialize_cte → 0.95x [REGRESSION (0.95x)]
- **Examples**: prefetch_fact_join, materialize_cte
- **Transforms**: prefetch_fact_join
- **Approach**: Prefetched fact join — slight regression due to extra materialization
- **Optimized SQL:**
```sql
-- W3: prefetch attempt (regression)
WITH ...
```

### W4: single_pass_aggregation → 0.0x [ERROR]
- **Examples**: single_pass_aggregation
- **Error**: column "ss_customer_sk" must appear in GROUP BY clause
- **Optimized SQL:**
```sql
-- W4: single pass attempt (error)
SELECT ...
```

## Original SQL (query_74, duckdb v1.4.3)

```sql
 1 | with year_total as (
 2 |  select c_customer_id customer_id
 3 |        ,c_first_name customer_first_name
 4 |        ,c_last_name customer_last_name
 5 |        ,d_year as year
 6 |        ,stddev_samp(ss_net_paid) year_total
 7 |        ,'s' sale_type
 8 |  from customer
 9 |      ,store_sales
10 |      ,date_dim
11 |  where c_customer_sk = ss_customer_sk
12 |    and ss_sold_date_sk = d_date_sk
13 |    and d_year in (1999,1999+1)
14 |  group by c_customer_id
15 |          ,c_first_name
16 |          ,c_last_name
17 |          ,d_year
18 |  union all
19 |  select c_customer_id customer_id
20 |        ,c_first_name customer_first_name
21 |        ,c_last_name customer_last_name
22 |        ,d_year as year
23 |        ,stddev_samp(ws_net_paid) year_total
24 |        ,'w' sale_type
25 |  from customer
26 |      ,web_sales
27 |      ,date_dim
28 |  where c_customer_sk = ws_bill_customer_sk
29 |    and ws_sold_date_sk = d_date_sk
30 |    and d_year in (1999,1999+1)
31 |  group by c_customer_id
32 |          ,c_first_name
33 |          ,c_last_name
34 |          ,d_year
35 |          )
36 |   select
37 |         t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
38 |  from year_total t_s_firstyear
39 |      ,year_total t_s_secyear
40 |      ,year_total t_w_firstyear
41 |      ,year_total t_w_secyear
42 |  where t_s_secyear.customer_id = t_s_firstyear.customer_id
43 |          and t_s_firstyear.customer_id = t_w_secyear.customer_id
44 |          and t_s_firstyear.customer_id = t_w_firstyear.customer_id
45 |          and t_s_firstyear.sale_type = 's'
46 |          and t_w_firstyear.sale_type = 'w'
47 |          and t_s_secyear.sale_type = 's'
48 |          and t_w_secyear.sale_type = 'w'
49 |          and t_s_firstyear.year = 1999
50 |          and t_s_secyear.year = 1999+1
51 |          and t_w_firstyear.year = 1999
52 |          and t_w_secyear.year = 1999+1
53 |          and t_s_firstyear.year_total > 0
54 |          and t_w_firstyear.year_total > 0
55 |          and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
56 |            > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
57 |  order by 2,1,3
58 |  LIMIT 100;
```

## EXPLAIN ANALYZE Plan

```
Total execution time: 3537ms

TOP_N [100 rows, 1.4ms]
  Top: 100
  Order By: customer_first_name ASC, customer_id ASC, customer_last_name ASC
  FILTER [4,387 rows, 1.7ms]
    Expression: (CASE  WHEN ((year_total > 0.0)) THEN ((year_total / year_total)) ELSE NULL END > CASE  WHEN ((ye...
    HASH_JOIN INNER on customer_id = customer_id [8,840 rows, 20.2ms]
      HASH_GROUP_BY [309K rows, 378.4ms, 11%]
        Aggregates: stddev_samp(#4)
        HASH_JOIN INNER on c_customer_sk = ss_customer_sk [5.4M rows, 598.3ms, 17%]
          SEQ_SCAN  customer [500K of 2.5M rows, 30.0ms]  Filters: optional: Dynamic Filter (c_first_name)
          HASH_JOIN INNER on ss_sold_date_sk = d_date_sk [5.5M rows, 38.9ms, 1%]
            SEQ_SCAN  store_sales [5.5M of 345.6M rows, 206.9ms, 6%]
            FILTER [366 rows]
              Expression: (d_date_sk BETWEEN 2450816 AND 2452642)
              SEQ_SCAN  date_dim [366 of 73K rows]  Filters: d_year=2000 AND d_year>=1999 AND d_year<=2000
      HASH_JOIN INNER on customer_id = customer_id [14K rows, 15.3ms]
        HASH_GROUP_BY [108K rows, 120.5ms, 3%]
          Aggregates: stddev_samp(#4)
          HASH_JOIN INNER on c_customer_sk = ws_bill_customer_sk [1.4M rows, 141.3ms, 4%]
            SEQ_SCAN  customer [500K of 2.5M rows, 29.5ms]  Filters: c_customer_sk>=2 AND c_customer_sk<=499998
            HASH_JOIN INNER on ws_sold_date_sk = d_date_sk [1.4M rows, 9.5ms]
              SEQ_SCAN  web_sales [1.4M of 86.4M rows, 40.9ms, 1%]
              FILTER [366 rows]
                Expression: (d_date_sk BETWEEN 2450816 AND 2452642)
                SEQ_SCAN  date_dim [366 of 73K rows]  Filters: d_year=2000 AND d_year>=1999 AND d_year<=2000
        HASH_JOIN INNER on customer_id = customer_id [66K rows, 22.9ms]
          FILTER [308K rows, 1.6ms]
            Expression: (year_total > 0.0)
            HASH_GROUP_BY [308K rows, 359.4ms, 10%]
              Aggregates: stddev_samp(#4)
              HASH_JOIN INNER on c_customer_sk = ss_customer_sk [5.4M rows, 601.4ms, 17%]
                SEQ_SCAN  customer [500K of 2.5M rows, 30.3ms]
                HASH_JOIN INNER on ss_sold_date_sk = d_date_sk [5.5M rows, 39.8ms, 1%]
                  SEQ_SCAN  store_sales [5.5M of 345.6M rows, 218.6ms, 6%]
                  FILTER [365 rows]
                    Expression: (d_date_sk BETWEEN 2450816 AND 2452642)
                    SEQ_SCAN  date_dim [365 of 73K rows]  Filters: d_year=1999 AND d_year>=1999 AND d_year<=2000
          FILTER [107K rows, 0.6ms]
            Expression: (year_total > 0.0)
            HASH_GROUP_BY [107K rows, 101.2ms, 3%]
              Aggregates: stddev_samp(#4)
              HASH_JOIN INNER on c_customer_sk = ws_bill_customer_sk [1.4M rows, 149.4ms, 4%]
                SEQ_SCAN  customer [500K of 2.5M rows, 18.5ms]  Filters: c_customer_sk>=2 AND c_customer_sk<=499998
                HASH_JOIN INNER on ws_sold_date_sk = d_date_sk [1.4M rows, 10.1ms]
                  SEQ_SCAN  web_sales [1.4M of 86.4M rows, 44.1ms, 1%]
                  FILTER [365 rows]
                    Expression: (d_date_sk BETWEEN 2450816 AND 2452642)
                    SEQ_SCAN  date_dim [365 of 73K rows]  Filters: d_year=1999 AND d_year>=1999 AND d_year<=2000
```

## Query Structure (DAG)

### 1. year_total
**Role**: CTE (Definition Order: 0)
**Intent**: Compute per-customer yearly standard deviation of net paid for store and web channels in 1999 and 2000.
**Stats**: 45% Cost | ~5.5M rows
**Flags**: GROUP_BY, UNION_ALL
**Outputs**: [customer_id, customer_first_name, customer_last_name, year, year_total, sale_type]
**Dependencies**: customer, store_sales (join), date_dim (join), web_sales (join)
**Joins**: c_customer_sk = ss_customer_sk | ss_sold_date_sk = d_date_sk
**Filters**: d_year IN (1999, 1999 + 1)
**Operators**: SEQ_SCAN[customer], SEQ_SCAN[store_sales], SEQ_SCAN[date_dim], SEQ_SCAN[customer], SEQ_SCAN[web_sales]
**Key Logic (SQL)**:
```sql
SELECT
  c_customer_id AS customer_id,
  c_first_name AS customer_first_name,
  c_last_name AS customer_last_name,
  d_year AS year,
  STDDEV_SAMP(ss_net_paid) AS year_total,
  's' AS sale_type
FROM customer, store_sales, date_dim
WHERE
  c_customer_sk = ss_customer_sk
  AND ss_sold_date_sk = d_date_sk
  AND d_year IN (1999, 1999 + 1)
GROUP BY
  c_customer_id,
  c_first_name,
  c_last_name,
  d_year
UNION ALL
SELECT
  c_customer_id AS customer_id,
...
```

### 2. main_query
**Role**: Root / Output (Definition Order: 1)
**Intent**: Self-join customer/channel/year variability rows, require positive 1999 variability baselines, compare web versus store year-over-year ratios, and return matching customer names.
**Stats**: 42% Cost | ~1k rows processed → 100 rows output
**Flags**: GROUP_BY, ORDER_BY, LIMIT(100)
**Outputs**: [customer_id, customer_first_name, customer_last_name] — ordered by 2 ASC, 1 ASC, 3 ASC
**Dependencies**: year_total AS t_s_firstyear (join), year_total AS t_s_secyear (join), year_total AS t_w_firstyear (join), year_total AS t_w_secyear (join)
**Joins**: t_s_secyear.customer_id = t_s_firstyear.customer_id | t_s_firstyear.customer_id = t_w_secyear.customer_id | t_s_firstyear.customer_id = t_w_firstyear.customer_id
**Filters**: t_s_firstyear.sale_type = 's' | t_w_firstyear.sale_type = 'w' | t_s_secyear.sale_type = 's' | t_w_secyear.sale_type = 'w' | t_s_firstyear.year = 1999 | t_s_secyear.year = 1999 + 1 | t_w_firstyear.year = 1999 | t_w_secyear.year = 1999 + 1 | t_s_firstyear.year_total > 0 | t_w_firstyear.year_total > 0 | CASE WHEN t_w_firstyear.year_total > 0 THEN t_w_secyear.year_total / t_w_firstyear.year_total ELSE NULL END > CASE WHEN t_s_firstyear.year_total > 0 THEN t_s_secyear.year_total / t_s_firstyear.year_total ELSE NULL END
**Operators**: HASH_JOIN
**Key Logic (SQL)**:
```sql
SELECT
  t_s_secyear.customer_id,
  t_s_secyear.customer_first_name,
  t_s_secyear.customer_last_name
FROM year_total AS t_s_firstyear, year_total AS t_s_secyear, year_total AS t_w_firstyear, year_total AS t_w_secyear
WHERE
  t_s_secyear.customer_id = t_s_firstyear.customer_id
  AND t_s_firstyear.customer_id = t_w_secyear.customer_id
  AND t_s_firstyear.customer_id = t_w_firstyear.customer_id
  AND t_s_firstyear.sale_type = 's'
  AND t_w_firstyear.sale_type = 'w'
  AND t_s_secyear.sale_type = 's'
  AND t_w_secyear.sale_type = 'w'
  AND t_s_firstyear.year = 1999
  AND t_s_secyear.year = 1999 + 1
  AND t_w_firstyear.year = 1999
  AND t_w_secyear.year = 1999 + 1
  AND t_s_firstyear.year_total > 0
  AND t_w_firstyear.year_total > 0
  AND CASE
...
```

### Edges
- year_total → main_query
- year_total → main_query
- year_total → main_query
- year_total → main_query


## Aggregation Semantics Check

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Changing group membership changes the result.
- **AVG and STDDEV are NOT duplicate-safe**: join-introduced row duplication changes the aggregate.
- When splitting with GROUP BY + aggregate, each branch must preserve exact GROUP BY columns and filter to the same row set.

## Engine Profile

*This is field intelligence gathered from 88 TPC-DS queries at SF1-SF10. Use it to guide your analysis but apply your own judgment — every query is different. Add to this knowledge if you observe something new.*

### Optimizer Strengths (DO NOT fight these)
- **INTRA_SCAN_PREDICATE_PUSHDOWN**: Pushes WHERE filters directly into SEQ_SCAN. Single-table predicates are applied at scan time, zero overhead.
- **SAME_COLUMN_OR**: OR on the SAME column (e.g., t_hour BETWEEN 8 AND 11 OR t_hour BETWEEN 16 AND 17) is handled in a single scan with range checks.
- **HASH_JOIN_SELECTION**: Selects hash joins automatically. Join ordering is generally sound for 2-4 table joins.
- **CTE_INLINING**: CTEs referenced once are typically inlined (treated as subquery). Multi-referenced CTEs may be materialized.
- **COLUMNAR_PROJECTION**: Only reads columns actually referenced. Unused columns have zero I/O cost.
- **PARALLEL_AGGREGATION**: Scans and aggregations parallelized across threads. PERFECT_HASH_GROUP_BY is highly efficient.
- **EXISTS_SEMI_JOIN**: EXISTS/NOT EXISTS uses semi-join with early termination — stops after first match per outer row.

### Optimizer Gaps (opportunities)
- **CROSS_CTE_PREDICATE_BLINDNESS**: Cannot push predicates from the outer query backward into CTE definitions.
  Opportunity: Move selective predicates INTO the CTE definition. Pre-filter dimensions/facts before they get materialized.
- **REDUNDANT_SCAN_ELIMINATION**: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
  Opportunity: Consolidate N subqueries on the same table into 1 scan with CASE WHEN / FILTER() inside aggregates.
- **CORRELATED_SUBQUERY_PARALYSIS**: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
  Opportunity: Convert correlated WHERE to CTE with GROUP BY on the correlation column, then JOIN back.
- **CROSS_COLUMN_OR_DECOMPOSITION**: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
  Opportunity: Split cross-column ORs into UNION ALL branches, each with a targeted single-column filter.
- **LEFT_JOIN_FILTER_ORDER_RIGIDITY**: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins.
  Opportunity: Pre-filter the selective dimension into a CTE, then use the filtered result as the JOIN partner.
- **UNION_CTE_SELF_JOIN_DECOMPOSITION**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.
  Opportunity: Split the UNION ALL into N separate CTEs (one per discriminator value).

## Tag-Matched Examples (16)

### union_cte_split (1.36x)
**Description:** Split a generic UNION ALL CTE into specialized CTEs when the main query filters by year or discriminator - eliminates redundant scans
**Principle:** CTE Specialization: when a generic CTE is scanned multiple times with different filters (e.g., by year), split it into specialized CTEs that embed the filter in their definition. Each specialized CTE processes only its relevant subset, eliminating redundant scans.

### intersect_to_exists (1.83x)
**Description:** Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning
**Principle:** Semi-Join Short-Circuit: replace INTERSECT with EXISTS to avoid full materialization and sorting. INTERSECT must compute complete result sets before intersecting; EXISTS stops at the first match per row, enabling semi-join optimizations.

### multi_intersect_exists_cte (2.39x)
**Description:** Convert cascading INTERSECT operations into correlated EXISTS subqueries with pre-materialized date and channel CTEs
**When NOT to apply:** Do not use when the INTERSECT operates on small result sets (< 1000 rows) where materialization cost is negligible. Also not applicable when the EXISTS correlation would be on non-indexed columns, as the correlated probe could be slower than the hash-based INTERSECT.

### deferred_window_aggregation (1.36x)
**Description:** When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joined with FULL OUTER JOIN followed by another WINDOW pass for NULL carry-forward: defer the WINDOW out of the CTEs, join daily totals, then compute cumulative sums once on the joined result. SUM() OVER() naturally skips NULLs, eliminating the need for a separate MAX() carry-forward window.
**Principle:** Deferred Aggregation: delay expensive operations (window functions) until after joins reduce the dataset. Computing window functions inside individual CTEs then joining is more expensive than joining first and computing windows once on the combined result.
**When NOT to apply:** Do not use when the CTE window function is referenced by other consumers besides the final join (the cumulative value is needed elsewhere). Do not use when the window function is not a monotonically accumulating SUM - e.g., AVG, COUNT, or non-monotonic window functions require separate computation. Only applies when the join is FULL OUTER and the carry-forward window is MAX/LAST_VALUE over a cumulative sum.

### shared_dimension_multi_channel (1.30x)
**Description:** Extract shared dimension filters (date, item, promotion) into CTEs when multiple channel CTEs (store/catalog/web) apply identical filters independently
**Principle:** Shared Dimension Extraction: when multiple channel CTEs (store/catalog/web) apply identical dimension filters, extract those shared filters into one CTE and reference it from each channel. Avoids redundant dimension scans.

### composite_decorrelate_union (2.42x)
**Description:** Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT customer CTEs with a shared date filter, and replace OR(EXISTS a, EXISTS b) with UNION of key sets
**Principle:** Composite Decorrelation: when multiple correlated EXISTS share common filters, extract shared dimensions into a single CTE and decorrelate the EXISTS checks into pre-materialized key sets joined via UNION.

### prefetch_fact_join (3.77x)
**Description:** Pre-filter dimension table into CTE, then pre-join with fact table in second CTE before joining other dimensions
**Principle:** Staged Join Pipeline: build a CTE chain that progressively reduces data — first CTE filters the dimension, second CTE pre-joins filtered dimension keys with the fact table, subsequent CTEs join remaining dimensions against the already-reduced fact set.
**When NOT to apply:** Do not use on queries with baseline runtime under 50ms — CTE materialization overhead dominates on fast queries. Do not use on window-function-dominated queries where filtering is not the bottleneck. Avoid on queries with 5+ table joins and complex inter-table predicates where forcing join order via CTEs prevents the optimizer from choosing a better plan. Caused 0.50x on Q25 (fast baseline query), 0.87x on Q51 (window-function bottleneck), and 0.77x on Q72 (complex multi-table join reordering).

### rollup_to_union_windowing (2.47x)
**Description:** Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each hierarchy level, combined with window functions for ranking
**When NOT to apply:** Do not use when ROLLUP generates all levels efficiently (small dimension tables, few groups) or when the query genuinely needs all possible grouping set combinations. Only beneficial when specific levels need different optimization paths.

### date_cte_isolate (4.00x)
**Description:** Extract date filtering into a separate CTE to enable predicate pushdown and reduce scans
**Principle:** Dimension Isolation: extract small dimension lookups into CTEs so they materialize once and subsequent joins probe a tiny hash table instead of rescanning.
**When NOT to apply:** Do not use when the optimizer already pushes date predicates effectively (e.g., simple equality filters on date columns in self-joins). Do not decompose an already-efficient existing CTE into sub-CTEs — this adds materialization overhead without reducing scans. Caused 0.49x regression on Q31 (DuckDB already optimized the date pushdown) and 0.71x on Q1 (decomposed a well-structured CTE into slower pieces).

### decorrelate (2.92x)
**Description:** Convert correlated subquery to separate CTE with GROUP BY, then JOIN
**Principle:** Decorrelation: convert correlated subqueries to standalone CTEs with GROUP BY, then JOIN. Correlated subqueries re-execute per outer row; a pre-computed CTE executes once.

### materialize_cte (1.37x)
**Description:** Extract repeated subquery patterns into a CTE to avoid recomputation
**Principle:** Shared Materialization: extract repeated subquery patterns into CTEs to avoid recomputation. When the same logical check appears multiple times, compute it once and reference the result.
**When NOT to apply:** NEVER convert EXISTS or NOT EXISTS subqueries into materialized CTEs when the EXISTS is used as a filter (not a data source). EXISTS uses semi-join short-circuiting — the database stops scanning as soon as one match is found. Materializing into a CTE forces a full scan of the subquery table, destroying this optimization. Caused 0.14x on Q16 (7x slowdown — EXISTS on catalog_sales materialized into full CTE scan) and 0.54x on Q95 (EXISTS on web_sales forced full materialization).

### multi_date_range_cte (2.35x)
**Description:** When query uses multiple date_dim aliases with different filters (d1, d2, d3), create separate CTEs for each date range and pre-join with fact tables
**Principle:** Early Selection per Alias: when a query joins the same dimension table multiple times with different filters (d1, d2, d3), create separate CTEs for each filter and pre-join with fact tables to reduce rows entering the main join.

### multi_dimension_prefetch (2.71x)
**Description:** Pre-filter multiple dimension tables (date + store) into separate CTEs before joining with fact table
**Principle:** Multi-Dimension Prefetch: when multiple dimension tables have selective filters, pre-filter ALL of them into CTEs before the fact table join. Combined selectivity compounds — each dimension CTE reduces the fact scan further.
**When NOT to apply:** Do not create dimension CTEs without a WHERE clause that actually reduces rows — an unfiltered dimension CTE is pure overhead (full scan + materialization for zero selectivity benefit). Avoid on queries with 5+ tables and complex inter-table predicates where forcing join order via CTEs prevents the optimizer from choosing a better plan. Caused 0.85x on Q67 (unfiltered dimension CTEs added overhead) and 0.77x on Q72 (forced suboptimal join ordering on complex multi-table query).

### or_to_union (3.17x)
**Description:** Split OR conditions on different columns into UNION ALL branches for better index usage
**Principle:** OR-to-UNION Decomposition: split OR conditions on different columns into separate UNION ALL branches, each with a focused predicate. The optimizer can use different access paths per branch instead of a single scan with a complex filter.
**When NOT to apply:** Do not split OR when all branches filter the SAME column on the same table (e.g., t_hour >= 8 OR t_hour <= 17). This duplicates the entire fact table scan for each branch with no selectivity benefit. Only apply when OR conditions span DIFFERENT tables or fundamentally different column families. Also never split into more than 3 UNION branches — each branch rescans the fact table. Caused 0.59x on Q90 (same-column time range split doubled fact scans) and historically 0.23x-0.41x on queries with 9+ UNION branches.

### early_filter (4.00x)
**Description:** Filter dimension tables FIRST, then join to fact tables to reduce expensive joins
**Principle:** Early Selection: filter small dimension tables first, then join to large fact tables. This reduces the fact table scan to only rows matching the filter, rather than scanning all rows and filtering after the join.

### dimension_cte_isolate (1.93x)
**Description:** Pre-filter ALL dimension tables into CTEs before joining with fact table, not just date_dim
**Principle:** Early Selection: pre-filter dimension tables into CTEs returning only surrogate keys before joining with fact tables. Each dimension CTE is tiny, creating small hash tables that speed up the fact table probe.

## Regression Warnings

### regression_q74_pushdown: pushdown on q74 (0.68x)
**Anti-pattern:** When splitting a UNION CTE by year, you MUST remove or replace the original UNION CTE. Keeping both the split and original versions causes redundant materialization and extreme cardinality misestimates.
**Mechanism:** Created year-specific CTEs (store_sales_1999, store_sales_2000, etc.) but KEPT the original year_total union CTE alongside them. The optimizer materializes both the split versions and the original union, resulting in redundant computation. Projection cardinality estimates show 10^16x errors from the confused CTE graph.

### regression_q31_pushdown: pushdown on q31 (0.49x)
**Anti-pattern:** When creating filtered versions of existing CTEs, always REMOVE the original unfiltered CTEs. Keeping both causes redundant materialization and 1000x+ cardinality misestimates on self-joins.
**Mechanism:** Created both filtered (store_sales_agg, web_sales_agg) AND original (ss, ws) versions of the same aggregations. The query does a 6-way self-join matching quarterly patterns (Q1->Q2->Q3). Duplicate CTEs doubled materialization and confused the optimizer's cardinality estimates for the multi-self-join.

### regression_q51_date_cte_isolate: date_cte_isolate on q51 (0.87x)
**Anti-pattern:** Do not materialize running/cumulative window aggregates into CTEs before joins that filter based on those aggregates. The optimizer can co-optimize window evaluation and join filtering together.
**Mechanism:** Materialized cumulative window functions (SUM() OVER ORDER BY) into separate CTEs (web_v1, store_v1) before a FULL OUTER JOIN that filters on web_cumulative > store_cumulative. The original evaluates windows lazily during the join, co-optimizing window computation with the join filter. Materialization forces full window computation before filtering.

## Correctness Constraints (4 — NEVER violate)

**[CRITICAL] COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.

**[CRITICAL] CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
  - Failure: Q21 — prefetched_inventory CTE omits i_item_id but main query references it in SELECT and GROUP BY
  - Failure: Q76 — filtered_store_dates CTE omits d_year and d_qoy but aggregation CTE uses them in GROUP BY

**[CRITICAL] LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.

**[CRITICAL] SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## Your Task

Work through these 3 steps in a `<reasoning>` block, then output the structured briefing below:

1. **DIAGNOSE**: Why did the best worker achieve 1.21x instead of the 2.0x target? Why did each other worker fail or regress? Be specific about structural mechanisms.
2. **IDENTIFY**: What optimization angles couldn't have been designed BEFORE seeing these empirical results? What did the results reveal about the query's actual execution behavior?
3. **SYNTHESIZE**: Design a strategy for the sniper that builds on the best foundation (if any) and exploits the newly-revealed angles. The sniper has full freedom — give it direction, not constraints.

### Output Format (follow EXACTLY)

```
=== SNIPE BRIEFING ===

FAILURE_SYNTHESIS:
<WHY the best worker won, WHY each other failed — structural mechanisms>

BEST_FOUNDATION:
<What to build on from the best result, or 'None — start fresh' if all regressed>

UNEXPLORED_ANGLES:
<What optimization approaches couldn't have been designed pre-empirically>

STRATEGY_GUIDANCE:
<Synthesized approach for the sniper — ADVISORY, not mandatory>

EXAMPLES: <ex1>, <ex2>, <ex3>

EXAMPLE_ADAPTATION:
<For each example: what to APPLY and what to IGNORE>

HAZARD_FLAGS:
<Risks based on observed failures — what NOT to do>

RETRY_WORTHINESS: high|low — <reason>
(Is there genuine headroom for a second sniper attempt if the first misses 2.0x?)

RETRY_DIGEST:
<5-10 line compact failure guide for sniper2 IF retry is needed.
What broke, why, what to change. The lesson, not the artifact.>
```