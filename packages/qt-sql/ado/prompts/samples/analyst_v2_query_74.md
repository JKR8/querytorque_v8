You are a senior query optimization architect. Your job is to deeply analyze a SQL query and produce a structured briefing for 4 specialist workers who will each write a different optimized version.

You are the ONLY call that sees all the data: EXPLAIN plans, DAG costs, full constraint list, global knowledge, and the complete example catalog. The workers will only see what YOU put in their briefings. Your output quality directly determines their success.

## Query: query_74
## Dialect: duckdb v1.4.3

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

**NOTE:** The EXPLAIN plan shows the PHYSICAL execution structure, which may differ significantly from the LOGICAL DAG below. The optimizer may have already split CTEs, reordered joins, or pushed predicates. When the EXPLAIN and DAG disagree, the EXPLAIN is ground truth for what the optimizer is already doing.

DuckDB EXPLAIN ANALYZE reports **operator-exclusive** wall-clock time per node (children's time is NOT included in the parent's reported time). The percentage annotations are also exclusive. You can sum sibling nodes to get pipeline cost. DAG cost percentages are derived metrics that may not reflect actual execution time — use EXPLAIN timings as ground truth.

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


## Pre-Computed Semantic Intent

**Query intent:** Find customers whose year-over-year change in web-payment variability (stddev of net paid) from 1999 to 2000 exceeds the corresponding change in store-payment variability.

START from this pre-computed intent. In your SEMANTIC_CONTRACT output, ENRICH it with: intersection/union semantics from JOIN types, aggregation function traps, NULL propagation paths, and filter dependencies. Do NOT re-derive what is already stated above.

## Aggregation Semantics Check

You MUST verify aggregation equivalence for any proposed restructuring:

- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. Returns NULL for 0-1 values. Changing group membership changes the result.
- `STDDEV_SAMP(x) FILTER (WHERE year=1999)` over a combined (1999,2000) group is NOT equivalent to `STDDEV_SAMP(x)` over only 1999 rows — FILTER still uses the combined group's membership for the stddev denominator.
- **AVG and STDDEV are NOT duplicate-safe**: if a join introduces row duplication, the aggregate result changes.
- When splitting a UNION ALL CTE with GROUP BY + aggregate, each split branch must preserve the exact GROUP BY columns and filter to the exact same row set as the original.
- **SAFE ALTERNATIVE**: If GROUP BY includes the discriminator column (e.g., d_year), each group is already partitioned. STDDEV_SAMP computed per-group is correct. You can then pivot using `MAX(CASE WHEN year = 1999 THEN year_total END) AS year_total_1999` because the GROUP BY guarantees exactly one row per (customer, year) — the MAX is just a row selector, not a real aggregation.

## Top 16 Tag-Matched Examples

### union_cte_split (1.36xx)
**Description:** Split a generic UNION ALL CTE into specialized CTEs when the main query filters by year or discriminator - eliminates redundant scans
**Principle:** CTE Specialization: when a generic CTE is scanned multiple times with different filters (e.g., by year), split it into specialized CTEs that embed the filter in their definition. Each specialized CTE processes only its relevant subset, eliminating redundant scans.

### intersect_to_exists (1.83xx)
**Description:** Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join planning
**Principle:** Semi-Join Short-Circuit: replace INTERSECT with EXISTS to avoid full materialization and sorting. INTERSECT must compute complete result sets before intersecting; EXISTS stops at the first match per row, enabling semi-join optimizations.

### multi_intersect_exists_cte (2.39xx)
**Description:** Convert cascading INTERSECT operations into correlated EXISTS subqueries with pre-materialized date and channel CTEs

### deferred_window_aggregation (1.36xx)
**Description:** When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joined with FULL OUTER JOIN followed by another WINDOW pass for NULL carry-forward: defer the WINDOW out of the CTEs, join daily totals, then compute cumulative sums once on the joined result. SUM() OVER() naturally skips NULLs, eliminating the need for a separate MAX() carry-forward window.
**Principle:** Deferred Aggregation: delay expensive operations (window functions) until after joins reduce the dataset. Computing window functions inside individual CTEs then joining is more expensive than joining first and computing windows once on the combined result.

### shared_dimension_multi_channel (1.30xx)
**Description:** Extract shared dimension filters (date, item, promotion) into CTEs when multiple channel CTEs (store/catalog/web) apply identical filters independently
**Principle:** Shared Dimension Extraction: when multiple channel CTEs (store/catalog/web) apply identical dimension filters, extract those shared filters into one CTE and reference it from each channel. Avoids redundant dimension scans.

### composite_decorrelate_union (2.42xx)
**Description:** Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT customer CTEs with a shared date filter, and replace OR(EXISTS a, EXISTS b) with UNION of key sets
**Principle:** Composite Decorrelation: when multiple correlated EXISTS share common filters, extract shared dimensions into a single CTE and decorrelate the EXISTS checks into pre-materialized key sets joined via UNION.

### prefetch_fact_join (3.77xx)
**Description:** Pre-filter dimension table into CTE, then pre-join with fact table in second CTE before joining other dimensions
**Principle:** Staged Join Pipeline: build a CTE chain that progressively reduces data — first CTE filters the dimension, second CTE pre-joins filtered dimension keys with the fact table, subsequent CTEs join remaining dimensions against the already-reduced fact set.

### rollup_to_union_windowing (2.47xx)
**Description:** Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each hierarchy level, combined with window functions for ranking

### date_cte_isolate (4.00xx)
**Description:** Extract date filtering into a separate CTE to enable predicate pushdown and reduce scans
**Principle:** Dimension Isolation: extract small dimension lookups into CTEs so they materialize once and subsequent joins probe a tiny hash table instead of rescanning.

### decorrelate (2.92xx)
**Description:** Convert correlated subquery to separate CTE with GROUP BY, then JOIN
**Principle:** Decorrelation: convert correlated subqueries to standalone CTEs with GROUP BY, then JOIN. Correlated subqueries re-execute per outer row; a pre-computed CTE executes once.

### materialize_cte (1.37xx)
**Description:** Extract repeated subquery patterns into a CTE to avoid recomputation
**Principle:** Shared Materialization: extract repeated subquery patterns into CTEs to avoid recomputation. When the same logical check appears multiple times, compute it once and reference the result.

### multi_date_range_cte (2.35xx)
**Description:** When query uses multiple date_dim aliases with different filters (d1, d2, d3), create separate CTEs for each date range and pre-join with fact tables
**Principle:** Early Selection per Alias: when a query joins the same dimension table multiple times with different filters (d1, d2, d3), create separate CTEs for each filter and pre-join with fact tables to reduce rows entering the main join.

### multi_dimension_prefetch (2.71xx)
**Description:** Pre-filter multiple dimension tables (date + store) into separate CTEs before joining with fact table
**Principle:** Multi-Dimension Prefetch: when multiple dimension tables have selective filters, pre-filter ALL of them into CTEs before the fact table join. Combined selectivity compounds — each dimension CTE reduces the fact scan further.

### or_to_union (3.17xx)
**Description:** Split OR conditions on different columns into UNION ALL branches for better index usage
**Principle:** OR-to-UNION Decomposition: split OR conditions on different columns into separate UNION ALL branches, each with a focused predicate. The optimizer can use different access paths per branch instead of a single scan with a complex filter.

### early_filter (4.00xx)
**Description:** Filter dimension tables FIRST, then join to fact tables to reduce expensive joins
**Principle:** Early Selection: filter small dimension tables first, then join to large fact tables. This reduces the fact table scan to only rows matching the filter, rather than scanning all rows and filtering after the join.

### dimension_cte_isolate (1.93xx)
**Description:** Pre-filter ALL dimension tables into CTEs before joining with fact table, not just date_dim
**Principle:** Early Selection: pre-filter dimension tables into CTEs returning only surrogate keys before joining with fact tables. Each dimension CTE is tiny, creating small hash tables that speed up the fact table probe.

## Full Example Catalog

- **channel_bitmap_aggregation** (6.24xx) — Consolidate repeated scans of the same fact table (one per time/channel bucket) 
- **composite_decorrelate_union** (2.42xx) — Decorrelate multiple correlated EXISTS subqueries into pre-materialized DISTINCT
- **date_cte_isolate** (4.00xx) — Extract date filtering into a separate CTE to enable predicate pushdown and redu
- **decorrelate** (2.92xx) — Convert correlated subquery to separate CTE with GROUP BY, then JOIN
- **deferred_window_aggregation** (1.36xx) — When multiple CTEs each perform GROUP BY + WINDOW (cumulative sum), then are joi
- **dimension_cte_isolate** (1.93xx) — Pre-filter ALL dimension tables into CTEs before joining with fact table, not ju
- **early_filter** (4.00xx) — Filter dimension tables FIRST, then join to fact tables to reduce expensive join
- **intersect_to_exists** (1.83xx) — Convert INTERSECT subquery pattern to multiple EXISTS clauses for better join pl
- **materialize_cte** (1.37xx) — Extract repeated subquery patterns into a CTE to avoid recomputation
- **multi_date_range_cte** (2.35xx) — When query uses multiple date_dim aliases with different filters (d1, d2, d3), c
- **multi_dimension_prefetch** (2.71xx) — Pre-filter multiple dimension tables (date + store) into separate CTEs before jo
- **multi_intersect_exists_cte** (2.39xx) — Convert cascading INTERSECT operations into correlated EXISTS subqueries with pr
- **or_to_union** (3.17xx) — Split OR conditions on different columns into UNION ALL branches for better inde
- **prefetch_fact_join** (3.77xx) — Pre-filter dimension table into CTE, then pre-join with fact table in second CTE
- **pushdown** (2.11xx) — Push filters from outer query into CTEs/subqueries to reduce intermediate result
- **rollup_to_union_windowing** (2.47xx) — Replace GROUP BY ROLLUP with explicit UNION ALL of pre-aggregated CTEs at each h
- **shared_dimension_multi_channel** (1.30xx) — Extract shared dimension filters (date, item, promotion) into CTEs when multiple
- **single_pass_aggregation** (4.47xx) — Consolidate multiple subqueries scanning the same table into a single CTE with c
- **union_cte_split** (1.36xx) — Split a generic UNION ALL CTE into specialized CTEs when the main query filters 

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

### regression_q74_pushdown: pushdown on q74 (0.68xx)
**Anti-pattern:** When splitting a UNION CTE by year, you MUST remove or replace the original UNION CTE. Keeping both the split and original versions causes redundant materialization and extreme cardinality misestimates.
**Mechanism:** Created year-specific CTEs (store_sales_1999, store_sales_2000, etc.) but KEPT the original year_total union CTE alongside them. The optimizer materializes both the split versions and the original union, resulting in redundant computation. Projection cardinality estimates show 10^16x errors from the confused CTE graph.

### regression_q31_pushdown: pushdown on q31 (0.49xx)
**Anti-pattern:** When creating filtered versions of existing CTEs, always REMOVE the original unfiltered CTEs. Keeping both causes redundant materialization and 1000x+ cardinality misestimates on self-joins.
**Mechanism:** Created both filtered (store_sales_agg, web_sales_agg) AND original (ss, ws) versions of the same aggregations. The query does a 6-way self-join matching quarterly patterns (Q1->Q2->Q3). Duplicate CTEs doubled materialization and confused the optimizer's cardinality estimates for the multi-self-join.

### regression_q51_date_cte_isolate: date_cte_isolate on q51 (0.87xx)
**Anti-pattern:** Do not materialize running/cumulative window aggregates into CTEs before joins that filter based on those aggregates. The optimizer can co-optimize window evaluation and join filtering together.
**Mechanism:** Materialized cumulative window functions (SUM() OVER ORDER BY) into separate CTEs (web_v1, store_v1) before a FULL OUTER JOIN that filters on web_cumulative > store_cumulative. The original evaluates windows lazily during the join, co-optimizing window computation with the join filter. Materialization forces full window computation before filtering.

## All Constraints (19 total)

### Constraint Quick-Filter (check prerequisites first)

- EXISTS/NOT EXISTS in query? -> Check KEEP_EXISTS_AS_EXISTS, NO_MATERIALIZE_EXISTS
- OR conditions in WHERE? -> Check OR_TO_UNION_GUARD, OR_TO_UNION_SELF_JOIN, DIMENSION_CTE_SAME_COLUMN_OR
- UNION ALL CTE being split? -> Check UNION_CTE_SPLIT_MUST_REPLACE, REMOVE_REPLACED_CTES
- Self-join (same table/CTE aliased 2+ times)? -> Check OR_TO_UNION_SELF_JOIN
- Creating new CTEs? -> Check NO_UNFILTERED_DIMENSION_CTE, CTE_COLUMN_COMPLETENESS, REMOVE_REPLACED_CTES, EARLY_FILTER_CTE_BEFORE_CHAIN
- Decorrelating subqueries? -> Check DECORRELATE_MUST_FILTER_FIRST
- Fast baseline (<100ms)? -> Check MIN_BASELINE_THRESHOLD
- Multiple dimension CTEs? -> Check NO_CROSS_JOIN_DIMENSIONS
- Fact table CTE chains? -> Check PREFETCH_MULTI_FACT_CHAIN
- Single-pass CASE branches? -> Check SINGLE_PASS_AGGREGATION_LIMIT

If the prerequisite doesn't exist in the query or your proposed transforms, skip that constraint entirely.

**[CRITICAL] COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.

**[CRITICAL] CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[CRITICAL] KEEP_EXISTS_AS_EXISTS**: Preserve EXISTS/NOT EXISTS subqueries as-is. Do NOT convert them to IN/NOT IN or to JOINs — this risks NULL-handling semantic changes and can introduce duplicate rows.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[CRITICAL] LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[CRITICAL] NO_CROSS_JOIN_DIMENSIONS**: NEVER combine multiple dimension tables into a single CTE via CROSS JOIN, Cartesian product, or JOIN ON TRUE. Even small dimensions (30 dates × 200 items × 20 promos = 120K rows) create huge intermediate results that prevent index use on fact tables. Always keep each dimension as a SEPARATE CTE: filtered_date AS (...), filtered_item AS (...), filtered_promotion AS (...). This was validated at 3.32x with separate CTEs vs 0.0076x with a merged CROSS JOIN CTE.
  - Observed: ? regressed to ?x

**[CRITICAL] NO_MATERIALIZE_EXISTS**: Keep EXISTS and NOT EXISTS as-is — they use semi-join short-circuiting that stops scanning after the first match. Converting them to materialized CTEs (e.g., WITH cte AS (SELECT DISTINCT ... FROM large_table)) forces a full table scan, which is catastrophically slower (0.14x observed on Q16). When you see EXISTS, preserve it.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[CRITICAL] OR_TO_UNION_SELF_JOIN**: Never apply or_to_union when the query contains a self-join (the same table appears twice with different aliases). Splitting self-join queries into UNION branches forces each branch to independently perform the self-join, doubling or tripling execution time. Observed 0.51x regression on Q23.
  - Observed: ? regressed to ?x

**[CRITICAL] SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

**[HIGH] MIN_BASELINE_THRESHOLD**: If the query execution plan shows very fast runtime (under 100ms), be conservative with CTE-based transforms. Each CTE adds materialization overhead (hash table creation, intermediate result storage). On fast queries, this overhead can exceed the filtering benefit. Prefer minimal changes or no change over adding multiple CTEs to an already-fast query.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[HIGH] NO_UNFILTERED_DIMENSION_CTE**: Every CTE you create must include a WHERE clause that actually reduces row count. Selecting fewer columns is not filtering — the CTE still materializes every row. If a dimension table has no predicate to push down, leave it as a direct join in the main query instead of wrapping it in a CTE.
  - Observed: ? regressed to ?x

**[HIGH] OR_TO_UNION_GUARD**: Only apply or_to_union when (a) the OR branches involve different tables or fundamentally different access paths — never when all branches filter the same column (e.g., t_hour ranges), since the optimizer already handles same-column ORs efficiently in a single scan — and (b) the result is 3 or fewer UNION ALL branches. Nested ORs that would expand into 4+ branches (e.g., 3 conditions x 3 values = 9 combinations) must be left as-is. Violating these rules causes 0.23x–0.59x regressions from multiplied fact table scans.
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[HIGH] PREFETCH_MULTI_FACT_CHAIN**: When using prefetch_fact_join, do not create more than 2 cascading CTEs that each reference a fact table (store_sales, catalog_sales, web_sales, etc.). Each CTE in the chain materializes a large intermediate result. Observed 0.78x regression on Q4 from 3 cascading fact CTEs.
  - Observed: ? regressed to ?x

**[HIGH] REMOVE_REPLACED_CTES**: When creating replacement CTEs, overwrite the original by using the same node_id in your rewrite_sets, or ensure the original is removed from the WITH clause. Every CTE in the final query should be actively used — dead CTEs still get materialized and waste resources (caused 0.49x on Q31, 0.68x on Q74).
  - Observed: ? regressed to ?x
  - Observed: ? regressed to ?x

**[HIGH] SINGLE_PASS_AGGREGATION_LIMIT**: When applying single_pass_aggregation (consolidating repeated scans into CASE WHEN aggregates), use at most 8 CASE branches. Beyond 8 branches, the per-row CASE evaluation overhead can negate the benefit of reducing table scans. If the original query has more than 8 distinct filter conditions, keep some scans separate rather than forcing all into one pass.
  - Observed: ? regressed to ?x

**[HIGH] UNION_CTE_SPLIT_MUST_REPLACE**: When applying union_cte_split (splitting UNION into CTEs), the original UNION must be eliminated from the main query. The main query should reference the split CTEs, not duplicate the UNION branches. If the rewritten query has more UNION ALL operations than the original, the rewrite is incorrect.
  - Observed: ? regressed to ?x

**[MEDIUM] DECORRELATE_MUST_FILTER_FIRST**: When decorrelating a correlated subquery into a JOIN, ensure all original WHERE filters are preserved in the replacement CTE or JOIN condition. A decorrelation without selective filters creates a cross-product that is larger than the original per-row correlated execution. The replacement CTE must filter to at most the same cardinality as the original subquery.
  - Observed: ? regressed to ?x

**[MEDIUM] DIMENSION_CTE_SAME_COLUMN_OR**: Do not create dimension CTEs to isolate OR conditions that filter the same column. The optimizer handles same-column ORs efficiently in a single scan. Only apply dimension_cte_isolate when filters span different columns or different dimension tables.
  - Observed: ? regressed to ?x

**[MEDIUM] EARLY_FILTER_CTE_BEFORE_CHAIN**: When creating an early_filter CTE, ensure it is actually referenced in the main query chain. The original unfiltered table reference must be replaced with the CTE reference. Do not create CTEs that filter a table if the main query still joins the original unfiltered table — this adds overhead without benefit.
  - Observed: ? regressed to ?x

**[MEDIUM] EXPLICIT_JOINS**: Convert comma-separated implicit joins to explicit JOIN ... ON syntax. This gives the optimizer better join-order freedom.

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

3. **AGGREGATION TRAP CHECK**: For every aggregate function in the query, verify: does my proposed restructuring change which rows participate in each group? STDDEV_SAMP, VARIANCE, PERCENTILE_CONT, CORR are grouping-sensitive. SUM, COUNT, MIN, MAX are grouping-insensitive (modulo duplicates). If the query uses FILTER clauses or conditional aggregation, verify equivalence explicitly.

4. **TRANSFORM SELECTION**: From the Transform Catalog below, identify ALL transforms that are structurally applicable (prerequisite exists in the query). Rank by expected value (rows affected x historical speedup). Select 4 that are structurally diverse.

5. **CONSTRAINT FILTERING**: For each constraint in the full list, check:
   - Does this query have the structure the constraint warns about? (EXISTS? OR conditions? Self-joins? UNION CTEs being split?)
   - Does any proposed transform create the anti-pattern?
   Select 3-6 that apply. Discard the rest.

6. **DAG DESIGN**: For each worker's strategy, define the target DAG topology. Verify that every node contract has exhaustive output columns by checking downstream references.

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
- [CONSTRAINT_ID]: [Why it applies to this query, 1 line]
- [CONSTRAINT_ID]: [Why it applies]
(Select 3-6 constraints from the full list above. Only include
constraints that are RELEVANT to this specific query.)

REGRESSION_WARNINGS:
1. [Pattern name] ([observed regression]):
   CAUSE: [What happened mechanistically]
   RULE: [Actionable avoidance rule for THIS query]
(If no regression warnings are relevant, write 'None applicable.')

=== WORKER 1 BRIEFING ===

STRATEGY: [strategy_name]
TARGET_DAG:
  [node] -> [node] -> [node]
NODE_CONTRACTS:
(Write all fields as SQL fragments, not natural language.
Example: 'WHERE: d_year IN (1999, 2000)' not 'WHERE: filter to target years'.
The worker uses these as specifications to code against.)
  [node_name]:
    FROM: [tables/CTEs]
    JOIN: [join conditions]
    WHERE: [filters]
    GROUP BY: [columns] (if applicable)
    AGGREGATE: [functions] (if applicable)
    OUTPUT: [exhaustive column list]
    EXPECTED_ROWS: [approximate row count from EXPLAIN analysis]
    CONSUMERS: [downstream nodes]
EXAMPLES: [ex1], [ex2], [ex3]
EXAMPLE_REASONING:
[Why each example's pattern matches THIS query's bottleneck.
What adaptation is needed.]
HAZARD_FLAGS:
- [Specific risk for this approach on this query]

=== WORKER 2 BRIEFING ===
[Same structure as Worker 1, DIFFERENT strategy]

=== WORKER 3 BRIEFING ===
[Same structure as Worker 1, DIFFERENT strategy]

=== WORKER 4 BRIEFING ===
[Same structure as Worker 1, DIFFERENT strategy]
```

## Transform Catalog

Select 4 transforms that are applicable to THIS query, maximizing structural diversity (each must attack a different part of the execution plan).

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

## Strategy Selection Rules

1. **CHECK APPLICABILITY**: Each transform has a structural prerequisite (correlated subquery, UNION ALL CTE, LEFT JOIN, etc.). Verify the query actually has the prerequisite before assigning a transform. DO NOT assign decorrelation if there are no correlated subqueries.
2. **CHECK OPTIMIZER OVERLAP**: Read the EXPLAIN plan. If the optimizer already performs a transform (e.g., already splits a UNION CTE, already pushes a predicate), that transform will have marginal benefit. Note this in your reasoning and prefer transforms the optimizer is NOT already doing.
3. **MAXIMIZE DIVERSITY**: Each worker must attack a different part of the execution plan. Do not assign 'pushdown variant A' and 'pushdown variant B'. Assign transforms from different categories above.
4. **ASSESS RISK PER-QUERY**: Risk is a function of (transform x query complexity), not an inherent property of the transform. Decorrelation is low-risk on a simple EXISTS and high-risk on nested correlation inside a CTE. Assess per-assignment.
5. **COMPOSITION IS ALLOWED**: A worker's strategy can combine 2 transforms from different categories (e.g., star_join_prefetch + scan_consolidation_pivot). The TARGET_DAG should reflect the combined structure. Do not assign two workers the same composition — each must include at least one unique transform.
6. **MINIMAL-CHANGE BASELINE**: If the EXPLAIN shows the optimizer already handles the primary bottleneck (e.g., already splits CTEs, already pushes predicates), consider assigning one worker as a minimal-change baseline: explicit JOINs only, no structural changes. This provides a regression-safe fallback.

Each worker gets 1-3 examples. If fewer than 2 examples genuinely match the worker's strategy, assign 1 and state 'No additional examples apply.' Do NOT pad with irrelevant examples — an irrelevant example is worse than no example because the worker will try to apply its pattern. No duplicate examples across workers. Use example IDs from the catalog above.

For TARGET_DAG: Define the CTE structure you want the worker to produce. The worker's job becomes pure SQL generation within your defined structure. For NODE_CONTRACTS: Be exhaustive with OUTPUT columns — missing columns cause semantic breaks.

## Output Consumption Spec

Each worker receives:
1. SHARED BRIEFING (SEMANTIC_CONTRACT + BOTTLENECK_DIAGNOSIS + ACTIVE_CONSTRAINTS + REGRESSION_WARNINGS)
2. Their specific WORKER N BRIEFING (STRATEGY + TARGET_DAG + NODE_CONTRACTS + EXAMPLES + EXAMPLE_REASONING + HAZARD_FLAGS)
3. Full before/after SQL for their assigned examples (retrieved by example ID)
4. The original query SQL (full, as reference)
5. Column completeness contract + output format spec

Workers do NOT see other workers' briefings.
Presentation order: briefing first (understanding), then examples (patterns), then original SQL (source), then output format (mechanics).