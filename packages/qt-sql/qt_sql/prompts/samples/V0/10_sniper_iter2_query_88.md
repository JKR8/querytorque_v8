## PREVIOUS SNIPER ATTEMPT (iter 1) — Learn from this

Your first attempt achieved **5.8x** against a target of **2.0x**.

### What went wrong and what to change:
All 4 workers achieved 5.27x–6.24x. W2's single-pass approach is near-optimal. Limited headroom for further improvement. A retry would focus on micro-optimizations (join order, CTE inlining).

---


You are a senior SQL optimization architect for DuckDB v1.4.3. You have FULL FREEDOM to design your own approach — you are NOT constrained to any specific DAG topology or CTE structure. The analyst's strategy guidance below is ADVISORY, not mandatory.

Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END around a division, keep it — guards prevent silent breakage. Strip benchmark comments (-- start query, -- end query) from output.

## Target: >=2.0x speedup

Your target is >=2.0x speedup on this query. This is the bar. Anything below 2.0x is a miss.

## Previous Optimization Attempts
Target: **>=2.0x** | 5 workers tried | 5 reached target

| Worker | Strategy | Speedup | Status | Error |
|--------|----------|---------|--------|-------|
| W2 ★ | moderate_dimension_isolation | 6.2373882475101565x | pass |  |
| W4 | novel_structural_transform | 6.104619135142192x | pass |  |
| W3 | aggressive_single_pass_restructure | 5.853343016368592x | pass |  |
| W5 | single_pass_aggregation + join_order_optimization | 5.8x | PASS |  |
| W1 | conservative_early_reduction | 5.269233093530888x | pass |  |


## Best Foundation SQL

The best previous result. You may build on this or start fresh.

```sql
WITH filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_name = 'ese'
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE (
        (hd_dep_count = -1 AND hd_vehicle_count <= -1 + 2)
        OR (hd_dep_count = 4 AND hd_vehicle_count <= 4 + 2)
        OR (hd_dep_count = 3 AND hd_vehicle_count <= 3 + 2)
    )
),
time_ranges AS (
    SELECT 
        t_time_sk,
        CASE 
            WHEN t_hour = 8 AND t_minute >= 30 THEN 1
            WHEN t_hour = 9 AND t_minute < 30 THEN 2
            WHEN t_hour = 9 AND t_minute >= 30 THEN 3
            WHEN t_hour = 10 AND t_minute < 30 THEN 4
            WHEN t_hour = 10 AND t_minute >= 30 THEN 5
            WHEN t_hour = 11 AND t_minute < 30 THEN 6
            WHEN t_hour = 11 AND t_minute >= 30 THEN 7
            WHEN t_hour = 12 AND t_minute < 30 THEN 8
        END AS time_window
    FROM time_dim
    WHERE (
        (t_hour = 8 AND t_minute >= 30) OR
        (t_hour = 9 AND t_minute < 30) OR
        (t_hour = 9 AND t_minute >= 30) OR
        (t_hour = 10 AND t_minute < 30) OR
        (t_hour = 10 AND t_minute >= 30) OR
        (t_hour = 11 AND t_minute < 30) OR
        (t_hour = 11 AND t_minute >= 30) OR
        (t_hour = 12 AND t_minute < 30)
    )
),
sales_with_time AS (
    SELECT 
        tr.time_window,
        ss.ss_sold_time_sk
    FROM store_sales ss
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_hd fhd ON ss.ss_hdemo_sk = fhd.hd_demo_sk
    JOIN time_ranges tr ON ss.ss_sold_time_sk = tr.t_time_sk
)
SELECT 
    COUNT(CASE WHEN time_window = 1 THEN 1 END) AS h8_30_to_9,
    COUNT(CASE WHEN time_window = 2 THEN 1 END) AS h9_to_9_30,
    COUNT(CASE WHEN time_window = 3 THEN 1 END) AS h9_30_to_10,
    COUNT(CASE WHEN time_window = 4 THEN 1 END) AS h10_to_10_30,
    COUNT(CASE WHEN time_window = 5 THEN 1 END) AS h10_30_to_11,
    COUNT(CASE WHEN time_window = 6 THEN 1 END) AS h11_to_11_30,
    COUNT(CASE WHEN time_window = 7 THEN 1 END) AS h11_30_to_12,
    COUNT(CASE WHEN time_window = 8 THEN 1 END) AS h12_to_12_30
FROM sales_with_time;
```

## Failure Synthesis (from diagnostic analyst)

All 4 workers achieved strong wins (5.27x–6.24x) by consolidating 8 independent store_sales scans into a single pass. W2 (6.24x) was best because it classified time windows in the CTE itself using CASE, avoiding any post-join filtering. W1 (5.27x) was slowest because it kept 8 correlated subqueries against a single qualified_sales CTE — still 8 scans of the CTE. W3/W4 used similar single-pass strategies with minor structural differences.

## Unexplored Angles

1. Aggregate pushdown: compute counts directly in the sales_with_time CTE instead of materializing individual rows then aggregating
2. Hash join order optimization: join the smallest dimension first (store ~5 rows) to reduce intermediate cardinality earliest
3. Bit manipulation: encode time window as a bitmap for the 8 windows and use a single GROUP BY with bit operations

## Strategy Guidance (ADVISORY — not mandatory)

W2's approach is nearly optimal — 6.24x from a single-pass scan. The remaining headroom is in join ordering (smallest dim first) and potentially pushing the aggregation into a single SELECT without materializing sales_with_time as a separate CTE.

## Example Adaptation Notes

single_pass_aggregation: W2 already applied this. The sniper should focus on whether inlining the final aggregation into the CTE body (no separate final_counts step) improves performance.
dimension_cte_isolate: Already applied by all workers. Verify that join order matches dimension cardinality (store first, then hd, then time).

## Reference Examples

Pattern reference only — do not copy table/column names or literals.

### 1. single_pass_aggregation (4.47x)

**Principle:** Single-Pass Aggregation: consolidate multiple scalar subqueries on the same table into one CTE using CASE expressions inside aggregate functions. Reduces N separate table scans to 1 pass.

**BEFORE (slow):**
```sql
select case when (select count(*) 
                  from store_sales 
                  where ss_quantity between 1 and 20) > 2972190
            then (select avg(ss_ext_sales_price) 
                  from store_sales 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 21 and 40) > 4505785
            then (select avg(ss_ext_sales_price)
                  from store_sales
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 41 and 60) > 1575726
            then (select avg(ss_ext_sales_price)
                  from store_sales
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 61 and 80) > 3188917
            then (select avg(ss_ext_sales_price)
                  from store_sales
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales
                  where ss_quantity between 81 and 100) > 3525216
            then (select avg(ss_ext_sales_price)
                  from store_sales
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_profit)
                  from store_sales
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1;
```

**AFTER (fast):**
[store_sales_aggregates]:
```sql
SELECT SUM(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 ELSE 0 END) AS cnt1, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_discount_amt END) AS avg_disc1, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_paid END) AS avg_paid1, SUM(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 ELSE 0 END) AS cnt2, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_discount_amt END) AS avg_disc2, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_paid END) AS avg_paid2, SUM(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 ELSE 0 END) AS cnt3, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_discount_amt END) AS avg_disc3, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_paid END) AS avg_paid3, SUM(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 ELSE 0 END) AS cnt4, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_discount_amt END) AS avg_disc4, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_paid END) AS avg_paid4, SUM(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 ELSE 0 END) AS cnt5, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_discount_amt END) AS avg_disc5, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_paid END) AS avg_paid5 FROM store_sales
```
[main_query]:
```sql
SELECT CASE WHEN cnt1 > 74129 THEN avg_disc1 ELSE avg_paid1 END AS bucket1, CASE WHEN cnt2 > 122840 THEN avg_disc2 ELSE avg_paid2 END AS bucket2, CASE WHEN cnt3 > 56580 THEN avg_disc3 ELSE avg_paid3 END AS bucket3, CASE WHEN cnt4 > 10097 THEN avg_disc4 ELSE avg_paid4 END AS bucket4, CASE WHEN cnt5 > 165306 THEN avg_disc5 ELSE avg_paid5 END AS bucket5 FROM store_sales_aggregates
```

### 2. dimension_cte_isolate (1.93x)

**Principle:** Early Selection: pre-filter dimension tables into CTEs returning only surrogate keys before joining with fact tables. Each dimension CTE is tiny, creating small hash tables that speed up the fact table probe.

**BEFORE (slow):**
```sql
select i_item_id, 
        avg(cs_quantity) agg1,
        avg(cs_list_price) agg2,
        avg(cs_coupon_amt) agg3,
        avg(cs_sales_price) agg4 
 from catalog_sales, customer_demographics, date_dim, item, promotion
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd_demo_sk and
       cs_promo_sk = p_promo_sk and
       cd_gender = 'M' and 
       cd_marital_status = 'S' and
       cd_education_status = 'Unknown' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 LIMIT 100;
```

**AFTER (fast):**
[filtered_dates]:
```sql
SELECT d_date_sk FROM date_dim WHERE d_year = 2000
```
[filtered_customer_demographics]:
```sql
SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'College'
```
[filtered_promotions]:
```sql
SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' OR p_channel_event = 'N'
```
[joined_facts]:
```sql
SELECT cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price FROM catalog_sales AS cs JOIN filtered_dates AS fd ON cs.cs_sold_date_sk = fd.d_date_sk JOIN filtered_customer_demographics AS fcd ON cs.cs_bill_cdemo_sk = fcd.cd_demo_sk JOIN filtered_promotions AS fp ON cs.cs_promo_sk = fp.p_promo_sk
```
[main_query]:
```sql
SELECT i_item_id, AVG(cs_quantity) AS agg1, AVG(cs_list_price) AS agg2, AVG(cs_coupon_amt) AS agg3, AVG(cs_sales_price) AS agg4 FROM joined_facts AS jf JOIN item AS i ON jf.cs_item_sk = i.i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

## Hazard Flags

- All workers passed — no semantic errors to avoid
- The 8 COUNT(CASE) pattern is proven correct
- Avoid re-introducing 8 separate scans (the original bottleneck)

## Engine Profile

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
    + Q6/Q11: 4.00x — date filter moved into CTE
    + Q63: 3.77x — pre-joined filtered dates with fact table before other dims
    + Q93: 2.97x — dimension filter applied before LEFT JOIN chain
- **REDUNDANT_SCAN_ELIMINATION**: Cannot detect when the same fact table is scanned N times with similar filters across subquery boundaries.
  Opportunity: Consolidate N subqueries on the same table into 1 scan with CASE WHEN / FILTER() inside aggregates.
    + Q88: 6.28x — 8 time-bucket subqueries consolidated into 1 scan with 8 CASE branches
    + Q9: 4.47x — 15 separate store_sales scans consolidated into 1 scan with 5 CASE buckets
- **CORRELATED_SUBQUERY_PARALYSIS**: Cannot automatically decorrelate correlated aggregate subqueries into GROUP BY + JOIN.
  Opportunity: Convert correlated WHERE to CTE with GROUP BY on the correlation column, then JOIN back.
    + Q1: 2.92x — correlated AVG with store_sk correlation converted to GROUP BY store_sk + JOIN
- **CROSS_COLUMN_OR_DECOMPOSITION**: Cannot decompose OR conditions that span DIFFERENT columns into independent targeted scans.
  Opportunity: Split cross-column ORs into UNION ALL branches, each with a targeted single-column filter.
    + Q88: 6.28x — 8 time-bucket subqueries with distinct hour ranges (distinct access paths)
    + Q15: 3.17x — (zip OR state OR price) split to 3 targeted branches
    + Q10: 1.49x, Q45: 1.35x, Q41: 1.89x
- **LEFT_JOIN_FILTER_ORDER_RIGIDITY**: Cannot reorder LEFT JOINs to apply selective dimension filters before expensive fact table joins.
  Opportunity: Pre-filter the selective dimension into a CTE, then use the filtered result as the JOIN partner.
    + Q93: 2.97x — filtered reason dimension FIRST, then LEFT JOIN to returns then fact
    + Q80: 1.40x — dimension isolation before fact join
- **UNION_CTE_SELF_JOIN_DECOMPOSITION**: When a UNION ALL CTE is self-joined N times with each join filtering to a different discriminator, the optimizer materializes the full UNION once and probes it N times, discarding most rows each time.
  Opportunity: Split the UNION ALL into N separate CTEs (one per discriminator value).
    + Q74: 1.36x — UNION of store/web sales split into separate year-partitioned CTEs

## Correctness Invariants (HARD STOPS — non-negotiable)

These 4 constraints are absolute. Even with full creative freedom, you may NEVER violate these:

- **COMPLETE_OUTPUT**: The rewritten query must output ALL columns from the original SELECT. Never drop, rename, or reorder output columns. Every column alias must be preserved exactly as in the original.
- **CTE_COLUMN_COMPLETENESS**: CRITICAL: When creating or modifying a CTE, its SELECT list MUST include ALL columns referenced by downstream queries. Check the Node Contracts section: every column in downstream_refs MUST appear in the CTE output. Also ensure: (1) JOIN columns used by consumers are included in SELECT, (2) every table referenced in WHERE is present in FROM/JOIN, (3) no ambiguous column names between the CTE and re-joined tables. Dropping a column that a downstream node needs will cause an execution error.
- **LITERAL_PRESERVATION**: CRITICAL: When rewriting SQL, you MUST copy ALL literal values (strings, numbers, dates) EXACTLY from the original query. Do NOT invent, substitute, or 'improve' any filter values. If the original says d_year = 2000, your rewrite MUST say d_year = 2000. If the original says ca_state = 'GA', your rewrite MUST say ca_state = 'GA'. Changing these values will produce WRONG RESULTS and the rewrite will be REJECTED.
- **SEMANTIC_EQUIVALENCE**: The rewritten query MUST return exactly the same rows, columns, and ordering as the original. This is the prime directive. Any rewrite that changes the result set — even by one row, one column, or a different sort order — is WRONG and will be REJECTED.

## Aggregation Semantics Check (HARD STOP)

- STDDEV_SAMP/VARIANCE are grouping-sensitive — changing group membership changes the result.
- AVG and STDDEV are NOT duplicate-safe.
- FILTER over a combined group != separate per-group computation.
- Verify aggregation equivalence for ANY proposed restructuring.

## Regression Warnings

### regression_q67_date_cte_isolate: date_cte_isolate on q67 (0.85x)
**Anti-pattern:** Do not materialize dimension filters into CTEs before complex aggregations (ROLLUP, CUBE, GROUPING SETS) with window functions. The optimizer needs to push aggregation through joins; CTEs create materialization barriers.
**Mechanism:** Materialized date, store, and item dimension filters into CTEs before a ROLLUP aggregation with window functions (RANK() OVER). CTE materialization prevents the optimizer from pushing the ROLLUP and window computation down through the join tree, forcing a full materialized intermediate before the expensive aggregation.

### regression_q90_materialize_cte: materialize_cte on q90 (0.59x)
**Anti-pattern:** Never convert OR conditions on the SAME column (e.g., range conditions on t_hour) into UNION ALL. The optimizer already handles same-column ORs as a single scan. UNION ALL only helps when branches access fundamentally different tables or columns.
**Mechanism:** Split a simple OR condition (t_hour BETWEEN 10 AND 11 OR t_hour BETWEEN 16 AND 17) into UNION ALL of two separate web_sales scans. This doubles the fact table scan. DuckDB handles same-column OR ranges efficiently in a single scan — the UNION ALL adds materialization overhead with zero selectivity benefit.

### regression_q1_decorrelate: decorrelate on q1 (0.71x)
**Anti-pattern:** Do not pre-aggregate GROUP BY results into CTEs when the query uses them in a correlated comparison (e.g., customer return > 1.2x store average). The optimizer can compute aggregates incrementally with filter pushdown; materialization loses this.
**Mechanism:** Pre-computed customer_total_return (GROUP BY customer, store) and store_avg_return (GROUP BY store) as separate CTEs. The original correlated subquery computed the per-store average incrementally during the customer scan, filtering as it goes. Materializing forces full aggregation of ALL stores before any filtering.


## Original SQL

```sql
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
```

## Rewrite Checklist (must pass before final SQL)

- Verify output schema matches the Column Completeness Contract (same columns, same names, same order).
- Keep all semantic invariants from `Correctness Invariants` (including join/null behavior).
- Verify aggregation equivalence: same rows participate in each group, same aggregate semantics.
- Preserve all literals exactly (numbers, strings, date values).
- Apply `Hazard Flags` as hard guards against known failure modes.

### Column Completeness Contract

Your `main_query` component MUST produce **exactly** these output columns (same names, same order):

  1. `*`

Do NOT add, remove, or rename any output columns. The result set schema must be identical to the original query.

## Output Format

Your response has **two parts** in order:

### Part 1: Modified Logic Tree

Show what changed using change markers. Generate the tree BEFORE writing SQL.

Change markers:
- `[+]` — New component added
- `[-]` — Component removed
- `[~]` — Component modified (describe what changed)
- `[=]` — Unchanged (no children needed)
- `[!]` — Structural change (e.g. CTE → subquery)

### Part 2: Component Payload JSON

```json
{
  "spec_version": "1.0",
  "dialect": "<dialect>",
  "rewrite_rules": [
    {"id": "R1", "type": "<transform_name>", "description": "<what changed>", "applied_to": ["<component_id>"]}
  ],
  "statements": [{
    "target_table": null,
    "change": "modified",
    "components": {
      "<cte_name>": {
        "type": "cte",
        "change": "modified",
        "sql": "<complete SQL for this CTE body>",
        "interfaces": {"outputs": ["col1", "col2"], "consumes": ["<upstream_id>"]}
      },
      "main_query": {
        "type": "main_query",
        "change": "modified",
        "sql": "<final SELECT>",
        "interfaces": {"outputs": ["col1", "col2"], "consumes": ["<cte_name>"]}
      }
    },
    "reconstruction_order": ["<cte_name>", "main_query"],
    "assembly_template": "WITH <cte_name> AS ({<cte_name>}) {main_query}"
  }],
  "macros": {},
  "frozen_blocks": [],
  "validation_checks": []
}
```

### Rules
- **Tree first, always.** Generate the Logic Tree before writing any SQL
- **One component at a time.** When writing SQL for component X, treat others as opaque interfaces
- **No ellipsis.** Every `sql` value must be complete, executable SQL
- **Frozen blocks are copy-paste.** Large CASE-WHEN lookups must be verbatim
- **Validate interfaces.** Verify every `consumes` reference exists in upstream `outputs`
- Only include components you **changed or added** — set unchanged components to `"change": "unchanged"` with `"sql": ""`
- `main_query` output columns must match the Column Completeness Contract above
- `reconstruction_order`: topological order of components for assembly

After the JSON, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
Expected speedup: <estimate>
```

Now output your Logic Tree and Component Payload JSON: