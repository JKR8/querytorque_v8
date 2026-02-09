## PREVIOUS SNIPER ATTEMPT (iter 1) — Learn from this

Your first attempt achieved **1.65x** against a target of **2.0x**.

### What went wrong and what to change:
W2 (1.18x) is the best foundation. Fully deferring customer join to post-comparison should yield the remaining speedup. Target: 1.8-2.5x.

---


You are a senior SQL optimization architect for DuckDB v1.4.3. You have FULL FREEDOM to design your own approach — you are NOT constrained to any specific DAG topology or CTE structure. The analyst's strategy guidance below is ADVISORY, not mandatory.

Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END around a division, keep it — guards prevent silent breakage. Strip benchmark comments (-- start query, -- end query) from output.

## Target: >=2.0x speedup

Your target is >=2.0x speedup on this query. This is the bar. Anything below 2.0x is a miss.

## Previous Optimization Attempts
Target: **>=2.0x** | 5 workers tried | none reached target

| Worker | Strategy | Speedup | Status | Error |
|--------|----------|---------|--------|-------|
| W5 ★ | date_cte_isolate + full_late_binding | 1.65x | PASS |  |
| W1 | decorrelate + early_filter | 1.21x | PASS |  |
| W2 | date_cte_isolate + late_attribute_binding | 1.18x | PASS |  |
| W3 | prefetch_fact_join + materialize_cte | 0.95x | PASS |  |
| W4 | single_pass_aggregation | 0.0x | ERROR | column "ss_customer_sk" must appear in GROUP BY clause |


## Best Foundation SQL

The best previous result. You may build on this or start fresh.

```sql
-- Sniper iter1: deferred customer join
WITH ...
```

## Failure Synthesis (from diagnostic analyst)

Workers 1-2 achieved modest wins (1.21x, 1.18x) via date CTE isolation and decorrelation, but both fell short of the 2.0x target. The core bottleneck -- 4 redundant customer hash joins at 1.2s -- was partially addressed by W2's late binding but not aggressively enough. W3's prefetch regressed (0.95x) due to unnecessary materialization. W4 crashed on a GROUP BY semantic error.

## Unexplored Angles

1. Fully defer customer join to after the 4-way self-join comparison (resolve_names pattern) -- joins ~4K rows instead of 5.4M
2. Combine channel-specific aggregation CTEs (store_agg, web_agg) with year-partitioned grouping to eliminate the UNION ALL year_total CTE entirely
3. Try single-pass aggregation with FILTER (WHERE d_year = X) to avoid the self-join entirely -- compute both years' STDDEV in one scan

## Strategy Guidance (ADVISORY — not mandatory)

Build on W2's date CTE isolation but FULLY defer customer join to the very end. The key insight: ss_customer_sk and ws_bill_customer_sk are sufficient FK keys for the 4-way self-join comparison. Customer name resolution (c_customer_id, c_first_name, c_last_name) is needed ONLY for the final output -- join customer against ~4K qualifying rows, not against 5.4M raw fact rows.

## Example Adaptation Notes

date_cte_isolate: Apply the shared date CTE pattern but extend it -- instead of just isolating dates, use the date CTE as the entry point for both store_agg and web_agg CTEs, ensuring a single scan of date_dim.
shared_dimension_multi_channel: Apply the shared dimension pattern but go further -- the customer dimension is NOT shared across channels here, it's resolved at the end. Focus on sharing date_dim.

## Reference Examples

Pattern reference only — do not copy table/column names or literals.

### 1. shared_dimension_multi_channel (1.30x)

**Principle:** Shared Dimension Extraction: when multiple channel CTEs (store/catalog/web) apply identical dimension filters, extract those shared filters into one CTE and reference it from each channel. Avoids redundant dimension scans.

**BEFORE (slow):**
```sql
with ssr as
 (select  s_store_id as store_id,
          sum(ss_ext_sales_price) as sales,
          sum(coalesce(sr_return_amt, 0)) as "returns",
          sum(ss_net_profit - coalesce(sr_net_loss, 0)) as profit
  from store_sales left outer join store_returns on
         (ss_item_sk = sr_item_sk and ss_ticket_number = sr_ticket_number),
     date_dim,
     store,
     item,
     promotion
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-28' as date) 
                  and (cast('1998-08-28' as date) + INTERVAL 30 DAY)
       and ss_store_sk = s_store_sk
       and ss_item_sk = i_item_sk
       and i_current_price > 50
       and ss_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
 group by s_store_id)
 ,
 csr as
 (select  cp_catalog_page_id as catalog_page_id,
          sum(cs_ext_sales_price) as sales,
          sum(coalesce(cr_return_amount, 0)) as "returns",
          sum(cs_net_profit - coalesce(cr_net_loss, 0)) as profit
  from catalog_sales left outer join catalog_returns on
         (cs_item_sk = cr_item_sk and cs_order_number = cr_order_number),
     date_dim,
     catalog_page,
     item,
     promotion
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-28' as date)
                  and (cast('1998-08-28' as date) + INTERVAL 30 DAY)
        and cs_catalog_page_sk = cp_catalog_page_sk
       and cs_item_sk = i_item_sk
       and i_current_price > 50
       and cs_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by cp_catalog_page_id)
 ,
 wsr as
 (select  web_site_id,
          sum(ws_ext_sales_price) as sales,
          sum(coalesce(wr_return_amt, 0)) as "returns",
          sum(ws_net_profit - coalesce(wr_net_loss, 0)) as profit
  from web_sales left outer join web_returns on
         (ws_item_sk = wr_item_sk and ws_order_number = wr_order_number),
     date_dim,
     web_site,
     item,
     promotion
 where ws_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-28' as date)
                  and (cast('1998-08-28' as date) + INTERVAL 30 DAY)
        and ws_web_site_sk = web_site_sk
       and ws_item_sk = i_item_sk
       and i_current_price > 50
       and ws_promo_sk = p_promo_sk
       and p_channel_tv = 'N'
group by web_site_id)
  select channel
        , id
        , sum(sales) as sales
        , sum("returns") as "returns"
        , sum(profit) as profit
 from 
 (select 'store channel' as channel
        , 'store' || store_id as id
        , sales
        , "returns"
        , profit
 from   ssr
 union all
 select 'catalog channel' as channel
        , 'catalog_page' || catalog_page_id as id
        , sales
        , "returns"
        , profit
 from  csr
 union all
 select 'web channel' as channel
        , 'web_site' || web_site_id as id
        , sales
        , "returns"
        , profit
 from   wsr
 ) x
 group by rollup (channel, id)
 order by channel
         ,id
 LIMIT 100;
```

**AFTER (fast):**
[filtered_dates]:
```sql
SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-08-28' AS DATE) AND (CAST('1998-08-28' AS DATE) + INTERVAL '30' DAY)
```
[filtered_items]:
```sql
SELECT i_item_sk FROM item WHERE i_current_price > 50
```
[filtered_promotions]:
```sql
SELECT p_promo_sk FROM promotion WHERE p_channel_tv = 'N'
```
[prefiltered_store_sales]:
```sql
SELECT ss_item_sk, ss_store_sk, ss_ticket_number, ss_ext_sales_price, ss_net_profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_items ON ss_item_sk = i_item_sk JOIN filtered_promotions ON ss_promo_sk = p_promo_sk
```
[prefiltered_web_sales]:
```sql
SELECT ws_item_sk, ws_web_site_sk, ws_order_number, ws_ext_sales_price, ws_net_profit FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_items ON ws_item_sk = i_item_sk JOIN filtered_promotions ON ws_promo_sk = p_promo_sk
```
[ssr]:
```sql
SELECT s_store_id AS store_id, SUM(ss_ext_sales_price) AS sales, SUM(COALESCE(sr_return_amt, 0)) AS returns, SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit FROM prefiltered_store_sales LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number) JOIN store ON ss_store_sk = s_store_sk GROUP BY s_store_id
```
[wsr]:
```sql
SELECT web_site_id, SUM(ws_ext_sales_price) AS sales, SUM(COALESCE(wr_return_amt, 0)) AS returns, SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit FROM prefiltered_web_sales LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number) JOIN web_site ON ws_web_site_sk = web_site_sk GROUP BY web_site_id
```

### 2. date_cte_isolate (4.00x)

**Principle:** Dimension Isolation: extract small dimension lookups into CTEs so they materialize once and subsequent joins probe a tiny hash table instead of rescanning.

**BEFORE (slow):**
```sql
select a.ca_state state, count(*) cnt
 from customer_address a
     ,customer c
     ,store_sales s
     ,date_dim d
     ,item i
 where       a.ca_address_sk = c.c_current_addr_sk
 	and c.c_customer_sk = s.ss_customer_sk
 	and s.ss_sold_date_sk = d.d_date_sk
 	and s.ss_item_sk = i.i_item_sk
 	and d.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from date_dim
               where d_year = 2002
 	        and d_moy = 3 )
 	and i.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
 	     from item j 
 	     where j.i_category = i.i_category)
 group by a.ca_state
 having count(*) >= 10
 order by cnt, a.ca_state
 LIMIT 100;
```

**AFTER (fast):**
[target_month]:
```sql
SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2000 AND d_moy = 1
```
[category_avg_price]:
```sql
SELECT i_category, AVG(i_current_price) * 1.2 AS avg_threshold FROM item GROUP BY i_category
```
[filtered_dates]:
```sql
SELECT d_date_sk FROM date_dim JOIN target_month ON d_month_seq = target_month.d_month_seq
```
[filtered_sales]:
```sql
SELECT ss_customer_sk, ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
```
[main_query]:
```sql
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address a JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk JOIN filtered_sales s ON c.c_customer_sk = s.ss_customer_sk JOIN item i ON s.ss_item_sk = i.i_item_sk JOIN category_avg_price cap ON i.i_category = cap.i_category WHERE i.i_current_price > cap.avg_threshold GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

## Hazard Flags

- STDDEV_SAMP semantics: grouping must be per-year to get correct variance
- year_total CTE: do NOT include -- the 4 channel x year CTEs replace it
- customer join position: defer to end, not in aggregation CTEs

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

### regression_q74_pushdown: pushdown on q74 (0.68x)
**Anti-pattern:** When splitting a UNION CTE by year, you MUST remove or replace the original UNION CTE. Keeping both the split and original versions causes redundant materialization and extreme cardinality misestimates.
**Mechanism:** Created year-specific CTEs (store_sales_1999, store_sales_2000, etc.) but KEPT the original year_total union CTE alongside them. The optimizer materializes both the split versions and the original union, resulting in redundant computation. Projection cardinality estimates show 10^16x errors from the confused CTE graph.

### regression_q31_pushdown: pushdown on q31 (0.49x)
**Anti-pattern:** When creating filtered versions of existing CTEs, always REMOVE the original unfiltered CTEs. Keeping both causes redundant materialization and 1000x+ cardinality misestimates on self-joins.
**Mechanism:** Created both filtered (store_sales_agg, web_sales_agg) AND original (ss, ws) versions of the same aggregations. The query does a 6-way self-join matching quarterly patterns (Q1->Q2->Q3). Duplicate CTEs doubled materialization and confused the optimizer's cardinality estimates for the multi-self-join.

### regression_q51_date_cte_isolate: date_cte_isolate on q51 (0.87x)
**Anti-pattern:** Do not materialize running/cumulative window aggregates into CTEs before joins that filter based on those aggregates. The optimizer can co-optimize window evaluation and join filtering together.
**Mechanism:** Materialized cumulative window functions (SUM() OVER ORDER BY) into separate CTEs (web_v1, store_v1) before a FULL OUTER JOIN that filters on web_cumulative > store_cumulative. The original evaluates windows lazily during the join, co-optimizing window computation with the join filter. Materialization forces full window computation before filtering.


## Original SQL

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

## Rewrite Checklist (must pass before final SQL)

- Follow every node in `TARGET_DAG` and produce each `NODE_CONTRACT` output column exactly.
- Keep all semantic invariants from `Semantic Contract` and `Constraints` (including join/null behavior).
- Preserve all literals and the exact final output schema/order.
- Apply `Hazard Flags` and `Regression Warnings` as hard guards against known failure modes.

## Output

Return the complete rewritten SQL query. The query must be syntactically
valid and ready to execute.

### Column Completeness Contract

Your rewritten query MUST produce **exactly** these output columns (same names, same order):

  1. `customer_id`
  2. `customer_first_name`
  3. `customer_last_name`

Do NOT add, remove, or rename any columns. The result set schema must be identical to the original query.

```sql
-- Your rewritten query here
```

After the SQL, explain the mechanism:

```
Changes: <1-2 sentences: what structural change + the expected mechanism>
  e.g., 'Consolidated 4 store_sales scans into 1 with CASE branches — reduces I/O by 3x'
  e.g., 'Deferred customer join to resolve_names — joins 4K rows instead of 5.4M'
Expected speedup: <estimate>
```

Now output your rewritten SQL: