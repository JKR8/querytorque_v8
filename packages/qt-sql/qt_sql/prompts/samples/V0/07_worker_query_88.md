You are a SQL rewrite engine for DuckDB v1.4.3. Follow the Target DAG structure below. Your job is to write correct, executable SQL for each node — not to decide whether to restructure. Preserve exact semantic equivalence (same rows, same columns, same ordering). Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN y/x END around a division, keep it — even when a WHERE clause makes the zero case unreachable. Guards prevent silent breakage if filters change upstream. Strip benchmark comments (-- start query, -- end query) from your output.

DuckDB specifics: columnar storage (SELECT only needed columns). CTEs referenced once are typically inlined; CTEs referenced multiple times may be materialized. FILTER clause is native (`COUNT(*) FILTER (WHERE cond)`). Predicate pushdown stops at UNION ALL boundaries and multi-level CTE references.

## Semantic Contract (MUST preserve)

This query counts store sales by 8 consecutive half-hour time windows (8:30-12:30) at store 'ese' for households matching 3 specific (hd_dep_count, hd_vehicle_count) conditions. Returns a single row with 8 COUNT columns. The 8 subqueries are independent — same fact table, same dim filters, different time_dim conditions. Cross-join semantics: each count is independent.

## Target DAG + Node Contracts

Build your rewrite following this CTE structure. Each node's OUTPUT list is exhaustive — your SQL must produce exactly those columns.

TARGET_DAG:
  filtered_store -> sales_with_time
  filtered_hd -> sales_with_time
  time_ranges -> sales_with_time -> final_counts

NODE_CONTRACTS:
  filtered_store:
    FROM: store
    WHERE: s_store_name = 'ese'
    OUTPUT: s_store_sk
    EXPECTED_ROWS: ~1-5
    CONSUMERS: sales_with_time
  filtered_hd:
    FROM: household_demographics
    WHERE: (hd_dep_count = -1 AND hd_vehicle_count <= 1)
       OR (hd_dep_count = 4 AND hd_vehicle_count <= 6)
       OR (hd_dep_count = 3 AND hd_vehicle_count <= 5)
    OUTPUT: hd_demo_sk
    EXPECTED_ROWS: ~1200
    CONSUMERS: sales_with_time
  time_ranges:
    FROM: time_dim
    WHERE: (t_hour BETWEEN 8 AND 12)
    SELECT: t_time_sk, CASE WHEN t_hour=8 AND t_minute>=30 THEN 1 WHEN t_hour=9 AND t_minute<30 THEN 2 ... WHEN t_hour=12 AND t_minute<30 THEN 8 END AS time_window
    OUTPUT: t_time_sk, time_window
    EXPECTED_ROWS: ~240 (8 half-hour windows x ~30 per window)
    CONSUMERS: sales_with_time
  sales_with_time:
    FROM: store_sales JOIN filtered_store JOIN filtered_hd JOIN time_ranges
    JOIN: ss_store_sk = s_store_sk, ss_hdemo_sk = hd_demo_sk, ss_sold_time_sk = t_time_sk
    OUTPUT: time_window
    EXPECTED_ROWS: ~10K-50K
    CONSUMERS: final_counts
  final_counts:
    FROM: sales_with_time
    AGGREGATE: COUNT(CASE WHEN time_window = N THEN 1 END) for N=1..8
    OUTPUT: h8_30_to_9, h9_to_9_30, h9_30_to_10, h10_to_10_30, h10_30_to_11, h11_to_11_30, h11_30_to_12, h12_to_12_30
    EXPECTED_ROWS: 1

## Hazard Flags (avoid these specific risks)

- The 8 subqueries use different OR conditions on household_demographics for different time windows. VERIFY: all 8 subqueries actually share the same hd filters — they do (all use the same 3 OR branches). The time window is the only differentiator.
- Do NOT use FILTER clause with COUNT — use COUNT(CASE WHEN ... THEN 1 END) for maximum DuckDB compatibility.
- Preserve exact literal values: hd_dep_count = -1, hd_dep_count = 4, hd_dep_count = 3, hd_vehicle_count <= -1+2, <= 4+2, <= 3+2.

## Regression Warnings (observed failures on similar queries)

None applicable — Q88's 8 independent subqueries have no cross-CTE dependencies that could cause the known regression patterns.

## Constraints (analyst-filtered for this query)

- LITERAL_PRESERVATION: hd_dep_count = -1, hd_dep_count = 4, hd_dep_count = 3 and corresponding hd_vehicle_count bounds must be preserved exactly.
- SEMANTIC_EQUIVALENCE: Each COUNT must produce identical results — no row duplication from joins.
- COMPLETE_OUTPUT: All 8 output columns with exact aliases.
- CTE_COLUMN_COMPLETENESS: Every CTE must pass through all columns needed by downstream consumers.

## Example Adaptation Notes

For each example: what to apply to your rewrite, and what to ignore.

dimension_cte_isolate:
  APPLY: Pre-filter household_demographics and store into CTEs. Q88 joins these 8 times each — pre-filtering from 7.2K/1K rows to ~1200/5 rows eliminates repeated hash probes.
  IGNORE: The date_dim isolation from the original example — Q88 uses time_dim, not date_dim.

date_cte_isolate:
  APPLY: Same pattern but for time_dim. Add CASE-based time window classification during the CTE so downstream only needs the window ID.
  IGNORE: The d_year filter pattern — Q88 uses t_hour/t_minute.

shared_dimension_multi_channel:
  APPLY: Shared dimension CTEs across all 8 time windows. Each window shares the same store + household filters.
  IGNORE: The multi-channel (store/web/catalog) structure — Q88 is single-channel (store_sales only).

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

### 3. dimension_cte_isolate (1.93x)

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

## Original SQL

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

## Rewrite Checklist (must pass before final SQL)

- Follow every node in `TARGET_DAG` and produce each `NODE_CONTRACT` output column exactly.
- Keep all semantic invariants from `Semantic Contract` and `Constraints` (including join/null behavior).
- Preserve all literals and the exact final output schema/order.
- Apply `Hazard Flags` and `Regression Warnings` as hard guards against known failure modes.

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