You are a SQL rewrite engine for DuckDB. Follow the Target DAG structure below. Your job is to write correct, executable SQL for each node — not to decide whether to restructure. Preserve exact semantic equivalence (same rows, same columns, same ordering).

DuckDB specifics: columnar storage (SELECT only needed columns). CTEs referenced once are typically inlined; CTEs referenced multiple times may be materialized. FILTER clause is native (`COUNT(*) FILTER (WHERE cond)`). Predicate pushdown stops at UNION ALL boundaries and multi-level CTE references.

## Semantic Contract (MUST preserve)

This query finds customers whose web-channel payment variability grew faster year-over-year than their store-channel variability. Intersection semantics: customers must have sales in BOTH channels (store and web) in BOTH years (1999 and 2000). This is enforced by the 4-way inner join — any rewrite must preserve this intersection. STDDEV_SAMP returns NULL for single-row groups. The year_total > 0 filter excludes NULL and zero-stddev customers. Output: 3 columns, ordered by first_name, customer_id, last_name. LIMIT 100.

## Target DAG + Node Contracts

Build your rewrite following this CTE structure. Each node's OUTPUT list is exhaustive — your SQL must produce exactly those columns.

TARGET_DAG:
  filtered_dates -> store_agg -+
  filtered_dates -> web_agg   -+-> compare_ratios -> resolve_names

NODE_CONTRACTS:
  filtered_dates:
    FROM: date_dim
    WHERE: d_year IN (1999, 1999 + 1)
    OUTPUT: d_date_sk, d_year
    EXPECTED_ROWS: ~730
    CONSUMERS: store_agg, web_agg
  store_agg:
    FROM: store_sales JOIN filtered_dates
    JOIN: ss_sold_date_sk = d_date_sk
    GROUP BY: ss_customer_sk, d_year
    AGGREGATE: STDDEV_SAMP(ss_net_paid) AS year_total
    OUTPUT: ss_customer_sk, d_year, year_total
    EXPECTED_ROWS: ~600K (300K per year)
    CONSUMERS: compare_ratios
  web_agg:
    FROM: web_sales JOIN filtered_dates
    JOIN: ws_sold_date_sk = d_date_sk
    GROUP BY: ws_bill_customer_sk, d_year
    AGGREGATE: STDDEV_SAMP(ws_net_paid) AS year_total
    OUTPUT: ws_bill_customer_sk, d_year, year_total
    EXPECTED_ROWS: ~200K (100K per year)
    CONSUMERS: compare_ratios
  compare_ratios:
    FROM: store_agg s1 JOIN store_agg s2 JOIN web_agg w1 JOIN web_agg w2
    JOIN: s1.ss_customer_sk = s2.ss_customer_sk = w1.ws_bill_customer_sk = w2.ws_bill_customer_sk
    WHERE: s1.d_year = 1999 AND s2.d_year = 1999 + 1
           AND w1.d_year = 1999 AND w2.d_year = 1999 + 1
           AND s1.year_total > 0 AND w1.year_total > 0
           AND (w2.year_total / w1.year_total) > (s2.year_total / s1.year_total)
    NOTE: The original uses CASE WHEN ... > 0 guards around divisions.
          These are redundant given the > 0 WHERE filters. Either form is correct.
    OUTPUT: ss_customer_sk
    EXPECTED_ROWS: ~4K
    CONSUMERS: resolve_names
  resolve_names:
    FROM: compare_ratios JOIN customer
    JOIN: ss_customer_sk = c_customer_sk
    OUTPUT: c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name
    EXPECTED_ROWS: ~4K -> 100 after ORDER BY + LIMIT
    ORDER BY: customer_first_name, customer_id, customer_last_name
    LIMIT: 100

## Hazard Flags (avoid these specific risks)

- STDDEV_SAMP(ss_net_paid) FILTER (WHERE d_year = 1999) computed over a combined 1999+2000 group IS NOT EQUIVALENT to STDDEV_SAMP computed over only 1999 rows. The group membership changes the variance. The target DAG avoids this: store_agg and web_agg GROUP BY d_year, so each group is naturally partitioned by year. STDDEV_SAMP is computed correctly per-partition. The compare_ratios CTE then filters to the specific year via WHERE d_year = 1999.
- Do NOT include a year_total CTE. The original UNION ALL CTE is fully replaced by store_agg + web_agg. Including both causes 0.68x regression.
- The customer join is deferred to resolve_names (joins ~4K rows, not 5.4M). Do NOT join customer in store_agg or web_agg — use ss_customer_sk/ws_bill_customer_sk as the join key throughout.

## Regression Warnings (observed failures on similar queries)

1. regression_q74_pushdown (0.68x):
   CAUSE: Created year-specific CTEs but KEPT the original year_total UNION CTE. DuckDB materialized both, causing redundant computation.
   RULE: Your rewrite must NOT include a year_total CTE. The 4 channel×year CTEs REPLACE it entirely.

## Constraints (analyst-filtered for this query)

- REMOVE_REPLACED_CTES: Your rewrite must NOT include a year_total CTE. The 4 channel×year CTEs REPLACE it entirely. Keeping both caused 0.68x regression on Q74.
- CTE_COLUMN_COMPLETENESS: Each CTE's SELECT must include ALL columns referenced by downstream consumers. Check: c_customer_sk flows through all CTEs to resolve_names.
- LITERAL_PRESERVATION: d_year IN (1999, 1999+1) must be preserved exactly. Do not substitute computed values.

## Why These Examples Match

date_cte_isolate (4.00x on Q6): Q74 joins date_dim 4 times with d_year filters. Pre-filtering date_dim from 73K to ~730 rows eliminates 4 full hash-join probes. Same pattern: date join -> date CTE -> probe reduction.

shared_dimension_multi_channel (1.30x on Q56): Q74 has two channels (store_sales, web_sales) that both join date_dim with the same filter. Shared dimension extraction into a single filtered_dates CTE avoids redundant dimension scans — exactly what our target DAG does.

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

## Original SQL

```sql
-- start query 74 in stream 0 using template query74.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as year
       ,stddev_samp(ss_net_paid) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
   and d_year in (1999,1999+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,d_year as year
       ,stddev_samp(ws_net_paid) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
   and d_year in (1999,1999+1)
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,d_year
         )
  select
        t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name
 from year_total t_s_firstyear
     ,year_total t_s_secyear
     ,year_total t_w_firstyear
     ,year_total t_w_secyear
 where t_s_secyear.customer_id = t_s_firstyear.customer_id
         and t_s_firstyear.customer_id = t_w_secyear.customer_id
         and t_s_firstyear.customer_id = t_w_firstyear.customer_id
         and t_s_firstyear.sale_type = 's'
         and t_w_firstyear.sale_type = 'w'
         and t_s_secyear.sale_type = 's'
         and t_w_secyear.sale_type = 'w'
         and t_s_firstyear.year = 1999
         and t_s_secyear.year = 1999+1
         and t_w_firstyear.year = 1999
         and t_w_secyear.year = 1999+1
         and t_s_firstyear.year_total > 0
         and t_w_firstyear.year_total > 0
         and case when t_w_firstyear.year_total > 0 then t_w_secyear.year_total / t_w_firstyear.year_total else null end
           > case when t_s_firstyear.year_total > 0 then t_s_secyear.year_total / t_s_firstyear.year_total else null end
 order by 2,1,3
 LIMIT 100;

-- end query 74 in stream 0 using template query74.tpl
```

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

After the SQL, briefly explain what you changed:

```
Changes: <1-2 sentence summary of the rewrite>
Expected speedup: <estimate>
```

Now output your rewritten SQL: