You are a SQL rewrite engine for DuckDB. Rewrite the query for maximum execution speed while preserving exact semantic equivalence (same rows, same columns, same ordering). If you cannot improve performance, return the original unchanged.

DuckDB specifics: columnar storage (SELECT only needed columns), CTEs inlined by default (no materialization fence), FILTER clause native (`COUNT(*) FILTER (WHERE cond)`), predicate pushdown stops at multi-level CTEs/window/UNION ALL.

## Semantic Contract (MUST preserve)

This query finds customers whose web-channel payment variability grew faster year-over-year than their store-channel variability. Intersection semantics: customers must have sales in BOTH channels (store and web) in BOTH years (1999 and 2000). This is enforced by the 4-way inner join — any rewrite must preserve this intersection. STDDEV_SAMP returns NULL for single-row groups. The year_total > 0 filter excludes NULL and zero-stddev customers. Output: 3 columns, ordered by first_name, customer_id, last_name. LIMIT 100.

## Target DAG + Node Contracts

Build your rewrite following this CTE structure. Each node's OUTPUT list is exhaustive — your SQL must produce exactly those columns.

TARGET_DAG:
  filtered_dates -> store_1999 -+
  filtered_dates -> store_2000 -+
  filtered_dates -> web_1999   -+-> main_query
  filtered_dates -> web_2000   -+

NODE_CONTRACTS:
  filtered_dates:
    FROM: date_dim
    WHERE: d_year IN (1999, 2000)
    OUTPUT: d_date_sk, d_year
    CONSUMERS: store_1999, store_2000, web_1999, web_2000
  store_1999:
    FROM: store_sales JOIN customer JOIN filtered_dates
    JOIN: c_customer_sk = ss_customer_sk, ss_sold_date_sk = d_date_sk
    WHERE: d_year = 1999
    GROUP BY: c_customer_id, c_first_name, c_last_name
    AGGREGATE: STDDEV_SAMP(ss_net_paid) AS year_total
    OUTPUT: customer_id, customer_first_name, customer_last_name, year_total
    CONSUMERS: main_query
  [store_2000, web_1999, web_2000: same pattern, different table/year]
  main_query:
    FROM: store_1999 JOIN store_2000 JOIN web_1999 JOIN web_2000
    JOIN: all on customer_id (INNER — preserves intersection semantics)
    WHERE: store_1999.year_total > 0 AND web_1999.year_total > 0
           AND (web_2000/web_1999) > (store_2000/store_1999)
    OUTPUT: customer_id, customer_first_name, customer_last_name
    ORDER BY: customer_first_name, customer_id, customer_last_name
    LIMIT: 100

## Hazard Flags (avoid these specific risks)

- STDDEV_SAMP(ss_net_paid) FILTER (WHERE d_year = 1999) computed over a combined 1999+2000 group IS NOT EQUIVALENT to STDDEV_SAMP computed over only 1999 rows. The group membership changes the variance. You MUST either: (a) GROUP BY d_year first then pivot, or (b) pre-filter fact rows to the target year before aggregation.
- Do NOT merge the 4 channel-year CTEs into fewer CTEs. Each must filter to its specific year BEFORE aggregation to preserve STDDEV_SAMP semantics.

## Constraints (analyst-filtered for this query)

- union_cte_split_must_replace: If splitting a UNION into CTEs, the UNION in the main query MUST be replaced with references to the new CTEs
- or_to_union_limit: Limit OR-to-UNION to 3 or fewer branches to avoid multiplying fact table scans
- decorrelate_must_filter_first: When decorrelating, apply dimension filters before the main join to reduce probe-side cardinality

## Why These Examples Match

date_cte_isolate (4.00x on Q6): Q74 joins date_dim 4 times with d_year filters. Pre-filtering date_dim from 73K to ~730 rows eliminates 4 full hash-join probes. Same pattern: date join -> date CTE -> probe reduction.

dimension_cte_isolate (1.93x on Q26): Q74 also joins customer 4 times. While customer can't be pre-filtered (no WHERE on customer), the date CTE pattern reduces probe-side cardinality which shrinks the customer join input.

## Reference Examples

Pattern reference only — do not copy table/column names or literals.

### 1. union_cte_split (1.36x)

**Principle:** CTE Specialization: when a generic CTE is scanned multiple times with different filters (e.g., by year), split it into specialized CTEs that embed the filter in their definition. Each specialized CTE processes only its relevant subset, eliminating redundant scans.

**BEFORE (slow):**
```sql
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
```

**AFTER (fast):**
[year_1998_dates]:
```sql
SELECT d_date_sk, d_week_seq, d_day_name FROM date_dim WHERE d_year = 1998
```
[year_1999_dates]:
```sql
SELECT d_date_sk, d_week_seq, d_day_name FROM date_dim WHERE d_year = 1999
```
[wswscs_1998]:
```sql
SELECT d_week_seq, SUM(CASE WHEN d_day_name='Sunday' THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN d_day_name='Monday' THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN d_day_name='Tuesday' THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN d_day_name='Wednesday' THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN d_day_name='Thursday' THEN sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN d_day_name='Friday' THEN sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN d_day_name='Saturday' THEN sales_price ELSE NULL END) AS sat_sales FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales) wscs JOIN year_1998_dates ON d_date_sk = sold_date_sk GROUP BY d_week_seq
```
[wswscs_1999]:
```sql
SELECT d_week_seq, SUM(CASE WHEN d_day_name='Sunday' THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN d_day_name='Monday' THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN d_day_name='Tuesday' THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN d_day_name='Wednesday' THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN d_day_name='Thursday' THEN sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN d_day_name='Friday' THEN sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN d_day_name='Saturday' THEN sales_price ELSE NULL END) AS sat_sales FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales) wscs JOIN year_1999_dates ON d_date_sk = sold_date_sk GROUP BY d_week_seq
```
[main_query]:
```sql
SELECT y.d_week_seq AS d_week_seq1, ROUND(y.sun_sales / NULLIF(z.sun_sales, 0), 2), ROUND(y.mon_sales / NULLIF(z.mon_sales, 0), 2), ROUND(y.tue_sales / NULLIF(z.tue_sales, 0), 2), ROUND(y.wed_sales / NULLIF(z.wed_sales, 0), 2), ROUND(y.thu_sales / NULLIF(z.thu_sales, 0), 2), ROUND(y.fri_sales / NULLIF(z.fri_sales, 0), 2), ROUND(y.sat_sales / NULLIF(z.sat_sales, 0), 2) FROM wswscs_1998 y JOIN wswscs_1999 z ON y.d_week_seq = z.d_week_seq - 53 ORDER BY y.d_week_seq
```

### 2. intersect_to_exists (1.83x)

**Principle:** Semi-Join Short-Circuit: replace INTERSECT with EXISTS to avoid full materialization and sorting. INTERSECT must compute complete result sets before intersecting; EXISTS stops at the first match per row, enabling semi-join optimizations.

**BEFORE (slow):**
```sql
with  cross_items as
 (select i_item_sk ss_item_sk
 from item,
 (select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from store_sales
     ,item iss
     ,date_dim d1
 where ss_item_sk = iss.i_item_sk
   and ss_sold_date_sk = d1.d_date_sk
   and d1.d_year between 2000 AND 2000 + 2
 intersect 
 select ics.i_brand_id
     ,ics.i_class_id
     ,ics.i_category_id
 from catalog_sales
     ,item ics
     ,date_dim d2
 where cs_item_sk = ics.i_item_sk
   and cs_sold_date_sk = d2.d_date_sk
   and d2.d_year between 2000 AND 2000 + 2
 intersect
 select iws.i_brand_id
     ,iws.i_class_id
     ,iws.i_category_id
 from web_sales
     ,item iws
     ,date_dim d3
 where ws_item_sk = iws.i_item_sk
   and ws_sold_date_sk = d3.d_date_sk
   and d3.d_year between 2000 AND 2000 + 2)
 where i_brand_id = brand_id
      and i_class_id = class_id
      and i_category_id = category_id
),
 avg_sales as
 (select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales
           ,date_dim
       where ss_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2
       union all 
       select cs_quantity quantity 
             ,cs_list_price list_price
       from catalog_sales
           ,date_dim
       where cs_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2 
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from web_sales
           ,date_dim
       where ws_sold_date_sk = d_date_sk
         and d_year between 2000 and 2000 + 2) x)
  select channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from(
       select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
       from store_sales
           ,item
           ,date_dim
       where ss_item_sk in (select ss_item_sk from cross_items)
         and ss_item_sk = i_item_sk
         and ss_sold_date_sk = d_date_sk
         and d_year = 2000+2 
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
       union all
       select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
       from catalog_sales
           ,item
           ,date_dim
       where cs_item_sk in (select ss_item_sk from cross_items)
         and cs_item_sk = i_item_sk
         and cs_sold_date_sk = d_date_sk
         and d_year = 2000+2 
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
       union all
       select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
       from web_sales
           ,item
           ,date_dim
       where ws_item_sk in (select ss_item_sk from cross_items)
         and ws_item_sk = i_item_sk
         and ws_sold_date_sk = d_date_sk
         and d_year = 2000+2
         and d_moy = 11
       group by i_brand_id,i_class_id,i_category_id
       having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
 ) y
 group by rollup (channel, i_brand_id,i_class_id,i_category_id)
 order by channel,i_brand_id,i_class_id,i_category_id
 LIMIT 100;
```

**AFTER (fast):**
[cross_items_flat]:
```sql
SELECT i.i_item_sk AS ss_item_sk FROM item i WHERE EXISTS (SELECT 1 FROM store_sales, item iss, date_dim d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 1999 AND 2001 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS (SELECT 1 FROM catalog_sales, item ics, date_dim d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 1999 AND 2001 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS (SELECT 1 FROM web_sales, item iws, date_dim d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 1999 AND 2001 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id)
```
[main_query]:
```sql
SELECT ... FROM ... WHERE i_item_sk IN (SELECT ss_item_sk FROM cross_items_flat) ...
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