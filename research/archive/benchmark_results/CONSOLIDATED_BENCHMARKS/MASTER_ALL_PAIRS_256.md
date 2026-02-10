# Master Before/After Pairs - All Benchmarks (256 Total)

**Date**: 2026-02-05  
**Total Pairs**: 256  
**Sources**: benchmark_v2 (88), Kimi Q1-Q99 (99), V2 Standard (68), Archive (1)

---

## Summary by Source

| Source | Count |
|--------|-------|
| benchmark_v2 | 88 |
| Kimi Q1-Q30 | 30 |
| Kimi Q31-Q99 | 69 |
| V2_Standard_Iter1 | 17 |
| V2_Standard_Iter2 | 14 |
| V2_Standard_Iter3 | 13 |
| V2_Standard_Iter4 | 12 |
| V2_Standard_Iter5 | 12 |
| Archive | 1 |
| **TOTAL** | **256** |

---

## All Pairs (256 total)

### 1. benchmark_v2 - Q1

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 1 in stream 0 using template query1.tpl
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100;

-- end query 1 in stream 0 using template query1.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_returns AS (SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk WHERE d_year = 2000), customer_total_return AS (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk, sr_store_sk), store_avg_return AS (SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_threshold FROM customer_total_return GROUP BY ctr_store_sk)
SELECT c_customer_id FROM customer_total_return AS ctr1 JOIN store ON s_store_sk = ctr1.ctr_store_sk JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk JOIN store_avg_return AS sar ON ctr1.ctr_store_sk = sar.ctr_store_sk WHERE s_state = 'SD' AND ctr1.ctr_total_return > sar.avg_threshold ORDER BY c_customer_id LIMIT 100
```

---

### 2. benchmark_v2 - Q10

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 10 in stream 0 using template query10.tpl
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Storey County','Marquette County','Warren County','Cochran County','Kandiyohi County') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 1 and 1+3) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 ANd 1+3) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 and 1+3))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
    ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH date_filter AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4), filtered_store_sales AS (SELECT ss_customer_sk FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk), filtered_web_sales AS (SELECT ws_bill_customer_sk FROM web_sales JOIN date_filter ON ws_sold_date_sk = d_date_sk), filtered_catalog_sales AS (SELECT cs_ship_customer_sk FROM catalog_sales JOIN date_filter ON cs_sold_date_sk = d_date_sk), customer_address_filtered AS (SELECT ca_address_sk FROM customer_address WHERE ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County'))
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3, cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5, cd_dep_college_count, COUNT(*) AS cnt6 FROM customer AS c JOIN customer_address_filtered AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk WHERE EXISTS(SELECT 1 FROM filtered_store_sales AS ss WHERE ss.ss_customer_sk = c.c_customer_sk) AND (EXISTS(SELECT 1 FROM filtered_web_sales AS ws WHERE ws.ws_bill_customer_sk = c.c_customer_sk) OR EXISTS(SELECT 1 FROM filtered_catalog_sales AS cs WHERE cs.cs_ship_customer_sk = c.c_customer_sk)) GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed...[truncated]
```

---

### 3. benchmark_v2 - Q13

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 13 in stream 0 using template query13.tpl
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Unknown'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'College'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = '4 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('SD', 'KS', 'MI')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MO', 'ND', 'CO')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('NH', 'OH', 'TX')
  and ss_net_profit between 50 and 250  
     ))
;

-- ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_store AS (SELECT s_store_sk FROM store), branch_1 AS (SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk JOIN customer_address ON ca_address_sk = ss_addr_sk WHERE cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('SD', 'KS', 'MI') AND ss_net_profit BETWEEN 100 AND 200), branch_2 AS (SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk JOIN customer_address ON ca_address_sk = ss_addr_sk WHERE cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('MO', 'ND', 'CO') AND ss_net_profit BETWEEN 150 AND 300), branch_3 AS (SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_store ON s_store_sk = ss_sto...[truncated]
```

---

### 4. benchmark_v2 - Q16

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 16 in stream 0 using template query16.tpl
select 
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
where
    d_date between '2002-4-01' and 
           (cast('2002-4-01' as date) + INTERVAL 60 DAY)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'WV'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Ziebach County','Luce County','Richland County','Daviess County',
                  'Barrow County'
)
and exists (select *
            from catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
 LIMIT 100;

-- end query 16 in stream 0 using template query16.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_call_center AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), filtered_customer_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), multi_warehouse_orders AS (SELECT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), returned_orders AS (SELECT DISTINCT cr_order_number FROM catalog_returns), filtered_catalog_sales AS (SELECT cs_order_number, cs_ext_ship_cost, cs_net_profit FROM catalog_sales AS cs WHERE cs.cs_ship_date_sk IN (SELECT d_date_sk FROM filtered_dates) AND cs.cs_ship_addr_sk IN (SELECT ca_address_sk FROM filtered_customer_address) AND cs.cs_call_center_sk IN (SELECT cc_call_center_sk FROM filtered_call_center))
SELECT COUNT(DISTINCT fcs.cs_order_number) AS "order count", SUM(fcs.cs_ext_ship_cost) AS "total shipping cost", SUM(fcs.cs_net_profit) AS "total net profit" FROM filtered_catalog_sales AS fcs JOIN multi_warehouse_orders AS mwo ON fcs.cs_order_number = mwo.cs_order_number WHERE NOT EXISTS(SELECT 1 FROM returned_orders AS ro WHERE fcs.cs_order_number = ro.cr_order_number) ORDER BY COUNT(DISTINCT fcs.cs_order_number) LIMIT 100
```

---

### 5. benchmark_v2 - Q18

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 18 in stream 0 using template query18.tpl
select i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as decimal(12,2))) agg1,
        avg( cast(cs_list_price as decimal(12,2))) agg2,
        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
        avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        avg( cast(c_birth_year as decimal(12,2))) agg6,
        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
 from catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'F' and 
       cd1.cd_education_status = 'Advanced Degree' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (10,7,8,4,1,2) and
       d_year = 1998 and
       ca_state in ('WA','GA','NC'
                   ,'ME','WY','OK','IN')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by ca_country,
        ca_state, 
        ca_county,
	i_item_id
 LIMIT 100;

-- end query 18 in stream 0 using template query18.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1998), filtered_cd1 AS (SELECT cd_demo_sk, cd_dep_count FROM customer_demographics WHERE cd_gender = 'F' AND cd_education_status = 'Advanced Degree'), filtered_customer AS (SELECT c_customer_sk, c_current_cdemo_sk, c_current_addr_sk, c_birth_year FROM customer WHERE c_birth_month IN (10, 7, 8, 4, 1, 2)), filtered_ca AS (SELECT ca_address_sk, ca_country, ca_state, ca_county FROM customer_address WHERE ca_state IN ('WA', 'GA', 'NC', 'ME', 'WY', 'OK', 'IN')), prefiltered_sales AS (SELECT cs_item_sk, cs_bill_cdemo_sk, cs_bill_customer_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price, cs_net_profit FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk)
SELECT i_item_id, fca.ca_country, fca.ca_state, fca.ca_county, AVG(CAST(cs_quantity AS DECIMAL(12, 2))) AS agg1, AVG(CAST(cs_list_price AS DECIMAL(12, 2))) AS agg2, AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2))) AS agg3, AVG(CAST(cs_sales_price AS DECIMAL(12, 2))) AS agg4, AVG(CAST(cs_net_profit AS DECIMAL(12, 2))) AS agg5, AVG(CAST(fc.c_birth_year AS DECIMAL(12, 2))) AS agg6, AVG(CAST(fcd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7 FROM prefiltered_sales AS ps JOIN filtered_cd1 AS fcd1 ON ps.cs_bill_cdemo_sk = fcd1.cd_demo_sk JOIN filtered_customer AS fc ON ps.cs_bill_customer_sk = fc.c_customer_sk JOIN filtered_ca AS fca ON fc.c_current_addr_sk = fca.ca_address_sk JOIN customer_demographics AS cd2 ON fc.c_current_cdemo_sk = cd2.cd_demo_sk ...[truncated]
```

---

### 6. benchmark_v2 - Q19

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 19 in stream 0 using template query19.tpl
select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=2
   and d_moy=12
   and d_year=1999
   and ss_customer_sk = c_customer_sk 
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
   and ss_store_sk = s_store_sk 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
 LIMIT 100;

-- end query 19 in stream 0 using template query19.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 1999), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id, i_manufact FROM item WHERE i_manager_id = 2), filtered_stores AS (SELECT s_store_sk, s_zip FROM store), filtered_customers AS (SELECT c_customer_sk, c_current_addr_sk FROM customer), filtered_addresses AS (SELECT ca_address_sk, ca_zip FROM customer_address), qualified_sales AS (SELECT ss_item_sk, ss_customer_sk, ss_ext_sales_price, ss_store_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), joined_data AS (SELECT i.i_brand_id, i.i_brand, i.i_manufact_id, i.i_manufact, qs.ss_ext_sales_price, ca.ca_zip, s.s_zip FROM qualified_sales AS qs JOIN filtered_items AS i ON qs.ss_item_sk = i.i_item_sk JOIN filtered_customers AS c ON qs.ss_customer_sk = c.c_customer_sk JOIN filtered_addresses AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN filtered_stores AS s ON qs.ss_store_sk = s.s_store_sk WHERE SUBSTRING(ca.ca_zip, 1, 5) <> SUBSTRING(s.s_zip, 1, 5))
SELECT i_brand_id AS brand_id, i_brand AS brand, i_manufact_id, i_manufact, SUM(ss_ext_sales_price) AS ext_price FROM joined_data GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact LIMIT 100
```

---

### 7. benchmark_v2 - Q2

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 2 in stream 0 using template query2.tpl
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales 
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as 
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,...[truncated]
```

#### AFTER (Optimized)
```sql
WITH wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs_1998 AS (SELECT d_week_seq, SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE NULL END) AS sat_sales FROM wscs, date_dim WHERE d_date_sk = sold_date_sk AND d_year = 1998 GROUP BY d_week_seq), wswscs_1999 AS (SELECT d_week_seq, SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE NULL END) ...[truncated]
```

---

### 8. benchmark_v2 - Q20

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 20 in stream 0 using template query20.tpl
select i_item_id
       ,i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in ('Shoes', 'Books', 'Women')
   and cs_sold_date_sk = d_date_sk
 and d_date between cast('2002-01-26' as date) 
 				and (cast('2002-01-26' as date) + INTERVAL 30 DAY)
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
 LIMIT 100;

-- end query 20 in stream 0 using template query20.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) AND (CAST('2002-01-26' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, i_item_sk FROM item WHERE i_category IN ('Shoes', 'Books', 'Women')), filtered_sales AS (SELECT cs_ext_sales_price, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN filtered_items ON cs_item_sk = i_item_sk)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(cs_ext_sales_price) AS itemrevenue, SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio FROM filtered_sales GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 9. benchmark_v2 - Q21

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 21 in stream 0 using template query21.tpl
select *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('2002-02-27' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('2002-02-27' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('2002-02-27' as date) - INTERVAL 30 DAY)
                    and (cast ('2002-02-27' as date) + INTERVAL 30 DAY)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 LIMIT 100;

-- end query 21 in stream 0 using template query21.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_date BETWEEN (CAST('2002-02-27' AS DATE) - INTERVAL '30' DAY) AND (CAST('2002-02-27' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id FROM item WHERE i_current_price BETWEEN 0.99 AND 1.49), filtered_inventory AS (SELECT inv_warehouse_sk, inv_item_sk, inv_quantity_on_hand, inv_date_sk FROM inventory), main_aggregation AS (SELECT w_warehouse_name, filtered_items.i_item_id, SUM(CASE WHEN (CAST(filtered_dates.d_date AS DATE) < CAST('2002-02-27' AS DATE)) THEN filtered_inventory.inv_quantity_on_hand ELSE 0 END) AS inv_before, SUM(CASE WHEN (CAST(filtered_dates.d_date AS DATE) >= CAST('2002-02-27' AS DATE)) THEN filtered_inventory.inv_quantity_on_hand ELSE 0 END) AS inv_after FROM filtered_inventory JOIN filtered_dates ON filtered_inventory.inv_date_sk = filtered_dates.d_date_sk JOIN filtered_items ON filtered_inventory.inv_item_sk = filtered_items.i_item_sk JOIN warehouse ON filtered_inventory.inv_warehouse_sk = warehouse.w_warehouse_sk GROUP BY w_warehouse_name, filtered_items.i_item_id)
SELECT * FROM main_aggregation WHERE (CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0 ORDER BY w_warehouse_name, i_item_id LIMIT 100
```

---

### 10. benchmark_v2 - Q22

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 22 in stream 0 using template query22.tpl
select i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           ,date_dim
           ,item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1188 and 1188 + 11
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
 LIMIT 100;

-- end query 22 in stream 0 using template query22.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1188 AND 1188 + 11), filtered_inventory AS (SELECT inv_item_sk, inv_quantity_on_hand FROM inventory JOIN filtered_dates ON inv_date_sk = d_date_sk)
SELECT i_product_name, i_brand, i_class, i_category, AVG(inv_quantity_on_hand) AS qoh FROM filtered_inventory JOIN item ON inv_item_sk = i_item_sk GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category) ORDER BY qoh, i_product_name, i_brand, i_class, i_category LIMIT 100
```

---

### 11. benchmark_v2 - Q23

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim 
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3) 
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales 
           ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH may_2000_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy = 5), frequent_ss_items AS (SELECT SUBSTRING(i_item_desc, 1, 30) AS itemdesc, i_item_sk AS item_sk, d_date AS solddate, COUNT(*) AS cnt FROM store_sales, date_dim, item WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk AND d_year IN (2000, 2001, 2002, 2003) GROUP BY SUBSTRING(i_item_desc, 1, 30), i_item_sk, d_date HAVING COUNT(*) > 4), max_store_sales AS (SELECT MAX(csales) AS tpcds_cmax FROM (SELECT c_customer_sk, SUM(ss_quantity * ss_sales_price) AS csales FROM store_sales, customer, date_dim WHERE ss_customer_sk = c_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (2000, 2001, 2002, 2003) GROUP BY c_customer_sk)), best_ss_customer AS (SELECT c_customer_sk, SUM(ss_quantity * ss_sales_price) AS ssales FROM store_sales, customer WHERE ss_customer_sk = c_customer_sk GROUP BY c_customer_sk HAVING SUM(ss_quantity * ss_sales_price) > (95 / 100.0) * (SELECT * FROM max_store_sales))
SELECT SUM(sales) FROM (SELECT cs_quantity * cs_list_price AS sales FROM catalog_sales JOIN may_2000_dates ON cs_sold_date_sk = d_date_sk WHERE cs_item_sk IN (SELECT item_sk FROM frequent_ss_items) AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer) UNION ALL SELECT ws_quantity * ws_list_price AS sales FROM web_sales JOIN may_2000_dates ON ws_sold_date_sk = d_date_sk WHERE ws_item_sk IN (SELECT item_sk FROM frequent_ss_items) AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM b...[truncated]
```

---

### 12. benchmark_v2 - Q24

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 24 in stream 0 using template query24.tpl
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ,customer
    ,customer_address
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_country <> upper(ca_country)
  and s_zip = ca_zip
and s_market_id=8
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = 'beige'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                                 from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name
;
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_ssales AS (SELECT c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size, SUM(ss_net_profit) AS netpaid FROM store_sales, store_returns, store, item, customer, customer_address WHERE ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_customer_sk = c_customer_sk AND ss_item_sk = i_item_sk AND ss_store_sk = s_store_sk AND c_current_addr_sk = ca_address_sk AND c_birth_country <> UPPER(ca_country) AND s_zip = ca_zip AND s_market_id = 8 AND i_color = 'beige' GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size), avg_threshold AS (SELECT 0.05 * AVG(netpaid) AS threshold FROM filtered_ssales), ssales AS (SELECT c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size, SUM(ss_net_profit) AS netpaid FROM store_sales, store_returns, store, item, customer, customer_address WHERE ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk AND ss_customer_sk = c_customer_sk AND ss_item_sk = i_item_sk AND ss_store_sk = s_store_sk AND c_current_addr_sk = ca_address_sk AND c_birth_country <> UPPER(ca_country) AND s_zip = ca_zip AND s_market_id = 8 GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, i_color, i_current_price, i_manager_id, i_units, i_size)
SELECT c_last_name, c_first_name, s_store_name, SUM(netpaid) AS paid FROM filtered_ssales GROUP ...[truncated]
```

---

### 13. benchmark_v2 - Q25

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 25 in stream 0 using template query25.tpl
select 
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 ,sum(ss_net_profit) as store_sales_profit
 ,sum(sr_net_loss) as store_returns_loss
 ,sum(cs_net_profit) as catalog_sales_profit
 from
 store_sales
 ,store_returns
 ,catalog_sales
 ,date_dim d1
 ,date_dim d2
 ,date_dim d3
 ,store
 ,item
 where
 d1.d_moy = 4
 and d1.d_year = 2000
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk = ss_item_sk
 and s_store_sk = ss_store_sk
 and ss_customer_sk = sr_customer_sk
 and ss_item_sk = sr_item_sk
 and ss_ticket_number = sr_ticket_number
 and sr_returned_date_sk = d2.d_date_sk
 and d2.d_moy               between 4 and  10
 and d2.d_year              = 2000
 and sr_customer_sk = cs_bill_customer_sk
 and sr_item_sk = cs_item_sk
 and cs_sold_date_sk = d3.d_date_sk
 and d3.d_moy               between 4 and  10 
 and d3.d_year              = 2000
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 LIMIT 100;

-- end query 25 in stream 0 using template query25.tpl

```

#### AFTER (Optimized)
```sql
WITH d1_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 2000), d2_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000), d3_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000), store_sales_filtered AS (SELECT ss_item_sk, ss_store_sk, ss_customer_sk, ss_ticket_number, ss_net_profit FROM store_sales JOIN d1_filtered ON ss_sold_date_sk = d1_filtered.d_date_sk), store_returns_filtered AS (SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_net_loss FROM store_returns JOIN d2_filtered ON sr_returned_date_sk = d2_filtered.d_date_sk), catalog_sales_filtered AS (SELECT cs_bill_customer_sk, cs_item_sk, cs_net_profit FROM catalog_sales JOIN d3_filtered ON cs_sold_date_sk = d3_filtered.d_date_sk)
SELECT i_item_id, i_item_desc, s_store_id, s_store_name, SUM(ss.ss_net_profit) AS store_sales_profit, SUM(sr.sr_net_loss) AS store_returns_loss, SUM(cs.cs_net_profit) AS catalog_sales_profit FROM store_sales_filtered AS ss JOIN item AS i ON i.i_item_sk = ss.ss_item_sk JOIN store AS s ON s.s_store_sk = ss.ss_store_sk JOIN store_returns_filtered AS sr ON ss.ss_customer_sk = sr.sr_customer_sk AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number JOIN catalog_sales_filtered AS cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk AND sr.sr_item_sk = cs.cs_item_sk GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name ORDER BY i_item_id, i_item_desc, s_store_id, s_store_...[truncated]
```

---

### 14. benchmark_v2 - Q26

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 26 in stream 0 using template query26.tpl
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

-- end query 26 in stream 0 using template query26.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_customer_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'M' AND cd_marital_status = 'S' AND cd_education_status = 'Unknown'), channel_email_branch AS (SELECT cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN filtered_customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk JOIN promotion ON cs_promo_sk = p_promo_sk WHERE p_channel_email = 'N'), channel_event_branch AS (SELECT cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN filtered_customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk JOIN promotion ON cs_promo_sk = p_promo_sk WHERE p_channel_event = 'N' AND (p_channel_email <> 'N' OR p_channel_email IS NULL)), combined_sales AS (SELECT cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price FROM channel_email_branch UNION ALL SELECT cs_item_sk, cs_quantity, cs_list_price, cs_coupon_amt, cs_sales_price FROM channel_event_branch)
SELECT i_item_id, AVG(cs_quantity) AS agg1, AVG(cs_list_price) AS agg2, AVG(cs_coupon_amt) AS agg3, AVG(cs_sales_price) AS agg4 FROM combined_sales JOIN item ON cs_item_sk = i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 15. benchmark_v2 - Q27

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 27 in stream 0 using template query27.tpl
select i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'D' and
       cd_education_status = 'Secondary' and
       d_year = 1999 and
       s_state in ('MO','AL', 'MI', 'TN', 'LA', 'SC')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 LIMIT 100;

-- end query 27 in stream 0 using template query27.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999), filtered_customer_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'D' AND cd_education_status = 'Secondary'), filtered_stores AS (SELECT s_store_sk, s_state FROM store WHERE s_state IN ('MO', 'AL', 'MI', 'TN', 'LA', 'SC')), filtered_sales AS (SELECT ss_item_sk, ss_store_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_customer_demographics ON ss_cdemo_sk = cd_demo_sk)
SELECT i_item_id, s.s_state, GROUPING(s.s_state) AS g_state, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM filtered_sales AS fs JOIN item AS i ON fs.ss_item_sk = i.i_item_sk JOIN filtered_stores AS s ON fs.ss_store_sk = s.s_store_sk GROUP BY ROLLUP (i_item_id, s.s_state) ORDER BY i_item_id, s.s_state LIMIT 100
```

---

### 16. benchmark_v2 - Q28

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 28 in stream 0 using template query28.tpl
select *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 131 and 131+10 
             or ss_coupon_amt between 16798 and 16798+1000
             or ss_wholesale_cost between 25 and 25+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 145 and 145+10
          or ss_coupon_amt between 14792 and 14792+1000
          or ss_wholesale_cost between 46 and 46+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 150 and 150+10
          or ss_coupon_amt between 6600 and 6600+1000
          or ss_wholesale_cost between 9 and 9+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 91 and 91+10
          or ss_coupon_amt between 13493 and 13493+1000
          or ss_wholesale_cost between 36 and 36+20)) B4,
...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_sales AS (SELECT ss_quantity, ss_list_price, ss_coupon_amt, ss_wholesale_cost FROM store_sales WHERE ss_quantity BETWEEN 0 AND 30)
SELECT AVG(CASE WHEN ss_quantity BETWEEN 0 AND 5 AND (ss_list_price BETWEEN 131 AND 141 OR ss_coupon_amt BETWEEN 16798 AND 17798 OR ss_wholesale_cost BETWEEN 25 AND 45) THEN ss_list_price END) AS B1_LP, COUNT(CASE WHEN ss_quantity BETWEEN 0 AND 5 AND (ss_list_price BETWEEN 131 AND 141 OR ss_coupon_amt BETWEEN 16798 AND 17798 OR ss_wholesale_cost BETWEEN 25 AND 45) THEN ss_list_price END) AS B1_CNT, COUNT(DISTINCT CASE WHEN ss_quantity BETWEEN 0 AND 5 AND (ss_list_price BETWEEN 131 AND 141 OR ss_coupon_amt BETWEEN 16798 AND 17798 OR ss_wholesale_cost BETWEEN 25 AND 45) THEN ss_list_price END) AS B1_CNTD, AVG(CASE WHEN ss_quantity BETWEEN 6 AND 10 AND (ss_list_price BETWEEN 145 AND 155 OR ss_coupon_amt BETWEEN 14792 AND 15792 OR ss_wholesale_cost BETWEEN 46 AND 66) THEN ss_list_price END) AS B2_LP, COUNT(CASE WHEN ss_quantity BETWEEN 6 AND 10 AND (ss_list_price BETWEEN 145 AND 155 OR ss_coupon_amt BETWEEN 14792 AND 15792 OR ss_wholesale_cost BETWEEN 46 AND 66) THEN ss_list_price END) AS B2_CNT, COUNT(DISTINCT CASE WHEN ss_quantity BETWEEN 6 AND 10 AND (ss_list_price BETWEEN 145 AND 155 OR ss_coupon_amt BETWEEN 14792 AND 15792 OR ss_wholesale_cost BETWEEN 46 AND 66) THEN ss_list_price END) AS B2_CNTD, AVG(CASE WHEN ss_quantity BETWEEN 11 AND 15 AND (ss_list_price BETWEEN 150 AND 160 OR ss_coupon_amt BETWEEN 6600 AND 7600 OR ss_wholesale...[truncated]
```

---

### 17. benchmark_v2 - Q29

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 29 in stream 0 using template query29.tpl
select  
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,avg(ss_quantity)        as store_sales_quantity
    ,avg(sr_return_quantity) as store_returns_quantity
    ,avg(cs_quantity)        as catalog_sales_quantity
 from
    store_sales
   ,store_returns
   ,catalog_sales
   ,date_dim             d1
   ,date_dim             d2
   ,date_dim             d3
   ,store
   ,item
 where
     d1.d_moy               = 4 
 and d1.d_year              = 1999
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 4 and  4 + 3 
 and d2.d_year              = 1999
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk     
 and d3.d_year              in (1999,1999+1,1999+2)
 group by
    i_item_id
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 order by
    i_item_id 
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 LIMIT 100;

-- end query 29 in stream 0 using template query29.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_d1 AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 1999), filtered_d2 AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 7 AND d_year = 1999), filtered_d3 AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000, 2001)), ss_filtered AS (SELECT ss_customer_sk, ss_item_sk, ss_ticket_number, ss_quantity, ss_store_sk FROM store_sales JOIN filtered_d1 ON ss_sold_date_sk = d_date_sk), sr_filtered AS (SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN filtered_d2 ON sr_returned_date_sk = d_date_sk), cs_filtered AS (SELECT cs_bill_customer_sk, cs_item_sk, cs_quantity FROM catalog_sales JOIN filtered_d3 ON cs_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, s_store_id, s_store_name, AVG(ss.ss_quantity) AS store_sales_quantity, AVG(sr.sr_return_quantity) AS store_returns_quantity, AVG(cs.cs_quantity) AS catalog_sales_quantity FROM ss_filtered AS ss JOIN store ON ss.ss_store_sk = s_store_sk JOIN item ON ss.ss_item_sk = i_item_sk JOIN sr_filtered AS sr ON ss.ss_customer_sk = sr.sr_customer_sk AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number JOIN cs_filtered AS cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk AND sr.sr_item_sk = cs.cs_item_sk GROUP BY i_item_id, i_item_desc, s_store_id, s_store_name ORDER BY i_item_id, i_item_desc, s_store_id, s_store_name LIMIT 100
```

---

### 18. benchmark_v2 - Q30

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 30 in stream 0 using template query30.tpl
with customer_total_return as
 (select wr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(wr_return_amt) as ctr_total_return
 from web_returns
     ,date_dim
     ,customer_address
 where wr_returned_date_sk = d_date_sk 
   and d_year =2002
   and wr_returning_addr_sk = ca_address_sk 
 group by wr_returning_customer_sk
         ,ca_state)
  select c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date_sk,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'IN'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                  ,c_last_review_date_sk,ctr_total_return
 LIMIT 100;

-- end query 30 in stream 0 using template query30.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_returns AS (SELECT wr_returning_customer_sk, ca_state, wr_return_amt FROM web_returns JOIN date_dim ON wr_returned_date_sk = d_date_sk JOIN customer_address ON wr_returning_addr_sk = ca_address_sk WHERE d_year = 2002), customer_total_return AS (SELECT wr_returning_customer_sk AS ctr_customer_sk, ca_state AS ctr_state, SUM(wr_return_amt) AS ctr_total_return FROM filtered_returns GROUP BY wr_returning_customer_sk, ca_state), state_avg_return AS (SELECT ctr_state, AVG(ctr_total_return) * 1.2 AS avg_threshold FROM customer_total_return GROUP BY ctr_state)
SELECT c_customer_id, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date_sk, ctr1.ctr_total_return FROM customer_total_return AS ctr1 JOIN customer_address AS ca ON ca.ca_address_sk = c_current_addr_sk AND ca.ca_state = 'IN' JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk JOIN state_avg_return AS sar ON ctr1.ctr_state = sar.ctr_state WHERE ctr1.ctr_total_return > sar.avg_threshold ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_day, c_birth_month, c_birth_year, c_birth_country, c_login, c_email_address, c_last_review_date_sk, ctr_total_return LIMIT 100
```

---

### 19. benchmark_v2 - Q31

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 31 in stream 0 using template query31.tpl
with ss as
 (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales
 from store_sales,date_dim,customer_address
 where ss_sold_date_sk = d_date_sk
  and ss_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year),
 ws as
 (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales
 from web_sales,date_dim,customer_address
 where ws_sold_date_sk = d_date_sk
  and ws_bill_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year)
 select 
        ss1.ca_county
       ,ss1.d_year
       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
 from
        ss ss1
       ,ss ss2
       ,ss ss3
       ,ws ws1
       ,ws ws2
       ,ws ws3
 where
    ss1.d_qoy = 1
    and ss1.d_year = 2000
    and ss1.ca_county = ss2.ca_county
    and ss2.d_qoy = 2
    and ss2.d_year = 2000
 and ss2.ca_county = ss3.ca_county
    and ss3.d_qoy = 3
    and ss3.d_year = 2000
    and ss1.ca_county = ws1.ca_county
    and ws1.d_qoy = 1
    and ws1.d_year = 2000
    and ws1.ca_county = ws2.ca_county
    and ws2.d_qoy = 2
    and ws2.d_year = 2000
    and ws1.ca_county = ws3.ca_county
    and ws3.d_qoy = 3
    and ws3.d_year =2000
    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end 
       > case when ss1.store_sales > 0 then ss2.st...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)), filtered_store_sales AS (SELECT ca_county, d_qoy, d_year, SUM(ss_ext_sales_price) AS store_sales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN customer_address ON ss_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year), filtered_web_sales AS (SELECT ca_county, d_qoy, d_year, SUM(ws_ext_sales_price) AS web_sales FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer_address ON ws_bill_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year), ss AS (SELECT ca_county, d_qoy, d_year, SUM(ss_ext_sales_price) AS store_sales FROM store_sales, date_dim, customer_address WHERE ss_sold_date_sk = d_date_sk AND ss_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year), ws AS (SELECT ca_county, d_qoy, d_year, SUM(ws_ext_sales_price) AS web_sales FROM web_sales, date_dim, customer_address WHERE ws_sold_date_sk = d_date_sk AND ws_bill_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year)
SELECT ss1.ca_county, ss1.d_year, ws2.web_sales / ws1.web_sales AS web_q1_q2_increase, ss2.store_sales / ss1.store_sales AS store_q1_q2_increase, ws3.web_sales / ws2.web_sales AS web_q2_q3_increase, ss3.store_sales / ss2.store_sales AS store_q2_q3_increase FROM filtered_store_sales AS ss1, filtered_store_sales AS ss2, filtered_store_sales AS ss3, filtered_web_sales AS ws1, filtered_web_sales AS ws2, filtered_web_sales AS ws3 WHERE ss1.d_qoy ...[truncated]
```

---

### 20. benchmark_v2 - Q32

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 32 in stream 0 using template query32.tpl
select sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 29
and i_item_sk = cs_item_sk 
and d_date between '1999-01-07' and 
        (cast('1999-01-07' as date) + INTERVAL 90 DAY)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1999-01-07' and
                             (cast('1999-01-07' as date) + INTERVAL 90 DAY)
          and d_date_sk = cs_sold_date_sk 
      )
 LIMIT 100;

-- end query 32 in stream 0 using template query32.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '1999-01-07' AND (CAST('1999-01-07' AS DATE) + INTERVAL '90' DAY)), item_avg_discount AS (SELECT cs_item_sk, AVG(cs_ext_discount_amt) AS avg_discount FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk GROUP BY cs_item_sk)
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount" FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN item ON i_item_sk = cs_item_sk JOIN item_avg_discount AS iad ON cs_item_sk = iad.cs_item_sk WHERE i_manufact_id = 29 AND cs_ext_discount_amt > 1.3 * iad.avg_discount LIMIT 100
```

---

### 21. benchmark_v2 - Q33

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 33 in stream 0 using template query33.tpl
with ss as (
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2002
 and     d_moy                   = 1
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 cs as (
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2002
 and     d_moy                   = 1
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 ws as (
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_y...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002 AND d_moy = 1), filtered_items AS (SELECT DISTINCT i_manufact_id FROM item WHERE i_category IN ('Home')), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_gmt_offset = -5), ss AS (SELECT i.i_manufact_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_addresses ON ss_addr_sk = ca_address_sk JOIN item AS i ON ss_item_sk = i.i_item_sk JOIN filtered_items AS fi ON i.i_manufact_id = fi.i_manufact_id GROUP BY i.i_manufact_id), cs AS (SELECT i.i_manufact_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN filtered_addresses ON cs_bill_addr_sk = ca_address_sk JOIN item AS i ON cs_item_sk = i.i_item_sk JOIN filtered_items AS fi ON i.i_manufact_id = fi.i_manufact_id GROUP BY i.i_manufact_id), ws AS (SELECT i.i_manufact_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_addresses ON ws_bill_addr_sk = ca_address_sk JOIN item AS i ON ws_item_sk = i.i_item_sk JOIN filtered_items AS fi ON i.i_manufact_id = fi.i_manufact_id GROUP BY i.i_manufact_id)
SELECT i_manufact_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_manufact_id ORDER BY total_sales LIMIT 100
```

---

### 22. benchmark_v2 - Q34

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 34 in stream 0 using template query34.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '1001-5000' or
         household_demographics.hd_buy_potential = '0-500')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0 
	then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
	else null 
	end)  > 1.2
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_county in ('Ziebach County','Daviess County','Walker County','Richland County',
                           'Barrow County','Franklin Parish','Williamson County','Luce County')
    group by ss_ticket_number,ss_customer_sk) dn,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;

-- end query 34 in stream 0 using template query34.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_store AS (SELECT s_store_sk FROM store WHERE s_county IN ('Ziebach County', 'Daviess County', 'Walker County', 'Richland County', 'Barrow County', 'Franklin Parish', 'Williamson County', 'Luce County')), filtered_dates_dom1_3 AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1998, 1999, 2000) AND d_dom BETWEEN 1 AND 3), filtered_dates_dom25_28 AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1998, 1999, 2000) AND d_dom BETWEEN 25 AND 28), filtered_hd_1001_5000 AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_buy_potential = '1001-5000' AND hd_vehicle_count > 0 AND (hd_dep_count / hd_vehicle_count) > 1.2), filtered_hd_0_500 AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_buy_potential = '0-500' AND hd_vehicle_count > 0 AND (hd_dep_count / hd_vehicle_count) > 1.2), dn_union AS (SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt FROM store_sales JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk JOIN filtered_dates_dom1_3 ON store_sales.ss_sold_date_sk = filtered_dates_dom1_3.d_date_sk JOIN filtered_hd_1001_5000 ON store_sales.ss_hdemo_sk = filtered_hd_1001_5000.hd_demo_sk GROUP BY ss_ticket_number, ss_customer_sk UNION ALL SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt FROM store_sales JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk JOIN filtered_dates_dom1_3 ON store_sales.ss_sold_date_sk = filtered_dates_dom1_3.d_date_sk JOIN filtered_hd_0_500 ON store_sales.ss_hdemo_sk ...[truncated]
```

---

### 23. benchmark_v2 - Q35

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 35 in stream 0 using template query35.tpl
select  
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  max(cd_dep_count),
  sum(cd_dep_count),
  max(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  max(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  max(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  max(cd_dep_college_count),
  sum(cd_dep_college_count),
  max(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_qoy < 4) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4))
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
      ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4), store_customers AS (SELECT DISTINCT ss_customer_sk AS customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), web_customers AS (SELECT DISTINCT ws_bill_customer_sk AS customer_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), catalog_customers AS (SELECT DISTINCT cs_ship_customer_sk AS customer_sk FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk)
SELECT ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count, COUNT(*) AS cnt1, MAX(cd.cd_dep_count), SUM(cd.cd_dep_count), MAX(cd.cd_dep_count), cd.cd_dep_employed_count, COUNT(*) AS cnt2, MAX(cd.cd_dep_employed_count), SUM(cd.cd_dep_employed_count), MAX(cd.cd_dep_employed_count), cd.cd_dep_college_count, COUNT(*) AS cnt3, MAX(cd.cd_dep_college_count), SUM(cd.cd_dep_college_count), MAX(cd.cd_dep_college_count) FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk JOIN store_customers AS sc ON c.c_customer_sk = sc.customer_sk WHERE EXISTS(SELECT 1 FROM web_customers AS wc WHERE wc.customer_sk = c.c_customer_sk) OR EXISTS(SELECT 1 FROM catalog_customers AS cc WHERE cc.customer_sk = c.c_customer_sk) GROUP BY ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count, cd.cd_dep_employed_count, cd.cd_dep_college_count ORDER BY ca.ca_state, cd.cd_gender, cd.cd...[truncated]
```

---

### 24. benchmark_v2 - Q36

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 36 in stream 0 using template query36.tpl
select 
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,item
   ,store
 where
    d1.d_year = 2002 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('SD','TN','GA','SC',
                 'MO','AL','MI','OH')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
 LIMIT 100;

-- end query 36 in stream 0 using template query36.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_state IN ('SD', 'TN', 'GA', 'SC', 'MO', 'AL', 'MI', 'OH')), sales_aggregates AS (SELECT ss_item_sk, ss_store_sk, SUM(ss_net_profit) AS total_net_profit, SUM(ss_ext_sales_price) AS total_sales_price FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk GROUP BY ss_item_sk, ss_store_sk)
SELECT total_net_profit / NULLIF(total_sales_price, 0) AS gross_margin, i_category, i_class, GROUPING(i_category) + GROUPING(i_class) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY total_net_profit / NULLIF(total_sales_price, 0) ASC) AS rank_within_parent FROM sales_aggregates JOIN item ON ss_item_sk = i_item_sk GROUP BY ROLLUP (i_category, i_class) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category END, rank_within_parent LIMIT 100
```

---

### 25. benchmark_v2 - Q37

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 37 in stream 0 using template query37.tpl
select i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 45 and 45 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-02-21' as date) and (cast('1999-02-21' as date) + INTERVAL 60 DAY)
 and i_manufact_id in (856,707,1000,747)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 LIMIT 100;

-- end query 37 in stream 0 using template query37.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1999-02-21' AS DATE) AND (CAST('1999-02-21' AS DATE) + INTERVAL '60' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_current_price FROM item WHERE i_current_price BETWEEN 45 AND 75 AND i_manufact_id IN (856, 707, 1000, 747)), filtered_inventory AS (SELECT inv_item_sk FROM inventory JOIN filtered_dates ON inv_date_sk = d_date_sk WHERE inv_quantity_on_hand BETWEEN 100 AND 500), filtered_sales AS (SELECT DISTINCT cs_item_sk FROM catalog_sales)
SELECT i_item_id, i_item_desc, i_current_price FROM filtered_items AS fi WHERE EXISTS(SELECT 1 FROM filtered_inventory AS finv WHERE finv.inv_item_sk = fi.i_item_sk) AND EXISTS(SELECT 1 FROM filtered_sales AS fs WHERE fs.cs_item_sk = fi.i_item_sk) ORDER BY i_item_id LIMIT 100
```

---

### 26. benchmark_v2 - Q38

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 38 in stream 0 using template query38.tpl
select count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, date_dim, customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from web_sales, date_dim, customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
) hot_cust
 LIMIT 100;

-- end query 38 in stream 0 using template query38.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1183 AND 1183 + 11), store_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM store_sales JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk), catalog_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM catalog_sales JOIN filtered_dates ON catalog_sales.cs_sold_date_sk = filtered_dates.d_date_sk JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk), web_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM web_sales JOIN filtered_dates ON web_sales.ws_sold_date_sk = filtered_dates.d_date_sk JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk)
SELECT COUNT(*) FROM (SELECT * FROM store_customers INTERSECT SELECT * FROM catalog_customers INTERSECT SELECT * FROM web_customers) AS hot_cust LIMIT 100
```

---

### 27. benchmark_v2 - Q39

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 39 in stream 0 using template query39.tpl
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =1998
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
  ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_moy FROM date_dim WHERE d_year = 1998 AND d_moy IN (1, 2)), inv AS (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, STDDEV_SAMP(inv_quantity_on_hand) AS stdev, AVG(inv_quantity_on_hand) AS mean, CASE WHEN AVG(inv_quantity_on_hand) = 0 THEN NULL ELSE STDDEV_SAMP(inv_quantity_on_hand) / AVG(inv_quantity_on_hand) END AS cov FROM inventory JOIN item ON inv_item_sk = i_item_sk JOIN warehouse ON inv_warehouse_sk = w_warehouse_sk JOIN filtered_dates ON inv_date_sk = d_date_sk GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy HAVING CASE WHEN AVG(inv_quantity_on_hand) = 0 THEN 0 ELSE STDDEV_SAMP(inv_quantity_on_hand) / AVG(inv_quantity_on_hand) END > 1)
SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov FROM inv AS inv1, inv AS inv2 WHERE inv1.i_item_sk = inv2.i_item_sk AND inv1.w_warehouse_sk = inv2.w_warehouse_sk AND inv1.d_moy = 1 AND inv2.d_moy = 1 + 1 ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.d_moy, inv2.mean, inv2.cov
```

---

### 28. benchmark_v2 - Q40

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 40 in stream 0 using template query40.tpl
select 
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2001-04-02' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2001-04-02' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left outer join catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('2001-04-02' as date) - INTERVAL 30 DAY)
                and (cast ('2001-04-02' as date) + INTERVAL 30 DAY) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
 LIMIT 100;

-- end query 40 in stream 0 using template query40.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_date BETWEEN (CAST('2001-04-02' AS DATE) - INTERVAL '30' DAY) AND (CAST('2001-04-02' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id FROM item WHERE i_current_price BETWEEN 0.99 AND 1.49), filtered_sales AS (SELECT cs_sales_price, cs_order_number, cs_item_sk, cs_warehouse_sk FROM catalog_sales WHERE cs_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates) AND cs_item_sk IN (SELECT i_item_sk FROM filtered_items))
SELECT w_state, fi.i_item_id, SUM(CASE WHEN (fd.d_date < CAST('2001-04-02' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before, SUM(CASE WHEN (fd.d_date >= CAST('2001-04-02' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after FROM filtered_sales AS cs JOIN filtered_dates AS fd ON cs.cs_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON cs.cs_item_sk = fi.i_item_sk JOIN warehouse AS w ON cs.cs_warehouse_sk = w_warehouse_sk LEFT OUTER JOIN catalog_returns AS cr ON (cs.cs_order_number = cr.cr_order_number AND cs.cs_item_sk = cr.cr_item_sk) GROUP BY w_state, fi.i_item_id ORDER BY w_state, fi.i_item_id LIMIT 100
```

---

### 29. benchmark_v2 - Q41

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 41 in stream 0 using template query41.tpl
select distinct(i_product_name)
 from item i1
 where i_manufact_id between 748 and 748+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'gainsboro' or i_color = 'aquamarine') and 
        (i_units = 'Ounce' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'chiffon' or i_color = 'violet') and
        (i_units = 'Ton' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'chartreuse' or i_color = 'blue') and
        (i_units = 'Each' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'dodger') and
        (i_units = 'Bunch' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'tomato') and 
        (i_units = 'Tbl' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'almond' or i_color = 'lime') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'small'...[truncated]
```

#### AFTER (Optimized)
```sql
WITH matching_items AS (SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Women' AND (i_color = 'gainsboro' OR i_color = 'aquamarine') AND (i_units = 'Ounce' OR i_units = 'Dozen') AND (i_size = 'medium' OR i_size = 'economy')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Women' AND (i_color = 'chiffon' OR i_color = 'violet') AND (i_units = 'Ton' OR i_units = 'Pound') AND (i_size = 'extra large' OR i_size = 'small')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Men' AND (i_color = 'chartreuse' OR i_color = 'blue') AND (i_units = 'Each' OR i_units = 'Oz') AND (i_size = 'N/A' OR i_size = 'large')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Men' AND (i_color = 'tan' OR i_color = 'dodger') AND (i_units = 'Bunch' OR i_units = 'Tsp') AND (i_size = 'medium' OR i_size = 'economy')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Women' AND (i_color = 'blanched' OR i_color = 'tomato') AND (i_units = 'Tbl' OR i_units = 'Case') AND (i_size = 'medium' OR i_size = 'economy')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Women' AND (i_color = 'almond' OR i_color = 'lime') AND (i_units = 'Box' OR i_units = 'Dram') AND (i_size = 'extra large' OR i_size = 'small')) UNION ALL SELECT DISTINCT i_manufact FROM item WHERE (i_category = 'Men' AND (i_color = 'peru' OR i_color = 'saddle') AND (i_units = 'Pallet' OR i_units = 'Gram') AND (i_size = 'N/A' OR i_size = 'large')) UNION A...[truncated]
```

---

### 30. benchmark_v2 - Q42

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 42 in stream 0 using template query42.tpl
select dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,sum(ss_ext_sales_price)
 from 	date_dim dt
 	,store_sales
 	,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = item.i_item_sk
 	and item.i_manager_id = 1  	
 	and dt.d_moy=11
 	and dt.d_year=2002
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
 LIMIT 100;

-- end query 42 in stream 0 using template query42.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year = 2002 AND d_moy = 11), filtered_items AS (SELECT i_item_sk, i_category_id, i_category FROM item WHERE i_manager_id = 1)
SELECT dt.d_year, item.i_category_id, item.i_category, SUM(ss_ext_sales_price) FROM filtered_dates AS dt JOIN store_sales ON dt.d_date_sk = store_sales.ss_sold_date_sk JOIN filtered_items AS item ON store_sales.ss_item_sk = item.i_item_sk GROUP BY dt.d_year, item.i_category_id, item.i_category ORDER BY SUM(ss_ext_sales_price) DESC, dt.d_year, item.i_category_id, item.i_category LIMIT 100
```

---

### 31. benchmark_v2 - Q43

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 43 in stream 0 using template query43.tpl
select s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -5 and
       d_year = 2000 
 group by s_store_name, s_store_id
 order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 LIMIT 100;

-- end query 43 in stream 0 using template query43.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2000), filtered_stores AS (SELECT s_store_sk, s_store_name, s_store_id FROM store WHERE s_gmt_offset = -5)
SELECT s_store_name, s_store_id, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales FROM store_sales JOIN filtered_dates ON d_date_sk = ss_sold_date_sk JOIN filtered_stores ON s_store_sk = ss_store_sk GROUP BY s_store_name, s_store_id ORDER BY s_store_name, s_store_id, sun_sales, mon_sales, tue_sales, wed_sales, thu_sales, fri_sales, sat_sales LIMIT 100
```

---

### 32. benchmark_v2 - Q44

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 44 in stream 0 using template query44.tpl
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) de...[truncated]
```

#### AFTER (Optimized)
```sql
WITH store_avg_profit AS (SELECT AVG(ss_net_profit) AS store_avg_net_profit FROM store_sales WHERE ss_store_sk = 146 AND ss_addr_sk IS NULL GROUP BY ss_store_sk), item_performance AS (SELECT ss_item_sk AS item_sk, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_store_sk = 146 GROUP BY ss_item_sk HAVING AVG(ss_net_profit) > 0.9 * (SELECT store_avg_net_profit FROM store_avg_profit)), ascending_ranks AS (SELECT item_sk, RANK() OVER (ORDER BY avg_net_profit ASC) AS rnk FROM item_performance WHERE rnk < 11), descending_ranks AS (SELECT item_sk, RANK() OVER (ORDER BY avg_net_profit DESC) AS rnk FROM item_performance WHERE rnk < 11)
SELECT asceding.rnk, i1.i_product_name AS best_performing, i2.i_product_name AS worst_performing FROM ascending_ranks AS asceding JOIN descending_ranks AS descending ON asceding.rnk = descending.rnk JOIN item AS i1 ON asceding.item_sk = i1.i_item_sk JOIN item AS i2 ON descending.item_sk = i2.i_item_sk ORDER BY asceding.rnk LIMIT 100
```

---

### 33. benchmark_v2 - Q45

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 45 in stream 0 using template query45.tpl
select ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 LIMIT 100;

-- end query 45 in stream 0 using template query45.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 2 AND d_year = 2000), item_list AS (SELECT i_item_id FROM item WHERE i_item_sk IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)), zip_branch AS (SELECT ws_sales_price, ca_zip, ca_city FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk JOIN item ON ws_item_sk = i_item_sk WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792')), item_branch AS (SELECT ws_sales_price, ca_zip, ca_city FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk JOIN item ON ws_item_sk = i_item_sk JOIN item_list ON i_item_id = item_list.i_item_id)
SELECT ca_zip, ca_city, SUM(ws_sales_price) FROM (SELECT ws_sales_price, ca_zip, ca_city FROM zip_branch UNION ALL SELECT ws_sales_price, ca_zip, ca_city FROM item_branch) AS combined GROUP BY ca_zip, ca_city ORDER BY ca_zip, ca_city LIMIT 100
```

---

### 34. benchmark_v2 - Q46

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 46 in stream 0 using template query46.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics,customer_address 
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 6 or
         household_demographics.hd_vehicle_count= 0)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1999,1999+1,1999+2) 
    and store.s_city in ('Five Points','Centerville','Oak Grove','Fairview','Liberty') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
 LIMIT 100;

-- end query 46 in stream 0 using template query46.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_dow IN (6, 0) AND d_year IN (1999, 2000, 2001)), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_city IN ('Five Points', 'Centerville', 'Oak Grove', 'Fairview', 'Liberty')), sales_branch1 AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_coupon_amt, ss_net_profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_dep_count = 6), sales_branch2 AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_coupon_amt, ss_net_profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_vehicle_count = 0), combined_sales AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_coupon_amt, ss_net_profit FROM sales_branch1 UNION ALL SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_coupon_amt, ss_net_profit FROM sales_branch2), dn AS (SELECT ss_ticket_number, ss_customer_sk, ca_city AS bought_city, SUM(ss_coupon_amt) AS amt, SUM(ss_net_profit) AS profit FROM combined_sales JOIN customer_address ON ss_addr_sk = ca_address_sk GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city)
SELECT c_last_name, c_first_name, ca_city, bought_city, ss_ticket_number, amt, profit FROM dn JOIN customer ON ss_customer_sk = c_customer_sk JOIN customer_a...[truncated]
```

---

### 35. benchmark_v2 - Q47

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 47 in stream 0 using template query47.tpl
with v1 as(
 select i_category, i_brand,
        s_store_name, s_company_name,
        d_year, d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 2001 or
         ( d_year = 2001-1 and d_moy =12) or
         ( d_year = 2001+1 and d_moy =1)
       )
 group by i_category, i_brand,
          s_store_name, s_company_name,
          d_year, d_moy),
 v2 as(
 select v1.s_store_name
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1....[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 OR (d_year = 2000 AND d_moy = 12) OR (d_year = 2002 AND d_moy = 1)), sales_2001 AS (SELECT i_category, i_brand, s_store_name, s_company_name, d_year, d_moy, SUM(ss_sales_price) AS sum_sales FROM item, store_sales, date_dim, store WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 2001 GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy), sales_2000_dec AS (SELECT i_category, i_brand, s_store_name, s_company_name, d_year, d_moy, SUM(ss_sales_price) AS sum_sales FROM item, store_sales, date_dim, store WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 2000 AND d_moy = 12 GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy), sales_2002_jan AS (SELECT i_category, i_brand, s_store_name, s_company_name, d_year, d_moy, SUM(ss_sales_price) AS sum_sales FROM item, store_sales, date_dim, store WHERE ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND ss_store_sk = s_store_sk AND d_year = 2002 AND d_moy = 1 GROUP BY i_category, i_brand, s_store_name, s_company_name, d_year, d_moy), v1_union AS (SELECT * FROM sales_2001 UNION ALL SELECT * FROM sales_2000_dec UNION ALL SELECT * FROM sales_2002_jan), v1 AS (SELECT i_category, i_brand, s_store_name, s_company_name, d_year, d_moy, sum_sales, AVG(sum_sales) OVER (PARTITION BY i_category, i_brand, s_store_name, s_comp...[truncated]
```

---

### 36. benchmark_v2 - Q48

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 48 in stream 0 using template query48.tpl
select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'U'
   and 
   cd_education_status = 'Primary'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'W'
   and 
   cd_education_status = 'College'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = 'D'
   and 
   cd_education_status = '2 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MD', 'MN', 'IA')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('VA', 'IL', 'TX')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MI', 'WI', 'IN')
  and ss_net_profit between 50 and 25000 
  )
 )
;

-- end query 48 in stream 0 using template query48.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999), branch_1_1 AS (SELECT ss_quantity FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk AND cd_marital_status = 'U' AND cd_education_status = 'Primary' JOIN customer_address ON ss_addr_sk = ca_address_sk AND ca_country = 'United States' AND ca_state IN ('MD', 'MN', 'IA') WHERE ss_sales_price BETWEEN 100.00 AND 150.00 AND ss_net_profit BETWEEN 0 AND 2000), branch_1_2 AS (SELECT ss_quantity FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk AND cd_marital_status = 'U' AND cd_education_status = 'Primary' JOIN customer_address ON ss_addr_sk = ca_address_sk AND ca_country = 'United States' AND ca_state IN ('VA', 'IL', 'TX') WHERE ss_sales_price BETWEEN 100.00 AND 150.00 AND ss_net_profit BETWEEN 150 AND 3000), branch_1_3 AS (SELECT ss_quantity FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk AND cd_marital_status = 'U' AND cd_education_status = 'Primary' JOIN customer_address ON ss_addr_sk = ca_address_sk AND ca_country = 'United States' AND ca_state IN ('MI', 'WI', 'IN') WHERE ss_sales_price BETWEEN 100.00 AND 150.00 AND ss_net_profit BETWEEN 50 AND 25000), branch_2_1 AS (SELECT...[truncated]
```

---

### 37. benchmark_v2 - Q49

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 49 in stream 0 using template query49.tpl
select channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		 web_sales ws left outer join web_returns wr 
 			on (ws.ws_order_number = wr.wr_order_number and 
 			ws.ws_item_sk = wr.wr_item_sk)
                 ,date_dim
 		where 
 			wr.wr_return_amt > 10000 
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 1999
                         and d_moy = 12
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where 
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union
 select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() ove...[truncated]
```

#### AFTER (Optimized)
```sql
WITH date_filter AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 12), web_channel_base AS (SELECT 'web' AS channel, ws.ws_item_sk AS item, (CAST(SUM(COALESCE(wr.wr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_quantity, 0)) AS DECIMAL(15, 4))) AS return_ratio, (CAST(SUM(COALESCE(wr.wr_return_amt, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(ws.ws_net_paid, 0)) AS DECIMAL(15, 4))) AS currency_ratio FROM web_sales AS ws LEFT OUTER JOIN web_returns AS wr ON (ws.ws_order_number = wr.wr_order_number AND ws.ws_item_sk = wr.wr_item_sk) JOIN date_filter ON ws.ws_sold_date_sk = date_filter.d_date_sk WHERE wr.wr_return_amt > 10000 AND ws.ws_net_profit > 1 AND ws.ws_net_paid > 0 AND ws.ws_quantity > 0 GROUP BY ws.ws_item_sk), catalog_channel_base AS (SELECT 'catalog' AS channel, cs.cs_item_sk AS item, (CAST(SUM(COALESCE(cr.cr_return_quantity, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_quantity, 0)) AS DECIMAL(15, 4))) AS return_ratio, (CAST(SUM(COALESCE(cr.cr_return_amount, 0)) AS DECIMAL(15, 4)) / CAST(SUM(COALESCE(cs.cs_net_paid, 0)) AS DECIMAL(15, 4))) AS currency_ratio FROM catalog_sales AS cs LEFT OUTER JOIN catalog_returns AS cr ON (cs.cs_order_number = cr.cr_order_number AND cs.cs_item_sk = cr.cr_item_sk) JOIN date_filter ON cs.cs_sold_date_sk = date_filter.d_date_sk WHERE cr.cr_return_amount > 10000 AND cs.cs_net_profit > 1 AND cs.cs_net_paid > 0 AND cs.cs_quantity > 0 GROUP BY cs.cs_item_sk), store_channel_base AS (SELECT 'store' AS ch...[truncated]
```

---

### 38. benchmark_v2 - Q50

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 50 in stream 0 using template query50.tpl
select 
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and 
                 (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and 
                 (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
                 (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   store_sales
  ,store_returns
  ,store
  ,date_dim d1
  ,date_dim d2
where
    d2.d_year = 2001
and d2.d_moy  = 8
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
group by
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
order by s_store_name
        ,s_company_id
        ,s_street_number
        ,s_street_name
        ,s_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_d2 AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy = 8), sales_with_d1 AS (SELECT ss_ticket_number, ss_item_sk, ss_customer_sk, ss_store_sk, ss_sold_date_sk FROM store_sales), returns_with_filtered_d2 AS (SELECT sr_ticket_number, sr_item_sk, sr_customer_sk, sr_returned_date_sk FROM store_returns JOIN filtered_d2 ON sr_returned_date_sk = filtered_d2.d_date_sk)
SELECT s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_state, s_zip, SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 30) AND (sr_returned_date_sk - ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 60) AND (sr_returned_date_sk - ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 90) AND (sr_returned_date_sk - ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN (sr_returned_date_sk - ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days" FROM sales_with_d1 AS ss JOIN returns_with_filtered_d2 AS sr ON ss.ss_ticket_number = sr.sr_ticket_number AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_customer_sk = sr.sr_customer_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk GROUP BY s_store_name, s_company_id, s_street_number, s_street_name, s_street_type, s_suite_number, s_city, s_county, s_...[truncated]
```

---

### 39. benchmark_v2 - Q51

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 51 in stream 0 using template query51.tpl
WITH web_v1 as (
select
  ws_item_sk item_sk, d_date,
  sum(sum(ws_sales_price))
      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from web_sales
    ,date_dim
where ws_sold_date_sk=d_date_sk
  and d_month_seq between 1216 and 1216+11
  and ws_item_sk is not NULL
group by ws_item_sk, d_date),
store_v1 as (
select
  ss_item_sk item_sk, d_date,
  sum(sum(ss_sales_price))
      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from store_sales
    ,date_dim
where ss_sold_date_sk=d_date_sk
  and d_month_seq between 1216 and 1216+11
  and ss_item_sk is not NULL
group by ss_item_sk, d_date)
 select *
from (select item_sk
     ,d_date
     ,web_sales
     ,store_sales
     ,max(web_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max(store_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1216 AND 1216 + 11), web_v1 AS (SELECT ws_item_sk AS item_sk, d_date, SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cume_sales FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk WHERE NOT ws_item_sk IS NULL GROUP BY ws_item_sk, d_date), store_v1 AS (SELECT ss_item_sk AS item_sk, d_date, SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cume_sales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk WHERE NOT ss_item_sk IS NULL GROUP BY ss_item_sk, d_date)
SELECT * FROM (SELECT item_sk, d_date, web_sales, store_sales, MAX(web_sales) OVER (PARTITION BY item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS web_cumulative, MAX(store_sales) OVER (PARTITION BY item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS store_cumulative FROM (SELECT CASE WHEN NOT web.item_sk IS NULL THEN web.item_sk ELSE store.item_sk END AS item_sk, CASE WHEN NOT web.d_date IS NULL THEN web.d_date ELSE store.d_date END AS d_date, web.cume_sales AS web_sales, store.cume_sales AS store_sales FROM web_v1 AS web FULL OUTER JOIN store_v1 AS store ON (web.item_sk = store.item_sk AND web.d_date = store.d_date)) AS x) AS y WHERE web_cumulative > store_cumulative ORDER BY item_sk, d_date LIMIT 100
```

---

### 40. benchmark_v2 - Q52

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 52 in stream 0 using template query52.tpl
select dt.d_year
 	,item.i_brand_id brand_id
 	,item.i_brand brand
 	,sum(ss_ext_sales_price) ext_price
 from date_dim dt
     ,store_sales
     ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
    and store_sales.ss_item_sk = item.i_item_sk
    and item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=2002
 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 order by dt.d_year
 	,ext_price desc
 	,brand_id
 LIMIT 100;

-- end query 52 in stream 0 using template query52.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year = 2002 AND d_moy = 12)
SELECT fd.d_year, i.i_brand_id AS brand_id, i.i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk WHERE i.i_manager_id = 1 GROUP BY fd.d_year, i.i_brand, i.i_brand_id ORDER BY fd.d_year, ext_price DESC, brand_id LIMIT 100
```

---

### 41. benchmark_v2 - Q53

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 53 in stream 0 using template query53.tpl
select * from 
(select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else null end > 0.1
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
 LIMIT 100;

-- end query 53 in stream 0 using template query53.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_qoy FROM date_dim WHERE d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)), filtered_items_branch1 AS (SELECT i_item_sk, i_manufact_id FROM item WHERE i_category IN ('Books', 'Children', 'Electronics') AND i_class IN ('personal', 'portable', 'reference', 'self-help') AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')), filtered_items_branch2 AS (SELECT i_item_sk, i_manufact_id FROM item WHERE i_category IN ('Women', 'Music', 'Men') AND i_class IN ('accessories', 'classical', 'fragrances', 'pants') AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')), filtered_items AS (SELECT i_item_sk, i_manufact_id FROM filtered_items_branch1 UNION ALL SELECT i_item_sk, i_manufact_id FROM filtered_items_branch2), joined_sales AS (SELECT i.i_manufact_id, d.d_qoy, ss.ss_sales_price FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN filtered_items AS i ON ss.ss_item_sk = i.i_item_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk), grouped_sales AS (SELECT i_manufact_id, d_qoy, SUM(ss_sales_price) AS sum_sales, AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales FROM joined_sales GROUP BY i_manufact_id, d_qoy)
SELECT i_manufact_id, sum_sales, avg_quarterly_sales FROM grouped_sales WHERE CASE WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - ...[truncated]
```

---

### 42. benchmark_v2 - Q54

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 54 in stream 0 using template query54.tpl
with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Women'
         and i_class = 'maternity'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 5
         and d_year = 1998
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1998 and d_moy = 5)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1998 and d_moy = 5)
 group by c_cus...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_date_customers AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 5 AND d_year = 1998), filtered_item AS (SELECT i_item_sk FROM item WHERE i_category = 'Women' AND i_class = 'maternity'), filtered_catalog_sales AS (SELECT cs_sold_date_sk, cs_bill_customer_sk, cs_item_sk FROM catalog_sales), filtered_web_sales AS (SELECT ws_sold_date_sk, ws_bill_customer_sk, ws_item_sk FROM web_sales), cs_qualified AS (SELECT DISTINCT cs_bill_customer_sk AS customer_sk, c_current_addr_sk FROM filtered_catalog_sales JOIN filtered_date_customers ON cs_sold_date_sk = d_date_sk JOIN filtered_item ON cs_item_sk = i_item_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk), ws_qualified AS (SELECT DISTINCT ws_bill_customer_sk AS customer_sk, c_current_addr_sk FROM filtered_web_sales JOIN filtered_date_customers ON ws_sold_date_sk = d_date_sk JOIN filtered_item ON ws_item_sk = i_item_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk), my_customers AS (SELECT * FROM cs_qualified UNION ALL SELECT * FROM ws_qualified), base_month AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 1998 AND d_moy = 5), date_range AS (SELECT d_date_sk FROM date_dim CROSS JOIN base_month WHERE d_month_seq BETWEEN base_month.d_month_seq + 1 AND base_month.d_month_seq + 3), filtered_store_sales AS (SELECT ss_customer_sk, ss_ext_sales_price FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk), my_revenue AS (SELECT mc.c_customer_sk, SUM(ss_ext_sales_price) AS revenue FROM my_customer...[truncated]
```

---

### 43. benchmark_v2 - Q55

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 55 in stream 0 using template query55.tpl
select i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=100
 	and d_moy=12
 	and d_year=2000
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
 LIMIT 100;

-- end query 55 in stream 0 using template query55.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 2000), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manager_id = 100), filtered_sales AS (SELECT ss_item_sk, ss_ext_sales_price FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk)
SELECT i_brand_id AS brand_id, i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price FROM filtered_items JOIN filtered_sales ON i_item_sk = ss_item_sk GROUP BY i_brand, i_brand_id ORDER BY ext_price DESC, i_brand_id LIMIT 100
```

---

### 44. benchmark_v2 - Q56

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 56 in stream 0 using template query56.tpl
with ss as (
 select i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where i_item_id in (select
     i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 2
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 cs as (
 select i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 2
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 ws as (
 select i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_mo...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy = 2), filtered_items AS (SELECT i_item_id, i_item_sk FROM item WHERE i_color IN ('powder', 'green', 'cyan')), filtered_ca AS (SELECT ca_address_sk FROM customer_address WHERE ca_gmt_offset = -6), ss AS (SELECT i.i_item_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_items AS i ON ss_item_sk = i.i_item_sk JOIN filtered_ca ON ss_addr_sk = ca_address_sk GROUP BY i.i_item_id), ws AS (SELECT i.i_item_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_items AS i ON ws_item_sk = i.i_item_sk JOIN filtered_ca ON ws_bill_addr_sk = ca_address_sk GROUP BY i.i_item_id), cs AS (SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_color IN ('powder', 'green', 'cyan')) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 2 AND cs_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -6 GROUP BY i_item_id)
SELECT i_item_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_item_id ORDER BY total_sales, i_item_id LIMIT 100
```

---

### 45. benchmark_v2 - Q57

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 57 in stream 0 using template query57.tpl
with v1 as(
 select i_category, i_brand,
        cc_name,
        d_year, d_moy,
        sum(cs_sales_price) sum_sales,
        avg(sum(cs_sales_price)) over
          (partition by i_category, i_brand,
                     cc_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     cc_name
           order by d_year, d_moy) rn
 from item, catalog_sales, date_dim, call_center
 where cs_item_sk = i_item_sk and
       cs_sold_date_sk = d_date_sk and
       cc_call_center_sk= cs_call_center_sk and
       (
         d_year = 1999 or
         ( d_year = 1999-1 and d_moy =12) or
         ( d_year = 1999+1 and d_moy =1)
       )
 group by i_category, i_brand,
          cc_name , d_year, d_moy),
 v2 as(
 select v1.i_brand
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1. cc_name = v1_lag. cc_name and
       v1. cc_name = v1_lead. cc_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1)
  select *
 from v2
 where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999 OR (d_year = 1998 AND d_moy = 12) OR (d_year = 2000 AND d_moy = 1)), filtered_catalog_sales AS (SELECT cs_item_sk, cs_call_center_sk, cs_sales_price, d_year, d_moy FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk), v1 AS (SELECT i_category, i_brand, cc_name, d_year, d_moy, SUM(cs_sales_price) AS sum_sales, AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales, RANK() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn FROM item, filtered_catalog_sales, call_center WHERE cs_item_sk = i_item_sk AND cc_call_center_sk = cs_call_center_sk GROUP BY i_category, i_brand, cc_name, d_year, d_moy), v2 AS (SELECT v1.i_brand, v1.d_year, v1.avg_monthly_sales, v1.sum_sales, v1_lag.sum_sales AS psum, v1_lead.sum_sales AS nsum FROM v1, v1 AS v1_lag, v1 AS v1_lead WHERE v1.i_category = v1_lag.i_category AND v1.i_category = v1_lead.i_category AND v1.i_brand = v1_lag.i_brand AND v1.i_brand = v1_lead.i_brand AND v1.cc_name = v1_lag.cc_name AND v1.cc_name = v1_lead.cc_name AND v1.rn = v1_lag.rn + 1 AND v1.rn = v1_lead.rn - 1)
SELECT * FROM v2 WHERE d_year = 1999 AND avg_monthly_sales > 0 AND CASE WHEN avg_monthly_sales > 0 THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales ELSE NULL END > 0.1 ORDER BY sum_sales - avg_monthly_sales, nsum LIMIT 100
```

---

### 46. benchmark_v2 - Q58

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 58 in stream 0 using template query58.tpl
with ss_items as
 (select i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev 
 from store_sales
     ,item
     ,date_dim
 where ss_item_sk = i_item_sk
   and d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq 
                                      from date_dim
                                      where d_date = '2001-03-24'))
   and ss_sold_date_sk   = d_date_sk
 group by i_item_id),
 cs_items as
 (select i_item_id item_id
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      ,item
      ,date_dim
 where cs_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq 
                                      from date_dim
                                      where d_date = '2001-03-24'))
  and  cs_sold_date_sk = d_date_sk
 group by i_item_id),
 ws_items as
 (select i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
 where ws_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq =(select d_week_seq 
                                     from date_dim
                                     where d_date = '2001-03-24'))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id)
  select ss_items.item_id
       ,ss_item_rev
       ,ss_item_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH target_week AS (SELECT d_week_seq FROM date_dim WHERE d_date = '2001-03-24'), target_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM target_week)), ss_items AS (SELECT i_item_id AS item_id, SUM(ss_ext_sales_price) AS ss_item_rev FROM store_sales JOIN item ON ss_item_sk = i_item_sk JOIN target_dates ON ss_sold_date_sk = d_date_sk GROUP BY i_item_id), ws_items AS (SELECT i_item_id AS item_id, SUM(ws_ext_sales_price) AS ws_item_rev FROM web_sales JOIN item ON ws_item_sk = i_item_sk JOIN target_dates ON ws_sold_date_sk = d_date_sk GROUP BY i_item_id), cs_items AS (SELECT i_item_id AS item_id, SUM(cs_ext_sales_price) AS cs_item_rev FROM catalog_sales, item, date_dim WHERE cs_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2001-03-24')) AND cs_sold_date_sk = d_date_sk GROUP BY i_item_id)
SELECT ss_items.item_id, ss_item_rev, ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev, cs_item_rev, cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev, ws_item_rev, ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev, (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average FROM ss_items, cs_items, ws_items WHERE ss_items.item_id = cs_items.item_id AND ss_items.item_id = ws_items.item_id AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item_rev AND ss_item_rev BETWEEN 0.9 * ws_item...[truncated]
```

---

### 47. benchmark_v2 - Q59

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 59 in stream 0 using template query59.tpl
with wss as 
 (select d_week_seq,
        ss_store_sk,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from store_sales,date_dim
 where d_date_sk = ss_sold_date_sk
 group by d_week_seq,ss_store_sk
 )
  select s_store_name1,s_store_id1,d_week_seq1
       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
 from
 (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
        ,s_store_id s_store_id1,sun_sales sun_sales1
        ,mon_sales mon_sales1,tue_sales tue_sales1
        ,wed_sales wed_sales1,thu_sales thu_sales1
        ,fri_sales fri_sales1,sat_sales sat_sales1
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and 
        d_month_seq between 1196 and 1196 + ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates_y AS (SELECT d_date_sk, d_week_seq FROM date_dim WHERE d_month_seq BETWEEN 1196 AND 1196 + 11), filtered_dates_x AS (SELECT d_date_sk, d_week_seq FROM date_dim WHERE d_month_seq BETWEEN 1196 + 12 AND 1196 + 23), wss_y AS (SELECT d_week_seq, ss_store_sk, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales FROM store_sales JOIN filtered_dates_y ON d_date_sk = ss_sold_date_sk GROUP BY d_week_seq, ss_store_sk), wss_x AS (SELECT d_week_seq, ss_store_sk, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_n...[truncated]
```

---

### 48. benchmark_v2 - Q60

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 60 in stream 0 using template query60.tpl
with ss as (
 select
          i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 cs as (
 select
          i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 ws as (
 select
          i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy = 8), children_items AS (SELECT i_item_id, i_item_sk FROM item WHERE i_category IN ('Children')), ss AS (SELECT i_item_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN customer_address ON ss_addr_sk = ca_address_sk JOIN children_items ON ss_item_sk = i_item_sk WHERE ca_gmt_offset = -7 GROUP BY i_item_id), ws AS (SELECT i_item_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer_address ON ws_bill_addr_sk = ca_address_sk JOIN children_items ON ws_item_sk = i_item_sk WHERE ca_gmt_offset = -7 GROUP BY i_item_id), cs AS (SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_category IN ('Children')) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 8 AND cs_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -7 GROUP BY i_item_id)
SELECT i_item_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_item_id ORDER BY i_item_id, total_sales LIMIT 100
```

---

### 49. benchmark_v2 - Q61

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 61 in stream 0 using template query61.tpl
select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        ,store
        ,promotion
        ,date_dim
        ,customer
        ,customer_address 
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales
        ,store
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) all_sales
order by promotions, total
 LIMIT 100;

-- end query 61 in stream 0 using template query61.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 11), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_gmt_offset = -7), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_gmt_offset = -7), filtered_items AS (SELECT i_item_sk FROM item WHERE i_category = 'Jewelry'), filtered_customers AS (SELECT c_customer_sk, c_current_addr_sk FROM customer), base_sales AS (SELECT ss_ext_sales_price, ss_promo_sk, ss_customer_sk, ss_store_sk, ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN filtered_items ON ss_item_sk = i_item_sk), promotion_sales AS (SELECT SUM(ss_ext_sales_price) AS promotions FROM base_sales AS bs JOIN filtered_customers AS c ON bs.ss_customer_sk = c.c_customer_sk JOIN filtered_addresses AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN promotion AS p ON bs.ss_promo_sk = p.p_promo_sk WHERE (p_channel_dmail = 'Y' OR p_channel_email = 'Y' OR p_channel_tv = 'Y')), all_sales AS (SELECT SUM(ss_ext_sales_price) AS total FROM base_sales AS bs JOIN filtered_customers AS c ON bs.ss_customer_sk = c.c_customer_sk JOIN filtered_addresses AS ca ON c.c_current_addr_sk = ca.ca_address_sk)
SELECT promotions, total, CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100 FROM promotion_sales, all_sales ORDER BY promotions, total LIMIT 100
```

---

### 50. benchmark_v2 - Q62

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 62 in stream 0 using template query62.tpl
select 
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   web_sales
  ,warehouse
  ,ship_mode
  ,web_site
  ,date_dim
where
    d_month_seq between 1194 and 1194 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
       ,web_name
 LIMIT 100;

-- end query 62 in stream 0 using template query62.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1194 AND 1194 + 11)
SELECT SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name, SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 30) AND (ws_ship_date_sk - ws_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 60) AND (ws_ship_date_sk - ws_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 90) AND (ws_ship_date_sk - ws_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN (ws_ship_date_sk - ws_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days" FROM web_sales JOIN filtered_dates ON web_sales.ws_ship_date_sk = filtered_dates.d_date_sk JOIN warehouse ON web_sales.ws_warehouse_sk = w_warehouse_sk JOIN ship_mode ON web_sales.ws_ship_mode_sk = sm_ship_mode_sk JOIN web_site ON web_sales.ws_web_site_sk = web_site_sk GROUP BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name ORDER BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name LIMIT 100
```

---

### 51. benchmark_v2 - Q63

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 63 in stream 0 using template query63.tpl
select * 
from (select i_manager_id
             ,sum(ss_sales_price) sum_sales
             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from item
          ,store_sales
          ,date_dim
          ,store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1181,1181+1,1181+2,1181+3,1181+4,1181+5,1181+6,1181+7,1181+8,1181+9,1181+10,1181+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
 LIMIT 100;

-- end query 63 in stream 0 using template query63.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_moy FROM date_dim WHERE d_month_seq IN (1181, 1182, 1183, 1184, 1185, 1186, 1187, 1188, 1189, 1190, 1191, 1192)), branch1_sales AS (SELECT i.i_manager_id, fd.d_moy, ss.ss_sales_price FROM store_sales AS ss JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE i.i_category IN ('Books', 'Children', 'Electronics') AND i.i_class IN ('personal', 'portable', 'reference', 'self-help') AND i.i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9')), branch2_sales AS (SELECT i.i_manager_id, fd.d_moy, ss.ss_sales_price FROM store_sales AS ss JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE i.i_category IN ('Women', 'Music', 'Men') AND i.i_class IN ('accessories', 'classical', 'fragrances', 'pants') AND i.i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1')), all_sales AS (SELECT i_manager_id, d_moy, ss_sales_price FROM branch1_sales UNION ALL SELECT i_manager_id, d_moy, ss_sales_price FROM branch2_sales), grouped_sales AS (SELECT i_manager_id, d_moy, SUM(ss_sales_price) AS sum_sales FROM all_sales GROUP BY i_manager_id, d_moy), windowed_sales AS (SELECT i_manager_id, d_moy, sum_sales, AVG(sum_sales) OVER (PARTITION BY i_manager_id) ...[truncated]
```

---

### 52. benchmark_v2 - Q64

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 64 in stream 0 using template query64.tpl
with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales
      ,catalog_returns
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_street_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales
        ,store_returns
        ,cs_ui
        ,date_dim d1
        ,date_dim d2
        ,date_dim d3
        ,store
        ,customer
        ,customer_demographics cd1
        ,customer_demographics cd2
        ,promotion
        ,household_demographics hd1
        ,household_demographics hd2
        ,customer_address ad1
        ,customer_address ad2
        ,income_band ib1
        ,income_band ib2
        ,item
  WHERE  ss_store_sk = s_store_sk AND
         ss_sold_date_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH cs_ui AS (SELECT cs_item_sk, SUM(cs_ext_list_price) AS sale, SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit) AS refund FROM catalog_sales, catalog_returns WHERE cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number GROUP BY cs_item_sk HAVING SUM(cs_ext_list_price) > 2 * SUM(cr_refunded_cash + cr_reversed_charge + cr_store_credit)), cross_sales_filtered_2001 AS (SELECT i_product_name AS product_name, i_item_sk AS item_sk, s_store_name AS store_name, s_zip AS store_zip, ad1.ca_street_number AS b_street_number, ad1.ca_street_name AS b_street_name, ad1.ca_city AS b_city, ad1.ca_zip AS b_zip, ad2.ca_street_number AS c_street_number, ad2.ca_street_name AS c_street_name, ad2.ca_city AS c_city, ad2.ca_zip AS c_zip, d1.d_year AS syear, d2.d_year AS fsyear, d3.d_year AS s2year, COUNT(*) AS cnt, SUM(ss_wholesale_cost) AS s1, SUM(ss_list_price) AS s2, SUM(ss_coupon_amt) AS s3 FROM store_sales, store_returns, cs_ui, date_dim AS d1, date_dim AS d2, date_dim AS d3, store, customer, customer_demographics AS cd1, customer_demographics AS cd2, promotion, household_demographics AS hd1, household_demographics AS hd2, customer_address AS ad1, customer_address AS ad2, income_band AS ib1, income_band AS ib2, item WHERE ss_store_sk = s_store_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year = 2001 AND ss_customer_sk = c_customer_sk AND ss_cdemo_sk = cd1.cd_demo_sk AND ss_hdemo_sk = hd1.hd_demo_sk AND ss_addr_sk = ad1.ca_address_sk AND ss_item_sk = i_item_sk AND ss_item_sk...[truncated]
```

---

### 53. benchmark_v2 - Q65

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 65 in stream 0 using template query65.tpl
select
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from store, item,
     (select ss_store_sk, avg(revenue) as ave
 	from
 	    (select  ss_store_sk, ss_item_sk, 
 		     sum(ss_sales_price) as revenue
 		from store_sales, date_dim
 		where ss_sold_date_sk = d_date_sk and d_month_seq between 1221 and 1221+11
 		group by ss_store_sk, ss_item_sk) sa
 	group by ss_store_sk) sb,
     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
 	from store_sales, date_dim
 	where ss_sold_date_sk = d_date_sk and d_month_seq between 1221 and 1221+11
 	group by ss_store_sk, ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and 
       sc.revenue <= 0.1 * sb.ave and
       s_store_sk = sc.ss_store_sk and
       i_item_sk = sc.ss_item_sk
 order by s_store_name, i_item_desc
 LIMIT 100;

-- end query 65 in stream 0 using template query65.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1221 AND 1221 + 11), store_item_revenue AS (SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk GROUP BY ss_store_sk, ss_item_sk), store_avg_revenue AS (SELECT ss_store_sk, AVG(revenue) AS ave FROM store_item_revenue GROUP BY ss_store_sk)
SELECT s_store_name, i_item_desc, sc.revenue, i_current_price, i_wholesale_cost, i_brand FROM store JOIN item ON 1 = 1 JOIN store_item_revenue AS sc ON s_store_sk = sc.ss_store_sk AND i_item_sk = sc.ss_item_sk JOIN store_avg_revenue AS sb ON sc.ss_store_sk = sb.ss_store_sk WHERE sc.revenue <= 0.1 * sb.ave ORDER BY s_store_name, i_item_desc LIMIT 100
```

---

### 54. benchmark_v2 - Q66

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 66 in stream 0 using template query66.tpl
select  
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year, d_moy FROM date_dim WHERE d_year = 1998), filtered_times AS (SELECT t_time_sk FROM time_dim WHERE t_time BETWEEN 48821 AND 48821 + 28800), filtered_ship_modes AS (SELECT sm_ship_mode_sk FROM ship_mode WHERE sm_carrier IN ('GREAT EASTERN', 'LATVIAN')), web_sales_agg AS (SELECT w_warehouse_name, w_warehouse_sq_ft, w_city, w_county, w_state, w_country, d_year AS year, 'GREAT EASTERN,LATVIAN' AS ship_carriers, SUM(CASE WHEN d_moy = 1 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jan_sales, SUM(CASE WHEN d_moy = 2 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS feb_sales, SUM(CASE WHEN d_moy = 3 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS mar_sales, SUM(CASE WHEN d_moy = 4 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS apr_sales, SUM(CASE WHEN d_moy = 5 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS may_sales, SUM(CASE WHEN d_moy = 6 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jun_sales, SUM(CASE WHEN d_moy = 7 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS jul_sales, SUM(CASE WHEN d_moy = 8 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS aug_sales, SUM(CASE WHEN d_moy = 9 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS sep_sales, SUM(CASE WHEN d_moy = 10 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS oct_sales, SUM(CASE WHEN d_moy = 11 THEN ws_ext_sales_price * ws_quantity ELSE 0 END) AS nov_sales, SUM(CASE WHEN d_moy = 12 THEN ws_ext_sales_price * ws_quantity ELSE 0 ...[truncated]
```

---

### 55. benchmark_v2 - Q67

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 67 in stream 0 using template query67.tpl
select *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales
                ,date_dim
                ,store
                ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1206 and 1206+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
 LIMIT 100;

-- end query 67 in stream 0 using template query67.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year, d_qoy, d_moy FROM date_dim WHERE d_month_seq BETWEEN 1206 AND 1217), joined_sales AS (SELECT i.i_category, i.i_class, i.i_brand, i.i_product_name, fd.d_year, fd.d_qoy, fd.d_moy, s.s_store_id, ss.ss_sales_price * ss.ss_quantity AS sales_amount FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk), aggregated_sales AS (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, SUM(COALESCE(sales_amount, 0)) AS sumsales FROM joined_sales GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)), ranked_sales AS (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk FROM aggregated_sales)
SELECT * FROM ranked_sales WHERE rk <= 100 ORDER BY i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, rk LIMIT 100
```

---

### 56. benchmark_v2 - Q68

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 68 in stream 0 using template query68.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from store_sales
           ,date_dim
           ,store
           ,household_demographics
           ,customer_address 
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_store_sk = store.s_store_sk  
        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        and date_dim.d_dom between 1 and 2 
        and (household_demographics.hd_dep_count = 8 or
             household_demographics.hd_vehicle_count= -1)
        and date_dim.d_year in (1998,1998+1,1998+2)
        and store.s_city in ('Pleasant Hill','Five Points')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer
      ,customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 LIMIT 100;

-- end query 68 in ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_dom BETWEEN 1 AND 2 AND d_year IN (1998, 1999, 2000)), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_city IN ('Pleasant Hill', 'Five Points')), sales_hdemo_dep8 AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_ext_sales_price, ss_ext_list_price, ss_ext_tax FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_dep_count = 8), sales_hdemo_vehicle AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_ext_sales_price, ss_ext_list_price, ss_ext_tax FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_vehicle_count = -1), union_sales AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_ext_sales_price, ss_ext_list_price, ss_ext_tax FROM sales_hdemo_dep8 UNION ALL SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ss_ext_sales_price, ss_ext_list_price, ss_ext_tax FROM sales_hdemo_vehicle), aggregated_sales AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city AS bought_city, SUM(ss_ext_sales_price) AS extended_price, SUM(ss_ext_list_price) AS list_price, SUM(ss_ext_tax) AS extended_tax FROM union_sales JOIN customer_address ON ss_addr_sk = ca_address_sk GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, ca_city)
SELEC...[truncated]
```

---

### 57. benchmark_v2 - Q69

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 69 in stream 0 using template query69.tpl
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('TX','VA','MI') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_moy between 1 and 1+2) and
   (not exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 1 and 1+2) and
    not exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 1 and 1+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 LIMIT 100;

-- end query 69 in stream 0 using template query69.tpl

```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy BETWEEN 1 AND 3), store_customers AS (SELECT DISTINCT ss_customer_sk FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk), web_customers AS (SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN date_range ON ws_sold_date_sk = d_date_sk), catalog_customers AS (SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN date_range ON cs_sold_date_sk = d_date_sk)
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk WHERE ca_state IN ('TX', 'VA', 'MI') AND c.c_customer_sk IN (SELECT ss_customer_sk FROM store_customers) AND NOT c.c_customer_sk IN (SELECT ws_bill_customer_sk FROM web_customers) AND NOT c.c_customer_sk IN (SELECT cs_ship_customer_sk FROM catalog_customers) GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating LIMIT 100
```

---

### 58. benchmark_v2 - Q7

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 7 in stream 0 using template query7.tpl
select i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 LIMIT 100;

-- end query 7 in stream 0 using template query7.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_customer_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), filtered_promotions AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' OR p_channel_event = 'N')
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_customer_demographics ON ss_cdemo_sk = cd_demo_sk JOIN filtered_promotions ON ss_promo_sk = p_promo_sk JOIN item ON ss_item_sk = i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 59. benchmark_v2 - Q70

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 70 in stream 0 using template query70.tpl
select 
    sum(ss_net_profit) as total_sum
   ,s_state
   ,s_county
   ,grouping(s_state)+grouping(s_county) as lochierarchy
   ,rank() over (
 	partition by grouping(s_state)+grouping(s_county),
 	case when grouping(s_county) = 0 then s_state end 
 	order by sum(ss_net_profit) desc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,store
 where
    d1.d_month_seq between 1213 and 1213+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state in
             ( select s_state
               from  (select s_state as s_state,
 			    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
                      from   store_sales, store, date_dim
                      where  d_month_seq between 1213 and 1213+11
 			    and d_date_sk = ss_sold_date_sk
 			    and s_store_sk  = ss_store_sk
                      group by s_state
                     ) tmp1 
               where ranking <= 5
             )
 group by rollup(s_state,s_county)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent
 LIMIT 100;

-- end query 70 in stream 0 using template query70.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1213 AND 1213 + 11), sales_with_dates AS (SELECT ss_net_profit, ss_store_sk, s_state, s_county FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON s_store_sk = ss_store_sk), state_ranking AS (SELECT s_state, RANK() OVER (ORDER BY SUM(ss_net_profit) DESC) AS ranking FROM sales_with_dates GROUP BY s_state HAVING RANK() OVER (ORDER BY SUM(ss_net_profit) DESC) <= 5)
SELECT SUM(ss_net_profit) AS total_sum, s_state, s_county, GROUPING(s_state) + GROUPING(s_county) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(s_state) + GROUPING(s_county), CASE WHEN GROUPING(s_county) = 0 THEN s_state END ORDER BY SUM(ss_net_profit) DESC) AS rank_within_parent FROM sales_with_dates WHERE s_state IN (SELECT s_state FROM state_ranking) GROUP BY ROLLUP (s_state, s_county) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN s_state END, rank_within_parent LIMIT 100
```

---

### 60. benchmark_v2 - Q71

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 71 in stream 0 using template query71.tpl
select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
 	sum(ext_price) ext_price
 from item, (select ws_ext_sales_price as ext_price, 
                        ws_sold_date_sk as sold_date_sk,
                        ws_item_sk as sold_item_sk,
                        ws_sold_time_sk as time_sk  
                 from web_sales,date_dim
                 where d_date_sk = ws_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 union all
                 select cs_ext_sales_price as ext_price,
                        cs_sold_date_sk as sold_date_sk,
                        cs_item_sk as sold_item_sk,
                        cs_sold_time_sk as time_sk
                 from catalog_sales,date_dim
                 where d_date_sk = cs_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 union all
                 select ss_ext_sales_price as ext_price,
                        ss_sold_date_sk as sold_date_sk,
                        ss_item_sk as sold_item_sk,
                        ss_sold_time_sk as time_sk
                 from store_sales,date_dim
                 where d_date_sk = ss_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 ) tmp,time_dim
 where
   sold_item_sk = i_item_sk
   and i_manager_id=1
   and time_sk = t_time_sk
   and (t_meal_time = 'breakfast' or t_meal_time = 'di...[truncated]
```

#### AFTER (Optimized)
```sql
WITH date_filter AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 12 AND d_year = 1998), breakfast_sales AS (SELECT ws_ext_sales_price AS ext_price, ws_item_sk AS sold_item_sk, ws_sold_time_sk AS time_sk FROM web_sales JOIN date_filter ON d_date_sk = ws_sold_date_sk UNION ALL SELECT cs_ext_sales_price AS ext_price, cs_item_sk AS sold_item_sk, cs_sold_time_sk AS time_sk FROM catalog_sales JOIN date_filter ON d_date_sk = cs_sold_date_sk UNION ALL SELECT ss_ext_sales_price AS ext_price, ss_item_sk AS sold_item_sk, ss_sold_time_sk AS time_sk FROM store_sales JOIN date_filter ON d_date_sk = ss_sold_date_sk), dinner_sales AS (SELECT ws_ext_sales_price AS ext_price, ws_item_sk AS sold_item_sk, ws_sold_time_sk AS time_sk FROM web_sales JOIN date_filter ON d_date_sk = ws_sold_date_sk UNION ALL SELECT cs_ext_sales_price AS ext_price, cs_item_sk AS sold_item_sk, cs_sold_time_sk AS time_sk FROM catalog_sales JOIN date_filter ON d_date_sk = cs_sold_date_sk UNION ALL SELECT ss_ext_sales_price AS ext_price, ss_item_sk AS sold_item_sk, ss_sold_time_sk AS time_sk FROM store_sales JOIN date_filter ON d_date_sk = ss_sold_date_sk)
SELECT i.i_brand_id AS brand_id, i.i_brand AS brand, t.t_hour, t.t_minute, SUM(bs.ext_price) AS ext_price FROM item AS i JOIN breakfast_sales AS bs ON i.i_item_sk = bs.sold_item_sk JOIN time_dim AS t ON bs.time_sk = t.t_time_sk WHERE i.i_manager_id = 1 AND t.t_meal_time = 'breakfast' GROUP BY i.i_brand, i.i_brand_id, t.t_hour, t.t_minute UNION ALL SELECT i.i_brand_id A...[truncated]
```

---

### 61. benchmark_v2 - Q72

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 72 in stream 0 using template query72.tpl
select i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
left outer join promotion on (cs_promo_sk=p_promo_sk)
left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity 
  and d3.d_date > d1.d_date + 5
  and hd_buy_potential = '501-1000'
  and d1.d_year = 2002
  and cd_marital_status = 'W'
group by i_item_desc,w_warehouse_name,d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d1.d_week_seq
 LIMIT 100;

-- end query 72 in stream 0 using template query72.tpl

```

#### AFTER (Optimized)
```sql
WITH sold_dates AS (SELECT d_date_sk, d_week_seq, d_date FROM date_dim WHERE d_year = 2002), ship_dates AS (SELECT d_date_sk, d_date FROM date_dim), filtered_cd AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_marital_status = 'W'), filtered_hd AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_buy_potential = '501-1000'), inventory_with_week AS (SELECT inv_item_sk, inv_warehouse_sk, inv_date_sk, inv_quantity_on_hand, d_week_seq FROM inventory JOIN date_dim ON inv_date_sk = d_date_sk), filtered_sales AS (SELECT cs_item_sk, cs_order_number, cs_promo_sk, cs_bill_cdemo_sk, cs_bill_hdemo_sk, cs_sold_date_sk, cs_ship_date_sk, cs_quantity FROM catalog_sales JOIN sold_dates ON cs_sold_date_sk = sold_dates.d_date_sk JOIN ship_dates ON cs_ship_date_sk = ship_dates.d_date_sk WHERE ship_dates.d_date > sold_dates.d_date + 5)
SELECT i_item_desc, w_warehouse_name, sold_dates.d_week_seq, SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, SUM(CASE WHEN NOT p_promo_sk IS NULL THEN 1 ELSE 0 END) AS promo, COUNT(*) AS total_cnt FROM filtered_sales JOIN inventory_with_week ON (cs_item_sk = inv_item_sk AND sold_dates.d_week_seq = inventory_with_week.d_week_seq) JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk) JOIN item ON (i_item_sk = cs_item_sk) JOIN filtered_cd ON (cs_bill_cdemo_sk = filtered_cd.cd_demo_sk) JOIN filtered_hd ON (cs_bill_hdemo_sk = filtered_hd.hd_demo_sk) LEFT OUTER JOIN promotion ON (cs_promo_sk = p_promo_sk) LEFT OUTER JOIN catalog_returns ON...[truncated]
```

---

### 62. benchmark_v2 - Q73

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 73 in stream 0 using template query73.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2 
    and (household_demographics.hd_buy_potential = '501-1000' or
         household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Fairfield County','Walker County','Daviess County','Barrow County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc;

-- end query 73 in stream 0 using template query73.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_dom BETWEEN 1 AND 2 AND d_year IN (2000, 2001, 2002)), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_county IN ('Fairfield County', 'Walker County', 'Daviess County', 'Barrow County')), buy_potential_501 AS (SELECT ss_ticket_number, ss_customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_buy_potential = '501-1000' AND hd_vehicle_count > 0 AND hd_dep_count / hd_vehicle_count > 1), buy_potential_unknown AS (SELECT ss_ticket_number, ss_customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_buy_potential = 'Unknown' AND hd_vehicle_count > 0 AND hd_dep_count / hd_vehicle_count > 1), dj AS (SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt FROM (SELECT * FROM buy_potential_501 UNION ALL SELECT * FROM buy_potential_unknown) AS combined GROUP BY ss_ticket_number, ss_customer_sk)
SELECT c_last_name, c_first_name, c_salutation, c_preferred_cust_flag, ss_ticket_number, cnt FROM dj JOIN customer ON ss_customer_sk = c_customer_sk WHERE cnt BETWEEN 1 AND 5 ORDER BY cnt DESC, c_last_name ASC
```

---

### 63. benchmark_v2 - Q74

**Source**: benchmark_v2

#### BEFORE (Original)
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
         and...[truncated]
```

#### AFTER (Optimized)
```sql
WITH web_sales_aggregated AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ws_net_paid) AS year_total, 'w' AS sale_type FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP BY c_customer_id, c_first_name, c_last_name, d_year HAVING STDDEV_SAMP(ws_net_paid) > 0), year_total AS (SELECT * FROM store_sales_aggregated UNION ALL SELECT * FROM web_sales_aggregated), store_sales_aggregated AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ss_net_paid) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP BY c_customer_id, c_first_name, c_last_name, d_year HAVING STDDEV_SAMP(ss_net_paid) > 0)
SELECT t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name FROM year_total AS t_s_firstyear, year_total AS t_s_secyear, year_total AS t_w_firstyear, year_total AS t_w_secyear WHERE t_s_secyear.customer_id = t_s_firstyear.customer_id AND t_s_firstyear.customer_id = t_w_secyear.customer_id AND t_s_firstyear.customer_id = t_w_firstyear.customer_id AND t_s_firstyear.sale_type = 's' AND t_w_firstyear.sale_type = 'w' AND t_s_secyear.sale_type = 's' AND t_w_secyear.sale_type = ...[truncated]
```

---

### 64. benchmark_v2 - Q75

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 75 in stream 0 using template query75.tpl
WITH all_sales AS (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk
                          JOIN date_dim ON d_date_sk=cs_sold_date_sk
                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number 
                                                    AND cs_item_sk=cr_item_sk)
       WHERE i_category='Home'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
       FROM store_sales JOIN item ON i_item_sk=ss_item_sk
                        JOIN date_dim ON d_date_sk=ss_sold_date_sk
                        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number 
                                                AND ss_item_sk=sr_item_sk)
       WHERE i_category='Home'
       UNION
       SELECT d_year
        ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 1998)), all_sales_filtered AS (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt FROM (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt, cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt FROM catalog_sales JOIN item ON i_item_sk = cs_item_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_sk LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk) WHERE i_category = 'Home' UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt, ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt FROM store_sales JOIN item ON i_item_sk = ss_item_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk) WHERE i_category = 'Home' UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt, ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt FROM web_sales JOIN item ON i_item_sk = ws_item_sk JOIN filtered_dates ON ws_sold_date_sk = d_date_sk LEFT JOIN web_returns ON (ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk) WHERE i_category = 'H...[truncated]
```

---

### 65. benchmark_v2 - Q76

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 76 in stream 0 using template query76.tpl
select channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, 'ss_hdemo_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales, item, date_dim
         WHERE ss_hdemo_sk IS NULL
           AND ss_sold_date_sk=d_date_sk
           AND ss_item_sk=i_item_sk
        UNION ALL
        SELECT 'web' as channel, 'ws_bill_addr_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales, item, date_dim
         WHERE ws_bill_addr_sk IS NULL
           AND ws_sold_date_sk=d_date_sk
           AND ws_item_sk=i_item_sk
        UNION ALL
        SELECT 'catalog' as channel, 'cs_warehouse_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales, item, date_dim
         WHERE cs_warehouse_sk IS NULL
           AND cs_sold_date_sk=d_date_sk
           AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
 LIMIT 100;

-- end query 76 in stream 0 using template query76.tpl

```

#### AFTER (Optimized)
```sql
WITH store_aggregated AS (SELECT 'store' AS channel, 'ss_hdemo_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(ss_ext_sales_price) AS sales_amt FROM store_sales, item, date_dim WHERE ss_hdemo_sk IS NULL AND ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category), web_aggregated AS (SELECT 'web' AS channel, 'ws_bill_addr_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(ws_ext_sales_price) AS sales_amt FROM web_sales, item, date_dim WHERE ws_bill_addr_sk IS NULL AND ws_sold_date_sk = d_date_sk AND ws_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category), catalog_aggregated AS (SELECT 'catalog' AS channel, 'cs_warehouse_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(cs_ext_sales_price) AS sales_amt FROM catalog_sales, item, date_dim WHERE cs_warehouse_sk IS NULL AND cs_sold_date_sk = d_date_sk AND cs_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category)
SELECT channel, col_name, d_year, d_qoy, i_category, sales_cnt, sales_amt FROM (SELECT * FROM store_aggregated UNION ALL SELECT * FROM web_aggregated UNION ALL SELECT * FROM catalog_aggregated) AS foo ORDER BY channel, col_name, d_year, d_qoy, i_category LIMIT 100
```

---

### 66. benchmark_v2 - Q77

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 77 in stream 0 using template query77.tpl
with ss as
 (select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from store_sales,
      date_dim,
      store
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date) 
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY) 
       and ss_store_sk = s_store_sk
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         sum(sr_return_amt) as "returns",
         sum(sr_net_loss) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
       and sr_store_sk = s_store_sk
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from catalog_sales,
      date_dim
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         sum(cr_return_amount) as "returns",
         sum(cr_net_loss) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
 group by cr_call_cen...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY)), filtered_store_sales AS (SELECT ss_store_sk, ss_ext_sales_price, ss_net_profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), filtered_web_sales AS (SELECT ws_web_page_sk, ws_ext_sales_price, ws_net_profit FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), ss AS (SELECT s_store_sk, SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS profit FROM filtered_store_sales JOIN store ON ss_store_sk = s_store_sk GROUP BY s_store_sk), ws AS (SELECT wp_web_page_sk, SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS profit FROM filtered_web_sales JOIN web_page ON ws_web_page_sk = wp_web_page_sk GROUP BY wp_web_page_sk), sr AS (SELECT s_store_sk, SUM(sr_return_amt) AS "returns", SUM(sr_net_loss) AS profit_loss FROM store_returns, date_dim, store WHERE sr_returned_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) AND sr_store_sk = s_store_sk GROUP BY s_store_sk), cs AS (SELECT cs_call_center_sk, SUM(cs_ext_sales_price) AS sales, SUM(cs_net_profit) AS profit FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) GROUP BY cs_call_center_sk), cr AS (SELECT cr_call_center_sk, SUM(cr_return_amount) AS "returns", SUM(cr...[truncated]
```

---

### 67. benchmark_v2 - Q78

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 78 in stream 0 using template query78.tpl
with ws as
  (select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk
   )
 select
ss_item_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_date_dim AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000), ss_with_returns_2000 AS (SELECT d_year AS ss_sold_year, ss_item_sk, ss_customer_sk, ss_ticket_number, SUM(ss_quantity) AS ss_qty, SUM(ss_wholesale_cost) AS ss_wc, SUM(ss_sales_price) AS ss_sp FROM store_sales JOIN filtered_date_dim ON ss_sold_date_sk = d_date_sk GROUP BY d_year, ss_item_sk, ss_customer_sk, ss_ticket_number), ss_anti_join_returns AS (SELECT ss_sold_year, ss_item_sk, ss_customer_sk, SUM(ss_qty) AS ss_qty, SUM(ss_wc) AS ss_wc, SUM(ss_sp) AS ss_sp FROM ss_with_returns_2000 WHERE NOT EXISTS(SELECT 1 FROM store_returns WHERE sr_ticket_number = ss_ticket_number AND sr_item_sk = ss_item_sk) GROUP BY ss_sold_year, ss_item_sk, ss_customer_sk), ws_with_returns_2000 AS (SELECT d_year AS ws_sold_year, ws_item_sk, ws_bill_customer_sk AS ws_customer_sk, ws_order_number, SUM(ws_quantity) AS ws_qty, SUM(ws_wholesale_cost) AS ws_wc, SUM(ws_sales_price) AS ws_sp FROM web_sales JOIN filtered_date_dim ON ws_sold_date_sk = d_date_sk GROUP BY d_year, ws_item_sk, ws_bill_customer_sk, ws_order_number), ws_anti_join_returns AS (SELECT ws_sold_year, ws_item_sk, ws_customer_sk, SUM(ws_qty) AS ws_qty, SUM(ws_wc) AS ws_wc, SUM(ws_sp) AS ws_sp FROM ws_with_returns_2000 WHERE NOT EXISTS(SELECT 1 FROM web_returns WHERE wr_order_number = ws_order_number AND wr_item_sk = ws_item_sk) GROUP BY ws_sold_year, ws_item_sk, ws_customer_sk), ws AS (SELECT d_year AS ws_sold_year, ws_item_sk, ws_bill_customer_sk AS ws_custom...[truncated]
```

---

### 68. benchmark_v2 - Q79

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 79 in stream 0 using template query79.tpl
select
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 5 or household_demographics.hd_vehicle_count > 4)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2) 
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
 LIMIT 100;

-- end query 79 in stream 0 using template query79.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_dow = 1 AND d_year IN (1998, 1999, 2000)), filtered_stores AS (SELECT s_store_sk, s_city FROM store WHERE s_number_employees BETWEEN 200 AND 295), branch1_sales AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, filtered_stores.s_city, SUM(ss_coupon_amt) AS amt, SUM(ss_net_profit) AS profit FROM store_sales JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk JOIN filtered_stores ON store_sales.ss_store_sk = filtered_stores.s_store_sk JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk WHERE household_demographics.hd_dep_count = 5 GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, filtered_stores.s_city), branch2_sales AS (SELECT ss_ticket_number, ss_customer_sk, ss_addr_sk, filtered_stores.s_city, SUM(ss_coupon_amt) AS amt, SUM(ss_net_profit) AS profit FROM store_sales JOIN filtered_dates ON store_sales.ss_sold_date_sk = filtered_dates.d_date_sk JOIN filtered_stores ON store_sales.ss_store_sk = filtered_stores.s_store_sk JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk WHERE household_demographics.hd_vehicle_count > 4 AND household_demographics.hd_dep_count <> 5 GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, filtered_stores.s_city)
SELECT c_last_name, c_first_name, SUBSTRING(s_city, 1, 30), ss_ticket_number, amt, profit FROM (SELECT * FROM branch1_sales UNION ALL SELECT * FROM branch2_sales)...[truncated]
```

---

### 69. benchmark_v2 - Q80

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 80 in stream 0 using template query80.tpl
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
       a...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-08-28' AS DATE) AND (CAST('1998-08-28' AS DATE) + INTERVAL '30' DAY)), filtered_promotions AS (SELECT p_promo_sk FROM promotion WHERE p_channel_tv = 'N'), filtered_items AS (SELECT i_item_sk FROM item WHERE i_current_price > 50), ssr AS (SELECT s_store_id AS store_id, SUM(ss_ext_sales_price) AS sales, SUM(COALESCE(sr_return_amt, 0)) AS "returns", SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_items ON ss_item_sk = i_item_sk JOIN filtered_promotions ON ss_promo_sk = p_promo_sk LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number) JOIN store ON ss_store_sk = s_store_sk GROUP BY s_store_id), wsr AS (SELECT web_site_id, SUM(ws_ext_sales_price) AS sales, SUM(COALESCE(wr_return_amt, 0)) AS "returns", SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_items ON ws_item_sk = i_item_sk JOIN filtered_promotions ON ws_promo_sk = p_promo_sk LEFT OUTER JOIN web_returns ON (ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number) JOIN web_site ON ws_web_site_sk = web_site_sk GROUP BY web_site_id), csr AS (SELECT cp_catalog_page_id AS catalog_page_id, SUM(cs_ext_sales_price) AS sales, SUM(COALESCE(cr_return_amount, 0)) AS "returns", SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS pr...[truncated]
```

---

### 70. benchmark_v2 - Q81

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 81 in stream 0 using template query81.tpl
with customer_total_return as
 (select cr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns
     ,date_dim
     ,customer_address
 where cr_returned_date_sk = d_date_sk 
   and d_year =2002
   and cr_returning_addr_sk = ca_address_sk 
 group by cr_returning_customer_sk
         ,ca_state )
  select c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'CA'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 LIMIT 100;

-- end query 81 in stream 0 using template query81.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_returns AS (SELECT cr_returning_customer_sk, ca_state, SUM(cr_return_amt_inc_tax) AS ctr_total_return FROM catalog_returns JOIN date_dim ON cr_returned_date_sk = d_date_sk JOIN customer_address ON cr_returning_addr_sk = ca_address_sk WHERE d_year = 2002 GROUP BY cr_returning_customer_sk, ca_state), state_avg_return AS (SELECT ca_state AS ctr_state, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM filtered_returns GROUP BY ca_state), customer_total_return AS (SELECT cr_returning_customer_sk AS ctr_customer_sk, ca_state AS ctr_state, SUM(cr_return_amt_inc_tax) AS ctr_total_return FROM catalog_returns, date_dim, customer_address WHERE cr_returned_date_sk = d_date_sk AND d_year = 2002 AND cr_returning_addr_sk = ca_address_sk GROUP BY cr_returning_customer_sk, ca_state)
SELECT c_customer_id, c_salutation, c_first_name, c_last_name, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_country, ca_gmt_offset, ca_location_type, ctr1.ctr_total_return FROM filtered_returns AS ctr1 JOIN state_avg_return AS sar ON ctr1.ca_state = sar.ctr_state JOIN customer ON ctr1.cr_returning_customer_sk = c_customer_sk JOIN customer_address ON ca_address_sk = c_current_addr_sk WHERE ca_state = 'CA' AND ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c_customer_id, c_salutation, c_first_name, c_last_name, ca_street_number, ca_street_name, ca_street_type, ca_suite_number, ca_city, ca_county, ca_state, ca_zip, ca_count...[truncated]
```

---

### 71. benchmark_v2 - Q82

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 82 in stream 0 using template query82.tpl
select i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 17 and 17+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-07-09' as date) and (cast('1999-07-09' as date) + INTERVAL 60 DAY)
 and i_manufact_id in (639,169,138,339)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 LIMIT 100;

-- end query 82 in stream 0 using template query82.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_current_price FROM item WHERE i_current_price BETWEEN 17 AND 47 AND i_manufact_id IN (639, 169, 138, 339)), inventory_with_dates AS (SELECT inv_item_sk FROM inventory JOIN filtered_dates ON inv_date_sk = d_date_sk WHERE inv_quantity_on_hand BETWEEN 100 AND 500)
SELECT fi.i_item_id, fi.i_item_desc, fi.i_current_price FROM filtered_items AS fi JOIN inventory_with_dates AS inv ON fi.i_item_sk = inv.inv_item_sk JOIN store_sales AS ss ON fi.i_item_sk = ss.ss_item_sk GROUP BY fi.i_item_id, fi.i_item_desc, fi.i_current_price ORDER BY fi.i_item_id LIMIT 100
```

---

### 72. benchmark_v2 - Q83

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 83 in stream 0 using template query83.tpl
with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
		where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
  select sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,...[truncated]
```

#### AFTER (Optimized)
```sql
WITH target_weeks AS (SELECT DISTINCT d_week_seq FROM date_dim WHERE d_date IN ('2001-06-06', '2001-09-02', '2001-11-11')), filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_week_seq IN (SELECT d_week_seq FROM target_weeks)), wr_items AS (SELECT i_item_id AS item_id, SUM(wr_return_quantity) AS wr_item_qty FROM web_returns JOIN item ON wr_item_sk = i_item_sk JOIN filtered_dates ON wr_returned_date_sk = d_date_sk GROUP BY i_item_id), sr_items AS (SELECT i_item_id AS item_id, SUM(sr_return_quantity) AS sr_item_qty FROM store_returns JOIN item ON sr_item_sk = i_item_sk JOIN filtered_dates ON sr_returned_date_sk = d_date_sk GROUP BY i_item_id), cr_items AS (SELECT i_item_id AS item_id, SUM(cr_return_quantity) AS cr_item_qty FROM catalog_returns, item, date_dim WHERE cr_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq IN (SELECT d_week_seq FROM date_dim WHERE d_date IN ('2001-06-06', '2001-09-02', '2001-11-11'))) AND cr_returned_date_sk = d_date_sk GROUP BY i_item_id)
SELECT sr_items.item_id, sr_item_qty, sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev, cr_item_qty, cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev, wr_item_qty, wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev, (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average FROM sr_items, cr_items, wr_items WHERE sr_items.item_id = cr_items.item_id AND sr_items.item_id = wr_items.item_id ...[truncated]
```

---

### 73. benchmark_v2 - Q84

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 84 in stream 0 using template query84.tpl
select c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  'Oakwood'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  5806
   and ib_upper_bound   <=  5806 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 LIMIT 100;

-- end query 84 in stream 0 using template query84.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_city = 'Oakwood'), filtered_income_band AS (SELECT ib_income_band_sk FROM income_band WHERE ib_lower_bound >= 5806 AND ib_upper_bound <= 5806 + 50000), filtered_household AS (SELECT hd_demo_sk FROM household_demographics AS hd JOIN filtered_income_band AS ib ON hd.hd_income_band_sk = ib.ib_income_band_sk), qualified_customers AS (SELECT c_customer_id, c_last_name, c_first_name, c_current_cdemo_sk FROM customer AS c JOIN filtered_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN filtered_household AS hd ON c.c_current_hdemo_sk = hd.hd_demo_sk), customer_demo_store AS (SELECT cd.cd_demo_sk, c.c_customer_id, c.c_last_name, c.c_first_name FROM qualified_customers AS c JOIN customer_demographics AS cd ON c.c_current_cdemo_sk = cd.cd_demo_sk JOIN store_returns AS sr ON cd.cd_demo_sk = sr.sr_cdemo_sk)
SELECT c_customer_id AS customer_id, COALESCE(c_last_name, '') || ', ' || COALESCE(c_first_name, '') AS customername FROM customer_demo_store GROUP BY c_customer_id, c_last_name, c_first_name ORDER BY c_customer_id LIMIT 100
```

---

### 74. benchmark_v2 - Q85

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 85 in stream 0 using template query85.tpl
select substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2000
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '4 yr Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Secondary' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'W'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000), demographic_filters AS (SELECT 'M' AS marital_status, '4 yr Degree' AS education_status, 100.00 AS min_price, 150.00 AS max_price UNION ALL SELECT 'S', 'Secondary', 50.00, 100.00 UNION ALL SELECT 'W', 'Advanced Degree', 150.00, 200.00), address_filters AS (SELECT 'FL,TX,DE' AS states, 100 AS min_profit, 200 AS max_profit UNION ALL SELECT 'IN,ND,ID', 150, 300 UNION ALL SELECT 'MT,IL,OH', 50, 250), filtered_sales AS (SELECT r_reason_desc, ws_quantity, wr_refunded_cash, wr_fee FROM web_sales JOIN web_returns ON ws_item_sk = wr_item_sk AND ws_order_number = wr_order_number JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN web_page ON ws_web_page_sk = wp_web_page_sk JOIN customer_demographics AS cd1 ON cd1.cd_demo_sk = wr_refunded_cdemo_sk JOIN customer_demographics AS cd2 ON cd2.cd_demo_sk = wr_returning_cdemo_sk JOIN customer_address ON ca_address_sk = wr_refunded_addr_sk JOIN reason ON r_reason_sk = wr_reason_sk JOIN demographic_filters AS df ON cd1.cd_marital_status = df.marital_status AND cd1.cd_marital_status = cd2.cd_marital_status AND cd1.cd_education_status = df.education_status AND cd1.cd_education_status = cd2.cd_education_status AND ws_sales_price BETWEEN df.min_price AND df.max_price JOIN address_filters AS af ON ca_country = 'United States' AND (ca_state IN (SELECT SPLIT_PART(af.states, ',', 1) UNION ALL SELECT SPLIT_PART(af.states, ',', 2) UNION ALL SELECT SPLIT_PART(af.states, ',', 3...[truncated]
```

---

### 75. benchmark_v2 - Q86

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 86 in stream 0 using template query86.tpl
select  
    sum(ws_net_paid) as total_sum
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ws_net_paid) desc) as rank_within_parent
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_month_seq between 1224 and 1224+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category end,
   rank_within_parent
 LIMIT 100;

-- end query 86 in stream 0 using template query86.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1224 AND 1235), filtered_sales AS (SELECT ws_item_sk, ws_net_paid FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk)
SELECT SUM(ws_net_paid) AS total_sum, i_category, i_class, GROUPING(i_category) + GROUPING(i_class) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY SUM(ws_net_paid) DESC) AS rank_within_parent FROM filtered_sales JOIN item ON i_item_sk = ws_item_sk GROUP BY ROLLUP (i_category, i_class) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category END, rank_within_parent LIMIT 100
```

---

### 76. benchmark_v2 - Q87

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 87 in stream 0 using template query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
) cool_cust
;

-- end query 87 in stream 0 using template query87.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1184 AND 1184 + 11), store_sales_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN customer ON ss_customer_sk = c_customer_sk), catalog_sales_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk), web_sales_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk)
SELECT COUNT(*) FROM ((SELECT * FROM store_sales_customers) EXCEPT (SELECT * FROM catalog_sales_customers) EXCEPT (SELECT * FROM web_sales_customers)) AS cool_cust
```

---

### 77. benchmark_v2 - Q88

**Source**: benchmark_v2

#### BEFORE (Original)
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
     and...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_sales AS (SELECT ss_sold_time_sk, ss_hdemo_sk, ss_store_sk FROM store_sales WHERE EXISTS(SELECT 1 FROM household_demographics WHERE store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk AND ((household_demographics.hd_dep_count = -1 AND household_demographics.hd_vehicle_count <= 1) OR (household_demographics.hd_dep_count = 4 AND household_demographics.hd_vehicle_count <= 6) OR (household_demographics.hd_dep_count = 3 AND household_demographics.hd_vehicle_count <= 5))) AND EXISTS(SELECT 1 FROM store WHERE store_sales.ss_store_sk = store.s_store_sk AND store.s_store_name = 'ese'))
SELECT * FROM (SELECT COUNT(*) AS h8_30_to_9 FROM filtered_sales JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk WHERE time_dim.t_hour = 8 AND time_dim.t_minute >= 30) AS s1, (SELECT COUNT(*) AS h9_to_9_30 FROM filtered_sales JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk WHERE time_dim.t_hour = 9 AND time_dim.t_minute < 30) AS s2, (SELECT COUNT(*) AS h9_30_to_10 FROM filtered_sales JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk WHERE time_dim.t_hour = 9 AND time_dim.t_minute >= 30) AS s3, (SELECT COUNT(*) AS h10_to_10_30 FROM filtered_sales JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk WHERE time_dim.t_hour = 10 AND time_dim.t_minute < 30) AS s4, (SELECT COUNT(*) AS h10_30_to_11 FROM filtered_sales JOIN time_dim ON ss_sold_time_sk = time_dim.t_time_sk WHERE time_dim.t_hour = 10 AND time_dim.t_minute >= 30) AS s5, (SELECT COUNT(*) AS h11_to_11_30 FROM fi...[truncated]
```

---

### 78. benchmark_v2 - Q89

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 89 in stream 0 using template query89.tpl
select *
from(
select i_category, i_class, i_brand,
       s_store_name, s_company_name,
       d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (1999) and
        ((i_category in ('Jewelry','Shoes','Electronics') and
          i_class in ('semi-precious','athletic','portable')
         )
      or (i_category in ('Men','Music','Women') and
          i_class in ('accessories','rock','maternity') 
        ))
group by i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
 LIMIT 100;

-- end query 89 in stream 0 using template query89.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999), branch1 AS (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, SUM(ss_sales_price) AS sum_sales FROM item JOIN store_sales ON ss_item_sk = i_item_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON ss_store_sk = s_store_sk WHERE i_category IN ('Jewelry', 'Shoes', 'Electronics') AND i_class IN ('semi-precious', 'athletic', 'portable') GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy), branch2 AS (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, SUM(ss_sales_price) AS sum_sales FROM item JOIN store_sales ON ss_item_sk = i_item_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON ss_store_sk = s_store_sk WHERE i_category IN ('Men', 'Music', 'Women') AND i_class IN ('accessories', 'rock', 'maternity') GROUP BY i_category, i_class, i_brand, s_store_name, s_company_name, d_moy), combined_branches AS (SELECT * FROM branch1 UNION ALL SELECT * FROM branch2), window_computation AS (SELECT *, AVG(sum_sales) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) AS avg_monthly_sales FROM combined_branches)
SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum_sales, avg_monthly_sales FROM window_computation WHERE CASE WHEN (avg_monthly_sales <> 0) THEN (ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales) ELSE NULL END > 0.1 ORDER BY sum_sales - avg_monthly_sales, s_s...[truncated]
```

---

### 79. benchmark_v2 - Q90

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 90 in stream 0 using template query90.tpl
select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 10 and 10+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 5000 and 5200) at_tbl,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 16 and 16+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 LIMIT 100;

-- end query 90 in stream 0 using template query90.tpl

```

#### AFTER (Optimized)
```sql
WITH common_sales AS (SELECT ws_sold_time_sk, ws_ship_hdemo_sk, ws_web_page_sk FROM web_sales), qualified_sales AS (SELECT cs.ws_sold_time_sk, t.t_hour, cs.ws_ship_hdemo_sk, cs.ws_web_page_sk FROM common_sales AS cs JOIN household_demographics AS hd ON cs.ws_ship_hdemo_sk = hd.hd_demo_sk JOIN time_dim AS t ON cs.ws_sold_time_sk = t.t_time_sk JOIN web_page AS wp ON cs.ws_web_page_sk = wp.wp_web_page_sk WHERE hd.hd_dep_count = 2 AND wp.wp_char_count BETWEEN 5000 AND 5200)
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio FROM (SELECT COUNT(*) AS amc FROM qualified_sales WHERE t_hour BETWEEN 10 AND 11) AS at_tbl, (SELECT COUNT(*) AS pmc FROM qualified_sales WHERE t_hour BETWEEN 16 AND 17) AS pt ORDER BY am_pm_ratio LIMIT 100
```

---

### 80. benchmark_v2 - Q91

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 91 in stream 0 using template query91.tpl
select  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        call_center,
        catalog_returns,
        date_dim,
        customer,
        customer_address,
        customer_demographics,
        household_demographics
where
        cr_call_center_sk       = cc_call_center_sk
and     cr_returned_date_sk     = d_date_sk
and     cr_returning_customer_sk= c_customer_sk
and     cd_demo_sk              = c_current_cdemo_sk
and     hd_demo_sk              = c_current_hdemo_sk
and     ca_address_sk           = c_current_addr_sk
and     d_year                  = 2001 
and     d_moy                   = 11
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like '1001-5000%'
and     ca_gmt_offset           = -6
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by sum(cr_net_loss) desc;

-- end query 91 in stream 0 using template query91.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy = 11), filtered_customer_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_gmt_offset = -6), filtered_household AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_buy_potential LIKE '1001-5000%'), branch1 AS (SELECT cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status, cr_net_loss FROM catalog_returns JOIN filtered_dates ON cr_returned_date_sk = d_date_sk JOIN customer ON cr_returning_customer_sk = c_customer_sk JOIN filtered_customer_address ON c_current_addr_sk = ca_address_sk JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk JOIN filtered_household ON hd_demo_sk = c_current_hdemo_sk JOIN call_center ON cr_call_center_sk = cc_call_center_sk WHERE cd_marital_status = 'M' AND cd_education_status = 'Unknown'), branch2 AS (SELECT cc_call_center_id, cc_name, cc_manager, cd_marital_status, cd_education_status, cr_net_loss FROM catalog_returns JOIN filtered_dates ON cr_returned_date_sk = d_date_sk JOIN customer ON cr_returning_customer_sk = c_customer_sk JOIN filtered_customer_address ON c_current_addr_sk = ca_address_sk JOIN customer_demographics ON cd_demo_sk = c_current_cdemo_sk JOIN filtered_household ON hd_demo_sk = c_current_hdemo_sk JOIN call_center ON cr_call_center_sk = cc_call_center_sk WHERE cd_marital_status = 'W' AND cd_education_status = 'Advanced Degree'), combined_returns AS (SELECT * FROM branch1 UNION ALL SELECT * FROM b...[truncated]
```

---

### 81. benchmark_v2 - Q92

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 92 in stream 0 using template query92.tpl
select 
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 320
and i_item_sk = ws_item_sk 
and d_date between '2002-02-26' and 
        (cast('2002-02-26' as date) + INTERVAL 90 DAY)
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '2002-02-26' and
                             (cast('2002-02-26' as date) + INTERVAL 90 DAY)
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
 LIMIT 100;

-- end query 92 in stream 0 using template query92.tpl

```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY)), item_manufact_filter AS (SELECT i_item_sk FROM item WHERE i_manufact_id = 320), ws_avg_discount AS (SELECT ws_item_sk, AVG(ws_ext_discount_amt) * 1.3 AS threshold FROM web_sales AS ws JOIN date_range AS dr ON ws.ws_sold_date_sk = dr.d_date_sk GROUP BY ws_item_sk)
SELECT SUM(ws.ws_ext_discount_amt) AS "Excess Discount Amount" FROM web_sales AS ws JOIN date_range AS dr ON ws.ws_sold_date_sk = dr.d_date_sk JOIN item_manufact_filter AS imf ON ws.ws_item_sk = imf.i_item_sk JOIN ws_avg_discount AS avg ON ws.ws_item_sk = avg.ws_item_sk WHERE ws.ws_ext_discount_amt > avg.threshold ORDER BY SUM(ws.ws_ext_discount_amt) LIMIT 100
```

---

### 82. benchmark_v2 - Q93

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 93 in stream 0 using template query93.tpl
select ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'duplicate purchase') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
 LIMIT 100;

-- end query 93 in stream 0 using template query93.tpl

```

#### AFTER (Optimized)
```sql
WITH duplicate_reason AS (SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'duplicate purchase'), filtered_returns AS (SELECT sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN duplicate_reason ON sr_reason_sk = r_reason_sk), sales_with_returns AS (SELECT ss_customer_sk, (ss_quantity - COALESCE(sr_return_quantity, 0)) * ss_sales_price AS act_sales FROM store_sales LEFT JOIN filtered_returns ON sr_item_sk = ss_item_sk AND sr_ticket_number = ss_ticket_number)
SELECT ss_customer_sk, SUM(act_sales) AS sumsales FROM sales_with_returns GROUP BY ss_customer_sk ORDER BY sumsales, ss_customer_sk LIMIT 100
```

---

### 83. benchmark_v2 - Q94

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 94 in stream 0 using template query94.tpl
select 
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '2000-2-01' and 
           (cast('2000-2-01' as date) + INTERVAL 60 DAY)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'OK'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
 LIMIT 100;

-- end query 94 in stream 0 using template query94.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2000-2-01' AND (CAST('2000-2-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'OK'), filtered_websites AS (SELECT web_site_sk FROM web_site WHERE web_company_name = 'pri'), filtered_sales AS (SELECT ws_order_number, ws_ext_ship_cost, ws_net_profit, ws_warehouse_sk FROM web_sales AS ws1 JOIN filtered_dates ON ws1.ws_ship_date_sk = filtered_dates.d_date_sk JOIN filtered_addresses ON ws1.ws_ship_addr_sk = filtered_addresses.ca_address_sk JOIN filtered_websites ON ws1.ws_web_site_sk = filtered_websites.web_site_sk), multi_warehouse_orders AS (SELECT ws_order_number FROM web_sales GROUP BY ws_order_number HAVING COUNT(DISTINCT ws_warehouse_sk) > 1), returned_orders AS (SELECT DISTINCT wr_order_number FROM web_returns)
SELECT COUNT(DISTINCT ws_order_number) AS "order count", SUM(ws_ext_ship_cost) AS "total shipping cost", SUM(ws_net_profit) AS "total net profit" FROM filtered_sales AS ws1 WHERE EXISTS(SELECT 1 FROM multi_warehouse_orders AS mwo WHERE ws1.ws_order_number = mwo.ws_order_number) AND NOT EXISTS(SELECT 1 FROM returned_orders AS ro WHERE ws1.ws_order_number = ro.wr_order_number) ORDER BY COUNT(DISTINCT ws_order_number) LIMIT 100
```

---

### 84. benchmark_v2 - Q95

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 95 in stream 0 using template query95.tpl
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select 
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '1999-2-01' and 
           (cast('1999-2-01' as date) + INTERVAL 60 DAY)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NC'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
 LIMIT 100;

-- end query 95 in stream 0 using template query95.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '1999-2-01' AND (CAST('1999-2-01' AS DATE) + INTERVAL '60' DAY)), filtered_ca AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'NC'), filtered_web AS (SELECT web_site_sk FROM web_site WHERE web_company_name = 'pri'), ws_wh AS (SELECT ws1.ws_order_number, ws1.ws_warehouse_sk AS wh1, ws2.ws_warehouse_sk AS wh2 FROM web_sales AS ws1 JOIN web_sales AS ws2 ON ws1.ws_order_number = ws2.ws_order_number AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk), valid_orders AS (SELECT DISTINCT ws_wh.ws_order_number FROM ws_wh JOIN web_returns ON ws_wh.ws_order_number = web_returns.wr_order_number), filtered_sales AS (SELECT ws_order_number, ws_ext_ship_cost, ws_net_profit FROM web_sales WHERE ws_ship_date_sk IN (SELECT d_date_sk FROM filtered_dates) AND ws_ship_addr_sk IN (SELECT ca_address_sk FROM filtered_ca) AND ws_web_site_sk IN (SELECT web_site_sk FROM filtered_web) AND ws_order_number IN (SELECT ws_order_number FROM ws_wh) AND ws_order_number IN (SELECT ws_order_number FROM valid_orders))
SELECT COUNT(DISTINCT ws_order_number) AS "order count", SUM(ws_ext_ship_cost) AS "total shipping cost", SUM(ws_net_profit) AS "total net profit" FROM filtered_sales ORDER BY COUNT(DISTINCT ws_order_number) LIMIT 100
```

---

### 85. benchmark_v2 - Q96

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 96 in stream 0 using template query96.tpl
select count(*) 
from store_sales
    ,household_demographics 
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk   
    and ss_hdemo_sk = household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 3
    and store.s_store_name = 'ese'
order by count(*)
 LIMIT 100;

-- end query 96 in stream 0 using template query96.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_time AS (SELECT t_time_sk FROM time_dim WHERE t_hour = 8 AND t_minute >= 30), filtered_hd AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_dep_count = 3), filtered_store AS (SELECT s_store_sk FROM store WHERE s_store_name = 'ese')
SELECT COUNT(*) FROM store_sales JOIN filtered_time ON ss_sold_time_sk = t_time_sk JOIN filtered_hd ON ss_hdemo_sk = hd_demo_sk JOIN filtered_store ON ss_store_sk = s_store_sk ORDER BY COUNT(*) LIMIT 100
```

---

### 86. benchmark_v2 - Q97

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 97 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
 LIMIT 100;

-- end query 97 in stream 0 using template query97.tpl

```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1214 AND 1225), ssci AS (SELECT ss_customer_sk AS customer_sk, ss_item_sk AS item_sk FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk GROUP BY ss_customer_sk, ss_item_sk), csci AS (SELECT cs_bill_customer_sk AS customer_sk, cs_item_sk AS item_sk FROM catalog_sales JOIN date_range ON cs_sold_date_sk = d_date_sk GROUP BY cs_bill_customer_sk, cs_item_sk)
SELECT SUM(CASE WHEN NOT ssci.customer_sk IS NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_only, SUM(CASE WHEN ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS catalog_only, SUM(CASE WHEN NOT ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_and_catalog FROM ssci FULL OUTER JOIN csci ON (ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk) LIMIT 100
```

---

### 87. benchmark_v2 - Q98

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 98 in stream 0 using template query98.tpl
select i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	store_sales
    	,item 
    	,date_dim
where 
	ss_item_sk = i_item_sk 
  	and i_category in ('Sports', 'Music', 'Shoes')
  	and ss_sold_date_sk = d_date_sk
	and d_date between cast('2002-05-20' as date) 
				and (cast('2002-05-20' as date) + INTERVAL 30 DAY)
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;

-- end query 98 in stream 0 using template query98.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2002-05-20' AS DATE) AND (CAST('2002-05-20' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Sports', 'Music', 'Shoes')), filtered_sales AS (SELECT ss_item_sk, ss_ext_sales_price FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ss_ext_sales_price) AS itemrevenue, SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio FROM filtered_sales JOIN filtered_items ON ss_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
```

---

### 88. benchmark_v2 - Q99

**Source**: benchmark_v2

#### BEFORE (Original)
```sql
-- start query 99 in stream 0 using template query99.tpl
select 
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between 1224 and 1224 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
 LIMIT 100;

-- end query 99 in stream 0 using template query99.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1224 AND 1224 + 11), filtered_sales AS (SELECT cs_sold_date_sk, cs_ship_date_sk, cs_warehouse_sk, cs_ship_mode_sk, cs_call_center_sk FROM catalog_sales JOIN filtered_dates ON cs_ship_date_sk = d_date_sk)
SELECT SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name, SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days" FROM filtered_sales, warehouse, ship_mode, call_center WHERE cs_warehouse_sk = w_warehouse_sk AND cs_ship_mode_sk = sm_ship_mode_sk AND cs_call_center_sk = cc_call_center_sk GROUP BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name ORDER BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name LIMIT 100
```

---

### 89. Kimi Q1-Q30 - Q1

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 1 in stream 0 using template query1.tpl
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100;

-- end query 1 in stream 0 using template query1.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_returns AS (SELECT sr.sr_customer_sk, sr.sr_store_sk, sr.SR_FEE FROM store_returns AS sr JOIN date_dim AS d ON sr.sr_returned_date_sk = d.d_date_sk JOIN store AS s ON sr.sr_store_sk = s.s_store_sk WHERE d.d_year = 2000 AND s.s_state = 'SD'), customer_total_return AS (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(SR_FEE) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk, sr_store_sk), store_avg_return AS (SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_store_sk)
SELECT c.c_customer_id FROM customer_total_return AS ctr1 JOIN store_avg_return AS sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id LIMIT 100
```

---

### 90. Kimi Q1-Q30 - Q10

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 10 in stream 0 using template query10.tpl
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Storey County','Marquette County','Warren County','Cochran County','Kandiyohi County') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 1 and 1+3) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 ANd 1+3) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 and 1+3))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
    ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 10 in stream 0 using template query10.tpl
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3,
  cd_dep_count,
  count(*) cnt4,
  cd_dep_employed_count,
  count(*) cnt5,
  cd_dep_college_count,
  count(*) cnt6
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_county in ('Storey County','Marquette County','Warren County','Cochran County','Kandiyohi County') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_moy between 1 and 1+3) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 ANd 1+3) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_moy between 1 and 1+3))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating,
          cd_dep_count,
    ...[truncated]
```

---

### 91. Kimi Q1-Q30 - Q11

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 11 in stream 0 using template query11.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ss_ext_list_price-ss_ext_discount_amt) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year 
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(ws_ext_list_price-ws_ext_discount_amt) year_total
       ,'w' sale_type
 from customer
     ,web_sales
     ,date_dim
 where c_customer_sk = ws_bill_customer_sk
   and ws_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag 
         ,c_birth_country
         ,c_login
         ,c_email_addres...[truncated]
```

#### AFTER (Optimized)
```sql
WITH year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (2001, 2002) GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total, 'w' AS sale_type FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk AND d_year IN (2001, 2002) GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year) SELECT t_s_secyear.customer_id, t_s_secyear.customer_first_name, t_s_secyear.customer_last_name, t_s_secyear.customer_birth_country FROM year_total AS t_s_firstyear, year_total AS t_s_secyear, year_total AS t_w...[truncated]
```

---

### 92. Kimi Q1-Q30 - Q12

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 12 in stream 0 using template query12.tpl
select i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ws_ext_sales_price) as itemrevenue 
      ,sum(ws_ext_sales_price)*100/sum(sum(ws_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	web_sales
    	,item 
    	,date_dim
where 
	ws_item_sk = i_item_sk 
  	and i_category in ('Books', 'Sports', 'Men')
  	and ws_sold_date_sk = d_date_sk
	and d_date between cast('1998-04-06' as date) 
				and (cast('1998-04-06' as date) + INTERVAL 30 DAY)
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio
 LIMIT 100;

-- end query 12 in stream 0 using template query12.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_sales AS (SELECT ws_item_sk, ws_ext_sales_price FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue, SUM(ws_ext_sales_price) * 100 / SUM(SUM(ws_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio FROM filtered_sales JOIN filtered_items ON ws_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 93. Kimi Q1-Q30 - Q13

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 13 in stream 0 using template query13.tpl
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Unknown'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'College'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = '4 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('SD', 'KS', 'MI')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MO', 'ND', 'CO')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('NH', 'OH', 'TX')
  and ss_net_profit between 50 and 250  
     ))
;

-- ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 13 in stream 0 using template query13.tpl
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Unknown'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'College'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = '4 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('SD', 'KS', 'MI')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MO', 'ND', 'CO')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('NH', 'OH', 'TX')
  and ss_net_profit between 50 and 250  
     ))
;

-- ...[truncated]
```

---

### 94. Kimi Q1-Q30 - Q14

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 14 in stream 0 using template query14.tpl
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
       ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH cross_items AS (SELECT i_item_sk AS ss_item_sk FROM item, (SELECT iss.i_brand_id AS brand_id, iss.i_class_id AS class_id, iss.i_category_id AS category_id FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT iws.i_brand_id, iws.i_class_id, iws.i_category_id FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2000 + 2) WHERE i_brand_id = brand_id AND i_class_id = class_id AND i_category_id = category_id), avg_sales AS (SELECT AVG(quantity * list_price) AS average_sales FROM (SELECT ss_quantity AS quantity, ss_list_price AS list_price FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT cs_quantity AS quantity, cs_list_price AS list_price FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT ws_quantity AS quantity, ws_list_price AS list_price FROM web_sales, date_dim WHERE ws_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2) AS x) SELECT channel, i_brand_id, i_class_id, i_category_id, SUM(sales), SUM(number_sales...[truncated]
```

---

### 95. Kimi Q1-Q30 - Q15

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 15 in stream 0 using template query15.tpl
select ca_zip
       ,sum(cs_sales_price)
 from catalog_sales
     ,customer
     ,customer_address
     ,date_dim
 where cs_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475',
                                   '85392', '85460', '80348', '81792')
 	      or ca_state in ('CA','WA','GA')
 	      or cs_sales_price > 500)
 	and cs_sold_date_sk = d_date_sk
 	and d_qoy = 1 and d_year = 2001
 group by ca_zip
 order by ca_zip
 LIMIT 100;

-- end query 15 in stream 0 using template query15.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001), filtered_sales AS (SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE ca_state IN ('CA', 'WA', 'GA') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE cs_sales_price > 500)
SELECT ca_zip, SUM(cs_sales_price) FROM filtered_sales GROUP BY ca_zip ORDER BY ca_zip LIMIT 100
```

---

### 96. Kimi Q1-Q30 - Q16

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 16 in stream 0 using template query16.tpl
select 
   count(distinct cs_order_number) as "order count"
  ,sum(cs_ext_ship_cost) as "total shipping cost"
  ,sum(cs_net_profit) as "total net profit"
from
   catalog_sales cs1
  ,date_dim
  ,customer_address
  ,call_center
where
    d_date between '2002-4-01' and 
           (cast('2002-4-01' as date) + INTERVAL 60 DAY)
and cs1.cs_ship_date_sk = d_date_sk
and cs1.cs_ship_addr_sk = ca_address_sk
and ca_state = 'WV'
and cs1.cs_call_center_sk = cc_call_center_sk
and cc_county in ('Ziebach County','Luce County','Richland County','Daviess County',
                  'Barrow County'
)
and exists (select *
            from catalog_sales cs2
            where cs1.cs_order_number = cs2.cs_order_number
              and cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk)
and not exists(select *
               from catalog_returns cr1
               where cs1.cs_order_number = cr1.cr_order_number)
order by count(distinct cs_order_number)
 LIMIT 100;

-- end query 16 in stream 0 using template query16.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), filtered_call_centers AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), multi_warehouse_orders AS (SELECT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), returned_orders AS (SELECT DISTINCT cr_order_number FROM catalog_returns), filtered_sales AS (SELECT cs.cs_order_number, cs.cs_ext_ship_cost, cs.cs_net_profit FROM catalog_sales AS cs JOIN filtered_dates AS fd ON cs.cs_ship_date_sk = fd.d_date_sk JOIN filtered_addresses AS fa ON cs.cs_ship_addr_sk = fa.ca_address_sk JOIN filtered_call_centers AS fcc ON cs.cs_call_center_sk = fcc.cc_call_center_sk JOIN multi_warehouse_orders AS mwo ON cs.cs_order_number = mwo.cs_order_number LEFT JOIN returned_orders AS ro ON cs.cs_order_number = ro.cr_order_number WHERE ro.cr_order_number IS NULL)
SELECT COUNT(DISTINCT cs_order_number) AS "order count", SUM(cs_ext_ship_cost) AS "total shipping cost", SUM(cs_net_profit) AS "total net profit" FROM filtered_sales ORDER BY COUNT(DISTINCT cs_order_number) LIMIT 100
```

---

### 97. Kimi Q1-Q30 - Q17

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 17 in stream 0 using template query17.tpl
select i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2001Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 17 in stream 0 using template query17.tpl
select i_item_id
       ,i_item_desc
       ,s_state
       ,count(ss_quantity) as store_sales_quantitycount
       ,avg(ss_quantity) as store_sales_quantityave
       ,stddev_samp(ss_quantity) as store_sales_quantitystdev
       ,stddev_samp(ss_quantity)/avg(ss_quantity) as store_sales_quantitycov
       ,count(sr_return_quantity) as store_returns_quantitycount
       ,avg(sr_return_quantity) as store_returns_quantityave
       ,stddev_samp(sr_return_quantity) as store_returns_quantitystdev
       ,stddev_samp(sr_return_quantity)/avg(sr_return_quantity) as store_returns_quantitycov
       ,count(cs_quantity) as catalog_sales_quantitycount ,avg(cs_quantity) as catalog_sales_quantityave
       ,stddev_samp(cs_quantity) as catalog_sales_quantitystdev
       ,stddev_samp(cs_quantity)/avg(cs_quantity) as catalog_sales_quantitycov
 from store_sales
     ,store_returns
     ,catalog_sales
     ,date_dim d1
     ,date_dim d2
     ,date_dim d3
     ,store
     ,item
 where d1.d_quarter_name = '2001Q1'
   and d1.d_date_sk = ss_sold_date_sk
   and i_item_sk = ss_item_sk
   and s_store_sk = ss_store_sk
   and ss_customer_sk = sr_customer_sk
   and ss_item_sk = sr_item_sk
   and ss_ticket_number = sr_ticket_number
   and sr_returned_date_sk = d2.d_date_sk
   and d2.d_quarter_name in ('2001Q1','2001Q2','2001Q3')
   and sr_customer_sk = cs_bill_customer_sk
   and sr_item_sk = cs_item_sk
   and cs_sold_date_sk = d3.d_date_sk
   and d3...[truncated]
```

---

### 98. Kimi Q1-Q30 - Q18

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 18 in stream 0 using template query18.tpl
select i_item_id,
        ca_country,
        ca_state, 
        ca_county,
        avg( cast(cs_quantity as decimal(12,2))) agg1,
        avg( cast(cs_list_price as decimal(12,2))) agg2,
        avg( cast(cs_coupon_amt as decimal(12,2))) agg3,
        avg( cast(cs_sales_price as decimal(12,2))) agg4,
        avg( cast(cs_net_profit as decimal(12,2))) agg5,
        avg( cast(c_birth_year as decimal(12,2))) agg6,
        avg( cast(cd1.cd_dep_count as decimal(12,2))) agg7
 from catalog_sales, customer_demographics cd1, 
      customer_demographics cd2, customer, customer_address, date_dim, item
 where cs_sold_date_sk = d_date_sk and
       cs_item_sk = i_item_sk and
       cs_bill_cdemo_sk = cd1.cd_demo_sk and
       cs_bill_customer_sk = c_customer_sk and
       cd1.cd_gender = 'F' and 
       cd1.cd_education_status = 'Advanced Degree' and
       c_current_cdemo_sk = cd2.cd_demo_sk and
       c_current_addr_sk = ca_address_sk and
       c_birth_month in (10,7,8,4,1,2) and
       d_year = 1998 and
       ca_state in ('WA','GA','NC'
                   ,'ME','WY','OK','IN')
 group by rollup (i_item_id, ca_country, ca_state, ca_county)
 order by ca_country,
        ca_state, 
        ca_county,
	i_item_id
 LIMIT 100;

-- end query 18 in stream 0 using template query18.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_date_dim AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1998), filtered_cd1 AS (SELECT cd_demo_sk, cd_dep_count FROM customer_demographics WHERE cd_gender = 'F' AND cd_education_status = 'Advanced Degree'), filtered_customer AS (SELECT c_customer_sk, c_current_cdemo_sk, c_current_addr_sk, c_birth_year FROM customer WHERE c_birth_month IN (10, 7, 8, 4, 1, 2)), filtered_customer_address AS (SELECT ca_address_sk, ca_country, ca_state, ca_county FROM customer_address WHERE ca_state IN ('WA', 'GA', 'NC', 'ME', 'WY', 'OK', 'IN'))
SELECT i_item_id, ca_country, ca_state, ca_county, AVG(CAST(cs_quantity AS DECIMAL(12, 2))) AS agg1, AVG(CAST(cs_list_price AS DECIMAL(12, 2))) AS agg2, AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2))) AS agg3, AVG(CAST(cs_sales_price AS DECIMAL(12, 2))) AS agg4, AVG(CAST(cs_net_profit AS DECIMAL(12, 2))) AS agg5, AVG(CAST(c_birth_year AS DECIMAL(12, 2))) AS agg6, AVG(CAST(cd1.cd_dep_count AS DECIMAL(12, 2))) AS agg7 FROM catalog_sales JOIN filtered_date_dim ON cs_sold_date_sk = d_date_sk JOIN item ON cs_item_sk = i_item_sk JOIN filtered_cd1 AS cd1 ON cs_bill_cdemo_sk = cd1.cd_demo_sk JOIN filtered_customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_demographics AS cd2 ON c_current_cdemo_sk = cd2.cd_demo_sk JOIN filtered_customer_address ON c_current_addr_sk = ca_address_sk GROUP BY ROLLUP (i_item_id, ca_country, ca_state, ca_county) ORDER BY ca_country, ca_state, ca_county, i_item_id LIMIT 100
```

---

### 99. Kimi Q1-Q30 - Q19

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 19 in stream 0 using template query19.tpl
select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id=2
   and d_moy=12
   and d_year=1999
   and ss_customer_sk = c_customer_sk 
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
   and ss_store_sk = s_store_sk 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
 LIMIT 100;

-- end query 19 in stream 0 using template query19.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 12), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand, i_manufact_id, i_manufact FROM item WHERE i_manager_id = 2)
SELECT i.i_brand_id AS brand_id, i.i_brand AS brand, i.i_manufact_id, i.i_manufact, SUM(ss.ss_ext_sales_price) AS ext_price FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN filtered_items AS i ON ss.ss_item_sk = i.i_item_sk JOIN customer AS c ON ss.ss_customer_sk = c.c_customer_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE SUBSTRING(ca.ca_zip, 1, 5) <> SUBSTRING(s.s_zip, 1, 5) GROUP BY i.i_brand, i.i_brand_id, i.i_manufact_id, i.i_manufact ORDER BY ext_price DESC, i.i_brand, i.i_brand_id, i.i_manufact_id, i.i_manufact LIMIT 100
```

---

### 100. Kimi Q1-Q30 - Q2

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 2 in stream 0 using template query2.tpl
with wscs as
 (select sold_date_sk
        ,sales_price
  from (select ws_sold_date_sk sold_date_sk
              ,ws_ext_sales_price sales_price
        from web_sales 
        union all
        select cs_sold_date_sk sold_date_sk
              ,cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as 
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1
        ,mon_sales mon_sales1
        ,...[truncated]
```

#### AFTER (Optimized)
```sql
WITH wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs AS (SELECT d_week_seq, SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN sales_price ELSE NULL END) AS sat_sales FROM wscs, date_dim WHERE d_date_sk = sold_date_sk AND d_year IN (1998, 1999) GROUP BY d_week_seq) SELECT d_week_seq1, ROUND(sun_sales1 / sun_sales2, 2), ROUND(mon_sales1 / mon_sales2, 2), ROUND(tue_sales1 / tue_sales2, 2), ROUND(wed_sales1 / wed_sales2, 2), ROUND(thu_sales1 / thu_sales2, 2), ROUND(fri_sales1 / fri_sales2, 2), ROUND(sat_sales1 / sat_sales2, 2) FROM (SELECT wswscs.d_week_seq AS d_week_seq1, sun_sales AS sun_sales1, mon_sales AS mon_sales1, tue_sales AS tue_sales1, wed_sales AS wed_sales1, thu_sales AS thu_sales1, fri_sales AS fri_sales1, sat_sales AS sat_sales1 FROM wswscs, date_dim WHERE date_dim.d_week...[truncated]
```

---

### 101. Kimi Q1-Q30 - Q20

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 20 in stream 0 using template query20.tpl
select i_item_id
       ,i_item_desc 
       ,i_category 
       ,i_class 
       ,i_current_price
       ,sum(cs_ext_sales_price) as itemrevenue 
       ,sum(cs_ext_sales_price)*100/sum(sum(cs_ext_sales_price)) over
           (partition by i_class) as revenueratio
 from	catalog_sales
     ,item 
     ,date_dim
 where cs_item_sk = i_item_sk 
   and i_category in ('Shoes', 'Books', 'Women')
   and cs_sold_date_sk = d_date_sk
 and d_date between cast('2002-01-26' as date) 
 				and (cast('2002-01-26' as date) + INTERVAL 30 DAY)
 group by i_item_id
         ,i_item_desc 
         ,i_category
         ,i_class
         ,i_current_price
 order by i_category
         ,i_class
         ,i_item_id
         ,i_item_desc
         ,revenueratio
 LIMIT 100;

-- end query 20 in stream 0 using template query20.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Shoes', 'Books', 'Women')), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2002-01-26' AS DATE) AND (CAST('2002-01-26' AS DATE) + INTERVAL '30' DAY))
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(cs_ext_sales_price) AS itemrevenue, SUM(cs_ext_sales_price) * 100 / SUM(SUM(cs_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio FROM catalog_sales JOIN filtered_items ON cs_item_sk = i_item_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 102. Kimi Q1-Q30 - Q21

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 21 in stream 0 using template query21.tpl
select *
 from(select w_warehouse_name
            ,i_item_id
            ,sum(case when (cast(d_date as date) < cast ('2002-02-27' as date))
	                then inv_quantity_on_hand 
                      else 0 end) as inv_before
            ,sum(case when (cast(d_date as date) >= cast ('2002-02-27' as date))
                      then inv_quantity_on_hand 
                      else 0 end) as inv_after
   from inventory
       ,warehouse
       ,item
       ,date_dim
   where i_current_price between 0.99 and 1.49
     and i_item_sk          = inv_item_sk
     and inv_warehouse_sk   = w_warehouse_sk
     and inv_date_sk    = d_date_sk
     and d_date between (cast ('2002-02-27' as date) - INTERVAL 30 DAY)
                    and (cast ('2002-02-27' as date) + INTERVAL 30 DAY)
   group by w_warehouse_name, i_item_id) x
 where (case when inv_before > 0 
             then inv_after / inv_before 
             else null
             end) between 2.0/3.0 and 3.0/2.0
 order by w_warehouse_name
         ,i_item_id
 LIMIT 100;

-- end query 21 in stream 0 using template query21.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_item_id FROM item WHERE i_current_price BETWEEN 0.99 AND 1.49), filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_date BETWEEN (CAST('2002-02-27' AS DATE) - INTERVAL '30' DAY) AND (CAST('2002-02-27' AS DATE) + INTERVAL '30' DAY)), filtered_inventory AS (SELECT w.w_warehouse_name, i.i_item_id, d.d_date, inv.inv_quantity_on_hand FROM inventory AS inv JOIN filtered_items AS i ON inv.inv_item_sk = i.i_item_sk JOIN warehouse AS w ON inv.inv_warehouse_sk = w.w_warehouse_sk JOIN filtered_dates AS d ON inv.inv_date_sk = d.d_date_sk)
SELECT w_warehouse_name, i_item_id, SUM(CASE WHEN (CAST(d_date AS DATE) < CAST('2002-02-27' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END) AS inv_before, SUM(CASE WHEN (CAST(d_date AS DATE) >= CAST('2002-02-27' AS DATE)) THEN inv_quantity_on_hand ELSE 0 END) AS inv_after FROM filtered_inventory GROUP BY w_warehouse_name, i_item_id HAVING (CASE WHEN inv_before > 0 THEN inv_after / inv_before ELSE NULL END) BETWEEN 2.0 / 3.0 AND 3.0 / 2.0 ORDER BY w_warehouse_name, i_item_id LIMIT 100
```

---

### 103. Kimi Q1-Q30 - Q22

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 22 in stream 0 using template query22.tpl
select i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,avg(inv_quantity_on_hand) qoh
       from inventory
           ,date_dim
           ,item
       where inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and d_month_seq between 1188 and 1188 + 11
       group by rollup(i_product_name
                       ,i_brand
                       ,i_class
                       ,i_category)
order by qoh, i_product_name, i_brand, i_class, i_category
 LIMIT 100;

-- end query 22 in stream 0 using template query22.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1188 AND 1188 + 11)
SELECT i_product_name, i_brand, i_class, i_category, AVG(inv_quantity_on_hand) AS qoh FROM inventory JOIN filtered_dates ON inv_date_sk = d_date_sk JOIN item ON inv_item_sk = i_item_sk GROUP BY ROLLUP (i_product_name, i_brand, i_class, i_category) ORDER BY qoh, i_product_name, i_brand, i_class, i_category LIMIT 100
```

---

### 104. Kimi Q1-Q30 - Q23

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim 
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3) 
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales 
           ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim 
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3) 
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales 
           ...[truncated]
```

---

### 105. Kimi Q1-Q30 - Q24

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 24 in stream 0 using template query24.tpl
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ,customer
    ,customer_address
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_country <> upper(ca_country)
  and s_zip = ca_zip
and s_market_id=8
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = 'beige'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                                 from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name
;
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 24 in stream 0 using template query24.tpl
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ,customer
    ,customer_address
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and ss_store_sk = s_store_sk
  and c_current_addr_sk = ca_address_sk
  and c_birth_country <> upper(ca_country)
  and s_zip = ca_zip
and s_market_id=8
group by c_last_name
        ,c_first_name
        ,s_store_name
        ,ca_state
        ,s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = 'beige'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                                 from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name
;
with ssales as
(select c_last_name
      ,c_first_name
      ,s_store_name
      ,ca_state
      ,s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_profit) netpaid
from store_sales
    ,store_returns
    ,store
    ,item
    ...[truncated]
```

---

### 106. Kimi Q1-Q30 - Q25

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 25 in stream 0 using template query25.tpl
select 
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 ,sum(ss_net_profit) as store_sales_profit
 ,sum(sr_net_loss) as store_returns_loss
 ,sum(cs_net_profit) as catalog_sales_profit
 from
 store_sales
 ,store_returns
 ,catalog_sales
 ,date_dim d1
 ,date_dim d2
 ,date_dim d3
 ,store
 ,item
 where
 d1.d_moy = 4
 and d1.d_year = 2000
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk = ss_item_sk
 and s_store_sk = ss_store_sk
 and ss_customer_sk = sr_customer_sk
 and ss_item_sk = sr_item_sk
 and ss_ticket_number = sr_ticket_number
 and sr_returned_date_sk = d2.d_date_sk
 and d2.d_moy               between 4 and  10
 and d2.d_year              = 2000
 and sr_customer_sk = cs_bill_customer_sk
 and sr_item_sk = cs_item_sk
 and cs_sold_date_sk = d3.d_date_sk
 and d3.d_moy               between 4 and  10 
 and d3.d_year              = 2000
 group by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 order by
 i_item_id
 ,i_item_desc
 ,s_store_id
 ,s_store_name
 LIMIT 100;

-- end query 25 in stream 0 using template query25.tpl

```

#### AFTER (Optimized)
```sql
WITH d1_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 2000), d2_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000), d3_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 10 AND d_year = 2000)
SELECT i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name, SUM(ss.ss_net_profit) AS store_sales_profit, SUM(sr.sr_net_loss) AS store_returns_loss, SUM(cs.cs_net_profit) AS catalog_sales_profit FROM store_sales AS ss JOIN d1_filtered ON ss.ss_sold_date_sk = d1_filtered.d_date_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk JOIN store_returns AS sr ON ss.ss_customer_sk = sr.sr_customer_sk AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number JOIN d2_filtered ON sr.sr_returned_date_sk = d2_filtered.d_date_sk JOIN catalog_sales AS cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk AND sr.sr_item_sk = cs.cs_item_sk JOIN d3_filtered ON cs.cs_sold_date_sk = d3_filtered.d_date_sk GROUP BY i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name ORDER BY i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name LIMIT 100
```

---

### 107. Kimi Q1-Q30 - Q26

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 26 in stream 0 using template query26.tpl
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

-- end query 26 in stream 0 using template query26.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_sales AS (SELECT i.i_item_id, cs.cs_quantity, cs.cs_list_price, cs.cs_coupon_amt, cs.cs_sales_price FROM catalog_sales AS cs JOIN filtered_dates AS d ON cs.cs_sold_date_sk = d.d_date_sk JOIN customer_demographics AS cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON cs.cs_item_sk = i.i_item_sk JOIN promotion AS p ON cs.cs_promo_sk = p.p_promo_sk WHERE cd.cd_gender = 'M' AND cd.cd_marital_status = 'S' AND cd.cd_education_status = 'Unknown' AND p.p_channel_email = 'N' UNION ALL SELECT i.i_item_id, cs.cs_quantity, cs.cs_list_price, cs.cs_coupon_amt, cs.cs_sales_price FROM catalog_sales AS cs JOIN filtered_dates AS d ON cs.cs_sold_date_sk = d.d_date_sk JOIN customer_demographics AS cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON cs.cs_item_sk = i.i_item_sk JOIN promotion AS p ON cs.cs_promo_sk = p.p_promo_sk WHERE cd.cd_gender = 'M' AND cd.cd_marital_status = 'S' AND cd.cd_education_status = 'Unknown' AND p.p_channel_event = 'N')
SELECT i_item_id, AVG(cs_quantity) AS agg1, AVG(cs_list_price) AS agg2, AVG(cs_coupon_amt) AS agg3, AVG(cs_sales_price) AS agg4 FROM filtered_sales GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 108. Kimi Q1-Q30 - Q27

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 27 in stream 0 using template query27.tpl
select i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales, customer_demographics, date_dim, store, item
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_store_sk = s_store_sk and
       ss_cdemo_sk = cd_demo_sk and
       cd_gender = 'F' and
       cd_marital_status = 'D' and
       cd_education_status = 'Secondary' and
       d_year = 1999 and
       s_state in ('MO','AL', 'MI', 'TN', 'LA', 'SC')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 LIMIT 100;

-- end query 27 in stream 0 using template query27.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999), filtered_demo AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'D' AND cd_education_status = 'Secondary'), filtered_store AS (SELECT s_store_sk, s_state FROM store WHERE s_state IN ('MO', 'AL', 'MI', 'TN', 'LA', 'SC'))
SELECT i.i_item_id, fs.s_state, GROUPING(fs.s_state) AS g_state, AVG(ss.ss_quantity) AS agg1, AVG(ss.ss_list_price) AS agg2, AVG(ss.ss_coupon_amt) AS agg3, AVG(ss.ss_sales_price) AS agg4 FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_demo AS fcd ON ss.ss_cdemo_sk = fcd.cd_demo_sk JOIN filtered_store AS fs ON ss.ss_store_sk = fs.s_store_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk GROUP BY ROLLUP (i.i_item_id, fs.s_state) ORDER BY i.i_item_id, fs.s_state LIMIT 100
```

---

### 109. Kimi Q1-Q30 - Q28

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 28 in stream 0 using template query28.tpl
select *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 131 and 131+10 
             or ss_coupon_amt between 16798 and 16798+1000
             or ss_wholesale_cost between 25 and 25+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 145 and 145+10
          or ss_coupon_amt between 14792 and 14792+1000
          or ss_wholesale_cost between 46 and 46+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 150 and 150+10
          or ss_coupon_amt between 6600 and 6600+1000
          or ss_wholesale_cost between 9 and 9+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 91 and 91+10
          or ss_coupon_amt between 13493 and 13493+1000
          or ss_wholesale_cost between 36 and 36+20)) B4,
...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 28 in stream 0 using template query28.tpl
select *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 131 and 131+10 
             or ss_coupon_amt between 16798 and 16798+1000
             or ss_wholesale_cost between 25 and 25+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 145 and 145+10
          or ss_coupon_amt between 14792 and 14792+1000
          or ss_wholesale_cost between 46 and 46+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 150 and 150+10
          or ss_coupon_amt between 6600 and 6600+1000
          or ss_wholesale_cost between 9 and 9+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 91 and 91+10
          or ss_coupon_amt between 13493 and 13493+1000
          or ss_wholesale_cost between 36 and 36+20)) B4,
...[truncated]
```

---

### 110. Kimi Q1-Q30 - Q29

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 29 in stream 0 using template query29.tpl
select  
     i_item_id
    ,i_item_desc
    ,s_store_id
    ,s_store_name
    ,avg(ss_quantity)        as store_sales_quantity
    ,avg(sr_return_quantity) as store_returns_quantity
    ,avg(cs_quantity)        as catalog_sales_quantity
 from
    store_sales
   ,store_returns
   ,catalog_sales
   ,date_dim             d1
   ,date_dim             d2
   ,date_dim             d3
   ,store
   ,item
 where
     d1.d_moy               = 4 
 and d1.d_year              = 1999
 and d1.d_date_sk           = ss_sold_date_sk
 and i_item_sk              = ss_item_sk
 and s_store_sk             = ss_store_sk
 and ss_customer_sk         = sr_customer_sk
 and ss_item_sk             = sr_item_sk
 and ss_ticket_number       = sr_ticket_number
 and sr_returned_date_sk    = d2.d_date_sk
 and d2.d_moy               between 4 and  4 + 3 
 and d2.d_year              = 1999
 and sr_customer_sk         = cs_bill_customer_sk
 and sr_item_sk             = cs_item_sk
 and cs_sold_date_sk        = d3.d_date_sk     
 and d3.d_year              in (1999,1999+1,1999+2)
 group by
    i_item_id
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 order by
    i_item_id 
   ,i_item_desc
   ,s_store_id
   ,s_store_name
 LIMIT 100;

-- end query 29 in stream 0 using template query29.tpl

```

#### AFTER (Optimized)
```sql
WITH d1_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy = 4 AND d_year = 1999), d2_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 4 AND 7 AND d_year = 1999), d3_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000, 2001)), filtered_store_sales AS (SELECT ss.ss_item_sk, ss.ss_store_sk, ss.ss_customer_sk, ss.ss_ticket_number, ss.ss_quantity FROM store_sales AS ss JOIN d1_filtered AS d1 ON ss.ss_sold_date_sk = d1.d_date_sk), filtered_store_returns AS (SELECT sr.sr_customer_sk, sr.sr_item_sk, sr.sr_ticket_number, sr.sr_return_quantity FROM store_returns AS sr JOIN d2_filtered AS d2 ON sr.sr_returned_date_sk = d2.d_date_sk), filtered_catalog_sales AS (SELECT cs.cs_bill_customer_sk, cs.cs_item_sk, cs.cs_quantity FROM catalog_sales AS cs JOIN d3_filtered AS d3 ON cs.cs_sold_date_sk = d3.d_date_sk)
SELECT i.i_item_id, i.i_item_desc, s.s_store_id, s.s_store_name, AVG(ss.ss_quantity) AS store_sales_quantity, AVG(sr.sr_return_quantity) AS store_returns_quantity, AVG(cs.cs_quantity) AS catalog_sales_quantity FROM filtered_store_sales AS ss JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk JOIN filtered_store_returns AS sr ON ss.ss_customer_sk = sr.sr_customer_sk AND ss.ss_item_sk = sr.sr_item_sk AND ss.ss_ticket_number = sr.sr_ticket_number JOIN filtered_catalog_sales AS cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk AND sr.sr_item_sk = cs.cs_item_sk GROUP BY i.i_item_id, i.i_item_desc, s.s_store_i...[truncated]
```

---

### 111. Kimi Q1-Q30 - Q3

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 3 in stream 0 using template query3.tpl
select dt.d_year 
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_sales_price) sum_agg
 from  date_dim dt 
      ,store_sales
      ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
   and store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 816
   and dt.d_moy=11
 group by dt.d_year
      ,item.i_brand
      ,item.i_brand_id
 order by dt.d_year
         ,sum_agg desc
         ,brand_id
 LIMIT 100;

-- end query 3 in stream 0 using template query3.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816), filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11)
SELECT fd.d_year, fi.i_brand_id AS brand_id, fi.i_brand AS brand, SUM(ss.ss_sales_price) AS sum_agg FROM filtered_dates AS fd JOIN store_sales AS ss ON fd.d_date_sk = ss.ss_sold_date_sk JOIN filtered_items AS fi ON ss.ss_item_sk = fi.i_item_sk GROUP BY fd.d_year, fi.i_brand, fi.i_brand_id ORDER BY fd.d_year, sum_agg DESC, brand_id LIMIT 100
```

---

### 112. Kimi Q1-Q30 - Q30

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 30 in stream 0 using template query30.tpl
with customer_total_return as
 (select wr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(wr_return_amt) as ctr_total_return
 from web_returns
     ,date_dim
     ,customer_address
 where wr_returned_date_sk = d_date_sk 
   and d_year =2002
   and wr_returning_addr_sk = ca_address_sk 
 group by wr_returning_customer_sk
         ,ca_state)
  select c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
       ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
       ,c_last_review_date_sk,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'IN'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,c_preferred_cust_flag
                  ,c_birth_day,c_birth_month,c_birth_year,c_birth_country,c_login,c_email_address
                  ,c_last_review_date_sk,ctr_total_return
 LIMIT 100;

-- end query 30 in stream 0 using template query30.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_returns AS (SELECT wr.wr_returning_customer_sk, ra.ca_state AS ctr_state, wr.wr_return_amt FROM web_returns AS wr JOIN date_dim AS d ON wr.wr_returned_date_sk = d.d_date_sk JOIN customer_address AS ra ON wr.wr_returning_addr_sk = ra.ca_address_sk JOIN customer AS c ON wr.wr_returning_customer_sk = c.c_customer_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk WHERE d.d_year = 2002 AND ca.ca_state = 'IN'), customer_total_return AS (SELECT ctr_customer_sk, ctr_state, SUM(wr_return_amt) AS ctr_total_return FROM filtered_returns GROUP BY ctr_customer_sk, ctr_state), state_avg_return AS (SELECT ctr_state, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_state)
SELECT c.c_customer_id, c.c_salutation, c.c_first_name, c.c_last_name, c.c_preferred_cust_flag, c.c_birth_day, c.c_birth_month, c.c_birth_year, c.c_birth_country, c.c_login, c.c_email_address, c.c_last_review_date_sk, ctr1.ctr_total_return FROM customer_total_return AS ctr1 JOIN state_avg_return AS sar ON ctr1.ctr_state = sar.ctr_state JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id, c.c_salutation, c.c_first_name, c.c_last_name, c.c_preferred_cust_flag, c.c_birth_day, c.c_birth_month, c.c_birth_year, c.c_birth_country, c.c_login, c.c_email_address, c.c_last_review_date_sk, ctr1.ctr_total_return LIMIT 100
```

---

### 113. Kimi Q1-Q30 - Q4

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 4 in stream 0 using template query4.tpl
with year_total as (
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum(((ss_ext_list_price-ss_ext_wholesale_cost-ss_ext_discount_amt)+ss_ext_sales_price)/2) year_total
       ,'s' sale_type
 from customer
     ,store_sales
     ,date_dim
 where c_customer_sk = ss_customer_sk
   and ss_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
         ,c_preferred_cust_flag
         ,c_birth_country
         ,c_login
         ,c_email_address
         ,d_year
 union all
 select c_customer_id customer_id
       ,c_first_name customer_first_name
       ,c_last_name customer_last_name
       ,c_preferred_cust_flag customer_preferred_cust_flag
       ,c_birth_country customer_birth_country
       ,c_login customer_login
       ,c_email_address customer_email_address
       ,d_year dyear
       ,sum((((cs_ext_list_price-cs_ext_wholesale_cost-cs_ext_discount_amt)+cs_ext_sales_price)/2) ) year_total
       ,'c' sale_type
 from customer
     ,catalog_sales
     ,date_dim
 where c_customer_sk = cs_bill_customer_sk
   and cs_sold_date_sk = d_date_sk
 group by c_customer_id
         ,c_first_name
         ,c_last_name
    ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (1999, 2000) GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer, catalog_sales, date_dim WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk AND d_year IN (1999, 2000) GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS ...[truncated]
```

---

### 114. Kimi Q1-Q30 - Q5

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 5 in stream 0 using template query5.tpl
with ssr as
 (select s_store_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as "returns",
        sum(net_loss) as profit_loss
 from
  ( select  ss_store_sk as store_sk,
            ss_sold_date_sk  as date_sk,
            ss_ext_sales_price as sales_price,
            ss_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as net_loss
    from store_sales
    union all
    select sr_store_sk as store_sk,
           sr_returned_date_sk as date_sk,
           cast(0 as decimal(7,2)) as sales_price,
           cast(0 as decimal(7,2)) as profit,
           sr_return_amt as return_amt,
           sr_net_loss as net_loss
    from store_returns
   ) salesreturns,
     date_dim,
     store
 where date_sk = d_date_sk
       and d_date between cast('2000-08-19' as date) 
                  and (cast('2000-08-19' as date) + INTERVAL 14 DAY)
       and store_sk = s_store_sk
 group by s_store_id)
 ,
 csr as
 (select cp_catalog_page_id,
        sum(sales_price) as sales,
        sum(profit) as profit,
        sum(return_amt) as "returns",
        sum(net_loss) as profit_loss
 from
  ( select  cs_catalog_page_sk as page_sk,
            cs_sold_date_sk  as date_sk,
            cs_ext_sales_price as sales_price,
            cs_net_profit as profit,
            cast(0 as decimal(7,2)) as return_amt,
            cast(0 as decimal(7,2)) as ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_sold_date_sk AS date_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales UNION ALL SELECT sr_store_sk AS store_sk, sr_returned_date_sk AS date_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS return_amt, sr_net_loss AS net_loss FROM store_returns) AS salesreturns, date_dim, store WHERE date_sk = d_date_sk AND d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY) AND store_sk = s_store_sk GROUP BY s_store_id), csr AS (SELECT cp_catalog_page_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT cs_catalog_page_sk AS page_sk, cs_sold_date_sk AS date_sk, cs_ext_sales_price AS sales_price, cs_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM catalog_sales UNION ALL SELECT cr_catalog_page_sk AS page_sk, cr_returned_date_sk AS date_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, cr_return_amount AS return_amt, cr_net_loss AS net_loss FROM catalog_returns) AS salesreturns, date_dim, catalog_page WHERE date_sk = d_date_sk AND d_date BETWEEN CAST('2000-08-19'...[truncated]
```

---

### 115. Kimi Q1-Q30 - Q6

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 6 in stream 0 using template query6.tpl
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

-- end query 6 in stream 0 using template query6.tpl

```

#### AFTER (Optimized)
```sql
WITH target_month AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), category_avg AS (SELECT i_category, AVG(i_current_price) * 1.2 AS price_threshold FROM item GROUP BY i_category)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN store_sales AS s ON c.c_customer_sk = s.ss_customer_sk JOIN date_dim AS d ON s.ss_sold_date_sk = d.d_date_sk JOIN target_month AS tm ON d.d_month_seq = tm.d_month_seq JOIN item AS i ON s.ss_item_sk = i.i_item_sk JOIN category_avg AS ca ON i.i_category = ca.i_category WHERE i.i_current_price > ca.price_threshold GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 116. Kimi Q1-Q30 - Q7

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 7 in stream 0 using template query7.tpl
select i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from store_sales, customer_demographics, date_dim, item, promotion
 where ss_sold_date_sk = d_date_sk and
       ss_item_sk = i_item_sk and
       ss_cdemo_sk = cd_demo_sk and
       ss_promo_sk = p_promo_sk and
       cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'College' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 2001 
 group by i_item_id
 order by i_item_id
 LIMIT 100;

-- end query 7 in stream 0 using template query7.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), promo_filtered_sales AS (SELECT i.i_item_id, ss.ss_quantity, ss.ss_list_price, ss.ss_coupon_amt, ss.ss_sales_price FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN filtered_demographics AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN promotion AS p ON ss.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_email = 'N' UNION ALL SELECT i.i_item_id, ss.ss_quantity, ss.ss_list_price, ss.ss_coupon_amt, ss.ss_sales_price FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN filtered_demographics AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN promotion AS p ON ss.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_event = 'N')
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM promo_filtered_sales GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 117. Kimi Q1-Q30 - Q8

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 8 in stream 0 using template query8.tpl
select s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substr(ca_zip,1,5) IN (
                          '47602','16704','35863','28577','83910','36201',
                          '58412','48162','28055','41419','80332',
                          '38607','77817','24891','16226','18410',
                          '21231','59345','13918','51089','20317',
                          '17167','54585','67881','78366','47770',
                          '18360','51717','73108','14440','21800',
                          '89338','45859','65501','34948','25973',
                          '73219','25333','17291','10374','18829',
                          '60736','82620','41351','52094','19326',
                          '25214','54207','40936','21814','79077',
                          '25178','75742','77454','30621','89193',
                          '27369','41232','48567','83041','71948',
                          '37119','68341','14073','16891','62878',
                          '49130','19833','24286','27700','40979',
                          '50412','81504','94835','84844','71954',
                          '39503','57649','18434','24987','12350',
                          '86379','27413','44529','98569','16515',
                          '27287','24255','21094','16005','56436',
    ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 8 in stream 0 using template query8.tpl
select s_store_name
      ,sum(ss_net_profit)
 from store_sales
     ,date_dim
     ,store,
     (select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM customer_address
      WHERE substr(ca_zip,1,5) IN (
                          '47602','16704','35863','28577','83910','36201',
                          '58412','48162','28055','41419','80332',
                          '38607','77817','24891','16226','18410',
                          '21231','59345','13918','51089','20317',
                          '17167','54585','67881','78366','47770',
                          '18360','51717','73108','14440','21800',
                          '89338','45859','65501','34948','25973',
                          '73219','25333','17291','10374','18829',
                          '60736','82620','41351','52094','19326',
                          '25214','54207','40936','21814','79077',
                          '25178','75742','77454','30621','89193',
                          '27369','41232','48567','83041','71948',
                          '37119','68341','14073','16891','62878',
                          '49130','19833','24286','27700','40979',
                          '50412','81504','94835','84844','71954',
                          '39503','57649','18434','24987','12350',
                          '86379','27413','44529','98569','16515',
                          '27287','24255','21094','16005','56436',
    ...[truncated]
```

---

### 118. Kimi Q1-Q30 - Q9

**Source**: Kimi Q1-Q30

#### BEFORE (Original)
```sql
-- start query 9 in stream 0 using template query9.tpl
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
            then (select avg(ss_ext_sales_price...[truncated]
```

#### AFTER (Optimized)
```sql
WITH sales_stats AS (SELECT COUNT(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 END) AS cnt1, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_sales_price END) AS avg_price1, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_profit END) AS avg_profit1, COUNT(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 END) AS cnt2, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_sales_price END) AS avg_price2, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_profit END) AS avg_profit2, COUNT(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 END) AS cnt3, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_sales_price END) AS avg_price3, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_profit END) AS avg_profit3, COUNT(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 END) AS cnt4, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_sales_price END) AS avg_price4, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_profit END) AS avg_profit4, COUNT(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 END) AS cnt5, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_sales_price END) AS avg_price5, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_profit END) AS avg_profit5 FROM store_sales)
SELECT CASE WHEN s.cnt1 > 2972190 THEN s.avg_price1 ELSE s.avg_profit1 END AS bucket1, CASE WHEN s.cnt2 > 4505785 THEN s.avg_price2 ELSE s.avg_profit2 END AS bucket2, CASE WHEN s.cnt3 > 1575726 THEN s.avg_price3 ELSE s.avg_profit3 END AS bucket3, CASE WHEN s.c...[truncated]
```

---

### 119. Kimi Q31-Q99 - Q31

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 31 in stream 0 using template query31.tpl
with ss as
 (select ca_county,d_qoy, d_year,sum(ss_ext_sales_price) as store_sales
 from store_sales,date_dim,customer_address
 where ss_sold_date_sk = d_date_sk
  and ss_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year),
 ws as
 (select ca_county,d_qoy, d_year,sum(ws_ext_sales_price) as web_sales
 from web_sales,date_dim,customer_address
 where ws_sold_date_sk = d_date_sk
  and ws_bill_addr_sk=ca_address_sk
 group by ca_county,d_qoy, d_year)
 select 
        ss1.ca_county
       ,ss1.d_year
       ,ws2.web_sales/ws1.web_sales web_q1_q2_increase
       ,ss2.store_sales/ss1.store_sales store_q1_q2_increase
       ,ws3.web_sales/ws2.web_sales web_q2_q3_increase
       ,ss3.store_sales/ss2.store_sales store_q2_q3_increase
 from
        ss ss1
       ,ss ss2
       ,ss ss3
       ,ws ws1
       ,ws ws2
       ,ws ws3
 where
    ss1.d_qoy = 1
    and ss1.d_year = 2000
    and ss1.ca_county = ss2.ca_county
    and ss2.d_qoy = 2
    and ss2.d_year = 2000
 and ss2.ca_county = ss3.ca_county
    and ss3.d_qoy = 3
    and ss3.d_year = 2000
    and ss1.ca_county = ws1.ca_county
    and ws1.d_qoy = 1
    and ws1.d_year = 2000
    and ws1.ca_county = ws2.ca_county
    and ws2.d_qoy = 2
    and ws2.d_year = 2000
    and ws1.ca_county = ws3.ca_county
    and ws3.d_qoy = 3
    and ws3.d_year =2000
    and case when ws1.web_sales > 0 then ws2.web_sales/ws1.web_sales else null end 
       > case when ss1.store_sales > 0 then ss2.st...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss AS (SELECT ca_county, d_qoy, d_year, SUM(ss_ext_sales_price) AS store_sales FROM store_sales, date_dim, customer_address WHERE ss_sold_date_sk = d_date_sk AND ss_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year), ws AS (SELECT ca_county, d_qoy, d_year, SUM(ws_ext_sales_price) AS web_sales FROM web_sales, date_dim, customer_address WHERE ws_sold_date_sk = d_date_sk AND ws_bill_addr_sk = ca_address_sk GROUP BY ca_county, d_qoy, d_year) SELECT ss1.ca_county, ss1.d_year, ws2.web_sales / ws1.web_sales AS web_q1_q2_increase, ss2.store_sales / ss1.store_sales AS store_q1_q2_increase, ws3.web_sales / ws2.web_sales AS web_q2_q3_increase, ss3.store_sales / ss2.store_sales AS store_q2_q3_increase FROM ss AS ss1, ss AS ss2, ss AS ss3, ws AS ws1, ws AS ws2, ws AS ws3 WHERE ss1.d_qoy = 1 AND ss1.d_year = 2000 AND ss1.ca_county = ss2.ca_county AND ss2.d_qoy = 2 AND ss2.d_year = 2000 AND ss2.ca_county = ss3.ca_county AND ss3.d_qoy = 3 AND ss3.d_year = 2000 AND ss1.ca_county = ws1.ca_county AND ws1.d_qoy = 1 AND ws1.d_year = 2000 AND ws1.ca_county = ws2.ca_county AND ws2.d_qoy = 2 AND ws2.d_year = 2000 AND ws1.ca_county = ws3.ca_county AND ws3.d_qoy = 3 AND ws3.d_year = 2000 AND CASE WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales ELSE NULL END > CASE WHEN ss1.store_sales > 0 THEN ss2.store_sales / ss1.store_sales ELSE NULL END AND CASE WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales ELSE NULL END > CASE WHEN ss2.store_sales > 0 THEN ss3.store_sales / ss2....[truncated]
```

---

### 120. Kimi Q31-Q99 - Q32

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 32 in stream 0 using template query32.tpl
select sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 29
and i_item_sk = cs_item_sk 
and d_date between '1999-01-07' and 
        (cast('1999-01-07' as date) + INTERVAL 90 DAY)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1999-01-07' and
                             (cast('1999-01-07' as date) + INTERVAL 90 DAY)
          and d_date_sk = cs_sold_date_sk 
      )
 LIMIT 100;

-- end query 32 in stream 0 using template query32.tpl

```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '1999-01-07' AND (CAST('1999-01-07' AS DATE) + INTERVAL '90' DAY)), target_items AS (SELECT i_item_sk FROM item WHERE i_manufact_id = 29), item_avg_discount AS (SELECT cs_item_sk, 1.3 * AVG(cs_ext_discount_amt) AS avg_discount_threshold FROM catalog_sales JOIN date_range ON cs_sold_date_sk = d_date_sk GROUP BY cs_item_sk)
SELECT SUM(cs.cs_ext_discount_amt) AS "excess discount amount" FROM catalog_sales AS cs JOIN target_items AS ti ON cs.cs_item_sk = ti.i_item_sk JOIN date_range AS dr ON cs.cs_sold_date_sk = dr.d_date_sk JOIN item_avg_discount AS iad ON cs.cs_item_sk = iad.cs_item_sk WHERE cs.cs_ext_discount_amt > iad.avg_discount_threshold LIMIT 100
```

---

### 121. Kimi Q31-Q99 - Q33

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 33 in stream 0 using template query33.tpl
with ss as (
 select
          i_manufact_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2002
 and     d_moy                   = 1
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 cs as (
 select
          i_manufact_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2002
 and     d_moy                   = 1
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -5 
 group by i_manufact_id),
 ws as (
 select
          i_manufact_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_manufact_id               in (select
  i_manufact_id
from
 item
where i_category in ('Home'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_y...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss AS (SELECT i_manufact_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales, date_dim, customer_address, item WHERE i_manufact_id IN (SELECT i_manufact_id FROM item WHERE i_category IN ('Home')) AND ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2002 AND d_moy = 1 AND ss_addr_sk = ca_address_sk AND ca_gmt_offset = -5 GROUP BY i_manufact_id), cs AS (SELECT i_manufact_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales, date_dim, customer_address, item WHERE i_manufact_id IN (SELECT i_manufact_id FROM item WHERE i_category IN ('Home')) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2002 AND d_moy = 1 AND cs_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -5 GROUP BY i_manufact_id), ws AS (SELECT i_manufact_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales, date_dim, customer_address, item WHERE i_manufact_id IN (SELECT i_manufact_id FROM item WHERE i_category IN ('Home')) AND ws_item_sk = i_item_sk AND ws_sold_date_sk = d_date_sk AND d_year = 2002 AND d_moy = 1 AND ws_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -5 GROUP BY i_manufact_id) SELECT i_manufact_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_manufact_id ORDER BY total_sales LIMIT 100
```

---

### 122. Kimi Q31-Q99 - Q34

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 34 in stream 0 using template query34.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (date_dim.d_dom between 1 and 3 or date_dim.d_dom between 25 and 28)
    and (household_demographics.hd_buy_potential = '1001-5000' or
         household_demographics.hd_buy_potential = '0-500')
    and household_demographics.hd_vehicle_count > 0
    and (case when household_demographics.hd_vehicle_count > 0 
	then household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count 
	else null 
	end)  > 1.2
    and date_dim.d_year in (1998,1998+1,1998+2)
    and store.s_county in ('Ziebach County','Daviess County','Walker County','Richland County',
                           'Barrow County','Franklin Parish','Williamson County','Luce County')
    group by ss_ticket_number,ss_customer_sk) dn,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 15 and 20
    order by c_last_name,c_first_name,c_salutation,c_preferred_cust_flag desc, ss_ticket_number;

-- end query 34 in stream 0 using template query34.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_sales AS (SELECT ss.ss_ticket_number, ss.ss_customer_sk FROM store_sales AS ss JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk JOIN household_demographics AS hd ON ss.ss_hdemo_sk = hd.hd_demo_sk WHERE d.d_dom BETWEEN 1 AND 3 AND hd.hd_buy_potential = '1001-5000' AND hd.hd_vehicle_count > 0 AND (CASE WHEN hd.hd_vehicle_count > 0 THEN hd.hd_dep_count / hd.hd_vehicle_count ELSE NULL END) > 1.2 AND d.d_year IN (1998, 1999, 2000) AND s.s_county IN ('Ziebach County', 'Daviess County', 'Walker County', 'Richland County', 'Barrow County', 'Franklin Parish', 'Williamson County', 'Luce County') UNION ALL SELECT ss.ss_ticket_number, ss.ss_customer_sk FROM store_sales AS ss JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk JOIN household_demographics AS hd ON ss.ss_hdemo_sk = hd.hd_demo_sk WHERE d.d_dom BETWEEN 1 AND 3 AND hd.hd_buy_potential = '0-500' AND hd.hd_vehicle_count > 0 AND (CASE WHEN hd.hd_vehicle_count > 0 THEN hd.hd_dep_count / hd.hd_vehicle_count ELSE NULL END) > 1.2 AND d.d_year IN (1998, 1999, 2000) AND s.s_county IN ('Ziebach County', 'Daviess County', 'Walker County', 'Richland County', 'Barrow County', 'Franklin Parish', 'Williamson County', 'Luce County') UNION ALL SELECT ss.ss_ticket_number, ss.ss_customer_sk FROM store_sales AS ss JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk JOIN househ...[truncated]
```

---

### 123. Kimi Q31-Q99 - Q35

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 35 in stream 0 using template query35.tpl
select  
  ca_state,
  cd_gender,
  cd_marital_status,
  cd_dep_count,
  count(*) cnt1,
  max(cd_dep_count),
  sum(cd_dep_count),
  max(cd_dep_count),
  cd_dep_employed_count,
  count(*) cnt2,
  max(cd_dep_employed_count),
  sum(cd_dep_employed_count),
  max(cd_dep_employed_count),
  cd_dep_college_count,
  count(*) cnt3,
  max(cd_dep_college_count),
  sum(cd_dep_college_count),
  max(cd_dep_college_count)
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2001 and
                d_qoy < 4) and
   (exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4) or 
    exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2001 and
                  d_qoy < 4))
 group by ca_state,
          cd_gender,
          cd_marital_status,
          cd_dep_count,
          cd_dep_employed_count,
          cd_dep_college_count
 order by ca_state,
      ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy < 4), store_customers AS (SELECT DISTINCT ss_customer_sk AS customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), web_customers AS (SELECT DISTINCT ws_bill_customer_sk AS customer_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), catalog_customers AS (SELECT DISTINCT cs_ship_customer_sk AS customer_sk FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk), branch_web AS (SELECT ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count, cd.cd_dep_employed_count, cd.cd_dep_college_count FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk JOIN store_customers AS sc ON c.c_customer_sk = sc.customer_sk JOIN web_customers AS wc ON c.c_customer_sk = wc.customer_sk), branch_catalog AS (SELECT ca.ca_state, cd.cd_gender, cd.cd_marital_status, cd.cd_dep_count, cd.cd_dep_employed_count, cd.cd_dep_college_count FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk JOIN store_customers AS sc ON c.c_customer_sk = sc.customer_sk JOIN catalog_customers AS cc ON c.c_customer_sk = cc.customer_sk), combined_customers AS (SELECT * FROM branch_web UNION ALL SELECT * FROM branch_catalog)
SELECT ca_state, cd_gender, cd_marital_stat...[truncated]
```

---

### 124. Kimi Q31-Q99 - Q36

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 36 in stream 0 using template query36.tpl
select 
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,item
   ,store
 where
    d1.d_year = 2002 
 and d1.d_date_sk = ss_sold_date_sk
 and i_item_sk  = ss_item_sk 
 and s_store_sk  = ss_store_sk
 and s_state in ('SD','TN','GA','SC',
                 'MO','AL','MI','OH')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
 LIMIT 100;

-- end query 36 in stream 0 using template query36.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002), filtered_stores AS (SELECT s_store_sk FROM store WHERE s_state IN ('SD', 'TN', 'GA', 'SC', 'MO', 'AL', 'MI', 'OH'))
SELECT SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin, i_category, i_class, GROUPING(i_category) + GROUPING(i_class) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY SUM(ss_net_profit) / SUM(ss_ext_sales_price) ASC) AS rank_within_parent FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores ON ss_store_sk = s_store_sk JOIN item ON i_item_sk = ss_item_sk GROUP BY ROLLUP (i_category, i_class) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category END, rank_within_parent LIMIT 100
```

---

### 125. Kimi Q31-Q99 - Q37

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 37 in stream 0 using template query37.tpl
select i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, catalog_sales
 where i_current_price between 45 and 45 + 30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-02-21' as date) and (cast('1999-02-21' as date) + INTERVAL 60 DAY)
 and i_manufact_id in (856,707,1000,747)
 and inv_quantity_on_hand between 100 and 500
 and cs_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 LIMIT 100;

-- end query 37 in stream 0 using template query37.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_current_price FROM item WHERE i_current_price BETWEEN 45 AND 45 + 30 AND i_manufact_id IN (856, 707, 1000, 747)), filtered_inventory AS (SELECT inv_item_sk, inv_date_sk FROM inventory WHERE inv_quantity_on_hand BETWEEN 100 AND 500), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1999-02-21' AS DATE) AND (CAST('1999-02-21' AS DATE) + INTERVAL '60' DAY))
SELECT fi.i_item_id, fi.i_item_desc, fi.i_current_price FROM filtered_items AS fi JOIN filtered_inventory AS inv ON fi.i_item_sk = inv.inv_item_sk JOIN filtered_dates AS d ON inv.inv_date_sk = d.d_date_sk JOIN catalog_sales AS cs ON fi.i_item_sk = cs.cs_item_sk GROUP BY fi.i_item_id, fi.i_item_desc, fi.i_current_price ORDER BY fi.i_item_id LIMIT 100
```

---

### 126. Kimi Q31-Q99 - Q38

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 38 in stream 0 using template query38.tpl
select count(*) from (
    select distinct c_last_name, c_first_name, d_date
    from store_sales, date_dim, customer
          where store_sales.ss_sold_date_sk = date_dim.d_date_sk
      and store_sales.ss_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from catalog_sales, date_dim, customer
          where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
      and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
  intersect
    select distinct c_last_name, c_first_name, d_date
    from web_sales, date_dim, customer
          where web_sales.ws_sold_date_sk = date_dim.d_date_sk
      and web_sales.ws_bill_customer_sk = customer.c_customer_sk
      and d_month_seq between 1183 and 1183 + 11
) hot_cust
 LIMIT 100;

-- end query 38 in stream 0 using template query38.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1183 AND 1183 + 11), store_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN customer ON ss_customer_sk = c_customer_sk), catalog_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk), web_customers AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk)
SELECT COUNT(*) FROM (SELECT * FROM store_customers INTERSECT SELECT * FROM catalog_customers INTERSECT SELECT * FROM web_customers) AS hot_cust LIMIT 100
```

---

### 127. Kimi Q31-Q99 - Q39

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 39 in stream 0 using template query39.tpl
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
        and inv_warehouse_sk = w_warehouse_sk
        and inv_date_sk = d_date_sk
        and d_year =1998
      group by w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy) foo
 where case mean when 0 then 0 else stdev/mean end > 1)
select inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean, inv1.cov
        ,inv2.w_warehouse_sk,inv2.i_item_sk,inv2.d_moy,inv2.mean, inv2.cov
from inv inv1,inv inv2
where inv1.i_item_sk = inv2.i_item_sk
  and inv1.w_warehouse_sk =  inv2.w_warehouse_sk
  and inv1.d_moy=1
  and inv2.d_moy=1+1
order by inv1.w_warehouse_sk,inv1.i_item_sk,inv1.d_moy,inv1.mean,inv1.cov
        ,inv2.d_moy,inv2.mean, inv2.cov
;
with inv as
(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
       ,stdev,mean, case mean when 0 then null else stdev/mean end cov
 from(select w_warehouse_name,w_warehouse_sk,i_item_sk,d_moy
            ,stddev_samp(inv_quantity_on_hand) stdev,avg(inv_quantity_on_hand) mean
      from inventory
          ,item
          ,warehouse
          ,date_dim
      where inv_item_sk = i_item_sk
  ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH inv AS (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, stdev, mean, CASE mean WHEN 0 THEN NULL ELSE stdev / mean END AS cov FROM (SELECT w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy, STDDEV_SAMP(inv_quantity_on_hand) AS stdev, AVG(inv_quantity_on_hand) AS mean FROM inventory, item, warehouse, date_dim WHERE inv_item_sk = i_item_sk AND inv_warehouse_sk = w_warehouse_sk AND inv_date_sk = d_date_sk AND d_year = 1998 AND d_moy IN (1, 2) GROUP BY w_warehouse_name, w_warehouse_sk, i_item_sk, d_moy) AS foo WHERE CASE mean WHEN 0 THEN 0 ELSE stdev / mean END > 1) SELECT inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.w_warehouse_sk, inv2.i_item_sk, inv2.d_moy, inv2.mean, inv2.cov FROM inv AS inv1, inv AS inv2 WHERE inv1.i_item_sk = inv2.i_item_sk AND inv1.w_warehouse_sk = inv2.w_warehouse_sk AND inv1.d_moy = 1 AND inv2.d_moy = 1 + 1 ORDER BY inv1.w_warehouse_sk, inv1.i_item_sk, inv1.d_moy, inv1.mean, inv1.cov, inv2.d_moy, inv2.mean, inv2.cov
```

---

### 128. Kimi Q31-Q99 - Q40

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 40 in stream 0 using template query40.tpl
select 
   w_state
  ,i_item_id
  ,sum(case when (cast(d_date as date) < cast ('2001-04-02' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_before
  ,sum(case when (cast(d_date as date) >= cast ('2001-04-02' as date)) 
 		then cs_sales_price - coalesce(cr_refunded_cash,0) else 0 end) as sales_after
 from
   catalog_sales left outer join catalog_returns on
       (cs_order_number = cr_order_number 
        and cs_item_sk = cr_item_sk)
  ,warehouse 
  ,item
  ,date_dim
 where
     i_current_price between 0.99 and 1.49
 and i_item_sk          = cs_item_sk
 and cs_warehouse_sk    = w_warehouse_sk 
 and cs_sold_date_sk    = d_date_sk
 and d_date between (cast ('2001-04-02' as date) - INTERVAL 30 DAY)
                and (cast ('2001-04-02' as date) + INTERVAL 30 DAY) 
 group by
    w_state,i_item_id
 order by w_state,i_item_id
 LIMIT 100;

-- end query 40 in stream 0 using template query40.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_item AS (SELECT i_item_sk, i_item_id FROM item WHERE i_current_price BETWEEN 0.99 AND 1.49), filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_date BETWEEN (CAST('2001-04-02' AS DATE) - INTERVAL '30' DAY) AND (CAST('2001-04-02' AS DATE) + INTERVAL '30' DAY))
SELECT w_state, i_item_id, SUM(CASE WHEN (CAST(d_date AS DATE) < CAST('2001-04-02' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_before, SUM(CASE WHEN (CAST(d_date AS DATE) >= CAST('2001-04-02' AS DATE)) THEN cs_sales_price - COALESCE(cr_refunded_cash, 0) ELSE 0 END) AS sales_after FROM catalog_sales LEFT OUTER JOIN catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk) JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk JOIN filtered_item ON i_item_sk = cs_item_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_sk GROUP BY w_state, i_item_id ORDER BY w_state, i_item_id LIMIT 100
```

---

### 129. Kimi Q31-Q99 - Q41

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 41 in stream 0 using template query41.tpl
select distinct(i_product_name)
 from item i1
 where i_manufact_id between 748 and 748+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'gainsboro' or i_color = 'aquamarine') and 
        (i_units = 'Ounce' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'chiffon' or i_color = 'violet') and
        (i_units = 'Ton' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'chartreuse' or i_color = 'blue') and
        (i_units = 'Each' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'dodger') and
        (i_units = 'Bunch' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'tomato') and 
        (i_units = 'Tbl' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'almond' or i_color = 'lime') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'small'...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 41 in stream 0 using template query41.tpl
select distinct(i_product_name)
 from item i1
 where i_manufact_id between 748 and 748+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'gainsboro' or i_color = 'aquamarine') and 
        (i_units = 'Ounce' or i_units = 'Dozen') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'chiffon' or i_color = 'violet') and
        (i_units = 'Ton' or i_units = 'Pound') and
        (i_size = 'extra large' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'chartreuse' or i_color = 'blue') and
        (i_units = 'Each' or i_units = 'Oz') and
        (i_size = 'N/A' or i_size = 'large')
        ) or
        (i_category = 'Men' and
        (i_color = 'tan' or i_color = 'dodger') and
        (i_units = 'Bunch' or i_units = 'Tsp') and
        (i_size = 'medium' or i_size = 'economy')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'blanched' or i_color = 'tomato') and 
        (i_units = 'Tbl' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'economy')
        ) or
        (i_category = 'Women' and
        (i_color = 'almond' or i_color = 'lime') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'extra large' or i_size = 'small'...[truncated]
```

---

### 130. Kimi Q31-Q99 - Q42

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 42 in stream 0 using template query42.tpl
select dt.d_year
 	,item.i_category_id
 	,item.i_category
 	,sum(ss_ext_sales_price)
 from 	date_dim dt
 	,store_sales
 	,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
 	and store_sales.ss_item_sk = item.i_item_sk
 	and item.i_manager_id = 1  	
 	and dt.d_moy=11
 	and dt.d_year=2002
 group by 	dt.d_year
 		,item.i_category_id
 		,item.i_category
 order by       sum(ss_ext_sales_price) desc,dt.d_year
 		,item.i_category_id
 		,item.i_category
 LIMIT 100;

-- end query 42 in stream 0 using template query42.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11 AND d_year = 2002), filtered_items AS (SELECT i_item_sk, i_category_id, i_category FROM item WHERE i_manager_id = 1)
SELECT fd.d_year, fi.i_category_id, fi.i_category, SUM(ss.ss_ext_sales_price) FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ss.ss_item_sk = fi.i_item_sk GROUP BY fd.d_year, fi.i_category_id, fi.i_category ORDER BY SUM(ss.ss_ext_sales_price) DESC, fd.d_year, fi.i_category_id, fi.i_category LIMIT 100
```

---

### 131. Kimi Q31-Q99 - Q43

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 43 in stream 0 using template query43.tpl
select s_store_name, s_store_id,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from date_dim, store_sales, store
 where d_date_sk = ss_sold_date_sk and
       s_store_sk = ss_store_sk and
       s_gmt_offset = -5 and
       d_year = 2000 
 group by s_store_name, s_store_id
 order by s_store_name, s_store_id,sun_sales,mon_sales,tue_sales,wed_sales,thu_sales,fri_sales,sat_sales
 LIMIT 100;

-- end query 43 in stream 0 using template query43.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_day_name FROM date_dim WHERE d_year = 2000), filtered_stores AS (SELECT s_store_sk, s_store_name, s_store_id FROM store WHERE s_gmt_offset = -5)
SELECT s.s_store_name, s.s_store_id, SUM(CASE WHEN d.d_day_name = 'Sunday' THEN ss.ss_sales_price END) AS sun_sales, SUM(CASE WHEN d.d_day_name = 'Monday' THEN ss.ss_sales_price END) AS mon_sales, SUM(CASE WHEN d.d_day_name = 'Tuesday' THEN ss.ss_sales_price END) AS tue_sales, SUM(CASE WHEN d.d_day_name = 'Wednesday' THEN ss.ss_sales_price END) AS wed_sales, SUM(CASE WHEN d.d_day_name = 'Thursday' THEN ss.ss_sales_price END) AS thu_sales, SUM(CASE WHEN d.d_day_name = 'Friday' THEN ss.ss_sales_price END) AS fri_sales, SUM(CASE WHEN d.d_day_name = 'Saturday' THEN ss.ss_sales_price END) AS sat_sales FROM filtered_dates AS d JOIN store_sales AS ss ON d.d_date_sk = ss.ss_sold_date_sk JOIN filtered_stores AS s ON s.s_store_sk = ss.ss_store_sk GROUP BY s.s_store_name, s.s_store_id ORDER BY s.s_store_name, s.s_store_id, sun_sales, mon_sales, tue_sales, wed_sales, thu_sales, fri_sales, sat_sales LIMIT 100
```

---

### 132. Kimi Q31-Q99 - Q44

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 44 in stream 0 using template query44.tpl
select asceding.rnk, i1.i_product_name best_performing, i2.i_product_name worst_performing
from(select *
     from (select item_sk,rank() over (order by rank_col asc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col 
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V1)V11
     where rnk  < 11) asceding,
    (select *
     from (select item_sk,rank() over (order by rank_col desc) rnk
           from (select ss_item_sk item_sk,avg(ss_net_profit) rank_col
                 from store_sales ss1
                 where ss_store_sk = 146
                 group by ss_item_sk
                 having avg(ss_net_profit) > 0.9*(select avg(ss_net_profit) rank_col
                                                  from store_sales
                                                  where ss_store_sk = 146
                                                    and ss_addr_sk is null
                                                  group by ss_store_sk))V2)V21
     where rnk  < 11) de...[truncated]
```

#### AFTER (Optimized)
```sql
WITH threshold_calc AS (SELECT AVG(ss_net_profit) * 0.9 AS profit_threshold FROM store_sales WHERE ss_store_sk = 146 AND ss_addr_sk IS NULL GROUP BY ss_store_sk), qualified_items AS (SELECT ss1.ss_item_sk AS item_sk, AVG(ss1.ss_net_profit) AS rank_col FROM store_sales AS ss1 CROSS JOIN threshold_calc WHERE ss1.ss_store_sk = 146 GROUP BY ss1.ss_item_sk HAVING AVG(ss1.ss_net_profit) > threshold_calc.profit_threshold), ascending AS (SELECT item_sk, rnk FROM (SELECT item_sk, RANK() OVER (ORDER BY rank_col ASC) AS rnk FROM qualified_items) AS t WHERE rnk < 11), descending AS (SELECT item_sk, rnk FROM (SELECT item_sk, RANK() OVER (ORDER BY rank_col DESC) AS rnk FROM qualified_items) AS t WHERE rnk < 11)
SELECT a.rnk, i1.i_product_name AS best_performing, i2.i_product_name AS worst_performing FROM ascending AS a JOIN descending AS d ON a.rnk = d.rnk JOIN item AS i1 ON i1.i_item_sk = a.item_sk JOIN item AS i2 ON i2.i_item_sk = d.item_sk ORDER BY a.rnk LIMIT 100
```

---

### 133. Kimi Q31-Q99 - Q45

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 45 in stream 0 using template query45.tpl
select ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 LIMIT 100;

-- end query 45 in stream 0 using template query45.tpl

```

#### AFTER (Optimized)
```sql
-- start query 45 in stream 0 using template query45.tpl
select ca_zip, ca_city, sum(ws_sales_price)
 from web_sales, customer, customer_address, date_dim, item
 where ws_bill_customer_sk = c_customer_sk
 	and c_current_addr_sk = ca_address_sk 
 	and ws_item_sk = i_item_sk 
 	and ( substr(ca_zip,1,5) in ('85669', '86197','88274','83405','86475', '85392', '85460', '80348', '81792')
 	      or 
 	      i_item_id in (select i_item_id
                             from item
                             where i_item_sk in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29)
                             )
 	    )
 	and ws_sold_date_sk = d_date_sk
 	and d_qoy = 2 and d_year = 2000
 group by ca_zip, ca_city
 order by ca_zip, ca_city
 LIMIT 100;

-- end query 45 in stream 0 using template query45.tpl

```

---

### 134. Kimi Q31-Q99 - Q46

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 46 in stream 0 using template query46.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics,customer_address 
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 6 or
         household_demographics.hd_vehicle_count= 0)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1999,1999+1,1999+2) 
    and store.s_city in ('Five Points','Centerville','Oak Grove','Fairview','Liberty') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
 LIMIT 100;

-- end query 46 in stream 0 using template query46.tpl

```

#### AFTER (Optimized)
```sql
-- start query 46 in stream 0 using template query46.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,amt,profit 
 from
   (select ss_ticket_number
          ,ss_customer_sk
          ,ca_city bought_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics,customer_address 
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and store_sales.ss_addr_sk = customer_address.ca_address_sk
    and (household_demographics.hd_dep_count = 6 or
         household_demographics.hd_vehicle_count= 0)
    and date_dim.d_dow in (6,0)
    and date_dim.d_year in (1999,1999+1,1999+2) 
    and store.s_city in ('Five Points','Centerville','Oak Grove','Fairview','Liberty') 
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,ca_city) dn,customer,customer_address current_addr
    where ss_customer_sk = c_customer_sk
      and customer.c_current_addr_sk = current_addr.ca_address_sk
      and current_addr.ca_city <> bought_city
  order by c_last_name
          ,c_first_name
          ,ca_city
          ,bought_city
          ,ss_ticket_number
 LIMIT 100;

-- end query 46 in stream 0 using template query46.tpl

```

---

### 135. Kimi Q31-Q99 - Q47

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 47 in stream 0 using template query47.tpl
with v1 as(
 select i_category, i_brand,
        s_store_name, s_company_name,
        d_year, d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 2001 or
         ( d_year = 2001-1 and d_moy =12) or
         ( d_year = 2001+1 and d_moy =1)
       )
 group by i_category, i_brand,
          s_store_name, s_company_name,
          d_year, d_moy),
 v2 as(
 select v1.s_store_name
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1....[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 47 in stream 0 using template query47.tpl
with v1 as(
 select i_category, i_brand,
        s_store_name, s_company_name,
        d_year, d_moy,
        sum(ss_sales_price) sum_sales,
        avg(sum(ss_sales_price)) over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     s_store_name, s_company_name
           order by d_year, d_moy) rn
 from item, store_sales, date_dim, store
 where ss_item_sk = i_item_sk and
       ss_sold_date_sk = d_date_sk and
       ss_store_sk = s_store_sk and
       (
         d_year = 2001 or
         ( d_year = 2001-1 and d_moy =12) or
         ( d_year = 2001+1 and d_moy =1)
       )
 group by i_category, i_brand,
          s_store_name, s_company_name,
          d_year, d_moy),
 v2 as(
 select v1.s_store_name
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1.s_store_name = v1_lag.s_store_name and
       v1.s_store_name = v1_lead.s_store_name and
       v1.s_company_name = v1_lag.s_company_name and
       v1.s_company_name = v1_lead.s_company_name and
       v1.rn = v1_lag.rn + 1 and
       v1....[truncated]
```

---

### 136. Kimi Q31-Q99 - Q48

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 48 in stream 0 using template query48.tpl
select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'U'
   and 
   cd_education_status = 'Primary'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'W'
   and 
   cd_education_status = 'College'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = 'D'
   and 
   cd_education_status = '2 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MD', 'MN', 'IA')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('VA', 'IL', 'TX')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MI', 'WI', 'IN')
  and ss_net_profit between 50 and 25000 
  )
 )
;

-- end query 48 in stream 0 using template query48.tpl

```

#### AFTER (Optimized)
```sql
-- start query 48 in stream 0 using template query48.tpl
select sum (ss_quantity)
 from store_sales, store, customer_demographics, customer_address, date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 1999
 and  
 (
  (
   cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'U'
   and 
   cd_education_status = 'Primary'
   and 
   ss_sales_price between 100.00 and 150.00  
   )
 or
  (
  cd_demo_sk = ss_cdemo_sk
   and 
   cd_marital_status = 'W'
   and 
   cd_education_status = 'College'
   and 
   ss_sales_price between 50.00 and 100.00   
  )
 or 
 (
  cd_demo_sk = ss_cdemo_sk
  and 
   cd_marital_status = 'D'
   and 
   cd_education_status = '2 yr Degree'
   and 
   ss_sales_price between 150.00 and 200.00  
 )
 )
 and
 (
  (
  ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MD', 'MN', 'IA')
  and ss_net_profit between 0 and 2000  
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('VA', 'IL', 'TX')
  and ss_net_profit between 150 and 3000 
  )
 or
  (ss_addr_sk = ca_address_sk
  and
  ca_country = 'United States'
  and
  ca_state in ('MI', 'WI', 'IN')
  and ss_net_profit between 50 and 25000 
  )
 )
;

-- end query 48 in stream 0 using template query48.tpl

```

---

### 137. Kimi Q31-Q99 - Q49

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 49 in stream 0 using template query49.tpl
select channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		 web_sales ws left outer join web_returns wr 
 			on (ws.ws_order_number = wr.wr_order_number and 
 			ws.ws_item_sk = wr.wr_item_sk)
                 ,date_dim
 		where 
 			wr.wr_return_amt > 10000 
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 1999
                         and d_moy = 12
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where 
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union
 select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() ove...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 49 in stream 0 using template query49.tpl
select channel, item, return_ratio, return_rank, currency_rank from
 (select
 'web' as channel
 ,web.item
 ,web.return_ratio
 ,web.return_rank
 ,web.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() over (order by return_ratio) as return_rank
 	,rank() over (order by currency_ratio) as currency_rank
 	from
 	(	select ws.ws_item_sk as item
 		,(cast(sum(coalesce(wr.wr_return_quantity,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_quantity,0)) as decimal(15,4) )) as return_ratio
 		,(cast(sum(coalesce(wr.wr_return_amt,0)) as decimal(15,4))/
 		cast(sum(coalesce(ws.ws_net_paid,0)) as decimal(15,4) )) as currency_ratio
 		from 
 		 web_sales ws left outer join web_returns wr 
 			on (ws.ws_order_number = wr.wr_order_number and 
 			ws.ws_item_sk = wr.wr_item_sk)
                 ,date_dim
 		where 
 			wr.wr_return_amt > 10000 
 			and ws.ws_net_profit > 1
                         and ws.ws_net_paid > 0
                         and ws.ws_quantity > 0
                         and ws_sold_date_sk = d_date_sk
                         and d_year = 1999
                         and d_moy = 12
 		group by ws.ws_item_sk
 	) in_web
 ) web
 where 
 (
 web.return_rank <= 10
 or
 web.currency_rank <= 10
 )
 union
 select 
 'catalog' as channel
 ,catalog.item
 ,catalog.return_ratio
 ,catalog.return_rank
 ,catalog.currency_rank
 from (
 	select 
 	 item
 	,return_ratio
 	,currency_ratio
 	,rank() ove...[truncated]
```

---

### 138. Kimi Q31-Q99 - Q50

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 50 in stream 0 using template query50.tpl
select 
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 30) and 
                 (sr_returned_date_sk - ss_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 60) and 
                 (sr_returned_date_sk - ss_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk > 90) and
                 (sr_returned_date_sk - ss_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (sr_returned_date_sk - ss_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   store_sales
  ,store_returns
  ,store
  ,date_dim d1
  ,date_dim d2
where
    d2.d_year = 2001
and d2.d_moy  = 8
and ss_ticket_number = sr_ticket_number
and ss_item_sk = sr_item_sk
and ss_sold_date_sk   = d1.d_date_sk
and sr_returned_date_sk   = d2.d_date_sk
and ss_customer_sk = sr_customer_sk
and ss_store_sk = s_store_sk
group by
   s_store_name
  ,s_company_id
  ,s_street_number
  ,s_street_name
  ,s_street_type
  ,s_suite_number
  ,s_city
  ,s_county
  ,s_state
  ,s_zip
order by s_store_name
        ,s_company_id
        ,s_street_number
        ,s_street_name
        ,s_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_d2 AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy = 8), filtered_returns AS (SELECT sr.sr_returned_date_sk, ss.ss_store_sk, ss.ss_sold_date_sk FROM store_returns AS sr JOIN filtered_d2 ON sr.sr_returned_date_sk = filtered_d2.d_date_sk JOIN store_sales AS ss ON sr.sr_ticket_number = ss.ss_ticket_number AND sr.sr_item_sk = ss.ss_item_sk AND sr.sr_customer_sk = ss.ss_customer_sk)
SELECT s.s_store_name, s.s_company_id, s.s_street_number, s.s_street_name, s.s_street_type, s.s_suite_number, s.s_city, s.s_county, s.s_state, s.s_zip, SUM(CASE WHEN (fr.sr_returned_date_sk - fr.ss_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN (fr.sr_returned_date_sk - fr.ss_sold_date_sk > 30) AND (fr.sr_returned_date_sk - fr.ss_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN (fr.sr_returned_date_sk - fr.ss_sold_date_sk > 60) AND (fr.sr_returned_date_sk - fr.ss_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN (fr.sr_returned_date_sk - fr.ss_sold_date_sk > 90) AND (fr.sr_returned_date_sk - fr.ss_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN (fr.sr_returned_date_sk - fr.ss_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days" FROM filtered_returns AS fr JOIN store AS s ON fr.ss_store_sk = s.s_store_sk GROUP BY s.s_store_name, s.s_company_id, s.s_street_number, s.s_street_name, s.s_street_type, s.s_suite_number, s.s_city, s.s_county, s.s_state, s.s_zip ORDER BY s.s_store_name, s.s_compa...[truncated]
```

---

### 139. Kimi Q31-Q99 - Q51

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 51 in stream 0 using template query51.tpl
WITH web_v1 as (
select
  ws_item_sk item_sk, d_date,
  sum(sum(ws_sales_price))
      over (partition by ws_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from web_sales
    ,date_dim
where ws_sold_date_sk=d_date_sk
  and d_month_seq between 1216 and 1216+11
  and ws_item_sk is not NULL
group by ws_item_sk, d_date),
store_v1 as (
select
  ss_item_sk item_sk, d_date,
  sum(sum(ss_sales_price))
      over (partition by ss_item_sk order by d_date rows between unbounded preceding and current row) cume_sales
from store_sales
    ,date_dim
where ss_sold_date_sk=d_date_sk
  and d_month_seq between 1216 and 1216+11
  and ss_item_sk is not NULL
group by ss_item_sk, d_date)
 select *
from (select item_sk
     ,d_date
     ,web_sales
     ,store_sales
     ,max(web_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) web_cumulative
     ,max(store_sales)
         over (partition by item_sk order by d_date rows between unbounded preceding and current row) store_cumulative
     from (select case when web.item_sk is not null then web.item_sk else store.item_sk end item_sk
                 ,case when web.d_date is not null then web.d_date else store.d_date end d_date
                 ,web.cume_sales web_sales
                 ,store.cume_sales store_sales
           from web_v1 web full outer join store_v1 store on (web.item_sk = store.item_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH web_v1 AS (SELECT ws_item_sk AS item_sk, d_date, SUM(SUM(ws_sales_price)) OVER (PARTITION BY ws_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cume_sales FROM web_sales, date_dim WHERE ws_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1216 AND 1216 + 11 AND NOT ws_item_sk IS NULL GROUP BY ws_item_sk, d_date), store_v1 AS (SELECT ss_item_sk AS item_sk, d_date, SUM(SUM(ss_sales_price)) OVER (PARTITION BY ss_item_sk ORDER BY d_date rows BETWEEN UNBOUNDED preceding AND CURRENT ROW) AS cume_sales FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1216 AND 1216 + 11 AND NOT ss_item_sk IS NULL GROUP BY ss_item_sk, d_date)
SELECT item_sk, d_date, web_sales, store_sales, web_sales AS web_cumulative, store_sales AS store_cumulative FROM (SELECT CASE WHEN NOT web.item_sk IS NULL THEN web.item_sk ELSE store.item_sk END AS item_sk, CASE WHEN NOT web.d_date IS NULL THEN web.d_date ELSE store.d_date END AS d_date, web.cume_sales AS web_sales, store.cume_sales AS store_sales FROM web_v1 AS web FULL OUTER JOIN store_v1 AS store ON (web.item_sk = store.item_sk AND web.d_date = store.d_date)) AS x WHERE web_sales > store_sales ORDER BY item_sk, d_date LIMIT 100
```

---

### 140. Kimi Q31-Q99 - Q52

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 52 in stream 0 using template query52.tpl
select dt.d_year
 	,item.i_brand_id brand_id
 	,item.i_brand brand
 	,sum(ss_ext_sales_price) ext_price
 from date_dim dt
     ,store_sales
     ,item
 where dt.d_date_sk = store_sales.ss_sold_date_sk
    and store_sales.ss_item_sk = item.i_item_sk
    and item.i_manager_id = 1
    and dt.d_moy=12
    and dt.d_year=2002
 group by dt.d_year
 	,item.i_brand
 	,item.i_brand_id
 order by dt.d_year
 	,ext_price desc
 	,brand_id
 LIMIT 100;

-- end query 52 in stream 0 using template query52.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 12 AND d_year = 2002), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manager_id = 1)
SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, SUM(ss.ss_ext_sales_price) AS ext_price FROM filtered_dates AS dt JOIN store_sales AS ss ON dt.d_date_sk = ss.ss_sold_date_sk JOIN filtered_items AS item ON ss.ss_item_sk = item.i_item_sk GROUP BY dt.d_year, item.i_brand, item.i_brand_id ORDER BY dt.d_year, ext_price DESC, brand_id LIMIT 100
```

---

### 141. Kimi Q31-Q99 - Q53

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 53 in stream 0 using template query53.tpl
select * from 
(select i_manufact_id,
sum(ss_sales_price) sum_sales,
avg(sum(ss_sales_price)) over (partition by i_manufact_id) avg_quarterly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
d_month_seq in (1200,1200+1,1200+2,1200+3,1200+4,1200+5,1200+6,1200+7,1200+8,1200+9,1200+10,1200+11) and
((i_category in ('Books','Children','Electronics') and
i_class in ('personal','portable','reference','self-help') and
i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		'exportiunivamalg #9','scholaramalgamalg #9'))
or(i_category in ('Women','Music','Men') and
i_class in ('accessories','classical','fragrances','pants') and
i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		'importoamalg #1')))
group by i_manufact_id, d_qoy ) tmp1
where case when avg_quarterly_sales > 0 
	then abs (sum_sales - avg_quarterly_sales)/ avg_quarterly_sales 
	else null end > 0.1
order by avg_quarterly_sales,
	 sum_sales,
	 i_manufact_id
 LIMIT 100;

-- end query 53 in stream 0 using template query53.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_qoy FROM date_dim WHERE d_month_seq IN (1200, 1200 + 1, 1200 + 2, 1200 + 3, 1200 + 4, 1200 + 5, 1200 + 6, 1200 + 7, 1200 + 8, 1200 + 9, 1200 + 10, 1200 + 11)), filtered_sales AS (SELECT i.i_manufact_id, d.d_qoy, ss.ss_sales_price FROM item AS i JOIN store_sales AS ss ON ss.ss_item_sk = i.i_item_sk JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE i.i_category IN ('Books', 'Children', 'Electronics') AND i.i_class IN ('personal', 'portable', 'reference', 'self-help') AND i.i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9') UNION ALL SELECT i.i_manufact_id, d.d_qoy, ss.ss_sales_price FROM item AS i JOIN store_sales AS ss ON ss.ss_item_sk = i.i_item_sk JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE i.i_category IN ('Women', 'Music', 'Men') AND i.i_class IN ('accessories', 'classical', 'fragrances', 'pants') AND i.i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'))
SELECT * FROM (SELECT i_manufact_id, SUM(ss_sales_price) AS sum_sales, AVG(SUM(ss_sales_price)) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales FROM filtered_sales GROUP BY i_manufact_id, d_qoy) AS tmp1 WHERE CASE WHEN avg_quarterly_sales > 0 THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales ELSE NULL END > 0.1 ORDER BY avg_...[truncated]
```

---

### 142. Kimi Q31-Q99 - Q54

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 54 in stream 0 using template query54.tpl
with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Women'
         and i_class = 'maternity'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 5
         and d_year = 1998
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1998 and d_moy = 5)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1998 and d_moy = 5)
 group by c_cus...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 54 in stream 0 using template query54.tpl
with my_customers as (
 select distinct c_customer_sk
        , c_current_addr_sk
 from   
        ( select cs_sold_date_sk sold_date_sk,
                 cs_bill_customer_sk customer_sk,
                 cs_item_sk item_sk
          from   catalog_sales
          union all
          select ws_sold_date_sk sold_date_sk,
                 ws_bill_customer_sk customer_sk,
                 ws_item_sk item_sk
          from   web_sales
         ) cs_or_ws_sales,
         item,
         date_dim,
         customer
 where   sold_date_sk = d_date_sk
         and item_sk = i_item_sk
         and i_category = 'Women'
         and i_class = 'maternity'
         and c_customer_sk = cs_or_ws_sales.customer_sk
         and d_moy = 5
         and d_year = 1998
 )
 , my_revenue as (
 select c_customer_sk,
        sum(ss_ext_sales_price) as revenue
 from   my_customers,
        store_sales,
        customer_address,
        store,
        date_dim
 where  c_current_addr_sk = ca_address_sk
        and ca_county = s_county
        and ca_state = s_state
        and ss_sold_date_sk = d_date_sk
        and c_customer_sk = ss_customer_sk
        and d_month_seq between (select distinct d_month_seq+1
                                 from   date_dim where d_year = 1998 and d_moy = 5)
                           and  (select distinct d_month_seq+3
                                 from   date_dim where d_year = 1998 and d_moy = 5)
 group by c_cus...[truncated]
```

---

### 143. Kimi Q31-Q99 - Q55

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 55 in stream 0 using template query55.tpl
select i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=100
 	and d_moy=12
 	and d_year=2000
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
 LIMIT 100;

-- end query 55 in stream 0 using template query55.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy = 12), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manager_id = 100)
SELECT i_brand_id AS brand_id, i_brand AS brand, SUM(ss_ext_sales_price) AS ext_price FROM store_sales JOIN filtered_dates AS fd ON ss_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ss_item_sk = fi.i_item_sk GROUP BY i_brand, i_brand_id ORDER BY ext_price DESC, i_brand_id LIMIT 100
```

---

### 144. Kimi Q31-Q99 - Q56

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 56 in stream 0 using template query56.tpl
with ss as (
 select i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where i_item_id in (select
     i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 2
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 cs as (
 select i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 2
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -6 
 group by i_item_id),
 ws as (
 select i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from item
where i_color in ('powder','green','cyan'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_mo...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss AS (SELECT i_item_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_color IN ('powder', 'green', 'cyan')) AND ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 2 AND ss_addr_sk = ca_address_sk AND ca_gmt_offset = -6 GROUP BY i_item_id), cs AS (SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_color IN ('powder', 'green', 'cyan')) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 2 AND cs_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -6 GROUP BY i_item_id), ws AS (SELECT i_item_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_color IN ('powder', 'green', 'cyan')) AND ws_item_sk = i_item_sk AND ws_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 2 AND ws_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -6 GROUP BY i_item_id) SELECT i_item_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_item_id ORDER BY total_sales, i_item_id LIMIT 100
```

---

### 145. Kimi Q31-Q99 - Q57

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 57 in stream 0 using template query57.tpl
with v1 as(
 select i_category, i_brand,
        cc_name,
        d_year, d_moy,
        sum(cs_sales_price) sum_sales,
        avg(sum(cs_sales_price)) over
          (partition by i_category, i_brand,
                     cc_name, d_year)
          avg_monthly_sales,
        rank() over
          (partition by i_category, i_brand,
                     cc_name
           order by d_year, d_moy) rn
 from item, catalog_sales, date_dim, call_center
 where cs_item_sk = i_item_sk and
       cs_sold_date_sk = d_date_sk and
       cc_call_center_sk= cs_call_center_sk and
       (
         d_year = 1999 or
         ( d_year = 1999-1 and d_moy =12) or
         ( d_year = 1999+1 and d_moy =1)
       )
 group by i_category, i_brand,
          cc_name , d_year, d_moy),
 v2 as(
 select v1.i_brand
        ,v1.d_year
        ,v1.avg_monthly_sales
        ,v1.sum_sales, v1_lag.sum_sales psum, v1_lead.sum_sales nsum
 from v1, v1 v1_lag, v1 v1_lead
 where v1.i_category = v1_lag.i_category and
       v1.i_category = v1_lead.i_category and
       v1.i_brand = v1_lag.i_brand and
       v1.i_brand = v1_lead.i_brand and
       v1. cc_name = v1_lag. cc_name and
       v1. cc_name = v1_lead. cc_name and
       v1.rn = v1_lag.rn + 1 and
       v1.rn = v1_lead.rn - 1)
  select *
 from v2
 where  d_year = 1999 and
        avg_monthly_sales > 0 and
        case when avg_monthly_sales > 0 then abs(sum_sales - avg_monthly_sales) / avg_monthly_sales ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH filtered_sales AS (SELECT i.i_category, i.i_brand, cc.cc_name, d.d_year, d.d_moy, cs.cs_sales_price FROM catalog_sales AS cs JOIN item AS i ON cs.cs_item_sk = i.i_item_sk JOIN date_dim AS d ON cs.cs_sold_date_sk = d.d_date_sk JOIN call_center AS cc ON cs.cs_call_center_sk = cc.cc_call_center_sk WHERE d.d_year = 1999 UNION ALL SELECT i.i_category, i.i_brand, cc.cc_name, d.d_year, d.d_moy, cs.cs_sales_price FROM catalog_sales AS cs JOIN item AS i ON cs.cs_item_sk = i.i_item_sk JOIN date_dim AS d ON cs.cs_sold_date_sk = d.d_date_sk JOIN call_center AS cc ON cs.cs_call_center_sk = cc.cc_call_center_sk WHERE d.d_year = 1998 AND d.d_moy = 12 UNION ALL SELECT i.i_category, i.i_brand, cc.cc_name, d.d_year, d.d_moy, cs.cs_sales_price FROM catalog_sales AS cs JOIN item AS i ON cs.cs_item_sk = i.i_item_sk JOIN date_dim AS d ON cs.cs_sold_date_sk = d.d_date_sk JOIN call_center AS cc ON cs.cs_call_center_sk = cc.cc_call_center_sk WHERE d.d_year = 2000 AND d.d_moy = 1), v1 AS (SELECT i_category, i_brand, cc_name, d_year, d_moy, SUM(cs_sales_price) AS sum_sales, AVG(SUM(cs_sales_price)) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales, RANK() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn FROM filtered_sales GROUP BY i_category, i_brand, cc_name, d_year, d_moy), v2 AS (SELECT v1.i_brand, v1.d_year, v1.avg_monthly_sales, v1.sum_sales, v1_lag.sum_sales AS psum, v1_lead.sum_sales AS nsum FROM v1, v1 AS v1_lag, v1 AS v1_lead WHERE...[truncated]
```

---

### 146. Kimi Q31-Q99 - Q58

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 58 in stream 0 using template query58.tpl
with ss_items as
 (select i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev 
 from store_sales
     ,item
     ,date_dim
 where ss_item_sk = i_item_sk
   and d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq 
                                      from date_dim
                                      where d_date = '2001-03-24'))
   and ss_sold_date_sk   = d_date_sk
 group by i_item_id),
 cs_items as
 (select i_item_id item_id
        ,sum(cs_ext_sales_price) cs_item_rev
  from catalog_sales
      ,item
      ,date_dim
 where cs_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq = (select d_week_seq 
                                      from date_dim
                                      where d_date = '2001-03-24'))
  and  cs_sold_date_sk = d_date_sk
 group by i_item_id),
 ws_items as
 (select i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
 where ws_item_sk = i_item_sk
  and  d_date in (select d_date
                  from date_dim
                  where d_week_seq =(select d_week_seq 
                                     from date_dim
                                     where d_date = '2001-03-24'))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id)
  select ss_items.item_id
       ,ss_item_rev
       ,ss_item_...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss_items AS (SELECT i_item_id AS item_id, SUM(ss_ext_sales_price) AS ss_item_rev FROM store_sales, item, date_dim WHERE ss_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2001-03-24')) AND ss_sold_date_sk = d_date_sk GROUP BY i_item_id), cs_items AS (SELECT i_item_id AS item_id, SUM(cs_ext_sales_price) AS cs_item_rev FROM catalog_sales, item, date_dim WHERE cs_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2001-03-24')) AND cs_sold_date_sk = d_date_sk GROUP BY i_item_id), ws_items AS (SELECT i_item_id AS item_id, SUM(ws_ext_sales_price) AS ws_item_rev FROM web_sales, item, date_dim WHERE ws_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq = (SELECT d_week_seq FROM date_dim WHERE d_date = '2001-03-24')) AND ws_sold_date_sk = d_date_sk GROUP BY i_item_id) SELECT ss_items.item_id, ss_item_rev, ss_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ss_dev, cs_item_rev, cs_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS cs_dev, ws_item_rev, ws_item_rev / ((ss_item_rev + cs_item_rev + ws_item_rev) / 3) * 100 AS ws_dev, (ss_item_rev + cs_item_rev + ws_item_rev) / 3 AS average FROM ss_items, cs_items, ws_items WHERE ss_items.item_id = cs_items.item_id AND ss_items.item_id = ws_items.item_id AND ss_item_rev BETWEEN 0.9 * cs_item_rev AND 1.1 * cs_item...[truncated]
```

---

### 147. Kimi Q31-Q99 - Q59

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 59 in stream 0 using template query59.tpl
with wss as 
 (select d_week_seq,
        ss_store_sk,
        sum(case when (d_day_name='Sunday') then ss_sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then ss_sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then ss_sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then ss_sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then ss_sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then ss_sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then ss_sales_price else null end) sat_sales
 from store_sales,date_dim
 where d_date_sk = ss_sold_date_sk
 group by d_week_seq,ss_store_sk
 )
  select s_store_name1,s_store_id1,d_week_seq1
       ,sun_sales1/sun_sales2,mon_sales1/mon_sales2
       ,tue_sales1/tue_sales2,wed_sales1/wed_sales2,thu_sales1/thu_sales2
       ,fri_sales1/fri_sales2,sat_sales1/sat_sales2
 from
 (select s_store_name s_store_name1,wss.d_week_seq d_week_seq1
        ,s_store_id s_store_id1,sun_sales sun_sales1
        ,mon_sales mon_sales1,tue_sales tue_sales1
        ,wed_sales wed_sales1,thu_sales thu_sales1
        ,fri_sales fri_sales1,sat_sales sat_sales1
  from wss,store,date_dim d
  where d.d_week_seq = wss.d_week_seq and
        ss_store_sk = s_store_sk and 
        d_month_seq between 1196 and 1196 + ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH wss_year1 AS (SELECT d_week_seq, ss_store_sk, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales FROM store_sales, date_dim WHERE d_date_sk = ss_sold_date_sk AND d_month_seq BETWEEN 1196 AND 1196 + 11 GROUP BY d_week_seq, ss_store_sk), wss_year2 AS (SELECT d_week_seq, ss_store_sk, SUM(CASE WHEN (d_day_name = 'Sunday') THEN ss_sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN ss_sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN ss_sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN ss_sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN ss_sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d_day_name = 'Friday') THEN ss_sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d_day_name = 'Saturday') THEN ss_sales_price ELSE NULL END) AS sat_sales FROM store_sales, date_dim WHERE...[truncated]
```

---

### 148. Kimi Q31-Q99 - Q60

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 60 in stream 0 using template query60.tpl
with ss as (
 select
          i_item_id,sum(ss_ext_sales_price) total_sales
 from
 	store_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ss_item_sk              = i_item_sk
 and     ss_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     ss_addr_sk              = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 cs as (
 select
          i_item_id,sum(cs_ext_sales_price) total_sales
 from
 	catalog_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     cs_item_sk              = i_item_sk
 and     cs_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and     d_moy                   = 8
 and     cs_bill_addr_sk         = ca_address_sk
 and     ca_gmt_offset           = -7 
 group by i_item_id),
 ws as (
 select
          i_item_id,sum(ws_ext_sales_price) total_sales
 from
 	web_sales,
 	date_dim,
         customer_address,
         item
 where
         i_item_id               in (select
  i_item_id
from
 item
where i_category in ('Children'))
 and     ws_item_sk              = i_item_sk
 and     ws_sold_date_sk         = d_date_sk
 and     d_year                  = 2000
 and...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss AS (SELECT i_item_id, SUM(ss_ext_sales_price) AS total_sales FROM store_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_category IN ('Children')) AND ss_item_sk = i_item_sk AND ss_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 8 AND ss_addr_sk = ca_address_sk AND ca_gmt_offset = -7 GROUP BY i_item_id), cs AS (SELECT i_item_id, SUM(cs_ext_sales_price) AS total_sales FROM catalog_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_category IN ('Children')) AND cs_item_sk = i_item_sk AND cs_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 8 AND cs_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -7 GROUP BY i_item_id), ws AS (SELECT i_item_id, SUM(ws_ext_sales_price) AS total_sales FROM web_sales, date_dim, customer_address, item WHERE i_item_id IN (SELECT i_item_id FROM item WHERE i_category IN ('Children')) AND ws_item_sk = i_item_sk AND ws_sold_date_sk = d_date_sk AND d_year = 2000 AND d_moy = 8 AND ws_bill_addr_sk = ca_address_sk AND ca_gmt_offset = -7 GROUP BY i_item_id) SELECT i_item_id, SUM(total_sales) AS total_sales FROM (SELECT * FROM ss UNION ALL SELECT * FROM cs UNION ALL SELECT * FROM ws) AS tmp1 GROUP BY i_item_id ORDER BY i_item_id, total_sales LIMIT 100
```

---

### 149. Kimi Q31-Q99 - Q61

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 61 in stream 0 using template query61.tpl
select promotions,total,cast(promotions as decimal(15,4))/cast(total as decimal(15,4))*100
from
  (select sum(ss_ext_sales_price) promotions
   from  store_sales
        ,store
        ,promotion
        ,date_dim
        ,customer
        ,customer_address 
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_promo_sk = p_promo_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk 
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   (p_channel_dmail = 'Y' or p_channel_email = 'Y' or p_channel_tv = 'Y')
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) promotional_sales,
  (select sum(ss_ext_sales_price) total
   from  store_sales
        ,store
        ,date_dim
        ,customer
        ,customer_address
        ,item
   where ss_sold_date_sk = d_date_sk
   and   ss_store_sk = s_store_sk
   and   ss_customer_sk= c_customer_sk
   and   ca_address_sk = c_current_addr_sk
   and   ss_item_sk = i_item_sk
   and   ca_gmt_offset = -7
   and   i_category = 'Jewelry'
   and   s_gmt_offset = -7
   and   d_year = 1999
   and   d_moy  = 11) all_sales
order by promotions, total
 LIMIT 100;

-- end query 61 in stream 0 using template query61.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 1999 AND d_moy = 11), base_sales AS (SELECT ss.ss_ext_sales_price, ss.ss_promo_sk FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk AND s.s_gmt_offset = -7 JOIN customer AS c ON ss.ss_customer_sk = c.c_customer_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk AND ca.ca_gmt_offset = -7 JOIN item AS i ON ss.ss_item_sk = i.i_item_sk AND i.i_category = 'Jewelry'), promotional_sales AS (SELECT SUM(ss_ext_sales_price) AS promotions FROM (SELECT bs.ss_ext_sales_price FROM base_sales AS bs JOIN promotion AS p ON bs.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_dmail = 'Y' UNION ALL SELECT bs.ss_ext_sales_price FROM base_sales AS bs JOIN promotion AS p ON bs.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_email = 'Y' UNION ALL SELECT bs.ss_ext_sales_price FROM base_sales AS bs JOIN promotion AS p ON bs.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_tv = 'Y') AS t), all_sales AS (SELECT SUM(ss_ext_sales_price) AS total FROM base_sales)
SELECT promotions, total, CAST(promotions AS DECIMAL(15, 4)) / CAST(total AS DECIMAL(15, 4)) * 100 FROM promotional_sales, all_sales ORDER BY promotions, total LIMIT 100
```

---

### 150. Kimi Q31-Q99 - Q62

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 62 in stream 0 using template query62.tpl
select 
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 30) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 60) and 
                 (ws_ship_date_sk - ws_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk > 90) and
                 (ws_ship_date_sk - ws_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (ws_ship_date_sk - ws_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   web_sales
  ,warehouse
  ,ship_mode
  ,web_site
  ,date_dim
where
    d_month_seq between 1194 and 1194 + 11
and ws_ship_date_sk   = d_date_sk
and ws_warehouse_sk   = w_warehouse_sk
and ws_ship_mode_sk   = sm_ship_mode_sk
and ws_web_site_sk    = web_site_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,web_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
       ,web_name
 LIMIT 100;

-- end query 62 in stream 0 using template query62.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1194 AND 1194 + 11), filtered_sales AS (SELECT ws_warehouse_sk, ws_ship_mode_sk, ws_web_site_sk, (ws_ship_date_sk - ws_sold_date_sk) AS days_diff FROM web_sales JOIN filtered_dates ON ws_ship_date_sk = d_date_sk)
SELECT SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name, SUM(CASE WHEN days_diff <= 30 THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN days_diff > 30 AND days_diff <= 60 THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN days_diff > 60 AND days_diff <= 90 THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN days_diff > 90 AND days_diff <= 120 THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN days_diff > 120 THEN 1 ELSE 0 END) AS ">120 days" FROM filtered_sales JOIN warehouse ON ws_warehouse_sk = w_warehouse_sk JOIN ship_mode ON ws_ship_mode_sk = sm_ship_mode_sk JOIN web_site ON ws_web_site_sk = web_site_sk GROUP BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name ORDER BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, web_name LIMIT 100
```

---

### 151. Kimi Q31-Q99 - Q63

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 63 in stream 0 using template query63.tpl
select * 
from (select i_manager_id
             ,sum(ss_sales_price) sum_sales
             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from item
          ,store_sales
          ,date_dim
          ,store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1181,1181+1,1181+2,1181+3,1181+4,1181+5,1181+6,1181+7,1181+8,1181+9,1181+10,1181+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
 LIMIT 100;

-- end query 63 in stream 0 using template query63.tpl

```

#### AFTER (Optimized)
```sql
-- start query 63 in stream 0 using template query63.tpl
select * 
from (select i_manager_id
             ,sum(ss_sales_price) sum_sales
             ,avg(sum(ss_sales_price)) over (partition by i_manager_id) avg_monthly_sales
      from item
          ,store_sales
          ,date_dim
          ,store
      where ss_item_sk = i_item_sk
        and ss_sold_date_sk = d_date_sk
        and ss_store_sk = s_store_sk
        and d_month_seq in (1181,1181+1,1181+2,1181+3,1181+4,1181+5,1181+6,1181+7,1181+8,1181+9,1181+10,1181+11)
        and ((    i_category in ('Books','Children','Electronics')
              and i_class in ('personal','portable','reference','self-help')
              and i_brand in ('scholaramalgamalg #14','scholaramalgamalg #7',
		                  'exportiunivamalg #9','scholaramalgamalg #9'))
           or(    i_category in ('Women','Music','Men')
              and i_class in ('accessories','classical','fragrances','pants')
              and i_brand in ('amalgimporto #1','edu packscholar #1','exportiimporto #1',
		                 'importoamalg #1')))
group by i_manager_id, d_moy) tmp1
where case when avg_monthly_sales > 0 then abs (sum_sales - avg_monthly_sales) / avg_monthly_sales else null end > 0.1
order by i_manager_id
        ,avg_monthly_sales
        ,sum_sales
 LIMIT 100;

-- end query 63 in stream 0 using template query63.tpl

```

---

### 152. Kimi Q31-Q99 - Q64

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 64 in stream 0 using template query64.tpl
with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales
      ,catalog_returns
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_street_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales
        ,store_returns
        ,cs_ui
        ,date_dim d1
        ,date_dim d2
        ,date_dim d3
        ,store
        ,customer
        ,customer_demographics cd1
        ,customer_demographics cd2
        ,promotion
        ,household_demographics hd1
        ,household_demographics hd2
        ,customer_address ad1
        ,customer_address ad2
        ,income_band ib1
        ,income_band ib2
        ,item
  WHERE  ss_store_sk = s_store_sk AND
         ss_sold_date_...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 64 in stream 0 using template query64.tpl
with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales
      ,catalog_returns
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select i_product_name product_name
     ,i_item_sk item_sk
     ,s_store_name store_name
     ,s_zip store_zip
     ,ad1.ca_street_number b_street_number
     ,ad1.ca_street_name b_street_name
     ,ad1.ca_city b_city
     ,ad1.ca_zip b_zip
     ,ad2.ca_street_number c_street_number
     ,ad2.ca_street_name c_street_name
     ,ad2.ca_city c_city
     ,ad2.ca_zip c_zip
     ,d1.d_year as syear
     ,d2.d_year as fsyear
     ,d3.d_year s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales
        ,store_returns
        ,cs_ui
        ,date_dim d1
        ,date_dim d2
        ,date_dim d3
        ,store
        ,customer
        ,customer_demographics cd1
        ,customer_demographics cd2
        ,promotion
        ,household_demographics hd1
        ,household_demographics hd2
        ,customer_address ad1
        ,customer_address ad2
        ,income_band ib1
        ,income_band ib2
        ,item
  WHERE  ss_store_sk = s_store_sk AND
         ss_sold_date_...[truncated]
```

---

### 153. Kimi Q31-Q99 - Q65

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 65 in stream 0 using template query65.tpl
select
	s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from store, item,
     (select ss_store_sk, avg(revenue) as ave
 	from
 	    (select  ss_store_sk, ss_item_sk, 
 		     sum(ss_sales_price) as revenue
 		from store_sales, date_dim
 		where ss_sold_date_sk = d_date_sk and d_month_seq between 1221 and 1221+11
 		group by ss_store_sk, ss_item_sk) sa
 	group by ss_store_sk) sb,
     (select  ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
 	from store_sales, date_dim
 	where ss_sold_date_sk = d_date_sk and d_month_seq between 1221 and 1221+11
 	group by ss_store_sk, ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and 
       sc.revenue <= 0.1 * sb.ave and
       s_store_sk = sc.ss_store_sk and
       i_item_sk = sc.ss_item_sk
 order by s_store_name, i_item_desc
 LIMIT 100;

-- end query 65 in stream 0 using template query65.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_sales AS (SELECT ss.ss_store_sk, ss.ss_item_sk, ss.ss_sales_price FROM store_sales AS ss JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk WHERE d.d_month_seq BETWEEN 1221 AND 1221 + 11), item_revenue AS (SELECT ss_store_sk, ss_item_sk, SUM(ss_sales_price) AS revenue FROM filtered_sales GROUP BY ss_store_sk, ss_item_sk), store_avg AS (SELECT ss_store_sk, AVG(revenue) AS ave FROM item_revenue GROUP BY ss_store_sk)
SELECT s.s_store_name, i.i_item_desc, ir.revenue, i.i_current_price, i.i_wholesale_cost, i.i_brand FROM item_revenue AS ir JOIN store_avg AS sb ON ir.ss_store_sk = sb.ss_store_sk JOIN store AS s ON s.s_store_sk = ir.ss_store_sk JOIN item AS i ON i.i_item_sk = ir.ss_item_sk WHERE ir.revenue <= 0.1 * sb.ave ORDER BY s.s_store_name, i.i_item_desc LIMIT 100
```

---

### 154. Kimi Q31-Q99 - Q66

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 66 in stream 0 using template query66.tpl
select  
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 66 in stream 0 using template query66.tpl
select  
         w_warehouse_name
 	,w_warehouse_sq_ft
 	,w_city
 	,w_county
 	,w_state
 	,w_country
        ,ship_carriers
        ,year
 	,sum(jan_sales) as jan_sales
 	,sum(feb_sales) as feb_sales
 	,sum(mar_sales) as mar_sales
 	,sum(apr_sales) as apr_sales
 	,sum(may_sales) as may_sales
 	,sum(jun_sales) as jun_sales
 	,sum(jul_sales) as jul_sales
 	,sum(aug_sales) as aug_sales
 	,sum(sep_sales) as sep_sales
 	,sum(oct_sales) as oct_sales
 	,sum(nov_sales) as nov_sales
 	,sum(dec_sales) as dec_sales
 	,sum(jan_sales/w_warehouse_sq_ft) as jan_sales_per_sq_foot
 	,sum(feb_sales/w_warehouse_sq_ft) as feb_sales_per_sq_foot
 	,sum(mar_sales/w_warehouse_sq_ft) as mar_sales_per_sq_foot
 	,sum(apr_sales/w_warehouse_sq_ft) as apr_sales_per_sq_foot
 	,sum(may_sales/w_warehouse_sq_ft) as may_sales_per_sq_foot
 	,sum(jun_sales/w_warehouse_sq_ft) as jun_sales_per_sq_foot
 	,sum(jul_sales/w_warehouse_sq_ft) as jul_sales_per_sq_foot
 	,sum(aug_sales/w_warehouse_sq_ft) as aug_sales_per_sq_foot
 	,sum(sep_sales/w_warehouse_sq_ft) as sep_sales_per_sq_foot
 	,sum(oct_sales/w_warehouse_sq_ft) as oct_sales_per_sq_foot
 	,sum(nov_sales/w_warehouse_sq_ft) as nov_sales_per_sq_foot
 	,sum(dec_sales/w_warehouse_sq_ft) as dec_sales_per_sq_foot
 	,sum(jan_net) as jan_net
 	,sum(feb_net) as feb_net
 	,sum(mar_net) as mar_net
 	,sum(apr_net) as apr_net
 	,sum(may_net) as may_net
 	,sum(jun_net) as jun_net
 	,sum(jul_net) as jul_net
 	,sum(aug_...[truncated]
```

---

### 155. Kimi Q31-Q99 - Q67

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 67 in stream 0 using template query67.tpl
select *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,d_year
                  ,d_qoy
                  ,d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales
                ,date_dim
                ,store
                ,item
       where  ss_sold_date_sk=d_date_sk
          and ss_item_sk=i_item_sk
          and ss_store_sk = s_store_sk
          and d_month_seq between 1206 and 1206+11
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
 LIMIT 100;

-- end query 67 in stream 0 using template query67.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year, d_qoy, d_moy FROM date_dim WHERE d_month_seq BETWEEN 1206 AND 1206 + 11), dw1 AS (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON ss_store_sk = s_store_sk JOIN item ON ss_item_sk = i_item_sk GROUP BY ROLLUP (i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id)), dw2 AS (SELECT i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk FROM dw1)
SELECT * FROM dw2 WHERE rk <= 100 ORDER BY i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy, s_store_id, sumsales, rk LIMIT 100
```

---

### 156. Kimi Q31-Q99 - Q68

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 68 in stream 0 using template query68.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from store_sales
           ,date_dim
           ,store
           ,household_demographics
           ,customer_address 
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_store_sk = store.s_store_sk  
        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        and date_dim.d_dom between 1 and 2 
        and (household_demographics.hd_dep_count = 8 or
             household_demographics.hd_vehicle_count= -1)
        and date_dim.d_year in (1998,1998+1,1998+2)
        and store.s_city in ('Pleasant Hill','Five Points')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer
      ,customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 LIMIT 100;

-- end query 68 in ...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 68 in stream 0 using template query68.tpl
select c_last_name
       ,c_first_name
       ,ca_city
       ,bought_city
       ,ss_ticket_number
       ,extended_price
       ,extended_tax
       ,list_price
 from (select ss_ticket_number
             ,ss_customer_sk
             ,ca_city bought_city
             ,sum(ss_ext_sales_price) extended_price 
             ,sum(ss_ext_list_price) list_price
             ,sum(ss_ext_tax) extended_tax 
       from store_sales
           ,date_dim
           ,store
           ,household_demographics
           ,customer_address 
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_store_sk = store.s_store_sk  
        and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
        and store_sales.ss_addr_sk = customer_address.ca_address_sk
        and date_dim.d_dom between 1 and 2 
        and (household_demographics.hd_dep_count = 8 or
             household_demographics.hd_vehicle_count= -1)
        and date_dim.d_year in (1998,1998+1,1998+2)
        and store.s_city in ('Pleasant Hill','Five Points')
       group by ss_ticket_number
               ,ss_customer_sk
               ,ss_addr_sk,ca_city) dn
      ,customer
      ,customer_address current_addr
 where ss_customer_sk = c_customer_sk
   and customer.c_current_addr_sk = current_addr.ca_address_sk
   and current_addr.ca_city <> bought_city
 order by c_last_name
         ,ss_ticket_number
 LIMIT 100;

-- end query 68 in ...[truncated]
```

---

### 157. Kimi Q31-Q99 - Q69

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 69 in stream 0 using template query69.tpl
select 
  cd_gender,
  cd_marital_status,
  cd_education_status,
  count(*) cnt1,
  cd_purchase_estimate,
  count(*) cnt2,
  cd_credit_rating,
  count(*) cnt3
 from
  customer c,customer_address ca,customer_demographics
 where
  c.c_current_addr_sk = ca.ca_address_sk and
  ca_state in ('TX','VA','MI') and
  cd_demo_sk = c.c_current_cdemo_sk and 
  exists (select *
          from store_sales,date_dim
          where c.c_customer_sk = ss_customer_sk and
                ss_sold_date_sk = d_date_sk and
                d_year = 2000 and
                d_moy between 1 and 1+2) and
   (not exists (select *
            from web_sales,date_dim
            where c.c_customer_sk = ws_bill_customer_sk and
                  ws_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 1 and 1+2) and
    not exists (select * 
            from catalog_sales,date_dim
            where c.c_customer_sk = cs_ship_customer_sk and
                  cs_sold_date_sk = d_date_sk and
                  d_year = 2000 and
                  d_moy between 1 and 1+2))
 group by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 order by cd_gender,
          cd_marital_status,
          cd_education_status,
          cd_purchase_estimate,
          cd_credit_rating
 LIMIT 100;

-- end query 69 in stream 0 using template query69.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 AND d_moy BETWEEN 1 AND 1 + 2), store_buyers AS (SELECT DISTINCT ss_customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), web_buyers AS (SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), catalog_buyers AS (SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk)
SELECT cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, COUNT(*) AS cnt1, cd.cd_purchase_estimate, COUNT(*) AS cnt2, cd.cd_credit_rating, COUNT(*) AS cnt3 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON c.c_current_cdemo_sk = cd.cd_demo_sk JOIN store_buyers AS sb ON c.c_customer_sk = sb.ss_customer_sk LEFT JOIN web_buyers AS wb ON c.c_customer_sk = wb.ws_bill_customer_sk LEFT JOIN catalog_buyers AS cb ON c.c_customer_sk = cb.cs_ship_customer_sk WHERE ca.ca_state IN ('TX', 'VA', 'MI') AND wb.ws_bill_customer_sk IS NULL AND cb.cs_ship_customer_sk IS NULL GROUP BY cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, cd.cd_purchase_estimate, cd.cd_credit_rating ORDER BY cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, cd.cd_purchase_estimate, cd.cd_credit_rating LIMIT 100
```

---

### 158. Kimi Q31-Q99 - Q70

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 70 in stream 0 using template query70.tpl
select 
    sum(ss_net_profit) as total_sum
   ,s_state
   ,s_county
   ,grouping(s_state)+grouping(s_county) as lochierarchy
   ,rank() over (
 	partition by grouping(s_state)+grouping(s_county),
 	case when grouping(s_county) = 0 then s_state end 
 	order by sum(ss_net_profit) desc) as rank_within_parent
 from
    store_sales
   ,date_dim       d1
   ,store
 where
    d1.d_month_seq between 1213 and 1213+11
 and d1.d_date_sk = ss_sold_date_sk
 and s_store_sk  = ss_store_sk
 and s_state in
             ( select s_state
               from  (select s_state as s_state,
 			    rank() over ( partition by s_state order by sum(ss_net_profit) desc) as ranking
                      from   store_sales, store, date_dim
                      where  d_month_seq between 1213 and 1213+11
 			    and d_date_sk = ss_sold_date_sk
 			    and s_store_sk  = ss_store_sk
                      group by s_state
                     ) tmp1 
               where ranking <= 5
             )
 group by rollup(s_state,s_county)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then s_state end
  ,rank_within_parent
 LIMIT 100;

-- end query 70 in stream 0 using template query70.tpl

```

#### AFTER (Optimized)
```sql
SELECT SUM(ss_net_profit) AS total_sum, s_state, s_county, GROUPING(s_state) + GROUPING(s_county) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(s_state) + GROUPING(s_county), CASE WHEN GROUPING(s_county) = 0 THEN s_state END ORDER BY SUM(ss_net_profit) DESC) AS rank_within_parent FROM store_sales, date_dim AS d1, store WHERE d1.d_month_seq BETWEEN 1213 AND 1213 + 11 AND d1.d_date_sk = ss_sold_date_sk AND s_store_sk = ss_store_sk AND EXISTS(SELECT 1 FROM (SELECT s2.s_state AS s_state, RANK() OVER (PARTITION BY s2.s_state ORDER BY SUM(ss2.ss_net_profit) DESC) AS ranking FROM store_sales AS ss2, store AS s2, date_dim AS d2 WHERE d2.d_month_seq BETWEEN 1213 AND 1213 + 11 AND d2.d_date_sk = ss2.ss_sold_date_sk AND s2.s_store_sk = ss2.ss_store_sk GROUP BY s2.s_state) AS tmp1 WHERE tmp1.s_state = store.s_state AND tmp1.ranking <= 5) GROUP BY ROLLUP (s_state, s_county) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN s_state END, rank_within_parent LIMIT 100
```

---

### 159. Kimi Q31-Q99 - Q71

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 71 in stream 0 using template query71.tpl
select i_brand_id brand_id, i_brand brand,t_hour,t_minute,
 	sum(ext_price) ext_price
 from item, (select ws_ext_sales_price as ext_price, 
                        ws_sold_date_sk as sold_date_sk,
                        ws_item_sk as sold_item_sk,
                        ws_sold_time_sk as time_sk  
                 from web_sales,date_dim
                 where d_date_sk = ws_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 union all
                 select cs_ext_sales_price as ext_price,
                        cs_sold_date_sk as sold_date_sk,
                        cs_item_sk as sold_item_sk,
                        cs_sold_time_sk as time_sk
                 from catalog_sales,date_dim
                 where d_date_sk = cs_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 union all
                 select ss_ext_sales_price as ext_price,
                        ss_sold_date_sk as sold_date_sk,
                        ss_item_sk as sold_item_sk,
                        ss_sold_time_sk as time_sk
                 from store_sales,date_dim
                 where d_date_sk = ss_sold_date_sk
                   and d_moy=12
                   and d_year=1998
                 ) tmp,time_dim
 where
   sold_item_sk = i_item_sk
   and i_manager_id=1
   and time_sk = t_time_sk
   and (t_meal_time = 'breakfast' or t_meal_time = 'di...[truncated]
```

#### AFTER (Optimized)
```sql
WITH date_filtered_sales AS (SELECT ws_ext_sales_price AS ext_price, ws_sold_date_sk AS sold_date_sk, ws_item_sk AS sold_item_sk, ws_sold_time_sk AS time_sk FROM web_sales, date_dim WHERE d_date_sk = ws_sold_date_sk AND d_moy = 12 AND d_year = 1998 UNION ALL SELECT cs_ext_sales_price AS ext_price, cs_sold_date_sk AS sold_date_sk, cs_item_sk AS sold_item_sk, cs_sold_time_sk AS time_sk FROM catalog_sales, date_dim WHERE d_date_sk = cs_sold_date_sk AND d_moy = 12 AND d_year = 1998 UNION ALL SELECT ss_ext_sales_price AS ext_price, ss_sold_date_sk AS sold_date_sk, ss_item_sk AS sold_item_sk, ss_sold_time_sk AS time_sk FROM store_sales, date_dim WHERE d_date_sk = ss_sold_date_sk AND d_moy = 12 AND d_year = 1998), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manager_id = 1), meal_sales AS (SELECT i_brand_id AS brand_id, i_brand AS brand, t_hour, t_minute, ext_price FROM filtered_items, date_filtered_sales, time_dim WHERE sold_item_sk = i_item_sk AND time_sk = t_time_sk AND t_meal_time = 'breakfast' UNION ALL SELECT i_brand_id AS brand_id, i_brand AS brand, t_hour, t_minute, ext_price FROM filtered_items, date_filtered_sales, time_dim WHERE sold_item_sk = i_item_sk AND time_sk = t_time_sk AND t_meal_time = 'dinner')
SELECT brand_id, brand, t_hour, t_minute, SUM(ext_price) AS ext_price FROM meal_sales GROUP BY brand, brand_id, t_hour, t_minute ORDER BY ext_price DESC, brand_id
```

---

### 160. Kimi Q31-Q99 - Q72

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 72 in stream 0 using template query72.tpl
select i_item_desc
      ,w_warehouse_name
      ,d1.d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from catalog_sales
join inventory on (cs_item_sk = inv_item_sk)
join warehouse on (w_warehouse_sk=inv_warehouse_sk)
join item on (i_item_sk = cs_item_sk)
join customer_demographics on (cs_bill_cdemo_sk = cd_demo_sk)
join household_demographics on (cs_bill_hdemo_sk = hd_demo_sk)
join date_dim d1 on (cs_sold_date_sk = d1.d_date_sk)
join date_dim d2 on (inv_date_sk = d2.d_date_sk)
join date_dim d3 on (cs_ship_date_sk = d3.d_date_sk)
left outer join promotion on (cs_promo_sk=p_promo_sk)
left outer join catalog_returns on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where d1.d_week_seq = d2.d_week_seq
  and inv_quantity_on_hand < cs_quantity 
  and d3.d_date > d1.d_date + 5
  and hd_buy_potential = '501-1000'
  and d1.d_year = 2002
  and cd_marital_status = 'W'
group by i_item_desc,w_warehouse_name,d1.d_week_seq
order by total_cnt desc, i_item_desc, w_warehouse_name, d1.d_week_seq
 LIMIT 100;

-- end query 72 in stream 0 using template query72.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_d1 AS (SELECT d_date_sk, d_week_seq, d_date FROM date_dim WHERE d_year = 2002), filtered_cd AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_marital_status = 'W'), filtered_hd AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_buy_potential = '501-1000')
SELECT i_item_desc, w_warehouse_name, d1.d_week_seq, SUM(CASE WHEN p_promo_sk IS NULL THEN 1 ELSE 0 END) AS no_promo, SUM(CASE WHEN NOT p_promo_sk IS NULL THEN 1 ELSE 0 END) AS promo, COUNT(*) AS total_cnt FROM catalog_sales JOIN filtered_d1 AS d1 ON (cs_sold_date_sk = d1.d_date_sk) JOIN inventory ON (cs_item_sk = inv_item_sk AND inv_quantity_on_hand < cs_quantity) JOIN warehouse ON (w_warehouse_sk = inv_warehouse_sk) JOIN item ON (i_item_sk = cs_item_sk) JOIN filtered_cd ON (cs_bill_cdemo_sk = cd_demo_sk) JOIN filtered_hd ON (cs_bill_hdemo_sk = hd_demo_sk) JOIN date_dim AS d2 ON (inv_date_sk = d2.d_date_sk AND d1.d_week_seq = d2.d_week_seq) JOIN date_dim AS d3 ON (cs_ship_date_sk = d3.d_date_sk AND d3.d_date > d1.d_date + 5) LEFT OUTER JOIN promotion ON (cs_promo_sk = p_promo_sk) LEFT OUTER JOIN catalog_returns ON (cr_item_sk = cs_item_sk AND cr_order_number = cs_order_number) GROUP BY i_item_desc, w_warehouse_name, d1.d_week_seq ORDER BY total_cnt DESC, i_item_desc, w_warehouse_name, d1.d_week_seq LIMIT 100
```

---

### 161. Kimi Q31-Q99 - Q73

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 73 in stream 0 using template query73.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2 
    and (household_demographics.hd_buy_potential = '501-1000' or
         household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Fairfield County','Walker County','Daviess County','Barrow County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc;

-- end query 73 in stream 0 using template query73.tpl

```

#### AFTER (Optimized)
```sql
-- start query 73 in stream 0 using template query73.tpl
select c_last_name
       ,c_first_name
       ,c_salutation
       ,c_preferred_cust_flag 
       ,ss_ticket_number
       ,cnt from
   (select ss_ticket_number
          ,ss_customer_sk
          ,count(*) cnt
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and date_dim.d_dom between 1 and 2 
    and (household_demographics.hd_buy_potential = '501-1000' or
         household_demographics.hd_buy_potential = 'Unknown')
    and household_demographics.hd_vehicle_count > 0
    and case when household_demographics.hd_vehicle_count > 0 then 
             household_demographics.hd_dep_count/ household_demographics.hd_vehicle_count else null end > 1
    and date_dim.d_year in (2000,2000+1,2000+2)
    and store.s_county in ('Fairfield County','Walker County','Daviess County','Barrow County')
    group by ss_ticket_number,ss_customer_sk) dj,customer
    where ss_customer_sk = c_customer_sk
      and cnt between 1 and 5
    order by cnt desc, c_last_name asc;

-- end query 73 in stream 0 using template query73.tpl

```

---

### 162. Kimi Q31-Q99 - Q74

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
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
         and...[truncated]
```

#### AFTER (Optimized)
```sql
WITH year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ss_net_paid) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP BY c_customer_id, c_first_name, c_last_name, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ws_net_paid) AS year_total, 'w' AS sale_type FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP BY c_customer_id, c_first_name, c_last_name, d_year), year_total_store AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ss_net_paid) AS year_total FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP BY c_customer_id, c_first_name, c_last_name, d_year), year_total_web AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, d_year AS year, STDDEV_SAMP(ws_net_paid) AS year_total FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk AND d_year IN (1999, 1999 + 1) GROUP ...[truncated]
```

---

### 163. Kimi Q31-Q99 - Q75

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 75 in stream 0 using template query75.tpl
WITH all_sales AS (
 SELECT d_year
       ,i_brand_id
       ,i_class_id
       ,i_category_id
       ,i_manufact_id
       ,SUM(sales_cnt) AS sales_cnt
       ,SUM(sales_amt) AS sales_amt
 FROM (SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,cs_quantity - COALESCE(cr_return_quantity,0) AS sales_cnt
             ,cs_ext_sales_price - COALESCE(cr_return_amount,0.0) AS sales_amt
       FROM catalog_sales JOIN item ON i_item_sk=cs_item_sk
                          JOIN date_dim ON d_date_sk=cs_sold_date_sk
                          LEFT JOIN catalog_returns ON (cs_order_number=cr_order_number 
                                                    AND cs_item_sk=cr_item_sk)
       WHERE i_category='Home'
       UNION
       SELECT d_year
             ,i_brand_id
             ,i_class_id
             ,i_category_id
             ,i_manufact_id
             ,ss_quantity - COALESCE(sr_return_quantity,0) AS sales_cnt
             ,ss_ext_sales_price - COALESCE(sr_return_amt,0.0) AS sales_amt
       FROM store_sales JOIN item ON i_item_sk=ss_item_sk
                        JOIN date_dim ON d_date_sk=ss_sold_date_sk
                        LEFT JOIN store_returns ON (ss_ticket_number=sr_ticket_number 
                                                AND ss_item_sk=sr_item_sk)
       WHERE i_category='Home'
       UNION
       SELECT d_year
        ...[truncated]
```

#### AFTER (Optimized)
```sql
WITH all_sales AS (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, SUM(sales_cnt) AS sales_cnt, SUM(sales_amt) AS sales_amt FROM (SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, cs_quantity - COALESCE(cr_return_quantity, 0) AS sales_cnt, cs_ext_sales_price - COALESCE(cr_return_amount, 0.0) AS sales_amt FROM catalog_sales JOIN item ON i_item_sk = cs_item_sk JOIN date_dim ON d_date_sk = cs_sold_date_sk LEFT JOIN catalog_returns ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk) WHERE i_category = 'Home' AND d_year IN (1998, 1999) UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ss_quantity - COALESCE(sr_return_quantity, 0) AS sales_cnt, ss_ext_sales_price - COALESCE(sr_return_amt, 0.0) AS sales_amt FROM store_sales JOIN item ON i_item_sk = ss_item_sk JOIN date_dim ON d_date_sk = ss_sold_date_sk LEFT JOIN store_returns ON (ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk) WHERE i_category = 'Home' AND d_year IN (1998, 1999) UNION SELECT d_year, i_brand_id, i_class_id, i_category_id, i_manufact_id, ws_quantity - COALESCE(wr_return_quantity, 0) AS sales_cnt, ws_ext_sales_price - COALESCE(wr_return_amt, 0.0) AS sales_amt FROM web_sales JOIN item ON i_item_sk = ws_item_sk JOIN date_dim ON d_date_sk = ws_sold_date_sk LEFT JOIN web_returns ON (ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk) WHERE i_category = 'Home' AND d_year IN (1998, 1999)) AS sales_detail GROUP BY d_ye...[truncated]
```

---

### 164. Kimi Q31-Q99 - Q76

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 76 in stream 0 using template query76.tpl
select channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, 'ss_hdemo_sk' col_name, d_year, d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales, item, date_dim
         WHERE ss_hdemo_sk IS NULL
           AND ss_sold_date_sk=d_date_sk
           AND ss_item_sk=i_item_sk
        UNION ALL
        SELECT 'web' as channel, 'ws_bill_addr_sk' col_name, d_year, d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales, item, date_dim
         WHERE ws_bill_addr_sk IS NULL
           AND ws_sold_date_sk=d_date_sk
           AND ws_item_sk=i_item_sk
        UNION ALL
        SELECT 'catalog' as channel, 'cs_warehouse_sk' col_name, d_year, d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales, item, date_dim
         WHERE cs_warehouse_sk IS NULL
           AND cs_sold_date_sk=d_date_sk
           AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
 LIMIT 100;

-- end query 76 in stream 0 using template query76.tpl

```

#### AFTER (Optimized)
```sql
WITH store_sales_agg AS (SELECT 'store' AS channel, 'ss_hdemo_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(ss_ext_sales_price) AS sales_amt FROM store_sales, item, date_dim WHERE ss_hdemo_sk IS NULL AND ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category), web_sales_agg AS (SELECT 'web' AS channel, 'ws_bill_addr_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(ws_ext_sales_price) AS sales_amt FROM web_sales, item, date_dim WHERE ws_bill_addr_sk IS NULL AND ws_sold_date_sk = d_date_sk AND ws_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category), catalog_sales_agg AS (SELECT 'catalog' AS channel, 'cs_warehouse_sk' AS col_name, d_year, d_qoy, i_category, COUNT(*) AS sales_cnt, SUM(cs_ext_sales_price) AS sales_amt FROM catalog_sales, item, date_dim WHERE cs_warehouse_sk IS NULL AND cs_sold_date_sk = d_date_sk AND cs_item_sk = i_item_sk GROUP BY d_year, d_qoy, i_category)
SELECT channel, col_name, d_year, d_qoy, i_category, sales_cnt, sales_amt FROM store_sales_agg UNION ALL SELECT channel, col_name, d_year, d_qoy, i_category, sales_cnt, sales_amt FROM web_sales_agg UNION ALL SELECT channel, col_name, d_year, d_qoy, i_category, sales_cnt, sales_amt FROM catalog_sales_agg ORDER BY channel, col_name, d_year, d_qoy, i_category LIMIT 100
```

---

### 165. Kimi Q31-Q99 - Q77

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 77 in stream 0 using template query77.tpl
with ss as
 (select s_store_sk,
         sum(ss_ext_sales_price) as sales,
         sum(ss_net_profit) as profit
 from store_sales,
      date_dim,
      store
 where ss_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date) 
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY) 
       and ss_store_sk = s_store_sk
 group by s_store_sk)
 ,
 sr as
 (select s_store_sk,
         sum(sr_return_amt) as "returns",
         sum(sr_net_loss) as profit_loss
 from store_returns,
      date_dim,
      store
 where sr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
       and sr_store_sk = s_store_sk
 group by s_store_sk), 
 cs as
 (select cs_call_center_sk,
        sum(cs_ext_sales_price) as sales,
        sum(cs_net_profit) as profit
 from catalog_sales,
      date_dim
 where cs_sold_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
 group by cs_call_center_sk 
 ), 
 cr as
 (select cr_call_center_sk,
         sum(cr_return_amount) as "returns",
         sum(cr_net_loss) as profit_loss
 from catalog_returns,
      date_dim
 where cr_returned_date_sk = d_date_sk
       and d_date between cast('1998-08-05' as date)
                  and (cast('1998-08-05' as date) + INTERVAL 30 DAY)
 group by cr_call_cen...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ss AS (SELECT s_store_sk, SUM(ss_ext_sales_price) AS sales, SUM(ss_net_profit) AS profit FROM store_sales, date_dim, store WHERE ss_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) AND ss_store_sk = s_store_sk GROUP BY s_store_sk), sr AS (SELECT s_store_sk, SUM(sr_return_amt) AS "returns", SUM(sr_net_loss) AS profit_loss FROM store_returns, date_dim, store WHERE sr_returned_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) AND sr_store_sk = s_store_sk GROUP BY s_store_sk), cs AS (SELECT cs_call_center_sk, SUM(cs_ext_sales_price) AS sales, SUM(cs_net_profit) AS profit FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) GROUP BY cs_call_center_sk), cr AS (SELECT cr_call_center_sk, SUM(cr_return_amount) AS "returns", SUM(cr_net_loss) AS profit_loss FROM catalog_returns, date_dim WHERE cr_returned_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) GROUP BY cr_call_center_sk), ws AS (SELECT wp_web_page_sk, SUM(ws_ext_sales_price) AS sales, SUM(ws_net_profit) AS profit FROM web_sales, date_dim, web_page WHERE ws_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-05' AS DATE) AND (CAST('1998-08-05' AS DATE) + INTERVAL '30' DAY) AND ws_web_page_sk = wp_web...[truncated]
```

---

### 166. Kimi Q31-Q99 - Q78

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 78 in stream 0 using template query78.tpl
with ws as
  (select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk
   )
 select
ss_item_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 78 in stream 0 using template query78.tpl
with ws as
  (select d_year AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales
   left join web_returns on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   join date_dim on ws_sold_date_sk = d_date_sk
   where wr_order_number is null
   group by d_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select d_year AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales
   left join catalog_returns on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   join date_dim on cs_sold_date_sk = d_date_sk
   where cr_order_number is null
   group by d_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select d_year AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales
   left join store_returns on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   join date_dim on ss_sold_date_sk = d_date_sk
   where sr_ticket_number is null
   group by d_year, ss_item_sk, ss_customer_sk
   )
 select
ss_item_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws...[truncated]
```

---

### 167. Kimi Q31-Q99 - Q79

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 79 in stream 0 using template query79.tpl
select
  c_last_name,c_first_name,substr(s_city,1,30),ss_ticket_number,amt,profit
  from
   (select ss_ticket_number
          ,ss_customer_sk
          ,store.s_city
          ,sum(ss_coupon_amt) amt
          ,sum(ss_net_profit) profit
    from store_sales,date_dim,store,household_demographics
    where store_sales.ss_sold_date_sk = date_dim.d_date_sk
    and store_sales.ss_store_sk = store.s_store_sk  
    and store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    and (household_demographics.hd_dep_count = 5 or household_demographics.hd_vehicle_count > 4)
    and date_dim.d_dow = 1
    and date_dim.d_year in (1998,1998+1,1998+2) 
    and store.s_number_employees between 200 and 295
    group by ss_ticket_number,ss_customer_sk,ss_addr_sk,store.s_city) ms,customer
    where ss_customer_sk = c_customer_sk
 order by c_last_name,c_first_name,substr(s_city,1,30), profit
 LIMIT 100;

-- end query 79 in stream 0 using template query79.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_dow = 1 AND d_year IN (1998, 1999, 2000)), filtered_stores AS (SELECT s_store_sk, s_city FROM store WHERE s_number_employees BETWEEN 200 AND 295), ms_dep AS (SELECT ss_ticket_number, ss_customer_sk, s.s_city, SUM(ss_coupon_amt) AS amt, SUM(ss_net_profit) AS profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores AS s ON ss_store_sk = s.s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_dep_count = 5 GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, s.s_city), ms_vehicle AS (SELECT ss_ticket_number, ss_customer_sk, s.s_city, SUM(ss_coupon_amt) AS amt, SUM(ss_net_profit) AS profit FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_stores AS s ON ss_store_sk = s.s_store_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk WHERE hd_vehicle_count > 4 GROUP BY ss_ticket_number, ss_customer_sk, ss_addr_sk, s.s_city), ms AS (SELECT ss_ticket_number, ss_customer_sk, s_city, amt, profit FROM ms_dep UNION ALL SELECT ss_ticket_number, ss_customer_sk, s_city, amt, profit FROM ms_vehicle)
SELECT c_last_name, c_first_name, SUBSTRING(s_city, 1, 30), ss_ticket_number, amt, profit FROM ms JOIN customer ON ss_customer_sk = c_customer_sk ORDER BY c_last_name, c_first_name, SUBSTRING(s_city, 1, 30), profit LIMIT 100
```

---

### 168. Kimi Q31-Q99 - Q80

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 80 in stream 0 using template query80.tpl
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
       a...[truncated]
```

#### AFTER (Optimized)
```sql
WITH ssr AS (SELECT s_store_id AS store_id, SUM(ss_ext_sales_price) AS sales, SUM(COALESCE(sr_return_amt, 0)) AS "returns", SUM(ss_net_profit - COALESCE(sr_net_loss, 0)) AS profit FROM store_sales LEFT OUTER JOIN store_returns ON (ss_item_sk = sr_item_sk AND ss_ticket_number = sr_ticket_number), date_dim, store, item, promotion WHERE ss_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-28' AS DATE) AND (CAST('1998-08-28' AS DATE) + INTERVAL '30' DAY) AND ss_store_sk = s_store_sk AND ss_item_sk = i_item_sk AND i_current_price > 50 AND ss_promo_sk = p_promo_sk AND p_channel_tv = 'N' GROUP BY s_store_id), csr AS (SELECT cp_catalog_page_id AS catalog_page_id, SUM(cs_ext_sales_price) AS sales, SUM(COALESCE(cr_return_amount, 0)) AS "returns", SUM(cs_net_profit - COALESCE(cr_net_loss, 0)) AS profit FROM catalog_sales LEFT OUTER JOIN catalog_returns ON (cs_item_sk = cr_item_sk AND cs_order_number = cr_order_number), date_dim, catalog_page, item, promotion WHERE cs_sold_date_sk = d_date_sk AND d_date BETWEEN CAST('1998-08-28' AS DATE) AND (CAST('1998-08-28' AS DATE) + INTERVAL '30' DAY) AND cs_catalog_page_sk = cp_catalog_page_sk AND cs_item_sk = i_item_sk AND i_current_price > 50 AND cs_promo_sk = p_promo_sk AND p_channel_tv = 'N' GROUP BY cp_catalog_page_id), wsr AS (SELECT web_site_id, SUM(ws_ext_sales_price) AS sales, SUM(COALESCE(wr_return_amt, 0)) AS "returns", SUM(ws_net_profit - COALESCE(wr_net_loss, 0)) AS profit FROM web_sales LEFT OUTER JOIN web_returns ON (ws_item_...[truncated]
```

---

### 169. Kimi Q31-Q99 - Q81

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 81 in stream 0 using template query81.tpl
with customer_total_return as
 (select cr_returning_customer_sk as ctr_customer_sk
        ,ca_state as ctr_state, 
 	sum(cr_return_amt_inc_tax) as ctr_total_return
 from catalog_returns
     ,date_dim
     ,customer_address
 where cr_returned_date_sk = d_date_sk 
   and d_year =2002
   and cr_returning_addr_sk = ca_address_sk 
 group by cr_returning_customer_sk
         ,ca_state )
  select c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 from customer_total_return ctr1
     ,customer_address
     ,customer
 where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
 			  from customer_total_return ctr2 
                  	  where ctr1.ctr_state = ctr2.ctr_state)
       and ca_address_sk = c_current_addr_sk
       and ca_state = 'CA'
       and ctr1.ctr_customer_sk = c_customer_sk
 order by c_customer_id,c_salutation,c_first_name,c_last_name,ca_street_number,ca_street_name
                   ,ca_street_type,ca_suite_number,ca_city,ca_county,ca_state,ca_zip,ca_country,ca_gmt_offset
                  ,ca_location_type,ctr_total_return
 LIMIT 100;

-- end query 81 in stream 0 using template query81.tpl

```

#### AFTER (Optimized)
```sql
WITH customer_total_return AS (SELECT cr.cr_returning_customer_sk AS ctr_customer_sk, ca.ca_state AS ctr_state, SUM(cr.cr_return_amt_inc_tax) AS ctr_total_return FROM catalog_returns AS cr JOIN date_dim AS d ON cr.cr_returned_date_sk = d.d_date_sk JOIN customer_address AS ca ON cr.cr_returning_addr_sk = ca.ca_address_sk JOIN customer AS c ON cr.cr_returning_customer_sk = c.c_customer_sk JOIN customer_address AS ca_current ON c.c_current_addr_sk = ca_current.ca_address_sk WHERE d.d_year = 2002 AND ca_current.ca_state = 'CA' GROUP BY cr.cr_returning_customer_sk, ca.ca_state), state_avg_return AS (SELECT ctr_state, AVG(ctr_total_return) * 1.2 AS avg_return_threshold FROM customer_total_return GROUP BY ctr_state)
SELECT c.c_customer_id, c.c_salutation, c.c_first_name, c.c_last_name, ca.ca_street_number, ca.ca_street_name, ca.ca_street_type, ca.ca_suite_number, ca.ca_city, ca.ca_county, ca.ca_state, ca.ca_zip, ca.ca_country, ca.ca_gmt_offset, ca.ca_location_type, ctr1.ctr_total_return FROM customer_total_return AS ctr1 JOIN state_avg_return AS sar ON ctr1.ctr_state = sar.ctr_state JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk WHERE ctr1.ctr_total_return > sar.avg_return_threshold ORDER BY c.c_customer_id, c.c_salutation, c.c_first_name, c.c_last_name, ca.ca_street_number, ca.ca_street_name, ca.ca_street_type, ca.ca_suite_number, ca.ca_city, ca.ca_county, ca.ca_state, ca.ca_zip, ca.ca_country, ca.c...[truncated]
```

---

### 170. Kimi Q31-Q99 - Q82

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 82 in stream 0 using template query82.tpl
select i_item_id
       ,i_item_desc
       ,i_current_price
 from item, inventory, date_dim, store_sales
 where i_current_price between 17 and 17+30
 and inv_item_sk = i_item_sk
 and d_date_sk=inv_date_sk
 and d_date between cast('1999-07-09' as date) and (cast('1999-07-09' as date) + INTERVAL 60 DAY)
 and i_manufact_id in (639,169,138,339)
 and inv_quantity_on_hand between 100 and 500
 and ss_item_sk = i_item_sk
 group by i_item_id,i_item_desc,i_current_price
 order by i_item_id
 LIMIT 100;

-- end query 82 in stream 0 using template query82.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_item AS (SELECT i_item_sk, i_item_id, i_item_desc, i_current_price FROM item WHERE i_current_price BETWEEN 17 AND 17 + 30 AND i_manufact_id IN (639, 169, 138, 339)), filtered_inventory AS (SELECT inv_item_sk, inv_date_sk FROM inventory WHERE inv_quantity_on_hand BETWEEN 100 AND 500), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1999-07-09' AS DATE) AND (CAST('1999-07-09' AS DATE) + INTERVAL '60' DAY)), filtered_sales AS (SELECT ss_item_sk FROM store_sales)
SELECT i.i_item_id, i.i_item_desc, i.i_current_price FROM filtered_item AS i JOIN filtered_inventory AS inv ON i.i_item_sk = inv.inv_item_sk JOIN filtered_dates AS d ON inv.inv_date_sk = d.d_date_sk JOIN filtered_sales AS ss ON i.i_item_sk = ss.ss_item_sk GROUP BY i.i_item_id, i.i_item_desc, i.i_current_price ORDER BY i.i_item_id LIMIT 100
```

---

### 171. Kimi Q31-Q99 - Q83

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 83 in stream 0 using template query83.tpl
with sr_items as
 (select i_item_id item_id,
        sum(sr_return_quantity) sr_item_qty
 from store_returns,
      item,
      date_dim
 where sr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   sr_returned_date_sk   = d_date_sk
 group by i_item_id),
 cr_items as
 (select i_item_id item_id,
        sum(cr_return_quantity) cr_item_qty
 from catalog_returns,
      item,
      date_dim
 where cr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
	  where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   cr_returned_date_sk   = d_date_sk
 group by i_item_id),
 wr_items as
 (select i_item_id item_id,
        sum(wr_return_quantity) wr_item_qty
 from web_returns,
      item,
      date_dim
 where wr_item_sk = i_item_sk
 and   d_date    in 
	(select d_date
	from date_dim
	where d_week_seq in 
		(select d_week_seq
		from date_dim
		where d_date in ('2001-06-06','2001-09-02','2001-11-11')))
 and   wr_returned_date_sk   = d_date_sk
 group by i_item_id)
  select sr_items.item_id
       ,sr_item_qty
       ,sr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 sr_dev
       ,cr_item_qty
       ,cr_item_qty/(sr_item_qty+cr_item_qty+wr_item_qty)/3.0 * 100 cr_dev
       ,wr_item_qty
       ,...[truncated]
```

#### AFTER (Optimized)
```sql
WITH target_week_dates AS (SELECT DISTINCT d_date_sk FROM date_dim WHERE d_week_seq IN (SELECT d_week_seq FROM date_dim WHERE d_date IN ('2001-06-06', '2001-09-02', '2001-11-11'))), sr_items AS (SELECT i_item_id AS item_id, SUM(sr_return_quantity) AS sr_item_qty FROM store_returns, item, target_week_dates WHERE sr_item_sk = i_item_sk AND sr_returned_date_sk = target_week_dates.d_date_sk GROUP BY i_item_id), cr_items AS (SELECT i_item_id AS item_id, SUM(cr_return_quantity) AS cr_item_qty FROM catalog_returns, item, target_week_dates WHERE cr_item_sk = i_item_sk AND cr_returned_date_sk = target_week_dates.d_date_sk GROUP BY i_item_id), wr_items AS (SELECT i_item_id AS item_id, SUM(wr_return_quantity) AS wr_item_qty FROM web_returns, item, date_dim WHERE wr_item_sk = i_item_sk AND d_date IN (SELECT d_date FROM date_dim WHERE d_week_seq IN (SELECT d_week_seq FROM date_dim WHERE d_date IN ('2001-06-06', '2001-09-02', '2001-11-11'))) AND wr_returned_date_sk = d_date_sk GROUP BY i_item_id) SELECT sr_items.item_id, sr_item_qty, sr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS sr_dev, cr_item_qty, cr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS cr_dev, wr_item_qty, wr_item_qty / (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 * 100 AS wr_dev, (sr_item_qty + cr_item_qty + wr_item_qty) / 3.0 AS average FROM sr_items, cr_items, wr_items WHERE sr_items.item_id = cr_items.item_id AND sr_items.item_id = wr_items.item_id ORDER BY sr_items.item_id,...[truncated]
```

---

### 172. Kimi Q31-Q99 - Q84

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 84 in stream 0 using template query84.tpl
select c_customer_id as customer_id
       , coalesce(c_last_name,'') || ', ' || coalesce(c_first_name,'') as customername
 from customer
     ,customer_address
     ,customer_demographics
     ,household_demographics
     ,income_band
     ,store_returns
 where ca_city	        =  'Oakwood'
   and c_current_addr_sk = ca_address_sk
   and ib_lower_bound   >=  5806
   and ib_upper_bound   <=  5806 + 50000
   and ib_income_band_sk = hd_income_band_sk
   and cd_demo_sk = c_current_cdemo_sk
   and hd_demo_sk = c_current_hdemo_sk
   and sr_cdemo_sk = cd_demo_sk
 order by c_customer_id
 LIMIT 100;

-- end query 84 in stream 0 using template query84.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_income_band AS (SELECT ib_income_band_sk FROM income_band WHERE ib_lower_bound >= 5806 AND ib_upper_bound <= 5806 + 50000), filtered_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_city = 'Oakwood')
SELECT c.c_customer_id AS customer_id, COALESCE(c.c_last_name, '') || ', ' || COALESCE(c.c_first_name, '') AS customername FROM customer AS c JOIN filtered_address AS a ON c.c_current_addr_sk = a.ca_address_sk JOIN household_demographics AS hd ON c.c_current_hdemo_sk = hd.hd_demo_sk JOIN filtered_income_band AS ib ON hd.hd_income_band_sk = ib.ib_income_band_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk JOIN store_returns AS sr ON sr.sr_cdemo_sk = cd.cd_demo_sk ORDER BY c_customer_id LIMIT 100
```

---

### 173. Kimi Q31-Q99 - Q85

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 85 in stream 0 using template query85.tpl
select substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2000
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '4 yr Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Secondary' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'W'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )...[truncated]
```

#### AFTER (Optimized)
```sql
-- start query 85 in stream 0 using template query85.tpl
select substr(r_reason_desc,1,20)
       ,avg(ws_quantity)
       ,avg(wr_refunded_cash)
       ,avg(wr_fee)
 from web_sales, web_returns, web_page, customer_demographics cd1,
      customer_demographics cd2, customer_address, date_dim, reason 
 where ws_web_page_sk = wp_web_page_sk
   and ws_item_sk = wr_item_sk
   and ws_order_number = wr_order_number
   and ws_sold_date_sk = d_date_sk and d_year = 2000
   and cd1.cd_demo_sk = wr_refunded_cdemo_sk 
   and cd2.cd_demo_sk = wr_returning_cdemo_sk
   and ca_address_sk = wr_refunded_addr_sk
   and r_reason_sk = wr_reason_sk
   and
   (
    (
     cd1.cd_marital_status = 'M'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = '4 yr Degree'
     and 
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 100.00 and 150.00
    )
   or
    (
     cd1.cd_marital_status = 'S'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Secondary' 
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 50.00 and 100.00
    )
   or
    (
     cd1.cd_marital_status = 'W'
     and
     cd1.cd_marital_status = cd2.cd_marital_status
     and
     cd1.cd_education_status = 'Advanced Degree'
     and
     cd1.cd_education_status = cd2.cd_education_status
     and
     ws_sales_price between 150.00 and 200.00
    )
   )...[truncated]
```

---

### 174. Kimi Q31-Q99 - Q86

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 86 in stream 0 using template query86.tpl
select  
    sum(ws_net_paid) as total_sum
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ws_net_paid) desc) as rank_within_parent
 from
    web_sales
   ,date_dim       d1
   ,item
 where
    d1.d_month_seq between 1224 and 1224+11
 and d1.d_date_sk = ws_sold_date_sk
 and i_item_sk  = ws_item_sk
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc,
   case when lochierarchy = 0 then i_category end,
   rank_within_parent
 LIMIT 100;

-- end query 86 in stream 0 using template query86.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1224 AND 1224 + 11), filtered_sales AS (SELECT ws_net_paid, ws_item_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk)
SELECT SUM(ws_net_paid) AS total_sum, i_category, i_class, GROUPING(i_category) + GROUPING(i_class) AS lochierarchy, RANK() OVER (PARTITION BY GROUPING(i_category) + GROUPING(i_class), CASE WHEN GROUPING(i_class) = 0 THEN i_category END ORDER BY SUM(ws_net_paid) DESC) AS rank_within_parent FROM filtered_sales JOIN item ON ws_item_sk = i_item_sk GROUP BY ROLLUP (i_category, i_class) ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category END, rank_within_parent LIMIT 100
```

---

### 175. Kimi Q31-Q99 - Q87

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 87 in stream 0 using template query87.tpl
select count(*) 
from ((select distinct c_last_name, c_first_name, d_date
       from store_sales, date_dim, customer
       where store_sales.ss_sold_date_sk = date_dim.d_date_sk
         and store_sales.ss_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from catalog_sales, date_dim, customer
       where catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
       except
      (select distinct c_last_name, c_first_name, d_date
       from web_sales, date_dim, customer
       where web_sales.ws_sold_date_sk = date_dim.d_date_sk
         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
         and d_month_seq between 1184 and 1184+11)
) cool_cust
;

-- end query 87 in stream 0 using template query87.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_date FROM date_dim WHERE d_month_seq BETWEEN 1184 AND 1184 + 11), store_branch AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN customer ON ss_customer_sk = c_customer_sk), catalog_branch AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk), web_branch AS (SELECT DISTINCT c_last_name, c_first_name, d_date FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN customer ON ws_bill_customer_sk = c_customer_sk)
SELECT COUNT(*) FROM ((SELECT * FROM store_branch) EXCEPT (SELECT * FROM catalog_branch) EXCEPT (SELECT * FROM web_branch)) AS cool_cust
```

---

### 176. Kimi Q31-Q99 - Q88

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
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
     and...[truncated]
```

#### AFTER (Optimized)
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
     and...[truncated]
```

---

### 177. Kimi Q31-Q99 - Q89

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 89 in stream 0 using template query89.tpl
select *
from(
select i_category, i_class, i_brand,
       s_store_name, s_company_name,
       d_moy,
       sum(ss_sales_price) sum_sales,
       avg(sum(ss_sales_price)) over
         (partition by i_category, i_brand, s_store_name, s_company_name)
         avg_monthly_sales
from item, store_sales, date_dim, store
where ss_item_sk = i_item_sk and
      ss_sold_date_sk = d_date_sk and
      ss_store_sk = s_store_sk and
      d_year in (1999) and
        ((i_category in ('Jewelry','Shoes','Electronics') and
          i_class in ('semi-precious','athletic','portable')
         )
      or (i_category in ('Men','Music','Women') and
          i_class in ('accessories','rock','maternity') 
        ))
group by i_category, i_class, i_brand,
         s_store_name, s_company_name, d_moy) tmp1
where case when (avg_monthly_sales <> 0) then (abs(sum_sales - avg_monthly_sales) / avg_monthly_sales) else null end > 0.1
order by sum_sales - avg_monthly_sales, s_store_name
 LIMIT 100;

-- end query 89 in stream 0 using template query89.tpl

```

#### AFTER (Optimized)
```sql
WITH sales_branch_1 AS (SELECT i.i_category, i.i_class, i.i_brand, s.s_store_name, s.s_company_name, d.d_moy, SUM(ss.ss_sales_price) AS sum_sales FROM item AS i JOIN store_sales AS ss ON ss.ss_item_sk = i.i_item_sk JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE d.d_year = 1999 AND i.i_category IN ('Jewelry', 'Shoes', 'Electronics') AND i.i_class IN ('semi-precious', 'athletic', 'portable') GROUP BY i.i_category, i.i_class, i.i_brand, s.s_store_name, s.s_company_name, d.d_moy), sales_branch_2 AS (SELECT i.i_category, i.i_class, i.i_brand, s.s_store_name, s.s_company_name, d.d_moy, SUM(ss.ss_sales_price) AS sum_sales FROM item AS i JOIN store_sales AS ss ON ss.ss_item_sk = i.i_item_sk JOIN date_dim AS d ON ss.ss_sold_date_sk = d.d_date_sk JOIN store AS s ON ss.ss_store_sk = s.s_store_sk WHERE d.d_year = 1999 AND i.i_category IN ('Men', 'Music', 'Women') AND i.i_class IN ('accessories', 'rock', 'maternity') GROUP BY i.i_category, i.i_class, i.i_brand, s.s_store_name, s.s_company_name, d.d_moy), tmp1 AS (SELECT i_category, i_class, i_brand, s_store_name, s_company_name, d_moy, sum_sales, AVG(sum_sales) OVER (PARTITION BY i_category, i_brand, s_store_name, s_company_name) AS avg_monthly_sales FROM (SELECT * FROM sales_branch_1 UNION ALL SELECT * FROM sales_branch_2) AS unioned_sales)
SELECT * FROM tmp1 WHERE CASE WHEN (avg_monthly_sales <> 0) THEN (ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales) ELSE NULL END >...[truncated]
```

---

### 178. Kimi Q31-Q99 - Q90

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 90 in stream 0 using template query90.tpl
select cast(amc as decimal(15,4))/cast(pmc as decimal(15,4)) am_pm_ratio
 from ( select count(*) amc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 10 and 10+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 5000 and 5200) at_tbl,
      ( select count(*) pmc
       from web_sales, household_demographics , time_dim, web_page
       where ws_sold_time_sk = time_dim.t_time_sk
         and ws_ship_hdemo_sk = household_demographics.hd_demo_sk
         and ws_web_page_sk = web_page.wp_web_page_sk
         and time_dim.t_hour between 16 and 16+1
         and household_demographics.hd_dep_count = 2
         and web_page.wp_char_count between 5000 and 5200) pt
 order by am_pm_ratio
 LIMIT 100;

-- end query 90 in stream 0 using template query90.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_web_data AS (SELECT CASE WHEN t.t_hour BETWEEN 10 AND 11 THEN 1 END AS am_flag, CASE WHEN t.t_hour BETWEEN 16 AND 17 THEN 1 END AS pm_flag FROM web_sales AS ws JOIN household_demographics AS hd ON ws.ws_ship_hdemo_sk = hd.hd_demo_sk JOIN time_dim AS t ON ws.ws_sold_time_sk = t.t_time_sk JOIN web_page AS wp ON ws.ws_web_page_sk = wp.wp_web_page_sk WHERE hd.hd_dep_count = 2 AND wp.wp_char_count BETWEEN 5000 AND 5200 AND (t.t_hour BETWEEN 10 AND 11 OR t.t_hour BETWEEN 16 AND 17)), counts AS (SELECT COUNT(am_flag) AS amc, COUNT(pm_flag) AS pmc FROM filtered_web_data)
SELECT CAST(amc AS DECIMAL(15, 4)) / CAST(pmc AS DECIMAL(15, 4)) AS am_pm_ratio FROM counts ORDER BY am_pm_ratio LIMIT 100
```

---

### 179. Kimi Q31-Q99 - Q91

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 91 in stream 0 using template query91.tpl
select  
        cc_call_center_id Call_Center,
        cc_name Call_Center_Name,
        cc_manager Manager,
        sum(cr_net_loss) Returns_Loss
from
        call_center,
        catalog_returns,
        date_dim,
        customer,
        customer_address,
        customer_demographics,
        household_demographics
where
        cr_call_center_sk       = cc_call_center_sk
and     cr_returned_date_sk     = d_date_sk
and     cr_returning_customer_sk= c_customer_sk
and     cd_demo_sk              = c_current_cdemo_sk
and     hd_demo_sk              = c_current_hdemo_sk
and     ca_address_sk           = c_current_addr_sk
and     d_year                  = 2001 
and     d_moy                   = 11
and     ( (cd_marital_status       = 'M' and cd_education_status     = 'Unknown')
        or(cd_marital_status       = 'W' and cd_education_status     = 'Advanced Degree'))
and     hd_buy_potential like '1001-5000%'
and     ca_gmt_offset           = -6
group by cc_call_center_id,cc_name,cc_manager,cd_marital_status,cd_education_status
order by sum(cr_net_loss) desc;

-- end query 91 in stream 0 using template query91.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy = 11), filtered_returns AS (SELECT cr.cr_net_loss, cc.cc_call_center_id, cc.cc_name, cc.cc_manager, cd.cd_marital_status, cd.cd_education_status FROM catalog_returns AS cr JOIN filtered_dates AS d ON cr.cr_returned_date_sk = d.d_date_sk JOIN call_center AS cc ON cr.cr_call_center_sk = cc.cc_call_center_sk JOIN customer AS c ON cr.cr_returning_customer_sk = c.c_customer_sk JOIN customer_demographics AS cd ON c.c_current_cdemo_sk = cd.cd_demo_sk JOIN household_demographics AS hd ON c.c_current_hdemo_sk = hd.hd_demo_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk WHERE cd.cd_marital_status = 'M' AND cd.cd_education_status = 'Unknown' AND hd.hd_buy_potential LIKE '1001-5000%' AND ca.ca_gmt_offset = -6 UNION ALL SELECT cr.cr_net_loss, cc.cc_call_center_id, cc.cc_name, cc.cc_manager, cd.cd_marital_status, cd.cd_education_status FROM catalog_returns AS cr JOIN filtered_dates AS d ON cr.cr_returned_date_sk = d.d_date_sk JOIN call_center AS cc ON cr.cr_call_center_sk = cc.cc_call_center_sk JOIN customer AS c ON cr.cr_returning_customer_sk = c.c_customer_sk JOIN customer_demographics AS cd ON c.c_current_cdemo_sk = cd.cd_demo_sk JOIN household_demographics AS hd ON c.c_current_hdemo_sk = hd.hd_demo_sk JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk WHERE cd.cd_marital_status = 'W' AND cd.cd_education_status = 'Advanced Degree' AND hd.hd_buy_potential LIKE ...[truncated]
```

---

### 180. Kimi Q31-Q99 - Q92

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 92 in stream 0 using template query92.tpl
select 
   sum(ws_ext_discount_amt)  as "Excess Discount Amount" 
from 
    web_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 320
and i_item_sk = ws_item_sk 
and d_date between '2002-02-26' and 
        (cast('2002-02-26' as date) + INTERVAL 90 DAY)
and d_date_sk = ws_sold_date_sk 
and ws_ext_discount_amt  
     > ( 
         SELECT 
            1.3 * avg(ws_ext_discount_amt) 
         FROM 
            web_sales 
           ,date_dim
         WHERE 
              ws_item_sk = i_item_sk 
          and d_date between '2002-02-26' and
                             (cast('2002-02-26' as date) + INTERVAL 90 DAY)
          and d_date_sk = ws_sold_date_sk 
      ) 
order by sum(ws_ext_discount_amt)
 LIMIT 100;

-- end query 92 in stream 0 using template query92.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-02-26' AND (CAST('2002-02-26' AS DATE) + INTERVAL '90' DAY)), filtered_items AS (SELECT i_item_sk FROM item WHERE i_manufact_id = 320), item_avg_discount AS (SELECT ws.ws_item_sk, 1.3 * AVG(ws.ws_ext_discount_amt) AS discount_threshold FROM web_sales AS ws JOIN filtered_dates AS fd ON ws.ws_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ws.ws_item_sk = fi.i_item_sk GROUP BY ws.ws_item_sk)
SELECT SUM(ws.ws_ext_discount_amt) AS "Excess Discount Amount" FROM web_sales AS ws JOIN filtered_dates AS fd ON ws.ws_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ws.ws_item_sk = fi.i_item_sk JOIN item_avg_discount AS iad ON ws.ws_item_sk = iad.ws_item_sk WHERE ws.ws_ext_discount_amt > iad.discount_threshold ORDER BY SUM(ws.ws_ext_discount_amt) LIMIT 100
```

---

### 181. Kimi Q31-Q99 - Q93

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 93 in stream 0 using template query93.tpl
select ss_customer_sk
            ,sum(act_sales) sumsales
      from (select ss_item_sk
                  ,ss_ticket_number
                  ,ss_customer_sk
                  ,case when sr_return_quantity is not null then (ss_quantity-sr_return_quantity)*ss_sales_price
                                                            else (ss_quantity*ss_sales_price) end act_sales
            from store_sales left outer join store_returns on (sr_item_sk = ss_item_sk
                                                               and sr_ticket_number = ss_ticket_number)
                ,reason
            where sr_reason_sk = r_reason_sk
              and r_reason_desc = 'duplicate purchase') t
      group by ss_customer_sk
      order by sumsales, ss_customer_sk
 LIMIT 100;

-- end query 93 in stream 0 using template query93.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_reason AS (SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'duplicate purchase'), filtered_returns AS (SELECT sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN filtered_reason ON sr_reason_sk = r_reason_sk)
SELECT ss_customer_sk, SUM(act_sales) AS sumsales FROM (SELECT ss.ss_customer_sk, CASE WHEN NOT fr.sr_return_quantity IS NULL THEN (ss.ss_quantity - fr.sr_return_quantity) * ss.ss_sales_price ELSE (ss.ss_quantity * ss.ss_sales_price) END AS act_sales FROM store_sales AS ss JOIN filtered_returns AS fr ON (fr.sr_item_sk = ss.ss_item_sk AND fr.sr_ticket_number = ss.ss_ticket_number)) AS t GROUP BY ss_customer_sk ORDER BY sumsales, ss_customer_sk LIMIT 100
```

---

### 182. Kimi Q31-Q99 - Q94

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 94 in stream 0 using template query94.tpl
select 
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '2000-2-01' and 
           (cast('2000-2-01' as date) + INTERVAL 60 DAY)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'OK'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and exists (select *
            from web_sales ws2
            where ws1.ws_order_number = ws2.ws_order_number
              and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
and not exists(select *
               from web_returns wr1
               where ws1.ws_order_number = wr1.wr_order_number)
order by count(distinct ws_order_number)
 LIMIT 100;

-- end query 94 in stream 0 using template query94.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2000-2-01' AND (CAST('2000-2-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'OK'), filtered_sites AS (SELECT web_site_sk FROM web_site WHERE web_company_name = 'pri'), multi_warehouse_orders AS (SELECT ws_order_number FROM web_sales GROUP BY ws_order_number HAVING COUNT(DISTINCT ws_warehouse_sk) > 1), orders_with_returns AS (SELECT DISTINCT wr_order_number FROM web_returns)
SELECT COUNT(DISTINCT ws1.ws_order_number) AS "order count", SUM(ws1.ws_ext_ship_cost) AS "total shipping cost", SUM(ws1.ws_net_profit) AS "total net profit" FROM web_sales AS ws1 JOIN filtered_dates ON ws1.ws_ship_date_sk = filtered_dates.d_date_sk JOIN filtered_addresses ON ws1.ws_ship_addr_sk = filtered_addresses.ca_address_sk JOIN filtered_sites ON ws1.ws_web_site_sk = filtered_sites.web_site_sk JOIN multi_warehouse_orders AS mwo ON ws1.ws_order_number = mwo.ws_order_number LEFT JOIN orders_with_returns AS owr ON ws1.ws_order_number = owr.wr_order_number WHERE owr.wr_order_number IS NULL ORDER BY COUNT(DISTINCT ws1.ws_order_number) LIMIT 100
```

---

### 183. Kimi Q31-Q99 - Q95

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 95 in stream 0 using template query95.tpl
with ws_wh as
(select ws1.ws_order_number,ws1.ws_warehouse_sk wh1,ws2.ws_warehouse_sk wh2
 from web_sales ws1,web_sales ws2
 where ws1.ws_order_number = ws2.ws_order_number
   and ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 select 
   count(distinct ws_order_number) as "order count"
  ,sum(ws_ext_ship_cost) as "total shipping cost"
  ,sum(ws_net_profit) as "total net profit"
from
   web_sales ws1
  ,date_dim
  ,customer_address
  ,web_site
where
    d_date between '1999-2-01' and 
           (cast('1999-2-01' as date) + INTERVAL 60 DAY)
and ws1.ws_ship_date_sk = d_date_sk
and ws1.ws_ship_addr_sk = ca_address_sk
and ca_state = 'NC'
and ws1.ws_web_site_sk = web_site_sk
and web_company_name = 'pri'
and ws1.ws_order_number in (select ws_order_number
                            from ws_wh)
and ws1.ws_order_number in (select wr_order_number
                            from web_returns,ws_wh
                            where wr_order_number = ws_wh.ws_order_number)
order by count(distinct ws_order_number)
 LIMIT 100;

-- end query 95 in stream 0 using template query95.tpl

```

#### AFTER (Optimized)
```sql
WITH ws_wh AS (SELECT ws1.ws_order_number, ws1.ws_warehouse_sk AS wh1, ws2.ws_warehouse_sk AS wh2 FROM web_sales AS ws1, web_sales AS ws2 WHERE ws1.ws_order_number = ws2.ws_order_number AND ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
SELECT COUNT(DISTINCT ws_order_number) AS "order count", SUM(ws_ext_ship_cost) AS "total shipping cost", SUM(ws_net_profit) AS "total net profit" FROM web_sales AS ws1 JOIN date_dim ON ws1.ws_ship_date_sk = d_date_sk JOIN customer_address ON ws1.ws_ship_addr_sk = ca_address_sk JOIN web_site ON ws1.ws_web_site_sk = web_site_sk WHERE d_date BETWEEN '1999-2-01' AND (CAST('1999-2-01' AS DATE) + INTERVAL '60' DAY) AND ca_state = 'NC' AND web_company_name = 'pri' AND EXISTS(SELECT 1 FROM ws_wh WHERE ws_wh.ws_order_number = ws1.ws_order_number) AND EXISTS(SELECT 1 FROM web_returns JOIN ws_wh ON wr_order_number = ws_wh.ws_order_number WHERE wr_order_number = ws1.ws_order_number) ORDER BY COUNT(DISTINCT ws_order_number) LIMIT 100
```

---

### 184. Kimi Q31-Q99 - Q96

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 96 in stream 0 using template query96.tpl
select count(*) 
from store_sales
    ,household_demographics 
    ,time_dim, store
where ss_sold_time_sk = time_dim.t_time_sk   
    and ss_hdemo_sk = household_demographics.hd_demo_sk 
    and ss_store_sk = s_store_sk
    and time_dim.t_hour = 8
    and time_dim.t_minute >= 30
    and household_demographics.hd_dep_count = 3
    and store.s_store_name = 'ese'
order by count(*)
 LIMIT 100;

-- end query 96 in stream 0 using template query96.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_time AS (SELECT t_time_sk FROM time_dim WHERE t_hour = 8 AND t_minute >= 30), filtered_household AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_dep_count = 3), filtered_store AS (SELECT s_store_sk FROM store WHERE s_store_name = 'ese')
SELECT COUNT(*) FROM store_sales JOIN filtered_time ON ss_sold_time_sk = filtered_time.t_time_sk JOIN filtered_household ON ss_hdemo_sk = filtered_household.hd_demo_sk JOIN filtered_store ON ss_store_sk = filtered_store.s_store_sk ORDER BY COUNT(*) LIMIT 100
```

---

### 185. Kimi Q31-Q99 - Q97

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 97 in stream 0 using template query97.tpl
with ssci as (
select ss_customer_sk customer_sk
      ,ss_item_sk item_sk
from store_sales,date_dim
where ss_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by ss_customer_sk
        ,ss_item_sk),
csci as(
 select cs_bill_customer_sk customer_sk
      ,cs_item_sk item_sk
from catalog_sales,date_dim
where cs_sold_date_sk = d_date_sk
  and d_month_seq between 1214 and 1214 + 11
group by cs_bill_customer_sk
        ,cs_item_sk)
 select sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1 else 0 end) store_only
      ,sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1 else 0 end) catalog_only
      ,sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 else 0 end) store_and_catalog
from ssci full outer join csci on (ssci.customer_sk=csci.customer_sk
                               and ssci.item_sk = csci.item_sk)
 LIMIT 100;

-- end query 97 in stream 0 using template query97.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1214 AND 1214 + 11), ssci AS (SELECT ss.ss_customer_sk AS customer_sk, ss.ss_item_sk AS item_sk FROM store_sales AS ss JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk GROUP BY ss.ss_customer_sk, ss.ss_item_sk), csci AS (SELECT cs.cs_bill_customer_sk AS customer_sk, cs.cs_item_sk AS item_sk FROM catalog_sales AS cs JOIN filtered_dates AS d ON cs.cs_sold_date_sk = d.d_date_sk GROUP BY cs.cs_bill_customer_sk, cs.cs_item_sk)
SELECT SUM(CASE WHEN NOT ssci.customer_sk IS NULL AND csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_only, SUM(CASE WHEN ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS catalog_only, SUM(CASE WHEN NOT ssci.customer_sk IS NULL AND NOT csci.customer_sk IS NULL THEN 1 ELSE 0 END) AS store_and_catalog FROM ssci FULL OUTER JOIN csci ON (ssci.customer_sk = csci.customer_sk AND ssci.item_sk = csci.item_sk) LIMIT 100
```

---

### 186. Kimi Q31-Q99 - Q98

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 98 in stream 0 using template query98.tpl
select i_item_id
      ,i_item_desc 
      ,i_category 
      ,i_class 
      ,i_current_price
      ,sum(ss_ext_sales_price) as itemrevenue 
      ,sum(ss_ext_sales_price)*100/sum(sum(ss_ext_sales_price)) over
          (partition by i_class) as revenueratio
from	
	store_sales
    	,item 
    	,date_dim
where 
	ss_item_sk = i_item_sk 
  	and i_category in ('Sports', 'Music', 'Shoes')
  	and ss_sold_date_sk = d_date_sk
	and d_date between cast('2002-05-20' as date) 
				and (cast('2002-05-20' as date) + INTERVAL 30 DAY)
group by 
	i_item_id
        ,i_item_desc 
        ,i_category
        ,i_class
        ,i_current_price
order by 
	i_category
        ,i_class
        ,i_item_id
        ,i_item_desc
        ,revenueratio;

-- end query 98 in stream 0 using template query98.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Sports', 'Music', 'Shoes')), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2002-05-20' AS DATE) AND (CAST('2002-05-20' AS DATE) + INTERVAL '30' DAY))
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ss_ext_sales_price) AS itemrevenue, SUM(ss_ext_sales_price) * 100 / SUM(SUM(ss_ext_sales_price)) OVER (PARTITION BY i_class) AS revenueratio FROM store_sales JOIN filtered_items ON ss_item_sk = i_item_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio
```

---

### 187. Kimi Q31-Q99 - Q99

**Source**: Kimi Q31-Q99

#### BEFORE (Original)
```sql
-- start query 99 in stream 0 using template query99.tpl
select 
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk <= 30 ) then 1 else 0 end)  as "30 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 30) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 60) then 1 else 0 end )  as "31-60 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 60) and 
                 (cs_ship_date_sk - cs_sold_date_sk <= 90) then 1 else 0 end)  as "61-90 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk > 90) and
                 (cs_ship_date_sk - cs_sold_date_sk <= 120) then 1 else 0 end)  as "91-120 days" 
  ,sum(case when (cs_ship_date_sk - cs_sold_date_sk  > 120) then 1 else 0 end)  as ">120 days" 
from
   catalog_sales
  ,warehouse
  ,ship_mode
  ,call_center
  ,date_dim
where
    d_month_seq between 1224 and 1224 + 11
and cs_ship_date_sk   = d_date_sk
and cs_warehouse_sk   = w_warehouse_sk
and cs_ship_mode_sk   = sm_ship_mode_sk
and cs_call_center_sk = cc_call_center_sk
group by
   substr(w_warehouse_name,1,20)
  ,sm_type
  ,cc_name
order by substr(w_warehouse_name,1,20)
        ,sm_type
        ,cc_name
 LIMIT 100;

-- end query 99 in stream 0 using template query99.tpl

```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq BETWEEN 1224 AND 1224 + 11), filtered_sales AS (SELECT cs_warehouse_sk, cs_ship_mode_sk, cs_call_center_sk, cs_ship_date_sk, cs_sold_date_sk FROM catalog_sales JOIN filtered_dates ON cs_ship_date_sk = d_date_sk)
SELECT SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name, SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk <= 30) THEN 1 ELSE 0 END) AS "30 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 30) AND (cs_ship_date_sk - cs_sold_date_sk <= 60) THEN 1 ELSE 0 END) AS "31-60 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 60) AND (cs_ship_date_sk - cs_sold_date_sk <= 90) THEN 1 ELSE 0 END) AS "61-90 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 90) AND (cs_ship_date_sk - cs_sold_date_sk <= 120) THEN 1 ELSE 0 END) AS "91-120 days", SUM(CASE WHEN (cs_ship_date_sk - cs_sold_date_sk > 120) THEN 1 ELSE 0 END) AS ">120 days" FROM filtered_sales JOIN warehouse ON cs_warehouse_sk = w_warehouse_sk JOIN ship_mode ON cs_ship_mode_sk = sm_ship_mode_sk JOIN call_center ON cs_call_center_sk = cc_call_center_sk GROUP BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name ORDER BY SUBSTRING(w_warehouse_name, 1, 20), sm_type, cc_name LIMIT 100
```

---

### 188. V2_Standard_Iter1 - Q1

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_stores AS (SELECT s_store_sk FROM store WHERE s_state = 'SD'), filtered_returns AS (SELECT sr.sr_customer_sk, sr.sr_store_sk, sr.sr_fee FROM store_returns AS sr JOIN date_dim AS d ON sr.sr_returned_date_sk = d.d_date_sk JOIN filtered_stores AS fs ON sr.sr_store_sk = fs.s_store_sk WHERE d.d_year = 2000), customer_total_return AS (SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk, sr_store_sk), store_avg_return AS (SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_threshold FROM customer_total_return GROUP BY ctr_store_sk)
SELECT c.c_customer_id FROM customer_total_return AS ctr1 JOIN store_avg_return AS sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN customer AS c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_threshold ORDER BY c.c_customer_id LIMIT 100
```

---

### 189. V2_Standard_Iter1 - Q10

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4), filtered_customers_base AS (SELECT DISTINCT c.c_customer_sk, c.c_current_cdemo_sk, cd.cd_gender, cd.cd_marital_status, cd.cd_education_status, cd.cd_purchase_estimate, cd.cd_credit_rating, cd.cd_dep_count, cd.cd_dep_employed_count, cd.cd_dep_college_count FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk WHERE ca.ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County')), customers_with_store_sales AS (SELECT fc.* FROM filtered_customers_base AS fc WHERE EXISTS(SELECT 1 FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk WHERE ss.ss_customer_sk = fc.c_customer_sk)), customers_with_web_sales AS (SELECT fc.* FROM filtered_customers_base AS fc WHERE EXISTS(SELECT 1 FROM web_sales AS ws JOIN filtered_dates AS fd ON ws.ws_sold_date_sk = fd.d_date_sk WHERE ws.ws_bill_customer_sk = fc.c_customer_sk)), customers_with_catalog_sales AS (SELECT fc.* FROM filtered_customers_base AS fc WHERE EXISTS(SELECT 1 FROM catalog_sales AS cs JOIN filtered_dates AS fd ON cs.cs_sold_date_sk = fd.d_date_sk WHERE cs.cs_ship_customer_sk = fc.c_customer_sk)), union_customers AS (SELECT * FROM customers_with_store_sales WHERE c_customer_sk IN (SELECT c_customer_sk FROM customers_with_web_sales) UNION SELECT * FROM...[truncated]
```

---

### 190. V2_Standard_Iter2 - Q10

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_filter AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4), store_customers AS (SELECT DISTINCT ss_customer_sk FROM store_sales JOIN date_filter ON ss_sold_date_sk = d_date_sk), web_customers AS (SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN date_filter ON ws_sold_date_sk = d_date_sk), catalog_customers AS (SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN date_filter ON cs_sold_date_sk = d_date_sk)
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3, cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5, cd_dep_college_count, COUNT(*) AS cnt6 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd_demo_sk = c.c_current_cdemo_sk WHERE ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County') AND c.c_customer_sk IN (SELECT ss_customer_sk FROM store_customers) AND (c.c_customer_sk IN (SELECT ws_bill_customer_sk FROM web_customers) OR c.c_customer_sk IN (SELECT cs_ship_customer_sk FROM catalog_customers)) GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed...[truncated]
```

---

### 191. V2_Standard_Iter3 - Q10

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH valid_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4), store_customers AS (SELECT DISTINCT ss_customer_sk FROM store_sales JOIN valid_dates ON ss_sold_date_sk = d_date_sk), web_or_catalog_customers AS (SELECT DISTINCT ws_bill_customer_sk AS customer_sk FROM web_sales JOIN valid_dates ON ws_sold_date_sk = d_date_sk UNION SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN valid_dates ON cs_sold_date_sk = d_date_sk)
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3, cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5, cd_dep_college_count, COUNT(*) AS cnt6 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics ON cd_demo_sk = c.c_current_cdemo_sk JOIN store_customers AS sc ON c.c_customer_sk = sc.ss_customer_sk JOIN web_or_catalog_customers AS wc ON c.c_customer_sk = wc.customer_sk WHERE ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County') GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count ORDER BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count LIMIT 100
```

---

### 192. V2_Standard_Iter4 - Q10

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4) SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3, cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5, cd_dep_college_count, COUNT(*) AS cnt6 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk WHERE ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County') AND EXISTS(SELECT 1 FROM store_sales AS ss JOIN date_range AS dr ON ss.ss_sold_date_sk = dr.d_date_sk WHERE ss.ss_customer_sk = c.c_customer_sk) AND EXISTS(SELECT 1 FROM web_sales AS ws JOIN date_range AS dr ON ws.ws_sold_date_sk = dr.d_date_sk WHERE ws.ws_bill_customer_sk = c.c_customer_sk) GROUP BY cd_gender, cd_marital_status, cd_education_status, cd_purchase_estimate, cd_credit_rating, cd_dep_count, cd_dep_employed_count, cd_dep_college_count UNION ALL SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, COUNT(*) AS cnt2, cd_credit_rating, COUNT(*) AS cnt3, cd_dep_count, COUNT(*) AS cnt4, cd_dep_employed_count, COUNT(*) AS cnt5, cd_dep_college_count, COUNT(*) AS cnt6 FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk JOIN customer_demographics AS cd ON cd.cd_demo_sk ...[truncated]
```

---

### 193. V2_Standard_Iter5 - Q10

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_moy BETWEEN 1 AND 4), filtered_customers AS (SELECT DISTINCT c.c_customer_sk, c.c_current_cdemo_sk, c.c_current_addr_sk FROM customer AS c JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk WHERE ca.ca_county IN ('Storey County', 'Marquette County', 'Warren County', 'Cochran County', 'Kandiyohi County')), store_customers AS (SELECT DISTINCT ss_customer_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk WHERE ss_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)), web_customers AS (SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk WHERE ws_bill_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)), catalog_customers AS (SELECT DISTINCT cs_ship_customer_sk FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk WHERE cs_ship_customer_sk IN (SELECT c_customer_sk FROM filtered_customers)), qualified_customers AS (SELECT DISTINCT fc.c_customer_sk, fc.c_current_cdemo_sk FROM filtered_customers AS fc WHERE EXISTS(SELECT 1 FROM store_customers AS sc WHERE sc.ss_customer_sk = fc.c_customer_sk) AND (EXISTS(SELECT 1 FROM web_customers AS wc WHERE wc.ws_bill_customer_sk = fc.c_customer_sk) OR EXISTS(SELECT 1 FROM catalog_customers AS cc WHERE cc.cs_ship_customer_sk = fc.c_customer_sk)))
SELECT cd_gender, cd_marital_status, cd_education_status, COUNT(*) AS cnt1, cd_purchase_estimate, CO...[truncated]
```

---

### 194. V2_Standard_Iter1 - Q11

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates_2001 AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_dates_2002 AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(ws_ext_list_price - ws_ext_discount_amt) AS year_total, 'w' AS sale_type FROM customer, web_sales, date_dim WHERE c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year), store_sales_2001 AS (SELECT ss_customer_sk, SUM(ss_ext_list_price - ss_ext_discount_amt) AS year_total FROM s...[truncated]
```

---

### 195. V2_Standard_Iter1 - Q12

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), filtered_sales AS (SELECT ws_item_sk, ws_ext_sales_price FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), aggregated_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM filtered_sales JOIN filtered_items ON ws_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue, itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio FROM aggregated_sales ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 196. V2_Standard_Iter2 - Q12

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), aggregated_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_items ON ws_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price) SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue, itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio FROM aggregated_sales ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 197. V2_Standard_Iter3 - Q12

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), filtered_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, ws_ext_sales_price FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk JOIN filtered_items ON ws_item_sk = i_item_sk), aggregated_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM filtered_sales GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue, itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio FROM aggregated_sales ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 198. V2_Standard_Iter4 - Q12

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), filtered_sales AS (SELECT ws_item_sk, ws_ext_sales_price FROM web_sales AS ws JOIN filtered_dates AS fd ON ws.ws_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ws.ws_item_sk = fi.i_item_sk), aggregated_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM filtered_sales JOIN filtered_items ON ws_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue, itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio FROM aggregated_sales ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 199. V2_Standard_Iter5 - Q12

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)), filtered_items AS (SELECT i_item_sk, i_item_id, i_item_desc, i_category, i_class, i_current_price FROM item WHERE i_category IN ('Books', 'Sports', 'Men')), filtered_sales_early AS (SELECT ws_item_sk, ws_ext_sales_price FROM web_sales WHERE ws_sold_date_sk IN (SELECT d_date_sk FROM filtered_dates)), aggregated_sales AS (SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, SUM(ws_ext_sales_price) AS itemrevenue FROM filtered_sales_early JOIN filtered_items ON ws_item_sk = i_item_sk GROUP BY i_item_id, i_item_desc, i_category, i_class, i_current_price), filtered_sales AS (SELECT ws_item_sk, ws_ext_sales_price FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, i_category, i_class, i_current_price, itemrevenue, itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio FROM aggregated_sales ORDER BY i_category, i_class, i_item_id, i_item_desc, revenueratio LIMIT 100
```

---

### 200. V2_Standard_Iter1 - Q13

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
-- start query 13 in stream 0 using template query13.tpl
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,store
     ,customer_demographics
     ,household_demographics
     ,customer_address
     ,date_dim
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk and d_year = 2001
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Unknown'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'College'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = '4 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('SD', 'KS', 'MI')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('MO', 'ND', 'CO')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_country = 'United States'
  and ca_state in ('NH', 'OH', 'TX')
  and ss_net_profit between 50 and 250  
     ))
;

-- ...[truncated]
```

---

### 201. V2_Standard_Iter2 - Q13

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_filtered_sales AS (SELECT ss_sold_date_sk, ss_store_sk, ss_hdemo_sk, ss_cdemo_sk, ss_addr_sk, ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost, ss_sales_price, ss_net_profit FROM store_sales WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM date_dim WHERE d_year = 2001))
SELECT AVG(ss_quantity) AS avg_ss_quantity, AVG(ss_ext_sales_price) AS avg_ss_ext_sales_price, AVG(ss_ext_wholesale_cost) AS avg_ss_ext_wholesale_cost, SUM(ss_ext_wholesale_cost) AS sum_ss_ext_wholesale_cost FROM (SELECT s.ss_quantity, s.ss_ext_sales_price, s.ss_ext_wholesale_cost FROM date_filtered_sales AS s JOIN store AS st ON s.ss_store_sk = st.s_store_sk JOIN household_demographics AS hd ON s.ss_hdemo_sk = hd.hd_demo_sk AND hd.hd_dep_count = 3 JOIN customer_demographics AS cd ON s.ss_cdemo_sk = cd.cd_demo_sk AND cd.cd_marital_status = 'D' AND cd.cd_education_status = 'Unknown' JOIN customer_address AS ca ON s.ss_addr_sk = ca.ca_address_sk AND ca.ca_country = 'United States' AND ca.ca_state IN ('SD', 'KS', 'MI') WHERE s.ss_sales_price BETWEEN 100.00 AND 150.00 AND s.ss_net_profit BETWEEN 100 AND 200 UNION ALL SELECT s.ss_quantity, s.ss_ext_sales_price, s.ss_ext_wholesale_cost FROM date_filtered_sales AS s JOIN store AS st ON s.ss_store_sk = st.s_store_sk JOIN household_demographics AS hd ON s.ss_hdemo_sk = hd.hd_demo_sk AND hd.hd_dep_count = 3 JOIN customer_demographics AS cd ON s.ss_cdemo_sk = cd.cd_demo_sk AND cd.cd_marital_status = 'D' AND cd.cd_education_status = 'Unknown' JOIN customer_...[truncated]
```

---

### 202. V2_Standard_Iter3 - Q13

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH all_qualified_sales AS (SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN store ON s_store_sk = ss_store_sk JOIN date_dim ON ss_sold_date_sk = d_date_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk JOIN customer_address ON ss_addr_sk = ca_address_sk WHERE d_year = 2001 AND cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('SD', 'KS', 'MI') AND ss_net_profit BETWEEN 100 AND 200 UNION ALL SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN store ON s_store_sk = ss_store_sk JOIN date_dim ON ss_sold_date_sk = d_date_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk JOIN customer_address ON ss_addr_sk = ca_address_sk WHERE d_year = 2001 AND cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('MO', 'ND', 'CO') AND ss_net_profit BETWEEN 150 AND 300 UNION ALL SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost FROM store_sales JOIN store ON s_store_sk = ss_store_sk JOIN date_dim ON ss_sold_date_sk = d_date_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk JOIN customer...[truncated]
```

---

### 203. V2_Standard_Iter4 - Q13

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH branch_1 AS (SELECT SUM(ss_quantity) AS sum_qty_1, COUNT(*) AS cnt_1, SUM(ss_ext_sales_price) AS sum_sales_1, SUM(ss_ext_wholesale_cost) AS sum_wholesale_1 FROM store_sales JOIN store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk JOIN customer_address ON ss_addr_sk = ca_address_sk JOIN date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2001 AND cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('SD', 'KS', 'MI') AND ss_net_profit BETWEEN 100 AND 200), branch_2 AS (SELECT SUM(ss_quantity) AS sum_qty_2, COUNT(*) AS cnt_2, SUM(ss_ext_sales_price) AS sum_sales_2, SUM(ss_ext_wholesale_cost) AS sum_wholesale_2 FROM store_sales JOIN store ON s_store_sk = ss_store_sk JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk JOIN household_demographics ON ss_hdemo_sk = hd_demo_sk JOIN customer_address ON ss_addr_sk = ca_address_sk JOIN date_dim ON ss_sold_date_sk = d_date_sk WHERE d_year = 2001 AND cd_marital_status = 'D' AND cd_education_status = 'Unknown' AND ss_sales_price BETWEEN 100.00 AND 150.00 AND hd_dep_count = 3 AND ca_country = 'United States' AND ca_state IN ('MO', 'ND', 'CO') AND ss_net_profit BETWEEN 150 AND 300), branch_3 AS (SELECT SUM(ss_quantity) AS sum_qty_3, COUNT(*) AS cnt_3, SUM(ss_ext_sales_price) AS sum_sales_3, SUM(ss_ext_wholesale_cost) AS...[truncated]
```

---

### 204. V2_Standard_Iter5 - Q13

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_date AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE (cd_marital_status = 'D' AND cd_education_status = 'Unknown') OR (cd_marital_status = 'S' AND cd_education_status = 'College') OR (cd_marital_status = 'M' AND cd_education_status = '4 yr Degree')), filtered_household AS (SELECT hd_demo_sk FROM household_demographics WHERE hd_dep_count IN (1, 3)), filtered_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_country = 'United States' AND (ca_state IN ('SD', 'KS', 'MI') OR ca_state IN ('MO', 'ND', 'CO') OR ca_state IN ('NH', 'OH', 'TX')))
SELECT AVG(ss_quantity), AVG(ss_ext_sales_price), AVG(ss_ext_wholesale_cost), SUM(ss_ext_wholesale_cost) FROM store_sales AS ss JOIN store AS s ON s.s_store_sk = ss.ss_store_sk JOIN filtered_date AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_demographics AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN filtered_household AS hd ON ss.ss_hdemo_sk = hd.hd_demo_sk JOIN filtered_address AS ca ON ss.ss_addr_sk = ca.ca_address_sk WHERE ((cd.cd_marital_status = 'D' AND cd.cd_education_status = 'Unknown' AND ss.ss_sales_price BETWEEN 100.00 AND 150.00 AND hd.hd_dep_count = 3) OR (cd.cd_marital_status = 'S' AND cd.cd_education_status = 'College' AND ss.ss_sales_price BETWEEN 50.00 AND 100.00 AND hd.hd_dep_count = 1) OR (cd.cd_marital_status = 'M' AND cd.cd_education_status = '4 yr Degree' AND ss.ss_sales_price BETWEEN 150.00 AND 200.00...[truncated]
```

---

### 205. V2_Standard_Iter1 - Q14

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH dates_3years AS (SELECT d_date_sk FROM date_dim WHERE d_year BETWEEN 2000 AND 2000 + 2), date_nov2002 AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 + 2 AND d_moy = 11), cross_items AS (SELECT i_item_sk AS ss_item_sk FROM item, (SELECT iss.i_brand_id AS brand_id, iss.i_class_id AS class_id, iss.i_category_id AS category_id FROM store_sales, item AS iss, dates_3years AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk INTERSECT SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id FROM catalog_sales, item AS ics, dates_3years AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk INTERSECT SELECT iws.i_brand_id, iws.i_class_id, iws.i_category_id FROM web_sales, item AS iws, dates_3years AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk) WHERE i_brand_id = brand_id AND i_class_id = class_id AND i_category_id = category_id), avg_sales AS (SELECT AVG(quantity * list_price) AS average_sales FROM (SELECT ss_quantity AS quantity, ss_list_price AS list_price FROM store_sales, dates_3years WHERE ss_sold_date_sk = d_date_sk UNION ALL SELECT cs_quantity AS quantity, cs_list_price AS list_price FROM catalog_sales, dates_3years WHERE cs_sold_date_sk = d_date_sk UNION ALL SELECT ws_quantity AS quantity, ws_list_price AS list_price FROM web_sales, dates_3years WHERE ws_sold_date_sk = d_date_sk) AS x)
SELECT channel, i_brand_id, i_class_id, i_category_id, SUM(sales), SUM(number_sales) FROM (SELECT 'store' AS cha...[truncated]
```

---

### 206. V2_Standard_Iter2 - Q14

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH cross_items_flat AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2002 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2002 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2002 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id)), cross_items AS (SELECT i_item_sk AS ss_item_sk FROM item, (SELECT iss.i_brand_id AS brand_id, iss.i_class_id AS class_id, iss.i_category_id AS category_id FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT iws.i_brand_id, iws.i_class_id, iws....[truncated]
```

---

### 207. V2_Standard_Iter3 - Q14

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_date_dim AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2002 AND d_moy = 11), cross_items_flat AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2002 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2002 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2002 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id)), avg_sales AS (SELECT AVG(quantity * list_price) AS average_sales FROM (SELECT ss_quantity AS quantity, ss_list_price AS list_price FROM store_sales, date_dim WHERE ss_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT cs_quantity AS quantity, cs_list_price AS list_price FROM catalog_sales, date_dim WHERE cs_sold_date_sk = d_date_sk AND d_year BETWEEN 2000 AND 2000 + 2 UNION ALL SELECT ws_quantity AS quantity, ws_list_price AS list_price FROM web_sales, date_dim W...[truncated]
```

---

### 208. V2_Standard_Iter4 - Q14

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH cross_items_flat_materialized AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2002 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2002 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_id = i.i_class_id AND ics.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2002 AND iws.i_brand_id = i.i_brand_id AND iws.i_class_id = i.i_class_id AND iws.i_category_id = i.i_category_id)), cross_items_flat AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE EXISTS(SELECT 1 FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2002 AND iss.i_brand_id = i.i_brand_id AND iss.i_class_id = i.i_class_id AND iss.i_category_id = i.i_category_id) AND EXISTS(SELECT 1 FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2002 AND ics.i_brand_id = i.i_brand_id AND ics.i_class_...[truncated]
```

---

### 209. V2_Standard_Iter5 - Q14

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2000 + 2 AND d_moy = 11), store_items AS (SELECT DISTINCT ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk), catalog_items AS (SELECT DISTINCT cs_item_sk FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk), web_items AS (SELECT DISTINCT ws_item_sk FROM web_sales JOIN filtered_dates ON ws_sold_date_sk = d_date_sk), cross_items_flat AS (SELECT i.i_item_sk AS ss_item_sk FROM item AS i WHERE i.i_item_sk IN (SELECT ss_item_sk FROM store_items) AND i.i_item_sk IN (SELECT cs_item_sk FROM catalog_items) AND i.i_item_sk IN (SELECT ws_item_sk FROM web_items)), cross_items AS (SELECT i_item_sk AS ss_item_sk FROM item, (SELECT iss.i_brand_id AS brand_id, iss.i_class_id AS class_id, iss.i_category_id AS category_id FROM store_sales, item AS iss, date_dim AS d1 WHERE ss_item_sk = iss.i_item_sk AND ss_sold_date_sk = d1.d_date_sk AND d1.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id FROM catalog_sales, item AS ics, date_dim AS d2 WHERE cs_item_sk = ics.i_item_sk AND cs_sold_date_sk = d2.d_date_sk AND d2.d_year BETWEEN 2000 AND 2000 + 2 INTERSECT SELECT iws.i_brand_id, iws.i_class_id, iws.i_category_id FROM web_sales, item AS iws, date_dim AS d3 WHERE ws_item_sk = iws.i_item_sk AND ws_sold_date_sk = d3.d_date_sk AND d3.d_year BETWEEN 2000 AND 2000 + 2) WHERE i_brand_id = brand_id AND i_class_id = class_id AND i_category_id ...[truncated]
```

---

### 210. V2_Standard_Iter1 - Q15

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 1 AND d_year = 2001), filtered_sales AS (SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE SUBSTRING(ca_zip, 1, 5) IN ('85669', '86197', '88274', '83405', '86475', '85392', '85460', '80348', '81792') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE ca_state IN ('CA', 'WA', 'GA') UNION ALL SELECT cs_sales_price, ca_zip FROM catalog_sales JOIN filtered_dates ON cs_sold_date_sk = d_date_sk JOIN customer ON cs_bill_customer_sk = c_customer_sk JOIN customer_address ON c_current_addr_sk = ca_address_sk WHERE cs_sales_price > 500)
SELECT ca_zip, SUM(cs_sales_price) FROM filtered_sales GROUP BY ca_zip ORDER BY ca_zip LIMIT 100
```

---

### 211. V2_Standard_Iter1 - Q16

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_address AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), filtered_call_center AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), multi_warehouse_orders AS (SELECT DISTINCT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), orders_with_returns AS (SELECT DISTINCT cr_order_number FROM catalog_returns), filtered_cs1 AS (SELECT cs1.* FROM catalog_sales AS cs1 JOIN filtered_dates AS fd ON cs1.cs_ship_date_sk = fd.d_date_sk JOIN filtered_address AS fa ON cs1.cs_ship_addr_sk = fa.ca_address_sk JOIN filtered_call_center AS fcc ON cs1.cs_call_center_sk = fcc.cc_call_center_sk)
SELECT COUNT(DISTINCT cs_order_number) AS "order count", SUM(cs_ext_ship_cost) AS "total shipping cost", SUM(cs_net_profit) AS "total net profit" FROM filtered_cs1 AS fcs1 JOIN multi_warehouse_orders AS mwo ON fcs1.cs_order_number = mwo.cs_order_number LEFT JOIN orders_with_returns AS owr ON fcs1.cs_order_number = owr.cr_order_number WHERE owr.cr_order_number IS NULL ORDER BY COUNT(DISTINCT cs_order_number) LIMIT 100
```

---

### 212. V2_Standard_Iter2 - Q16

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), multi_warehouse_orders AS (SELECT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), non_returned_orders AS (SELECT cr_order_number FROM catalog_returns)
SELECT COUNT(DISTINCT cs1.cs_order_number) AS "order count", SUM(cs1.cs_ext_ship_cost) AS "total shipping cost", SUM(cs1.cs_net_profit) AS "total net profit" FROM catalog_sales AS cs1 JOIN date_range AS dr ON cs1.cs_ship_date_sk = dr.d_date_sk JOIN customer_address ON cs1.cs_ship_addr_sk = ca_address_sk JOIN call_center ON cs1.cs_call_center_sk = cc_call_center_sk JOIN multi_warehouse_orders AS mwo ON cs1.cs_order_number = mwo.cs_order_number LEFT JOIN non_returned_orders AS nro ON cs1.cs_order_number = nro.cr_order_number WHERE ca_state = 'WV' AND cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County') AND nro.cr_order_number IS NULL ORDER BY COUNT(DISTINCT cs1.cs_order_number) LIMIT 100
```

---

### 213. V2_Standard_Iter3 - Q16

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), filtered_call_centers AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), multi_warehouse_orders AS (SELECT DISTINCT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), returned_orders AS (SELECT DISTINCT cr_order_number FROM catalog_returns)
SELECT COUNT(DISTINCT cs1.cs_order_number) AS "order count", SUM(cs1.cs_ext_ship_cost) AS "total shipping cost", SUM(cs1.cs_net_profit) AS "total net profit" FROM catalog_sales AS cs1 JOIN filtered_dates ON cs1.cs_ship_date_sk = d_date_sk JOIN filtered_addresses ON cs1.cs_ship_addr_sk = ca_address_sk JOIN filtered_call_centers ON cs1.cs_call_center_sk = cc_call_center_sk JOIN multi_warehouse_orders AS mwo ON cs1.cs_order_number = mwo.cs_order_number LEFT JOIN returned_orders AS ro ON cs1.cs_order_number = ro.cr_order_number WHERE ro.cr_order_number IS NULL ORDER BY COUNT(DISTINCT cs1.cs_order_number) LIMIT 100
```

---

### 214. V2_Standard_Iter4 - Q16

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), filtered_call_centers AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), multi_warehouse_orders AS (SELECT cs_order_number FROM catalog_sales GROUP BY cs_order_number HAVING COUNT(DISTINCT cs_warehouse_sk) > 1), returned_orders AS (SELECT cr_order_number FROM catalog_returns)
SELECT COUNT(DISTINCT cs1.cs_order_number) AS "order count", SUM(cs1.cs_ext_ship_cost) AS "total shipping cost", SUM(cs1.cs_net_profit) AS "total net profit" FROM catalog_sales AS cs1 JOIN filtered_dates AS fd ON cs1.cs_ship_date_sk = fd.d_date_sk JOIN filtered_addresses AS fa ON cs1.cs_ship_addr_sk = fa.ca_address_sk JOIN filtered_call_centers AS fc ON cs1.cs_call_center_sk = fc.cc_call_center_sk JOIN multi_warehouse_orders AS mwo ON cs1.cs_order_number = mwo.cs_order_number LEFT JOIN returned_orders AS ro ON cs1.cs_order_number = ro.cr_order_number WHERE ro.cr_order_number IS NULL ORDER BY COUNT(DISTINCT cs1.cs_order_number) LIMIT 100
```

---

### 215. V2_Standard_Iter5 - Q16

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN '2002-4-01' AND (CAST('2002-4-01' AS DATE) + INTERVAL '60' DAY)), filtered_addresses AS (SELECT ca_address_sk FROM customer_address WHERE ca_state = 'WV'), filtered_call_centers AS (SELECT cc_call_center_sk FROM call_center WHERE cc_county IN ('Ziebach County', 'Luce County', 'Richland County', 'Daviess County', 'Barrow County')), filtered_cs1 AS (SELECT cs1.cs_order_number, cs1.cs_ext_ship_cost, cs1.cs_net_profit FROM catalog_sales AS cs1 JOIN filtered_dates ON cs1.cs_ship_date_sk = filtered_dates.d_date_sk JOIN filtered_addresses ON cs1.cs_ship_addr_sk = filtered_addresses.ca_address_sk JOIN filtered_call_centers ON cs1.cs_call_center_sk = filtered_call_centers.cc_call_center_sk), multi_warehouse_orders AS (SELECT cs2.cs_order_number FROM catalog_sales AS cs2 JOIN filtered_cs1 AS fcs1 ON cs2.cs_order_number = fcs1.cs_order_number WHERE EXISTS(SELECT 1 FROM filtered_cs1 AS fcs2 WHERE fcs2.cs_order_number = fcs1.cs_order_number AND fcs2.rowid <> fcs1.rowid)), non_returned_orders AS (SELECT fcs1.cs_order_number FROM filtered_cs1 AS fcs1 WHERE NOT EXISTS(SELECT 1 FROM catalog_returns AS cr WHERE cr.cr_order_number = fcs1.cs_order_number))
SELECT COUNT(DISTINCT fcs1.cs_order_number) AS "order count", SUM(fcs1.cs_ext_ship_cost) AS "total shipping cost", SUM(fcs1.cs_net_profit) AS "total net profit" FROM filtered_cs1 AS fcs1 WHERE EXISTS(SELECT 1 FROM catalog_sales AS cs2 WHERE fcs1.cs_order_number = cs2.c...[truncated]
```

---

### 216. V2_Standard_Iter1 - Q17

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_d1 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name = '2001Q1'), filtered_d2 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), filtered_d3 AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), filtered_store_sales AS (SELECT ss_item_sk, ss_customer_sk, ss_ticket_number, ss_store_sk, ss_quantity FROM store_sales JOIN filtered_d1 ON ss_sold_date_sk = d_date_sk), filtered_store_returns AS (SELECT sr_item_sk, sr_customer_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN filtered_d2 ON sr_returned_date_sk = d_date_sk), filtered_catalog_sales AS (SELECT cs_item_sk, cs_bill_customer_sk, cs_quantity FROM catalog_sales JOIN filtered_d3 ON cs_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, s_state, COUNT(ss.ss_quantity) AS store_sales_quantitycount, AVG(ss.ss_quantity) AS store_sales_quantityave, STDDEV_SAMP(ss.ss_quantity) AS store_sales_quantitystdev, STDDEV_SAMP(ss.ss_quantity) / AVG(ss.ss_quantity) AS store_sales_quantitycov, COUNT(sr.sr_return_quantity) AS store_returns_quantitycount, AVG(sr.sr_return_quantity) AS store_returns_quantityave, STDDEV_SAMP(sr.sr_return_quantity) AS store_returns_quantitystdev, STDDEV_SAMP(sr.sr_return_quantity) / AVG(sr.sr_return_quantity) AS store_returns_quantitycov, COUNT(cs.cs_quantity) AS catalog_sales_quantitycount, AVG(cs.cs_quantity) AS catalog_sales_quantityave, STDDEV_SAMP(cs.cs_quantity) AS catalog_sales_quantitystdev...[truncated]
```

---

### 217. V2_Standard_Iter2 - Q17

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH d1_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name = '2001Q1'), d2_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), d3_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), filtered_store_sales AS (SELECT ss_customer_sk, ss_item_sk, ss_ticket_number, ss_store_sk, ss_quantity FROM store_sales JOIN d1_filtered ON ss_sold_date_sk = d_date_sk), filtered_store_returns AS (SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_return_quantity, sr_returned_date_sk FROM store_returns JOIN d2_filtered ON sr_returned_date_sk = d_date_sk), filtered_catalog_sales AS (SELECT cs_bill_customer_sk, cs_item_sk, cs_quantity FROM catalog_sales JOIN d3_filtered ON cs_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, s_state, COUNT(ss_quantity) AS store_sales_quantitycount, AVG(ss_quantity) AS store_sales_quantityave, STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev, STDDEV_SAMP(ss_quantity) / AVG(ss_quantity) AS store_sales_quantitycov, COUNT(sr_return_quantity) AS store_returns_quantitycount, AVG(sr_return_quantity) AS store_returns_quantityave, STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev, STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov, COUNT(cs_quantity) AS catalog_sales_quantitycount, AVG(cs_quantity) AS catalog_sales_quantityave, STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev, STDDEV_SAMP(cs_q...[truncated]
```

---

### 218. V2_Standard_Iter3 - Q17

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH d1_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name = '2001Q1'), filtered_store_sales AS (SELECT ss_customer_sk, ss_item_sk, ss_ticket_number, ss_store_sk, ss_quantity FROM store_sales JOIN d1_filtered ON ss_sold_date_sk = d_date_sk), d2_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), filtered_store_returns AS (SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_return_quantity FROM store_returns JOIN d2_filtered ON sr_returned_date_sk = d_date_sk), d3_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_quarter_name IN ('2001Q1', '2001Q2', '2001Q3')), filtered_catalog_sales AS (SELECT cs_bill_customer_sk, cs_item_sk, cs_quantity FROM catalog_sales JOIN d3_filtered ON cs_sold_date_sk = d_date_sk)
SELECT i_item_id, i_item_desc, s_state, COUNT(ss_quantity) AS store_sales_quantitycount, AVG(ss_quantity) AS store_sales_quantityave, STDDEV_SAMP(ss_quantity) AS store_sales_quantitystdev, STDDEV_SAMP(ss_quantity) / AVG(ss_quantity) AS store_sales_quantitycov, COUNT(sr_return_quantity) AS store_returns_quantitycount, AVG(sr_return_quantity) AS store_returns_quantityave, STDDEV_SAMP(sr_return_quantity) AS store_returns_quantitystdev, STDDEV_SAMP(sr_return_quantity) / AVG(sr_return_quantity) AS store_returns_quantitycov, COUNT(cs_quantity) AS catalog_sales_quantitycount, AVG(cs_quantity) AS catalog_sales_quantityave, STDDEV_SAMP(cs_quantity) AS catalog_sales_quantitystdev, STDDEV_SAMP(cs_quantity) / AVG(cs_qua...[truncated]
```

---

### 219. V2_Standard_Iter1 - Q2

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_week_seq, d_year FROM date_dim WHERE d_year IN (1998, 1999)), wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs AS (SELECT d.d_week_seq, d.d_year, SUM(CASE WHEN (d.d_day_name = 'Sunday') THEN w.sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d.d_day_name = 'Monday') THEN w.sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d.d_day_name = 'Tuesday') THEN w.sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d.d_day_name = 'Wednesday') THEN w.sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d.d_day_name = 'Thursday') THEN w.sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d.d_day_name = 'Friday') THEN w.sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d.d_day_name = 'Saturday') THEN w.sales_price ELSE NULL END) AS sat_sales FROM wscs AS w JOIN filtered_dates AS d ON d.d_date_sk = w.sold_date_sk GROUP BY d.d_week_seq, d.d_year)
SELECT y.d_week_seq1, ROUND(y.sun_sales1 / NULLIF(z.sun_sales2, 0), 2), ROUND(y.mon_sales1 / NULLIF(z.mon_sales2, 0), 2), ROUND(y.tue_sales1 / NULLIF(z.tue_sales2, 0), 2), ROUND(y.wed_sales1 / NULLIF(z.wed_sales2, 0), 2), ROUND(y.thu_sales1 / NULLIF(z.thu_sales2, 0), 2), ROUND(y.fri_sales1 / NULLIF(z.fri_sales2, 0), 2), ROUND(y.sat_sales1 / NULLIF(z.sat_sales2, 0), 2) FROM (SELE...[truncated]
```

---

### 220. V2_Standard_Iter2 - Q2

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs_both_years AS (SELECT d.d_week_seq, d.d_year, SUM(CASE WHEN (d.d_day_name = 'Sunday') THEN w.sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d.d_day_name = 'Monday') THEN w.sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d.d_day_name = 'Tuesday') THEN w.sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d.d_day_name = 'Wednesday') THEN w.sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d.d_day_name = 'Thursday') THEN w.sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d.d_day_name = 'Friday') THEN w.sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d.d_day_name = 'Saturday') THEN w.sales_price ELSE NULL END) AS sat_sales FROM wscs AS w, date_dim AS d WHERE d.d_date_sk = w.sold_date_sk AND d.d_year IN (1998, 1999) GROUP BY d.d_week_seq, d.d_year), wswscs AS (SELECT d_week_seq, SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS thu...[truncated]
```

---

### 221. V2_Standard_Iter3 - Q2

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_week_seq, d_day_name FROM date_dim WHERE d_year IN (1998, 1999)), wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs AS (SELECT d.d_week_seq, d.d_year, SUM(CASE WHEN (d.d_day_name = 'Sunday') THEN w.sales_price END) AS sun_sales, SUM(CASE WHEN (d.d_day_name = 'Monday') THEN w.sales_price END) AS mon_sales, SUM(CASE WHEN (d.d_day_name = 'Tuesday') THEN w.sales_price END) AS tue_sales, SUM(CASE WHEN (d.d_day_name = 'Wednesday') THEN w.sales_price END) AS wed_sales, SUM(CASE WHEN (d.d_day_name = 'Thursday') THEN w.sales_price END) AS thu_sales, SUM(CASE WHEN (d.d_day_name = 'Friday') THEN w.sales_price END) AS fri_sales, SUM(CASE WHEN (d.d_day_name = 'Saturday') THEN w.sales_price END) AS sat_sales FROM wscs AS w JOIN filtered_dates AS d ON d.d_date_sk = w.sold_date_sk GROUP BY d.d_week_seq, d.d_year)
SELECT y.d_week_seq AS d_week_seq1, ROUND(y.sun_sales / NULLIF(z.sun_sales, 0), 2), ROUND(y.mon_sales / NULLIF(z.mon_sales, 0), 2), ROUND(y.tue_sales / NULLIF(z.tue_sales, 0), 2), ROUND(y.wed_sales / NULLIF(z.wed_sales, 0), 2), ROUND(y.thu_sales / NULLIF(z.thu_sales, 0), 2), ROUND(y.fri_sales / NULLIF(z.fri_sales, 0), 2), ROUND(y.sat_sales / NULLIF(z.sat_sales, 0), 2) FROM (SELECT * FROM wswscs WHERE d_year = 1998) AS y JOIN (SELECT * FROM wsw...[truncated]
```

---

### 222. V2_Standard_Iter4 - Q2

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH wscs AS (SELECT sold_date_sk, sales_price FROM (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales)), wswscs_both_years AS (SELECT d.d_week_seq, d.d_year, SUM(CASE WHEN (d.d_day_name = 'Sunday') THEN w.sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d.d_day_name = 'Monday') THEN w.sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d.d_day_name = 'Tuesday') THEN w.sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d.d_day_name = 'Wednesday') THEN w.sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d.d_day_name = 'Thursday') THEN w.sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d.d_day_name = 'Friday') THEN w.sales_price ELSE NULL END) AS fri_sales, SUM(CASE WHEN (d.d_day_name = 'Saturday') THEN w.sales_price ELSE NULL END) AS sat_sales FROM wscs AS w JOIN date_dim AS d ON d.d_date_sk = w.sold_date_sk WHERE d.d_year IN (1998, 1999) GROUP BY d.d_week_seq, d.d_year), wswscs AS (SELECT d_week_seq, SUM(CASE WHEN (d_day_name = 'Sunday') THEN sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d_day_name = 'Monday') THEN sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d_day_name = 'Tuesday') THEN sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d_day_name = 'Wednesday') THEN sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d_day_name = 'Thursday') THEN sales_price ELSE NULL END) AS ...[truncated]
```

---

### 223. V2_Standard_Iter5 - Q2

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH wscs_1998 AS (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales WHERE EXISTS(SELECT 1 FROM date_dim WHERE d_date_sk = ws_sold_date_sk AND d_year = 1998) UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales WHERE EXISTS(SELECT 1 FROM date_dim WHERE d_date_sk = cs_sold_date_sk AND d_year = 1998)), wscs_1999 AS (SELECT ws_sold_date_sk AS sold_date_sk, ws_ext_sales_price AS sales_price FROM web_sales WHERE EXISTS(SELECT 1 FROM date_dim WHERE d_date_sk = ws_sold_date_sk AND d_year = 1999) UNION ALL SELECT cs_sold_date_sk AS sold_date_sk, cs_ext_sales_price AS sales_price FROM catalog_sales WHERE EXISTS(SELECT 1 FROM date_dim WHERE d_date_sk = cs_sold_date_sk AND d_year = 1999)), dates_1998 AS (SELECT d_date_sk, d_week_seq FROM date_dim WHERE d_year = 1998), dates_1999 AS (SELECT d_date_sk, d_week_seq FROM date_dim WHERE d_year = 1999), wswscs_1998 AS (SELECT d.d_week_seq, SUM(CASE WHEN (d.d_day_name = 'Sunday') THEN w.sales_price ELSE NULL END) AS sun_sales, SUM(CASE WHEN (d.d_day_name = 'Monday') THEN w.sales_price ELSE NULL END) AS mon_sales, SUM(CASE WHEN (d.d_day_name = 'Tuesday') THEN w.sales_price ELSE NULL END) AS tue_sales, SUM(CASE WHEN (d.d_day_name = 'Wednesday') THEN w.sales_price ELSE NULL END) AS wed_sales, SUM(CASE WHEN (d.d_day_name = 'Thursday') THEN w.sales_price ELSE NULL END) AS thu_sales, SUM(CASE WHEN (d.d_day_name = 'Friday') THEN w.sales_price ELSE NULL END) AS fri...[truncated]
```

---

### 224. V2_Standard_Iter1 - Q3

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816)
SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, SUM(ss_sales_price) AS sum_agg FROM store_sales JOIN filtered_dates AS dt ON store_sales.ss_sold_date_sk = dt.d_date_sk JOIN filtered_items AS item ON store_sales.ss_item_sk = item.i_item_sk GROUP BY dt.d_year, item.i_brand, item.i_brand_id ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100
```

---

### 225. V2_Standard_Iter2 - Q3

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816), filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11)
SELECT dt.d_year, fi.i_brand_id AS brand_id, fi.i_brand AS brand, SUM(ss_sales_price) AS sum_agg FROM store_sales AS ss JOIN filtered_dates AS dt ON ss.ss_sold_date_sk = dt.d_date_sk JOIN filtered_items AS fi ON ss.ss_item_sk = fi.i_item_sk GROUP BY dt.d_year, fi.i_brand, fi.i_brand_id ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100
```

---

### 226. V2_Standard_Iter3 - Q3

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816)
SELECT dt.d_year, item.i_brand_id AS brand_id, item.i_brand AS brand, SUM(ss_sales_price) AS sum_agg FROM store_sales JOIN filtered_dates AS dt ON store_sales.ss_sold_date_sk = dt.d_date_sk JOIN filtered_items AS item ON store_sales.ss_item_sk = item.i_item_sk GROUP BY dt.d_year, item.i_brand, item.i_brand_id ORDER BY dt.d_year, sum_agg DESC, brand_id LIMIT 100
```

---

### 227. V2_Standard_Iter4 - Q3

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816), filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11)
SELECT fd.d_year, fi.i_brand_id AS brand_id, fi.i_brand AS brand, SUM(ss_sales_price) AS sum_agg FROM store_sales AS ss JOIN filtered_items AS fi ON ss.ss_item_sk = fi.i_item_sk JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk GROUP BY fd.d_year, fi.i_brand, fi.i_brand_id ORDER BY fd.d_year, sum_agg DESC, fi.i_brand_id LIMIT 100
```

---

### 228. V2_Standard_Iter5 - Q3

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11), filtered_items AS (SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816), filtered_sales AS (SELECT ss.ss_sales_price, fd.d_year, fi.i_brand_id, fi.i_brand FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_items AS fi ON ss.ss_item_sk = fi.i_item_sk)
SELECT d_year, i_brand_id AS brand_id, i_brand AS brand, SUM(ss_sales_price) AS sum_agg FROM filtered_sales GROUP BY d_year, i_brand, i_brand_id ORDER BY d_year, sum_agg DESC, i_brand_id LIMIT 100
```

---

### 229. V2_Standard_Iter1 - Q4

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000)), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer, catalog_sales, date_dim WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_...[truncated]
```

---

### 230. V2_Standard_Iter2 - Q4

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year IN (1999, 2000)), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer, store_sales, date_dim WHERE c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer, catalog_sales, date_dim WHERE c_customer_sk = cs_bill_customer_sk AND cs_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_...[truncated]
```

---

### 231. V2_Standard_Iter3 - Q4

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 2000)), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer JOIN store_sales ON c_customer_sk = ss_customer_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_i...[truncated]
```

---

### 232. V2_Standard_Iter4 - Q4

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates_1999 AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year = 1999), filtered_dates_2000 AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year = 2000), filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 2000)), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer JOIN store_sales ON c_customer_sk = ss_customer_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_s...[truncated]
```

---

### 233. V2_Standard_Iter5 - Q4

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk, d_year FROM date_dim WHERE d_year IN (1999, 2000)), year_total AS (SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM(((ss_ext_list_price - ss_ext_wholesale_cost - ss_ext_discount_amt) + ss_ext_sales_price) / 2) AS year_total, 's' AS sale_type FROM customer JOIN store_sales ON c_customer_sk = ss_customer_sk JOIN filtered_dates ON ss_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_id, c_first_name AS customer_first_name, c_last_name AS customer_last_name, c_preferred_cust_flag AS customer_preferred_cust_flag, c_birth_country AS customer_birth_country, c_login AS customer_login, c_email_address AS customer_email_address, d_year AS dyear, SUM((((cs_ext_list_price - cs_ext_wholesale_cost - cs_ext_discount_amt) + cs_ext_sales_price) / 2)) AS year_total, 'c' AS sale_type FROM customer JOIN catalog_sales ON c_customer_sk = cs_bill_customer_sk JOIN filtered_dates ON cs_sold_date_sk = d_date_sk GROUP BY c_customer_id, c_first_name, c_last_name, c_preferred_cust_flag, c_birth_country, c_login, c_email_address, d_year UNION ALL SELECT c_customer_id AS customer_i...[truncated]
```

---

### 234. V2_Standard_Iter1 - Q5

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY)), ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk UNION ALL SELECT sr_store_sk AS store_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS return_amt, sr_net_loss AS net_loss FROM store_returns JOIN date_range ON sr_returned_date_sk = d_date_sk) AS salesreturns, store WHERE store_sk = s_store_sk GROUP BY s_store_id), csr AS (SELECT cp_catalog_page_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT cs_catalog_page_sk AS page_sk, cs_sold_date_sk AS date_sk, cs_ext_sales_price AS sales_price, cs_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM catalog_sales UNION ALL SELECT cr_catalog_page_sk AS page_sk, cr_returned_date_sk AS date_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, cr_return_amount AS return_amt, cr_net_loss AS net_loss FROM catalog_returns) AS salesreturns, date_dim, catalog_page WHERE da...[truncated]
```

---

### 235. V2_Standard_Iter2 - Q5

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY)), ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk UNION ALL SELECT sr_store_sk AS store_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS return_amt, sr_net_loss AS net_loss FROM store_returns JOIN date_range ON sr_returned_date_sk = d_date_sk) AS salesreturns, store WHERE store_sk = s_store_sk GROUP BY s_store_id), csr AS (SELECT cp_catalog_page_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT cs_catalog_page_sk AS page_sk, cs_sold_date_sk AS date_sk, cs_ext_sales_price AS sales_price, cs_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM catalog_sales UNION ALL SELECT cr_catalog_page_sk AS page_sk, cr_returned_date_sk AS date_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, cr_return_amount AS return_amt, cr_net_loss AS net_loss FROM catalog_returns) AS salesreturns, date_dim, catalog_page WHERE da...[truncated]
```

---

### 236. V2_Standard_Iter3 - Q5

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY)), ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk UNION ALL SELECT sr_store_sk AS store_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS return_amt, sr_net_loss AS net_loss FROM store_returns JOIN date_range ON sr_returned_date_sk = d_date_sk) AS salesreturns, store WHERE store_sk = s_store_sk GROUP BY s_store_id), wsr AS (SELECT web_site_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ws_web_site_sk AS wsr_web_site_sk, ws_ext_sales_price AS sales_price, ws_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM web_sales JOIN date_range ON ws_sold_date_sk = d_date_sk UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, wr_return_amt AS return_amt, wr_net_loss AS net_loss FROM web_returns JOIN date_range ON wr_returned_date_sk = d_date_sk LEFT OUTER JOIN web_s...[truncated]
```

---

### 237. V2_Standard_Iter4 - Q5

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY)), wsr AS (SELECT web_site_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ws_web_site_sk AS wsr_web_site_sk, ws_ext_sales_price AS sales_price, ws_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM web_sales JOIN date_range ON ws_sold_date_sk = d_date_sk UNION ALL SELECT ws_web_site_sk AS wsr_web_site_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, wr_return_amt AS return_amt, wr_net_loss AS net_loss FROM (SELECT wr_item_sk, wr_order_number, wr_return_amt, wr_net_loss, ws_web_site_sk FROM web_returns JOIN date_range ON wr_returned_date_sk = d_date_sk) AS filtered_returns LEFT OUTER JOIN web_sales ON (wr_item_sk = ws_item_sk AND wr_order_number = ws_order_number)) AS salesreturns, web_site WHERE wsr_web_site_sk = web_site_sk GROUP BY web_site_id), ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk UNION ALL SELECT sr_store_sk AS store_sk, CAST(0 AS ...[truncated]
```

---

### 238. V2_Standard_Iter5 - Q5

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_range AS (SELECT d_date_sk FROM date_dim WHERE d_date BETWEEN CAST('2000-08-19' AS DATE) AND (CAST('2000-08-19' AS DATE) + INTERVAL '14' DAY)), filtered_store AS (SELECT s_store_sk, s_store_id FROM store), filtered_web_site AS (SELECT web_site_sk, web_site_id FROM web_site), web_sales_filtered AS (SELECT ws_web_site_sk, ws_ext_sales_price, ws_net_profit FROM web_sales JOIN date_range ON ws_sold_date_sk = d_date_sk), web_returns_filtered AS (SELECT wr_web_site_sk, wr_return_amt, wr_net_loss FROM web_returns JOIN date_range ON wr_returned_date_sk = d_date_sk), store_sales_filtered AS (SELECT ss_store_sk, ss_ext_sales_price, ss_net_profit FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk), store_returns_filtered AS (SELECT sr_store_sk, sr_return_amt, sr_net_loss FROM store_returns JOIN date_range ON sr_returned_date_sk = d_date_sk), ssr AS (SELECT s_store_id, SUM(sales_price) AS sales, SUM(profit) AS profit, SUM(return_amt) AS "returns", SUM(net_loss) AS profit_loss FROM (SELECT ss_store_sk AS store_sk, ss_ext_sales_price AS sales_price, ss_net_profit AS profit, CAST(0 AS DECIMAL(7, 2)) AS return_amt, CAST(0 AS DECIMAL(7, 2)) AS net_loss FROM store_sales JOIN date_range ON ss_sold_date_sk = d_date_sk UNION ALL SELECT sr_store_sk AS store_sk, CAST(0 AS DECIMAL(7, 2)) AS sales_price, CAST(0 AS DECIMAL(7, 2)) AS profit, sr_return_amt AS return_amt, sr_net_loss AS net_loss FROM store_returns JOIN date_range ON sr_returned_date_sk = d_date_sk) AS salesreturns,...[truncated]
```

---

### 239. V2_Standard_Iter1 - Q6

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq = (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3))
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN store_sales AS s ON c.c_customer_sk = s.ss_customer_sk JOIN filtered_dates AS fd ON s.ss_sold_date_sk = fd.d_date_sk JOIN item AS i ON s.ss_item_sk = i.i_item_sk WHERE i.i_current_price > 1.2 * (SELECT AVG(j.i_current_price) FROM item AS j WHERE j.i_category = i.i_category) GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 240. V2_Standard_Iter2 - Q6

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH target_month_seq AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq = (SELECT d_month_seq FROM target_month_seq)), category_avg_price AS (SELECT i_category, AVG(i_current_price) * 1.2 AS avg_threshold FROM item GROUP BY i_category), filtered_sales_items AS (SELECT ss_customer_sk, ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN filtered_sales_items AS s ON c.c_customer_sk = s.ss_customer_sk JOIN item AS i ON s.ss_item_sk = i.i_item_sk JOIN category_avg_price AS cap ON i.i_category = cap.i_category WHERE i.i_current_price > cap.avg_threshold GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 241. V2_Standard_Iter3 - Q6

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH target_month_seq AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq = (SELECT d_month_seq FROM target_month_seq)), category_avg_price AS (SELECT i_category, AVG(i_current_price) * 1.2 AS avg_threshold FROM item GROUP BY i_category), filtered_sales_items AS (SELECT ss_customer_sk, ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN item ON ss_item_sk = i_item_sk JOIN category_avg_price AS cap ON i.i_category = cap.i_category WHERE i.i_current_price > cap.avg_threshold)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN filtered_sales_items AS s ON c.c_customer_sk = s.ss_customer_sk GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 242. V2_Standard_Iter4 - Q6

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH target_month_seq AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq = (SELECT d_month_seq FROM target_month_seq)), category_avg_price AS (SELECT i_category, AVG(i_current_price) * 1.2 AS avg_threshold FROM item GROUP BY i_category), filtered_items_by_price AS (SELECT i_item_sk, i_category FROM item AS i JOIN category_avg_price AS cap ON i.i_category = cap.i_category WHERE i.i_current_price > cap.avg_threshold), early_filtered_sales AS (SELECT ss_customer_sk, ss_item_sk FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_items_by_price AS fip ON ss.ss_item_sk = fip.i_item_sk), filtered_sales_items AS (SELECT ss_customer_sk, ss_item_sk FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN early_filtered_sales AS s ON c.c_customer_sk = s.ss_customer_sk GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 243. V2_Standard_Iter5 - Q6

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH target_month_seq AS (SELECT DISTINCT d_month_seq FROM date_dim WHERE d_year = 2002 AND d_moy = 3), filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_month_seq = (SELECT d_month_seq FROM target_month_seq)), category_avg_price AS (SELECT i_category, AVG(i_current_price) * 1.2 AS avg_threshold FROM item GROUP BY i_category), filtered_sales_items AS (SELECT ss.ss_customer_sk, ss.ss_item_sk, i.i_category, i.i_current_price, cap.avg_threshold FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN category_avg_price AS cap ON i.i_category = cap.i_category WHERE i.i_current_price > cap.avg_threshold)
SELECT a.ca_state AS state, COUNT(*) AS cnt FROM customer_address AS a JOIN customer AS c ON a.ca_address_sk = c.c_current_addr_sk JOIN (SELECT ss_customer_sk FROM filtered_sales_items) AS s ON c.c_customer_sk = s.ss_customer_sk GROUP BY a.ca_state HAVING COUNT(*) >= 10 ORDER BY cnt, a.ca_state LIMIT 100
```

---

### 244. V2_Standard_Iter1 - Q7

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_date AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_cdemo AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), union_sales AS (SELECT i.i_item_id, ss.ss_quantity, ss.ss_list_price, ss.ss_coupon_amt, ss.ss_sales_price FROM store_sales AS ss JOIN filtered_date AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_cdemo AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN promotion AS p ON ss.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_email = 'N' UNION ALL SELECT i.i_item_id, ss.ss_quantity, ss.ss_list_price, ss.ss_coupon_amt, ss.ss_sales_price FROM store_sales AS ss JOIN filtered_date AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_cdemo AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk JOIN promotion AS p ON ss.ss_promo_sk = p.p_promo_sk WHERE p.p_channel_event = 'N' AND (p.p_channel_email <> 'N' OR p.p_channel_email IS NULL))
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM union_sales GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 245. V2_Standard_Iter2 - Q7

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_cd AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), filtered_promo AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' OR p_channel_event = 'N'), filtered_store_sales AS (SELECT ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN filtered_cd ON ss_cdemo_sk = cd_demo_sk JOIN filtered_promo ON ss_promo_sk = p_promo_sk)
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM filtered_store_sales JOIN item ON ss_item_sk = i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 246. V2_Standard_Iter3 - Q7

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), cd_filtered AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), promo_email AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N'), promo_event AS (SELECT p_promo_sk FROM promotion WHERE p_channel_event = 'N' AND p_channel_email <> 'N')
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM ((SELECT ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price FROM store_sales JOIN date_filtered ON ss_sold_date_sk = d_date_sk JOIN cd_filtered ON ss_cdemo_sk = cd_demo_sk JOIN promo_email ON ss_promo_sk = p_promo_sk) UNION ALL (SELECT ss_item_sk, ss_quantity, ss_list_price, ss_coupon_amt, ss_sales_price FROM store_sales JOIN date_filtered ON ss_sold_date_sk = d_date_sk JOIN cd_filtered ON ss_cdemo_sk = cd_demo_sk JOIN promo_event ON ss_promo_sk = p_promo_sk)) AS combined JOIN item ON ss_item_sk = i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 247. V2_Standard_Iter4 - Q7

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), filtered_customer_demographics AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), filtered_promotions AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' OR p_channel_event = 'N'), filtered_items AS (SELECT i_item_sk, i_item_id FROM item)
SELECT i.i_item_id, AVG(ss.ss_quantity) AS agg1, AVG(ss.ss_list_price) AS agg2, AVG(ss.ss_coupon_amt) AS agg3, AVG(ss.ss_sales_price) AS agg4 FROM store_sales AS ss JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk JOIN filtered_customer_demographics AS fcd ON ss.ss_cdemo_sk = fcd.cd_demo_sk JOIN filtered_promotions AS fp ON ss.ss_promo_sk = fp.p_promo_sk JOIN filtered_items AS i ON ss.ss_item_sk = i.i_item_sk GROUP BY i.i_item_id ORDER BY i.i_item_id LIMIT 100
```

---

### 248. V2_Standard_Iter5 - Q7

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_year = 2001), cd_filtered AS (SELECT cd_demo_sk FROM customer_demographics WHERE cd_gender = 'F' AND cd_marital_status = 'W' AND cd_education_status = 'College'), promo_filtered AS (SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N' OR p_channel_event = 'N')
SELECT i_item_id, AVG(ss_quantity) AS agg1, AVG(ss_list_price) AS agg2, AVG(ss_coupon_amt) AS agg3, AVG(ss_sales_price) AS agg4 FROM store_sales AS ss JOIN date_filtered AS df ON ss.ss_sold_date_sk = df.d_date_sk JOIN cd_filtered AS cd ON ss.ss_cdemo_sk = cd.cd_demo_sk JOIN promo_filtered AS pf ON ss.ss_promo_sk = pf.p_promo_sk JOIN item AS i ON ss.ss_item_sk = i.i_item_sk GROUP BY i_item_id ORDER BY i_item_id LIMIT 100
```

---

### 249. V2_Standard_Iter1 - Q8

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH filtered_dates AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 2 AND d_year = 1998)
SELECT s_store_name, SUM(ss_net_profit) FROM store_sales JOIN filtered_dates ON ss_sold_date_sk = d_date_sk JOIN store ON ss_store_sk = s_store_sk, (SELECT ca_zip FROM (SELECT SUBSTRING(ca_zip, 1, 5) AS ca_zip FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ('47602', '16704', '35863', '28577', '83910', '36201', '58412', '48162', '28055', '41419', '80332', '38607', '77817', '24891', '16226', '18410', '21231', '59345', '13918', '51089', '20317', '17167', '54585', '67881', '78366', '47770', '18360', '51717', '73108', '14440', '21800', '89338', '45859', '65501', '34948', '25973', '73219', '25333', '17291', '10374', '18829', '60736', '82620', '41351', '52094', '19326', '25214', '54207', '40936', '21814', '79077', '25178', '75742', '77454', '30621', '89193', '27369', '41232', '48567', '83041', '71948', '37119', '68341', '14073', '16891', '62878', '49130', '19833', '24286', '27700', '40979', '50412', '81504', '94835', '84844', '71954', '39503', '57649', '18434', '24987', '12350', '86379', '27413', '44529', '98569', '16515', '27287', '24255', '21094', '16005', '56436', '91110', '68293', '56455', '54558', '10298', '83647', '32754', '27052', '51766', '19444', '13869', '45645', '94791', '57631', '20712', '37788', '41807', '46507', '21727', '71836', '81070', '50632', '88086', '63991', '20244', '31655', '51782', '29818', '63792', '68605', '94898', '36430', '57025', '20601', '82080', '33869', '...[truncated]
```

---

### 250. V2_Standard_Iter2 - Q8

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH date_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 2 AND d_year = 1998), zip_set1 AS (SELECT DISTINCT SUBSTRING(ca_zip, 1, 5) AS ca_zip FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ('47602', '16704', '35863', '28577', '83910', '36201', '58412', '48162', '28055', '41419', '80332', '38607', '77817', '24891', '16226', '18410', '21231', '59345', '13918', '51089', '20317', '17167', '54585', '67881', '78366', '47770', '18360', '51717', '73108', '14440', '21800', '89338', '45859', '65501', '34948', '25973', '73219', '25333', '17291', '10374', '18829', '60736', '82620', '41351', '52094', '19326', '25214', '54207', '40936', '21814', '79077', '25178', '75742', '77454', '30621', '89193', '27369', '41232', '48567', '83041', '71948', '37119', '68341', '14073', '16891', '62878', '49130', '19833', '24286', '27700', '40979', '50412', '81504', '94835', '84844', '71954', '39503', '57649', '18434', '24987', '12350', '86379', '27413', '44529', '98569', '16515', '27287', '24255', '21094', '16005', '56436', '91110', '68293', '56455', '54558', '10298', '83647', '32754', '27052', '51766', '19444', '13869', '45645', '94791', '57631', '20712', '37788', '41807', '46507', '21727', '71836', '81070', '50632', '88086', '63991', '20244', '31655', '51782', '29818', '63792', '68605', '94898', '36430', '57025', '20601', '82080', '33869', '22728', '35834', '29086', '92645', '98584', '98072', '11652', '78093', '57553', '43830', '71144', '53565', '18700', '90209', '71256', '38353', '543...[truncated]
```

---

### 251. V2_Standard_Iter3 - Q8

**Source**: V2_Standard_Iter3

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH zip_set2 AS (SELECT SUBSTRING(ca_zip, 1, 5) AS ca_zip, COUNT(*) AS cnt FROM customer_address JOIN customer ON ca_address_sk = c_current_addr_sk WHERE c_preferred_cust_flag = 'Y' GROUP BY SUBSTRING(ca_zip, 1, 5) HAVING COUNT(*) > 10), zip_prefixes AS (SELECT DISTINCT SUBSTRING(ca_zip, 1, 2) AS zip_prefix FROM zip_set2), filtered_stores AS (SELECT s_store_sk, s_store_name, s_zip FROM store WHERE SUBSTRING(s_zip, 1, 2) IN (SELECT zip_prefix FROM zip_prefixes)), date_filtered AS (SELECT d_date_sk FROM date_dim WHERE d_qoy = 2 AND d_year = 1998), zip_set1 AS (SELECT DISTINCT SUBSTRING(ca_zip, 1, 5) AS ca_zip FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ('47602', '16704', '35863', '28577', '83910', '36201', '58412', '48162', '28055', '41419', '80332', '38607', '77817', '24891', '16226', '18410', '21231', '59345', '13918', '51089', '20317', '17167', '54585', '67881', '78366', '47770', '18360', '51717', '73108', '14440', '21800', '89338', '45859', '65501', '34948', '25973', '73219', '25333', '17291', '10374', '18829', '60736', '82620', '41351', '52094', '19326', '25214', '54207', '40936', '21814', '79077', '25178', '75742', '77454', '30621', '89193', '27369', '41232', '48567', '83041', '71948', '37119', '68341', '14073', '16891', '62878', '49130', '19833', '24286', '27700', '40979', '50412', '81504', '94835', '84844', '71954', '39503', '57649', '18434', '24987', '12350', '86379', '27413', '44529', '98569', '16515', '27287', '24255', '21094', '16005', '56436', '91110', ...[truncated]
```

---

### 252. V2_Standard_Iter4 - Q8

**Source**: V2_Standard_Iter4

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH zip_set2 AS (SELECT SUBSTRING(ca_zip, 1, 5) AS ca_zip, COUNT(*) AS cnt FROM customer_address JOIN customer ON ca_address_sk = c_current_addr_sk WHERE c_preferred_cust_flag = 'Y' GROUP BY SUBSTRING(ca_zip, 1, 5) HAVING COUNT(*) > 10), zip_set1 AS (SELECT DISTINCT SUBSTRING(ca_zip, 1, 5) AS ca_zip FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ('47602', '16704', '35863', '28577', '83910', '36201', '58412', '48162', '28055', '41419', '80332', '38607', '77817', '24891', '16226', '18410', '21231', '59345', '13918', '51089', '20317', '17167', '54585', '67881', '78366', '47770', '18360', '51717', '73108', '14440', '21800', '89338', '45859', '65501', '34948', '25973', '73219', '25333', '17291', '10374', '18829', '60736', '82620', '41351', '52094', '19326', '25214', '54207', '40936', '21814', '79077', '25178', '75742', '77454', '30621', '89193', '27369', '41232', '48567', '83041', '71948', '37119', '68341', '14073', '16891', '62878', '49130', '19833', '24286', '27700', '40979', '50412', '81504', '94835', '84844', '71954', '39503', '57649', '18434', '24987', '12350', '86379', '27413', '44529', '98569', '16515', '27287', '24255', '21094', '16005', '56436', '91110', '68293', '56455', '54558', '10298', '83647', '32754', '27052', '51766', '19444', '13869', '45645', '94791', '57631', '20712', '37788', '41807', '46507', '21727', '71836', '81070', '50632', '88086', '63991', '20244', '31655', '51782', '29818', '63792', '68605', '94898', '36430', '57025', '20601', '82080', '33869',...[truncated]
```

---

### 253. V2_Standard_Iter5 - Q8

**Source**: V2_Standard_Iter5

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH zip_set2 AS (SELECT SUBSTRING(ca_zip, 1, 5) AS ca_zip, COUNT(*) AS cnt FROM customer_address JOIN customer ON ca_address_sk = c_current_addr_sk WHERE c_preferred_cust_flag = 'Y' GROUP BY SUBSTRING(ca_zip, 1, 5) HAVING COUNT(*) > 10), zip_set1 AS (SELECT DISTINCT SUBSTRING(ca_zip, 1, 5) AS ca_zip FROM customer_address WHERE SUBSTRING(ca_zip, 1, 5) IN ('47602', '16704', '35863', '28577', '83910', '36201', '58412', '48162', '28055', '41419', '80332', '38607', '77817', '24891', '16226', '18410', '21231', '59345', '13918', '51089', '20317', '17167', '54585', '67881', '78366', '47770', '18360', '51717', '73108', '14440', '21800', '89338', '45859', '65501', '34948', '25973', '73219', '25333', '17291', '10374', '18829', '60736', '82620', '41351', '52094', '19326', '25214', '54207', '40936', '21814', '79077', '25178', '75742', '77454', '30621', '89193', '27369', '41232', '48567', '83041', '71948', '37119', '68341', '14073', '16891', '62878', '49130', '19833', '24286', '27700', '40979', '50412', '81504', '94835', '84844', '71954', '39503', '57649', '18434', '24987', '12350', '86379', '27413', '44529', '98569', '16515', '27287', '24255', '21094', '16005', '56436', '91110', '68293', '56455', '54558', '10298', '83647', '32754', '27052', '51766', '19444', '13869', '45645', '94791', '57631', '20712', '37788', '41807', '46507', '21727', '71836', '81070', '50632', '88086', '63991', '20244', '31655', '51782', '29818', '63792', '68605', '94898', '36430', '57025', '20601', '82080', '33869',...[truncated]
```

---

### 254. V2_Standard_Iter1 - Q9

**Source**: V2_Standard_Iter1

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH store_sales_aggregates AS (SELECT SUM(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 ELSE 0 END) AS cnt_1_20, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_sales_price END) AS avg_ext_1_20, AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_profit END) AS avg_net_1_20, SUM(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 ELSE 0 END) AS cnt_21_40, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_ext_sales_price END) AS avg_ext_21_40, AVG(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN ss_net_profit END) AS avg_net_21_40, SUM(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN 1 ELSE 0 END) AS cnt_41_60, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_ext_sales_price END) AS avg_ext_41_60, AVG(CASE WHEN ss_quantity BETWEEN 41 AND 60 THEN ss_net_profit END) AS avg_net_41_60, SUM(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN 1 ELSE 0 END) AS cnt_61_80, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_ext_sales_price END) AS avg_ext_61_80, AVG(CASE WHEN ss_quantity BETWEEN 61 AND 80 THEN ss_net_profit END) AS avg_net_61_80, SUM(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN 1 ELSE 0 END) AS cnt_81_100, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_ext_sales_price END) AS avg_ext_81_100, AVG(CASE WHEN ss_quantity BETWEEN 81 AND 100 THEN ss_net_profit END) AS avg_net_81_100 FROM store_sales)
SELECT CASE WHEN ssa.cnt_1_20 > 2972190 THEN ssa.avg_ext_1_20 ELSE ssa.avg_net_1_20 END AS bucket1, CASE WHEN ssa.cnt_21_40 > 4505785 THEN ssa.avg_ext_21_40 ELSE ssa.avg_net_...[truncated]
```

---

### 255. V2_Standard_Iter2 - Q9

**Source**: V2_Standard_Iter2

#### BEFORE (Original)
```sql
[V2 Standard - see prompt]
```

#### AFTER (Optimized)
```sql
WITH quantity_1_20_stats AS (SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20), quantity_21_40_stats AS (SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40), quantity_41_60_stats AS (SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_quantity BETWEEN 41 AND 60), quantity_61_80_stats AS (SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_quantity BETWEEN 61 AND 80), quantity_81_100_stats AS (SELECT COUNT(*) AS cnt, AVG(ss_ext_sales_price) AS avg_ext_price, AVG(ss_net_profit) AS avg_net_profit FROM store_sales WHERE ss_quantity BETWEEN 81 AND 100)
SELECT CASE WHEN q1.cnt > 2972190 THEN q1.avg_ext_price ELSE q1.avg_net_profit END AS bucket1, CASE WHEN q2.cnt > 4505785 THEN q2.avg_ext_price ELSE q2.avg_net_profit END AS bucket2, CASE WHEN q3.cnt > 1575726 THEN q3.avg_ext_price ELSE q3.avg_net_profit END AS bucket3, CASE WHEN q4.cnt > 3188917 THEN q4.avg_ext_price ELSE q4.avg_net_profit END AS bucket4, CASE WHEN q5.cnt > 3525216 THEN q5.avg_ext_price ELSE q5.avg_net_profit END AS bucket5 FROM reason CROSS JOIN quantity_1_20_stats AS q1 CROSS JOIN quantity_21_40_stats AS q2 CROSS JOIN quantity_41_60_stats AS q3 CROSS JOIN...[truncated]
```

---

### 256. Archive - Q1

**Source**: Archive

#### BEFORE (Original)
```sql
-- TPC-DS Query 1 - Original
-- Runtime: 0.581s avg (SF100)

with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
LIMIT 100;

```

#### AFTER (Optimized)
```sql
-- TPC-DS Query 1 - Optimized
-- Runtime: 0.116s avg (SF100)
-- Speedup: 5x
--
-- Techniques:
--   1. Predicate pushdown (s_state = 'SD' before aggregation)
--   2. Window function replaces correlated subquery
--   3. Early join enables filter pushdown
--   4. Late materialization of customer lookup

WITH sd_store_returns AS (
    -- Filter to 'SD' stores and year 2000 BEFORE aggregation
    SELECT
        sr_customer_sk,
        sr_store_sk,
        SUM(sr_fee) AS ctr_total_return
    FROM store_returns
    JOIN date_dim ON sr_returned_date_sk = d_date_sk
    JOIN store ON sr_store_sk = s_store_sk
    WHERE d_year = 2000
      AND s_state = 'SD'
    GROUP BY sr_customer_sk, sr_store_sk
),
high_return_candidates AS (
    -- Window function replaces correlated subquery for avg calculation
    SELECT
        sr_customer_sk,
        ctr_total_return
    FROM (
        SELECT
            sr_customer_sk,
            ctr_total_return,
            AVG(ctr_total_return) OVER (PARTITION BY sr_store_sk) as store_avg
        FROM sd_store_returns
    )
    WHERE ctr_total_return > (store_avg * 1.2)
)
-- Late materialization: only look up customer_id for qualifying rows
SELECT
    c_customer_id
FROM high_return_candidates
JOIN customer ON c_customer_sk = sr_customer_sk
ORDER BY c_customer_id
LIMIT 100;

```

---

