WITH filtered_store_sales AS (
  SELECT 
    ss_item_sk,
    ss_store_sk,
    ss_customer_sk,
    ss_ticket_number,
    SUM(ss_quantity) AS ss_quantity_sum,
    COUNT(ss_quantity) AS ss_quantity_cnt
  FROM store_sales
  JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
  WHERE d1.d_moy = 4 
    AND d1.d_year = 1999
  GROUP BY ss_item_sk, ss_store_sk, ss_customer_sk, ss_ticket_number
),

filtered_store_returns AS (
  SELECT 
    sr_item_sk,
    sr_customer_sk,
    sr_ticket_number,
    SUM(sr_return_quantity) AS sr_return_quantity_sum,
    COUNT(sr_return_quantity) AS sr_return_quantity_cnt
  FROM store_returns
  JOIN date_dim d2 ON d2.d_date_sk = sr_returned_date_sk
  WHERE d2.d_moy BETWEEN 4 AND 7 
    AND d2.d_year = 1999
  GROUP BY sr_item_sk, sr_customer_sk, sr_ticket_number
),

filtered_catalog_sales AS (
  SELECT 
    cs_item_sk,
    cs_bill_customer_sk,
    SUM(cs_quantity) AS cs_quantity_sum,
    COUNT(cs_quantity) AS cs_quantity_cnt
  FROM catalog_sales
  JOIN date_dim d3 ON d3.d_date_sk = cs_sold_date_sk
  WHERE d3.d_year IN (1999, 2000, 2001)
  GROUP BY cs_item_sk, cs_bill_customer_sk
)

SELECT
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name,
  SUM(ss.ss_quantity_sum) * 1.0 / SUM(ss.ss_quantity_cnt) AS store_sales_quantity,
  SUM(sr.sr_return_quantity_sum) * 1.0 / SUM(sr.sr_return_quantity_cnt) AS store_returns_quantity,
  SUM(cs.cs_quantity_sum) * 1.0 / SUM(cs.cs_quantity_cnt) AS catalog_sales_quantity
FROM filtered_store_sales ss
JOIN filtered_store_returns sr 
  ON ss.ss_item_sk = sr.sr_item_sk
  AND ss.ss_customer_sk = sr.sr_customer_sk
  AND ss.ss_ticket_number = sr.sr_ticket_number
JOIN filtered_catalog_sales cs
  ON sr.sr_item_sk = cs.cs_item_sk
  AND sr.sr_customer_sk = cs.cs_bill_customer_sk
JOIN store s ON ss.ss_store_sk = s.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
GROUP BY
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name
ORDER BY
  i.i_item_id,
  i.i_item_desc,
  s.s_store_id,
  s.s_store_name
LIMIT 100