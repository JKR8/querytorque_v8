WITH filtered_item AS (
  SELECT 
    i_item_sk,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
  FROM item
  WHERE i_category = 'Home'
),
filtered_dates AS (
  SELECT 
    d_date_sk,
    d_year
  FROM date_dim
  WHERE d_year IN (1998, 1999)
),
catalog_sales_prep AS (
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    cs.cs_quantity - COALESCE(cr.cr_return_quantity, 0) AS sales_cnt,
    cs.cs_ext_sales_price - COALESCE(cr.cr_return_amount, 0.0) AS sales_amt
  FROM catalog_sales cs
  JOIN filtered_item i ON i.i_item_sk = cs.cs_item_sk
  JOIN filtered_dates d ON d.d_date_sk = cs.cs_sold_date_sk
  LEFT JOIN catalog_returns cr ON (
    cs.cs_order_number = cr.cr_order_number 
    AND cs.cs_item_sk = cr.cr_item_sk
  )
),
store_sales_prep AS (
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    ss.ss_quantity - COALESCE(sr.sr_return_quantity, 0) AS sales_cnt,
    ss.ss_ext_sales_price - COALESCE(sr.sr_return_amt, 0.0) AS sales_amt
  FROM store_sales ss
  JOIN filtered_item i ON i.i_item_sk = ss.ss_item_sk
  JOIN filtered_dates d ON d.d_date_sk = ss.ss_sold_date_sk
  LEFT JOIN store_returns sr ON (
    ss.ss_ticket_number = sr.sr_ticket_number 
    AND ss.ss_item_sk = sr.sr_item_sk
  )
),
web_sales_prep AS (
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    ws.ws_quantity - COALESCE(wr.wr_return_quantity, 0) AS sales_cnt,
    ws.ws_ext_sales_price - COALESCE(wr.wr_return_amt, 0.0) AS sales_amt
  FROM web_sales ws
  JOIN filtered_item i ON i.i_item_sk = ws.ws_item_sk
  JOIN filtered_dates d ON d.d_date_sk = ws.ws_sold_date_sk
  LEFT JOIN web_returns wr ON (
    ws.ws_order_number = wr.wr_order_number 
    AND ws.ws_item_sk = wr.wr_item_sk
  )
),
combined_sales AS (
  SELECT * FROM catalog_sales_prep
  UNION ALL
  SELECT * FROM store_sales_prep
  UNION ALL
  SELECT * FROM web_sales_prep
),
all_sales AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(sales_cnt) AS sales_cnt,
    SUM(sales_amt) AS sales_amt
  FROM combined_sales
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
),
curr_year_sales AS (
  SELECT *
  FROM all_sales
  WHERE d_year = 1999
),
prev_year_sales AS (
  SELECT *
  FROM all_sales
  WHERE d_year = 1998
)
SELECT
  1998 AS prev_year,
  1999 AS year,
  curr.i_brand_id,
  curr.i_class_id,
  curr.i_category_id,
  curr.i_manufact_id,
  prev.sales_cnt AS prev_yr_cnt,
  curr.sales_cnt AS curr_yr_cnt,
  curr.sales_cnt - prev.sales_cnt AS sales_cnt_diff,
  curr.sales_amt - prev.sales_amt AS sales_amt_diff
FROM curr_year_sales curr
JOIN prev_year_sales prev ON (
  curr.i_brand_id = prev.i_brand_id
  AND curr.i_class_id = prev.i_class_id
  AND curr.i_category_id = prev.i_category_id
  AND curr.i_manufact_id = prev.i_manufact_id
)
WHERE CAST(curr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY
  sales_cnt_diff,
  sales_amt_diff
LIMIT 100