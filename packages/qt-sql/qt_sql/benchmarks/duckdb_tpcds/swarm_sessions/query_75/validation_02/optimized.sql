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
catalog_sales_detail AS (
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
store_sales_detail AS (
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
web_sales_detail AS (
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
all_sales AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(sales_cnt) AS sales_cnt,
    SUM(sales_amt) AS sales_amt
  FROM (
    SELECT * FROM catalog_sales_detail
    UNION ALL
    SELECT * FROM store_sales_detail
    UNION ALL
    SELECT * FROM web_sales_detail
  ) sales_detail
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
)
SELECT
  prev_yr.d_year AS prev_year,
  curr_yr.d_year AS year,
  curr_yr.i_brand_id,
  curr_yr.i_class_id,
  curr_yr.i_category_id,
  curr_yr.i_manufact_id,
  prev_yr.sales_cnt AS prev_yr_cnt,
  curr_yr.sales_cnt AS curr_yr_cnt,
  curr_yr.sales_cnt - prev_yr.sales_cnt AS sales_cnt_diff,
  curr_yr.sales_amt - prev_yr.sales_amt AS sales_amt_diff
FROM all_sales curr_yr
JOIN all_sales prev_yr ON (
  curr_yr.i_brand_id = prev_yr.i_brand_id
  AND curr_yr.i_class_id = prev_yr.i_class_id
  AND curr_yr.i_category_id = prev_yr.i_category_id
  AND curr_yr.i_manufact_id = prev_yr.i_manufact_id
)
WHERE
  curr_yr.d_year = 1999
  AND prev_yr.d_year = 1998
  AND CAST(curr_yr.sales_cnt AS DECIMAL(17, 2)) / CAST(prev_yr.sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY
  sales_cnt_diff,
  sales_amt_diff
LIMIT 100