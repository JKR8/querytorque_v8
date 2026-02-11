WITH filtered_items AS (
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
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_year IN (1998, 1999)
),
catalog_returns_agg AS (
  SELECT
    cr_order_number,
    cr_item_sk,
    SUM(cr_return_quantity) AS cr_return_quantity,
    SUM(cr_return_amount) AS cr_return_amount
  FROM catalog_returns
  GROUP BY cr_order_number, cr_item_sk
),
store_returns_agg AS (
  SELECT
    sr_ticket_number,
    sr_item_sk,
    SUM(sr_return_quantity) AS sr_return_quantity,
    SUM(sr_return_amt) AS sr_return_amt
  FROM store_returns
  GROUP BY sr_ticket_number, sr_item_sk
),
web_returns_agg AS (
  SELECT
    wr_order_number,
    wr_item_sk,
    SUM(wr_return_quantity) AS wr_return_quantity,
    SUM(wr_return_amt) AS wr_return_amt
  FROM web_returns
  GROUP BY wr_order_number, wr_item_sk
),
channel_sales AS (
  -- Catalog sales
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    cs.cs_quantity - COALESCE(cr.cr_return_quantity, 0) AS sales_cnt,
    cs.cs_ext_sales_price - COALESCE(cr.cr_return_amount, 0.0) AS sales_amt
  FROM catalog_sales cs
  JOIN filtered_items i ON cs.cs_item_sk = i.i_item_sk
  JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
  LEFT JOIN catalog_returns_agg cr ON 
    cs.cs_order_number = cr.cr_order_number 
    AND cs.cs_item_sk = cr.cr_item_sk
  
  UNION ALL
  
  -- Store sales
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    ss.ss_quantity - COALESCE(sr.sr_return_quantity, 0) AS sales_cnt,
    ss.ss_ext_sales_price - COALESCE(sr.sr_return_amt, 0.0) AS sales_amt
  FROM store_sales ss
  JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  LEFT JOIN store_returns_agg sr ON 
    ss.ss_ticket_number = sr.sr_ticket_number 
    AND ss.ss_item_sk = sr.sr_item_sk
  
  UNION ALL
  
  -- Web sales
  SELECT
    d.d_year,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    i.i_manufact_id,
    ws.ws_quantity - COALESCE(wr.wr_return_quantity, 0) AS sales_cnt,
    ws.ws_ext_sales_price - COALESCE(wr.wr_return_amt, 0.0) AS sales_amt
  FROM web_sales ws
  JOIN filtered_items i ON ws.ws_item_sk = i.i_item_sk
  JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
  LEFT JOIN web_returns_agg wr ON 
    ws.ws_order_number = wr.wr_order_number 
    AND ws.ws_item_sk = wr.wr_item_sk
),
aggregated_sales AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(sales_cnt) AS sales_cnt,
    SUM(sales_amt) AS sales_amt
  FROM channel_sales
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
),
windowed_sales AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    sales_cnt,
    sales_amt,
    LAG(sales_cnt) OVER (
      PARTITION BY i_brand_id, i_class_id, i_category_id, i_manufact_id 
      ORDER BY d_year
    ) AS prev_sales_cnt,
    LAG(sales_amt) OVER (
      PARTITION BY i_brand_id, i_class_id, i_category_id, i_manufact_id 
      ORDER BY d_year
    ) AS prev_sales_amt
  FROM aggregated_sales
)
SELECT
  d_year - 1 AS prev_year,
  d_year AS year,
  i_brand_id,
  i_class_id,
  i_category_id,
  i_manufact_id,
  prev_sales_cnt AS prev_yr_cnt,
  sales_cnt AS curr_yr_cnt,
  sales_cnt - prev_sales_cnt AS sales_cnt_diff,
  sales_amt - prev_sales_amt AS sales_amt_diff
FROM windowed_sales
WHERE
  d_year = 1999
  AND prev_sales_cnt IS NOT NULL
  AND (sales_cnt * 10) < (prev_sales_cnt * 9)  -- Integer comparison for ratio < 0.9
ORDER BY
  sales_cnt_diff,
  sales_amt_diff
LIMIT 100;