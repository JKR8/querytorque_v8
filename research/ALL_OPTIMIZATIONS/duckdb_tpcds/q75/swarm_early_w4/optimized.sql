WITH catalog_sales_data AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(cs_quantity - COALESCE(cr_return_quantity, 0)) AS sales_cnt,
    SUM(cs_ext_sales_price - COALESCE(cr_return_amount, 0.0)) AS sales_amt
  FROM catalog_sales
  JOIN item ON i_item_sk = cs_item_sk
  JOIN date_dim ON d_date_sk = cs_sold_date_sk
  LEFT JOIN catalog_returns ON cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk
  WHERE i_category = 'Home'
    AND d_year IN (1998, 1999)
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
),
store_sales_data AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(ss_quantity - COALESCE(sr_return_quantity, 0)) AS sales_cnt,
    SUM(ss_ext_sales_price - COALESCE(sr_return_amt, 0.0)) AS sales_amt
  FROM store_sales
  JOIN item ON i_item_sk = ss_item_sk
  JOIN date_dim ON d_date_sk = ss_sold_date_sk
  LEFT JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
  WHERE i_category = 'Home'
    AND d_year IN (1998, 1999)
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
),
web_sales_data AS (
  SELECT
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id,
    SUM(ws_quantity - COALESCE(wr_return_quantity, 0)) AS sales_cnt,
    SUM(ws_ext_sales_price - COALESCE(wr_return_amt, 0.0)) AS sales_amt
  FROM web_sales
  JOIN item ON i_item_sk = ws_item_sk
  JOIN date_dim ON d_date_sk = ws_sold_date_sk
  LEFT JOIN web_returns ON ws_order_number = wr_order_number AND ws_item_sk = wr_item_sk
  WHERE i_category = 'Home'
    AND d_year IN (1998, 1999)
  GROUP BY
    d_year,
    i_brand_id,
    i_class_id,
    i_category_id,
    i_manufact_id
),
combined_sales AS (
  SELECT * FROM catalog_sales_data
  UNION ALL
  SELECT * FROM store_sales_data
  UNION ALL
  SELECT * FROM web_sales_data
),
aggregated AS (
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
windowed AS (
  SELECT
    d_year AS year,
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
    ) AS prev_sales_amt,
    LAG(d_year) OVER (
      PARTITION BY i_brand_id, i_class_id, i_category_id, i_manufact_id
      ORDER BY d_year
    ) AS prev_year
  FROM aggregated
)
SELECT
  prev_year AS prev_year,
  year AS year,
  i_brand_id,
  i_class_id,
  i_category_id,
  i_manufact_id,
  prev_sales_cnt AS prev_yr_cnt,
  sales_cnt AS curr_yr_cnt,
  sales_cnt - prev_sales_cnt AS sales_cnt_diff,
  sales_amt - prev_sales_amt AS sales_amt_diff
FROM windowed
WHERE year = 1999
  AND prev_year = 1998
  AND prev_sales_cnt > 0
  AND CAST(sales_cnt AS DECIMAL(17, 2)) / CAST(prev_sales_cnt AS DECIMAL(17, 2)) < 0.9
ORDER BY
  sales_cnt_diff,
  sales_amt_diff
LIMIT 100