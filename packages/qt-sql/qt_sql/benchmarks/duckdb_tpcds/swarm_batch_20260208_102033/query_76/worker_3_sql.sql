WITH item_dim AS (
  SELECT 
    i_item_sk,
    i_category
  FROM item
),
date_dim AS (
  SELECT 
    d_date_sk,
    d_year,
    d_qoy
  FROM date_dim
),
store_sales_filtered AS (
  SELECT 
    ss_sold_date_sk,
    ss_item_sk,
    ss_ext_sales_price
  FROM store_sales
  WHERE ss_hdemo_sk IS NULL
),
web_sales_filtered AS (
  SELECT 
    ws_sold_date_sk,
    ws_item_sk,
    ws_ext_sales_price
  FROM web_sales
  WHERE ws_bill_addr_sk IS NULL
),
catalog_sales_filtered AS (
  SELECT 
    cs_sold_date_sk,
    cs_item_sk,
    cs_ext_sales_price
  FROM catalog_sales
  WHERE cs_warehouse_sk IS NULL
),
store_channel AS (
  SELECT
    'store' AS channel,
    'ss_hdemo_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ss_ext_sales_price AS ext_sales_price
  FROM store_sales_filtered s
  JOIN date_dim d ON s.ss_sold_date_sk = d.d_date_sk
  JOIN item_dim i ON s.ss_item_sk = i.i_item_sk
),
web_channel AS (
  SELECT
    'web' AS channel,
    'ws_bill_addr_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ws_ext_sales_price AS ext_sales_price
  FROM web_sales_filtered w
  JOIN date_dim d ON w.ws_sold_date_sk = d.d_date_sk
  JOIN item_dim i ON w.ws_item_sk = i.i_item_sk
),
catalog_channel AS (
  SELECT
    'catalog' AS channel,
    'cs_warehouse_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    cs_ext_sales_price AS ext_sales_price
  FROM catalog_sales_filtered c
  JOIN date_dim d ON c.cs_sold_date_sk = d.d_date_sk
  JOIN item_dim i ON c.cs_item_sk = i.i_item_sk
),
all_channels AS (
  SELECT * FROM store_channel
  UNION ALL
  SELECT * FROM web_channel
  UNION ALL
  SELECT * FROM catalog_channel
)
SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) AS sales_cnt,
  SUM(ext_sales_price) AS sales_amt
FROM all_channels
GROUP BY
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category
ORDER BY
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category
LIMIT 100;