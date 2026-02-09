WITH store_sales_filtered AS (
  SELECT
    'store' AS channel,
    'ss_hdemo_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ss_ext_sales_price AS ext_sales_price
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE ss_hdemo_sk IS NULL
),
web_sales_filtered AS (
  SELECT
    'web' AS channel,
    'ws_bill_addr_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    ws_ext_sales_price AS ext_sales_price
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  JOIN item ON ws_item_sk = i_item_sk
  WHERE ws_bill_addr_sk IS NULL
),
catalog_sales_filtered AS (
  SELECT
    'catalog' AS channel,
    'cs_warehouse_sk' AS col_name,
    d_year,
    d_qoy,
    i_category,
    cs_ext_sales_price AS ext_sales_price
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  JOIN item ON cs_item_sk = i_item_sk
  WHERE cs_warehouse_sk IS NULL
)
SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) AS sales_cnt,
  SUM(ext_sales_price) AS sales_amt
FROM (
  SELECT * FROM store_sales_filtered
  UNION ALL
  SELECT * FROM web_sales_filtered
  UNION ALL
  SELECT * FROM catalog_sales_filtered
) AS foo
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
LIMIT 100