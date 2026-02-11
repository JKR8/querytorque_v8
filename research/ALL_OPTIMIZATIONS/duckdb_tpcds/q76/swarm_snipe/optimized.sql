SELECT
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  COUNT(*) AS sales_cnt,
  SUM(ext_sales_price) AS sales_amt
FROM (
  SELECT
    'store' AS channel,
    'ss_hdemo_sk' AS col_name,
    d.d_year,
    d.d_qoy,
    i.i_category,
    ss.ss_ext_sales_price AS ext_sales_price
  FROM store_sales ss
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  WHERE ss.ss_hdemo_sk IS NULL
  UNION ALL
  SELECT
    'web' AS channel,
    'ws_bill_addr_sk' AS col_name,
    d.d_year,
    d.d_qoy,
    i.i_category,
    ws.ws_ext_sales_price AS ext_sales_price
  FROM web_sales ws
  JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
  JOIN item i ON ws.ws_item_sk = i.i_item_sk
  WHERE ws.ws_bill_addr_sk IS NULL
  UNION ALL
  SELECT
    'catalog' AS channel,
    'cs_warehouse_sk' AS col_name,
    d.d_year,
    d.d_qoy,
    i.i_category,
    cs.cs_ext_sales_price AS ext_sales_price
  FROM catalog_sales cs
  JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  WHERE cs.cs_warehouse_sk IS NULL
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