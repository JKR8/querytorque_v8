WITH 
  date_dim_cte AS (
    SELECT 
      d_date_sk,
      d_year,
      d_qoy
    FROM date_dim
  ),
  item_cte AS (
    SELECT 
      i_item_sk,
      i_category
    FROM item
  ),
  store_agg AS (
    SELECT 
      ss_sold_date_sk,
      ss_item_sk,
      COUNT(*) AS cnt,
      SUM(ss_ext_sales_price) AS amt
    FROM store_sales
    WHERE ss_hdemo_sk IS NULL
    GROUP BY ss_sold_date_sk, ss_item_sk
  ),
  web_agg AS (
    SELECT 
      ws_sold_date_sk,
      ws_item_sk,
      COUNT(*) AS cnt,
      SUM(ws_ext_sales_price) AS amt
    FROM web_sales
    WHERE ws_bill_addr_sk IS NULL
    GROUP BY ws_sold_date_sk, ws_item_sk
  ),
  catalog_agg AS (
    SELECT 
      cs_sold_date_sk,
      cs_item_sk,
      COUNT(*) AS cnt,
      SUM(cs_ext_sales_price) AS amt
    FROM catalog_sales
    WHERE cs_warehouse_sk IS NULL
    GROUP BY cs_sold_date_sk, cs_item_sk
  ),
  combined AS (
    SELECT 
      'store' AS channel,
      'ss_hdemo_sk' AS col_name,
      d.d_year,
      d.d_qoy,
      i.i_category,
      s.cnt,
      s.amt
    FROM store_agg s
    JOIN date_dim_cte d ON s.ss_sold_date_sk = d.d_date_sk
    JOIN item_cte i ON s.ss_item_sk = i.i_item_sk
    UNION ALL
    SELECT 
      'web' AS channel,
      'ws_bill_addr_sk' AS col_name,
      d.d_year,
      d.d_qoy,
      i.i_category,
      w.cnt,
      w.amt
    FROM web_agg w
    JOIN date_dim_cte d ON w.ws_sold_date_sk = d.d_date_sk
    JOIN item_cte i ON w.ws_item_sk = i.i_item_sk
    UNION ALL
    SELECT 
      'catalog' AS channel,
      'cs_warehouse_sk' AS col_name,
      d.d_year,
      d.d_qoy,
      i.i_category,
      c.cnt,
      c.amt
    FROM catalog_agg c
    JOIN date_dim_cte d ON c.cs_sold_date_sk = d.d_date_sk
    JOIN item_cte i ON c.cs_item_sk = i.i_item_sk
  )
SELECT 
  channel,
  col_name,
  d_year,
  d_qoy,
  i_category,
  SUM(cnt) AS sales_cnt,
  SUM(amt) AS sales_amt
FROM combined
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