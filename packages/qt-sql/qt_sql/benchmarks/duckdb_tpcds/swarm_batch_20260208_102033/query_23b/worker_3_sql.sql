WITH filtered_dates_2000_2003 AS (
  SELECT d_date_sk, d_date
  FROM date_dim
  WHERE d_year IN (2000, 2001, 2002, 2003)
),
filtered_dates_2000_05 AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 5
),
frequent_ss_items AS (
  SELECT
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    i_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN filtered_dates_2000_2003 ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  GROUP BY
    SUBSTRING(i_item_desc, 1, 30),
    i_item_sk,
    d_date
  HAVING COUNT(*) > 4
),
max_store_sales AS (
  SELECT MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM store_sales
    JOIN filtered_dates_2000_2003 ON ss_sold_date_sk = d_date_sk
    JOIN customer ON ss_customer_sk = c_customer_sk
    GROUP BY c_customer_sk
  )
),
best_ss_customer AS (
  SELECT
    c_customer_sk,
    SUM(ss_quantity * ss_sales_price) AS ssales
  FROM store_sales
  JOIN customer ON ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING SUM(ss_quantity * ss_sales_price) > (95 / 100.0) * (SELECT tpcds_cmax FROM max_store_sales)
),
catalog_sales_prejoined AS (
  SELECT
    cs_bill_customer_sk,
    cs_item_sk,
    cs_quantity,
    cs_list_price
  FROM catalog_sales
  JOIN filtered_dates_2000_05 ON cs_sold_date_sk = d_date_sk
),
web_sales_prejoined AS (
  SELECT
    ws_bill_customer_sk,
    ws_item_sk,
    ws_quantity,
    ws_list_price
  FROM web_sales
  JOIN filtered_dates_2000_05 ON ws_sold_date_sk = d_date_sk
)
SELECT
  c_last_name,
  c_first_name,
  sales
FROM (
  SELECT
    c_last_name,
    c_first_name,
    SUM(cs_quantity * cs_list_price) AS sales
  FROM catalog_sales_prejoined cs
  JOIN customer ON cs.cs_bill_customer_sk = c_customer_sk
  WHERE cs.cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND cs.cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
  GROUP BY c_last_name, c_first_name
  
  UNION ALL
  
  SELECT
    c_last_name,
    c_first_name,
    SUM(ws_quantity * ws_list_price) AS sales
  FROM web_sales_prejoined ws
  JOIN customer ON ws.ws_bill_customer_sk = c_customer_sk
  WHERE ws.ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND ws.ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
  GROUP BY c_last_name, c_first_name
)
ORDER BY
  c_last_name,
  c_first_name,
  sales
LIMIT 100