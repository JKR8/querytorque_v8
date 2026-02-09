WITH 
date_dim_years AS (
  SELECT d_date_sk
  FROM date_dim 
  WHERE d_year IN (2000, 2001, 2002, 2003)
),
date_dim_may2000 AS (
  SELECT d_date_sk
  FROM date_dim 
  WHERE d_year = 2000 AND d_moy = 5
),
store_sales_filtered AS (
  SELECT 
    ss_item_sk,
    ss_customer_sk,
    ss_quantity,
    ss_sales_price,
    d_date_sk
  FROM store_sales
  JOIN date_dim_years ON ss_sold_date_sk = d_date_sk
),
frequent_ss_items AS (
  SELECT
    i_item_sk AS item_sk,
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales_filtered
  JOIN item ON ss_item_sk = i_item_sk
  GROUP BY
    i_item_sk,
    SUBSTRING(i_item_desc, 1, 30),
    d_date
  HAVING COUNT(*) > 4
),
frequent_ss_items_sk AS (
  SELECT DISTINCT item_sk
  FROM frequent_ss_items
),
max_store_sales AS (
  SELECT
    MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM store_sales_filtered
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
  HAVING SUM(ss_quantity * ss_sales_price) > (
    95 / 100.0
  ) * (SELECT tpcds_cmax FROM max_store_sales)
),
catalog_sales_may AS (
  SELECT cs_quantity * cs_list_price AS sales
  FROM catalog_sales
  JOIN date_dim_may2000 ON cs_sold_date_sk = d_date_sk
  JOIN frequent_ss_items_sk ON cs_item_sk = item_sk
  JOIN best_ss_customer ON cs_bill_customer_sk = c_customer_sk
),
web_sales_may AS (
  SELECT ws_quantity * ws_list_price AS sales
  FROM web_sales
  JOIN date_dim_may2000 ON ws_sold_date_sk = d_date_sk
  JOIN frequent_ss_items_sk ON ws_item_sk = item_sk
  JOIN best_ss_customer ON ws_bill_customer_sk = c_customer_sk
)
SELECT SUM(sales)
FROM (
  SELECT sales FROM catalog_sales_may
  UNION ALL
  SELECT sales FROM web_sales_may
)
LIMIT 100;