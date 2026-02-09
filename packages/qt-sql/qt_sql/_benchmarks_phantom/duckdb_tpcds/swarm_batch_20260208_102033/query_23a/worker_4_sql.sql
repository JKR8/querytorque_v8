WITH frequent_ss_items AS (
  SELECT
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    i_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
  GROUP BY SUBSTRING(i_item_desc, 1, 30), i_item_sk, d_date
  HAVING COUNT(*) > 4
),
max_store_sales AS (
  SELECT MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM store_sales
    JOIN customer ON ss_customer_sk = c_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year IN (2000, 2000 + 1, 2000 + 2, 2000 + 3)
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
catalog_sales_filtered AS (
  SELECT cs_quantity * cs_list_price AS sales
  FROM catalog_sales
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND EXISTS (SELECT 1 FROM frequent_ss_items WHERE item_sk = cs_item_sk)
    AND EXISTS (SELECT 1 FROM best_ss_customer WHERE c_customer_sk = cs_bill_customer_sk)
),
web_sales_filtered AS (
  SELECT ws_quantity * ws_list_price AS sales
  FROM web_sales
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2000
    AND d_moy = 5
    AND EXISTS (SELECT 1 FROM frequent_ss_items WHERE item_sk = ws_item_sk)
    AND EXISTS (SELECT 1 FROM best_ss_customer WHERE c_customer_sk = ws_bill_customer_sk)
)
SELECT SUM(sales)
FROM (
  SELECT sales FROM catalog_sales_filtered
  UNION ALL
  SELECT sales FROM web_sales_filtered
)
LIMIT 100