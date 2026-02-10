WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 5
), frequent_ss_items AS (
  SELECT i_item_sk AS item_sk
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year IN (2000, 2001, 2002, 2003)
  GROUP BY i_item_sk, d_date
  HAVING COUNT(*) > 4
), max_store_sales AS (
  SELECT MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss_quantity * ss_sales_price) AS csales
    FROM store_sales
    JOIN customer ON ss_customer_sk = c_customer_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year IN (2000, 2001, 2002, 2003)
    GROUP BY c_customer_sk
  )
), best_ss_customer AS (
  SELECT
    c_customer_sk,
    SUM(ss_quantity * ss_sales_price) AS ssales
  FROM store_sales
  JOIN customer ON ss_customer_sk = c_customer_sk
  GROUP BY c_customer_sk
  HAVING SUM(ss_quantity * ss_sales_price) > (
    95 / 100.0 * (SELECT tpcds_cmax FROM max_store_sales)
  )
)
SELECT SUM(sales)
FROM (
  SELECT cs_quantity * cs_list_price AS sales
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  WHERE cs_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND cs_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
  UNION ALL
  SELECT ws_quantity * ws_list_price AS sales
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  WHERE ws_item_sk IN (SELECT item_sk FROM frequent_ss_items)
    AND ws_bill_customer_sk IN (SELECT c_customer_sk FROM best_ss_customer)
)
LIMIT 100