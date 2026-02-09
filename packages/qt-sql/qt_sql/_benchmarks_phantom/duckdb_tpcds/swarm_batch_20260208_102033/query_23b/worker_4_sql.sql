WITH frequent_ss_items AS (
  SELECT
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    i_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year IN (2000, 2001, 2002, 2003)
  GROUP BY
    SUBSTRING(i_item_desc, 1, 30),
    i_item_sk,
    d_date
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
), filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 5
), frequent_items_set AS (
  SELECT DISTINCT item_sk
  FROM frequent_ss_items
), best_customers_set AS (
  SELECT c_customer_sk
  FROM best_ss_customer
), catalog_results AS (
  SELECT
    c_last_name,
    c_first_name,
    SUM(cs_quantity * cs_list_price) AS sales
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  JOIN customer ON cs_bill_customer_sk = c_customer_sk
  JOIN frequent_items_set ON cs_item_sk = item_sk
  JOIN best_customers_set ON cs_bill_customer_sk = c_customer_sk
  GROUP BY c_last_name, c_first_name
), web_results AS (
  SELECT
    c_last_name,
    c_first_name,
    SUM(ws_quantity * ws_list_price) AS sales
  FROM web_sales
  JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  JOIN customer ON ws_bill_customer_sk = c_customer_sk
  JOIN frequent_items_set ON ws_item_sk = item_sk
  JOIN best_customers_set ON ws_bill_customer_sk = c_customer_sk
  GROUP BY c_last_name, c_first_name
)
SELECT c_last_name, c_first_name, sales
FROM (
  SELECT * FROM catalog_results
  UNION ALL
  SELECT * FROM web_results
)
ORDER BY c_last_name, c_first_name, sales
LIMIT 100