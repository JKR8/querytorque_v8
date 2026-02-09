WITH store_sales_base AS (
  SELECT
    ss_item_sk,
    ss_customer_sk,
    d_date,
    i_item_desc,
    ss_quantity * ss_sales_price AS sales_amount
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  JOIN item ON ss_item_sk = i_item_sk
  WHERE d_year IN (2000, 2001, 2002, 2003)
),
frequent_ss_items AS (
  SELECT
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    ss_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales_base
  GROUP BY
    SUBSTRING(i_item_desc, 1, 30),
    ss_item_sk,
    d_date
  HAVING COUNT(*) > 4
),
max_store_sales AS (
  SELECT MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      ss_customer_sk,
      SUM(sales_amount) AS csales
    FROM store_sales_base
    GROUP BY ss_customer_sk
  )
),
best_ss_customer AS (
  SELECT
    ss_customer_sk AS c_customer_sk,
    SUM(ss_quantity * ss_sales_price) AS ssales
  FROM store_sales
  GROUP BY ss_customer_sk
  HAVING SUM(ss_quantity * ss_sales_price) > (
    95 / 100.0 * (SELECT tpcds_cmax FROM max_store_sales)
  )
),
frequent_items AS (
  SELECT DISTINCT item_sk FROM frequent_ss_items
),
best_customers AS (
  SELECT DISTINCT c_customer_sk FROM best_ss_customer
),
may2000_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 AND d_moy = 5
)
SELECT SUM(sales) AS "SUM(sales)"
FROM (
  SELECT cs_quantity * cs_list_price AS sales
  FROM catalog_sales
  JOIN may2000_dates ON cs_sold_date_sk = d_date_sk
  JOIN frequent_items ON cs_item_sk = frequent_items.item_sk
  JOIN best_customers ON cs_bill_customer_sk = best_customers.c_customer_sk
  UNION ALL
  SELECT ws_quantity * ws_list_price AS sales
  FROM web_sales
  JOIN may2000_dates ON ws_sold_date_sk = d_date_sk
  JOIN frequent_items ON ws_item_sk = frequent_items.item_sk
  JOIN best_customers ON ws_bill_customer_sk = best_customers.c_customer_sk
)
LIMIT 100