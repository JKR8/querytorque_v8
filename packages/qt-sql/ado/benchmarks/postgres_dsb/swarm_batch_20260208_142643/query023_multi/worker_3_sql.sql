WITH filtered_date_1998 AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998
),
filtered_date_1998_m3 AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998 AND d_moy = 3
),
filtered_item AS (
  SELECT i_item_sk, SUBSTRING(i_item_desc FROM 1 FOR 30) AS itemdesc
  FROM item
  WHERE i_manager_id BETWEEN 81 AND 100
    AND i_category IN ('Home', 'Jewelry', 'Music')
),
frequent_ss_items AS (
  SELECT
    fi.itemdesc,
    ss.ss_item_sk AS item_sk,
    d.d_date AS solddate,
    COUNT(*) AS cnt
  FROM store_sales ss
  INNER JOIN filtered_date_1998 fd ON ss.ss_sold_date_sk = fd.d_date_sk
  INNER JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
  INNER JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  GROUP BY fi.itemdesc, ss.ss_item_sk, d.d_date
  HAVING COUNT(*) > 4
),
frequent_items_sk AS (
  SELECT DISTINCT item_sk
  FROM frequent_ss_items
),
store_sales_filtered AS (
  SELECT ss_customer_sk, ss_quantity, ss_sales_price
  FROM store_sales
  INNER JOIN filtered_date_1998 fd ON ss_sold_date_sk = fd.d_date_sk
  WHERE ss_wholesale_cost BETWEEN 26 AND 36
),
max_store_sales AS (
  SELECT
    MAX(csales) AS tpcds_cmax
  FROM (
    SELECT
      c_customer_sk,
      SUM(ss.ss_quantity * ss.ss_sales_price) AS csales
    FROM store_sales_filtered ss
    INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
    GROUP BY c_customer_sk
  ) AS tmp1
),
store_sales_all AS (
  SELECT ss_customer_sk, ss_quantity, ss_sales_price
  FROM store_sales
),
best_ss_customer AS (
  SELECT
    c_customer_sk,
    SUM(ss.ss_quantity * ss.ss_sales_price) AS ssales
  FROM store_sales_all ss
  INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk
  WHERE c_birth_year BETWEEN 1927 AND 1933
  GROUP BY c_customer_sk
  HAVING SUM(ss.ss_quantity * ss.ss_sales_price) > (
    95 / 100.0 * (SELECT tpcds_cmax FROM max_store_sales)
  )
)
SELECT
  SUM(sales)
FROM (
  SELECT
    cs_quantity * cs_list_price AS sales
  FROM catalog_sales cs
  INNER JOIN filtered_date_1998_m3 fd ON cs.cs_sold_date_sk = fd.d_date_sk
  INNER JOIN frequent_items_sk fis ON cs.cs_item_sk = fis.item_sk
  INNER JOIN best_ss_customer bsc ON cs.cs_bill_customer_sk = bsc.c_customer_sk
  WHERE cs_wholesale_cost BETWEEN 26 AND 36
  UNION ALL
  SELECT
    ws_quantity * ws_list_price AS sales
  FROM web_sales ws
  INNER JOIN filtered_date_1998_m3 fd ON ws.ws_sold_date_sk = fd.d_date_sk
  INNER JOIN frequent_items_sk fis ON ws.ws_item_sk = fis.item_sk
  INNER JOIN best_ss_customer bsc ON ws.ws_bill_customer_sk = bsc.c_customer_sk
  WHERE ws_wholesale_cost BETWEEN 26 AND 36
) AS tmp2
LIMIT 100;