WITH combined_store_sales AS (
  SELECT
    ss_item_sk,
    ss_customer_sk,
    d_date,
    d_year,
    i_item_desc,
    ss_quantity,
    ss_sales_price,
    CASE WHEN d_year IN (2000, 2001, 2002, 2003) THEN 1 ELSE 0 END AS is_frequent_period,
    CASE
      WHEN d_year IN (2000, 2001, 2002, 2003)
      THEN ss_quantity * ss_sales_price
      ELSE 0
    END AS csales_value,
    ss_quantity * ss_sales_price AS ssales_value
  FROM store_sales
  LEFT JOIN date_dim
    ON ss_sold_date_sk = d_date_sk
  LEFT JOIN item
    ON ss_item_sk = i_item_sk
  LEFT JOIN customer
    ON ss_customer_sk = c_customer_sk
  WHERE
    d_year IN (2000, 2001, 2002, 2003) OR d_year IS NULL
), frequent_ss_items AS (
  SELECT
    SUBSTRING(i_item_desc, 1, 30) AS itemdesc,
    ss_item_sk AS item_sk,
    d_date AS solddate,
    COUNT(*) AS cnt
  FROM combined_store_sales
  WHERE
    is_frequent_period = 1
  GROUP BY
    SUBSTRING(i_item_desc, 1, 30),
    ss_item_sk,
    d_date
  HAVING
    COUNT(*) > 4
), customer_sales AS (
  SELECT
    ss_customer_sk,
    SUM(csales_value) AS csales,
    SUM(ssales_value) AS ssales
  FROM combined_store_sales
  GROUP BY
    ss_customer_sk
), max_store_sales AS (
  SELECT
    MAX(csales) AS tpcds_cmax
  FROM customer_sales
), best_ss_customer AS (
  SELECT
    ss_customer_sk AS c_customer_sk,
    ssales
  FROM customer_sales
  WHERE
    ssales > (
      95 / 100.0
    ) * (
      SELECT
        tpcds_cmax
      FROM max_store_sales
    )
), frequent_item_sk AS (
  SELECT
    item_sk
  FROM frequent_ss_items
), best_customer_sk AS (
  SELECT
    c_customer_sk
  FROM best_ss_customer
)
SELECT
  SUM(sales)
FROM (
  SELECT
    cs_quantity * cs_list_price AS sales
  FROM catalog_sales, date_dim
  WHERE
    d_year = 2000
    AND d_moy = 5
    AND cs_sold_date_sk = d_date_sk
    AND cs_item_sk IN (
      SELECT
        item_sk
      FROM frequent_item_sk
    )
    AND cs_bill_customer_sk IN (
      SELECT
        c_customer_sk
      FROM best_customer_sk
    )
  UNION ALL
  SELECT
    ws_quantity * ws_list_price AS sales
  FROM web_sales, date_dim
  WHERE
    d_year = 2000
    AND d_moy = 5
    AND ws_sold_date_sk = d_date_sk
    AND ws_item_sk IN (
      SELECT
        item_sk
      FROM frequent_item_sk
    )
    AND ws_bill_customer_sk IN (
      SELECT
        c_customer_sk
      FROM best_customer_sk
    )
)
LIMIT 100