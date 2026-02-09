WITH ssci AS (
  SELECT
    ss_customer_sk AS customer_sk,
    ss_item_sk AS item_sk
  FROM store_sales, date_dim
  WHERE
    ss_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1214 AND 1214 + 11
  GROUP BY
    ss_customer_sk,
    ss_item_sk
), csci AS (
  SELECT
    cs_bill_customer_sk AS customer_sk,
    cs_item_sk AS item_sk
  FROM catalog_sales, date_dim
  WHERE
    cs_sold_date_sk = d_date_sk AND d_month_seq BETWEEN 1214 AND 1214 + 11
  GROUP BY
    cs_bill_customer_sk,
    cs_item_sk
), all_pairs AS (
  SELECT customer_sk, item_sk, 1 AS store_flag, 0 AS catalog_flag FROM ssci
  UNION ALL
  SELECT customer_sk, item_sk, 0 AS store_flag, 1 AS catalog_flag FROM csci
), aggregated AS (
  SELECT
    customer_sk,
    item_sk,
    BOOL_OR(store_flag = 1) AS in_store,
    BOOL_OR(catalog_flag = 1) AS in_catalog
  FROM all_pairs
  GROUP BY customer_sk, item_sk
)
SELECT
  SUM(CASE WHEN in_store AND NOT in_catalog THEN 1 ELSE 0 END) AS store_only,
  SUM(CASE WHEN NOT in_store AND in_catalog THEN 1 ELSE 0 END) AS catalog_only,
  SUM(CASE WHEN in_store AND in_catalog THEN 1 ELSE 0 END) AS store_and_catalog
FROM aggregated
LIMIT 100