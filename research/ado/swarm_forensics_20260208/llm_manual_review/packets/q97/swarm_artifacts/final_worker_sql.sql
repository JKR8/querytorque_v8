WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1214 AND 1214 + 11
),
combined_sales AS (
  -- Store sales with channel indicator
  SELECT 
    ss_customer_sk AS customer_sk,
    ss_item_sk AS item_sk,
    1 AS store_flag,
    0 AS catalog_flag
  FROM store_sales
  JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
  GROUP BY ss_customer_sk, ss_item_sk
  
  UNION ALL
  
  -- Catalog sales with channel indicator
  SELECT 
    cs_bill_customer_sk AS customer_sk,
    cs_item_sk AS item_sk,
    0 AS store_flag,
    1 AS catalog_flag
  FROM catalog_sales
  JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
  GROUP BY cs_bill_customer_sk, cs_item_sk
),
customer_item_flags AS (
  SELECT 
    customer_sk,
    item_sk,
    MAX(store_flag) AS has_store,
    MAX(catalog_flag) AS has_catalog
  FROM combined_sales
  GROUP BY customer_sk, item_sk
)
SELECT
  COUNT(*) FILTER (WHERE has_store = 1 AND has_catalog = 0) AS store_only,
  COUNT(*) FILTER (WHERE has_store = 0 AND has_catalog = 1) AS catalog_only,
  COUNT(*) FILTER (WHERE has_store = 1 AND has_catalog = 1) AS store_and_catalog
FROM customer_item_flags
LIMIT 100