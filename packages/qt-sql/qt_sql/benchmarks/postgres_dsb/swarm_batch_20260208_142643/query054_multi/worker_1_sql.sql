WITH date_range AS (
  SELECT 
    d_month_seq + 1 AS start_month_seq,
    d_month_seq + 3 AS end_month_seq
  FROM date_dim 
  WHERE d_year = 1998 AND d_moy = 11
  LIMIT 1
),
filtered_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_category = 'Books' 
    AND i_class = 'fiction'
),
filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998 AND d_moy = 11
),
my_customers AS (
  SELECT DISTINCT
    c_customer_sk,
    c_current_addr_sk
  FROM (
    SELECT
      cs_sold_date_sk AS sold_date_sk,
      cs_bill_customer_sk AS customer_sk,
      cs_item_sk AS item_sk
    FROM catalog_sales
    WHERE cs_wholesale_cost BETWEEN 70 AND 100
    UNION ALL
    SELECT
      ws_sold_date_sk AS sold_date_sk,
      ws_bill_customer_sk AS customer_sk,
      ws_item_sk AS item_sk
    FROM web_sales
    WHERE ws_wholesale_cost BETWEEN 70 AND 100
  ) AS cs_or_ws_sales
  INNER JOIN filtered_items ON cs_or_ws_sales.item_sk = filtered_items.i_item_sk
  INNER JOIN filtered_dates ON cs_or_ws_sales.sold_date_sk = filtered_dates.d_date_sk
  INNER JOIN customer ON cs_or_ws_sales.customer_sk = customer.c_customer_sk
  WHERE customer.c_birth_year BETWEEN 1993 AND 2006
),
my_revenue AS (
  SELECT
    my_customers.c_customer_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM my_customers
  INNER JOIN store_sales ON my_customers.c_customer_sk = store_sales.ss_customer_sk
  INNER JOIN customer_address ON my_customers.c_current_addr_sk = customer_address.ca_address_sk
  INNER JOIN store ON customer_address.ca_county = store.s_county 
    AND customer_address.ca_state = store.s_state
  INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
  CROSS JOIN date_range
  WHERE store_sales.ss_wholesale_cost BETWEEN 70 AND 100
    AND store.s_state IN ('GA', 'IA', 'LA', 'MO', 'OH', 'PA', 'SD', 'TN', 'TX', 'VA')
    AND date_dim.d_month_seq BETWEEN date_range.start_month_seq AND date_range.end_month_seq
  GROUP BY my_customers.c_customer_sk
),
segments AS (
  SELECT
    CAST((revenue / 50) AS INT) AS segment
  FROM my_revenue
)
SELECT
  segment,
  COUNT(*) AS num_customers,
  segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100