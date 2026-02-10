WITH may_1998_dates AS (
  SELECT DISTINCT d_date_sk, d_month_seq
  FROM date_dim
  WHERE d_year = 1998 AND d_moy = 5
),
women_maternity_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_category = 'Women' AND i_class = 'maternity'
),
cs_or_ws_sales AS (
  SELECT
    cs_sold_date_sk AS sold_date_sk,
    cs_bill_customer_sk AS customer_sk,
    cs_item_sk AS item_sk
  FROM catalog_sales
  UNION ALL
  SELECT
    ws_sold_date_sk AS sold_date_sk,
    ws_bill_customer_sk AS customer_sk,
    ws_item_sk AS item_sk
  FROM web_sales
),
my_customers AS (
  SELECT DISTINCT
    c_customer_sk,
    c_current_addr_sk
  FROM cs_or_ws_sales
  JOIN may_1998_dates ON sold_date_sk = d_date_sk
  JOIN women_maternity_items ON item_sk = i_item_sk
  JOIN customer ON customer_sk = c_customer_sk
),
month_seq_range AS (
  SELECT
    d_month_seq + 1 AS start_seq,
    d_month_seq + 3 AS end_seq
  FROM may_1998_dates
  LIMIT 1
),
store_sales_dates AS (
  SELECT d_date_sk
  FROM date_dim
  CROSS JOIN month_seq_range
  WHERE d_month_seq BETWEEN start_seq AND end_seq
),
my_revenue AS (
  SELECT
    c_customer_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM my_customers
  JOIN customer_address ON c_current_addr_sk = ca_address_sk
  JOIN store ON ca_county = s_county AND ca_state = s_state
  JOIN store_sales ON c_customer_sk = ss_customer_sk
  JOIN store_sales_dates ON ss_sold_date_sk = d_date_sk
  GROUP BY c_customer_sk
),
segments AS (
  SELECT CAST((revenue / 50) AS INT) AS segment
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