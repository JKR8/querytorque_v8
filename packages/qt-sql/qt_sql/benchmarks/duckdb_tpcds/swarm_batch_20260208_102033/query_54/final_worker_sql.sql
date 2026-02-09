WITH may_dates AS (
  SELECT d_date_sk, d_month_seq
  FROM date_dim
  WHERE d_year = 1998 AND d_moy = 5
),
three_month_range AS (
  SELECT (d_month_seq + 1) AS start_seq, (d_month_seq + 3) AS end_seq
  FROM may_dates
),
filtered_customers AS (
  SELECT DISTINCT
    c.c_customer_sk,
    c.c_current_addr_sk
  FROM (
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
  ) sales
  JOIN may_dates ON sales.sold_date_sk = may_dates.d_date_sk
  JOIN item i ON sales.item_sk = i.i_item_sk
  JOIN customer c ON sales.customer_sk = c.c_customer_sk
  WHERE i.i_category = 'Women'
    AND i.i_class = 'maternity'
),
customer_revenue AS (
  SELECT
    fc.c_customer_sk,
    SUM(ss_ext_sales_price) AS revenue
  FROM filtered_customers fc
  JOIN customer_address ca ON fc.c_current_addr_sk = ca.ca_address_sk
  JOIN store s ON ca.ca_county = s.s_county AND ca.ca_state = s.s_state
  JOIN store_sales ss ON fc.c_customer_sk = ss.ss_customer_sk
  JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
  CROSS JOIN three_month_range tmr
  WHERE d.d_month_seq BETWEEN tmr.start_seq AND tmr.end_seq
  GROUP BY fc.c_customer_sk
),
segments AS (
  SELECT CAST((revenue / 50) AS INT) AS segment
  FROM customer_revenue
)
SELECT
  segment,
  COUNT(*) AS num_customers,
  segment * 50 AS segment_base
FROM segments
GROUP BY segment
ORDER BY segment, num_customers
LIMIT 100