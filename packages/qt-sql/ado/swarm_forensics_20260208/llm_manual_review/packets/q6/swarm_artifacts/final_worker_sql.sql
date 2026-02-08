WITH target_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq = (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2002 AND d_moy = 3
  )
),
category_avg_price AS (
  SELECT
    i_category,
    AVG(i_current_price) * 1.2 AS price_threshold
  FROM item
  GROUP BY i_category
),
filtered_sales AS (
  SELECT
    s.ss_customer_sk,
    i.i_item_sk
  FROM store_sales s
  JOIN target_dates d ON s.ss_sold_date_sk = d.d_date_sk
  JOIN item i ON s.ss_item_sk = i.i_item_sk
  JOIN category_avg_price cap ON i.i_category = cap.i_category
  WHERE i.i_current_price > cap.price_threshold
)
SELECT
  a.ca_state AS state,
  COUNT(*) AS cnt
FROM customer_address a
JOIN customer c ON a.ca_address_sk = c.c_current_addr_sk
JOIN filtered_sales fs ON c.c_customer_sk = fs.ss_customer_sk
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100;