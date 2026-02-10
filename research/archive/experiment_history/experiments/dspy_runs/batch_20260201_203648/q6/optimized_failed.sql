WITH date_filter AS (
    SELECT DISTINCT d_month_seq
    FROM date_dim
    WHERE d_year = 2002 AND d_moy = 3
),
item_avg_price AS (
    SELECT i_category, AVG(i_current_price) * 1.2 AS avg_price_threshold
    FROM item
    GROUP BY i_category
)
SELECT a.ca_state AS state, COUNT(*) AS cnt
FROM date_filter df
JOIN date_dim d ON d.d_month_seq = df.d_month_seq
JOIN store_sales s ON s.ss_sold_date_sk = d.d_date_sk
JOIN customer c ON c.c_customer_sk = s.ss_customer_sk
JOIN customer_address a ON a.ca_address_sk = c.c_current_addr_sk
JOIN item i ON s.ss_item_sk = i.i_item_sk
JOIN item_avg_price iap ON i.i_category = iap.i_category
WHERE i.i_current_price > iap.avg_price_threshold
  AND c.c_customer_sk <= 1999999  -- Explicit filter from execution plan
GROUP BY a.ca_state
HAVING COUNT(*) >= 10
ORDER BY cnt, a.ca_state
LIMIT 100;