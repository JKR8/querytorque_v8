WITH date_filter AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1221 AND 1221 + 11
),
store_item_revenue AS (
  SELECT
    ss_store_sk,
    ss_item_sk,
    SUM(ss_sales_price) AS revenue,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY ss_store_sk) AS store_avg
  FROM store_sales
  JOIN date_filter ON store_sales.ss_sold_date_sk = date_filter.d_date_sk
  GROUP BY ss_store_sk, ss_item_sk
)
SELECT
  s.s_store_name,
  i.i_item_desc,
  sir.revenue,
  i.i_current_price,
  i.i_wholesale_cost,
  i.i_brand
FROM store_item_revenue sir
JOIN store s ON s.s_store_sk = sir.ss_store_sk
JOIN item i ON i.i_item_sk = sir.ss_item_sk
WHERE sir.revenue <= 0.1 * sir.store_avg
ORDER BY s.s_store_name, i.i_item_desc
LIMIT 100;