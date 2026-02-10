WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1213 AND 1213 + 11
),
store_sales_filtered AS (
  SELECT
    ss_store_sk,
    ss_item_sk,
    SUM(ss_sales_price) AS revenue,
    AVG(SUM(ss_sales_price)) OVER (PARTITION BY ss_store_sk) AS store_avg_revenue
  FROM store_sales
  JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
  WHERE ss_sales_price / ss_list_price BETWEEN 38 * 0.01 AND 48 * 0.01
  GROUP BY ss_store_sk, ss_item_sk
),
filtered_store AS (
  SELECT s_store_sk, s_store_name
  FROM store
  WHERE s_state IN ('TN', 'TX', 'VA')
),
filtered_item AS (
  SELECT i_item_sk, i_item_desc, i_current_price, i_wholesale_cost, i_brand
  FROM item
  WHERE i_manager_id BETWEEN 32 AND 36
)
SELECT
  s.s_store_name,
  i.i_item_desc,
  ss.revenue,
  i.i_current_price,
  i.i_wholesale_cost,
  i.i_brand
FROM store_sales_filtered ss
JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
WHERE ss.revenue <= ss.store_avg_revenue * 0.1
ORDER BY
  s.s_store_name,
  i.i_item_desc
LIMIT 100;
