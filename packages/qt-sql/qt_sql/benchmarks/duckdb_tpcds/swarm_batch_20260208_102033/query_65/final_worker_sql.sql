WITH date_filter AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_month_seq BETWEEN 1221 AND 1221 + 11
),

filtered_sales AS (
  SELECT
    ss_store_sk,
    ss_item_sk,
    SUM(ss_sales_price) AS revenue
  FROM store_sales
  JOIN date_filter ON ss_sold_date_sk = d_date_sk
  GROUP BY ss_store_sk, ss_item_sk
),

store_averages AS (
  SELECT
    ss_store_sk,
    AVG(revenue) AS ave
  FROM filtered_sales
  GROUP BY ss_store_sk
),

low_revenue_items AS (
  SELECT
    fs.ss_store_sk,
    fs.ss_item_sk,
    fs.revenue
  FROM filtered_sales fs
  JOIN store_averages sa ON fs.ss_store_sk = sa.ss_store_sk
  WHERE fs.revenue <= 0.1 * sa.ave
)

SELECT
  s.s_store_name,
  i.i_item_desc,
  lri.revenue,
  i.i_current_price,
  i.i_wholesale_cost,
  i.i_brand
FROM low_revenue_items lri
JOIN store s ON lri.ss_store_sk = s.s_store_sk
JOIN item i ON lri.ss_item_sk = i.i_item_sk
ORDER BY
  s.s_store_name,
  i.i_item_desc
LIMIT 100;