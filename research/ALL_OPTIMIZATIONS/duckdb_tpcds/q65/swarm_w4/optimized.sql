WITH store_item_revenue AS (
  SELECT
    ss_store_sk,
    ss_item_sk,
    SUM(ss_sales_price) AS revenue
  FROM store_sales
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_month_seq BETWEEN 1221 AND 1221 + 11
  GROUP BY ss_store_sk, ss_item_sk
),
store_avg_revenue AS (
  SELECT
    ss_store_sk,
    AVG(revenue) AS ave
  FROM store_item_revenue
  GROUP BY ss_store_sk
)
SELECT
  s_store_name,
  i_item_desc,
  sir.revenue,
  i_current_price,
  i_wholesale_cost,
  i_brand
FROM store_item_revenue sir
JOIN store_avg_revenue sar ON sir.ss_store_sk = sar.ss_store_sk
JOIN store ON s_store_sk = sir.ss_store_sk
JOIN item ON i_item_sk = sir.ss_item_sk
WHERE sir.revenue <= 0.1 * sar.ave
ORDER BY s_store_name, i_item_desc
LIMIT 100