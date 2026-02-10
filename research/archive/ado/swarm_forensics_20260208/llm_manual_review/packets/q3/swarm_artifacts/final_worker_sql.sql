WITH filtered_date AS (
  SELECT d_date_sk, d_year
  FROM date_dim
  WHERE d_moy = 11
),
filtered_item AS (
  SELECT i_item_sk, i_brand_id, i_brand
  FROM item
  WHERE i_manufact_id = 816
),
sales_agg AS (
  SELECT 
    ss.ss_sold_date_sk,
    ss.ss_item_sk,
    SUM(ss.ss_sales_price) AS sum_sales
  FROM store_sales ss
  GROUP BY ss.ss_sold_date_sk, ss.ss_item_sk
)
SELECT
  dt.d_year,
  it.i_brand_id AS brand_id,
  it.i_brand AS brand,
  SUM(sa.sum_sales) AS sum_agg
FROM filtered_date dt
JOIN sales_agg sa ON dt.d_date_sk = sa.ss_sold_date_sk
JOIN filtered_item it ON sa.ss_item_sk = it.i_item_sk
GROUP BY dt.d_year, it.i_brand, it.i_brand_id
ORDER BY dt.d_year, sum_agg DESC, brand_id
LIMIT 100