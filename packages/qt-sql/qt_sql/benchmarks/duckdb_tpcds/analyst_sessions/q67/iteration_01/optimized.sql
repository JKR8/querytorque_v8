WITH filtered_dates AS (
  SELECT d_date_sk, d_year, d_qoy, d_moy
  FROM date_dim
  WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
filtered_sales AS (
  SELECT 
    ss_item_sk,
    ss_store_sk,
    ss_sales_price,
    ss_quantity,
    d_year,
    d_qoy,
    d_moy
  FROM store_sales
  JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
),
base_aggregation AS (
  SELECT
    i.i_category,
    i.i_class,
    i.i_brand,
    i.i_product_name,
    f.d_year,
    f.d_qoy,
    f.d_moy,
    s.s_store_id,
    SUM(f.ss_sales_price * f.ss_quantity) AS sumsales
  FROM filtered_sales f
  JOIN item i ON f.ss_item_sk = i.i_item_sk
  JOIN store s ON f.ss_store_sk = s.s_store_sk
  GROUP BY 
    i.i_category,
    i.i_class,
    i.i_brand,
    i.i_product_name,
    f.d_year,
    f.d_qoy,
    f.d_moy,
    s.s_store_id
)
SELECT *
FROM (
  SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
  FROM base_aggregation
  GROUP BY ROLLUP (
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id
  )
) AS ranked
WHERE rk <= 100
ORDER BY
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id,
  sumsales,
  rk
LIMIT 100