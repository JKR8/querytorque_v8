WITH filtered_dates AS (
  SELECT d_date_sk, d_year, d_qoy, d_moy
  FROM date_dim
  WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
base_aggregates AS (
  SELECT
    i.i_category,
    i.i_class,
    i.i_brand,
    i.i_product_name,
    d.d_year,
    d.d_qoy,
    d.d_moy,
    s.s_store_id,
    SUM(ss.ss_sales_price * ss.ss_quantity) AS sumsales
  FROM store_sales ss
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  GROUP BY
    i.i_category,
    i.i_class,
    i.i_brand,
    i.i_product_name,
    d.d_year,
    d.d_qoy,
    d.d_moy,
    s.s_store_id
),
ranked_aggregates AS (
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
  FROM base_aggregates
)
SELECT
  i_category,
  i_class,
  i_brand,
  i_product_name,
  d_year,
  d_qoy,
  d_moy,
  s_store_id,
  SUM(sumsales) AS sumsales,
  MIN(rk) AS rk
FROM ranked_aggregates
WHERE rk <= 100
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