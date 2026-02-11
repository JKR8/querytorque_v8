WITH filtered_dates AS (
  SELECT d_date_sk, d_year, d_qoy, d_moy
  FROM date_dim
  WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
filtered_sales AS (
  SELECT 
    ss_sales_price,
    ss_quantity,
    ss_item_sk,
    ss_store_sk,
    d.d_year,
    d.d_qoy,
    d.d_moy
  FROM store_sales ss
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
),
item_dim AS (
  SELECT 
    i_item_sk,
    i_category,
    i_class,
    i_brand,
    i_product_name
  FROM item
),
store_dim AS (
  SELECT 
    s_store_sk,
    s_store_id
  FROM store
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
  FROM (
    SELECT
      i.i_category,
      i.i_class,
      i.i_brand,
      i.i_product_name,
      fs.d_year,
      fs.d_qoy,
      fs.d_moy,
      s.s_store_id,
      SUM(COALESCE(fs.ss_sales_price * fs.ss_quantity, 0)) AS sumsales
    FROM filtered_sales fs
    JOIN item_dim i ON fs.ss_item_sk = i.i_item_sk
    JOIN store_dim s ON fs.ss_store_sk = s.s_store_sk
    GROUP BY ROLLUP (
      i.i_category,
      i.i_class,
      i.i_brand,
      i.i_product_name,
      fs.d_year,
      fs.d_qoy,
      fs.d_moy,
      s.s_store_id
    )
  ) AS dw1
) AS dw2
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