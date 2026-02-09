WITH filtered_sales AS (
  SELECT 
    ss_item_sk,
    ss_store_sk,
    ss_sales_price,
    ss_quantity
  FROM store_sales
  WHERE ss_sold_date_sk IN (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
  )
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
  sumsales,
  rk
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
      COALESCE(i_category, 'ALL') AS i_category,
      COALESCE(i_class, 'ALL') AS i_class,
      COALESCE(i_brand, 'ALL') AS i_brand,
      COALESCE(i_product_name, 'ALL') AS i_product_name,
      d_year,
      d_qoy,
      d_moy,
      s_store_id,
      SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales
    FROM filtered_sales
    JOIN item ON ss_item_sk = i_item_sk
    JOIN store ON ss_store_sk = s_store_sk
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
    GROUP BY
      ROLLUP (
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id
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
LIMIT 100;