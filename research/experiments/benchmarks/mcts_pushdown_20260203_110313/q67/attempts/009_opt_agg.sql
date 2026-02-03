/* start query 67 in stream 0 using template query67.tpl */
WITH sales_aggregation AS (
  SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sumsales,
    RANK() OVER (
      PARTITION BY i_category
      ORDER BY SUM(COALESCE(ss_sales_price * ss_quantity, 0)) DESC
    ) AS rk
  FROM store_sales, date_dim, store, item
  WHERE
    ss_sold_date_sk = d_date_sk
    AND ss_item_sk = i_item_sk
    AND ss_store_sk = s_store_sk
    AND d_month_seq BETWEEN 1206 AND 1206 + 11
  GROUP BY
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id
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
FROM sales_aggregation
WHERE
  rk <= 100
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