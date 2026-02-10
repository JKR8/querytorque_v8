WITH filtered_dates AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_month_seq BETWEEN 1188 AND 1188 + 11
)
SELECT
  i_product_name,
  i_brand,
  i_class,
  i_category,
  AVG(inv_quantity_on_hand) AS qoh
FROM inventory, filtered_dates, item
WHERE
  inv_date_sk = d_date_sk AND inv_item_sk = i_item_sk
GROUP BY
  ROLLUP (
    i_product_name,
    i_brand,
    i_class,
    i_category
  )
ORDER BY
  qoh,
  i_product_name,
  i_brand,
  i_class,
  i_category
LIMIT 100