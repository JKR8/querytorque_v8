WITH filtered_item AS (
  SELECT
    i_item_sk,
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
  FROM item
  WHERE i_category IN ('Sports', 'Music', 'Shoes')
),
filtered_date AS (
  SELECT
    d_date_sk
  FROM date_dim
  WHERE
    d_date BETWEEN CAST('2002-05-20' AS DATE)
    AND (CAST('2002-05-20' AS DATE) + INTERVAL '30' DAY)
)
SELECT
  i.i_item_id,
  i.i_item_desc,
  i.i_category,
  i.i_class,
  i.i_current_price,
  SUM(ss.ss_ext_sales_price) AS itemrevenue,
  SUM(ss.ss_ext_sales_price) * 100 / SUM(SUM(ss.ss_ext_sales_price)) OVER (PARTITION BY i.i_class) AS revenueratio
FROM store_sales ss
JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
GROUP BY
  i.i_item_id,
  i.i_item_desc,
  i.i_category,
  i.i_class,
  i.i_current_price
ORDER BY
  i.i_category,
  i.i_class,
  i.i_item_id,
  i.i_item_desc,
  revenueratio