WITH filtered_item AS (
  SELECT 
    i_item_sk,
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
  FROM item
  WHERE i_category IN ('Books', 'Sports', 'Men')
),
filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_date BETWEEN CAST('1998-04-06' AS DATE) 
    AND (CAST('1998-04-06' AS DATE) + INTERVAL '30' DAY)
),
joined_sales AS (
  SELECT
    i.i_item_id,
    i.i_item_desc,
    i.i_category,
    i.i_class,
    i.i_current_price,
    ws.ws_ext_sales_price
  FROM web_sales ws
  JOIN filtered_item i ON ws.ws_item_sk = i.i_item_sk
  JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
),
aggregated AS (
  SELECT
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price,
    SUM(ws_ext_sales_price) AS itemrevenue
  FROM joined_sales
  GROUP BY
    i_item_id,
    i_item_desc,
    i_category,
    i_class,
    i_current_price
)
SELECT
  i_item_id,
  i_item_desc,
  i_category,
  i_class,
  i_current_price,
  itemrevenue,
  itemrevenue * 100 / SUM(itemrevenue) OVER (PARTITION BY i_class) AS revenueratio
FROM aggregated
ORDER BY
  i_category,
  i_class,
  i_item_id,
  i_item_desc,
  revenueratio
LIMIT 100