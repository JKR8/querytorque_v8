WITH filtered_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year BETWEEN 2000 AND 2002
),
cross_items AS (
  SELECT
    i_item_sk AS ss_item_sk,
    i_brand_id,
    i_class_id,
    i_category_id
  FROM item
  WHERE EXISTS (
    SELECT 1
    FROM store_sales
    JOIN item iss ON ss_item_sk = iss.i_item_sk
    JOIN filtered_dates d1 ON ss_sold_date_sk = d1.d_date_sk
    WHERE iss.i_brand_id = item.i_brand_id
      AND iss.i_class_id = item.i_class_id
      AND iss.i_category_id = item.i_category_id
  )
  AND EXISTS (
    SELECT 1
    FROM catalog_sales
    JOIN item ics ON cs_item_sk = ics.i_item_sk
    JOIN filtered_dates d2 ON cs_sold_date_sk = d2.d_date_sk
    WHERE ics.i_brand_id = item.i_brand_id
      AND ics.i_class_id = item.i_class_id
      AND ics.i_category_id = item.i_category_id
  )
  AND EXISTS (
    SELECT 1
    FROM web_sales
    JOIN item iws ON ws_item_sk = iws.i_item_sk
    JOIN filtered_dates d3 ON ws_sold_date_sk = d3.d_date_sk
    WHERE iws.i_brand_id = item.i_brand_id
      AND iws.i_class_id = item.i_class_id
      AND iws.i_category_id = item.i_category_id
  )
),
avg_sales AS (
  SELECT
    AVG(quantity * list_price) AS average_sales
  FROM (
    SELECT ss_quantity AS quantity, ss_list_price AS list_price
    FROM store_sales
    JOIN filtered_dates ON ss_sold_date_sk = d_date_sk
    UNION ALL
    SELECT cs_quantity, cs_list_price
    FROM catalog_sales
    JOIN filtered_dates ON cs_sold_date_sk = d_date_sk
    UNION ALL
    SELECT ws_quantity, ws_list_price
    FROM web_sales
    JOIN filtered_dates ON ws_sold_date_sk = d_date_sk
  ) x
),
nov2002_dates AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 2000 + 2
    AND d_moy = 11
),
store_sales_data AS (
  SELECT
    'store' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(ss_quantity * ss_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM store_sales ss
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN nov2002_dates nd ON ss.ss_sold_date_sk = nd.d_date_sk
  WHERE EXISTS (
    SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = ss.ss_item_sk
  )
  GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
  HAVING SUM(ss_quantity * ss_list_price) > (SELECT average_sales FROM avg_sales)
),
catalog_sales_data AS (
  SELECT
    'catalog' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(cs_quantity * cs_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM catalog_sales cs
  JOIN item i ON cs.cs_item_sk = i.i_item_sk
  JOIN nov2002_dates nd ON cs.cs_sold_date_sk = nd.d_date_sk
  WHERE EXISTS (
    SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = cs.cs_item_sk
  )
  GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
  HAVING SUM(cs_quantity * cs_list_price) > (SELECT average_sales FROM avg_sales)
),
web_sales_data AS (
  SELECT
    'web' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(ws_quantity * ws_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM web_sales ws
  JOIN item i ON ws.ws_item_sk = i.i_item_sk
  JOIN nov2002_dates nd ON ws.ws_sold_date_sk = nd.d_date_sk
  WHERE EXISTS (
    SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = ws.ws_item_sk
  )
  GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
  HAVING SUM(ws_quantity * ws_list_price) > (SELECT average_sales FROM avg_sales)
)
SELECT
  channel,
  i_brand_id,
  i_class_id,
  i_category_id,
  SUM(sales) AS "SUM(sales)",
  SUM(number_sales) AS "SUM(number_sales)"
FROM (
  SELECT * FROM store_sales_data
  UNION ALL
  SELECT * FROM catalog_sales_data
  UNION ALL
  SELECT * FROM web_sales_data
) y
GROUP BY ROLLUP(channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel, i_brand_id, i_class_id, i_category_id
LIMIT 100;