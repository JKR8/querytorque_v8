WITH cross_items AS (
  SELECT DISTINCT i_item_sk AS ss_item_sk
  FROM item i
  WHERE EXISTS (
    SELECT 1
    FROM store_sales, item iss, date_dim d1
    WHERE ss_item_sk = iss.i_item_sk
      AND ss_sold_date_sk = d1.d_date_sk
      AND d1.d_year BETWEEN 2000 AND 2000 + 2
      AND iss.i_brand_id = i.i_brand_id
      AND iss.i_class_id = i.i_class_id
      AND iss.i_category_id = i.i_category_id
  )
    AND EXISTS (
    SELECT 1
    FROM catalog_sales, item ics, date_dim d2
    WHERE cs_item_sk = ics.i_item_sk
      AND cs_sold_date_sk = d2.d_date_sk
      AND d2.d_year BETWEEN 2000 AND 2000 + 2
      AND ics.i_brand_id = i.i_brand_id
      AND ics.i_class_id = i.i_class_id
      AND ics.i_category_id = i.i_category_id
  )
    AND EXISTS (
    SELECT 1
    FROM web_sales, item iws, date_dim d3
    WHERE ws_item_sk = iws.i_item_sk
      AND ws_sold_date_sk = d3.d_date_sk
      AND d3.d_year BETWEEN 2000 AND 2000 + 2
      AND iws.i_brand_id = i.i_brand_id
      AND iws.i_class_id = i.i_class_id
      AND iws.i_category_id = i.i_category_id
  )
),
avg_sales AS (
  SELECT
    AVG(quantity * list_price) AS average_sales
  FROM (
    SELECT
      ss_quantity AS quantity,
      ss_list_price AS list_price
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2000 + 2
    UNION ALL
    SELECT
      cs_quantity AS quantity,
      cs_list_price AS list_price
    FROM catalog_sales
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2000 + 2
    UNION ALL
    SELECT
      ws_quantity AS quantity,
      ws_list_price AS list_price
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    WHERE d_year BETWEEN 2000 AND 2000 + 2
  ) AS x
),
store_agg AS (
  SELECT
    'store' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(ss_quantity * ss_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM store_sales
  JOIN cross_items ci ON ss_item_sk = ci.ss_item_sk
  JOIN item i ON ss_item_sk = i.i_item_sk
  JOIN date_dim ON ss_sold_date_sk = d_date_sk
  WHERE d_year = 2000 + 2
    AND d_moy = 11
  GROUP BY
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id
  HAVING
    SUM(ss_quantity * ss_list_price) > (SELECT average_sales FROM avg_sales)
),
catalog_agg AS (
  SELECT
    'catalog' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(cs_quantity * cs_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM catalog_sales
  JOIN cross_items ci ON cs_item_sk = ci.ss_item_sk
  JOIN item i ON cs_item_sk = i.i_item_sk
  JOIN date_dim ON cs_sold_date_sk = d_date_sk
  WHERE d_year = 2000 + 2
    AND d_moy = 11
  GROUP BY
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id
  HAVING
    SUM(cs_quantity * cs_list_price) > (SELECT average_sales FROM avg_sales)
),
web_agg AS (
  SELECT
    'web' AS channel,
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id,
    SUM(ws_quantity * ws_list_price) AS sales,
    COUNT(*) AS number_sales
  FROM web_sales
  JOIN cross_items ci ON ws_item_sk = ci.ss_item_sk
  JOIN item i ON ws_item_sk = i.i_item_sk
  JOIN date_dim ON ws_sold_date_sk = d_date_sk
  WHERE d_year = 2000 + 2
    AND d_moy = 11
  GROUP BY
    i.i_brand_id,
    i.i_class_id,
    i.i_category_id
  HAVING
    SUM(ws_quantity * ws_list_price) > (SELECT average_sales FROM avg_sales)
)
SELECT
  channel,
  i_brand_id,
  i_class_id,
  i_category_id,
  SUM(sales) AS "SUM(sales)",
  SUM(number_sales) AS "SUM(number_sales)"
FROM (
  SELECT * FROM store_agg
  UNION ALL
  SELECT * FROM catalog_agg
  UNION ALL
  SELECT * FROM web_agg
) AS y
GROUP BY
  ROLLUP (
    channel,
    i_brand_id,
    i_class_id,
    i_category_id
  )
ORDER BY
  channel,
  i_brand_id,
  i_class_id,
  i_category_id
LIMIT 100