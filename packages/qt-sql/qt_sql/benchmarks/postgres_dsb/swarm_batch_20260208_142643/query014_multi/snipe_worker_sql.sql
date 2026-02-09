WITH cross_items AS (
    SELECT DISTINCT ss_item_sk
    FROM (
        SELECT ss_item_sk
        FROM store_sales
        JOIN item ON ss_item_sk = i_item_sk
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND i_category IN ('Electronics', 'Home', 'Men')
          AND i_manager_id BETWEEN 25 AND 34
          AND ss_wholesale_cost BETWEEN 34 AND 54
        INTERSECT
        SELECT cs_item_sk
        FROM catalog_sales
        JOIN item ON cs_item_sk = i_item_sk
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND i_category IN ('Electronics', 'Home', 'Men')
          AND i_manager_id BETWEEN 25 AND 34
          AND cs_wholesale_cost BETWEEN 34 AND 54
        INTERSECT
        SELECT ws_item_sk
        FROM web_sales
        JOIN item ON ws_item_sk = i_item_sk
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND i_category IN ('Electronics', 'Home', 'Men')
          AND i_manager_id BETWEEN 25 AND 34
          AND ws_wholesale_cost BETWEEN 34 AND 54
    ) AS items
),
avg_sales AS (
    SELECT AVG(quantity * list_price) AS average_sales
    FROM (
        SELECT ss_quantity AS quantity, ss_list_price AS list_price
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND ss_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT cs_quantity AS quantity, cs_list_price AS list_price
        FROM catalog_sales
        JOIN date_dim ON cs_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND cs_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT ws_quantity AS quantity, ws_list_price AS list_price
        FROM web_sales
        JOIN date_dim ON ws_sold_date_sk = d_date_sk
        WHERE d_year BETWEEN 2000 AND 2002
          AND ws_wholesale_cost BETWEEN 34 AND 54
    ) AS x
),
week_2000 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_year = 2000 AND d_moy = 12 AND d_dom = 17
),
week_2001 AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE d_year = 2000 + 1 AND d_moy = 12 AND d_dom = 17
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_class_id, i_category_id
    FROM item
    WHERE i_category IN ('Electronics', 'Home', 'Men')
      AND i_manager_id BETWEEN 25 AND 34
),
store_sales_base AS (
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        d_week_seq,
        d_year
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    WHERE ss_wholesale_cost BETWEEN 34 AND 54
      AND d_year IN (2000, 2001)
),
this_year_data AS (
    SELECT 
        'store' AS channel,
        i.i_brand_id,
        i.i_class_id,
        i.i_category_id,
        SUM(ss.ss_quantity * ss.ss_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM store_sales_base ss
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN cross_items ci ON ss.ss_item_sk = ci.ss_item_sk
    JOIN week_2001 w ON ss.d_week_seq = w.d_week_seq
    WHERE ss.d_year = 2001
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
),
last_year_data AS (
    SELECT 
        'store' AS channel,
        i.i_brand_id,
        i.i_class_id,
        i.i_category_id,
        SUM(ss.ss_quantity * ss.ss_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM store_sales_base ss
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN cross_items ci ON ss.ss_item_sk = ci.ss_item_sk
    JOIN week_2000 w ON ss.d_week_seq = w.d_week_seq
    WHERE ss.d_year = 2000
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
),
avg_sales_val AS (
    SELECT average_sales FROM avg_sales
)
SELECT
    ty.channel AS ty_channel,
    ty.i_brand_id AS ty_brand,
    ty.i_class_id AS ty_class,
    ty.i_category_id AS ty_category,
    ty.sales AS ty_sales,
    ty.number_sales AS ty_number_sales,
    ly.channel AS ly_channel,
    ly.i_brand_id AS ly_brand,
    ly.i_class_id AS ly_class,
    ly.i_category_id AS ly_category,
    ly.sales AS ly_sales,
    ly.number_sales AS ly_number_sales
FROM this_year_data ty
JOIN last_year_data ly
    ON ty.i_brand_id = ly.i_brand_id
    AND ty.i_class_id = ly.i_class_id
    AND ty.i_category_id = ly.i_category_id
CROSS JOIN avg_sales_val av
WHERE ty.sales > av.average_sales
  AND ly.sales > av.average_sales
ORDER BY ty.channel, ty.i_brand_id, ty.i_class_id, ty.i_category_id
LIMIT 100;