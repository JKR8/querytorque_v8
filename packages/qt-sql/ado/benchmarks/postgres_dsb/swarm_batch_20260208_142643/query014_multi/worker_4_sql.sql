WITH filtered_date AS (
    SELECT d_date_sk, d_week_seq
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2002
),
filtered_item AS (
    SELECT 
        i_item_sk,
        i_brand_id,
        i_class_id,
        i_category_id
    FROM item
    WHERE i_category IN ('Electronics', 'Home', 'Men')
      AND i_manager_id BETWEEN 25 AND 34
),
cross_items AS (
    SELECT i_item_sk AS ss_item_sk
    FROM filtered_item
    WHERE EXISTS (
        SELECT 1
        FROM store_sales
        JOIN filtered_item iss ON ss_item_sk = iss.i_item_sk
        JOIN filtered_date d1 ON ss_sold_date_sk = d1.d_date_sk
        WHERE iss.i_brand_id = filtered_item.i_brand_id
          AND iss.i_class_id = filtered_item.i_class_id
          AND iss.i_category_id = filtered_item.i_category_id
          AND ss_wholesale_cost BETWEEN 34 AND 54
    )
    AND EXISTS (
        SELECT 1
        FROM catalog_sales
        JOIN filtered_item ics ON cs_item_sk = ics.i_item_sk
        JOIN filtered_date d2 ON cs_sold_date_sk = d2.d_date_sk
        WHERE ics.i_brand_id = filtered_item.i_brand_id
          AND ics.i_class_id = filtered_item.i_class_id
          AND ics.i_category_id = filtered_item.i_category_id
          AND cs_wholesale_cost BETWEEN 34 AND 54
    )
    AND EXISTS (
        SELECT 1
        FROM web_sales
        JOIN filtered_item iws ON ws_item_sk = iws.i_item_sk
        JOIN filtered_date d3 ON ws_sold_date_sk = d3.d_date_sk
        WHERE iws.i_brand_id = filtered_item.i_brand_id
          AND iws.i_class_id = filtered_item.i_class_id
          AND iws.i_category_id = filtered_item.i_category_id
          AND ws_wholesale_cost BETWEEN 34 AND 54
    )
),
avg_sales AS (
    SELECT AVG(quantity * list_price) AS average_sales
    FROM (
        SELECT ss_quantity AS quantity, ss_list_price AS list_price
        FROM store_sales
        JOIN filtered_date ON ss_sold_date_sk = d_date_sk
        WHERE ss_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT cs_quantity AS quantity, cs_list_price AS list_price
        FROM catalog_sales
        JOIN filtered_date ON cs_sold_date_sk = d_date_sk
        WHERE cs_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT ws_quantity AS quantity, ws_list_price AS list_price
        FROM web_sales
        JOIN filtered_date ON ws_sold_date_sk = d_date_sk
        WHERE ws_wholesale_cost BETWEEN 34 AND 54
    ) AS x
),
week_seq AS (
    SELECT 
        d_week_seq AS week_2000,
        LEAD(d_week_seq) OVER (ORDER BY d_year, d_moy, d_dom) AS week_2001
    FROM date_dim
    WHERE (d_year = 2000 AND d_moy = 12 AND d_dom = 17)
       OR (d_year = 2001 AND d_moy = 12 AND d_dom = 17)
    ORDER BY d_year
    LIMIT 1
),
store_sales_data AS (
    SELECT 
        'store' AS channel,
        i.i_brand_id,
        i.i_class_id,
        i.i_category_id,
        d.d_week_seq,
        SUM(ss_quantity * ss_list_price) AS sales,
        COUNT(*) AS number_sales
    FROM store_sales ss
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE ss.ss_item_sk IN (SELECT ss_item_sk FROM cross_items)
      AND ss.ss_wholesale_cost BETWEEN 34 AND 54
      AND d.d_week_seq IN (SELECT week_2000 FROM week_seq UNION ALL SELECT week_2001 FROM week_seq)
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id, d.d_week_seq
),
filtered_sales AS (
    SELECT 
        channel,
        i_brand_id,
        i_class_id,
        i_category_id,
        sales,
        number_sales,
        d_week_seq
    FROM store_sales_data
    WHERE sales > (SELECT average_sales FROM avg_sales)
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
FROM (SELECT * FROM filtered_sales WHERE d_week_seq = (SELECT week_2001 FROM week_seq)) ty
JOIN (SELECT * FROM filtered_sales WHERE d_week_seq = (SELECT week_2000 FROM week_seq)) ly
    ON ty.i_brand_id = ly.i_brand_id
   AND ty.i_class_id = ly.i_class_id
   AND ty.i_category_id = ly.i_category_id
ORDER BY ty.channel, ty.i_brand_id, ty.i_class_id, ty.i_category_id
LIMIT 100;