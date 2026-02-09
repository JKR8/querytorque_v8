WITH filtered_item AS (
    SELECT 
        i_item_sk,
        i_brand_id,
        i_class_id,
        i_category_id
    FROM item
    WHERE i_category IN ('Electronics', 'Home', 'Men')
      AND i_manager_id BETWEEN 25 AND 34
),
filtered_date_range AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2002
),
filtered_date_specific AS (
    SELECT d_week_seq
    FROM date_dim
    WHERE (d_year = 2000 AND d_moy = 12 AND d_dom = 17)
       OR (d_year = 2001 AND d_moy = 12 AND d_dom = 17)
),
store_brand_class_category AS (
    SELECT DISTINCT 
        iss.i_brand_id,
        iss.i_class_id,
        iss.i_category_id
    FROM store_sales
    INNER JOIN filtered_item iss ON store_sales.ss_item_sk = iss.i_item_sk
    INNER JOIN filtered_date_range d1 ON store_sales.ss_sold_date_sk = d1.d_date_sk
    WHERE store_sales.ss_wholesale_cost BETWEEN 34 AND 54
),
catalog_brand_class_category AS (
    SELECT DISTINCT 
        ics.i_brand_id,
        ics.i_class_id,
        ics.i_category_id
    FROM catalog_sales
    INNER JOIN filtered_item ics ON catalog_sales.cs_item_sk = ics.i_item_sk
    INNER JOIN filtered_date_range d2 ON catalog_sales.cs_sold_date_sk = d2.d_date_sk
    WHERE catalog_sales.cs_wholesale_cost BETWEEN 34 AND 54
),
web_brand_class_category AS (
    SELECT DISTINCT 
        iws.i_brand_id,
        iws.i_class_id,
        iws.i_category_id
    FROM web_sales
    INNER JOIN filtered_item iws ON web_sales.ws_item_sk = iws.i_item_sk
    INNER JOIN filtered_date_range d3 ON web_sales.ws_sold_date_sk = d3.d_date_sk
    WHERE web_sales.ws_wholesale_cost BETWEEN 34 AND 54
),
cross_items AS (
    SELECT i_item_sk AS ss_item_sk
    FROM filtered_item i
    WHERE EXISTS (
        SELECT 1 FROM store_brand_class_category s
        WHERE s.i_brand_id = i.i_brand_id
          AND s.i_class_id = i.i_class_id
          AND s.i_category_id = i.i_category_id
    ) AND EXISTS (
        SELECT 1 FROM catalog_brand_class_category c
        WHERE c.i_brand_id = i.i_brand_id
          AND c.i_class_id = i.i_class_id
          AND c.i_category_id = i.i_category_id
    ) AND EXISTS (
        SELECT 1 FROM web_brand_class_category w
        WHERE w.i_brand_id = i.i_brand_id
          AND w.i_class_id = i.i_class_id
          AND w.i_category_id = i.i_category_id
    )
),
avg_sales AS (
    SELECT AVG(quantity * list_price) AS average_sales
    FROM (
        SELECT 
            ss_quantity AS quantity,
            ss_list_price AS list_price
        FROM store_sales
        INNER JOIN filtered_date_range ON store_sales.ss_sold_date_sk = filtered_date_range.d_date_sk
        WHERE store_sales.ss_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT 
            cs_quantity AS quantity,
            cs_list_price AS list_price
        FROM catalog_sales
        INNER JOIN filtered_date_range ON catalog_sales.cs_sold_date_sk = filtered_date_range.d_date_sk
        WHERE catalog_sales.cs_wholesale_cost BETWEEN 34 AND 54
        UNION ALL
        SELECT 
            ws_quantity AS quantity,
            ws_list_price AS list_price
        FROM web_sales
        INNER JOIN filtered_date_range ON web_sales.ws_sold_date_sk = filtered_date_range.d_date_sk
        WHERE web_sales.ws_wholesale_cost BETWEEN 34 AND 54
    ) AS x
),
store_sales_filtered AS (
    SELECT
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        d_week_seq
    FROM store_sales
    INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    WHERE store_sales.ss_wholesale_cost BETWEEN 34 AND 54
      AND date_dim.d_week_seq IN (SELECT d_week_seq FROM filtered_date_specific)
),
store_sales_aggregated AS (
    SELECT
        'store' AS channel,
        filtered_item.i_brand_id,
        filtered_item.i_class_id,
        filtered_item.i_category_id,
        SUM(store_sales_filtered.ss_quantity * store_sales_filtered.ss_list_price) AS sales,
        COUNT(*) AS number_sales,
        store_sales_filtered.d_week_seq
    FROM store_sales_filtered
    INNER JOIN filtered_item ON store_sales_filtered.ss_item_sk = filtered_item.i_item_sk
    WHERE EXISTS (SELECT 1 FROM cross_items WHERE ss_item_sk = store_sales_filtered.ss_item_sk)
    GROUP BY
        filtered_item.i_brand_id,
        filtered_item.i_class_id,
        filtered_item.i_category_id,
        store_sales_filtered.d_week_seq
    HAVING SUM(store_sales_filtered.ss_quantity * store_sales_filtered.ss_list_price) > (
        SELECT average_sales FROM avg_sales
    )
),
week_seq_2000 AS (SELECT d_week_seq FROM filtered_date_specific WHERE EXISTS (
    SELECT 1 FROM date_dim WHERE d_year = 2000 AND d_moy = 12 AND d_dom = 17
    AND filtered_date_specific.d_week_seq = date_dim.d_week_seq
)),
week_seq_2001 AS (SELECT d_week_seq FROM filtered_date_specific WHERE EXISTS (
    SELECT 1 FROM date_dim WHERE d_year = 2001 AND d_moy = 12 AND d_dom = 17
    AND filtered_date_specific.d_week_seq = date_dim.d_week_seq
))
SELECT
    this_year.channel AS ty_channel,
    this_year.i_brand_id AS ty_brand,
    this_year.i_class_id AS ty_class,
    this_year.i_category_id AS ty_category,
    this_year.sales AS ty_sales,
    this_year.number_sales AS ty_number_sales,
    last_year.channel AS ly_channel,
    last_year.i_brand_id AS ly_brand,
    last_year.i_class_id AS ly_class,
    last_year.i_category_id AS ly_category,
    last_year.sales AS ly_sales,
    last_year.number_sales AS ly_number_sales
FROM (
    SELECT * FROM store_sales_aggregated 
    WHERE d_week_seq IN (SELECT d_week_seq FROM week_seq_2001)
) AS this_year
INNER JOIN (
    SELECT * FROM store_sales_aggregated 
    WHERE d_week_seq IN (SELECT d_week_seq FROM week_seq_2000)
) AS last_year ON this_year.i_brand_id = last_year.i_brand_id
    AND this_year.i_class_id = last_year.i_class_id
    AND this_year.i_category_id = last_year.i_category_id
ORDER BY
    this_year.channel,
    this_year.i_brand_id,
    this_year.i_class_id,
    this_year.i_category_id
LIMIT 100;