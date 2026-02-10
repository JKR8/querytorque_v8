WITH filtered_dates AS (
    SELECT d_date_sk, d_year, d_moy, d_week_seq
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2002
),
cross_items AS (
    SELECT i_item_sk AS ss_item_sk
    FROM item i
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT iss.i_brand_id, iss.i_class_id, iss.i_category_id
            FROM store_sales ss
            JOIN item iss ON ss.ss_item_sk = iss.i_item_sk
            JOIN filtered_dates d1 ON ss.ss_sold_date_sk = d1.d_date_sk
            WHERE d1.d_year BETWEEN 2000 AND 2002
            
            INTERSECT
            
            SELECT ics.i_brand_id, ics.i_class_id, ics.i_category_id
            FROM catalog_sales cs
            JOIN item ics ON cs.cs_item_sk = ics.i_item_sk
            JOIN filtered_dates d2 ON cs.cs_sold_date_sk = d2.d_date_sk
            WHERE d2.d_year BETWEEN 2000 AND 2002
            
            INTERSECT
            
            SELECT iws.i_brand_id, iws.i_class_id, iws.i_category_id
            FROM web_sales ws
            JOIN item iws ON ws.ws_item_sk = iws.i_item_sk
            JOIN filtered_dates d3 ON ws.ws_sold_date_sk = d3.d_date_sk
            WHERE d3.d_year BETWEEN 2000 AND 2002
        ) x
        WHERE i.i_brand_id = x.i_brand_id
          AND i.i_class_id = x.i_class_id
          AND i.i_category_id = x.i_category_id
    )
),
avg_sales AS (
    SELECT AVG(quantity * list_price) AS average_sales
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
    ) sales_data
)
SELECT channel, i_brand_id, i_class_id, i_category_id, 
       SUM(sales) AS sum_sales, SUM(number_sales) AS sum_number_sales
FROM (
    SELECT 'store' AS channel, i.i_brand_id, i.i_class_id, i.i_category_id,
           SUM(ss.ss_quantity * ss.ss_list_price) AS sales,
           COUNT(*) AS number_sales
    FROM store_sales ss
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year = 2002 
      AND d.d_moy = 11
      AND EXISTS (SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = ss.ss_item_sk)
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
    HAVING SUM(ss.ss_quantity * ss.ss_list_price) > (SELECT average_sales FROM avg_sales)
    
    UNION ALL
    
    SELECT 'catalog' AS channel, i.i_brand_id, i.i_class_id, i.i_category_id,
           SUM(cs.cs_quantity * cs.cs_list_price) AS sales,
           COUNT(*) AS number_sales
    FROM catalog_sales cs
    JOIN item i ON cs.cs_item_sk = i.i_item_sk
    JOIN filtered_dates d ON cs.cs_sold_date_sk = d.d_date_sk
    WHERE d.d_year = 2002 
      AND d.d_moy = 11
      AND EXISTS (SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = cs.cs_item_sk)
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
    HAVING SUM(cs.cs_quantity * cs.cs_list_price) > (SELECT average_sales FROM avg_sales)
    
    UNION ALL
    
    SELECT 'web' AS channel, i.i_brand_id, i.i_class_id, i.i_category_id,
           SUM(ws.ws_quantity * ws.ws_list_price) AS sales,
           COUNT(*) AS number_sales
    FROM web_sales ws
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
    JOIN filtered_dates d ON ws.ws_sold_date_sk = d.d_date_sk
    WHERE d.d_year = 2002 
      AND d.d_moy = 11
      AND EXISTS (SELECT 1 FROM cross_items ci WHERE ci.ss_item_sk = ws.ws_item_sk)
    GROUP BY i.i_brand_id, i.i_class_id, i.i_category_id
    HAVING SUM(ws.ws_quantity * ws.ws_list_price) > (SELECT average_sales FROM avg_sales)
) y
GROUP BY ROLLUP (channel, i_brand_id, i_class_id, i_category_id)
ORDER BY channel, i_brand_id, i_class_id, i_category_id
LIMIT 100;