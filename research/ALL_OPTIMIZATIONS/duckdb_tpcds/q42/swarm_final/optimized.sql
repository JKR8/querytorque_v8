WITH filtered_dates AS (
    SELECT d_date_sk, d_year
    FROM date_dim 
    WHERE d_moy = 11 
      AND d_year = 2002
), filtered_items AS (
    SELECT i_item_sk, i_category_id, i_category
    FROM item
    WHERE i_manager_id = 1
), pre_aggregated_sales AS (
    SELECT 
        fd.d_year,
        ss.ss_item_sk,
        SUM(ss.ss_ext_sales_price) AS item_sales
    FROM store_sales ss
    INNER JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
    GROUP BY fd.d_year, ss.ss_item_sk
)
SELECT 
    pa.d_year,
    fi.i_category_id,
    fi.i_category,
    SUM(pa.item_sales) AS "SUM(ss_ext_sales_price)"
FROM pre_aggregated_sales pa
INNER JOIN filtered_items fi ON pa.ss_item_sk = fi.i_item_sk
GROUP BY pa.d_year, fi.i_category_id, fi.i_category
ORDER BY 
    "SUM(ss_ext_sales_price)" DESC,
    pa.d_year,
    fi.i_category_id,
    fi.i_category
LIMIT 100;