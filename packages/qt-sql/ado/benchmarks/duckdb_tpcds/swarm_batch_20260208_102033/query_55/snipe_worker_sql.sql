WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_moy = 12
      AND d_year = 2000
),
filtered_items AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 100
)
SELECT
    fi.i_brand_id AS brand_id,
    fi.i_brand AS brand,
    SUM(ss.ss_ext_sales_price) AS ext_price
FROM store_sales ss
JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
GROUP BY
    fi.i_brand_id,
    fi.i_brand
ORDER BY
    ext_price DESC,
    brand_id
LIMIT 100