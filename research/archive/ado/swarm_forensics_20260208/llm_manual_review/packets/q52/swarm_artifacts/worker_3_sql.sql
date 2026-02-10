WITH filtered_dates AS (
    SELECT d_date_sk, d_year
    FROM date_dim
    WHERE d_moy = 12
      AND d_year = 2002
),
filtered_items AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 1
),
filtered_sales AS (
    SELECT ss.ss_item_sk, ss.ss_ext_sales_price, fd.d_year
    FROM store_sales ss
    JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
)
SELECT
    fs.d_year,
    fi.i_brand_id AS brand_id,
    fi.i_brand AS brand,
    SUM(fs.ss_ext_sales_price) AS ext_price
FROM filtered_sales fs
JOIN filtered_items fi ON fs.ss_item_sk = fi.i_item_sk
GROUP BY
    fs.d_year,
    fi.i_brand,
    fi.i_brand_id
ORDER BY
    fs.d_year,
    ext_price DESC,
    brand_id
LIMIT 100