WITH filtered_date AS (
    SELECT d_date_sk, d_year
    FROM date_dim
    WHERE d_moy = 12
      AND d_year = 2002
),
filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand
    FROM item
    WHERE i_manager_id = 1
)
SELECT
    fd.d_year,
    fi.i_brand_id AS brand_id,
    fi.i_brand AS brand,
    SUM(ss.ss_ext_sales_price) AS ext_price
FROM store_sales ss
JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
GROUP BY
    fd.d_year,
    fi.i_brand,
    fi.i_brand_id
ORDER BY
    fd.d_year,
    ext_price DESC,
    brand_id
LIMIT 100