WITH v1_raw AS (
    -- Branch 1: d_year = 1999
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) AS sum_sales
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN call_center ON cc_call_center_sk = cs_call_center_sk
    WHERE d_year = 1999
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy

    UNION ALL

    -- Branch 2: d_year = 1998 AND d_moy = 12
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) AS sum_sales
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN call_center ON cc_call_center_sk = cs_call_center_sk
    WHERE d_year = 1998 AND d_moy = 12
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy

    UNION ALL

    -- Branch 3: d_year = 2000 AND d_moy = 1
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        SUM(cs_sales_price) AS sum_sales
    FROM catalog_sales
    JOIN item ON cs_item_sk = i_item_sk
    JOIN date_dim ON cs_sold_date_sk = d_date_sk
    JOIN call_center ON cc_call_center_sk = cs_call_center_sk
    WHERE d_year = 2000 AND d_moy = 1
    GROUP BY i_category, i_brand, cc_name, d_year, d_moy
),
v1 AS (
    SELECT
        i_category,
        i_brand,
        cc_name,
        d_year,
        d_moy,
        sum_sales,
        AVG(sum_sales) OVER (PARTITION BY i_category, i_brand, cc_name, d_year) AS avg_monthly_sales,
        RANK() OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS rn,
        LAG(sum_sales) OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS psum,
        LEAD(sum_sales) OVER (PARTITION BY i_category, i_brand, cc_name ORDER BY d_year, d_moy) AS nsum
    FROM v1_raw
),
v2 AS (
    SELECT
        i_brand,
        d_year,
        avg_monthly_sales,
        sum_sales,
        psum,
        nsum
    FROM v1
    WHERE psum IS NOT NULL
      AND nsum IS NOT NULL
)
SELECT *
FROM v2
WHERE d_year = 1999
  AND avg_monthly_sales > 0
  AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY sum_sales - avg_monthly_sales,
         nsum
LIMIT 100