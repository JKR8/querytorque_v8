WITH filtered_dates AS (
    SELECT d_date_sk, d_year, d_moy
    FROM date_dim
    WHERE (d_year = 2001 OR (d_year = 2000 AND d_moy = 12) OR (d_year = 2002 AND d_moy = 1))
      AND d_date_sk BETWEEN 2450816 AND 2452642  -- Optional range filter based on estimates
),
v1 AS (
    SELECT 
        i_category, 
        i_brand,
        s_store_name, 
        s_company_name,
        d_year, 
        d_moy,
        SUM(ss_sales_price) AS sum_sales,
        AVG(SUM(ss_sales_price)) OVER (
            PARTITION BY i_category, i_brand, s_store_name, s_company_name, d_year
        ) AS avg_monthly_sales,
        RANK() OVER (
            PARTITION BY i_category, i_brand, s_store_name, s_company_name
            ORDER BY d_year, d_moy
        ) AS rn
    FROM 
        item
    INNER JOIN 
        store_sales ON ss_item_sk = i_item_sk
    INNER JOIN 
        filtered_dates ON ss_sold_date_sk = d_date_sk
    INNER JOIN 
        store ON ss_store_sk = s_store_sk
    GROUP BY 
        i_category, i_brand, s_store_name, s_company_name, d_year, d_moy
),
v2 AS (
    SELECT 
        s_store_name,
        d_year,
        avg_monthly_sales,
        sum_sales,
        LAG(sum_sales) OVER (
            PARTITION BY i_category, i_brand, s_store_name, s_company_name
            ORDER BY d_year, d_moy
        ) AS psum,
        LEAD(sum_sales) OVER (
            PARTITION BY i_category, i_brand, s_store_name, s_company_name
            ORDER BY d_year, d_moy
        ) AS nsum
    FROM 
        v1
)
SELECT 
    *
FROM 
    v2
WHERE 
    d_year = 2001
    AND avg_monthly_sales > 0
    AND ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales > 0.1
ORDER BY 
    sum_sales - avg_monthly_sales, 
    nsum
LIMIT 100;