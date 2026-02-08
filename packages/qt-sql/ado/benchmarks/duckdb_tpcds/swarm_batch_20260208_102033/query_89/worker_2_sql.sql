WITH filtered_dates AS (
    SELECT d_date_sk, d_moy
    FROM date_dim
    WHERE d_year = 1999
),
filtered_items AS (
    SELECT i_item_sk, i_category, i_class, i_brand
    FROM item
    WHERE (
        i_category IN ('Jewelry', 'Shoes', 'Electronics')
        AND i_class IN ('semi-precious', 'athletic', 'portable')
    )
    OR (
        i_category IN ('Men', 'Music', 'Women')
        AND i_class IN ('accessories', 'rock', 'maternity')
    )
),
filtered_stores AS (
    SELECT s_store_sk, s_store_name, s_company_name
    FROM store
),
sales_aggregated AS (
    SELECT
        i.i_category,
        i.i_class,
        i.i_brand,
        s.s_store_name,
        s.s_company_name,
        d.d_moy,
        SUM(ss.ss_sales_price) AS sum_sales,
        AVG(SUM(ss.ss_sales_price)) OVER (
            PARTITION BY i.i_category, i.i_brand, s.s_store_name, s.s_company_name
        ) AS avg_monthly_sales
    FROM store_sales ss
    JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_stores s ON ss.ss_store_sk = s.s_store_sk
    GROUP BY
        i.i_category,
        i.i_class,
        i.i_brand,
        s.s_store_name,
        s.s_company_name,
        d.d_moy
)
SELECT *
FROM sales_aggregated
WHERE
    CASE
        WHEN avg_monthly_sales <> 0
        THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
        ELSE NULL
    END > 0.1
ORDER BY
    sum_sales - avg_monthly_sales,
    s_store_name
LIMIT 100;