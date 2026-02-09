WITH filtered_dates AS (
    SELECT 
        d_date_sk,
        d_year,
        d_qoy,
        d_moy
    FROM date_dim
    WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
fact_agg AS (
    SELECT
        ss_item_sk,
        ss_store_sk,
        d.d_date_sk,
        SUM(COALESCE(ss_sales_price * ss_quantity, 0)) AS sum_sales
    FROM store_sales
    JOIN filtered_dates d ON ss_sold_date_sk = d.d_date_sk
    GROUP BY 1, 2, 3
),
detailed_agg AS (
    SELECT
        i.i_category,
        i.i_class,
        i.i_brand,
        i.i_product_name,
        d.d_year,
        d.d_qoy,
        d.d_moy,
        s.s_store_id,
        fa.sum_sales AS sumsales
    FROM fact_agg fa
    JOIN item i ON fa.ss_item_sk = i.i_item_sk
    JOIN store s ON fa.ss_store_sk = s.s_store_sk
    JOIN filtered_dates d ON fa.d_date_sk = d.d_date_sk
    GROUP BY 
        i.i_category,
        i.i_class,
        i.i_brand,
        i.i_product_name,
        d.d_year,
        d.d_qoy,
        d.d_moy,
        s.s_store_id,
        fa.sum_sales
)
SELECT
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    rk
FROM (
    SELECT
        *,
        RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
    FROM detailed_agg
) ranked
WHERE rk <= 100
ORDER BY
    i_category,
    i_class,
    i_brand,
    i_product_name,
    d_year,
    d_qoy,
    d_moy,
    s_store_id,
    sumsales,
    rk
LIMIT 100;