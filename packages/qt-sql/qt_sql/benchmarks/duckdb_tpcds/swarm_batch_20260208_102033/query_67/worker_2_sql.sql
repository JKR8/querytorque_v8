WITH filtered_dates AS (
    SELECT d_date_sk, d_year, d_qoy, d_moy
    FROM date_dim
    WHERE d_month_seq BETWEEN 1206 AND 1206 + 11
),
joined_data AS (
    SELECT
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d.d_year,
        d.d_qoy,
        d.d_moy,
        s.s_store_id,
        COALESCE(ss_sales_price * ss_quantity, 0) AS sales_amount
    FROM store_sales ss
    JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    JOIN store s ON ss.ss_store_sk = s.s_store_sk
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
        i_category,
        i_class,
        i_brand,
        i_product_name,
        d_year,
        d_qoy,
        d_moy,
        s_store_id,
        sumsales,
        RANK() OVER (PARTITION BY i_category ORDER BY sumsales DESC) AS rk
    FROM (
        SELECT
            i_category,
            i_class,
            i_brand,
            i_product_name,
            d_year,
            d_qoy,
            d_moy,
            s_store_id,
            SUM(sales_amount) AS sumsales
        FROM joined_data
        GROUP BY
            ROLLUP (
                i_category,
                i_class,
                i_brand,
                i_product_name,
                d_year,
                d_qoy,
                d_moy,
                s_store_id
            )
    ) AS dw1
) AS dw2
WHERE
    rk <= 100
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