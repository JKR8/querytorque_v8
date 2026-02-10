WITH filtered_date AS (
    SELECT d_date_sk, d_moy
    FROM date_dim
    WHERE d_month_seq IN (
        1181, 1181 + 1, 1181 + 2, 1181 + 3, 1181 + 4, 1181 + 5,
        1181 + 6, 1181 + 7, 1181 + 8, 1181 + 9, 1181 + 10, 1181 + 11
    )
),
filtered_item AS (
    SELECT i_item_sk, i_manager_id
    FROM item
    WHERE (
        i_category IN ('Books', 'Children', 'Electronics')
        AND i_class IN ('personal', 'portable', 'reference', 'self-help')
        AND i_brand IN (
            'scholaramalgamalg #14',
            'scholaramalgamalg #7',
            'exportiunivamalg #9',
            'scholaramalgamalg #9'
        )
    ) OR (
        i_category IN ('Women', 'Music', 'Men')
        AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
        AND i_brand IN (
            'amalgimporto #1',
            'edu packscholar #1',
            'exportiimporto #1',
            'importoamalg #1'
        )
    )
),
early_agg AS (
    SELECT
        ss_item_sk,
        ss_sold_date_sk,
        ss_store_sk,
        SUM(ss_sales_price) AS ss_sales_price_sum
    FROM store_sales
    GROUP BY ss_item_sk, ss_sold_date_sk, ss_store_sk
),
joined_base AS (
    SELECT
        fi.i_manager_id,
        fd.d_moy,
        ea.ss_sales_price_sum
    FROM early_agg ea
    INNER JOIN filtered_item fi ON ea.ss_item_sk = fi.i_item_sk
    INNER JOIN filtered_date fd ON ea.ss_sold_date_sk = fd.d_date_sk
    INNER JOIN store s ON ea.ss_store_sk = s.s_store_sk
),
monthly_sales AS (
    SELECT
        i_manager_id,
        d_moy,
        SUM(ss_sales_price_sum) AS sum_sales
    FROM joined_base
    GROUP BY i_manager_id, d_moy
),
manager_avg AS (
    SELECT
        i_manager_id,
        AVG(sum_sales) AS avg_monthly_sales
    FROM monthly_sales
    GROUP BY i_manager_id
)
SELECT
    ms.i_manager_id,
    ms.sum_sales,
    ma.avg_monthly_sales
FROM monthly_sales ms
INNER JOIN manager_avg ma ON ms.i_manager_id = ma.i_manager_id
WHERE
    CASE
        WHEN ma.avg_monthly_sales > 0
        THEN ABS(ms.sum_sales - ma.avg_monthly_sales) / ma.avg_monthly_sales
        ELSE NULL
    END > 0.1
ORDER BY
    ms.i_manager_id,
    ma.avg_monthly_sales,
    ms.sum_sales
LIMIT 100