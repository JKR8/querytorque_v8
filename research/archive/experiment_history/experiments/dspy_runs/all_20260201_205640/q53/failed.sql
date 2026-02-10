WITH filtered_items AS (
    SELECT i_item_sk, i_manufact_id
    FROM item
    WHERE (
        (i_category IN ('Books','Children','Electronics') 
         AND i_class IN ('personal','portable','reference','self-help')
         AND i_brand IN ('scholaramalgamalg #14','scholaramalgamalg #7',
                         'exportiunivamalg #9','scholaramalgamalg #9'))
        OR (i_category IN ('Women','Music','Men')
            AND i_class IN ('accessories','classical','fragrances','pants')
            AND i_brand IN ('amalgimporto #1','edu packscholar #1',
                            'exportiimporto #1','importoamalg #1'))
    )
), filtered_dates AS (
    SELECT d_date_sk, d_qoy
    FROM date_dim
    WHERE d_month_seq BETWEEN 1200 AND 1211
), filtered_stores AS (
    SELECT s_store_sk
    FROM store
    WHERE s_store_sk <= 400
)
SELECT * 
FROM (
    SELECT 
        fi.i_manufact_id,
        SUM(ss.ss_sales_price) AS sum_sales,
        AVG(SUM(ss.ss_sales_price)) OVER (PARTITION BY fi.i_manufact_id) AS avg_quarterly_sales,
        fd.d_qoy
    FROM store_sales ss
    JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
    JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_stores fs ON ss.ss_store_sk = fs.s_store_sk
    GROUP BY fi.i_manufact_id, fd.d_qoy
) tmp1
WHERE CASE WHEN avg_quarterly_sales > 0 
      THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales 
      ELSE NULL END > 0.1
ORDER BY avg_quarterly_sales,
         sum_sales,
         i_manufact_id
LIMIT 100;