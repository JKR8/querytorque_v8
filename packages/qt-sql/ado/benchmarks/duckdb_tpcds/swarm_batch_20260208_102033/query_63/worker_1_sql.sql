WITH filtered_dates AS (
  SELECT d_date_sk, d_moy
  FROM date_dim
  WHERE d_month_seq BETWEEN 1181 AND 1192
),
filtered_items AS (
  SELECT i_item_sk, i_manager_id
  FROM item
  WHERE (
    (i_category IN ('Books', 'Children', 'Electronics')
     AND i_class IN ('personal', 'portable', 'reference', 'self-help')
     AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 
                     'exportiunivamalg #9', 'scholaramalgamalg #9'))
    OR
    (i_category IN ('Women', 'Music', 'Men')
     AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
     AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 
                     'exportiimporto #1', 'importoamalg #1'))
  )
),
monthly_sales AS (
  SELECT
    i.i_manager_id,
    d.d_moy,
    SUM(ss.ss_sales_price) AS sum_sales
  FROM store_sales ss
  JOIN filtered_dates d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
  GROUP BY i.i_manager_id, d.d_moy
),
with_avg AS (
  SELECT
    i_manager_id,
    sum_sales,
    AVG(sum_sales) OVER (PARTITION BY i_manager_id) AS avg_monthly_sales
  FROM monthly_sales
)
SELECT *
FROM with_avg
WHERE CASE
  WHEN avg_monthly_sales > 0
  THEN ABS(sum_sales - avg_monthly_sales) / avg_monthly_sales
  ELSE NULL
END > 0.1
ORDER BY i_manager_id, avg_monthly_sales, sum_sales
LIMIT 100