WITH filtered_dates AS (
  SELECT d_date_sk, d_qoy
  FROM date_dim
  WHERE d_month_seq IN (1200, 1201, 1202, 1203, 1204, 1205, 1206, 1207, 1208, 1209, 1210, 1211)
),
filtered_items AS (
  SELECT i_item_sk, i_manufact_id
  FROM item
  WHERE (
    (i_category IN ('Books', 'Children', 'Electronics')
     AND i_class IN ('personal', 'portable', 'reference', 'self-help')
     AND i_brand IN ('scholaramalgamalg #14', 'scholaramalgamalg #7', 'exportiunivamalg #9', 'scholaramalgamalg #9'))
    OR
    (i_category IN ('Women', 'Music', 'Men')
     AND i_class IN ('accessories', 'classical', 'fragrances', 'pants')
     AND i_brand IN ('amalgimporto #1', 'edu packscholar #1', 'exportiimporto #1', 'importoamalg #1'))
  )
),
prejoined_sales AS (
  SELECT 
    i.i_manufact_id,
    fd.d_qoy,
    ss.ss_sales_price
  FROM store_sales ss
  JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
  JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
  JOIN store s ON ss.ss_store_sk = s.s_store_sk
),
aggregated AS (
  SELECT
    i_manufact_id,
    d_qoy,
    SUM(ss_sales_price) AS sum_sales
  FROM prejoined_sales
  GROUP BY i_manufact_id, d_qoy
),
windowed AS (
  SELECT
    i_manufact_id,
    sum_sales,
    AVG(sum_sales) OVER (PARTITION BY i_manufact_id) AS avg_quarterly_sales
  FROM aggregated
)
SELECT
  i_manufact_id,
  sum_sales,
  avg_quarterly_sales
FROM windowed
WHERE
  CASE
    WHEN avg_quarterly_sales > 0
    THEN ABS(sum_sales - avg_quarterly_sales) / avg_quarterly_sales
    ELSE NULL
  END > 0.1
ORDER BY
  avg_quarterly_sales,
  sum_sales,
  i_manufact_id
LIMIT 100