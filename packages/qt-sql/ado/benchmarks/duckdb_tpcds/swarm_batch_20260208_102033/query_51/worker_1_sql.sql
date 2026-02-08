WITH filtered_dates AS (
  SELECT d_date_sk, d_date
  FROM date_dim
  WHERE d_month_seq BETWEEN 1216 AND 1216 + 11
),
web_v1 AS (
  SELECT
    ws_item_sk AS item_sk,
    d.d_date,
    SUM(SUM(ws_sales_price)) OVER (
      PARTITION BY ws_item_sk
      ORDER BY d.d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cume_sales
  FROM web_sales
  JOIN filtered_dates d ON ws_sold_date_sk = d.d_date_sk
  WHERE NOT ws_item_sk IS NULL
  GROUP BY ws_item_sk, d.d_date
),
store_v1 AS (
  SELECT
    ss_item_sk AS item_sk,
    d.d_date,
    SUM(SUM(ss_sales_price)) OVER (
      PARTITION BY ss_item_sk
      ORDER BY d.d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cume_sales
  FROM store_sales
  JOIN filtered_dates d ON ss_sold_date_sk = d.d_date_sk
  WHERE NOT ss_item_sk IS NULL
  GROUP BY ss_item_sk, d.d_date
)
SELECT *
FROM (
  SELECT
    item_sk,
    d_date,
    web_sales,
    store_sales,
    MAX(web_sales) OVER (
      PARTITION BY item_sk
      ORDER BY d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS web_cumulative,
    MAX(store_sales) OVER (
      PARTITION BY item_sk
      ORDER BY d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS store_cumulative
  FROM (
    SELECT
      COALESCE(web.item_sk, store.item_sk) AS item_sk,
      COALESCE(web.d_date, store.d_date) AS d_date,
      web.cume_sales AS web_sales,
      store.cume_sales AS store_sales
    FROM web_v1 AS web
    FULL OUTER JOIN store_v1 AS store
      ON (web.item_sk = store.item_sk AND web.d_date = store.d_date)
  ) AS x
) AS y
WHERE web_cumulative > store_cumulative
ORDER BY item_sk, d_date
LIMIT 100