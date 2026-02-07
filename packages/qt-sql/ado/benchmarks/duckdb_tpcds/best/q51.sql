WITH web_v1 AS (
  SELECT
    ws_item_sk AS item_sk,
    d_date,
    SUM(SUM(ws_sales_price)) OVER (
      PARTITION BY ws_item_sk
      ORDER BY d_date
      rows BETWEEN UNBOUNDED preceding AND CURRENT ROW
    ) AS cume_sales
  FROM web_sales, date_dim
  WHERE
    ws_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND NOT ws_item_sk IS NULL
  GROUP BY
    ws_item_sk,
    d_date
), store_v1 AS (
  SELECT
    ss_item_sk AS item_sk,
    d_date,
    SUM(SUM(ss_sales_price)) OVER (
      PARTITION BY ss_item_sk
      ORDER BY d_date
      rows BETWEEN UNBOUNDED preceding AND CURRENT ROW
    ) AS cume_sales
  FROM store_sales, date_dim
  WHERE
    ss_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND NOT ss_item_sk IS NULL
  GROUP BY
    ss_item_sk,
    d_date
)
SELECT
  COALESCE(web.item_sk, store.item_sk) AS item_sk,
  COALESCE(web.d_date, store.d_date) AS d_date,
  web.cume_sales AS web_sales,
  store.cume_sales AS store_sales,
  web.cume_sales AS web_cumulative,
  store.cume_sales AS store_cumulative
FROM web_v1 AS web
INNER JOIN store_v1 AS store
  ON web.item_sk = store.item_sk AND web.d_date = store.d_date
WHERE web.cume_sales > store.cume_sales
ORDER BY item_sk, d_date
LIMIT 100
