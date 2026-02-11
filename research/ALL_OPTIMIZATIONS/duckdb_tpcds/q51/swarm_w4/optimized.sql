WITH web_v1 AS (
  SELECT
    ws_item_sk AS item_sk,
    d_date,
    SUM(SUM(ws_sales_price)) OVER (
      PARTITION BY ws_item_sk
      ORDER BY d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cume_sales
  FROM web_sales, date_dim
  WHERE
    ws_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND ws_item_sk IS NOT NULL
  GROUP BY ws_item_sk, d_date
),
store_v1 AS (
  SELECT
    ss_item_sk AS item_sk,
    d_date,
    SUM(SUM(ss_sales_price)) OVER (
      PARTITION BY ss_item_sk
      ORDER BY d_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cume_sales
  FROM store_sales, date_dim
  WHERE
    ss_sold_date_sk = d_date_sk
    AND d_month_seq BETWEEN 1216 AND 1216 + 11
    AND ss_item_sk IS NOT NULL
  GROUP BY ss_item_sk, d_date
),
-- Transform FULL OUTER JOIN to UNION pattern with explicit handling of NULLs
combined_data AS (
  -- Web-only rows
  SELECT
    w.item_sk,
    w.d_date,
    w.cume_sales AS web_sales,
    NULL AS store_sales
  FROM web_v1 w
  WHERE NOT EXISTS (
    SELECT 1 FROM store_v1 s 
    WHERE s.item_sk = w.item_sk AND s.d_date = w.d_date
  )
  
  UNION ALL
  
  -- Store-only rows
  SELECT
    s.item_sk,
    s.d_date,
    NULL AS web_sales,
    s.cume_sales AS store_sales
  FROM store_v1 s
  WHERE NOT EXISTS (
    SELECT 1 FROM web_v1 w 
    WHERE w.item_sk = s.item_sk AND w.d_date = s.d_date
  )
  
  UNION ALL
  
  -- Matching rows from both sources
  SELECT
    COALESCE(w.item_sk, s.item_sk) AS item_sk,
    COALESCE(w.d_date, s.d_date) AS d_date,
    w.cume_sales AS web_sales,
    s.cume_sales AS store_sales
  FROM web_v1 w
  INNER JOIN store_v1 s 
    ON w.item_sk = s.item_sk AND w.d_date = s.d_date
),
windowed_data AS (
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
  FROM combined_data
)
SELECT
  item_sk,
  d_date,
  web_sales,
  store_sales,
  web_cumulative,
  store_cumulative
FROM windowed_data
WHERE web_cumulative > store_cumulative
ORDER BY item_sk, d_date
LIMIT 100;