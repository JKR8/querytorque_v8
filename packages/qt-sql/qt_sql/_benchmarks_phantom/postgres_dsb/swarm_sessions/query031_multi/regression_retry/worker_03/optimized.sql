WITH filtered_date AS (
  SELECT d_date_sk
  FROM date_dim
  WHERE d_year = 1998
    AND d_qoy IN (1, 2, 3)
),
filtered_item AS (
  SELECT i_item_sk
  FROM item
  WHERE i_color IN ('blanched', 'rosy')
    AND i_manager_id BETWEEN 16 AND 35
),
filtered_address AS (
  SELECT ca_address_sk, ca_county
  FROM customer_address
  WHERE ca_state IN ('TX', 'VA')
),
ss_agg AS (
  SELECT
    ca.ca_county,
    d.d_qoy,
    SUM(ss_ext_sales_price) AS store_sales
  FROM store_sales ss
  JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
  JOIN filtered_address ca ON ss.ss_addr_sk = ca.ca_address_sk
  JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
  WHERE ss.ss_list_price BETWEEN 286 AND 300
  GROUP BY ca.ca_county, d.d_qoy
),
ws_agg AS (
  SELECT
    ca.ca_county,
    d.d_qoy,
    SUM(ws_ext_sales_price) AS web_sales
  FROM web_sales ws
  JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
  JOIN filtered_address ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
  JOIN filtered_item i ON ws.ws_item_sk = i.i_item_sk
  WHERE ws.ws_list_price BETWEEN 286 AND 300
  GROUP BY ca.ca_county, d.d_qoy
),
combined AS (
  SELECT
    COALESCE(s.ca_county, w.ca_county) AS ca_county,
    COALESCE(s.d_qoy, w.d_qoy) AS d_qoy,
    s.store_sales,
    w.web_sales
  FROM ss_agg s
  FULL OUTER JOIN ws_agg w ON s.ca_county = w.ca_county AND s.d_qoy = w.d_qoy
  WHERE s.ca_county IS NOT NULL AND w.ca_county IS NOT NULL
),
windowed AS (
  SELECT
    ca_county,
    store_sales,
    LAG(store_sales, 1) OVER (PARTITION BY ca_county ORDER BY d_qoy) AS prev_store_sales,
    LEAD(store_sales, 1) OVER (PARTITION BY ca_county ORDER BY d_qoy) AS next_store_sales,
    web_sales,
    LAG(web_sales, 1) OVER (PARTITION BY ca_county ORDER BY d_qoy) AS prev_web_sales,
    LEAD(web_sales, 1) OVER (PARTITION BY ca_county ORDER BY d_qoy) AS next_web_sales,
    d_qoy
  FROM combined
)
SELECT
  ca_county,
  1998 AS d_year,
  CASE WHEN prev_web_sales > 0 THEN web_sales / prev_web_sales ELSE NULL END AS web_q1_q2_increase,
  CASE WHEN prev_store_sales > 0 THEN store_sales / prev_store_sales ELSE NULL END AS store_q1_q2_increase,
  CASE WHEN web_sales > 0 THEN next_web_sales / web_sales ELSE NULL END AS web_q2_q3_increase,
  CASE WHEN store_sales > 0 THEN next_store_sales / store_sales ELSE NULL END AS store_q2_q3_increase
FROM windowed
WHERE d_qoy = 2
  AND CASE WHEN prev_web_sales > 0 THEN web_sales / prev_web_sales ELSE NULL END > 
      CASE WHEN prev_store_sales > 0 THEN store_sales / prev_store_sales ELSE NULL END
  AND CASE WHEN web_sales > 0 THEN next_web_sales / web_sales ELSE NULL END > 
      CASE WHEN store_sales > 0 THEN next_store_sales / store_sales ELSE NULL END
  AND prev_web_sales IS NOT NULL
  AND web_sales IS NOT NULL
  AND next_web_sales IS NOT NULL
  AND prev_store_sales IS NOT NULL
  AND store_sales IS NOT NULL
  AND next_store_sales IS NOT NULL
ORDER BY web_q1_q2_increase;