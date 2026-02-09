WITH filtered_dates AS (
  SELECT d_date_sk, d_qoy, d_year
  FROM date_dim
  WHERE d_year = 1998
    AND d_qoy IN (1, 2, 3)
),
filtered_items AS (
  SELECT i_item_sk
  FROM item
  WHERE i_color IN ('blanched', 'rosy')
    AND i_manager_id BETWEEN 16 AND 35
),
filtered_addresses AS (
  SELECT ca_address_sk, ca_county
  FROM customer_address
  WHERE ca_state IN ('TX', 'VA')
),
combined_sales AS (
  SELECT
    fa.ca_county,
    fd.d_qoy,
    fd.d_year,
    SUM(CASE WHEN ss.ss_item_sk IS NOT NULL THEN ss.ss_ext_sales_price END) AS store_sales,
    SUM(CASE WHEN ws.ws_item_sk IS NOT NULL THEN ws.ws_ext_sales_price END) AS web_sales
  FROM filtered_dates fd
  LEFT JOIN store_sales ss ON ss.ss_sold_date_sk = fd.d_date_sk
    AND ss.ss_list_price BETWEEN 286 AND 300
  LEFT JOIN web_sales ws ON ws.ws_sold_date_sk = fd.d_date_sk
    AND ws.ws_list_price BETWEEN 286 AND 300
  INNER JOIN filtered_items fi ON (ss.ss_item_sk = fi.i_item_sk OR ws.ws_item_sk = fi.i_item_sk)
  INNER JOIN filtered_addresses fa ON (ss.ss_addr_sk = fa.ca_address_sk OR ws.ws_bill_addr_sk = fa.ca_address_sk)
  GROUP BY fa.ca_county, fd.d_qoy, fd.d_year
),
pivoted AS (
  SELECT
    ca_county,
    d_year,
    MAX(CASE WHEN d_qoy = 1 THEN store_sales END) AS store_q1,
    MAX(CASE WHEN d_qoy = 2 THEN store_sales END) AS store_q2,
    MAX(CASE WHEN d_qoy = 3 THEN store_sales END) AS store_q3,
    MAX(CASE WHEN d_qoy = 1 THEN web_sales END) AS web_q1,
    MAX(CASE WHEN d_qoy = 2 THEN web_sales END) AS web_q2,
    MAX(CASE WHEN d_qoy = 3 THEN web_sales END) AS web_q3
  FROM combined_sales
  GROUP BY ca_county, d_year
  HAVING
    COUNT(CASE WHEN d_qoy = 1 THEN 1 END) = 1
    AND COUNT(CASE WHEN d_qoy = 2 THEN 1 END) = 1
    AND COUNT(CASE WHEN d_qoy = 3 THEN 1 END) = 1
)
SELECT
  ca_county,
  d_year,
  CASE WHEN web_q1 > 0 THEN web_q2 / web_q1 ELSE NULL END AS web_q1_q2_increase,
  CASE WHEN store_q1 > 0 THEN store_q2 / store_q1 ELSE NULL END AS store_q1_q2_increase,
  CASE WHEN web_q2 > 0 THEN web_q3 / web_q2 ELSE NULL END AS web_q2_q3_increase,
  CASE WHEN store_q2 > 0 THEN store_q3 / store_q2 ELSE NULL END AS store_q2_q3_increase
FROM pivoted
WHERE
  CASE WHEN web_q1 > 0 THEN web_q2 / web_q1 ELSE NULL END
    > CASE WHEN store_q1 > 0 THEN store_q2 / store_q1 ELSE NULL END
  AND CASE WHEN web_q2 > 0 THEN web_q3 / web_q2 ELSE NULL END
    > CASE WHEN store_q2 > 0 THEN store_q3 / store_q2 ELSE NULL END
ORDER BY web_q1_q2_increase