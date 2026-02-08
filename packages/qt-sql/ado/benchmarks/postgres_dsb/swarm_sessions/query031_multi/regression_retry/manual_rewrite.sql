-- Manual rewrite for query031_multi
-- Strategy: dimension_prefetch + early_filter + d_year pushdown
-- Key insight: Keep ss/ws SEPARATE (preserves PG CTE short-circuit)
-- Push d_year=1998 into CTEs to reduce date_dim scan
-- Pre-filter small dimension tables into CTEs for index reuse

WITH filtered_date AS (
  SELECT d_date_sk, d_qoy, d_year
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
ss AS (
  SELECT
    fa.ca_county,
    fd.d_qoy,
    fd.d_year,
    SUM(ss_ext_sales_price) AS store_sales
  FROM store_sales
  JOIN filtered_date fd ON ss_sold_date_sk = fd.d_date_sk
  JOIN filtered_address fa ON ss_addr_sk = fa.ca_address_sk
  JOIN filtered_item fi ON ss_item_sk = fi.i_item_sk
  WHERE ss_list_price BETWEEN 286 AND 300
  GROUP BY fa.ca_county, fd.d_qoy, fd.d_year
),
ws AS (
  SELECT
    fa.ca_county,
    fd.d_qoy,
    fd.d_year,
    SUM(ws_ext_sales_price) AS web_sales
  FROM web_sales
  JOIN filtered_date fd ON ws_sold_date_sk = fd.d_date_sk
  JOIN filtered_address fa ON ws_bill_addr_sk = fa.ca_address_sk
  JOIN filtered_item fi ON ws_item_sk = fi.i_item_sk
  WHERE ws_list_price BETWEEN 286 AND 300
  GROUP BY fa.ca_county, fd.d_qoy, fd.d_year
)
SELECT
  ss1.ca_county,
  ss1.d_year,
  ws2.web_sales / ws1.web_sales AS web_q1_q2_increase,
  ss2.store_sales / ss1.store_sales AS store_q1_q2_increase,
  ws3.web_sales / ws2.web_sales AS web_q2_q3_increase,
  ss3.store_sales / ss2.store_sales AS store_q2_q3_increase
FROM ss AS ss1
JOIN ss AS ss2 ON ss1.ca_county = ss2.ca_county AND ss2.d_qoy = 2
JOIN ss AS ss3 ON ss2.ca_county = ss3.ca_county AND ss3.d_qoy = 3
JOIN ws AS ws1 ON ss1.ca_county = ws1.ca_county AND ws1.d_qoy = 1
JOIN ws AS ws2 ON ws1.ca_county = ws2.ca_county AND ws2.d_qoy = 2
JOIN ws AS ws3 ON ws1.ca_county = ws3.ca_county AND ws3.d_qoy = 3
WHERE ss1.d_qoy = 1
  AND CASE WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales ELSE NULL END
     > CASE WHEN ss1.store_sales > 0 THEN ss2.store_sales / ss1.store_sales ELSE NULL END
  AND CASE WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales ELSE NULL END
     > CASE WHEN ss2.store_sales > 0 THEN ss3.store_sales / ss2.store_sales ELSE NULL END
ORDER BY web_q1_q2_increase
