WITH filtered_date AS (
    SELECT d_date_sk, d_qoy, d_year
    FROM date_dim
    WHERE d_year = 1998
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_color IN ('blanched', 'rosy')
      AND i_manager_id BETWEEN 16 AND 35
),
filtered_ca AS (
    SELECT ca_address_sk, ca_county
    FROM customer_address
    WHERE ca_state IN ('TX', 'VA')
),
ss AS (
    SELECT
        ca.ca_county,
        d.d_qoy,
        d.d_year,
        SUM(ss.ss_ext_sales_price) AS store_sales
    FROM store_sales ss
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_ca ca ON ss.ss_addr_sk = ca.ca_address_sk
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    WHERE ss.ss_list_price BETWEEN 286 AND 300
    GROUP BY ca.ca_county, d.d_qoy, d.d_year
),
ws AS (
    SELECT
        ca.ca_county,
        d.d_qoy,
        d.d_year,
        SUM(ws.ws_ext_sales_price) AS web_sales
    FROM web_sales ws
    JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN filtered_ca ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
    JOIN filtered_item i ON ws.ws_item_sk = i.i_item_sk
    WHERE ws.ws_list_price BETWEEN 286 AND 300
    GROUP BY ca.ca_county, d.d_qoy, d.d_year
)
SELECT
    ss1.ca_county,
    ss1.d_year,
    ws2.web_sales / ws1.web_sales AS web_q1_q2_increase,
    ss2.store_sales / ss1.store_sales AS store_q1_q2_increase,
    ws3.web_sales / ws2.web_sales AS web_q2_q3_increase,
    ss3.store_sales / ss2.store_sales AS store_q2_q3_increase
FROM ss ss1
JOIN ss ss2 ON ss1.ca_county = ss2.ca_county
JOIN ss ss3 ON ss2.ca_county = ss3.ca_county
JOIN ws ws1 ON ss1.ca_county = ws1.ca_county
JOIN ws ws2 ON ws1.ca_county = ws2.ca_county
JOIN ws ws3 ON ws1.ca_county = ws3.ca_county
WHERE ss1.d_qoy = 1
  AND ss2.d_qoy = 2
  AND ss3.d_qoy = 3
  AND ws1.d_qoy = 1
  AND ws2.d_qoy = 2
  AND ws3.d_qoy = 3
  AND CASE WHEN ws1.web_sales > 0 THEN ws2.web_sales / ws1.web_sales ELSE NULL END > 
     CASE WHEN ss1.store_sales > 0 THEN ss2.store_sales / ss1.store_sales ELSE NULL END
  AND CASE WHEN ws2.web_sales > 0 THEN ws3.web_sales / ws2.web_sales ELSE NULL END > 
     CASE WHEN ss2.store_sales > 0 THEN ss3.store_sales / ss2.store_sales ELSE NULL END
ORDER BY web_q1_q2_increase;