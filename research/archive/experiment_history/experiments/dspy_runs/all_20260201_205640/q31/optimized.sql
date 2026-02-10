-- start query 31 in stream 0 using template query31.tpl
WITH ss AS (
    SELECT 
        ca_county,
        d_qoy,
        d_year,
        SUM(ss_ext_sales_price) AS store_sales
    FROM store_sales
    JOIN date_dim ON ss_sold_date_sk = d_date_sk
    JOIN customer_address ON ss_addr_sk = ca_address_sk
    WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
    GROUP BY ca_county, d_qoy, d_year
),
ws AS (
    SELECT 
        ca_county,
        d_qoy,
        d_year,
        SUM(ws_ext_sales_price) AS web_sales
    FROM web_sales
    JOIN date_dim ON ws_sold_date_sk = d_date_sk
    JOIN customer_address ON ws_bill_addr_sk = ca_address_sk
    WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
    GROUP BY ca_county, d_qoy, d_year
),
combined AS (
    SELECT 
        COALESCE(ss.ca_county, ws.ca_county) AS ca_county,
        COALESCE(ss.d_qoy, ws.d_qoy) AS d_qoy,
        COALESCE(ss.d_year, ws.d_year) AS d_year,
        COALESCE(ss.store_sales, 0) AS store_sales,
        COALESCE(ws.web_sales, 0) AS web_sales
    FROM ss
    FULL OUTER JOIN ws 
        ON ss.ca_county = ws.ca_county 
        AND ss.d_qoy = ws.d_qoy 
        AND ss.d_year = ws.d_year
),
aggregated AS (
    SELECT 
        ca_county,
        MAX(CASE WHEN d_qoy = 1 THEN store_sales END) AS store_q1,
        MAX(CASE WHEN d_qoy = 2 THEN store_sales END) AS store_q2,
        MAX(CASE WHEN d_qoy = 3 THEN store_sales END) AS store_q3,
        MAX(CASE WHEN d_qoy = 1 THEN web_sales END) AS web_q1,
        MAX(CASE WHEN d_qoy = 2 THEN web_sales END) AS web_q2,
        MAX(CASE WHEN d_qoy = 3 THEN web_sales END) AS web_q3
    FROM combined
    GROUP BY ca_county
)
SELECT 
    ca_county,
    2000 AS d_year,
    CASE WHEN web_q1 > 0 THEN web_q2 / web_q1 ELSE NULL END AS web_q1_q2_increase,
    CASE WHEN store_q1 > 0 THEN store_q2 / store_q1 ELSE NULL END AS store_q1_q2_increase,
    CASE WHEN web_q2 > 0 THEN web_q3 / web_q2 ELSE NULL END AS web_q2_q3_increase,
    CASE WHEN store_q2 > 0 THEN store_q3 / store_q2 ELSE NULL END AS store_q2_q3_increase
FROM aggregated
WHERE 
    (web_q1 > 0 AND web_q2 > 0 AND store_q1 > 0 AND store_q2 > 0 AND web_q2 / web_q1 > store_q2 / store_q1)
    AND (web_q2 > 0 AND web_q3 > 0 AND store_q2 > 0 AND store_q3 > 0 AND web_q3 / web_q2 > store_q3 / store_q2)
ORDER BY web_q1_q2_increase;

-- end query 31 in stream 0 using template query31.tpl