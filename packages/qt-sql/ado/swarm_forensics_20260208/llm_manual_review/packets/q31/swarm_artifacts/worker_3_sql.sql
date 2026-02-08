WITH filtered_date AS (
    SELECT d_date_sk, d_qoy, d_year
    FROM date_dim
    WHERE d_year = 2000 AND d_qoy IN (1, 2, 3)
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_county
    FROM customer_address
),
store_sales_prefetch AS (
    SELECT
        ca_county,
        d_qoy,
        d_year,
        SUM(ss_ext_sales_price) AS store_sales
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_customer_address ON ss_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
),
web_sales_prefetch AS (
    SELECT
        ca_county,
        d_qoy,
        d_year,
        SUM(ws_ext_sales_price) AS web_sales
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = d_date_sk
    JOIN filtered_customer_address ON ws_bill_addr_sk = ca_address_sk
    GROUP BY ca_county, d_qoy, d_year
),
combined_quarters AS (
    SELECT
        ca_county,
        d_year,
        MAX(CASE WHEN d_qoy = 1 THEN store_sales END) AS store_q1,
        MAX(CASE WHEN d_qoy = 2 THEN store_sales END) AS store_q2,
        MAX(CASE WHEN d_qoy = 3 THEN store_sales END) AS store_q3,
        MAX(CASE WHEN d_qoy = 1 THEN web_sales END) AS web_q1,
        MAX(CASE WHEN d_qoy = 2 THEN web_sales END) AS web_q2,
        MAX(CASE WHEN d_qoy = 3 THEN web_sales END) AS web_q3
    FROM (
        SELECT ca_county, d_qoy, d_year, store_sales, NULL AS web_sales
        FROM store_sales_prefetch
        UNION ALL
        SELECT ca_county, d_qoy, d_year, NULL AS store_sales, web_sales
        FROM web_sales_prefetch
    ) combined
    GROUP BY ca_county, d_year
    HAVING 
        store_q1 IS NOT NULL AND store_q2 IS NOT NULL AND store_q3 IS NOT NULL
        AND web_q1 IS NOT NULL AND web_q2 IS NOT NULL AND web_q3 IS NOT NULL
)
SELECT
    ca_county,
    d_year,
    CASE WHEN web_q1 > 0 THEN web_q2 / web_q1 ELSE NULL END AS web_q1_q2_increase,
    CASE WHEN store_q1 > 0 THEN store_q2 / store_q1 ELSE NULL END AS store_q1_q2_increase,
    CASE WHEN web_q2 > 0 THEN web_q3 / web_q2 ELSE NULL END AS web_q2_q3_increase,
    CASE WHEN store_q2 > 0 THEN store_q3 / store_q2 ELSE NULL END AS store_q2_q3_increase
FROM combined_quarters
WHERE 
    CASE WHEN web_q1 > 0 THEN web_q2 / web_q1 ELSE NULL END > 
    CASE WHEN store_q1 > 0 THEN store_q2 / store_q1 ELSE NULL END
    AND CASE WHEN web_q2 > 0 THEN web_q3 / web_q2 ELSE NULL END > 
    CASE WHEN store_q2 > 0 THEN store_q3 / store_q2 ELSE NULL END
ORDER BY web_q1_q2_increase;