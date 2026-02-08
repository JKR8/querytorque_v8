WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
      AND d_qoy < 4
)
SELECT
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    COUNT(*) AS cnt1,
    MAX(cd_dep_count) AS "MAX(cd_dep_count)",
    SUM(cd_dep_count) AS "SUM(cd_dep_count)",
    MAX(cd_dep_count) AS "MAX(cd_dep_count)",
    cd_dep_employed_count,
    COUNT(*) AS cnt2,
    MAX(cd_dep_employed_count) AS "MAX(cd_dep_employed_count)",
    SUM(cd_dep_employed_count) AS "SUM(cd_dep_employed_count)",
    MAX(cd_dep_employed_count) AS "MAX(cd_dep_employed_count)",
    cd_dep_college_count,
    COUNT(*) AS cnt3,
    MAX(cd_dep_college_count) AS "MAX(cd_dep_college_count)",
    SUM(cd_dep_college_count) AS "SUM(cd_dep_college_count)",
    MAX(cd_dep_college_count) AS "MAX(cd_dep_college_count)"
FROM customer AS c
JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
WHERE EXISTS (
    SELECT 1
    FROM store_sales AS ss
    JOIN filtered_dates AS fd ON ss.ss_sold_date_sk = fd.d_date_sk
    WHERE ss.ss_customer_sk = c.c_customer_sk
)
AND (
    EXISTS (
        SELECT 1
        FROM web_sales AS ws
        JOIN filtered_dates AS fd ON ws.ws_sold_date_sk = fd.d_date_sk
        WHERE ws.ws_bill_customer_sk = c.c_customer_sk
    )
    OR EXISTS (
        SELECT 1
        FROM catalog_sales AS cs
        JOIN filtered_dates AS fd ON cs.cs_sold_date_sk = fd.d_date_sk
        WHERE cs.cs_ship_customer_sk = c.c_customer_sk
    )
)
GROUP BY
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
ORDER BY
    ca_state,
    cd_gender,
    cd_marital_status,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
LIMIT 100