WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_cd1 AS (
    SELECT cd_demo_sk, cd_dep_count
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_education_status = 'Advanced Degree'
),
filtered_customer AS (
    SELECT 
        c_customer_sk,
        c_current_cdemo_sk,
        c_current_addr_sk,
        c_birth_year
    FROM customer
    WHERE c_birth_month IN (10, 7, 8, 4, 1, 2)
),
filtered_address AS (
    SELECT 
        ca_address_sk,
        ca_country,
        ca_state,
        ca_county
    FROM customer_address
    WHERE ca_state IN ('WA', 'GA', 'NC', 'ME', 'WY', 'OK', 'IN')
),
joined_facts AS (
    SELECT 
        cs.cs_item_sk,
        cs.cs_quantity,
        cs.cs_list_price,
        cs.cs_coupon_amt,
        cs.cs_sales_price,
        cs.cs_net_profit,
        cd1.cd_dep_count,
        c.c_birth_year,
        c.c_current_cdemo_sk,
        c.c_current_addr_sk,
        a.ca_country,
        a.ca_state,
        a.ca_county
    FROM catalog_sales cs
    JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_cd1 cd1 ON cs.cs_bill_cdemo_sk = cd1.cd_demo_sk
    JOIN filtered_customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
    JOIN filtered_address a ON c.c_current_addr_sk = a.ca_address_sk
)
SELECT
    i.i_item_id,
    jf.ca_country,
    jf.ca_state,
    jf.ca_county,
    AVG(CAST(jf.cs_quantity AS DECIMAL(12, 2))) AS agg1,
    AVG(CAST(jf.cs_list_price AS DECIMAL(12, 2))) AS agg2,
    AVG(CAST(jf.cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
    AVG(CAST(jf.cs_sales_price AS DECIMAL(12, 2))) AS agg4,
    AVG(CAST(jf.cs_net_profit AS DECIMAL(12, 2))) AS agg5,
    AVG(CAST(jf.c_birth_year AS DECIMAL(12, 2))) AS agg6,
    AVG(CAST(jf.cd_dep_count AS DECIMAL(12, 2))) AS agg7
FROM joined_facts jf
JOIN item i ON jf.cs_item_sk = i.i_item_sk
JOIN customer_demographics cd2 ON jf.c_current_cdemo_sk = cd2.cd_demo_sk
GROUP BY ROLLUP (
    i.i_item_id,
    jf.ca_country,
    jf.ca_state,
    jf.ca_county
)
ORDER BY
    jf.ca_country,
    jf.ca_state,
    jf.ca_county,
    i.i_item_id
LIMIT 100;