WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Jewelry'
),
filtered_cd AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_education_status = 'College'
),
filtered_customer AS (
    SELECT c_customer_sk, c_birth_year, c_current_addr_sk
    FROM customer
    WHERE c_birth_month = 1
),
filtered_ca AS (
    SELECT ca_address_sk, ca_country, ca_state, ca_county
    FROM customer_address
    WHERE ca_state IN ('GA', 'LA', 'SD')
),
filtered_cs AS (
    SELECT 
        cs_sold_date_sk,
        cs_item_sk,
        cs_bill_cdemo_sk,
        cs_bill_customer_sk,
        cs_quantity,
        cs_list_price,
        cs_coupon_amt,
        cs_sales_price,
        cs_net_profit
    FROM catalog_sales
    WHERE cs_wholesale_cost BETWEEN 52 AND 57
)
SELECT
    i.i_item_id,
    ca.ca_country,
    ca.ca_state,
    ca.ca_county,
    AVG(CAST(cs.cs_quantity AS DECIMAL(12, 2))) AS agg1,
    AVG(CAST(cs.cs_list_price AS DECIMAL(12, 2))) AS agg2,
    AVG(CAST(cs.cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
    AVG(CAST(cs.cs_sales_price AS DECIMAL(12, 2))) AS agg4,
    AVG(CAST(cs.cs_net_profit AS DECIMAL(12, 2))) AS agg5,
    AVG(CAST(c.c_birth_year AS DECIMAL(12, 2))) AS agg6
FROM filtered_cs cs
JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
JOIN filtered_cd cd ON cs.cs_bill_cdemo_sk = cd.cd_demo_sk
JOIN filtered_customer c ON cs.cs_bill_customer_sk = c.c_customer_sk
JOIN filtered_ca ca ON c.c_current_addr_sk = ca.ca_address_sk
GROUP BY ROLLUP (
    i.i_item_id,
    ca.ca_country,
    ca.ca_state,
    ca.ca_county
)
ORDER BY
    ca.ca_country,
    ca.ca_state,
    ca.ca_county,
    i.i_item_id
LIMIT 100;