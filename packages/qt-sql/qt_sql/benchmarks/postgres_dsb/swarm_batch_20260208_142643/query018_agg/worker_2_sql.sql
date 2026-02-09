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
filtered_customer_demographics AS (
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
filtered_customer_address AS (
    SELECT ca_address_sk, ca_country, ca_state, ca_county
    FROM customer_address
    WHERE ca_state IN ('GA', 'LA', 'SD')
)
SELECT
    fi.i_item_id,
    fca.ca_country,
    fca.ca_state,
    fca.ca_county,
    AVG(CAST(cs.cs_quantity AS DECIMAL(12, 2))) AS agg1,
    AVG(CAST(cs.cs_list_price AS DECIMAL(12, 2))) AS agg2,
    AVG(CAST(cs.cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
    AVG(CAST(cs.cs_sales_price AS DECIMAL(12, 2))) AS agg4,
    AVG(CAST(cs.cs_net_profit AS DECIMAL(12, 2))) AS agg5,
    AVG(CAST(fc.c_birth_year AS DECIMAL(12, 2))) AS agg6
FROM catalog_sales cs
JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
JOIN filtered_item fi ON cs.cs_item_sk = fi.i_item_sk
JOIN filtered_customer_demographics fcd ON cs.cs_bill_cdemo_sk = fcd.cd_demo_sk
JOIN filtered_customer fc ON cs.cs_bill_customer_sk = fc.c_customer_sk
JOIN filtered_customer_address fca ON fc.c_current_addr_sk = fca.ca_address_sk
WHERE cs.cs_wholesale_cost BETWEEN 52 AND 57
GROUP BY ROLLUP (
    fi.i_item_id,
    fca.ca_country,
    fca.ca_state,
    fca.ca_county
)
ORDER BY
    fca.ca_country,
    fca.ca_state,
    fca.ca_county,
    fi.i_item_id
LIMIT 100