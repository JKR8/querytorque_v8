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
    SELECT c_customer_sk, c_current_addr_sk, c_birth_year
    FROM customer
    WHERE c_birth_month = 1
),
filtered_customer_address AS (
    SELECT ca_address_sk, ca_country, ca_state, ca_county
    FROM customer_address
    WHERE ca_state IN ('GA', 'LA', 'SD')
)
SELECT
    filtered_item.i_item_id,
    filtered_customer_address.ca_country,
    filtered_customer_address.ca_state,
    filtered_customer_address.ca_county,
    AVG(CAST(cs_quantity AS DECIMAL(12, 2))) AS agg1,
    AVG(CAST(cs_list_price AS DECIMAL(12, 2))) AS agg2,
    AVG(CAST(cs_coupon_amt AS DECIMAL(12, 2))) AS agg3,
    AVG(CAST(cs_sales_price AS DECIMAL(12, 2))) AS agg4,
    AVG(CAST(cs_net_profit AS DECIMAL(12, 2))) AS agg5,
    AVG(CAST(filtered_customer.c_birth_year AS DECIMAL(12, 2))) AS agg6
FROM catalog_sales
INNER JOIN filtered_date ON catalog_sales.cs_sold_date_sk = filtered_date.d_date_sk
INNER JOIN filtered_item ON catalog_sales.cs_item_sk = filtered_item.i_item_sk
INNER JOIN filtered_customer_demographics ON catalog_sales.cs_bill_cdemo_sk = filtered_customer_demographics.cd_demo_sk
INNER JOIN filtered_customer ON catalog_sales.cs_bill_customer_sk = filtered_customer.c_customer_sk
INNER JOIN filtered_customer_address ON filtered_customer.c_current_addr_sk = filtered_customer_address.ca_address_sk
WHERE catalog_sales.cs_wholesale_cost BETWEEN 52 AND 57
GROUP BY ROLLUP (
    filtered_item.i_item_id,
    filtered_customer_address.ca_country,
    filtered_customer_address.ca_state,
    filtered_customer_address.ca_county
)
ORDER BY
    filtered_customer_address.ca_country,
    filtered_customer_address.ca_state,
    filtered_customer_address.ca_county,
    filtered_item.i_item_id
LIMIT 100