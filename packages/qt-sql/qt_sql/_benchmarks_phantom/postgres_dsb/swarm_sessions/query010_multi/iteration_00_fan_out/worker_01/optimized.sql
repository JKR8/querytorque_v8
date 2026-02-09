WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
      AND d_moy BETWEEN 4 AND 7
),
filtered_items AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Men')
      AND i_manager_id BETWEEN 51 AND 60
),
prefiltered_customers AS (
    SELECT 
        c.c_customer_sk,
        c.c_current_cdemo_sk,
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status,
        cd.cd_purchase_estimate,
        cd.cd_credit_rating,
        cd.cd_dep_count,
        cd.cd_dep_employed_count,
        cd.cd_dep_college_count
    FROM customer AS c
    JOIN customer_address AS ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics AS cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
    WHERE ca.ca_county IN ('Hickman County', 'Ohio County', 'Parke County', 'Pointe Coupee Parish', 'Siskiyou County')
      AND c.c_birth_month IN (3, 8)
      AND cd.cd_marital_status IN ('S', 'U', 'M')
      AND cd.cd_education_status IN ('Secondary', 'Advanced Degree', '4 yr Degree')
      AND cd.cd_gender = 'M'
)
SELECT
    cd_gender,
    cd_marital_status,
    cd_education_status,
    COUNT(*) AS cnt1,
    cd_purchase_estimate,
    COUNT(*) AS cnt2,
    cd_credit_rating,
    COUNT(*) AS cnt3,
    cd_dep_count,
    COUNT(*) AS cnt4,
    cd_dep_employed_count,
    COUNT(*) AS cnt5,
    cd_dep_college_count,
    COUNT(*) AS cnt6
FROM prefiltered_customers AS c
WHERE EXISTS (
    SELECT 1
    FROM store_sales AS ss
    JOIN filtered_dates AS d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_items AS i ON ss.ss_item_sk = i.i_item_sk
    WHERE ss.ss_customer_sk = c.c_customer_sk
      AND ss.ss_sales_price / ss.ss_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
)
AND (
    EXISTS (
        SELECT 1
        FROM web_sales AS ws
        JOIN filtered_dates AS d ON ws.ws_sold_date_sk = d.d_date_sk
        JOIN filtered_items AS i ON ws.ws_item_sk = i.i_item_sk
        WHERE ws.ws_bill_customer_sk = c.c_customer_sk
          AND ws.ws_sales_price / ws.ws_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
    )
    OR EXISTS (
        SELECT 1
        FROM catalog_sales AS cs
        JOIN filtered_dates AS d ON cs.cs_sold_date_sk = d.d_date_sk
        JOIN filtered_items AS i ON cs.cs_item_sk = i.i_item_sk
        WHERE cs.cs_ship_customer_sk = c.c_customer_sk
          AND cs.cs_sales_price / cs.cs_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
    )
)
GROUP BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
ORDER BY
    cd_gender,
    cd_marital_status,
    cd_education_status,
    cd_purchase_estimate,
    cd_credit_rating,
    cd_dep_count,
    cd_dep_employed_count,
    cd_dep_college_count
LIMIT 100;