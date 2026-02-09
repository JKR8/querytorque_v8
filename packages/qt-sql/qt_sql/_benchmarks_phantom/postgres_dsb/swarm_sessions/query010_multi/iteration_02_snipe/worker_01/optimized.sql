WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
      AND d_moy BETWEEN 4 AND 7
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Men')
      AND i_manager_id BETWEEN 51 AND 60
),
store_sales_customers AS (
    SELECT DISTINCT ss_customer_sk
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_item ON ss_item_sk = i_item_sk
    WHERE ss_sales_price / ss_list_price BETWEEN 0.11 AND 0.21
),
eligible_customers AS (
    SELECT 
        c.c_customer_sk,
        cd.cd_gender,
        cd.cd_marital_status,
        cd.cd_education_status,
        cd.cd_purchase_estimate,
        cd.cd_credit_rating,
        cd.cd_dep_count,
        cd.cd_dep_employed_count,
        cd.cd_dep_college_count
    FROM store_sales_customers ssc
    JOIN customer c ON ssc.ss_customer_sk = c.c_customer_sk
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
    WHERE ca.ca_county IN ('Hickman County', 'Ohio County', 'Parke County', 'Pointe Coupee Parish', 'Siskiyou County')
      AND c.c_birth_month IN (3, 8)
      AND cd.cd_marital_status IN ('S', 'U', 'M')
      AND cd.cd_education_status IN ('Secondary', 'Advanced Degree', '4 yr Degree')
      AND cd.cd_gender = 'M'
      AND (EXISTS (
          SELECT 1
          FROM web_sales
          JOIN filtered_date ON ws_sold_date_sk = d_date_sk
          JOIN filtered_item ON ws_item_sk = i_item_sk
          WHERE ws_bill_customer_sk = c.c_customer_sk
            AND ws_sales_price / ws_list_price BETWEEN 0.11 AND 0.21
      ) OR EXISTS (
          SELECT 1
          FROM catalog_sales
          JOIN filtered_date ON cs_sold_date_sk = d_date_sk
          JOIN filtered_item ON cs_item_sk = i_item_sk
          WHERE cs_ship_customer_sk = c.c_customer_sk
            AND cs_sales_price / cs_list_price BETWEEN 0.11 AND 0.21
      ))
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
FROM eligible_customers
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