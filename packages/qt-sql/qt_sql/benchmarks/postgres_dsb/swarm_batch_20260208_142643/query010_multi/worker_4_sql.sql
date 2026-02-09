WITH 
-- Pre-filter dimensions for better selectivity and cardinality estimates
filtered_date AS (
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
-- Base customer set with all demographic info and filters applied
base_customers AS (
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
    FROM customer c
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    JOIN customer_demographics cd ON cd.cd_demo_sk = c.c_current_cdemo_sk
    WHERE ca.ca_county IN (
        'Hickman County',
        'Ohio County',
        'Parke County', 
        'Pointe Coupee Parish',
        'Siskiyou County'
    )
      AND c.c_birth_month IN (3, 8)
      AND cd.cd_marital_status IN ('S', 'U', 'M')
      AND cd.cd_education_status IN ('Secondary', 'Advanced Degree', '4 yr Degree')
      AND cd.cd_gender = 'M'
),
-- Store sales customers (mandatory condition)
store_customers AS (
    SELECT DISTINCT bc.*
    FROM base_customers bc
    JOIN store_sales ss ON bc.c_customer_sk = ss.ss_customer_sk
    JOIN filtered_date fd ON ss.ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
    WHERE ss.ss_sales_price / ss.ss_list_price BETWEEN 0.11 AND 0.21
),
-- Web sales customers (first channel)
web_customers AS (
    SELECT DISTINCT bc.*
    FROM base_customers bc
    JOIN web_sales ws ON bc.c_customer_sk = ws.ws_bill_customer_sk
    JOIN filtered_date fd ON ws.ws_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON ws.ws_item_sk = fi.i_item_sk
    WHERE ws.ws_sales_price / ws.ws_list_price BETWEEN 0.11 AND 0.21
),
-- Catalog sales customers (second channel)
catalog_customers AS (
    SELECT DISTINCT bc.*
    FROM base_customers bc
    JOIN catalog_sales cs ON bc.c_customer_sk = cs.cs_ship_customer_sk
    JOIN filtered_date fd ON cs.cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_item fi ON cs.cs_item_sk = fi.i_item_sk
    WHERE cs.cs_sales_price / cs.cs_list_price BETWEEN 0.11 AND 0.21
),
-- Transform OR condition into UNION ALL: customers with store sales AND (web OR catalog)
final_customers AS (
    -- Store + Web branch
    SELECT * FROM store_customers sc
    WHERE EXISTS (SELECT 1 FROM web_customers wc WHERE wc.c_customer_sk = sc.c_customer_sk)
    
    UNION ALL
    
    -- Store + Catalog branch (excluding those already counted in web branch)
    SELECT * FROM store_customers sc
    WHERE EXISTS (SELECT 1 FROM catalog_customers cc WHERE cc.c_customer_sk = sc.c_customer_sk)
      AND NOT EXISTS (SELECT 1 FROM web_customers wc WHERE wc.c_customer_sk = sc.c_customer_sk)
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
FROM final_customers
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