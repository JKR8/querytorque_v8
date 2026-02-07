WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1998
      AND d_moy BETWEEN 4 AND 4 + 3
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Children', 'Electronics', 'Men')
      AND i_manager_id BETWEEN 51 AND 60
),
sales_customers AS (
    SELECT DISTINCT ss_customer_sk AS customer_key,
           TRUE AS store_sales
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_item ON ss_item_sk = i_item_sk
    WHERE ss_sales_price / ss_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
    
    UNION ALL
    
    SELECT DISTINCT ws_bill_customer_sk AS customer_key,
           TRUE AS web_sales
    FROM web_sales
    JOIN filtered_date ON ws_sold_date_sk = d_date_sk
    JOIN filtered_item ON ws_item_sk = i_item_sk
    WHERE ws_sales_price / ws_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
    
    UNION ALL
    
    SELECT DISTINCT cs_ship_customer_sk AS customer_key,
           TRUE AS catalog_sales
    FROM catalog_sales
    JOIN filtered_date ON cs_sold_date_sk = d_date_sk
    JOIN filtered_item ON cs_item_sk = i_item_sk
    WHERE cs_sales_price / cs_list_price BETWEEN 11 * 0.01 AND 21 * 0.01
),
unified_sales AS (
    SELECT customer_key,
           BOOL_OR(store_sales) AS store_sales,
           BOOL_OR(web_sales) AS web_sales,
           BOOL_OR(catalog_sales) AS catalog_sales
    FROM sales_customers
    GROUP BY customer_key
)
SELECT cd_gender,
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
FROM customer c
JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
JOIN customer_demographics cd ON cd_demo_sk = c.c_current_cdemo_sk
JOIN unified_sales us ON c.c_customer_sk = us.customer_key
WHERE ca_county IN ('Hickman County', 'Ohio County', 'Parke County', 
                    'Pointe Coupee Parish', 'Siskiyou County')
  AND c.c_birth_month IN (3, 8)
  AND cd_marital_status IN ('S', 'U', 'M')
  AND cd_education_status IN ('Secondary', 'Advanced Degree', '4 yr Degree')
  AND cd_gender = 'M'
  AND us.store_sales = TRUE
  AND (us.web_sales = TRUE OR us.catalog_sales = TRUE)
GROUP BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
ORDER BY cd_gender,
         cd_marital_status,
         cd_education_status,
         cd_purchase_estimate,
         cd_credit_rating,
         cd_dep_count,
         cd_dep_employed_count,
         cd_dep_college_count
LIMIT 100;