WITH date_filter AS MATERIALIZED (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
cd_filter AS MATERIALIZED (
    SELECT cd_demo_sk, cd_marital_status, cd_education_status
    FROM customer_demographics
    WHERE (cd_marital_status = 'U' AND cd_education_status = 'Primary')
       OR (cd_marital_status = 'W' AND cd_education_status = 'College')
       OR (cd_marital_status = 'D' AND cd_education_status = '2 yr Degree')
),
ca_filter AS MATERIALIZED (
    SELECT ca_address_sk, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND (ca_state IN ('MD', 'MN', 'IA')
        OR ca_state IN ('VA', 'IL', 'TX')
        OR ca_state IN ('MI', 'WI', 'IN'))
)
SELECT SUM(ss_quantity)
FROM store_sales
JOIN store ON s_store_sk = ss_store_sk
JOIN date_filter ON ss_sold_date_sk = d_date_sk
JOIN cd_filter ON cd_demo_sk = ss_cdemo_sk
JOIN ca_filter ON ss_addr_sk = ca_address_sk
WHERE 
(
  (
   cd_marital_status = 'U'
   AND cd_education_status = 'Primary'
   AND ss_sales_price between 100.00 and 150.00  
   )
 OR
  (
   cd_marital_status = 'W'
   AND cd_education_status = 'College'
   AND ss_sales_price between 50.00 and 100.00   
  )
 OR 
 (
   cd_marital_status = 'D'
   AND cd_education_status = '2 yr Degree'
   AND ss_sales_price between 150.00 and 200.00  
 )
)
AND
(
  (
  ca_state in ('MD', 'MN', 'IA')
  AND ss_net_profit between 0 and 2000  
  )
 OR
  (ca_state in ('VA', 'IL', 'TX')
  AND ss_net_profit between 150 and 3000 
  )
 OR
  (ca_state in ('MI', 'WI', 'IN')
  AND ss_net_profit between 50 and 25000 
  )
)
;