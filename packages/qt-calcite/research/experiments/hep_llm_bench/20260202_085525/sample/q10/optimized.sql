SELECT customer_demographics.cd_gender AS CD_GENDER, customer_demographics.cd_marital_status AS CD_MARITAL_STATUS, customer_demographics.cd_education_status AS CD_EDUCATION_STATUS, COUNT(*) AS CNT1, customer_demographics.cd_purchase_estimate AS CD_PURCHASE_ESTIMATE, COUNT(*) AS CNT2, customer_demographics.cd_credit_rating AS CD_CREDIT_RATING, COUNT(*) AS CNT3, customer_demographics.cd_dep_count AS CD_DEP_COUNT, COUNT(*) AS CNT4, customer_demographics.cd_dep_employed_count AS CD_DEP_EMPLOYED_COUNT, COUNT(*) AS CNT5, customer_demographics.cd_dep_college_count AS CD_DEP_COLLEGE_COUNT, COUNT(*) AS CNT6
FROM (SELECT $cor0.c_customer_sk, $cor0.c_customer_id, $cor0.c_current_cdemo_sk, $cor0.c_current_hdemo_sk, $cor0.c_current_addr_sk, $cor0.c_first_shipto_date_sk, $cor0.c_first_sales_date_sk, $cor0.c_salutation, $cor0.c_first_name, $cor0.c_last_name, $cor0.c_preferred_cust_flag, $cor0.c_birth_day, $cor0.c_birth_month, $cor0.c_birth_year, $cor0.c_birth_country, $cor0.c_login, $cor0.c_email_address, $cor0.c_last_review_date_sk
FROM ((customer AS $cor0, LATERAL (SELECT TRUE AS i
FROM store_sales,
date_dim
WHERE $cor0.c_customer_sk = store_sales.ss_customer_sk AND store_sales.ss_sold_date_sk = date_dim.d_date_sk AND date_dim.d_year = 2002 AND date_dim.d_moy >= 1 AND date_dim.d_moy <= 1 + 3
GROUP BY TRUE) AS t1) AS $cor0, LATERAL (SELECT TRUE AS i
FROM web_sales,
date_dim AS date_dim0
WHERE $cor0.c_customer_sk = web_sales.ws_bill_customer_sk AND web_sales.ws_sold_date_sk = date_dim0.d_date_sk AND date_dim0.d_year = 2002 AND date_dim0.d_moy >= 1 AND date_dim0.d_moy <= 1 + 3
GROUP BY TRUE) AS t4) AS $cor0,
LATERAL (SELECT TRUE AS i
FROM catalog_sales,
date_dim AS date_dim1
WHERE $cor0.c_customer_sk = catalog_sales.cs_ship_customer_sk AND catalog_sales.cs_sold_date_sk = date_dim1.d_date_sk AND date_dim1.d_year = 2002 AND date_dim1.d_moy >= 1 AND date_dim1.d_moy <= 1 + 3
GROUP BY TRUE) AS t7
WHERE $cor0.i0 IS NOT NULL OR $cor0.i1 IS NOT NULL) AS t9
INNER JOIN (SELECT *
FROM customer_address
WHERE ca_county IN ('Dona Ana County', 'Jefferson County', 'La Porte County', 'Rush County', 'Toole County')) AS t10 ON t9.c_current_addr_sk = t10.ca_address_sk
INNER JOIN customer_demographics ON t9.c_current_cdemo_sk = customer_demographics.cd_demo_sk
GROUP BY customer_demographics.cd_gender, customer_demographics.cd_marital_status, customer_demographics.cd_education_status, customer_demographics.cd_purchase_estimate, customer_demographics.cd_credit_rating, customer_demographics.cd_dep_count, customer_demographics.cd_dep_employed_count, customer_demographics.cd_dep_college_count
ORDER BY customer_demographics.cd_gender, customer_demographics.cd_marital_status, customer_demographics.cd_education_status, customer_demographics.cd_purchase_estimate, customer_demographics.cd_credit_rating, customer_demographics.cd_dep_count, customer_demographics.cd_dep_employed_count, customer_demographics.cd_dep_college_count
FETCH NEXT 100 ROWS ONLY