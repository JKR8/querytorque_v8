WITH filtered_fact AS (SELECT ss_quantity, ss_ext_sales_price, ss_ext_wholesale_cost
FROM store_sales
INNER JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk AND date_dim.d_year = 2001
INNER JOIN store ON store_sales.ss_store_sk = store.s_store_sk
INNER JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
INNER JOIN customer_demographics ON store_sales.ss_cdemo_sk = customer_demographics.cd_demo_sk
INNER JOIN customer_address ON store_sales.ss_addr_sk = customer_address.ca_address_sk
WHERE (
  (
    (customer_demographics.cd_marital_status = 'M' AND customer_demographics.cd_education_status = '2 yr Degree' AND store_sales.ss_sales_price BETWEEN 100.00 AND 150.00 AND household_demographics.hd_dep_count = 3)
    OR
    (customer_demographics.cd_marital_status = 'U' AND customer_demographics.cd_education_status = 'College' AND store_sales.ss_sales_price BETWEEN 50.00 AND 100.00 AND household_demographics.hd_dep_count = 1)
    OR
    (customer_demographics.cd_marital_status = 'S' AND customer_demographics.cd_education_status = 'Unknown' AND store_sales.ss_sales_price BETWEEN 150.00 AND 200.00 AND household_demographics.hd_dep_count = 1)
  )
  AND
  (
    (customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('CO', 'NC', 'TX') AND store_sales.ss_net_profit BETWEEN 100 AND 200)
    OR
    (customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('AR', 'NY', 'TX') AND store_sales.ss_net_profit BETWEEN 150 AND 300)
    OR
    (customer_address.ca_country = 'United States' AND customer_address.ca_state IN ('IA', 'IL', 'NC') AND store_sales.ss_net_profit BETWEEN 50 AND 250)
  )
)) SELECT
  AVG(ss_quantity) AS "avg(ss_quantity)",
  AVG(ss_ext_sales_price) AS "avg(ss_ext_sales_price)",
  AVG(ss_ext_wholesale_cost) AS "avg(ss_ext_wholesale_cost)",
  SUM(ss_ext_wholesale_cost) AS "sum(ss_ext_wholesale_cost)"
FROM filtered_fact