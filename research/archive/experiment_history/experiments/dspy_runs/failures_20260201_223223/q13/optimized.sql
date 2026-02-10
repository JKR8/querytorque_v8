-- start query 13 in stream 0 using template query13.tpl
WITH filtered_date AS MATERIALIZED (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
),
filtered_customer_address AS MATERIALIZED (
    SELECT ca_address_sk, ca_country, ca_state
    FROM customer_address
    WHERE ca_country = 'United States'
      AND ca_state IN ('SD', 'KS', 'MI', 'MO', 'ND', 'CO', 'NH', 'OH', 'TX')
),
filtered_store AS MATERIALIZED (
    SELECT s_store_sk
    FROM store
    WHERE s_store_sk <= 400
)
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
     ,filtered_store
     ,customer_demographics
     ,household_demographics
     ,filtered_customer_address
     ,filtered_date
 where s_store_sk = ss_store_sk
 and  ss_sold_date_sk = d_date_sk
 and((ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'D'
  and cd_education_status = 'Unknown'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'S'
  and cd_education_status = 'College'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (ss_hdemo_sk=hd_demo_sk
  and cd_demo_sk = ss_cdemo_sk
  and cd_marital_status = 'M'
  and cd_education_status = '4 yr Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ss_addr_sk = ca_address_sk
  and ca_state in ('SD', 'KS', 'MI')
  and ss_net_profit between 100 and 200  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_state in ('MO', 'ND', 'CO')
  and ss_net_profit between 150 and 300  
     ) or
     (ss_addr_sk = ca_address_sk
  and ca_state in ('NH', 'OH', 'TX')
  and ss_net_profit between 50 and 250  
     ))
;

-- end query 13 in stream 0 using template query13.tpl