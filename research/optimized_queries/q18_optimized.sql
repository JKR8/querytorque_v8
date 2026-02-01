-- Q18: Date filter pushdown (catalog_sales)
-- Sample DB: 1.63x speedup, CORRECT
-- Pattern: Push date filter into CTE before joining demographics

WITH filtered_sales AS (
    SELECT cs_bill_customer_sk, cs_bill_cdemo_sk, cs_item_sk,
           cs_quantity, cs_list_price, cs_sales_price,
           cs_coupon_amt, cs_net_profit
    FROM catalog_sales, date_dim
    WHERE cs_sold_date_sk = d_date_sk
      AND d_year = 2000
)
SELECT i_item_id, ca_country, ca_state, ca_county,
       avg(CAST(cs_quantity AS DECIMAL(12,2))) agg1,
       avg(CAST(cs_list_price AS DECIMAL(12,2))) agg2,
       avg(CAST(cs_coupon_amt AS DECIMAL(12,2))) agg3,
       avg(CAST(cs_sales_price AS DECIMAL(12,2))) agg4,
       avg(CAST(cs_net_profit AS DECIMAL(12,2))) agg5,
       avg(CAST(c_birth_year AS DECIMAL(12,2))) agg6,
       avg(CAST(cd1.cd_dep_count AS DECIMAL(12,2))) agg7
FROM filtered_sales cs, customer_demographics cd1, customer c,
     customer_address ca, item
WHERE cs.cs_bill_cdemo_sk = cd1.cd_demo_sk
  AND cs.cs_bill_customer_sk = c.c_customer_sk
  AND cd1.cd_gender = 'M'
  AND cd1.cd_education_status = 'Unknown'
  AND c.c_current_cdemo_sk = cd1.cd_demo_sk
  AND c.c_current_addr_sk = ca.ca_address_sk
  AND c_birth_month IN (3,8,10,7,2,1)
  AND ca_state IN ('SD','NE','TX','IA','MS','WI','AL')
  AND cs.cs_item_sk = i_item_sk
GROUP BY ROLLUP(i_item_id, ca_country, ca_state, ca_county)
ORDER BY ca_country, ca_state, ca_county, i_item_id
LIMIT 100;
