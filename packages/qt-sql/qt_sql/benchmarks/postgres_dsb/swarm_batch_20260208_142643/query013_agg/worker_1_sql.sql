WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001
)
SELECT
  AVG(ss_quantity),
  AVG(ss_ext_sales_price),
  AVG(ss_ext_wholesale_cost),
  SUM(ss_ext_wholesale_cost)
FROM store_sales
JOIN filtered_dates ON ss_sold_date_sk = filtered_dates.d_date_sk
JOIN store ON s_store_sk = ss_store_sk
JOIN customer_demographics ON cd_demo_sk = ss_cdemo_sk
JOIN household_demographics ON hd_demo_sk = ss_hdemo_sk
JOIN customer_address ON ca_address_sk = ss_addr_sk
WHERE (
    (
        cd_marital_status = 'U'
        AND cd_education_status = 'College'
        AND ss_sales_price BETWEEN 100.00 AND 150.00
        AND hd_dep_count = 3
    )
    OR (
        cd_marital_status = 'W'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 50.00 AND 100.00
        AND hd_dep_count = 1
    )
    OR (
        cd_marital_status = 'D'
        AND cd_education_status = 'Secondary'
        AND ss_sales_price BETWEEN 150.00 AND 200.00
        AND hd_dep_count = 1
    )
)
AND (
    (
        ca_country = 'United States'
        AND ca_state IN ('IA', 'MO', 'TX')
        AND ss_net_profit BETWEEN 100 AND 200
    )
    OR (
        ca_country = 'United States'
        AND ca_state IN ('GA', 'LA', 'SD')
        AND ss_net_profit BETWEEN 150 AND 300
    )
    OR (
        ca_country = 'United States'
        AND ca_state IN ('TN', 'TX', 'VA')
        AND ss_net_profit BETWEEN 50 AND 250
    )
)