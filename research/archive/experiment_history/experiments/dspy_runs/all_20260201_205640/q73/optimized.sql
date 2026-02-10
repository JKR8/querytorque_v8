-- start query 73 in stream 0 using template query73.tpl
WITH filtered_sales AS (
    SELECT ss_ticket_number,
           ss_customer_sk,
           COUNT(*) AS cnt
    FROM store_sales
    JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN store ON store_sales.ss_store_sk = store.s_store_sk
    JOIN household_demographics ON store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    WHERE date_dim.d_dom BETWEEN 1 AND 2
      AND date_dim.d_year IN (2000, 2001, 2002)
      AND store.s_county IN ('Fairfield County', 'Walker County', 'Daviess County', 'Barrow County')
      AND (household_demographics.hd_buy_potential = '501-1000' OR household_demographics.hd_buy_potential = 'Unknown')
      AND household_demographics.hd_vehicle_count > 0
      AND household_demographics.hd_dep_count / household_demographics.hd_vehicle_count > 1
    GROUP BY ss_ticket_number, ss_customer_sk
    HAVING COUNT(*) BETWEEN 1 AND 5
)
SELECT c.c_last_name,
       c.c_first_name,
       c.c_salutation,
       c.c_preferred_cust_flag,
       fs.ss_ticket_number,
       fs.cnt
FROM filtered_sales fs
JOIN customer c ON fs.ss_customer_sk = c.c_customer_sk
ORDER BY fs.cnt DESC, c.c_last_name ASC;

-- end query 73 in stream 0 using template query73.tpl