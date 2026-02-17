-- TPC-DS Query 10 style - CTE with multiple aggregations
WITH customer_total_return AS (
    SELECT 
        wr_returning_customer_sk as ctr_customer_sk,
        ca_address_sk as ctr_address_sk,
        SUM(wr_return_amt) as ctr_total_return
    FROM web_returns
    JOIN date_dim ON wr_returned_date_sk = d_date_sk
    JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
    WHERE d_year = 2000
    GROUP BY wr_returning_customer_sk, ca_address_sk
)
SELECT 
    c_customer_id,
    c_salutation,
    c_first_name,
    c_last_name,
    ca_street_number,
    ca_street_name,
    ca_city,
    ca_zip,
    ctr_total_return
FROM customer_total_return ctr1
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ON ctr1.ctr_address_sk = ca_address_sk
WHERE ctr1.ctr_total_return > (
    SELECT AVG(ctr_total_return) * 1.2
    FROM customer_total_return ctr2
    WHERE ctr1.ctr_address_sk = ctr2.ctr_address_sk
)
ORDER BY ctr_total_return DESC
LIMIT 100;
