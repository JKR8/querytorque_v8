WITH customer_total_return AS (
    SELECT
        wr_returning_customer_sk AS ctr_customer_sk,
        ca_state AS ctr_state,
        wr_reason_sk AS ctr_reason_sk,
        SUM(wr_return_amt) AS ctr_total_return
    FROM web_returns
    JOIN date_dim ON wr_returned_date_sk = d_date_sk
    JOIN customer_address ON wr_returning_addr_sk = ca_address_sk
    JOIN item ON wr_item_sk = i_item_sk
    WHERE d_year = 1998
        AND i_manager_id BETWEEN 17 AND 26
        AND wr_return_amt / wr_return_quantity BETWEEN 25 AND 54
    GROUP BY
        wr_returning_customer_sk,
        ca_state,
        wr_reason_sk
),
state_thresholds AS (
    SELECT
        ctr_state,
        AVG(ctr_total_return) * 1.2 AS avg_threshold
    FROM customer_total_return
    GROUP BY ctr_state
)
SELECT
    c_customer_id,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date_sk,
    ctr_total_return
FROM customer_total_return ctr1
JOIN state_thresholds st ON ctr1.ctr_state = st.ctr_state
JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk
JOIN customer_address ca ON c_current_addr_sk = ca_address_sk
WHERE ctr1.ctr_total_return > st.avg_threshold
    AND ca.ca_state IN ('IL', 'LA', 'MN', 'SC')
    AND ctr1.ctr_reason_sk IN (50, 73)
    AND c_birth_year BETWEEN 1961 AND 1967
ORDER BY
    c_customer_id,
    c_salutation,
    c_first_name,
    c_last_name,
    c_preferred_cust_flag,
    c_birth_day,
    c_birth_month,
    c_birth_year,
    c_birth_country,
    c_login,
    c_email_address,
    c_last_review_date_sk,
    ctr_total_return
LIMIT 100