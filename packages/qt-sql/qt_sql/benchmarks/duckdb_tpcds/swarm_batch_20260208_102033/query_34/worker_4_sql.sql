WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE (
        d_dom BETWEEN 1 AND 3 OR d_dom BETWEEN 25 AND 28
    )
    AND d_year IN (1998, 1999, 2000)
),
filtered_store AS (
    SELECT s_store_sk
    FROM store
    WHERE s_county IN (
        'Ziebach County', 'Daviess County', 'Walker County', 'Richland County',
        'Barrow County', 'Franklin Parish', 'Williamson County', 'Luce County'
    )
),
filtered_hd AS (
    SELECT hd_demo_sk
    FROM household_demographics
    WHERE hd_buy_potential IN ('1001-5000', '0-500')
    AND hd_vehicle_count > 0
    AND hd_dep_count / hd_vehicle_count > 1.2
),
filtered_sales AS (
    SELECT ss_ticket_number, ss_customer_sk
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    JOIN filtered_store ON ss_store_sk = s_store_sk
    JOIN filtered_hd ON ss_hdemo_sk = hd_demo_sk
),
dn AS (
    SELECT ss_ticket_number, ss_customer_sk, COUNT(*) AS cnt
    FROM filtered_sales
    GROUP BY ss_ticket_number, ss_customer_sk
    HAVING COUNT(*) BETWEEN 15 AND 20
)
SELECT
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag,
    ss_ticket_number,
    cnt
FROM dn
JOIN customer ON ss_customer_sk = c_customer_sk
ORDER BY
    c_last_name,
    c_first_name,
    c_salutation,
    c_preferred_cust_flag DESC,
    ss_ticket_number;