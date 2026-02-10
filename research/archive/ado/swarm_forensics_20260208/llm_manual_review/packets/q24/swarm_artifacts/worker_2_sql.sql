WITH filtered_store AS (
    SELECT s_store_sk, s_store_name, s_state, s_zip
    FROM store
    WHERE s_market_id = 8
),
filtered_customer_addr AS (
    SELECT 
        c.c_customer_sk,
        c.c_first_name,
        c.c_last_name,
        c.c_birth_country,
        ca.ca_address_sk,
        ca.ca_country,
        ca.ca_state,
        ca.ca_zip
    FROM customer c
    JOIN customer_address ca ON c.c_current_addr_sk = ca.ca_address_sk
    WHERE c.c_birth_country <> UPPER(ca.ca_country)
),
filtered_item AS (
    SELECT 
        i_item_sk,
        i_color,
        i_current_price,
        i_manager_id,
        i_units,
        i_size
    FROM item
),
ssales AS (
    SELECT
        fca.c_last_name,
        fca.c_first_name,
        fs.s_store_name,
        fca.ca_state,
        fs.s_state,
        fi.i_color,
        fi.i_current_price,
        fi.i_manager_id,
        fi.i_units,
        fi.i_size,
        SUM(ss_net_profit) AS netpaid
    FROM store_sales ss
    JOIN store_returns sr ON ss.ss_ticket_number = sr.sr_ticket_number
        AND ss.ss_item_sk = sr.sr_item_sk
    JOIN filtered_store fs ON ss.ss_store_sk = fs.s_store_sk
    JOIN filtered_item fi ON ss.ss_item_sk = fi.i_item_sk
    JOIN filtered_customer_addr fca ON ss.ss_customer_sk = fca.c_customer_sk
        AND fs.s_zip = fca.ca_zip
    GROUP BY
        fca.c_last_name,
        fca.c_first_name,
        fs.s_store_name,
        fca.ca_state,
        fs.s_state,
        fi.i_color,
        fi.i_current_price,
        fi.i_manager_id,
        fi.i_units,
        fi.i_size
)
SELECT
    c_last_name,
    c_first_name,
    s_store_name,
    SUM(netpaid) AS paid
FROM ssales
WHERE
    i_color = 'beige'
GROUP BY
    c_last_name,
    c_first_name,
    s_store_name
HAVING
    SUM(netpaid) > (
        SELECT
            0.05 * AVG(netpaid)
        FROM ssales
    )
ORDER BY
    c_last_name,
    c_first_name,
    s_store_name