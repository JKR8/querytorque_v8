WITH ssales AS (
    SELECT 
        c_last_name,
        c_first_name,
        s_store_name,
        ca_state,
        s_state,
        i_color,
        i_current_price,
        i_manager_id,
        i_units,
        i_size,
        SUM(ss_net_profit) AS netpaid
    FROM store_sales
    INNER JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
    INNER JOIN store ON ss_store_sk = s_store_sk
    INNER JOIN item ON ss_item_sk = i_item_sk
    INNER JOIN customer ON ss_customer_sk = c_customer_sk
    INNER JOIN customer_address ON c_current_addr_sk = ca_address_sk
    WHERE c_birth_country <> UPPER(ca_country)
      AND s_zip = ca_zip
      AND s_market_id = 8
      AND i_color IN ('beige', 'blue')  -- Filter early for both colors
    GROUP BY c_last_name, c_first_name, s_store_name, ca_state, s_state, 
             i_color, i_current_price, i_manager_id, i_units, i_size
)
SELECT 
    c_last_name,
    c_first_name,
    s_store_name,
    SUM(netpaid) AS paid
FROM ssales
WHERE i_color = 'beige'
GROUP BY c_last_name, c_first_name, s_store_name
HAVING SUM(netpaid) > (SELECT 0.05 * AVG(netpaid) FROM ssales WHERE i_color = 'beige')  -- Subquery on filtered data
UNION ALL
SELECT 
    c_last_name,
    c_first_name,
    s_store_name,
    SUM(netpaid) AS paid
FROM ssales
WHERE i_color = 'blue'
GROUP BY c_last_name, c_first_name, s_store_name
HAVING SUM(netpaid) > (SELECT 0.05 * AVG(netpaid) FROM ssales WHERE i_color = 'blue')  -- Subquery on filtered data
ORDER BY c_last_name, c_first_name, s_store_name;