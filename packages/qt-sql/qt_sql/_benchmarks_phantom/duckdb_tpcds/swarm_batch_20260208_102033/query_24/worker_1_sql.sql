WITH filtered_store AS (
  SELECT s_store_sk, s_store_name, s_state, s_zip
  FROM store
  WHERE s_market_id = 8
),
filtered_customer_address AS (
  SELECT ca_address_sk, ca_state, ca_country, ca_zip
  FROM customer_address
),
filtered_customer AS (
  SELECT c_customer_sk, c_last_name, c_first_name, c_current_addr_sk, c_birth_country
  FROM customer
),
joined_customer_address AS (
  SELECT 
    c.c_customer_sk, 
    c.c_last_name, 
    c.c_first_name, 
    c.c_birth_country,
    ca.ca_address_sk,
    ca.ca_state,
    ca.ca_country,
    ca.ca_zip
  FROM filtered_customer c
  JOIN filtered_customer_address ca 
    ON c.c_current_addr_sk = ca.ca_address_sk
   AND c.c_birth_country <> UPPER(ca.ca_country)
),
joined_store_customer AS (
  SELECT 
    s.s_store_sk,
    s.s_store_name,
    s.s_state,
    s.s_zip,
    jc.c_customer_sk,
    jc.c_last_name,
    jc.c_first_name,
    jc.ca_state,
    jc.ca_zip
  FROM filtered_store s
  JOIN joined_customer_address jc 
    ON s.s_zip = jc.ca_zip
),
ssales AS (
  SELECT
    jsc.c_last_name,
    jsc.c_first_name,
    jsc.s_store_name,
    jsc.ca_state,
    s.s_state,
    i.i_color,
    i.i_current_price,
    i.i_manager_id,
    i.i_units,
    i.i_size,
    SUM(ss.ss_net_profit) AS netpaid
  FROM store_sales ss
  JOIN store_returns sr 
    ON ss.ss_ticket_number = sr.sr_ticket_number
   AND ss.ss_item_sk = sr.sr_item_sk
  JOIN joined_store_customer jsc 
    ON ss.ss_customer_sk = jsc.c_customer_sk
   AND ss.ss_store_sk = jsc.s_store_sk
  JOIN item i 
    ON ss.ss_item_sk = i.i_item_sk
  GROUP BY
    jsc.c_last_name,
    jsc.c_first_name,
    jsc.s_store_name,
    jsc.ca_state,
    s.s_state,
    i.i_color,
    i.i_current_price,
    i.i_manager_id,
    i.i_units,
    i.i_size
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