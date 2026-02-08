WITH filtered_sales AS (
  SELECT
    ss_ticket_number,
    ss_item_sk,
    ss_customer_sk,
    ss_store_sk,
    ss_net_profit
  FROM store_sales
  JOIN store_returns ON ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk
  WHERE ss_net_profit IS NOT NULL
),
filtered_customers AS (
  SELECT
    c_customer_sk,
    c_last_name,
    c_first_name,
    c_current_addr_sk,
    c_birth_country
  FROM customer
  WHERE c_birth_country IS NOT NULL
),
filtered_stores AS (
  SELECT
    s_store_sk,
    s_store_name,
    s_state,
    s_zip,
    s_market_id
  FROM store
  WHERE s_market_id = 8
),
filtered_addresses AS (
  SELECT
    ca_address_sk,
    ca_zip,
    UPPER(ca_country) AS ca_country_upper
  FROM customer_address
  WHERE ca_zip IS NOT NULL AND ca_country IS NOT NULL
),
joined_data AS (
  SELECT
    c.c_last_name,
    c.c_first_name,
    s.s_store_name,
    s.s_state,
    i.i_color,
    ss.ss_net_profit
  FROM filtered_sales ss
  JOIN filtered_customers c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN filtered_stores s ON ss.ss_store_sk = s.s_store_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN filtered_addresses ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE c.c_birth_country <> ca.ca_country_upper
    AND s.s_zip = ca.ca_zip
    AND i.i_color = 'beige'
),
color_aggregates AS (
  SELECT
    c_last_name,
    c_first_name,
    s_store_name,
    SUM(ss_net_profit) AS paid,
    COUNT(*) AS cnt
  FROM joined_data
  GROUP BY
    c_last_name,
    c_first_name,
    s_store_name
),
global_avg AS (
  SELECT
    0.05 * AVG(ss_net_profit) AS threshold
  FROM filtered_sales ss
  JOIN filtered_customers c ON ss.ss_customer_sk = c.c_customer_sk
  JOIN filtered_stores s ON ss.ss_store_sk = s.s_store_sk
  JOIN item i ON ss.ss_item_sk = i.i_item_sk
  JOIN filtered_addresses ca ON c.c_current_addr_sk = ca.ca_address_sk
  WHERE c.c_birth_country <> ca.ca_country_upper
    AND s.s_zip = ca.ca_zip
)
SELECT
  c_last_name,
  c_first_name,
  s_store_name,
  paid
FROM color_aggregates
WHERE paid > (SELECT threshold FROM global_avg)
ORDER BY
  c_last_name,
  c_first_name,
  s_store_name