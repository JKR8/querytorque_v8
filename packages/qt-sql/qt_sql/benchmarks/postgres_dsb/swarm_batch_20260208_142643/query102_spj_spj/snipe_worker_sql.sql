WITH 
-- Early filtered dimensions (independent filters)
filtered_date AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2002
),
filtered_item AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Home', 'Men', 'Music')
      AND i_manager_id IN (1, 2, 4, 11, 12, 14, 21, 29, 32, 52)
),
filtered_customer_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('GA', 'MD', 'MO', 'NC', 'OR')
),
-- Pre-join customer with demographics and filtered address
customer_prejoin AS (
    SELECT 
        c_customer_sk,
        c_current_cdemo_sk,
        c_current_hdemo_sk,
        c_current_addr_sk
    FROM customer
    WHERE c_current_addr_sk IN (SELECT ca_address_sk FROM filtered_customer_address)
),
-- Store-warehouse state pairs
store_warehouse_state AS (
    SELECT DISTINCT s_state AS state
    FROM store
    INTERSECT
    SELECT DISTINCT w_state
    FROM warehouse
),
-- Main fact join with pre-filtered dimensions
store_sales_filtered AS (
    SELECT 
        ss_item_sk,
        ss_ticket_number,
        ss_customer_sk,
        ss_quantity,
        ss_sold_date_sk,
        d1.d_date AS ss_date
    FROM store_sales
    JOIN filtered_date d1 ON ss_sold_date_sk = d1.d_date_sk
    JOIN filtered_item i ON ss_item_sk = i.i_item_sk
),
web_sales_filtered AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_bill_customer_sk,
        ws_warehouse_sk,
        ws_sold_date_sk,
        ws_wholesale_cost
    FROM web_sales
    WHERE ws_wholesale_cost BETWEEN 34 AND 54
)
-- Final aggregation
SELECT
    MIN(ss.ss_item_sk),
    MIN(ss.ss_ticket_number),
    MIN(ws.ws_order_number),
    MIN(c.c_customer_sk),
    MIN(cd.cd_demo_sk),
    MIN(hd.hd_demo_sk)
FROM store_sales_filtered ss
JOIN web_sales_filtered ws ON ws.ws_item_sk = ss.ss_item_sk
JOIN date_dim d2 ON ws.ws_sold_date_sk = d2.d_date_sk
JOIN customer_prejoin c ON ss.ss_customer_sk = c.c_customer_sk 
    AND ws.ws_bill_customer_sk = c.c_customer_sk
JOIN inventory inv ON inv.inv_item_sk = ss.ss_item_sk
    AND inv.inv_date_sk = ss.ss_sold_date_sk
    AND inv.inv_warehouse_sk = ws.ws_warehouse_sk
JOIN warehouse w ON ws.ws_warehouse_sk = w.w_warehouse_sk
JOIN store_warehouse_state sws ON w.w_state = sws.state
JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
WHERE d2.d_date BETWEEN ss.ss_date AND (ss.ss_date + INTERVAL '30 DAY')
  AND inv.inv_quantity_on_hand >= ss.ss_quantity;