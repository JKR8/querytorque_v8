WITH filtered_item AS (
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
filtered_customer AS (
    SELECT c_customer_sk, c_current_cdemo_sk, c_current_hdemo_sk
    FROM customer
    WHERE c_current_addr_sk IN (SELECT ca_address_sk FROM filtered_customer_address)
),
d1_dates AS (
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_year = 2002
),
d2_date_range AS (
    SELECT 
        d1.d_date_sk AS d1_date_sk,
        d1.d_date AS d1_date,
        d2.d_date_sk AS d2_date_sk
    FROM d1_dates d1
    CROSS JOIN LATERAL (
        SELECT d_date_sk
        FROM date_dim
        WHERE d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '30 DAY')
    ) d2
),
store_sales_base AS (
    SELECT 
        ss_item_sk,
        ss_ticket_number,
        ss_customer_sk,
        ss_sold_date_sk,
        ss_quantity,
        inv_warehouse_sk
    FROM store_sales
    JOIN inventory ON ss_item_sk = inv_item_sk AND ss_sold_date_sk = inv_date_sk
    WHERE inv_quantity_on_hand >= ss_quantity
),
store_sales_filtered AS (
    SELECT DISTINCT
        ss.ss_item_sk,
        ss.ss_ticket_number,
        ss.ss_customer_sk,
        ss.ss_sold_date_sk,
        ss.inv_warehouse_sk
    FROM store_sales_base ss
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN d1_dates d1 ON ss.ss_sold_date_sk = d1.d_date_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
),
web_sales_filtered AS (
    SELECT DISTINCT
        ws.ws_item_sk,
        ws.ws_order_number,
        ws.ws_bill_customer_sk,
        ws.ws_sold_date_sk,
        ws.ws_warehouse_sk
    FROM web_sales ws
    WHERE ws.ws_wholesale_cost BETWEEN 34 AND 54
),
combined_sales AS (
    SELECT
        ss.ss_item_sk,
        ss.ss_ticket_number,
        ws.ws_order_number,
        ss.ss_customer_sk,
        c.c_current_cdemo_sk,
        c.c_current_hdemo_sk
    FROM store_sales_filtered ss
    JOIN web_sales_filtered ws ON ss.ss_item_sk = ws.ws_item_sk
                              AND ss.ss_customer_sk = ws.ws_bill_customer_sk
                              AND ss.inv_warehouse_sk = ws.ws_warehouse_sk
    JOIN d2_date_range dr ON ss.ss_sold_date_sk = dr.d1_date_sk
                         AND ws.ws_sold_date_sk = dr.d2_date_sk
    JOIN filtered_customer c ON ss.ss_customer_sk = c.c_customer_sk
    JOIN warehouse w ON ws.ws_warehouse_sk = w.w_warehouse_sk
    JOIN store s ON s.s_state = w.w_state
)
SELECT
    MIN(ss_item_sk),
    MIN(ss_ticket_number),
    MIN(ws_order_number),
    MIN(ss_customer_sk),
    MIN(c_current_cdemo_sk),
    MIN(c_current_hdemo_sk)
FROM combined_sales;