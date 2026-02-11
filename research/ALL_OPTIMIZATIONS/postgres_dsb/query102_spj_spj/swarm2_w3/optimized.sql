WITH filtered_d1 AS (
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
filtered_ca AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_state IN ('GA', 'MD', 'MO', 'NC', 'OR')
),
filtered_inv AS (
    SELECT inv_item_sk, inv_date_sk, inv_warehouse_sk
    FROM inventory
),
store_sales_pre AS (
    SELECT 
        ss_item_sk,
        ss_ticket_number,
        ss_customer_sk,
        ss_quantity,
        ss_sold_date_sk
    FROM store_sales
    INNER JOIN filtered_d1 ON ss_sold_date_sk = filtered_d1.d_date_sk
    INNER JOIN filtered_item ON ss_item_sk = filtered_item.i_item_sk
),
web_sales_pre AS (
    SELECT 
        ws_item_sk,
        ws_order_number,
        ws_bill_customer_sk,
        ws_warehouse_sk,
        ws_sold_date_sk
    FROM web_sales
    WHERE ws_wholesale_cost BETWEEN 34 AND 54
),
qualified_sales AS (
    SELECT 
        ss.ss_item_sk,
        ss.ss_ticket_number,
        ws.ws_order_number,
        c.c_customer_sk,
        cd.cd_demo_sk,
        hd.hd_demo_sk
    FROM store_sales_pre ss
    INNER JOIN web_sales_pre ws ON ss.ss_item_sk = ws.ws_item_sk
    INNER JOIN filtered_d1 d1 ON ss.ss_sold_date_sk = d1.d_date_sk
    INNER JOIN date_dim d2 ON ws.ws_sold_date_sk = d2.d_date_sk
    INNER JOIN customer c ON ss.ss_customer_sk = c.c_customer_sk 
                          AND ws.ws_bill_customer_sk = c.c_customer_sk
    INNER JOIN filtered_ca ON c.c_current_addr_sk = filtered_ca.ca_address_sk
    INNER JOIN filtered_inv inv ON inv.inv_item_sk = ss.ss_item_sk 
                                AND inv.inv_date_sk = ss.ss_sold_date_sk
                                AND ws.ws_warehouse_sk = inv.inv_warehouse_sk
    INNER JOIN warehouse w ON ws.ws_warehouse_sk = w.w_warehouse_sk
    INNER JOIN store s ON s.s_state = w.w_state
    INNER JOIN customer_demographics cd ON c.c_current_cdemo_sk = cd.cd_demo_sk
    INNER JOIN household_demographics hd ON c.c_current_hdemo_sk = hd.hd_demo_sk
    WHERE d2.d_date BETWEEN d1.d_date AND (d1.d_date + INTERVAL '30 DAY')
      AND inv.inv_quantity_on_hand >= ss.ss_quantity
)
SELECT 
    MIN(ss_item_sk),
    MIN(ss_ticket_number),
    MIN(ws_order_number),
    MIN(c_customer_sk),
    MIN(cd_demo_sk),
    MIN(hd_demo_sk)
FROM qualified_sales;