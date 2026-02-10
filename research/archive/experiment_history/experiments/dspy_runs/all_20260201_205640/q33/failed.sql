-- start query 33 in stream 0 using template query33.tpl
WITH filtered_items AS (
    SELECT i_manufact_id, i_item_sk
    FROM item
    WHERE i_category = 'Home'
),
date_filter AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002 AND d_moy = 1
),
addr_filter AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -5
),
ss AS (
    SELECT 
        i.i_manufact_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales ss
    INNER JOIN filtered_items i ON ss.ss_item_sk = i.i_item_sk
    INNER JOIN date_filter d ON ss.ss_sold_date_sk = d.d_date_sk
    INNER JOIN addr_filter ca ON ss.ss_addr_sk = ca.ca_address_sk
    GROUP BY i.i_manufact_id
),
cs AS (
    SELECT 
        i.i_manufact_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales cs
    INNER JOIN filtered_items i ON cs.cs_item_sk = i.i_item_sk
    INNER JOIN date_filter d ON cs.cs_sold_date_sk = d.d_date_sk
    INNER JOIN addr_filter ca ON cs.cs_bill_addr_sk = ca.ca_address_sk
    GROUP BY i.i_manufact_id
),
ws AS (
    SELECT 
        i.i_manufact_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM web_sales ws
    INNER JOIN filtered_items i ON ws.ws_item_sk = i.i_item_sk
    INNER JOIN date_filter d ON ws.ws_sold_date_sk = d.d_date_sk
    INNER JOIN addr_filter ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
    GROUP BY i.i_manufact_id
)
SELECT i_manufact_id, SUM(total_sales) AS total_sales
FROM (
    SELECT * FROM ss
    UNION ALL
    SELECT * FROM cs
    UNION ALL
    SELECT * FROM ws
) tmp1
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100;

-- end query 33 in stream 0 using template query33.tpl