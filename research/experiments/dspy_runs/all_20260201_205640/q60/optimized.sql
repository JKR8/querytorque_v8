WITH filtered_items AS (
    SELECT DISTINCT i_item_id
    FROM item
    WHERE i_category = 'Children'
),
ss AS (
    SELECT
        i.i_item_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales ss
    JOIN item i ON ss.ss_item_sk = i.i_item_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN customer_address ca ON ss.ss_addr_sk = ca.ca_address_sk
    WHERE i.i_item_id IN (SELECT i_item_id FROM filtered_items)
        AND d.d_year = 2000
        AND d.d_moy = 8
        AND ca.ca_gmt_offset = -7
    GROUP BY i.i_item_id
),
cs AS (
    SELECT
        i.i_item_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales cs
    JOIN item i ON cs.cs_item_sk = i.i_item_sk
    JOIN date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN customer_address ca ON cs.cs_bill_addr_sk = ca.ca_address_sk
    WHERE i.i_item_id IN (SELECT i_item_id FROM filtered_items)
        AND d.d_year = 2000
        AND d.d_moy = 8
        AND ca.ca_gmt_offset = -7
    GROUP BY i.i_item_id
),
ws AS (
    SELECT
        i.i_item_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM web_sales ws
    JOIN item i ON ws.ws_item_sk = i.i_item_sk
    JOIN date_dim d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN customer_address ca ON ws.ws_bill_addr_sk = ca.ca_address_sk
    WHERE i.i_item_id IN (SELECT i_item_id FROM filtered_items)
        AND d.d_year = 2000
        AND d.d_moy = 8
        AND ca.ca_gmt_offset = -7
    GROUP BY i.i_item_id
)
SELECT
    i_item_id,
    SUM(total_sales) AS total_sales
FROM (
    SELECT * FROM ss
    UNION ALL
    SELECT * FROM cs
    UNION ALL
    SELECT * FROM ws
) tmp1
GROUP BY i_item_id
ORDER BY i_item_id, total_sales
LIMIT 100;