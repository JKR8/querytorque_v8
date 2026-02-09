WITH filtered_items AS (
    SELECT i_item_id, i_item_sk
    FROM item
    WHERE i_category IN ('Children')
),
filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2000
      AND d_moy = 8
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -7
),
ss AS (
    SELECT
        fi.i_item_id,
        SUM(ss_ext_sales_price) AS total_sales
    FROM store_sales
    JOIN filtered_items fi ON ss_item_sk = fi.i_item_sk
    JOIN filtered_date fd ON ss_sold_date_sk = fd.d_date_sk
    JOIN filtered_address fa ON ss_addr_sk = fa.ca_address_sk
    GROUP BY fi.i_item_id
),
cs AS (
    SELECT
        fi.i_item_id,
        SUM(cs_ext_sales_price) AS total_sales
    FROM catalog_sales
    JOIN filtered_items fi ON cs_item_sk = fi.i_item_sk
    JOIN filtered_date fd ON cs_sold_date_sk = fd.d_date_sk
    JOIN filtered_address fa ON cs_bill_addr_sk = fa.ca_address_sk
    GROUP BY fi.i_item_id
),
ws AS (
    SELECT
        fi.i_item_id,
        SUM(ws_ext_sales_price) AS total_sales
    FROM web_sales
    JOIN filtered_items fi ON ws_item_sk = fi.i_item_sk
    JOIN filtered_date fd ON ws_sold_date_sk = fd.d_date_sk
    JOIN filtered_address fa ON ws_bill_addr_sk = fa.ca_address_sk
    GROUP BY fi.i_item_id
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
) AS tmp1
GROUP BY i_item_id
ORDER BY i_item_id, total_sales
LIMIT 100