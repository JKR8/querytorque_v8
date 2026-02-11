WITH filtered_item AS (
    SELECT i_item_sk, i_manufact_id
    FROM item
    WHERE i_category IN ('Home')
),
filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
      AND d_moy = 1
),
filtered_address AS (
    SELECT ca_address_sk
    FROM customer_address
    WHERE ca_gmt_offset = -5
),
store_sales_filtered AS (
    SELECT i.i_manufact_id, ss.ss_ext_sales_price
    FROM store_sales ss
    JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
    JOIN filtered_date d ON ss.ss_sold_date_sk = d.d_date_sk
    JOIN filtered_address a ON ss.ss_addr_sk = a.ca_address_sk
),
catalog_sales_filtered AS (
    SELECT i.i_manufact_id, cs.cs_ext_sales_price
    FROM catalog_sales cs
    JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
    JOIN filtered_date d ON cs.cs_sold_date_sk = d.d_date_sk
    JOIN filtered_address a ON cs.cs_bill_addr_sk = a.ca_address_sk
),
web_sales_filtered AS (
    SELECT i.i_manufact_id, ws.ws_ext_sales_price
    FROM web_sales ws
    JOIN filtered_item i ON ws.ws_item_sk = i.i_item_sk
    JOIN filtered_date d ON ws.ws_sold_date_sk = d.d_date_sk
    JOIN filtered_address a ON ws.ws_bill_addr_sk = a.ca_address_sk
),
combined_sales AS (
    SELECT i_manufact_id, ss_ext_sales_price AS sales_price FROM store_sales_filtered
    UNION ALL
    SELECT i_manufact_id, cs_ext_sales_price FROM catalog_sales_filtered
    UNION ALL
    SELECT i_manufact_id, ws_ext_sales_price FROM web_sales_filtered
)
SELECT
    i_manufact_id,
    SUM(sales_price) AS total_sales
FROM combined_sales
GROUP BY i_manufact_id
ORDER BY total_sales
LIMIT 100