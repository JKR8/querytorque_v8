WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2000 + 1
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_cdemo_sk, c_current_addr_sk
    FROM customer
    JOIN customer_demographics ON c_current_cdemo_sk = cd_demo_sk
    WHERE cd_marital_status = 'S'
      AND cd_education_status = 'Secondary'
),
filtered_item1 AS (
    SELECT i_item_sk
    FROM item
    WHERE i_category IN ('Electronics', 'Men')
),
filtered_item2 AS (
    SELECT i_item_sk
    FROM item
    WHERE i_manager_id BETWEEN 81 AND 100
),
qualified_sales AS (
    SELECT 
        ss_ticket_number,
        ss_item_sk,
        ss_customer_sk,
        ss_list_price
    FROM store_sales
    JOIN filtered_date ON ss_sold_date_sk = d_date_sk
    WHERE ss_list_price BETWEEN 16 AND 30
)
SELECT
    i1.i_item_sk AS i_item_sk,
    i2.i_item_sk AS i_item_sk,
    COUNT(*) AS cnt
FROM qualified_sales s1
JOIN qualified_sales s2 ON s1.ss_ticket_number = s2.ss_ticket_number
JOIN filtered_item1 i1 ON s1.ss_item_sk = i1.i_item_sk
JOIN filtered_item2 i2 ON s2.ss_item_sk = i2.i_item_sk
JOIN filtered_customer c ON s1.ss_customer_sk = c.c_customer_sk
JOIN customer_address ON c.c_current_addr_sk = ca_address_sk
WHERE i1.i_item_sk < i2.i_item_sk
GROUP BY i1.i_item_sk, i2.i_item_sk
ORDER BY cnt