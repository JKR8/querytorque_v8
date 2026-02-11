WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2001
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
filtered_cdemo AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'S'
      AND cd_education_status = 'Secondary'
),
s1_base AS (
    SELECT ss_ticket_number, ss_item_sk, ss_customer_sk, ss_sold_date_sk
    FROM store_sales
    WHERE ss_list_price BETWEEN 16 AND 30
),
s2_base AS (
    SELECT ss_ticket_number, ss_item_sk
    FROM store_sales
    WHERE ss_list_price BETWEEN 16 AND 30
)
SELECT
    s1_filtered.ss_item_sk AS i_item_sk,
    s2_filtered.ss_item_sk AS i_item_sk,
    COUNT(*) AS cnt
FROM s1_base AS s1_filtered
JOIN s2_base AS s2_filtered
    ON s1_filtered.ss_ticket_number = s2_filtered.ss_ticket_number
JOIN filtered_item1 AS item1
    ON s1_filtered.ss_item_sk = item1.i_item_sk
JOIN filtered_item2 AS item2
    ON s2_filtered.ss_item_sk = item2.i_item_sk
JOIN filtered_date
    ON s1_filtered.ss_sold_date_sk = filtered_date.d_date_sk
JOIN customer
    ON s1_filtered.ss_customer_sk = customer.c_customer_sk
JOIN filtered_cdemo
    ON customer.c_current_cdemo_sk = filtered_cdemo.cd_demo_sk
JOIN customer_address
    ON customer.c_current_addr_sk = customer_address.ca_address_sk
WHERE s1_filtered.ss_item_sk < s2_filtered.ss_item_sk
GROUP BY s1_filtered.ss_item_sk, s2_filtered.ss_item_sk
ORDER BY cnt;
