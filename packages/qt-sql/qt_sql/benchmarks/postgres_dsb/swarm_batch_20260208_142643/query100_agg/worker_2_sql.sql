WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year BETWEEN 2000 AND 2000 + 1
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_marital_status = 'S'
      AND cd_education_status = 'Secondary'
),
filtered_customer AS (
    SELECT c_customer_sk, c_current_addr_sk, c_current_cdemo_sk
    FROM customer
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
    JOIN filtered_customer_demographics ON c_current_cdemo_sk = cd_demo_sk
)
SELECT
    item1.i_item_sk,
    item2.i_item_sk,
    COUNT(*) AS cnt
FROM store_sales s1
JOIN store_sales s2 ON s1.ss_ticket_number = s2.ss_ticket_number
JOIN filtered_date ON s1.ss_sold_date_sk = filtered_date.d_date_sk
JOIN filtered_customer ON s1.ss_customer_sk = filtered_customer.c_customer_sk
JOIN item AS item1 ON s1.ss_item_sk = item1.i_item_sk
JOIN item AS item2 ON s2.ss_item_sk = item2.i_item_sk
WHERE item1.i_item_sk < item2.i_item_sk
  AND item1.i_category IN ('Electronics', 'Men')
  AND item2.i_manager_id BETWEEN 81 AND 100
  AND s1.ss_list_price BETWEEN 16 AND 30
  AND s2.ss_list_price BETWEEN 16 AND 30
GROUP BY
    item1.i_item_sk,
    item2.i_item_sk
ORDER BY
    cnt