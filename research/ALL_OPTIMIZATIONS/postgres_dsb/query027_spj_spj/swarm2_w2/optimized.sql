WITH filtered_date AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
),
filtered_store AS (
    SELECT s_store_sk, s_state
    FROM store
    WHERE s_state = 'VA'
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Jewelry'
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'M'
      AND cd_marital_status = 'S'
      AND cd_education_status = 'Advanced Degree'
)
SELECT
    MIN(i_item_id),
    MIN(s_state),
    MIN(ss_quantity),
    MIN(ss_list_price),
    MIN(ss_coupon_amt),
    MIN(ss_sales_price),
    MIN(ss_item_sk),
    MIN(ss_ticket_number)
FROM store_sales
JOIN filtered_date ON store_sales.ss_sold_date_sk = filtered_date.d_date_sk
JOIN filtered_item ON store_sales.ss_item_sk = filtered_item.i_item_sk
JOIN filtered_store ON store_sales.ss_store_sk = filtered_store.s_store_sk
JOIN filtered_customer_demographics ON store_sales.ss_cdemo_sk = filtered_customer_demographics.cd_demo_sk