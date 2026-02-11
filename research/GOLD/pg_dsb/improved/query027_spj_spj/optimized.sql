WITH 
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
filtered_date_dim AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2002
),
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'M'
      AND cd_marital_status = 'S'
      AND cd_education_status = 'Advanced Degree'
),
filtered_sales_keys AS (
    SELECT 
        ss_item_sk,
        ss_ticket_number,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price
    FROM filtered_store
    JOIN filtered_item ON TRUE
    JOIN filtered_date_dim ON TRUE
    JOIN filtered_customer_demographics ON TRUE
    JOIN LATERAL (
        SELECT 
            ss_item_sk,
            ss_ticket_number,
            ss_quantity,
            ss_list_price,
            ss_coupon_amt,
            ss_sales_price
        FROM store_sales
        WHERE ss_store_sk = filtered_store.s_store_sk
          AND ss_item_sk = filtered_item.i_item_sk
          AND ss_sold_date_sk = filtered_date_dim.d_date_sk
          AND ss_cdemo_sk = filtered_customer_demographics.cd_demo_sk
    ) ss ON TRUE
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
FROM filtered_sales_keys
JOIN filtered_item ON filtered_sales_keys.ss_item_sk = filtered_item.i_item_sk
JOIN filtered_store ON TRUE;
