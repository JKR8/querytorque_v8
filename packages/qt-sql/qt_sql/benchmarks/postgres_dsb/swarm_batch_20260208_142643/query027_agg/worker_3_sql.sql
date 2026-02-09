WITH 
filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'M'
      AND cd_marital_status = 'S'
      AND cd_education_status = 'Advanced Degree'
),
filtered_date_dim AS (
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
filtered_store_sales AS (
    SELECT 
        ss_item_sk,
        ss_store_sk,
        ss_cdemo_sk,
        ss_sold_date_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price
    FROM store_sales
    WHERE ss_cdemo_sk IN (SELECT cd_demo_sk FROM filtered_customer_demographics)
      AND ss_sold_date_sk IN (SELECT d_date_sk FROM filtered_date_dim)
      AND ss_store_sk IN (SELECT s_store_sk FROM filtered_store)
      AND ss_item_sk IN (SELECT i_item_sk FROM filtered_item)
)
SELECT
    i.i_item_id,
    s.s_state,
    GROUPING(s.s_state) AS g_state,
    AVG(ss.ss_quantity) AS agg1,
    AVG(ss.ss_list_price) AS agg2,
    AVG(ss.ss_coupon_amt) AS agg3,
    AVG(ss.ss_sales_price) AS agg4
FROM filtered_store_sales ss
JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
GROUP BY ROLLUP (
    i.i_item_id,
    s.s_state
)
ORDER BY
    i.i_item_id,
    s.s_state
LIMIT 100;