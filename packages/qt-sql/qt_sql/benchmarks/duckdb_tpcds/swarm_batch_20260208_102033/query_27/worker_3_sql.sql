WITH filtered_customer_demographics AS (
    SELECT cd_demo_sk
    FROM customer_demographics
    WHERE cd_gender = 'F'
      AND cd_marital_status = 'D'
      AND cd_education_status = 'Secondary'
),
filtered_date_dim AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 1999
),
filtered_store AS (
    SELECT s_store_sk, s_state
    FROM store
    WHERE s_state IN ('MO', 'AL', 'MI', 'TN', 'LA', 'SC')
),
filtered_item AS (
    SELECT i_item_sk, i_item_id
    FROM item
),
joined_fact AS (
    SELECT
        i.i_item_id,
        s.s_state,
        ss.ss_quantity,
        ss.ss_list_price,
        ss.ss_coupon_amt,
        ss.ss_sales_price
    FROM store_sales ss
    INNER JOIN filtered_customer_demographics cd ON ss.ss_cdemo_sk = cd.cd_demo_sk
    INNER JOIN filtered_date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
    INNER JOIN filtered_store s ON ss.ss_store_sk = s.s_store_sk
    INNER JOIN filtered_item i ON ss.ss_item_sk = i.i_item_sk
)
SELECT
    i_item_id,
    s_state,
    GROUPING(s_state) AS g_state,
    AVG(ss_quantity) AS agg1,
    AVG(ss_list_price) AS agg2,
    AVG(ss_coupon_amt) AS agg3,
    AVG(ss_sales_price) AS agg4
FROM joined_fact
GROUP BY ROLLUP (i_item_id, s_state)
ORDER BY i_item_id, s_state
LIMIT 100;