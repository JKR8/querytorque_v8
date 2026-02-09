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
state_branches AS (
    -- Branch 1: MO
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'MO'
    
    UNION ALL
    
    -- Branch 2: AL
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'AL'
    
    UNION ALL
    
    -- Branch 3: MI
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'MI'
    
    UNION ALL
    
    -- Branch 4: TN
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'TN'
    
    UNION ALL
    
    -- Branch 5: LA
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'LA'
    
    UNION ALL
    
    -- Branch 6: SC
    SELECT 
        ss_item_sk,
        ss_quantity,
        ss_list_price,
        ss_coupon_amt,
        ss_sales_price,
        s_store_sk,
        s_state
    FROM store_sales
    JOIN store ON ss_store_sk = s_store_sk
    WHERE s_state = 'SC'
)
SELECT
    i.i_item_id,
    sb.s_state,
    GROUPING(sb.s_state) AS g_state,
    AVG(sb.ss_quantity) AS agg1,
    AVG(sb.ss_list_price) AS agg2,
    AVG(sb.ss_coupon_amt) AS agg3,
    AVG(sb.ss_sales_price) AS agg4
FROM state_branches sb
JOIN filtered_date_dim fd ON sb.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_customer_demographics fcd ON sb.ss_cdemo_sk = fcd.cd_demo_sk
JOIN item i ON sb.ss_item_sk = i.i_item_sk
GROUP BY ROLLUP (
    i.i_item_id,
    sb.s_state
)
ORDER BY
    i.i_item_id,
    sb.s_state
LIMIT 100