WITH filtered_demographics AS (
    SELECT cd_demo_sk 
    FROM customer_demographics 
    WHERE cd_gender = 'F' 
      AND cd_marital_status = 'D' 
      AND cd_education_status = 'Secondary'
),
filtered_dates AS (
    SELECT d_date_sk 
    FROM date_dim 
    WHERE d_year = 1999
),
filtered_stores AS (
    SELECT s_store_sk, s_state 
    FROM store 
    WHERE s_state IN ('MO','AL', 'MI', 'TN', 'LA', 'SC')
)
SELECT 
    i.i_item_id,
    fs.s_state, 
    grouping(fs.s_state) as g_state,
    avg(ss.ss_quantity) as agg1,
    avg(ss.ss_list_price) as agg2,
    avg(ss.ss_coupon_amt) as agg3,
    avg(ss.ss_sales_price) as agg4
FROM store_sales ss
JOIN filtered_demographics fd ON ss.ss_cdemo_sk = fd.cd_demo_sk
JOIN filtered_dates fdt ON ss.ss_sold_date_sk = fdt.d_date_sk
JOIN filtered_stores fs ON ss.ss_store_sk = fs.s_store_sk
JOIN item i ON ss.ss_item_sk = i.i_item_sk
GROUP BY rollup (i.i_item_id, fs.s_state)
ORDER BY i.i_item_id, fs.s_state
LIMIT 100;