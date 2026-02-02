SELECT 
    i_item_id, 
    AVG(cs_quantity) AS agg1,
    AVG(cs_list_price) AS agg2,
    AVG(cs_coupon_amt) AS agg3,
    AVG(cs_sales_price) AS agg4 
FROM catalog_sales
INNER JOIN date_dim ON cs_sold_date_sk = d_date_sk
INNER JOIN customer_demographics ON cs_bill_cdemo_sk = cd_demo_sk
INNER JOIN item ON cs_item_sk = i_item_sk
INNER JOIN promotion ON cs_promo_sk = p_promo_sk
WHERE d_year = 2001
    AND cd_gender = 'M'
    AND cd_marital_status = 'S'
    AND cd_education_status = 'Unknown'
    AND (p_channel_email = 'N' OR p_channel_event = 'N')
GROUP BY i_item_id
ORDER BY i_item_id
LIMIT 100;