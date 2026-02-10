WITH date_items AS (
    SELECT 
        d_date_sk,
        ss_item_sk,
        ss_ext_sales_price,
        ss_customer_sk,
        ss_store_sk,
        i_brand,
        i_brand_id,
        i_manufact_id,
        i_manufact
    FROM date_dim
    JOIN store_sales ON d_date_sk = ss_sold_date_sk
    JOIN item ON ss_item_sk = i_item_sk
    WHERE i_manager_id = 2
      AND d_moy = 12
      AND d_year = 1999
),
customer_zip AS (
    SELECT 
        c_customer_sk,
        SUBSTRING(ca_zip, 1, 5) AS ca_zip_prefix
    FROM customer
    JOIN customer_address ON c_current_addr_sk = ca_address_sk
),
store_zip AS (
    SELECT 
        s_store_sk,
        SUBSTRING(s_zip, 1, 5) AS s_zip_prefix
    FROM store
),
filtered_sales AS (
    -- Branch 1: ca_zip_prefix < s_zip_prefix
    SELECT 
        di.ss_ext_sales_price,
        di.i_brand,
        di.i_brand_id,
        di.i_manufact_id,
        di.i_manufact
    FROM date_items di
    JOIN customer_zip cz ON di.ss_customer_sk = cz.c_customer_sk
    JOIN store_zip sz ON di.ss_store_sk = sz.s_store_sk
    WHERE cz.ca_zip_prefix < sz.s_zip_prefix
    
    UNION ALL
    
    -- Branch 2: ca_zip_prefix > s_zip_prefix
    SELECT 
        di.ss_ext_sales_price,
        di.i_brand,
        di.i_brand_id,
        di.i_manufact_id,
        di.i_manufact
    FROM date_items di
    JOIN customer_zip cz ON di.ss_customer_sk = cz.c_customer_sk
    JOIN store_zip sz ON di.ss_store_sk = sz.s_store_sk
    WHERE cz.ca_zip_prefix > sz.s_zip_prefix
)
SELECT 
    i_brand_id AS brand_id,
    i_brand AS brand,
    i_manufact_id,
    i_manufact,
    SUM(ss_ext_sales_price) AS ext_price
FROM filtered_sales
GROUP BY 
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
ORDER BY 
    ext_price DESC,
    i_brand,
    i_brand_id,
    i_manufact_id,
    i_manufact
LIMIT 100;