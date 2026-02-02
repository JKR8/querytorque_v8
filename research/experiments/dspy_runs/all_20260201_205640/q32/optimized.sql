WITH avg_discount AS (
    SELECT 
        cs_item_sk,
        1.3 * AVG(cs_ext_discount_amt) AS avg_disc_amt
    FROM 
        catalog_sales
    INNER JOIN 
        date_dim ON d_date_sk = cs_sold_date_sk
    WHERE 
        d_date BETWEEN DATE '1999-01-07' AND (DATE '1999-01-07' + INTERVAL '90' DAY)
    GROUP BY 
        cs_item_sk
)
SELECT 
    SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM 
    catalog_sales
INNER JOIN 
    item ON i_item_sk = cs_item_sk
INNER JOIN 
    date_dim ON d_date_sk = cs_sold_date_sk
INNER JOIN 
    avg_discount ON catalog_sales.cs_item_sk = avg_discount.cs_item_sk
WHERE 
    i_manufact_id = 29
    AND d_date BETWEEN DATE '1999-01-07' AND (DATE '1999-01-07' + INTERVAL '90' DAY)
    AND cs_ext_discount_amt > avg_discount.avg_disc_amt
LIMIT 100;