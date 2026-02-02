-- start query 29 in stream 0 using template query29.tpl
SELECT  
     i.i_item_id
    ,i.i_item_desc
    ,s.s_store_id
    ,s.s_store_name
    ,AVG(ss.ss_quantity)        AS store_sales_quantity
    ,AVG(sr.sr_return_quantity) AS store_returns_quantity
    ,AVG(cs.cs_quantity)        AS catalog_sales_quantity
FROM item i
JOIN store s ON s.s_store_sk <= 400  -- Apply known filter early
JOIN (
    SELECT ss_item_sk, ss_store_sk, ss_customer_sk, ss_ticket_number, ss_quantity
    FROM store_sales
    JOIN date_dim d1 ON d1.d_date_sk = ss_sold_date_sk
    WHERE d1.d_moy = 4 AND d1.d_year = 1999
      AND ss_item_sk <= 203999  -- Apply known filter early
) ss ON i.i_item_sk = ss.ss_item_sk AND s.s_store_sk = ss.ss_store_sk
JOIN (
    SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_return_quantity
    FROM store_returns
    JOIN date_dim d2 ON d2.d_date_sk = sr_returned_date_sk
    WHERE d2.d_moy BETWEEN 4 AND 7 AND d2.d_year = 1999
      AND sr_customer_sk >= 3  -- Apply known filter early
) sr ON ss.ss_customer_sk = sr.sr_customer_sk 
    AND ss.ss_item_sk = sr.sr_item_sk 
    AND ss.ss_ticket_number = sr.sr_ticket_number
JOIN (
    SELECT cs_bill_customer_sk, cs_item_sk, cs_quantity
    FROM catalog_sales
    JOIN date_dim d3 ON d3.d_date_sk = cs_sold_date_sk
    WHERE d3.d_year IN (1999, 2000, 2001)
      AND cs_bill_customer_sk <= 1999995  -- Apply known filter early
) cs ON sr.sr_customer_sk = cs.cs_bill_customer_sk 
    AND sr.sr_item_sk = cs.cs_item_sk
GROUP BY
    i.i_item_id
   ,i.i_item_desc
   ,s.s_store_id
   ,s.s_store_name
ORDER BY
    i.i_item_id 
   ,i.i_item_desc
   ,s.s_store_id
   ,s.s_store_name
LIMIT 100;

-- end query 29 in stream 0 using template query29.tpl