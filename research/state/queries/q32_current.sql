-- Q32 current state: baseline
-- Source: /mnt/d/TPC-DS/queries_duckdb_converted/query_32.sql
-- Best speedup: 1.00x

-- start query 32 in stream 0 using template query32.tpl
select sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 29
and i_item_sk = cs_item_sk 
and d_date between '1999-01-07' and 
        (cast('1999-01-07' as date) + INTERVAL 90 DAY)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1999-01-07' and
                             (cast('1999-01-07' as date) + INTERVAL 90 DAY)
          and d_date_sk = cs_sold_date_sk 
      )
 LIMIT 100;

-- end query 32 in stream 0 using template query32.tpl
