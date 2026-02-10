-- start query 93 in stream 0 using template query93.tpl
select ss_customer_sk
       ,sum(act_sales) sumsales
from (
    select ss.ss_item_sk
           ,ss.ss_ticket_number
           ,ss.ss_customer_sk
           ,case when sr.sr_return_quantity is not null 
                 then (ss.ss_quantity - sr.sr_return_quantity) * ss.ss_sales_price
                 else (ss.ss_quantity * ss.ss_sales_price) 
            end act_sales
    from store_sales ss
    left outer join store_returns sr 
        on sr.sr_item_sk = ss.ss_item_sk
        and sr.sr_ticket_number = ss.ss_ticket_number
    left outer join reason r 
        on sr.sr_reason_sk = r.r_reason_sk
        and r.r_reason_desc = 'duplicate purchase'
    where (sr.sr_reason_sk is null or r.r_reason_sk is not null)
) t
group by ss_customer_sk
order by sumsales, ss_customer_sk
LIMIT 100;

-- end query 93 in stream 0 using template query93.tpl