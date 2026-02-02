with customer_total_return as (
    select 
        sr_customer_sk as ctr_customer_sk,
        sr_store_sk as ctr_store_sk,
        sum(SR_FEE) as ctr_total_return,
        avg(sum(SR_FEE)) over (partition by sr_store_sk) * 1.2 as store_avg_threshold
    from store_returns
    join date_dim on sr_returned_date_sk = d_date_sk
    join store on sr_store_sk = s_store_sk and s_state = 'SD'
    where d_year = 2000
    group by sr_customer_sk, sr_store_sk
)
select c_customer_id
from customer_total_return ctr
join customer on ctr.ctr_customer_sk = c_customer_sk
where ctr_total_return > store_avg_threshold
order by c_customer_id
LIMIT 100;