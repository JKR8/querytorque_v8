-- Q1 Optimized by Gemini 3.0 Pro
-- Pattern: Predicate pushdown + window function for avg
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
,avg(sum(SR_FEE)) over (partition by sr_store_sk) as ctr_store_avg
from store_returns
,date_dim
,store
where sr_returned_date_sk = d_date_sk
and d_year =2000
and sr_store_sk = s_store_sk
and s_state = 'SD'
group by sr_customer_sk
,sr_store_sk)
 select c_customer_id
from customer_total_return ctr1
,customer
where ctr1.ctr_total_return > ctr1.ctr_store_avg*1.2
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
 LIMIT 100;
