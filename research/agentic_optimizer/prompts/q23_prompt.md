# Q23 Optimization Prompt

> **Result:** ✅ 2.18x speedup (Gemini, 2025-02-01)
> **Approach:** Block Map + Algorithm + Operations JSON
> **Key optimization:** Join elimination with IS NOT NULL

---

Optimize this SQL query.

## Block Map
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BLOCK                  │ CLAUSE   │ CONTENT SUMMARY                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│ frequent_ss_items      │ .select  │ itemdesc, item_sk, solddate, cnt              │
│                        │ .from    │ store_sales                                   │
│                        │ .where   │ ss_sold_date_sk = d_date_sk AND ss_item_sk... │
│                        │ .group_by │ itemdesc, i_item_sk, d_date                   │
│                        │ .having  │ COUNT(*) > 4                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│ max_store_sales        │ .select  │ tpcds_cmax                                    │
│                        │ .from    │ (subquery: store_sales, customer, date_dim)   │
│                        │ .where   │ ss_customer_sk = c_customer_sk AND ss_sold... │
│                        │ .group_by │ c_customer_sk                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│ best_ss_customer       │ .select  │ c_customer_sk, ssales                         │
│                        │ .from    │ store_sales                                   │
│                        │ .where   │ ss_customer_sk = c_customer_sk                │
│                        │ .group_by │ c_customer_sk                                 │
│                        │ .having  │ SUM(ss_quantity * ss_sales_price) > (50 / ... │
├─────────────────────────────────────────────────────────────────────────────────┤
│ main_query             │ .select  │ c_last_name, c_first_name, sales              │
│                        │ .from    │ (subquery: catalog_sales, customer, date_d... │
│                        │ .where   │ ss_sold_date_sk = d_date_sk AND ss_item_sk... │
│                        │ .group_by │ itemdesc, i_item_sk, d_date                   │
│                        │ .having  │ COUNT(*) > 4                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

Refs:
  main_query.from → frequent_ss_items
  main_query.from → best_ss_customer
  main_query.from → frequent_ss_items
  main_query.from → best_ss_customer

Repeated Scans:
  date_dim: 4× (frequent_ss_items.from, max_store_sales.from, main_query.from)
  customer: 4× (max_store_sales.from, best_ss_customer.from, main_query.from)
  store_sales: 3× (frequent_ss_items.from, max_store_sales.from, best_ss_customer.from)
  item: 2× (frequent_ss_items.from, main_query.from)

```

---

## Algorithm

### 1. ANALYZE
- Identify repeated scans of same table
- Check filter gaps (scans without year filter referencing filtered CTEs)
- Note which clauses reference which CTEs

### 2. IDENTIFY OPPORTUNITIES

| Pattern | Signal | Fix |
|---------|--------|-----|
| Scan consolidation | Same table in N CTEs | Single CTE with CASE WHEN |
| Join elimination | Table only for FK validation | `IS NOT NULL` check |
| Join reordering | IN to large CTE | JOIN smallest first |

### 3. VERIFY
- If removing join, add `WHERE fk IS NOT NULL`
- If filter gap, check if intentional (e.g., all-time vs period)

**Principle**: Reduce rows as early as possible.

---

## SQL
```sql
-- start query 23 in stream 0 using template query23.tpl
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim 
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk 
    and d_year in (2000,2000+1,2000+2,2000+3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3) 
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
from
 max_store_sales))
  select sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales 
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer))
 LIMIT 100;
with frequent_ss_items as
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
      ,item
  where ss_sold_date_sk = d_date_sk
    and ss_item_sk = i_item_sk
    and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)
  group by substr(i_item_desc,1,30),i_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,customer
            ,date_dim 
        where ss_customer_sk = c_customer_sk
         and ss_sold_date_sk = d_date_sk
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by c_customer_sk)),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
      ,customer
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
 from max_store_sales))
  select c_last_name,c_first_name,sales
 from (select c_last_name,c_first_name,sum(cs_quantity*cs_list_price) sales
        from catalog_sales
            ,customer
            ,date_dim 
        where d_year = 2000 
         and d_moy = 5 
         and cs_sold_date_sk = d_date_sk 
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and cs_bill_customer_sk = c_customer_sk 
       group by c_last_name,c_first_name
      union all
      select c_last_name,c_first_name,sum(ws_quantity*ws_list_price) sales
       from web_sales
           ,customer
           ,date_dim 
       where d_year = 2000 
         and d_moy = 5 
         and ws_sold_date_sk = d_date_sk 
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)
         and ws_bill_customer_sk = c_customer_sk
       group by c_last_name,c_first_name) 
     order by c_last_name,c_first_name,sales
 LIMIT 100;

-- end query 23 in stream 0 using template query23.tpl

```

---

## Output

### Operations

| Op | Fields | Description |
|----|--------|-------------|
| `add_cte` | `after`, `name`, `sql` | Insert new CTE after specified CTE |
| `delete_cte` | `name` | Remove CTE |
| `replace_cte` | `name`, `sql` | Replace entire CTE body |
| `replace_clause` | `target`, `sql` | Replace clause (`""` to remove) |
| `patch` | `target`, `patches[]` | Snippet search/replace within clause |

### Block ID Syntax
```
{cte}.select    {cte}.from    {cte}.where    {cte}.group_by    {cte}.having
main_query.union[N].select    main_query.union[N].from    ...
```

### Example
```json
{
  "operations": [
    {"op": "add_cte", "name": "new_agg", "after": "cte_a", "sql": "SELECT x, SUM(y) FROM t GROUP BY x"},
    {"op": "replace_cte", "name": "cte_b", "sql": "SELECT * FROM new_agg WHERE z > 10"},
    {"op": "replace_clause", "target": "cte_c.from", "sql": "new_agg"},
    {"op": "delete_cte", "name": "cte_d"}
  ],
  "semantic_warnings": ["Removed join - added IS NOT NULL to preserve filtering"],
  "explanation": "Brief description of optimization"
}
```

### Rules
1. Operations apply sequentially
2. `patch.search` must be unique within target clause
3. `add_cte.sql` = query body only (no CTE name)
4. All CTE refs must resolve after ops