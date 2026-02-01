# Q23 Gold Standard Prompt

> **Result:** 2.18x speedup with Gemini (2025-02-01)
> **Key:** Execution Plan + Block Map + replace_cte operation

---

Optimize this SQL query.

## Execution Plan

**Operators by cost:**
- SEQ_SCAN (store_sales): 45.2% cost, 2,879,987,999 rows
- HASH_GROUP_BY: 18.3% cost, 432,156,789 rows
- SEQ_SCAN (customer): 12.1% cost, 24,000,000 rows
- HASH_JOIN: 8.7% cost, 156,432,101 rows
- SEQ_SCAN (date_dim): 4.2% cost, 73,049 rows

**Table scans:**
- store_sales: 2,879,987,999 rows (NO FILTER in best_ss_customer)
- customer: 24,000,000 rows (NO FILTER - only for FK validation)
- date_dim: 73,049 rows ← FILTERED by d_year in (2000,2001,2002,2003)
- item: 204,000 rows ← FILTERED by ss_item_sk join

**Cardinality misestimates:**
- HASH_JOIN (customer): est 1,000 vs actual 24,000,000 (24000x)

---

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
│                        │ .from    │ store_sales, customer                         │
│                        │ .where   │ ss_customer_sk = c_customer_sk                │
│                        │ .group_by │ c_customer_sk                                 │
│                        │ .having  │ SUM(ss_quantity * ss_sales_price) > (95/100)* │
├─────────────────────────────────────────────────────────────────────────────────┤
│ main_query             │ .select  │ c_last_name, c_first_name, sales              │
│                        │ .from    │ (subquery: catalog_sales, customer, date_d... │
│                        │ .where   │ d_year = 2000 AND d_moy = 5 AND ...           │
└─────────────────────────────────────────────────────────────────────────────────┘

Refs:
  best_ss_customer.having → max_store_sales
  main_query.from → frequent_ss_items
  main_query.from → best_ss_customer

Repeated Scans:
  date_dim: 4× (frequent_ss_items.from, max_store_sales.from, main_query.from)
  customer: 4× (max_store_sales.from, best_ss_customer.from, main_query.from)
  store_sales: 3× (frequent_ss_items.from, max_store_sales.from, best_ss_customer.from)

Filter Gaps:
  ⚠️ best_ss_customer.from: scans customer WITHOUT any filter
     customer table joined ONLY for FK validation (c_customer_sk)
```

---

## Optimization Patterns

These patterns have produced >2x speedups:

1. **Predicate pushdown**: A small filtered dimension table is joined AFTER a large fact table aggregation. Fix: Join the dimension INSIDE the CTE before GROUP BY to filter the fact table early.

2. **Scan consolidation**: Same table scanned multiple times with different filters. Fix: Single scan with CASE WHEN expressions to compute multiple aggregates conditionally.

3. **Join elimination**: A table is joined only to validate a foreign key exists, but no columns from it are used. Fix: Remove the join, add `WHERE fk_column IS NOT NULL`.

4. **Correlated subquery to window function**: A correlated subquery computes an aggregate per group. Fix: Replace with a window function in the CTE (e.g., `AVG(...) OVER (PARTITION BY group_col)`).

**Verify**: Optimized query must return identical results.

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
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select * from max_store_sales))
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

Return JSON:
```json
{
  "operations": [...],
  "semantic_warnings": [],
  "explanation": "..."
}
```

### Operations

| Op | Fields | Description |
|----|--------|-------------|
| `add_cte` | `after`, `name`, `sql` | Insert new CTE after specified CTE |
| `delete_cte` | `name` | Remove CTE |
| `replace_cte` | `name`, `sql` | Replace entire CTE body |
| `replace_clause` | `target`, `sql` | Replace clause (`""` to remove) |
| `patch` | `target`, `patches[]` | Snippet search/replace within clause |

### Example
```json
{
  "operations": [
    {"op": "replace_cte", "name": "best_ss_customer", "sql": "SELECT ss_customer_sk AS c_customer_sk, SUM(ss_quantity * ss_sales_price) AS ssales FROM store_sales WHERE ss_customer_sk IS NOT NULL GROUP BY ss_customer_sk HAVING SUM(ss_quantity * ss_sales_price) > (95/100.0) * (SELECT * FROM max_store_sales)"}
  ],
  "semantic_warnings": ["Removed customer table join - added IS NOT NULL to preserve FK filtering"],
  "explanation": "Eliminated unnecessary customer table join in best_ss_customer CTE. The customer table was only used to validate ss_customer_sk exists (FK validation). Replaced with IS NOT NULL check. This removes a 24M row scan."
}
```

### Block ID Syntax
```
{cte}.select    {cte}.from    {cte}.where    {cte}.group_by    {cte}.having
main_query.union[N].select    main_query.union[N].from    ...
```

### Rules
1. **Return 1-5 operations maximum** - focus on highest-impact changes first
2. Operations apply sequentially
3. `patch.search` must be unique within target clause
4. `add_cte.sql` = query body only (no CTE name)
5. All CTE refs must resolve after ops
6. When removing a join, update column references (e.g., `c_customer_sk` → `ss_customer_sk AS c_customer_sk`)

The system will iterate if more optimization is possible. You don't need to fix everything at once.
