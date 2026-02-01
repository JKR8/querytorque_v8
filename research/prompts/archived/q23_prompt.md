# Q23 Optimization Prompt

> **Result:** ✅ 2.18x speedup (Gemini, 2025-02-01)
> **Previous:** DeepSeek-reasoner failed (broke semantics)
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
WITH frequent_ss_items AS
  (SELECT itemdesc,
          i_item_sk item_sk,
          d_date solddate,
          count(*) cnt
   FROM store_sales,
        date_dim,
     (SELECT SUBSTRING(i_item_desc, 1, 30) itemdesc,
             *
      FROM item) sq1
   WHERE ss_sold_date_sk = d_date_sk
     AND ss_item_sk = i_item_sk
     AND d_year IN (2000,
                    2000+1,
                    2000+2,
                    2000+3)
   GROUP BY itemdesc,
            i_item_sk,
            d_date
   HAVING count(*) >4),
     max_store_sales AS
  (SELECT max(csales) tpcds_cmax
   FROM
     (SELECT c_customer_sk,
             sum(ss_quantity*ss_sales_price) csales
      FROM store_sales,
           customer,
           date_dim
      WHERE ss_customer_sk = c_customer_sk
        AND ss_sold_date_sk = d_date_sk
        AND d_year IN (2000,
                       2000+1,
                       2000+2,
                       2000+3)
      GROUP BY c_customer_sk) sq2),
     best_ss_customer AS
  (SELECT c_customer_sk,
          sum(ss_quantity*ss_sales_price) ssales
   FROM store_sales,
        customer,
        max_store_sales
   WHERE ss_customer_sk = c_customer_sk
   GROUP BY c_customer_sk
   HAVING sum(ss_quantity*ss_sales_price) > (50/100.0) * max(tpcds_cmax))
SELECT c_last_name,
       c_first_name,
       sales
FROM
  (SELECT c_last_name,
          c_first_name,
          sum(cs_quantity*cs_list_price) sales
   FROM catalog_sales,
        customer,
        date_dim,
        frequent_ss_items,
        best_ss_customer
   WHERE d_year = 2000
     AND d_moy = 2
     AND cs_sold_date_sk = d_date_sk
     AND cs_item_sk = item_sk
     AND cs_bill_customer_sk = best_ss_customer.c_customer_sk
     AND cs_bill_customer_sk = customer.c_customer_sk
   GROUP BY c_last_name,
            c_first_name
   UNION ALL SELECT c_last_name,
                    c_first_name,
                    sum(ws_quantity*ws_list_price) sales
   FROM web_sales,
        customer,
        date_dim,
        frequent_ss_items,
        best_ss_customer
   WHERE d_year = 2000
     AND d_moy = 2
     AND ws_sold_date_sk = d_date_sk
     AND ws_item_sk = item_sk
     AND ws_bill_customer_sk = best_ss_customer.c_customer_sk
     AND ws_bill_customer_sk = customer.c_customer_sk
   GROUP BY c_last_name,
            c_first_name) sq3
ORDER BY c_last_name NULLS FIRST,
         c_first_name NULLS FIRST,
         sales NULLS FIRST
LIMIT 100;
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
| `add_cte` | `after`, `name`, `sql` | Insert new CTE |
| `delete_cte` | `name` | Remove CTE |
| `replace_clause` | `target`, `sql` | Replace clause (`""` to remove) |
| `patch` | `target`, `patches[]` | Snippet search/replace |

### Block ID Syntax
```
{cte}.select    {cte}.from    {cte}.where    {cte}.group_by    {cte}.having
main_query.union[N].select    main_query.union[N].from    ...
```

### Rules
1. Operations apply sequentially
2. `patch.search` must be unique within target clause
3. `add_cte.sql` = query body only (no CTE name)
4. All CTE refs must resolve after ops