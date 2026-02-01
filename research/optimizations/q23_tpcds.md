# TPC-DS Q23 Optimization

**Status:** ✅ SUCCESS
**Best Speedup:** 2.18x
**Winning Model:** Gemini (2025-02-01)
**Previous Attempts:** Gemini (failed), DeepSeek-reasoner (failed), Manual (1.25x)

---

## ✅ WINNING OPTIMIZATION (Gemini - 2025-02-01)

### Result

| Metric | Value |
|--------|-------|
| Original time | 24.54s |
| Optimized time | 11.25s |
| **Speedup** | **2.18x** |
| Semantics | ✅ Exact match (7,637,648.56) |

### Prompt Used

See [prompts/q23_prompt.md](prompts/q23_prompt.md) - Block Map + Algorithm format.

### Gemini Response

```json
{
  "operations": [
    {
      "op": "replace_cte",
      "name": "frequent_ss_items",
      "sql": "select ss_item_sk item_sk,d_date solddate,count(*) cnt\n  from store_sales\n      ,date_dim\n  where ss_sold_date_sk = d_date_sk\n    and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)\n    and ss_item_sk is not null\n  group by ss_item_sk,d_date\n  having count(*) >4"
    },
    {
      "op": "replace_cte",
      "name": "max_store_sales",
      "sql": "select max(csales) tpcds_cmax\n  from (select ss_customer_sk,sum(ss_quantity*ss_sales_price) csales\n        from store_sales\n            ,date_dim \n        where ss_sold_date_sk = d_date_sk\n         and ss_customer_sk is not null\n         and d_year in (2000,2000+1,2000+2,2000+3)\n        group by ss_customer_sk) x"
    },
    {
      "op": "replace_cte",
      "name": "best_ss_customer",
      "sql": "select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) ssales\n  from store_sales\n  where ss_customer_sk is not null\n  group by ss_customer_sk\n  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select\n  *\n from max_store_sales)"
    }
  ],
  "semantic_warnings": [
    "Removed joins to `item` and `customer` in CTEs. This assumes referential integrity (Foreign Keys in `store_sales` point to valid rows). `IS NOT NULL` checks added to preserve inner join semantics regarding NULL keys."
  ],
  "explanation": "Removed unnecessary joins to dimension tables (`item`, `customer`) in the CTEs by using the foreign keys present in `store_sales` (e.g., `ss_item_sk`, `ss_customer_sk`) for grouping and selection. This avoids scanning these dimension tables and performing large joins, while `IS NOT NULL` predicates maintain the filtering behavior of the original inner joins."
}
```

### Optimized Query

```sql
with frequent_ss_items as
 (select ss_item_sk item_sk,d_date solddate,count(*) cnt
  from store_sales
      ,date_dim
  where ss_sold_date_sk = d_date_sk
    and d_year in (2000,2000 + 1,2000 + 2,2000 + 3)
    and ss_item_sk is not null
  group by ss_item_sk,d_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select ss_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales
            ,date_dim
        where ss_sold_date_sk = d_date_sk
         and ss_customer_sk is not null
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by ss_customer_sk) x),
 best_ss_customer as
 (select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
  where ss_customer_sk is not null
  group by ss_customer_sk
  having sum(ss_quantity*ss_sales_price) > (95/100.0) * (select
  *
 from max_store_sales))
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
```

### Why It Worked

| CTE | Original | Optimized | Savings |
|-----|----------|-----------|---------|
| `frequent_ss_items` | Joins `item` (204K rows) | `ss_item_sk IS NOT NULL` | Eliminates dimension scan |
| `max_store_sales` | Joins `customer` (12M rows) | `ss_customer_sk IS NOT NULL` | Eliminates dimension scan |
| `best_ss_customer` | Joins `customer` (12M rows) | `ss_customer_sk IS NOT NULL` | Eliminates dimension scan |

**Key insight:** These joins were only validating FK existence, not using any columns from the dimension tables. `IS NOT NULL` preserves the same filtering semantics.

---

## Previous Attempts (Historical Record)

---

## Original Query (Variant 1)

```sql
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
```

---

## Gemini Response

```json
{
  "patches": [
    {
      "search": "from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales\n          from store_sales\n              ,customer...",
      "replace": "from (select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) csales\n          from store_sales\n              ,date_dim...",
      "description": "Remove unnecessary join to 'customer' table in 'max_store_sales' CTE."
    },
    {
      "search": "best_ss_customer as\n   (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales\n    from store_sales\n        ,customer...",
      "replace": "best_ss_customer as\n   (select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) ssales\n    from store_sales...",
      "description": "Remove unnecessary join to 'customer' table in 'best_ss_customer' CTE."
    }
  ],
  "explanation": "Eliminating redundant joins in CTEs that process store_sales (3.4B rows). Grouping by foreign key directly instead of joining customer table."
}
```

---

## ❌ Why It Failed

**Semantic Error:** Removing the `customer` table join without adding `WHERE ss_customer_sk IS NOT NULL`.

The join `ss_customer_sk = c_customer_sk` implicitly filters out NULL foreign keys. When you remove the join and group directly by `ss_customer_sk`, rows with NULL customer keys are included in the aggregation.

This changes:
1. The `max(csales)` threshold in `max_store_sales`
2. Which customers pass the HAVING clause in `best_ss_customer`
3. The final result: **NULL instead of 7637648.56**

---

## Sample DB Validation Failure

The 1% sample database **did not catch this issue** because both original and optimized returned NULL (sample didn't have enough data to pass the HAVING clause).

**Lesson:** Sample validation with NULL results is insufficient. Need additional checks:
- Compare intermediate CTE results
- Use larger samples for complex HAVING clauses
- Validate on full data when possible

---

## Fixed Optimization

Adding `WHERE ss_customer_sk IS NOT NULL` preserves semantics:

```sql
 max_store_sales as
 (select max(csales) tpcds_cmax
  from (select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales, date_dim
        where ss_sold_date_sk = d_date_sk
         and ss_customer_sk IS NOT NULL  -- REQUIRED for semantic equivalence
         and d_year in (2000,2000+1,2000+2,2000+3)
        group by ss_customer_sk)),
 best_ss_customer as
 (select ss_customer_sk c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales
  where ss_customer_sk IS NOT NULL  -- REQUIRED for semantic equivalence
  group by ss_customer_sk
  ...
```

---

## Benchmark

| Version | Time (cache-warmed avg) | Speedup | Correct? |
|---------|------------------------|---------|----------|
| Original | 24.81s | 1.00x | - |
| Gemini (unfixed) | 20.04s | 1.24x | ❌ NULL vs 7637648.56 |
| Gemini (fixed) | 20.11s | 1.23x | ✅ 7637648.56 |

---

## Key Insights

1. **Foreign key NULL handling is critical.** When removing a join, you must preserve the NULL filtering behavior with an explicit `IS NOT NULL` clause.

2. **Sample validation is insufficient for queries with HAVING clauses.** The sample may not have enough data to trigger the filter conditions.

3. **1.23x speedup is modest.** The store_sales table is scanned 4 times in this query, but removing 2 customer joins only eliminates 2 * 24M row lookups. The main bottleneck (store_sales scans) remains.

4. **The prompt correctly identified the bottleneck** (store_sales scanned 4 times = 13.8B rows), but the fix addressed a secondary inefficiency.

---

---

## DeepSeek-reasoner Response

```json
{
  "patches": [
    {
      "search": "best_ss_customer as (select c_customer_sk... from store_sales, customer...)",
      "replace": "...from store_sales, customer, date_dim where... and d_year in (2000,2000+1,2000+2,2000+3)...",
      "description": "Add year filter to best_ss_customer CTE"
    },
    {
      "search": "and cs_item_sk in...",
      "replace": "...and cs_sold_date_sk in (select d_date_sk from date_dim where d_year = 2000 and d_moy = 5)",
      "description": "Push date filter into catalog_sales scan"
    },
    {
      "search": "and ws_item_sk in...",
      "replace": "...and ws_sold_date_sk in (select d_date_sk from date_dim where d_year = 2000 and d_moy = 5)",
      "description": "Push date filter into web_sales scan"
    }
  ]
}
```

### ❌ Why DeepSeek Also Failed

**Patch 1 breaks semantics:** The original query intentionally compares:
- `max_store_sales`: Max customer sales for years 2000-2003
- `best_ss_customer`: Customers whose **ALL-TIME** sales exceed 95% of that max

DeepSeek assumed the missing year filter was an oversight. It wasn't - the query intentionally uses total lifetime value to identify "best" customers.

**Patches 2-3 make it slower:** The date filter is already applied via the `date_dim` join. Adding redundant IN subqueries actually **decreased performance by 13%** (0.87x speedup).

---

## Full Benchmark Results

| Model | Patches | Time | Speedup | Correct? |
|-------|---------|------|---------|----------|
| Original | - | 23.76s | 1.00x | ✅ |
| Gemini | Remove customer joins | 20.04s | 1.19x | ❌ (NULL fkeys) |
| Gemini (fixed) | + IS NOT NULL | 20.11s | 1.18x | ✅ |
| DeepSeek (all) | Year filter + IN subqueries | 22.23s | 1.07x | ❌ (wrong semantics) |
| DeepSeek (2+3 only) | IN subqueries only | 27.43s | 0.87x | ✅ (slower!) |

---

## Why Q23 Is Hard to Optimize

1. **Deceptive "missing" filter**: The `best_ss_customer` CTE intentionally has no year filter - it calculates lifetime customer value, not periodic.

2. **Redundant filter trap**: The date filters already exist via joins. Adding IN subqueries adds overhead without benefit.

3. **Main bottleneck untouchable**: The `store_sales` table is scanned 4 times (13.8B rows total). Reducing this requires query restructuring (e.g., materialized CTE), which changes execution fundamentally.

4. **No safe local optimizations**: Every "obvious" optimization either breaks semantics or makes it slower.

---

## Comparison to Q1/Q2

| Query | Optimization | Speedup | Model | Correct? |
|-------|--------------|---------|-------|----------|
| Q1 | Predicate pushdown into CTE | 2.10x | DeepSeek-reasoner | ✅ |
| Q2 | Filter pushdown via IN subquery | 2.09x | Gemini | ✅ |
| Q23 | Various attempts | ❌ | Both | ❌ or slower |

**Key Insight:** Q1 and Q2 had clear filter pushdown opportunities. Q23's "opportunities" are traps - what looks like a missing filter is actually intentional business logic.

---

## ✅ Manual Optimization (1.25x)

Through iterative testing on a 1% sample DB, we found an optimization that works:

```sql
with ss_agg as
 (select ss_customer_sk,
         sum(ss_quantity*ss_sales_price) as total_sales,
         sum(case when d_year in (2000,2001,2002,2003)
             then ss_quantity*ss_sales_price else 0 end) as filtered_sales
  from store_sales
  left join date_dim on ss_sold_date_sk = d_date_sk
  where ss_customer_sk is not null
  group by ss_customer_sk),
 best_ss_customer as
 (select ss_customer_sk c_customer_sk
  from ss_agg
  where total_sales > (select max(filtered_sales) * 0.95 from ss_agg)),
 frequent_ss_items as
 (select distinct i_item_sk item_sk
  from store_sales ss
  inner join date_dim d on ss.ss_sold_date_sk = d.d_date_sk
    and d.d_year in (2000,2001,2002,2003)
  inner join item i on ss.ss_item_sk = i.i_item_sk
  group by substr(i.i_item_desc,1,30), i.i_item_sk, d.d_date
  having count(*) > 4),
 target_dates as
 (select d_date_sk from date_dim where d_year = 2000 and d_moy = 5),
 cs_filtered as
 (select cs_quantity, cs_list_price, cs_item_sk
  from catalog_sales cs
  inner join target_dates td on cs.cs_sold_date_sk = td.d_date_sk
  inner join best_ss_customer bc on cs.cs_bill_customer_sk = bc.c_customer_sk),
 ws_filtered as
 (select ws_quantity, ws_list_price, ws_item_sk
  from web_sales ws
  inner join target_dates td on ws.ws_sold_date_sk = td.d_date_sk
  inner join best_ss_customer bc on ws.ws_bill_customer_sk = bc.c_customer_sk)
select sum(sales)
from (
  select cs.cs_quantity * cs.cs_list_price as sales
  from cs_filtered cs
  inner join frequent_ss_items fi on cs.cs_item_sk = fi.item_sk
  union all
  select ws.ws_quantity * ws.ws_list_price as sales
  from ws_filtered ws
  inner join frequent_ss_items fi on ws.ws_item_sk = fi.item_sk
)
```

### Optimization Techniques Used

| Technique | What Changed | Impact |
|-----------|--------------|--------|
| **Scan consolidation** | Single store_sales scan with CASE WHEN for both filtered and all-time | Reduces 2 scans → 1 |
| **Join elimination** | Remove customer table, use `ss_customer_sk IS NOT NULL` | Avoids 24M row lookup |
| **Join reordering** | Filter by date + best_ss_customer FIRST, then frequent_ss_items | Smaller intermediate |

### Benchmark

| Version | Time (avg) | Speedup | Correct? |
|---------|-----------|---------|----------|
| Original | 23.28s | 1.00x | ✅ |
| Manual V14 | 18.63s | 1.25x | ✅ |

### Why LLMs Failed But Manual Succeeded

1. **LLMs made assumptions**: Both assumed "missing" filters were bugs
2. **Manual tested incrementally**: Sample DB allowed rapid iteration (0.3s vs 25s)
3. **Different optimization class**: Q23 needed scan consolidation + join reordering, not just predicate pushdown
