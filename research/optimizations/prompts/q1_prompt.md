# Q1 Optimization Prompt

Optimize this SQL query.

## Execution Plan

**Operators by cost:**
- SEQ_SCAN (store_returns): 69.0% cost, 345,507,384 rows
- HASH_GROUP_BY: 22.0% cost, 5,436,732 rows
- SEQ_SCAN (customer): 4.0% cost, 24,000,000 rows
- HASH_JOIN: 2.0% cost, 671,569 rows
- CTE: 1.0% cost, 0 rows

**Table scans:**
- store_returns: 345,507,384 rows (NO FILTER)
- date_dim: 73,049 rows ← FILTERED by d_year=2000
- customer: 24,000,000 rows (NO FILTER)
- store: 402 rows ← FILTERED by s_state='TN'

**Cardinality misestimates:**
- PROJECTION: est 0 vs actual 671,594 (671594x)
- HASH_JOIN: est 0 vs actual 671,594 (671594x)
- HASH_JOIN: est 0 vs actual 671,569 (671569x)

---

## Block Map
```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BLOCK                  │ CLAUSE   │ CONTENT SUMMARY                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│ customer_total_return  │ .select  │ ctr_customer_sk, ctr_store_sk, ctr_total_r... │
│                        │ .from    │ store_returns                                 │
│                        │ .where   │ sr_returned_date_sk = d_date_sk AND d_year... │
│                        │ .group_by │ sr_customer_sk, sr_store_sk                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│ main_query             │ .select  │ c_customer_id                                 │
│                        │ .from    │                                               │
│                        │ .where   │ ctr1.ctr_total_return > (SELECT AVG(ctr_to... │
│                        │ .group_by │ sr_customer_sk, sr_store_sk                   │
└─────────────────────────────────────────────────────────────────────────────────┘

Refs:
  main_query.from → customer_total_return
  main_query.where → customer_total_return

Repeated Scans:
  date_dim: 2× (customer_total_return.from, main_query.from)

Filter Gaps:
  ⚠️ main_query.from: scans date_dim WITHOUT year filter
     but refs customer_total_return which HAS year filter
```

---

## Optimization Patterns

These patterns have produced >2x speedups:

1. **Predicate pushdown**: A small filtered dimension table is joined AFTER a large fact table aggregation. Fix: Join the dimension INSIDE the CTE before GROUP BY to filter the fact table early.

2. **Scan consolidation**: Same table scanned multiple times with different filters. Fix: Single scan with CASE WHEN expressions to compute multiple aggregates conditionally.

3. **Join elimination**: A table is joined only to validate a foreign key exists, but no columns from it are used. Fix: Remove the join, add `WHERE fk_column IS NOT NULL`.

**Verify**: Optimized query must return identical results.

---

## SQL
```sql
WITH customer_total_return AS
  (SELECT sr_customer_sk AS ctr_customer_sk,
          sr_store_sk AS ctr_store_sk,
          sum(sr_return_amt) AS ctr_total_return
   FROM store_returns,
        date_dim
   WHERE sr_returned_date_sk = d_date_sk
     AND d_year = 2000
   GROUP BY sr_customer_sk,
            sr_store_sk)
SELECT c_customer_id
FROM customer_total_return ctr1,
     store,
     customer
WHERE ctr1.ctr_total_return >
    (SELECT avg(ctr_total_return)*1.2
     FROM customer_total_return ctr2
     WHERE ctr1.ctr_store_sk = ctr2.ctr_store_sk)
  AND s_store_sk = ctr1.ctr_store_sk
  AND s_state = 'TN'
  AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY c_customer_id
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