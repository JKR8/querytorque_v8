# TPC-DS Q1 Optimization

**Status:** ✅ SUCCESS
**Speedup:** 2.10x (cache-warmed, 3-run avg)
**Model:** DeepSeek-reasoner

---

## Original Query

```sql
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_FEE) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year = 2000
group by sr_customer_sk
,sr_store_sk)
select c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'SD'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
LIMIT 100;
```

---

## Prompt Used

```
Optimize this SQL query.

## Algorithm

1. ANALYZE: Find where rows/cost are largest in the plan.
2. OPTIMIZE: For each bottleneck, ask "what could reduce it earlier?"
   - Can a filter be pushed inside a CTE instead of applied after?
   - Can a small table join happen inside an aggregation to filter before GROUP BY?
   - Is there a correlated subquery? Convert to CTE + JOIN.
3. VERIFY: Result must be semantically equivalent.

Principle: Reduce rows as early as possible.

## Plan

Operators by cost:
- SEQ_SCAN store_returns: 67% cost, 5,500,000 rows
- HASH_GROUP_BY: 22% cost, 5,400,000 rows
- SEQ_SCAN customer: 3% cost, 24,000,000 rows

Scans:
- store_returns: 345,507,384 rows (no filter)
- date_dim: 73,049 → 366 rows (filtered)
- customer: 24,000,000 rows (no filter)
- store: 402 → 41 rows (filtered)

Misestimates:
- FILTER: est 4 vs actual 305,574 (76393x)
- HASH_JOIN: est 4 vs actual 777,703 (194426x)

## Data Flow

- CTE customer_total_return: [store_returns, date_dim] → GROUP BY → 5,500,000 rows
- Main query: [store, customer]

## SQL

```sql
<original query here>
```

## Output

Return JSON:
```json
{
  "patches": [
    {"search": "exact text from SQL", "replace": "new text", "description": "why"}
  ],
  "explanation": "summary"
}
```

Rules: search must match EXACTLY (including whitespace), patches apply in order, valid JSON only.
```

---

## LLM Response

```json
{
  "patches": [
    {
      "search": "with customer_total_return as\n(select sr_customer_sk as ctr_customer_sk\n,sr_store_sk as ctr_store_sk\n,sum(SR_FEE) as ctr_total_return\nfrom store_returns\n,date_dim\nwhere sr_returned_date_sk = d_date_sk\nand d_year = 2000\ngroup by sr_customer_sk\n,sr_store_sk)",
      "replace": "with store_sd as (\n  select s_store_sk\n  from store\n  where s_state = 'SD'\n), customer_total_return as (\n  select sr_customer_sk as ctr_customer_sk\n  ,sr_store_sk as ctr_store_sk\n  ,sum(SR_FEE) as ctr_total_return\n  from store_returns\n  inner join date_dim on sr_returned_date_sk = d_date_sk\n  inner join store_sd on sr_store_sk = s_store_sk\n  where d_year = 2000\n  group by sr_customer_sk\n  ,sr_store_sk)",
      "description": "Create CTE for SD stores first and join early with store_returns to filter rows before aggregation."
    },
    {
      "search": "from customer_total_return ctr1\n,store\n,customer\nwhere ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2\nfrom customer_total_return ctr2\nwhere ctr1.ctr_store_sk = ctr2.ctr_store_sk)\nand s_store_sk = ctr1.ctr_store_sk\nand s_state = 'SD'\nand ctr1.ctr_customer_sk = c_customer_sk",
      "replace": "from customer_total_return ctr1\ninner join customer on ctr1.ctr_customer_sk = c_customer_sk\nwhere ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2\n  from customer_total_return ctr2\n  where ctr1.ctr_store_sk = ctr2.ctr_store_sk)",
      "description": "Remove store table join (already filtered in CTE) and use explicit JOIN syntax."
    }
  ],
  "explanation": "The main optimization is moving the store filter (s_state='SD') into a CTE and joining it with store_returns BEFORE the aggregation in customer_total_return. This reduces the store_returns rows from 29M to only those from SD stores (~10%). Also converting implicit joins to explicit JOINs."
}
```

---

## Optimized Query

```sql
with store_sd as (
  select s_store_sk
  from store
  where s_state = 'SD'
), customer_total_return as (
  select sr_customer_sk as ctr_customer_sk
  ,sr_store_sk as ctr_store_sk
  ,sum(SR_FEE) as ctr_total_return
  from store_returns
  inner join date_dim on sr_returned_date_sk = d_date_sk
  inner join store_sd on sr_store_sk = s_store_sk
  where d_year = 2000
  group by sr_customer_sk
  ,sr_store_sk)
select c_customer_id
from customer_total_return ctr1
inner join customer on ctr1.ctr_customer_sk = c_customer_sk
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
  from customer_total_return ctr2
  where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
order by c_customer_id
LIMIT 100;
```

---

## Benchmark

| Metric | Original | Optimized |
|--------|----------|-----------|
| Time (cold cache) | ~0.36s | ~0.14s |
| **Speedup (cold)** | 1.00x | **2.64x** |
| Time (warm cache, 3-run avg) | 0.286s | 0.137s |
| **Speedup (warm)** | 1.00x | **2.10x** |
| Rows returned | 100 | 100 ✅ |

---

## AST Issues Detected (not useful)

```
- [QT-AGG-002] Aggregate After Join
- [SQL-JOIN-001] Cartesian Join
- [SQL-JOIN-002] Implicit Join Syntax
- [SQL-SUB-001] Correlated Subquery in WHERE
- [SQL-CTE-001] CTE Referenced Multiple Times
- [SQL-CTE-006] Aggregate CTE Reused
```

**9 issues detected. 0 were the actual problem.**

---

## Key Insight

The optimization was **predicate pushdown**: joining `store WHERE s_state='SD'` (41 rows) INSIDE the CTE before GROUP BY, instead of filtering AFTER aggregating 29M rows.

This was visible in the EXPLAIN plan data:
- `store: 402 → 41 rows (filtered)` showed high selectivity
- `store_returns: 345M rows (no filter)` showed the bottleneck

---

## Block Map Prompt Test (New Approach)

Using the new Block Map + Operations format:

### Response (DeepSeek Reasoner)

```json
{
  "operations": [
    {
      "op": "add_cte",
      "after": "customer_total_return",
      "name": "avg_sales",
      "sql": "SELECT ctr_store_sk, AVG(ctr_total_return) AS avg_ctr_total FROM customer_total_return GROUP BY ctr_store_sk"
    },
    {
      "op": "replace_clause",
      "target": "main_query.from",
      "sql": "customer_total_return ctr1, store, customer, avg_sales"
    },
    {
      "op": "replace_clause",
      "target": "main_query.where",
      "sql": "ctr1.ctr_total_return > (avg_sales.avg_ctr_total * 1.2) AND ctr1.ctr_store_sk = avg_sales.ctr_store_sk AND s_store_sk = ctr1.ctr_store_sk AND s_state = 'TN' AND ctr1.ctr_customer_sk = c_customer_sk"
    }
  ],
  "explanation": "Decorrelated the expensive subquery in the main WHERE clause. The original query performed a correlated subquery to calculate the average sales per store for every row. By moving this calculation to a new CTE ('avg_sales') and joining it in the main query, we reduce the complexity from O(N*M) to a single aggregation pass and a hash join."
}
```

### Optimization Type

**Subquery Decorrelation**: Convert correlated subquery to materialized CTE + JOIN

### Results

| Database | Original | Optimized | Speedup | Correct? |
|----------|----------|-----------|---------|----------|
| Sample (1%) | 0.027s | 0.018s | **1.54x** | ✅ |
| Full SF100 | 0.26s | 0.26s | **1.01x** | ✅ |

### Finding: Sample vs Full Speedup Mismatch

The sample DB showed 1.54x speedup, but full SF100 showed only 1.01x.

**Why?** DuckDB's optimizer already handles correlated subqueries efficiently at scale. The decorrelation was redundant - DuckDB does this automatically when it detects the pattern.

**Lesson:** Sample DB speedups don't always translate to full DB speedups. Query optimizer behavior varies with data statistics.

---

## Gemini Response (With Updated Prompt)

Using the prompt with execution plan data + optimization patterns:

```json
{
  "operations": [
    {
      "op": "replace_clause",
      "target": "customer_total_return",
      "sql": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, sum(sr_return_amt) AS ctr_total_return, avg(sum(sr_return_amt)) OVER (PARTITION BY sr_store_sk) AS ctr_store_avg FROM store_returns, date_dim, store WHERE sr_returned_date_sk = d_date_sk AND d_year = 2000 AND sr_store_sk = s_store_sk AND s_state = 'TN' GROUP BY sr_customer_sk, sr_store_sk"
    },
    {
      "op": "replace_clause",
      "target": "main_query.from",
      "sql": "customer_total_return ctr1, customer"
    },
    {
      "op": "replace_clause",
      "target": "main_query.where",
      "sql": "ctr1.ctr_total_return > (ctr1.ctr_store_avg * 1.2) AND ctr1.ctr_customer_sk = c_customer_sk"
    }
  ],
  "explanation": "1. Predicate Pushdown: s_state = 'TN' moved into CTE. 2. Window Functions: Correlated subquery replaced with AVG(...) OVER (PARTITION BY sr_store_sk). 3. Join Consolidation: store table removed from main query."
}
```

### Results

| Database | Original | Optimized | Speedup |
|----------|----------|-----------|---------|
| Sample (1%) | 0.027s | 0.016s | 1.70x |
| Full SF100 | 0.27s | 0.11s | **2.54x** |

### Techniques Used

1. **Predicate pushdown**: Joined `store` with filter inside CTE
2. **Correlated subquery → window function**: Replaced subquery with `AVG(...) OVER (PARTITION BY sr_store_sk)`
3. **Join consolidation**: Removed `store` from main query

This is better than the original DeepSeek result (2.54x vs 2.10x) because the window function eliminates the correlated subquery entirely.
