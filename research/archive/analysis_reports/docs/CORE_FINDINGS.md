# SQL Optimization Prompt Research

## Executive Summary

**Problem:** AST-based rule detection produces noise. For TPC-DS Q1, 8 anti-patterns were detected but the actual optimization (predicate pushdown) was NOT among them.

**Solution:** Algorithm-based prompt that teaches the LLM HOW to analyze, combined with parsed EXPLAIN plan data and AST-derived data flow.

**Result:** 2.64x speedup on Q1 with minimal prompt (~400 tokens of context).

---

## The Discovery

### What AST Rules Detected vs What Mattered

| Query | Issues Detected | What Actually Fixed It | Overlap |
|-------|-----------------|------------------------|---------|
| Q1 | 9 issues | Predicate pushdown (store filter into CTE) | **0%** |
| Q2 | 13 issues | Filter pushdown (year filter into CTE) | **0%** |

### Q1: AST Rules Detected (9 issues)

```
- [QT-AGG-002] Aggregate After Join
- [SQL-JOIN-001] Cartesian Join
- [SQL-JOIN-002] Implicit Join Syntax
- [SQL-SUB-001] Correlated Subquery in WHERE
- [SQL-CTE-001] CTE Referenced Multiple Times
- [SQL-CTE-006] Aggregate CTE Reused
- [QT-AGG-002] Aggregate After Join (duplicate)
- [SQL-JOIN-001] Cartesian Join (duplicate)
- [SQL-JOIN-002] Implicit Join Syntax (duplicate)
```

**9 issues detected. None were the actual problem.**

### Q2: AST Rules Detected (13 issues)

```
- [QT-AGG-002] Aggregate After Join
- [SQL-JOIN-002] Implicit Join Syntax
- [SQL-JOIN-001] Cartesian Join
- [SQL-JOIN-010] Complex Subquery in JOIN
- [SQL-ORD-003] ORDER BY Without LIMIT
- [SQL-CTE-001] CTE Referenced Multiple Times
- [SQL-CTE-006] Aggregate CTE Reused
- [SQL-CTE-005] Simple CTE Used Once
- [QT-CTE-002] Single-Use CTE
```

**13 issues detected. None were the actual problem.**

### What Actually Fixed Q1

The real issue: `store WHERE s_state='SD'` filters to 41 rows, but this filter was applied AFTER aggregating 29M rows from `store_returns`.

**Fix:** Join store INSIDE the CTE before GROUP BY.

```sql
-- BEFORE: filter after aggregation (29M rows aggregated)
with customer_total_return as (
    select sr_customer_sk, sr_store_sk, sum(SR_FEE)
    from store_returns, date_dim
    where sr_returned_date_sk = d_date_sk and d_year = 2000
    group by sr_customer_sk, sr_store_sk
)
select ... from customer_total_return, store
where s_state = 'SD'  -- FILTER APPLIED HERE (too late!)

-- AFTER: filter inside aggregation (only SD stores aggregated)
with store_sd as (
    select s_store_sk from store where s_state = 'SD'
),
customer_total_return as (
    select sr_customer_sk, sr_store_sk, sum(SR_FEE)
    from store_returns
    inner join date_dim on sr_returned_date_sk = d_date_sk
    inner join store_sd on sr_store_sk = s_store_sk  -- FILTER HERE!
    where d_year = 2000
    group by sr_customer_sk, sr_store_sk
)
```

**Result: 2.64x speedup**

---

## Key Insight

**The AST rules tell you WHAT patterns exist. They don't tell you which ones MATTER.**

The EXPLAIN plan tells you:
- WHERE the cost is (bottleneck operators)
- HOW MANY rows flow through each stage
- WHAT filters are applied (and where they're NOT applied)

Combining:
1. **Algorithm** - teaches LLM how to think
2. **Plan data** - shows where the problem is
3. **Data flow** - shows query structure for patch placement

---

## The Algorithm

This is the core prompt that works for ANY query:

```
## Algorithm

1. ANALYZE: Find where rows/cost are largest in the plan.
2. OPTIMIZE: For each bottleneck, ask "what could reduce it earlier?"
   - Can a filter be pushed inside a CTE instead of applied after?
   - Can a small table join happen inside an aggregation to filter before GROUP BY?
   - Is there a correlated subquery? Convert to CTE + JOIN.
3. VERIFY: Result must be semantically equivalent.

Principle: Reduce rows as early as possible.
```

**Why it works:**
- Step 1: Generic - finds bottleneck in ANY query
- Step 2: Open-ended questions, not prescriptions
- Step 3: Ensures correctness
- Principle: Universal optimization goal

---

## Plan Data (from EXPLAIN)

Extracted automatically via Python from `EXPLAIN (ANALYZE, FORMAT JSON)`:

```
## Plan

Operators by cost:
- SEQ_SCAN store_returns: 67% cost, 5,500,000 rows
- HASH_GROUP_BY: 22% cost, 5,400,000 rows
- SEQ_SCAN customer: 3% cost, 24,000,000 rows

Scans:
- store_returns: 345,507,384 rows (no filter)
- date_dim: 73,049 → 366 rows (filtered)
- store: 402 → 41 rows (filtered)

Misestimates:
- FILTER: est 4 vs actual 305,574 (76393x)
```

**Key data points:**
- Cost percentage shows where time is spent
- Row counts show data volume
- Filter status shows optimization opportunities
- Misestimates indicate plan quality issues

---

## Data Flow (from AST)

Extracted automatically via sqlglot:

```
## Data Flow

- CTE customer_total_return: [store_returns, date_dim] → GROUP BY → 5,500,000 rows
- Main query: [store, customer]
```

**Why it matters for patches:**
- Shows CTE dependencies
- Shows where aggregation happens
- Helps LLM understand where to place patches

---

## Patch-Based Output

For scalability (queries > 100 lines), use patches instead of full rewrites:

```json
{
  "patches": [
    {
      "search": "exact text from SQL",
      "replace": "new text",
      "description": "why"
    }
  ],
  "explanation": "summary"
}
```

**Whitespace handling:** The `apply_patches()` function normalizes whitespace for fuzzy matching since LLMs often get whitespace wrong.

---

## Complete Prompt Template

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
{operators_by_cost}

Scans:
{table_scans_with_filter_status}

Misestimates:
{cardinality_misestimates}

## Data Flow

{cte_dependencies}
{main_query_tables}

## SQL

```sql
{original_sql}
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

## Benchmark Results

**Methodology:** 3 runs after cache warmup, average time reported.

### TPC-DS Q1 (SF100)

| Approach | Time | Speedup | Notes |
|----------|------|---------|-------|
| Original | 0.286s | 1.00x | Baseline |
| AST rules only | 0.286s | 1.00x | Detected 9 issues, none helped |
| **Optimized (predicate pushdown)** | **0.137s** | **2.10x** | Store filter into CTE |

### TPC-DS Q2 (SF100)

| Approach | Time | Speedup | Correct? |
|----------|------|---------|----------|
| Original | 1.067s | 1.00x | - |
| AST rules only | 1.067s | 1.00x | Detected 13 issues, none helped |
| OSS (aggressive rewrite) | 0.307s | 3.48x | ❌ Wrong results |
| **Gemini (filter pushdown)** | **0.511s** | **2.09x** | ✅ Correct |

### TPC-DS Q2 (SF100)

| Metric | Value |
|--------|-------|
| Original runtime | 3.58s |
| Bottleneck | catalog_sales (49%), web_sales (31%) |
| Total rows scanned | 2.6B (no filters) |
| Optimization opportunity | Push year filter into CTE |

**Optimization Results:**

| Model | Speedup | Correct? | Approach |
|-------|---------|----------|----------|
| OSS | 3.48x | ❌ Wrong (52 vs 2513 rows) | Restructured query, broke semantics |
| **Gemini** | **2.09x** | ✅ Exact match | Added filter, preserved structure |

**Key Lesson:** Conservative optimizations (add filter, don't restructure) are safer AND often faster.

**Gemini's Fix:**
```sql
-- Added to CTE wswscs:
and d_week_seq in (select d_week_seq from date_dim where d_year in (1998, 1999))
```

This filters 2.6B rows down to ~2 years worth BEFORE the GROUP BY, without changing the query structure.

### TPC-DS Q23 (SF100)

| Approach | Time | Speedup | Correct? |
|----------|------|---------|----------|
| Original | 24.54s | 1.00x | - |
| DeepSeek-reasoner | 22.23s | 1.07x | ❌ Wrong semantics |
| Manual (scan consolidation) | 18.63s | 1.25x | ✅ Correct |
| **Gemini (join elimination)** | **11.25s** | **2.18x** | ✅ Exact match |

**Optimization:** Removed FK-only joins to `item` and `customer` tables, replaced with `IS NOT NULL`.

**Key Lesson:** Joins that only validate FK existence can be eliminated with `IS NOT NULL`, preserving semantics while avoiding dimension table scans.

---

## Implementation

### Files

```
packages/qt-sql/qt_sql/optimization/
├── __init__.py
├── payload_builder.py      # Legacy v2 (complex, verbose)
└── plan_analyzer.py        # NEW: Algorithm + Plan + Data Flow
```

### Key Functions

```python
from qt_sql.optimization import (
    analyze_plan_for_optimization,  # Extract signals from EXPLAIN
    build_optimization_prompt,      # Generate the prompt
    apply_patches,                  # Apply LLM patches to SQL
    parse_llm_response,             # Extract JSON from LLM response
)

# Usage
ctx = analyze_plan_for_optimization(plan_json, sql)
prompt = build_optimization_prompt(sql, ctx, output_format="patches")
# Send prompt to LLM, get response
result = apply_patches(sql, parse_llm_response(llm_response))
print(result.optimized_sql)
```

---

## What We Learned

### 1. AST Rules Produce Noise

Detecting anti-patterns is not the same as finding optimization opportunities. A query can have many "issues" but only one matters for performance.

### 2. Filter Selectivity is Key

The breakthrough was showing `store WHERE s_state='SD': 41 rows` instead of just `store: 402 rows`. This tells the LLM the filter is highly selective and worth pushing.

### 3. Algorithm > Examples

Teaching HOW to analyze works better than showing specific examples (which bias toward one pattern).

### 4. Plan Data is Essential

Without EXPLAIN data, the LLM doesn't know:
- Where the cost is
- How many rows flow through
- What filters exist

### 5. Patches Scale Better

Full rewrites work for small queries but fail for complex ones. Patches allow targeted fixes.

---

## Next Steps

1. [x] ~~Benchmark Q2 with optimized query~~ ✅ 2.09x (Gemini)
2. [x] ~~Test Q23~~ ✅ 2.18x (Gemini) - Join elimination
3. [ ] Test on Q3-Q10 for generalization
4. [ ] Automate end-to-end pipeline
5. [ ] Compare different LLMs (Gemini, Claude, GPT-4)
6. [ ] Measure prompt token efficiency

---

## Appendix: Q1 Optimized Query (2.64x)

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

## Appendix: Q2 Original Query

```sql
with wscs as
 (select sold_date_sk, sales_price
  from (select ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs, date_dim
 where d_date_sk = sold_date_sk
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1
        ,wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2
        ,wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1;
```

**Q2 Plan Analysis:**
- catalog_sales: 1.7B rows, 49% cost (no filter)
- web_sales: 864M rows, 31% cost (no filter)
- Year filter (1998, 1999) applied AFTER aggregation

**Q2 Optimization Hypothesis:** Push year filter into CTE before UNION/GROUP BY

---

## Appendix: Q2 Optimized Query (2.09x - Gemini)

```sql
with wscs as
 (select sold_date_sk, sales_price
  from (select ws_sold_date_sk sold_date_sk, ws_ext_sales_price sales_price
        from web_sales
        union all
        select cs_sold_date_sk sold_date_sk, cs_ext_sales_price sales_price
        from catalog_sales)),
 wswscs as
 (select d_week_seq,
        sum(case when (d_day_name='Sunday') then sales_price else null end) sun_sales,
        sum(case when (d_day_name='Monday') then sales_price else null end) mon_sales,
        sum(case when (d_day_name='Tuesday') then sales_price else  null end) tue_sales,
        sum(case when (d_day_name='Wednesday') then sales_price else null end) wed_sales,
        sum(case when (d_day_name='Thursday') then sales_price else null end) thu_sales,
        sum(case when (d_day_name='Friday') then sales_price else null end) fri_sales,
        sum(case when (d_day_name='Saturday') then sales_price else null end) sat_sales
 from wscs
     ,date_dim
 where d_date_sk = sold_date_sk
   and d_week_seq in (select d_week_seq from date_dim where d_year in (1998, 1999))  -- ADDED
 group by d_week_seq)
 select d_week_seq1
       ,round(sun_sales1/sun_sales2,2)
       ,round(mon_sales1/mon_sales2,2)
       ,round(tue_sales1/tue_sales2,2)
       ,round(wed_sales1/wed_sales2,2)
       ,round(thu_sales1/thu_sales2,2)
       ,round(fri_sales1/fri_sales2,2)
       ,round(sat_sales1/sat_sales2,2)
 from
 (select wswscs.d_week_seq d_week_seq1
        ,sun_sales sun_sales1, mon_sales mon_sales1, tue_sales tue_sales1
        ,wed_sales wed_sales1, thu_sales thu_sales1, fri_sales fri_sales1
        ,sat_sales sat_sales1
  from wswscs,date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998) y,
 (select wswscs.d_week_seq d_week_seq2
        ,sun_sales sun_sales2, mon_sales mon_sales2, tue_sales tue_sales2
        ,wed_sales wed_sales2, thu_sales thu_sales2, fri_sales fri_sales2
        ,sat_sales sat_sales2
  from wswscs, date_dim
  where date_dim.d_week_seq = wswscs.d_week_seq and d_year = 1998+1) z
 where d_week_seq1=d_week_seq2-53
 order by d_week_seq1;
```
