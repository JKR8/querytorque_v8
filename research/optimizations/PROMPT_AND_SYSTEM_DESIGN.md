# SQL Optimization Prompt & System Design

This document captures the prompt format and system architecture for LLM-based SQL optimization.

---

## The Problem

AST-based rule detection produces noise:
- Q1: 9 issues detected, 0 were the actual problem
- Q2: 13 issues detected, 0 were the actual problem

EXPLAIN plan analysis identifies bottlenecks but requires structured prompting to get useful optimizations from LLMs.

---

## Two Modes

### 1. One-Shot Mode

For simple optimizations (predicate pushdown, filter pushdown):
- Single LLM call with structured prompt
- Works well for Q1, Q2 style queries

### 2. Agentic Loop Mode

For complex optimizations (scan consolidation, join reordering):
- Multiple iterations with sample DB feedback
- Required for Q23 style queries

---

## Block Map Format

The Block Map provides a clause-level view of query structure:

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ BLOCK                  │ CLAUSE   │ CONTENT SUMMARY                             │
├─────────────────────────────────────────────────────────────────────────────────┤
│ frequent_ss_items      │ .select  │ substr(...), i_item_sk, d_date, count(*)    │
│                        │ .from    │ store_sales, date_dim, item                 │
│                        │ .where   │ ss_sold_date_sk = d_date_sk, ss_item_sk...  │
│                        │ .group_by│ substr(...), i_item_sk, d_date              │
│                        │ .having  │ count(*) > 4                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│ max_store_sales        │ .select  │ max(csales) tpcds_cmax                      │
│                        │ .from    │ (subquery: store_sales, customer, date_dim) │
├─────────────────────────────────────────────────────────────────────────────────┤
│ best_ss_customer       │ .select  │ c_customer_sk, sum(...) ssales              │
│                        │ .from    │ store_sales, customer                       │
│                        │ .where   │ ss_customer_sk = c_customer_sk              │
│                        │ .group_by│ c_customer_sk                               │
│                        │ .having  │ sum(...) > 0.95 * (select from max_store_sales) │
├─────────────────────────────────────────────────────────────────────────────────┤
│ main_query.union[0]    │ .select  │ cs_quantity * cs_list_price sales           │
│                        │ .from    │ catalog_sales, date_dim                     │
│                        │ .where   │ d_year=2000, d_moy=5, IN(frequent), IN(best)│
└─────────────────────────────────────────────────────────────────────────────────┘

Refs:
  best_ss_customer.having    → max_store_sales
  main_query.union[0].where  → frequent_ss_items, best_ss_customer

Repeated Scans:
  store_sales: 4× (frequent_ss_items.from, max_store_sales.from, best_ss_customer.from)

Filter Gaps:
  ⚠️ best_ss_customer.from: scans store_sales WITHOUT year filter
     but .having refs max_store_sales which HAS year filter
```

---

## Algorithm Section

```markdown
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
```

---

## Operations Format

```json
{
  "operations": [
    {"op": "add_cte", "after": "cte_name", "name": "new_cte", "sql": "SELECT ..."},
    {"op": "delete_cte", "name": "old_cte"},
    {"op": "replace_clause", "target": "cte_name.from", "sql": "new FROM clause"},
    {"op": "patch", "target": "cte_name.where", "patches": [
      {"search": "old text", "replace": "new text"}
    ]}
  ],
  "semantic_warnings": [],
  "explanation": "summary"
}
```

### Block ID Syntax

```
{cte}.select       {cte}.from        {cte}.where
{cte}.group_by     {cte}.having      {cte}.order_by

main_query.union[N].select    main_query.union[N].from
main_query.union[N].where     main_query.union[N].group_by
```

---

## Agentic Loop Feedback

When an optimization fails:

### Semantic Error
```
Semantics: ❌ INCORRECT
  Original:  7637648.56
  Optimized: None

The optimization changed the query semantics. Common causes:
- Removed a join without adding IS NOT NULL for the FK column
- Added a filter that was intentionally missing (e.g., all-time vs period)

Revert the breaking change and try a different approach.
```

### Performance Regression
```
Speedup: 0.87x
⚠️ Optimization made query SLOWER. Try a different approach:
- Scan consolidation (CASE WHEN for conditional aggregates)
- Join reordering (filter by smallest result first)
```

### Negligible Improvement
```
Speedup: 1.05x
⚠️ Negligible speedup (<10%). Try more aggressive optimization:
- Can multiple scans of same table be consolidated?
- Can a join be eliminated with IS NOT NULL?
```

---

## Implementation

### Files

```
packages/qt-sql/qt_sql/optimization/
├── block_map.py           # Block Map generation + one-shot prompt
│   ├── generate_block_map(sql) → BlockMapResult
│   ├── format_block_map(result) → str
│   └── build_full_prompt(sql, plan_summary) → str
│
├── iterative_optimizer.py # Agentic loop
│   ├── test_optimization(original, optimized, db_path) → TestResult
│   ├── format_test_feedback(result, iteration) → str
│   ├── apply_operations(sql, operations) → str
│   ├── parse_response(response) → dict
│   └── run_optimization_loop(sql, db_path, llm_callback, ...) → dict
│
└── __init__.py            # Exports
```

### Usage

```python
from qt_sql.optimization import (
    build_full_prompt,
    run_optimization_loop,
    apply_operations,
    parse_response,
)

# One-shot
prompt = build_full_prompt(sql, plan_summary)
response = llm.complete(prompt)
optimized = apply_operations(sql, parse_response(response)["operations"])

# Agentic loop
result = run_optimization_loop(
    sql=original_sql,
    sample_db_path="/path/to/sample.duckdb",
    llm_callback=lambda p: llm.complete(p),
    max_iterations=5,
    target_speedup=1.5,
)
print(f"Best speedup: {result['best_speedup']:.2f}x")
print(result['best_sql'])
```

---

## Key Learnings

### What Works

1. **Predicate/Filter Pushdown** (Q1, Q2)
   - LLMs can do this in one shot
   - "Add filter to CTE" is additive and safe

2. **Scan Consolidation** (Q23)
   - Requires iteration
   - CASE WHEN to merge filtered + unfiltered aggregates

3. **Join Elimination** (Q23)
   - Must add `IS NOT NULL` for FK column
   - LLMs often forget this → semantic error

### What Fails

1. **Adding "missing" filters**
   - Often intentional (all-time vs period comparison)
   - LLMs assume consistency = bugs

2. **Redundant filter addition**
   - Filter already exists via JOIN
   - Adding IN subquery just adds overhead

3. **Aggressive restructuring**
   - OSS on Q2: added GROUP BY column → wrong semantics
   - Conservative patches safer than rewrites

---

## Benchmark Results

| Query | Optimization | Speedup | Model | Iterations |
|-------|--------------|---------|-------|------------|
| Q1 | Predicate pushdown | 2.10x | DeepSeek-reasoner | 1 |
| Q2 | Filter pushdown | 2.09x | Gemini | 1 |
| Q23 | Scan consolidation + join reorder | 1.25x | Manual | 15 |

Q23 required 15 iterations on sample DB. LLMs failed in one-shot mode:
- Gemini: Broke semantics (NULL handling)
- DeepSeek: Broke semantics (added year filter that was intentionally missing)
