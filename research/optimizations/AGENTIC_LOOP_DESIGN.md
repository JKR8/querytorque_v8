# Agentic SQL Optimization Loop Design

> Architecture and requirements for iterative LLM-based SQL optimization

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AGENTIC OPTIMIZER                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌───────────────┐     ┌───────────────┐     ┌───────────────────────────┐  │
│  │ Block Map +   │     │               │     │                           │  │
│  │ Algorithm +   │────▶│  LLM (DeepSeek│────▶│  Operations JSON          │  │
│  │ Examples      │     │  Reasoner)    │     │  {add_cte, patch, ...}    │  │
│  └───────────────┘     └───────────────┘     └─────────────┬─────────────┘  │
│         ▲                                                   │               │
│         │                                                   ▼               │
│         │              ┌───────────────────────────────────────────────┐    │
│         │              │              apply_operations()                │    │
│         │              │  Original SQL + Ops → Optimized SQL            │    │
│         │              └─────────────────────────┬─────────────────────┘    │
│         │                                        │                          │
│         │                                        ▼                          │
│         │              ┌───────────────────────────────────────────────┐    │
│         │              │              Sample DB Test                    │    │
│         │              │  ┌─────────────────┐  ┌─────────────────────┐ │    │
│         │              │  │ Original Query  │  │ Optimized Query     │ │    │
│         │              │  │ Run 3x, avg     │  │ Run 3x, avg         │ │    │
│         │              │  └─────────────────┘  └─────────────────────┘ │    │
│         │              │                                               │    │
│         │              │  Compare: speedup ratio + result equality     │    │
│         │              └───────────────────────────┬───────────────────┘    │
│         │                                          │                        │
│         │                                          ▼                        │
│         │              ┌───────────────────────────────────────────────┐    │
│         │              │           Feedback Generator                   │    │
│         │              │                                               │    │
│  ┌──────┴──────┐       │  Based on result, generate specific feedback  │    │
│  │ Append to   │◀──────│  with next steps to try                       │    │
│  │ Prompt      │       │                                               │    │
│  └─────────────┘       └───────────────────────────────────────────────┘    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Optimization Patterns (Generic)

### 1. Scan Consolidation

**Signal:** Block Map shows same table scanned N times across CTEs

**Pattern:** Merge multiple aggregations into single scan with conditional expressions

```sql
-- BEFORE: Two scans of same table
cte_filtered AS (
  SELECT key, sum(value) FROM big_table WHERE filter_col = X GROUP BY key
),
cte_all AS (
  SELECT key, sum(value) FROM big_table GROUP BY key
)

-- AFTER: Single scan with CASE WHEN
cte_combined AS (
  SELECT key,
         sum(CASE WHEN filter_col = X THEN value ELSE 0 END) AS filtered_sum,
         sum(value) AS total_sum
  FROM big_table
  GROUP BY key
)
```

**When to use:** Multiple CTEs aggregate the same table with different WHERE clauses

---

### 2. Join Elimination

**Signal:** Table joined only to validate foreign key exists, no columns used from it

**Pattern:** Replace FK join with IS NOT NULL check

```sql
-- BEFORE: Join just to validate FK
SELECT a.id, sum(a.value)
FROM fact_table a
JOIN dim_table d ON a.dim_key = d.id
GROUP BY a.id

-- AFTER: IS NOT NULL preserves same filtering
SELECT id, sum(value)
FROM fact_table
WHERE dim_key IS NOT NULL
GROUP BY id
```

**When to use:** Dimension table provides no columns to output, only validates FK

**Critical:** The join `a.dim_key = d.id` implicitly filters NULL keys. Must add explicit IS NOT NULL.

---

### 3. Predicate Pushdown

**Signal:** Filter applied after aggregation could be applied before

**Pattern:** Move filter into CTE/subquery before GROUP BY

```sql
-- BEFORE: Filter after CTE
WITH agg AS (SELECT region, sum(sales) FROM orders GROUP BY region)
SELECT * FROM agg WHERE region = 'West'

-- AFTER: Filter pushed into CTE
WITH agg AS (SELECT region, sum(sales) FROM orders WHERE region = 'West' GROUP BY region)
SELECT * FROM agg
```

**When to use:** Filter column exists in base table before aggregation

---

### 4. Join Reordering

**Signal:** Large table joined before filtering by smaller result set

**Pattern:** Filter by smallest result set first

```sql
-- BEFORE: Big table first
SELECT ... FROM big_fact_table f
JOIN small_filter_cte s ON f.key = s.key
WHERE f.date = '2024-01-01'

-- AFTER: Filter first, then join
SELECT ... FROM (
  SELECT * FROM big_fact_table WHERE date = '2024-01-01'
) f
JOIN small_filter_cte s ON f.key = s.key
```

**When to use:** CTE or subquery produces small result that filters large table

---

## Filter Gap Analysis

**What it is:** A CTE scans a table without a filter, but references another CTE that has that filter.

**Two possibilities:**

| Scenario | What to do |
|----------|------------|
| Oversight | Add the missing filter to reduce scan size |
| Intentional | Leave it alone - different time ranges or aggregation scopes are valid |

**How to tell the difference:**
- Look at what the CTE computes (all-time total? periodic max? running sum?)
- Check if the HAVING clause compares two different scopes
- If unsure, test both versions on sample DB

---

## Feedback Messages

### On Syntax Error
```
❌ ERROR: {error message}

Fix the SQL syntax error and try again.
```

### On Semantic Error
```
Semantics: ❌ INCORRECT
  Original:  {result}
  Optimized: {result}

The optimization changed the query semantics. Common causes:
- Removed a join without adding IS NOT NULL for the FK column
- Added a filter that changes the aggregation scope
- Changed GROUP BY columns

Revert the breaking change and try a different approach.
```

### On Regression (slower)
```
Speedup: 0.87x
⚠️ Optimization made query SLOWER.

Check if added subqueries or CTEs are being scanned multiple times.
Try a different approach.
```

### On Negligible Improvement
```
Speedup: 1.05x
⚠️ Negligible speedup (<10%).

Look at the Block Map for:
- Repeated scans: Can they be consolidated?
- Large table joins: Can any be eliminated with IS NOT NULL?
- Filter gaps: Is there a missing predicate pushdown?
```

### On Good Improvement
```
Speedup: 1.52x
Semantics: ✅ CORRECT

✅ Good speedup achieved. Consider if further improvement possible,
or accept this result.
```

---

## Sample DB Strategy

| Check | Purpose |
|-------|---------|
| Syntax | Query executes without error |
| Result equality | `original == optimized` |
| Row count | Same number of rows |
| Timing (3 runs, discard first) | Measure speedup ratio |

**Limitations:**
- NULL results can match spuriously
- HAVING clauses may not trigger on small data
- Speedup ratios may differ from full data

**Mitigation:**
- Validate final candidate on full database
- Use 1-5% sample for iteration, not 0.1%

---

## Termination Conditions

| Condition | Action |
|-----------|--------|
| `speedup >= 2.0x && correct` | Success, promote to full DB test |
| `iterations >= max_iterations` | Return best correct result |
| `no operations returned` | Stop, LLM has no more ideas |
| `consecutive failures >= 3` | Stop, likely stuck |

**Why 2x threshold?**
- Tested: 1.5x on sample → 1.0x on full (decorrelation, DuckDB already optimizes)
- Tested: 2.1x on sample → 2.1x on full (predicate pushdown, real win)

Sample speedups below 2x often don't survive to full DB - the optimizer already handles them.

---

## Implementation

```python
result = run_optimization_loop(
    sql=query,
    sample_db_path="/path/to/sample.duckdb",
    llm_callback=lambda prompt: deepseek.complete(prompt),
    max_iterations=10,
    target_speedup=1.5,
)

if result["best_speedup"] > 1.2:
    # Test on full database
    full_result = test_optimization(
        original_sql=query,
        optimized_sql=result["best_sql"],
        db_path="/path/to/full.duckdb",
    )
```
