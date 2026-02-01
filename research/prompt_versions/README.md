# Prompt Versions Comparison

All prompt formats tested for SQL optimization, ordered by complexity.

---

## Summary Table

| Version | Name | Complexity | Best Speedup | Output Format | Key Features |
|---------|------|------------|--------------|---------------|--------------|
| **v1** | Minimal | Low | 2.17x | SQL only | Row counts + "Reduce rows early" |
| **v2** | Algo-Plan-Example | Low-Med | 2.54x | SQL only | Algorithm + Example + Plan Summary |
| **v3** | Algo-Plan | Low-Med | 2.10x | Patches JSON | Algorithm + Plan + JSON patches |
| **v4** | Algo-Plan-SQL | Low-Med | 1.50x (Kimi) | SQL + explanation | Simple version, works well with Kimi |
| **v5** | Block-Map-Patterns | Med-High | - | Operations JSON | Block Map + 5 Optimization Patterns |
| **v6** | Full (Current) | High | 2.98x | Operations JSON | Plan + Block Map + Patterns + Ops |
| **v7** | Optimized | Med | **5.52x** | SQL + 1 sentence | Best of all: Example + Selectivity + Simple output |

---

## v7: Optimized (RECOMMENDED)

**File:** `v7_optimized.txt` | **Example:** `v7_q1_example.txt`

**Design choices - combines best elements:**

| Element | From | Why It Works |
|---------|------|--------------|
| Algorithm steps | v4 | Guides thinking: FIND -> REDUCE -> VERIFY |
| WRONG/RIGHT example | v2 | Teaches exact pattern (2.54x with Gemini) |
| Filter selectivity | v1 | "store: 402 -> 41 rows" is actionable |
| Bottleneck markers | new | `<-- BOTTLENECK` and `<-- HIGH SELECTIVITY` |
| Simple output | v4 | SQL + 1 sentence (no JSON complexity) |

**What we removed:**
- Block Map tables (verbose, not always useful)
- JSON operations format (complex, hurts some models)
- 5 optimization patterns list (example teaches better than list)
- DSPy multi-turn format (confused Kimi)

**Structure:**
```
1. Algorithm (3 clear steps)
2. WRONG/RIGHT Example (teaches the pattern)
3. Bottlenecks (cost-ordered, row counts)
4. Table Scans with Selectivity (shows filter opportunities)
5. SQL
6. Output: SQL + 1 sentence explanation
```

---

## v7 Test Results (2026-02-02)

Tested on queries where DSPy struggled with Kimi:

| Query | DSPy + Kimi | v7 + Kimi | Improvement |
|-------|-------------|-----------|-------------|
| Q3 | 0.61x (slower!) | **5.52x** | 9x better |
| Q27 | 0.71x (slower!) | **3.28x** | 4.6x better |
| Q15 | (DeepSeek: 2.98x) | **1.86x** | Kimi now works |

**Why v7 works:**
1. Simple algorithm (FIND → REDUCE → VERIFY) vs complex patterns list
2. WRONG/RIGHT example teaches visually (Kimi learns from examples)
3. Bottleneck markers (`<-- BOTTLENECK`, `<-- HIGH SELECTIVITY, JOIN EARLY`)
4. Simple output (SQL + 1 sentence) vs complex JSON operations

**Kimi's Q3 optimization (5.52x):**
```sql
WITH filtered_item AS (
    SELECT i_item_sk, i_brand_id, i_brand FROM item WHERE i_manufact_id = 816
),
filtered_date AS (
    SELECT d_date_sk, d_year FROM date_dim WHERE d_moy = 11
)
SELECT fd.d_year, fi.i_brand_id brand_id, fi.i_brand brand, SUM(ss.ss_sales_price) sum_agg
FROM filtered_item fi
JOIN store_sales ss ON ss.ss_item_sk = fi.i_item_sk
JOIN filtered_date fd ON fd.d_date_sk = ss.ss_sold_date_sk
GROUP BY fd.d_year, fi.i_brand, fi.i_brand_id
ORDER BY fd.d_year, sum_agg DESC, brand_id
LIMIT 100;
```

---

## v1: Minimal (BEST SIMPLICITY)

**File:** `v1_minimal.txt`

```
Optimize this SQL. Reduce rows early.

Row counts:
- store_returns: 29M rows
- store WHERE s_state='SD': 41 rows
- date_dim WHERE d_year=2000: 366 rows

```sql
<query>
```

Return optimized SQL only.
```

**Key insight:** Shows FILTER SELECTIVITY (not just total rows). DeepSeek immediately understood to push `store` filter into CTE.

**Result:** 2.17x speedup on Q1

---

## v2: Algo-Plan-Example

**File:** `v2_algo_plan_example.txt`

```
Optimize this SQL query.

## Optimization Process

1. ANALYZE: Look at the plan summary. Find where rows are largest.
2. OPTIMIZE: For each large row source, ask "what could reduce it earlier?"
3. VERIFY: The result must be semantically equivalent.

Key principle: Reduce rows as early as possible.

## Example: Predicate Pushdown

WRONG - filter after aggregation:
<example with comment showing it aggregates ALL stores>

RIGHT - filter inside aggregation:
<example with filter INSIDE before GROUP BY>

The RIGHT version aggregates 100x fewer rows.

## Plan Summary
<operators by cost, table scans, cardinality misestimates>

## SQL
<query>

Return only the optimized SQL.
```

**Key insight:** WRONG vs RIGHT example teaches the exact pattern we want.

**Result:** Gemini achieved 2.54x on Q1 with this format.

---

## v3: Algo-Plan-Patches

**File:** `v3_algo_plan_patches.txt`

```
Optimize this SQL query.

## Optimization Process
<same as v2>

## Plan Summary
<operators by cost, table scans>

## SQL
<query>

## Response Format

Return JSON with patches:
{
  "patches": [
    {"search": "exact text", "replace": "new text", "description": "why"}
  ],
  "explanation": "Brief explanation"
}
```

**Key insight:** Patches allow surgical edits vs full rewrite.

**Result:** 2.10x on Q1

---

## v4: Algo-Plan-SQL (Kimi Optimized)

**File:** `v4_algo_plan_sql.txt`

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
- SEQ_SCAN (store_sales): 94.2% cost, 2,859,381 rows
<etc>

Scans:
- date_dim: 150 rows <- FILTERED by d_moy=11
- store_sales: 2,859,381 rows (NO FILTER)
<etc>

## SQL
<query>

## Output

Return the optimized SQL query directly. Explain your changes briefly.
```

**Key insight:** Simple, clear, no JSON complexity. Works best with Kimi K2.5.

**Results with Kimi:**
- Q1: 1.50x
- Q3: 1.13x
- Q27: 1.08x

DSPy's complex format HURT Kimi (Q3: 0.61x with DSPy format vs 1.13x with v4).

---

## v5: Block-Map-Patterns

**File:** `v5_block_map_patterns.txt`

```
Optimize this SQL query.

## Block Map
<ASCII table showing CTE/query structure>

Refs:
  main_query.from -> customer_total_return

Repeated Scans:
  date_dim: 2x (customer_total_return.from, main_query.from)

Filter Gaps:
  main_query.from: scans date_dim WITHOUT year filter

## Optimization Patterns
1. Dimension filter hoisting...
2. Correlated subquery to window function...
3. Join elimination...
4. UNION ALL decomposition...
5. Scan consolidation...

## SQL
<query>

## Output
Return JSON with operations...
```

**Key insight:** Block Map visualizes structure, but may be overkill for simple models.

---

## v6: Full (Current DSPy Format)

**File:** `v6_full_current.txt`

```
## Execution Plan
<operators by cost, table scans>

## Block Map
<ASCII structure>

## Optimization Patterns
<5 proven patterns>

## SQL
<query>

## Output
Return JSON with operations...
```

**Key insight:** Most complete, but DSPy adds USER/ASSISTANT few-shot demos which can confuse some models.

**Best results:** DeepSeek achieved 2.98x on Q15.

---

## Recommendations

### For DeepSeek V3/R1
Use **v6 (Full)** or **v2 (Algo-Plan-Example)**. DeepSeek handles complex prompts well.

### For Kimi K2.5
Use **v4 (Algo-Plan-SQL)**. Simple format, no JSON, just SQL output.

### For Gemini
Use **v2 (Algo-Plan-Example)**. The WRONG/RIGHT example pattern works well.

### For Quick Testing
Use **v1 (Minimal)**. Fastest to iterate, surprisingly effective.

---

## The Middle Ground

**v4 (Algo-Plan-SQL)** appears to be the sweet spot:
- Has algorithm steps (teaches the optimization process)
- Has plan data (shows where bottlenecks are)
- Simple output (SQL + brief explanation)
- No complex JSON parsing needed
- Works across multiple models

This is what you used successfully with Kimi in the chat window.
