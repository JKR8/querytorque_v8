# Mode 3: Iterative Improvement (Evolutionary Optimization)

**Date:** 2026-02-05

---

## Overview

Mode 3 uses an **evolutionary hill-climbing strategy** where each iteration builds on the best result so far, rotating gold examples and incorporating ML/AST hints until the target speedup is achieved.

---

## Strategy

**Continuous improvement through iteration**
- 1 worker attempts optimization serially up to 5 times
- Each iteration benchmarked on full DB (5-run trimmed mean)
- Stops when speedup > 2.0x OR 5 iterations exhausted
- Gold examples rotate each iteration (different optimization strategies)
- **Input to iteration N is the best SQL from iterations 1..N-1**
- History includes all previous attempts with their speedups
- ML recommendations or AST pattern hints updated each iteration

---

## Key Differences from Mode 1 & 2

| Aspect | Mode 1 (Single) | Mode 2 (Parallel) | Mode 3 (Iterative) |
|--------|-----------------|-------------------|-------------------|
| **Workers** | 1 | 5 | 1 |
| **Max attempts** | 3 retries | 1 attempt + 1 retry | 5 iterations |
| **Learning from** | Failures (errors) | Competition | Successes (speedups) |
| **Input evolves** | ❌ Always original | ❌ Always original | ✅ Best so far |
| **Examples** | Static | Per-worker | Rotate each iteration |
| **Validation** | Sample DB first | Sample DB first | Full DB every iteration |
| **Stops when** | Success or 3 retries | All workers done | Target met or 5 iterations |
| **History** | Error messages | Worker comparison | Speedup progression |

---

## Execution Flow

```
┌─────────────────────────────────────────────────┐
│ Mode 3: Iterative Improvement                  │
└─────────────────────────────────────────────────┘

Iteration 1:
  ├─ Input: Original SQL
  ├─ Examples: Set 1 (decorrelate, early_filter, or_to_union)
  ├─ ML hints: Initial AST analysis
  ├─ Call LLM → Get rewrite
  ├─ Benchmark on full DB (5-run trimmed mean)
  └─ Result: 1.5x speedup ✓ (below target, continue)

Iteration 2:
  ├─ Input: Optimized SQL from Iteration 1 (1.5x)
  ├─ Examples: Set 2 (date_cte_isolate, pushdown, materialize_cte)
  ├─ ML hints: Updated based on current SQL state
  ├─ History: "Previous: 1.5x with decorrelate. Build on this."
  ├─ Call LLM → Get rewrite
  ├─ Benchmark on full DB (5-run trimmed mean)
  └─ Result: 1.8x speedup ✓ (improvement! below target, continue)

Iteration 3:
  ├─ Input: Optimized SQL from Iteration 2 (1.8x)
  ├─ Examples: Set 3 (flatten_subquery, reorder_join, inline_cte)
  ├─ ML hints: Updated hints (gap analysis: need 0.2x more)
  ├─ History: "Iter 1: 1.5x (decorrelate). Iter 2: 1.8x (pushdown). Keep improving."
  ├─ Call LLM → Get rewrite
  ├─ Benchmark on full DB (5-run trimmed mean)
  └─ Result: 2.3x speedup ✅ TARGET MET! Stop early.

Winner: Iteration 3 (2.3x speedup)
```

---

## CLI Command

```bash
qt-sql optimize q1.sql \
  --mode iterative \
  --iterations 5 \
  --full-db tpcds_sf100.duckdb \
  --target-speedup 2.0
```

---

## Example Output

```bash
$ qt-sql optimize q1.sql --mode iterative

Optimizing q1.sql (Mode: Iterative Improvement)
Provider: deepseek (deepseek-reasoner)
Max iterations: 5
Target speedup: 2.0x

[Iteration 1/5]
  ├─ Input: Original SQL
  ├─ Examples: decorrelate, early_filter, or_to_union
  ├─ ML hints: ✓ (3 recommendations)
  ├─ Generating prompt... ✓
  ├─ Calling LLM... ✓ (3.2s)
  ├─ Assembling SQL... ✓
  ├─ Benchmarking full DB (5 runs)... ⏳
  │    Run 1: 4187ms
  │    Run 2: 4305ms
  │    Run 3: 4261ms  ← middle 3
  │    Run 4: 4198ms  ←
  │    Run 5: 4112ms  ←
  │    Trimmed mean: 4190ms (original: 12431ms)
  └─ Speedup: 2.97x ✅ TARGET MET!

🏆 Success after 1 iteration!
Speedup: 2.97x (exceeded 2.0x target)
Saved to: q1_optimized.sql

Iterations summary:
  1. Iteration 1: 2.97x ✅ (decorrelate + early_filter)
```

**Example with multiple iterations:**

```bash
$ qt-sql optimize q15.sql --mode iterative

Optimizing q15.sql (Mode: Iterative Improvement)
Provider: deepseek (deepseek-reasoner)
Max iterations: 5
Target speedup: 2.0x

[Iteration 1/5]
  ├─ Input: Original SQL
  ├─ Examples: decorrelate, early_filter, or_to_union
  ├─ ML hints: ✓ (correlated subquery detected)
  ├─ Benchmarking... ✓
  └─ Speedup: 1.45x (below target, continue)

[Iteration 2/5]
  ├─ Input: Best SQL from Iteration 1 (1.45x)
  ├─ Examples: date_cte_isolate, pushdown, materialize_cte
  ├─ ML hints: ✓ (date filter pushdown opportunity)
  ├─ History: Iteration 1 achieved 1.45x via decorrelation
  ├─ Benchmarking... ✓
  └─ Speedup: 1.72x (improvement! below target, continue)

[Iteration 3/5]
  ├─ Input: Best SQL from Iteration 2 (1.72x)
  ├─ Examples: flatten_subquery, reorder_join, inline_cte
  ├─ ML hints: ✓ (join reordering opportunity, gap: 0.28x)
  ├─ History: Iter 1: 1.45x (decorrelate), Iter 2: 1.72x (pushdown)
  ├─ Benchmarking... ✓
  └─ Speedup: 2.08x ✅ TARGET MET!

🏆 Success after 3 iterations!
Speedup: 2.08x (exceeded 2.0x target)
Saved to: q15_optimized.sql

Iterations summary:
  1. Iteration 1: 1.45x (decorrelate)
  2. Iteration 2: 1.72x (pushdown + date_cte_isolate)
  3. Iteration 3: 2.08x ✅ (reorder_join)

Transform stack:
  decorrelate → pushdown → reorder_join
```

---

## Prompt Structure

### Iteration 1 (Clean Slate)

```
You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

[Gold Examples: decorrelate, early_filter, or_to_union]

## ML Recommendations
Based on AST analysis, high-confidence opportunities:
1. decorrelate (confidence: 0.89) - Correlated subquery detected in WHERE clause
2. early_filter (confidence: 0.76) - Date dimension filter can be pushed earlier
3. or_to_union (confidence: 0.42) - OR conditions in WHERE clause

## Target Nodes
[customer_total_return] GROUP_BY
[main_query] CORRELATED

## Query DAG
[Full DAG representation...]

Now output your rewrite_sets:
```

### Iteration 2 (Building on Previous)

```
You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

[Gold Examples: date_cte_isolate, pushdown, materialize_cte]

## Previous Iterations

### Iteration 1: 1.45x speedup ✓
**Transform used:** decorrelate
**Key changes:**
- Converted correlated subquery to CTE with GROUP BY
- Materialized store_avg_return

**SQL State:**
WITH store_avg_return AS (
  SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS threshold
  FROM customer_total_return
  GROUP BY ctr_store_sk
)
SELECT c_customer_id
FROM customer_total_return ctr1
JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk
...

## Current Challenge
**Current speedup:** 1.45x
**Target:** 2.0x
**Gap:** 0.55x (need 38% more improvement)

## ML Recommendations
Based on current SQL state:
1. pushdown (confidence: 0.82) - date_dim filter can be pushed into CTE
2. date_cte_isolate (confidence: 0.71) - Extract date filtering to reduce join size
3. materialize_cte (confidence: 0.58) - Multiple CTE references detected

## Target Nodes
[Same as before...]

Now output your rewrite_sets to improve upon the 1.45x result:
```

### Iteration 3 (Cumulative History)

```
You are an autonomous Query Rewrite Engine. Your goal is to maximize execution
speed while strictly preserving semantic invariants.

[Gold Examples: flatten_subquery, reorder_join, inline_cte]

## Previous Iterations

### Iteration 1: 1.45x speedup ✓
**Transform:** decorrelate
**Impact:** Eliminated correlated subquery, created materialized CTE

### Iteration 2: 1.72x speedup ✓ (improvement: +0.27x)
**Transform:** pushdown + date_cte_isolate
**Impact:** Pushed date filter earlier, reduced rows before aggregation

**SQL State:**
WITH date_filtered_returns AS (
  SELECT sr_customer_sk, sr_store_sk, sr_fee
  FROM store_returns
  JOIN date_dim ON sr_returned_date_sk = d_date_sk
  WHERE d_year = 2000
),
customer_total_return AS (
  SELECT sr_customer_sk AS ctr_customer_sk,
         sr_store_sk AS ctr_store_sk,
         SUM(sr_fee) AS ctr_total_return
  FROM date_filtered_returns
  GROUP BY sr_customer_sk, sr_store_sk
),
...

## Current Challenge
**Current speedup:** 1.72x
**Target:** 2.0x
**Gap:** 0.28x (need 16% more improvement)

## ML Recommendations
Based on current SQL state:
1. reorder_join (confidence: 0.88) - Join order not optimal (large fact table first)
2. inline_cte (confidence: 0.62) - Single-use date_filtered_returns can be inlined
3. remove_redundant (confidence: 0.41) - ORDER BY may be unnecessary

## AST Patterns Detected
- Large fact table joined before dimension filter (reorder_join opportunity)
- Store dimension filter applied late (early_filter opportunity)

Now output your rewrite_sets to bridge the 0.28x gap:
```

---

## Storage Structure

```
results/q1_20260205_103000_iterative/
├── input/
│   ├── original.sql                    # Original query
│   ├── query_dag.json                  # Initial DAG
│   └── metadata.json                   # Query ID, complexity
│
├── iterations/
│   ├── iteration_1/
│   │   ├── input.sql                   # Original SQL
│   │   ├── examples.json               # Set 1: decorrelate, early_filter, or_to_union
│   │   ├── ml_hints.json               # Initial ML recommendations
│   │   ├── prompt.txt                  # Full prompt
│   │   ├── llm_response.json           # Raw LLM response
│   │   ├── optimized.sql               # Generated SQL
│   │   ├── benchmark_runs.json         # All 5 runs
│   │   └── benchmark_summary.json      # Trimmed mean: 1.45x
│   │
│   ├── iteration_2/
│   │   ├── input.sql                   # Best SQL from iteration 1
│   │   ├── examples.json               # Set 2: date_cte_isolate, pushdown, materialize_cte
│   │   ├── ml_hints.json               # Updated ML hints
│   │   ├── history.txt                 # Summary of iteration 1
│   │   ├── prompt.txt                  # Prompt with history
│   │   ├── llm_response.json
│   │   ├── optimized.sql
│   │   ├── benchmark_runs.json
│   │   └── benchmark_summary.json      # 1.72x (improvement!)
│   │
│   ├── iteration_3/
│   │   ├── input.sql                   # Best SQL from iteration 2
│   │   ├── examples.json               # Set 3: flatten_subquery, reorder_join, inline_cte
│   │   ├── ml_hints.json               # Updated (gap analysis)
│   │   ├── history.txt                 # Summary of iterations 1-2
│   │   ├── prompt.txt
│   │   ├── llm_response.json
│   │   ├── optimized.sql
│   │   ├── benchmark_runs.json
│   │   └── benchmark_summary.json      # 2.08x ✅ TARGET MET
│   │
│   └── iteration_N/
│       └── ... (up to 5 iterations)
│
├── progression/
│   ├── speedup_graph.json              # [1.45, 1.72, 2.08]
│   ├── transform_stack.json            # [decorrelate, pushdown, reorder_join]
│   └── convergence_analysis.txt        # Rate of improvement
│
├── winner/
│   ├── optimized.sql                   # Best result (iteration 3)
│   ├── transform_stack.txt             # Full transform chain
│   └── comparison.txt                  # Original vs final
│
└── summary.json
```

---

## Example Rotation Strategy

### Example Sets (12 transforms, 3 per set, 4 rotations)

```python
EXAMPLE_ROTATION = [
    # Set 1: High-impact structural transforms
    ["decorrelate", "early_filter", "or_to_union"],

    # Set 2: Filter/predicate optimization
    ["date_cte_isolate", "pushdown", "multi_push_predicate"],

    # Set 3: Join and subquery optimization
    ["flatten_subquery", "reorder_join", "materialize_cte"],

    # Set 4: Cleanup and refinement
    ["inline_cte", "remove_redundant", "semantic_rewrite"],

    # Set 5: Mixed (if 5 iterations needed)
    ["decorrelate", "reorder_join", "pushdown"],
]
```

---

## ML Hints Generation

### Priority 1: ML Model (if available)

```python
def get_ml_hints(sql: str, iteration: int, current_speedup: float) -> dict:
    """Get ML recommendations based on current SQL state."""
    # Call ML model trained on historical optimizations
    features = extract_features(sql)
    predictions = ml_model.predict(features)

    return {
        "iteration": iteration,
        "current_speedup": current_speedup,
        "target": 2.0,
        "gap": 2.0 - current_speedup,
        "recommendations": [
            {
                "transform": pred.transform,
                "confidence": pred.confidence,
                "reason": pred.explanation,
                "estimated_gain": pred.speedup_delta
            }
            for pred in predictions.top_k(3)
        ]
    }
```

### Fallback: AST Pattern Matching

```python
def get_ast_hints(sql: str) -> dict:
    """Fallback: AST-based pattern detection."""
    ast = parse_sql(sql)
    patterns = []

    # Detect correlated subqueries
    if has_correlated_subquery(ast):
        patterns.append({
            "pattern": "correlated_subquery",
            "severity": "high",
            "transform": "decorrelate",
            "confidence": 0.85
        })

    # Detect large fact table joins
    if has_large_fact_join(ast):
        patterns.append({
            "pattern": "fact_table_join_order",
            "severity": "high",
            "transform": "reorder_join",
            "confidence": 0.80
        })

    # Detect late filters
    if has_late_dimension_filter(ast):
        patterns.append({
            "pattern": "late_dimension_filter",
            "severity": "medium",
            "transform": "early_filter",
            "confidence": 0.70
        })

    return {
        "method": "ast_pattern_matching",
        "patterns": patterns,
        "recommendations": [
            {"transform": p["transform"], "confidence": p["confidence"], "reason": p["pattern"]}
            for p in sorted(patterns, key=lambda x: x["confidence"], reverse=True)
        ][:3]
    }
```

---

## History Section Format

```markdown
## Previous Iterations

### Iteration 1: 1.45x speedup ✓
**Transform:** decorrelate
**Key changes:**
- Converted correlated subquery to CTE with GROUP BY
- Materialized store_avg_return
**Benchmark:** 12431ms → 8573ms

### Iteration 2: 1.72x speedup ✓ (improvement: +0.27x over iteration 1)
**Transform:** pushdown + date_cte_isolate
**Key changes:**
- Pushed date filter (d_year = 2000) into store_returns CTE
- Reduced rows before aggregation from 2.8M → 287K
**Benchmark:** 12431ms → 7226ms

## Current Challenge
**Current best:** 1.72x speedup
**Target:** 2.0x
**Gap:** 0.28x (need 16% more improvement)

**Analysis:**
- Decorrelation eliminated correlated scan (major win)
- Filter pushdown reduced aggregation input (incremental win)
- Next opportunity: Join reordering (store filter applied late)

Now try to bridge the 0.28x gap while preserving all previous optimizations.
```

---

## Configuration Options

```bash
# Default: 5 iterations, stop early if target met
qt-sql optimize q1.sql --mode iterative

# Custom iteration count
qt-sql optimize q1.sql --mode iterative --iterations 10

# Force all iterations (don't stop early)
qt-sql optimize q1.sql --mode iterative --no-early-stop

# Custom target speedup
qt-sql optimize q1.sql --mode iterative --target-speedup 3.0

# Disable ML hints (use AST only)
qt-sql optimize q1.sql --mode iterative --no-ml-hints

# Custom example rotation
qt-sql optimize q1.sql --mode iterative --example-strategy diverse
```

---

## Comparison: All Three Modes

| Aspect | Mode 1 (Single) | Mode 2 (Parallel) | Mode 3 (Iterative) |
|--------|-----------------|-------------------|-------------------|
| **Strategy** | Retry on failure | Parallel diversity | Evolutionary improvement |
| **Workers** | 1 | 5 | 1 |
| **Max attempts** | 3 | 5 + 1 retry each | 5 |
| **Learns from** | Errors | Competition | Successes |
| **Input** | Original (static) | Original (static) | Best so far (evolving) |
| **Examples** | Static | Per-worker | Rotate |
| **Validation** | Sample → Full | Sample → Full | Full only |
| **Benchmark** | Once (on success) | All valid | Every iteration |
| **Stops when** | Success or 3 fails | All done | Target met or 5 iterations |
| **LLM calls** | 1-3 | 5-10 | 1-5 |
| **Cost** | Low | High | Medium |
| **Time** | Fast (10-60s) | Medium (15-30s) | Slow (30-120s) |
| **Best for** | Production, reliability | Research, exploration | Maximum speedup, stacking |

---

## Use Cases

### When to Use Mode 3

✅ **Best for:**
- Complex queries where multiple optimizations stack well
- When you want absolute maximum speedup (not just meet target)
- Research: Understanding optimization composition
- Queries that respond well to iterative refinement
- When you have time/budget for multiple full DB benchmarks

❌ **Not ideal for:**
- Fast feedback needed (use Mode 1 or Mode 2)
- Cost-sensitive scenarios (many full DB benchmarks)
- Queries with one clear optimization (Mode 1 sufficient)

---

## Example: Q1 with Mode 3

```bash
$ qt-sql optimize q1.sql --mode iterative --iterations 5 --full-db tpcds_sf100.duckdb

Optimizing q1.sql (Mode: Iterative Improvement)

[Iteration 1/5] decorrelate, early_filter, or_to_union
  Speedup: 1.85x ✓ (below target)

[Iteration 2/5] date_cte_isolate, pushdown, materialize_cte
  Input: Best from iteration 1 (1.85x)
  History: Previous 1.85x via decorrelation
  Speedup: 2.15x ✅ TARGET MET!

🏆 Success after 2 iterations!
Final speedup: 2.15x
Transform stack: decorrelate → pushdown
```

---

## Summary

**Mode 3 (Iterative):**
- ✅ Evolutionary hill-climbing
- ✅ Builds on best result
- ✅ Rotating examples
- ✅ ML/AST hints updated each iteration
- ✅ Maximum speedup potential
- ✅ Full history of progress
- ✅ Transform stacking

**Choose Mode 3 when:**
- You want the best possible result
- Multiple optimizations can compose
- Time and cost are acceptable
- Learning from successes, not failures
