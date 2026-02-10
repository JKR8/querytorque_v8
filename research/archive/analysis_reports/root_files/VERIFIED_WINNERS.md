# Verified TPC-DS Query Optimization Winners

**Last Updated:** 2026-02-05
**Collated from:** ml_training_data, adaptive_runs, mcts_llm_runs, kimi_benchmark

---

## All Verified Winners (Full DB Speedup >= 1.1x)

| Query | Speedup | Transform | Source | Optimized SQL Location |
|-------|---------|-----------|--------|------------------------|
| **Q11** | **4.00x** | TBD | v5_run_20260205 | (latest run - verify location) |
| **Q15** | **3.17x** | TBD | v5_run_20260205 | (latest run - verify location) |
| Q1 | 2.92x | `decorrelate` | ml_training_data | research/optimized_queries/verified/q1_optimized.sql |
| Q1 | 2.84x | `decorrelate` | deepseek_20260203_230527 | research/experiments/adaptive_runs/deepseek_20260203_230527/q1/ |
| Q9 | 2.11x | `pushdown` (quantity_range) | deepseek_20260204_082844 | research/experiments/adaptive_runs/deepseek_20260204_082844/q9/ |
| Q9 | 2.07x | `pushdown` | v5_run_20260205 | (latest run - verify location) |
| Q15 | 2.78x | `or_to_union` | ml_training_data | research/optimized_queries/verified/q15_optimized.sql |
| Q93 | 2.73x | `early_filter` | ml_training_data | kimi q31-q99 benchmark |
| Q90 | 1.57x | `early_filter` | ml_training_data | kimi q31-q99 benchmark |
| Q95 | 1.37x | `materialize_cte` | ml_training_data | kimi q31-q99 benchmark |
| Q74 | 1.36x | `union_cte_split` | ml_training_data | kimi q31-q99 benchmark |
| Q6 | 1.33x | `date_cte_isolate` | ml_training_data | kimi q1-q30 benchmark |
| Q13 | 1.27x | `reorder_join` + `push_pred` | deepseek_mcts_20260203_212241 | research/experiments/mcts_llm_runs/ |
| Q73 | 1.24x | `materialize_cte` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q80 | 1.24x | `early_filter` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q83 | 1.24x | `date_cte_isolate` | ml_training_data | kimi q31-q99 benchmark |
| Q27 | 1.23x | `early_filter` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q62 | 1.23x | `date_cte_isolate` | ml_training_data | kimi q31-q99 benchmark |
| Q84 | 1.22x | `early_filter` | ml_training_data | kimi q31-q99 benchmark |
| Q78 | 1.21x | `projection_prune` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q5 | 1.20x | `pushdown` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q6 | 1.18x | `decorrelate` | deepseek_20260204_082844 | research/experiments/adaptive_runs/deepseek_20260204_082844/q6/ |
| Q15 | 1.16x | `reorder_join` | deepseek_mcts_20260203_212241 | research/experiments/mcts_llm_runs/ |
| Q16 | 1.13x | `reorder_join` | deepseek_mcts_20260203_212241 | research/experiments/mcts_llm_runs/ |
| Q10 | 1.12x | `push_pred` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q18 | 1.11x | `early_filter` | kimi_benchmark_20260202 | research/experiments/benchmarks/kimi_benchmark_20260202_221828/ |
| Q12 | 1.10x | `decorrelate` | deepseek_mcts_20260203_212241 | research/experiments/mcts_llm_runs/ |
| Q28 | 1.33x | TBD | kimi_benchmark_20260202 | kimi q1-q30 benchmark |
| Q66 | 1.23x | TBD | kimi_benchmark_20260202 | kimi q31-q99 benchmark |
| Q17 | 1.19x | TBD | kimi_benchmark_20260202 | kimi q1-q30 benchmark |
| Q37 | 1.16x | TBD | kimi_benchmark_20260202 | kimi q31-q99 benchmark |
| Q18 | 1.14x | TBD | kimi_benchmark_20260202 | kimi q1-q30 benchmark |
| Q41 | 1.14x | TBD | kimi_benchmark_20260202 | kimi q31-q99 benchmark |
| Q76 | 1.10x | TBD | kimi_benchmark_20260202 | kimi q31-q99 benchmark |

---

## Verified Transforms Summary

| Transform | Example File | Best Query | Best Speedup | Status |
|-----------|--------------|------------|--------------|--------|
| `decorrelate` | decorrelate.json | Q1 | 2.92x | ✓ VERIFIED |
| `or_to_union` | or_to_union.json | Q15 | 2.78x | ✓ VERIFIED |
| `early_filter` | early_filter.json | Q93 | 2.73x | ✓ VERIFIED |
| `pushdown` | quantity_range_pushdown.json | Q9 | 2.11x | ✓ VERIFIED |
| `union_cte_split` | union_cte_split.json | Q74 | 1.36x | ✓ VERIFIED |
| `materialize_cte` | materialize_cte.json | Q95 | 1.37x | ✓ VERIFIED |
| `date_cte_isolate` | date_cte_isolate.json | Q6 | 1.33x | ✓ VERIFIED (needs real SQL) |
| `reorder_join` | reorder_join.json | Q13 | 1.27x | ✓ VERIFIED (needs real SQL) |

---

## Unverified Transforms (No Winning Query)

| Transform | Example File | Status |
|-----------|--------------|--------|
| `flatten_subquery` | flatten_subquery.json | UNVERIFIED - no winning query |
| `inline_cte` | inline_cte.json | UNVERIFIED - no winning query |
| `multi_push_predicate` | multi_push_predicate.json | UNVERIFIED - no winning query |
| `remove_redundant` | remove_redundant.json | UNVERIFIED - no winning query |
| `semantic_late_materialization` | semantic_late_materialization.json | UNVERIFIED - never recommended by ML |

---

## Example Files Needing Updates

### 1. `date_cte_isolate.json`
**Current:** Fabricated example
**Needs:** Real SQL from Q6 (1.33x)
**Location:** `research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready/q6_optimized.sql`

### 2. `reorder_join.json`
**Current:** Theoretical example
**Needs:** Real SQL from Q13 (1.27x)
**Location:** `research/experiments/mcts_llm_runs/deepseek_mcts_llm_20260203_212241/`

### 3. `quantity_range_pushdown.json`
**Current:** Theoretical example claiming Q9
**Verified:** Q9 actually achieved 2.11x with pushdown!
**Location:** `research/experiments/adaptive_runs/deepseek_20260204_082844/q9/attempt_01_response.txt`

---

## ML Recommendation Coverage

The ML system (pattern_weights.json) can only recommend 4 transforms based on GLD pattern detection:

| Transform | Times Recommended | Has GLD Rule? |
|-----------|-------------------|---------------|
| `decorrelate` | 71 | GLD-001, GLD-005 |
| `early_filter` | 57 | GLD-003 |
| `or_to_union` | 51 | GLD-002 |
| `union_cte_split` | 12 | GLD-004, GLD-006 |

Other transforms are only included as padding when ML recommendations run out.

---

## Source Runs Summary

### 1. ml_training_data.csv
- 99 queries analyzed
- 12 winners (>1.2x speedup)
- Best: Q1 (2.92x), Q15 (2.78x), Q93 (2.73x)

### 2. deepseek_20260204_082844 (Adaptive Rewriter)
- 10 queries (Q1-Q10)
- Winners: Q1 (2.67x), Q9 (2.11x), Q6 (1.18x)

### 3. deepseek_mcts_20260203_212241 (MCTS LLM)
- 21 queries (Q1-Q21)
- Winners: Q13 (1.27x), Q15 (1.16x), Q16 (1.13x), Q12 (1.10x)

### 4. kimi_benchmark_20260202_221828
- 37 queries benchmarked on full DB
- Winners: Q27 (1.23x), Q73 (1.24x), Q78 (1.21x), Q80 (1.24x), Q5 (1.2x)
- Additional kimi winners: Q28 (1.33x), Q66 (1.23x), Q17 (1.19x), Q37 (1.16x), Q18 (1.14x), Q41 (1.14x), Q76 (1.10x)

### 5. v5_run_20260205 (Latest - In Progress)
- Q2-Q17 tested so far
- **BIG WINS:**
  - **Q11: 4.00x** (record speedup!)
  - **Q15: 3.17x**
  - **Q9: 2.07x**

---

## Next Steps

1. **Update `date_cte_isolate.json`** with real Q6 SQL
2. **Update `reorder_join.json`** with real Q13 SQL
3. **Update `quantity_range_pushdown.json`** with real Q9 SQL (now verified at 2.11x!)
4. **Consider removing** unverified examples (flatten_subquery, inline_cte, multi_push_predicate, remove_redundant, semantic_late_materialization)
5. **Add GLD rules** for `date_cte_isolate`, `reorder_join`, `materialize_cte` to enable ML recommendations
