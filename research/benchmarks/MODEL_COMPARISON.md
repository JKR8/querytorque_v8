# Model Comparison - SQL Optimization

Full benchmark comparison of LLM providers on TPC-DS SF100 optimization task.

---

## Full Benchmark Runs (99 Queries) - Full DB Validated Only

| Model | Date | Validated | Avg Speedup | Wins (≥1.2x) | Top Win | Link |
|-------|------|-----------|-------------|--------------|---------|------|
| **Kimi K2.5** | 2026-02-02 | 47/99 | **1.17x** | 15 | **2.81x** (Q1) | [Report](../experiments/benchmarks/kimi_benchmark_20260202_221828/REPORT.md) |
| **DeepSeek V3** | 2026-02-01 | 72/99 | 1.14x | 20 | **2.98x** (Q15) | [Results](deepseek/2026-02-01.md) |
| Claude Sonnet 4 | - | - | - | - | - | - | Pending |

---

## Kimi K2.5 Full Results Summary

**Date:** 2026-02-02
**Mode:** DAG v2 Optimizer
**Provider:** moonshotai/kimi-k2.5 via OpenRouter

| Metric | Value |
|--------|-------|
| Full DB Validated | 47/99 |
| Avg Speedup | **1.17x** |
| Wins (≥1.2x) | 15 |
| Top Speedup | **2.81x** (Q1) |
| Total Tokens | 196,893 in / 578,826 out |
| Estimated Cost | ~$0.50 |

### Kimi Top Wins (Full DB Validated)

| Query | Speedup | Transform | Key Optimization |
|-------|---------|-----------|------------------|
| Q1 | **2.81x** | decorrelate | Correlated subquery → pre-computed CTE |
| Q93 | **2.71x** | early_filter | Dimension filter before fact join |
| Q15 | **2.67x** | or_to_union | OR → UNION ALL + date CTE |
| Q90 | **1.84x** | early_filter | Early reason dimension filter |
| Q74 | **1.42x** | pushdown | Year filter into CTE |
| Q95 | **1.36x** | cte_opt | Date filter optimization |
| Q80 | **1.24x** | early_filter | Store returns filter |
| Q73 | **1.24x** | pushdown | Date range filter |
| Q27 | **1.23x** | early_filter | State filter to dimension |
| Q78 | **1.21x** | projection_prune | Unused column elimination |

### Kimi Failures (12 queries)

| Type | Queries | Issue |
|------|---------|-------|
| Semantic Error | Q2, Q7, Q16, Q26, Q35, Q51, Q59, Q65, Q81 | Value mismatch |
| Binder Error | Q30, Q44 | Column reference errors |
| Timeout | Q67 | >300s execution |

---

## DeepSeek V3 Full Results Summary

**Date:** 2026-02-01
**Mode:** DSPy Pipeline

| Metric | Value |
|--------|-------|
| Queries Optimized | 82/99 |
| Average Speedup | 1.14x |
| Wins (≥1.2x) | 20 |
| Regressions (<1x) | 29 |
| Validated | 72/82 |

### DeepSeek Top Wins

| Query | Speedup | Pattern |
|-------|---------|---------|
| Q15 | **2.98x** | UNION ALL (OR decomposition) |
| Q39 | **2.44x** | CTE date filtering |
| Q23 | **2.33x** | UNION ALL (failed validation) |
| Q45 | **2.26x** | Materialized subquery |
| Q95 | **2.25x** | MAT_CTE + EXISTS |
| Q24 | **2.16x** | Scan consolidation |
| Q92 | **2.06x** | CTE date + item filter |
| Q41 | **1.69x** | Predicate simplification |
| Q74 | **1.64x** | UNION + early filter |
| Q88 | **1.56x** | Predicate pushdown |

---

## Cost Comparison

| Model | Input $/1M | Output $/1M | Est. Full Run Cost |
|-------|------------|-------------|-------------------|
| DeepSeek V3 | $0.14 | $0.28 | ~$1 |
| Kimi K2.5 | $0.125 | $0.55 | ~$0.50 |
| Claude Sonnet 4 | $3.00 | $15.00 | ~$15 |

*Costs via OpenRouter*

---

## Key Findings

1. **Kimi K2.5** achieved highest single-query speedup (2.92x on Q1) with better decorrelation
2. **DeepSeek V3** achieved highest overall speedup (2.98x on Q15) with OR→UNION pattern
3. **Kimi** has lower failure rate on full DB validation (47/47 vs 72/82)
4. **DeepSeek** produces more regressions but also more wins
5. Both models excel at different patterns:
   - Kimi: Decorrelation, early filter pushdown
   - DeepSeek: UNION ALL decomposition, date filtering
6. Cost is comparable (~$0.50-$1) for full 99-query runs

---

## Data Locations

```
research/experiments/benchmarks/
├── kimi_benchmark_20260202_221828/      # Kimi full run
│   ├── REPORT.md                        # Human-readable report
│   ├── summary.json                     # Machine-readable results
│   ├── full_db_validation.json          # Full DB validation
│   └── q{1-99}/                         # Per-query SQL + validation

research/benchmarks/
├── deepseek/
│   └── 2026-02-01.md                    # DeepSeek results report

research/experiments/dspy_runs/
├── all_20260201_205640/                 # DeepSeek full run
│   └── results.json                     # Machine-readable results
```

---

*Updated: 2026-02-03*
