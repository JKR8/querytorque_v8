# QueryTorque Benchmark Results

**Benchmark:** TPC-DS SF100 (100GB DuckDB)
**Queries:** 99 standard TPC-DS queries
**Validation:** Sample-based checksum comparison

---

## Latest Results by Provider

| Provider | Date | Optimized | Avg Speedup | Wins (>=1.2x) | Regressions | Validated |
|----------|------|-----------|-------------|---------------|-------------|-----------|
| [Kimi K2.5](research/experiments/benchmarks/kimi_benchmark_20260202_221828/REPORT.md) | 2026-02-02 | 87/99 | 1.04x (sample) / 1.17x (full) | 15 | 36 | 47/47 full |
| [DeepSeek V3](research/benchmarks/deepseek/2026-02-01.md) | 2026-02-01 | 82/99 | 1.14x | 20 | 29 | 72/82 |

---

## Best Wins Across All Providers

| Query | DeepSeek V3 | Kimi K2.5 | Best | Pattern |
|-------|-------------|-----------|------|---------|
| Q1 | - | **2.81x** | Kimi | Decorrelation |
| Q15 | 2.98x | 2.67x | DeepSeek | UNION ALL decomposition |
| Q23 | 2.33x | 1.08x | DeepSeek | UNION (DeepSeek failed validation) |
| Q24 | 2.16x | 0.87x | DeepSeek | Scan consolidation |
| Q39 | 2.44x | 0.99x | DeepSeek | Early date filtering |
| Q45 | 2.26x | 0.97x | DeepSeek | Materialized subquery |
| Q74 | - | **1.42x** | Kimi | Year filter pushdown |
| Q78 | - | **1.21x** | Kimi | Projection pruning |
| Q80 | - | **1.24x** | Kimi | Early filter pushdown |
| Q90 | - | **1.84x** | Kimi | Early dimension filtering |
| Q92 | 2.06x | 0.95x | DeepSeek | Early date + item filtering |
| Q93 | - | **2.71x** | Kimi | Early filter pushdown |
| Q95 | 2.25x | 1.36x | DeepSeek | MAT_CTE + EXISTS |

---

## Quick Links

- [Model Comparison](research/benchmarks/MODEL_COMPARISON.md) - Side-by-side LLM comparison
- [Benchmark Methodology](research/benchmarks/README.md)
- [DeepSeek V3 Results](research/benchmarks/deepseek/2026-02-01.md)
- [Template for New Runs](research/benchmarks/_template.md)

---

## Pattern Summary

| Pattern | Description | Typical Speedup |
|---------|-------------|-----------------|
| `DECORRELATE` | Convert correlated subquery to pre-computed CTE | 2-3x |
| `UNION` | Split OR conditions into UNION ALL branches | 2-3x |
| `CTE_DATE` | Early date filtering via CTE | 1.5-2.5x |
| `EARLY_FILTER` | Push dimension filter before fact join | 1.5-2.5x |
| `MAT_CTE` | Materialized CTE for dimension filtering | 1.2-2x |
| `CTE` | Generic CTE restructuring | 1.1-1.5x |
| `PRED` | Predicate pushdown | 1.1-1.3x |
| `EXISTS` | IN to EXISTS conversion | 1.1-1.3x |

---

*Updated: 2026-02-02*
