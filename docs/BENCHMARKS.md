# QueryTorque Benchmark Results

**Benchmark:** TPC-DS SF100 (100GB DuckDB)
**Queries:** 99 standard TPC-DS queries
**Validation:** Row-level checksum comparison

---

## Latest Results by Provider

| Provider | Date | Mode | Full DB Validated | Avg Speedup | Wins (≥1.2x) | Top Speedup |
|----------|------|------|-------------------|-------------|--------------|-------------|
| [Kimi K2.5](research/experiments/benchmarks/kimi_benchmark_20260202_221828/REPORT.md) | 2026-02-02 | DAG v2 | 47/99 | **1.17x** | 15 | **2.81x** (Q1) |
| [DeepSeek V3](research/benchmarks/deepseek/2026-02-01.md) | 2026-02-01 | DSPy | 72/99 | 1.14x | 20 | **2.98x** (Q15) |

### Primary Benchmark Reports (Canonical Sources)

| Provider | Full Report | Summary JSON | Per-Query Data |
|----------|-------------|--------------|----------------|
| **Kimi K2.5** | [REPORT.md](research/experiments/benchmarks/kimi_benchmark_20260202_221828/REPORT.md) | [summary.json](research/experiments/benchmarks/kimi_benchmark_20260202_221828/summary.json) | [q1-q99/](research/experiments/benchmarks/kimi_benchmark_20260202_221828/) |
| **DeepSeek V3** | [2026-02-01.md](research/benchmarks/deepseek/2026-02-01.md) | [results.json](research/experiments/dspy_runs/all_20260201_205640/results.json) | [dspy_runs/](research/experiments/dspy_runs/all_20260201_205640/) |

---

## Best Wins Across All Providers (≥1.5x on Full DB)

| Query | DeepSeek V3 | Kimi K2.5 | Best | Pattern |
|-------|-------------|-----------|------|---------|
| Q1 | 1.35x | **2.81x** | Kimi | Decorrelation + early filter |
| Q15 | **2.98x** | 2.67x | DeepSeek | OR → UNION ALL decomposition |
| Q23 | 2.33x (invalid) | 1.08x | Kimi | Early filter pushdown |
| Q24 | **2.16x** | 0.87x | DeepSeek | Scan consolidation |
| Q39 | **2.44x** | 0.99x | DeepSeek | CTE date filter split |
| Q45 | **2.26x** | 0.97x | DeepSeek | Materialized subquery |
| Q74 | 1.64x | **1.42x** | DeepSeek | Year filter pushdown |
| Q80 | - | **1.24x** | Kimi | Store returns early filter |
| Q90 | 1.32x | **1.84x** | Kimi | Early dimension filtering |
| Q92 | **2.06x** | 0.95x | DeepSeek | Early date + item filtering |
| Q93 | - | **2.71x** | Kimi | Early filter pushdown |
| Q95 | **2.25x** | 1.36x | DeepSeek | MAT_CTE + EXISTS |

### Kimi K2.5 - Full DB Validated Top Performers

| Query | Speedup | Transform | Key Optimization |
|-------|---------|-----------|------------------|
| Q1 | **2.81x** | decorrelate | Correlated subquery → pre-computed CTE |
| Q93 | **2.71x** | early_filter | Dimension filter pushed before fact join |
| Q15 | **2.67x** | or_to_union | OR → UNION ALL branches + date CTE |
| Q90 | **1.84x** | early_filter | Early reason dimension filter |
| Q74 | **1.42x** | pushdown | Year filter into CTE |
| Q95 | **1.36x** | cte_opt | Date filter optimization |
| Q80 | **1.24x** | early_filter | Store returns filter |
| Q73 | **1.24x** | pushdown | Date range filter |
| Q27 | **1.23x** | early_filter | State filter to dimension |
| Q78 | **1.21x** | projection_prune | Unused column elimination |

---

## Quick Links

### Reports
- [Model Comparison](research/benchmarks/MODEL_COMPARISON.md) - Full side-by-side LLM comparison
- [Kimi K2.5 Full Report](research/experiments/benchmarks/kimi_benchmark_20260202_221828/REPORT.md)
- [DeepSeek V3 Results](research/benchmarks/deepseek/2026-02-01.md)

### Knowledge Base
- [Optimization Patterns](research/knowledge_base/OPTIMIZATION_PATTERNS.md) - Proven patterns with examples
- [Strategies YAML](packages/qt-sql/qt_sql/optimization/strategies.yaml) - Machine-readable patterns
- [Rulebook YAML](packages/qt-sql/qt_sql/rulebook.yaml) - 40+ semantic optimization rules

### Data
- [Kimi Raw Results](research/experiments/benchmarks/kimi_benchmark_20260202_221828/summary.json)
- [DeepSeek Raw Results](research/experiments/dspy_runs/all_20260201_205640/results.json)
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
| `MULTI_PUSH_PRED` | Multi-node predicate pushdown through CTEs | 1.1-1.5x |
| `EXISTS` | IN to EXISTS conversion | 1.1-1.3x |

---

## File Organization

```
BENCHMARKS.md                           # ← YOU ARE HERE (root dashboard)

research/
├── benchmarks/
│   ├── MODEL_COMPARISON.md             # Full model comparison
│   ├── deepseek/2026-02-01.md          # DeepSeek results
│   └── README.md                       # Methodology
│
├── experiments/benchmarks/             # Primary benchmark data
│   └── kimi_benchmark_20260202_221828/ # Kimi full run
│       ├── REPORT.md                   # Human-readable
│       ├── summary.json                # Machine-readable
│       └── q{1-99}/                    # Per-query artifacts
│
├── experiments/dspy_runs/              # DSPy optimization runs
│   └── all_20260201_205640/            # DeepSeek full run
│       └── results.json
│
└── knowledge_base/                     # Proven patterns
    └── OPTIMIZATION_PATTERNS.md
```

*Updated: 2026-02-03*
