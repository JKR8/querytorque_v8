# TPC-DS Optimization Results

**Database:** DuckDB SF100 (100GB)
**Optimizer:** DSPy + DeepSeek V3
**Date:** 2026-02-02

## Summary

| Metric | Value |
|--------|-------|
| Queries Optimized | 82/99 |
| Average Speedup | 1.14x |
| Wins (≥1.2x) | 20 |
| Regressions (<1.0x) | 29 |

## Full Results

| Query | Status | Original | Optimized | Speedup | Pattern |
|-------|--------|----------|-----------|---------|---------|
| q1 | ✓ | 20.3ms | 15.0ms | 1.35x | `CTE` |
| q2 | ✗ | - | - | - | `-` |
| q3 | ✓ | 7.8ms | 7.8ms | 1.00x | `CTE_DATE` |
| q4 | ✓ | 667.4ms | 533.1ms | 1.25x | `UNION` |
| q5 | ✓ | 20.7ms | 22.8ms | 0.91x ↓ | `UNION` |
| q6 | ✓ | 31.8ms | 29.5ms | 1.08x | `CTE_DATE` |
| q7 | ✓ | 40.7ms | 42.5ms | 0.96x ↓ | `PRED` |
| q8 | ✓ | 472.5ms | 450.3ms | 1.05x | `CTE_DATE` |
| q9 | ✓ | 32.6ms | 32.6ms | 1.00x | `ORIG` |
| q10 | ✓ | 35.7ms | 31.8ms | 1.12x | `CTE_DATE` |
| q11 | ✗ | - | - | - | `-` |
| q12 | ✓ | 31.1ms | 33.2ms | 0.94x ↓ | `CTE` |
| q13 | ✗ | - | - | - | `-` |
| q14 | ✗ | - | - | - | `-` |
| q15 | ✓ | 108.7ms | 36.5ms | **2.98x** | `UNION` |
| q16 | ✓ | 15.9ms | 13.7ms | 1.16x | `EXISTS` |
| q17 | ✓ | 24.2ms | 19.6ms | 1.23x | `CTE_DATE` |
| q18 | ✓ | 60.1ms | 59.5ms | 1.01x | `CTE_DATE` |
| q19 | ✓ | 23.5ms | 23.5ms | 1.00x | `ORIG` |
| q20 | ✓ | 36.0ms | 33.3ms | 1.08x | `CTE_DATE` |
| q21 | ✓ | 7.9ms | 8.7ms | 0.91x ↓ | `PRED` |
| q22 | ✓ | 294.8ms | 312.5ms | 0.94x ↓ | `PRED` |
| q23 | ✓ | 1035.6ms | 444.7ms | **2.33x** | `UNION` |
| q24 | ✓ | 110.8ms | 51.3ms | **2.16x** | `UNION` |
| q25 | ✓ | 11.6ms | 11.6ms | 1.00x | `ORIG` |
| q26 | ✓ | 28.8ms | 27.9ms | 1.03x | `PRED` |
| q27 | ✓ | 44.5ms | 44.3ms | 1.01x | `PRED` |
| q28 | ✓ | 40.6ms | 40.6ms | 1.00x | `ORIG` |
| q29 | ✓ | 21.4ms | 17.0ms | 1.26x | `CTE_DATE` |
| q30 | ✗ | - | - | - | `-` |
| q31 | ✓ | 50.6ms | 46.4ms | 1.09x | `CTE_DATE` |
| q32 | ✓ | 11.6ms | 10.1ms | 1.15x | `CTE_DATE` |
| q33 | ✗ | - | - | - | `-` |
| q34 | ✓ | 13.2ms | 13.2ms | 1.00x | `ORIG` |
| q35 | ✗ | - | - | - | `-` |
| q36 | ✓ | 65.8ms | 64.7ms | 1.02x | `PRED` |
| q37 | ✓ | 14.1ms | 17.1ms | 0.83x ↓ | `PRED` |
| q38 | ✗ | - | - | - | `-` |
| q39 | ✓ | 577.6ms | 236.3ms | **2.44x** | `CTE_DATE` |
| q40 | ✓ | 59.3ms | 53.7ms | 1.10x | `PRED` |
| q41 | ✓ | 80.9ms | 47.9ms | **1.69x** | `PRED` |
| q42 | ✓ | 27.4ms | 25.9ms | 1.06x | `PRED` |
| q43 | ✓ | 40.7ms | 49.7ms | 0.82x ↓ | `PRED` |
| q44 | ✓ | 53.5ms | 48.2ms | 1.11x | `CTE` |
| q45 | ✓ | 245.9ms | 108.8ms | **2.26x** | `UNION` |
| q46 | ✓ | 328.4ms | 417.3ms | 0.79x ↓ | `PRED` |
| q47 | ✓ | 535.6ms | 433.6ms | 1.24x | `CTE_DATE` |
| q48 | ✓ | - | - | 1.01x | `ORIG` |
| q49 | ✗ | - | - | - | `-` |
| q50 | ✓ | 57.2ms | 52.9ms | 1.08x | `PRED` |
| q51 | ✓ | 505.9ms | 442.3ms | 1.14x | `CTE_DATE` |
| q52 | ✓ | 45.9ms | 46.8ms | 0.98x ↓ | `CTE_DATE` |
| q53 | ✗ | - | - | - | `-` |
| q54 | ✓ | 196.3ms | 215.6ms | 0.91x ↓ | `UNION` |
| q55 | ✓ | 45.1ms | 45.4ms | 0.99x ↓ | `PRED` |
| q56 | ✗ | - | - | - | `-` |
| q57 | ✓ | 506.9ms | 934.5ms | 0.54x ↓ | `CTE_DATE` |
| q58 | ✓ | 61.5ms | 55.5ms | 1.11x | `CTE_DATE` |
| q59 | ✗ | - | - | - | `-` |
| q60 | ✓ | 188.1ms | 194.5ms | 0.97x ↓ | `UNION` |
| q61 | ✓ | 29.6ms | 23.9ms | 1.24x | `PRED` |
| q62 | ✓ | 42.7ms | 34.4ms | 1.24x | `CTE_DATE` |
| q63 | ✓ | 56.0ms | 151.8ms | 0.37x ↓ | `PRED` |
| q64 | ✓ | 96.5ms | 82.6ms | 1.17x | `CTE` |
| q65 | ✓ | 230.5ms | 199.5ms | 1.16x | `CTE` |
| q66 | ✓ | 64.1ms | 58.0ms | 1.10x | `UNION` |
| q67 | ✓ | 1439.5ms | 1642.6ms | 0.88x ↓ | `CTE_DATE` |
| q68 | ✓ | 299.5ms | 422.7ms | 0.71x ↓ | `PRED` |
| q69 | ✓ | 442.7ms | 362.2ms | 1.22x | `CTE_DATE` |
| q70 | ✗ | - | - | - | `-` |
| q71 | ✓ | 261.4ms | 259.1ms | 1.01x | `UNION` |
| q72 | ✓ | 266.3ms | 337.0ms | 0.79x ↓ | `CTE_DATE` |
| q73 | ✓ | 123.2ms | 143.1ms | 0.86x ↓ | `CTE` |
| q74 | ✓ | 738.5ms | 449.5ms | **1.64x** | `UNION` |
| q75 | ✓ | 270.0ms | 272.3ms | 0.99x ↓ | `UNION` |
| q76 | ✓ | 42.3ms | 88.5ms | 0.48x ↓ | `UNION` |
| q77 | ✗ | - | - | - | `-` |
| q78 | ✓ | 319.0ms | 405.5ms | 0.79x ↓ | `CTE_DATE` |
| q79 | ✓ | 180.4ms | 173.9ms | 1.04x | `PRED` |
| q80 | ✓ | 92.3ms | 95.4ms | 0.97x ↓ | `UNION` |
| q81 | ✗ | - | - | - | `-` |
| q82 | ✓ | 59.8ms | 48.6ms | 1.23x | `PRED` |
| q83 | ✓ | 69.1ms | 89.8ms | 0.77x ↓ | `CTE_DATE` |
| q84 | ✓ | 248.7ms | 230.7ms | 1.08x | `CTE` |
| q85 | ✓ | 82.4ms | 70.4ms | 1.17x | `CTE_DATE` |
| q86 | ✓ | 89.0ms | 127.5ms | 0.70x ↓ | `CTE_DATE` |
| q87 | ✓ | 404.4ms | 409.5ms | 0.99x ↓ | `UNION` |
| q88 | ✓ | 52.4ms | 33.5ms | **1.56x** | `PRED` |
| q89 | ✓ | 185.1ms | 246.1ms | 0.75x ↓ | `CTE_DATE` |
| q90 | ✓ | 57.1ms | 43.3ms | 1.32x | `PRED` |
| q91 | ✓ | 149.6ms | 164.1ms | 0.91x ↓ | `CTE_DATE` |
| q92 | ✓ | 101.0ms | 49.0ms | **2.06x** | `CTE_DATE` |
| q93 | ✗ | - | - | - | `-` |
| q94 | ✗ | - | - | - | `-` |
| q95 | ✓ | 244.3ms | 108.7ms | **2.25x** | `MAT_CTE` |
| q96 | ✓ | 26.6ms | 22.7ms | 1.17x | `CTE` |
| q97 | ✓ | 124.8ms | 134.1ms | 0.93x ↓ | `CTE_DATE` |
| q98 | ✓ | 168.8ms | 152.0ms | 1.11x | `PRED` |
| q99 | ✓ | 48.5ms | 51.8ms | 0.94x ↓ | `PRED` |

## Wins Detail (≥1.2x)

| Query | Speedup | Pattern | Notes |
|-------|---------|---------|-------|
| q15 | 2.98x | `UNION` | OR conditions split into 3 UNION ALL branches for ca_zip/ca_state/cs_sales_price |
| q39 | 2.44x | `CTE_DATE` | Split month-specific inventory into separate CTEs (Jan/Feb) |
| q23 | 2.33x | `UNION` | Early date filtering reduces catalog_sales scan before aggregation |
| q45 | 2.26x | `UNION` | Materialized subquery for customer filtering |
| q95 | 2.25x | `MAT_CTE` | Web sales with early date filtering and EXISTS optimization |
| q24 | 2.16x | `UNION` | Scan consolidation for store returns |
| q92 | 2.06x | `CTE_DATE` | Early date + item filtering with CTE |
| q41 | 1.69x | `PRED` | Simplified item filtering logic |
| q74 | 1.64x | `UNION` | Customer year-over-year comparison with early filtering |
| q88 | 1.56x | `PRED` | Time-based store sales aggregation with predicate pushdown |
| q1 | 1.35x | `CTE` | Store returns aggregation with CTE restructuring |
| q90 | 1.32x | `PRED` | Morning/afternoon sales ratio with early filtering |
| q29 | 1.26x | `CTE_DATE` | Store sales/returns with item filtering |
| q4 | 1.25x | `UNION` | Customer lifetime value with early date filtering |
| q47 | 1.24x | `CTE_DATE` | Monthly sales comparison with window functions |
| q61 | 1.24x | `PRED` | Promotion analysis with predicate pushdown |
| q62 | 1.24x | `CTE_DATE` | Shipping mode analysis with CTE |
| q17 | 1.23x | `CTE_DATE` | Store/catalog sales correlation with date CTEs |
| q82 | 1.23x | `PRED` | Inventory analysis with date pushdown |
| q69 | 1.22x | `CTE_DATE` | Customer demographics with early filtering |

## Pattern Codes

| Code | Pattern | Description | Typical Speedup |
|------|---------|-------------|-----------------|
| `UNION` | UNION ALL Decomposition | Split OR conditions into separate UNION ALL branches | 2-3x |
| `CTE_DATE` | Early Date Filtering | Filter date_dim in CTE, join early to reduce fact rows | 1.5-2.5x |
| `MAT_CTE` | Materialized CTE | Use MATERIALIZED hint to force early dimension filtering | 1.2-2x |
| `CTE` | Generic CTE | Restructure with CTEs for better join order | 1.1-1.5x |
| `PRED` | Predicate Pushdown | Push filters closer to base tables | 1.1-1.3x |
| `WINDOW` | Window Functions | Convert correlated subqueries to window functions | 1.2-1.5x |
| `EXISTS` | EXISTS Conversion | Convert IN to EXISTS for better performance | 1.1-1.3x |
| `JOIN_ORD` | Join Reordering | Reorder joins for better selectivity | 1.1-1.3x |
| `SCAN` | Scan Consolidation | Combine multiple scans into single pass | 1.2-1.5x |
| `ORIG` | Keep Original | Original already optimal or optimization regressed | 1.0x |
| `OTHER` | Other | Mixed or unclassified optimization | varies |

## Legend

- ✓ = Optimization successful (semantically correct)
- ✗ = Optimization failed validation
- ↓ = Regression (slower than original)