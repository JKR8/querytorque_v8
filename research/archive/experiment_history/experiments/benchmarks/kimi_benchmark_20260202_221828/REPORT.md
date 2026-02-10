# Kimi K2.5 TPC-DS Optimization Benchmark Report

**Date:** 2026-02-02
**Model:** moonshotai/kimi-k2.5 via OpenRouter
**Database:** TPC-DS SF100 (DuckDB)
**Mode:** DAG v2 Optimizer

---

## Executive Summary

| Metric | Value |
|--------|-------|
| Queries Tested | 99 |
| Sample DB Passed | 87/99 (87.9%) |
| Avg Sample Speedup | 1.04x |
| Full DB Validated | 47/47 (100%) |
| Avg Full DB Speedup | 1.17x |
| Top Speedup | 2.92x (Q1) |
| Improvements (>1x) | 47 |
| Regressions (<1x) | 36 |
| Failures | 12 |

---

## Full Results Table

| Query | Sample Status | Sample Speedup | Sample Time | Full DB Status | Full DB Speedup | Full DB Time |
|-------|---------------|----------------|-------------|----------------|-----------------|--------------|
| Q1 | ✓ pass | **2.92x** | 239→82ms | ✓ pass | **2.81x** | 241→86ms |
| Q2 | ✗ fail | 2.10x | 937→447ms | - | - | - |
| Q3 | ✓ pass | 0.98x | 296→303ms | - | - | - |
| Q4 | ✓ pass | 1.03x | 10.2→9.9s | ✓ pass | 1.00x | 10.5→10.5s |
| Q5 | ✓ pass | 1.09x | 1169→1074ms | ✓ pass | **1.20x** | 1563→1300ms |
| Q6 | ✓ pass | **1.33x** | 419→315ms | ✓ pass | **1.21x** | 334→276ms |
| Q7 | ✗ fail | 0.52x | 660→1281ms | - | - | - |
| Q8 | ✓ pass | 1.03x | 704→683ms | ✓ pass | 0.98x | 696→709ms |
| Q9 | ✓ pass | 0.42x | 2206→5249ms | - | - | - |
| Q10 | ✓ pass | 1.02x | 290→285ms | ✓ pass | **1.12x** | 362→323ms |
| Q11 | ✓ pass | 0.98x | 6017→6119ms | - | - | - |
| Q12 | ✓ pass | 1.01x | 110→109ms | ✓ pass | 0.95x | 99→104ms |
| Q13 | ✓ pass | 1.01x | 981→974ms | ✓ pass | **1.06x** | 1240→1167ms |
| Q14 | ✓ pass | 0.95x | 9211→9708ms | - | - | - |
| Q15 | ✓ pass | **2.78x** | 150→54ms | ✓ pass | **2.67x** | 142→53ms |
| Q16 | ✗ fail | 0.01x | 40→4520ms | - | - | - |
| Q17 | ✓ pass | 1.19x | 864→728ms | ✓ pass | 0.99x | 1059→1069ms |
| Q18 | ✓ pass | 1.14x | 424→371ms | ✓ pass | **1.11x** | 641→576ms |
| Q19 | ✓ pass | 1.04x | 389→376ms | ✓ pass | **1.05x** | 405→385ms |
| Q20 | ✓ pass | 1.07x | 72→67ms | ✓ pass | 0.94x | 73→78ms |
| Q21 | ✓ pass | 0.99x | 71→72ms | - | - | - |
| Q22 | ✓ pass | 0.98x | 7655→7801ms | - | - | - |
| Q23 | ✓ pass | 1.06x | 24.4→22.9s | ✓ pass | **1.08x** | 27.3→25.3s |
| Q24 | ✓ pass | 0.87x | 780→900ms | - | - | - |
| Q25 | ✓ pass | 0.98x | 515→524ms | - | - | - |
| Q26 | ✗ fail | 0.55x | 167→305ms | - | - | - |
| Q27 | ✓ pass | 1.01x | 703→697ms | ✓ pass | **1.23x** | 1031→835ms |
| Q28 | ✓ pass | **1.33x** | 3731→2814ms | ✓ pass | 1.00x | 2561→2565ms |
| Q29 | ✓ pass | 0.95x | 692→727ms | - | - | - |
| Q30 | ✗ error | - | - | - | - | - |
| Q31 | ✓ pass | 1.04x | 661→639ms | ✓ pass | 0.96x | 847→880ms |
| Q32 | ✓ pass | 0.27x | 17→66ms | - | - | - |
| Q33 | ✓ pass | 1.05x | 339→322ms | ✓ pass | 1.03x | 348→340ms |
| Q34 | ✓ pass | 0.29x | 540→1844ms | - | - | - |
| Q35 | ✗ fail | 1.51x | 1148→761ms | - | - | - |
| Q36 | ✓ pass | 0.96x | 897→933ms | - | - | - |
| Q37 | ✓ pass | 1.16x | 124→107ms | ✓ pass | 0.98x | 108→111ms |
| Q38 | ✓ pass | 0.99x | 1599→1617ms | - | - | - |
| Q39 | ✓ pass | 0.99x | 452→456ms | - | - | - |
| Q40 | ✓ pass | 1.07x | 252→236ms | ✓ pass | 1.01x | 254→250ms |
| Q41 | ✓ pass | 1.14x | 20→17ms | ✓ pass | 0.76x | 19→25ms |
| Q42 | ✓ pass | 0.94x | 218→232ms | - | - | - |
| Q43 | ✓ pass | 0.98x | 619→630ms | - | - | - |
| Q44 | ✗ error | - | - | - | - | - |
| Q45 | ✓ pass | 1.08x | 192→178ms | ✓ pass | 0.97x | 188→194ms |
| Q46 | ✓ pass | 1.02x | 860→845ms | ✓ pass | 1.03x | 885→858ms |
| Q47 | ✓ pass | 1.00x | 2706→2710ms | - | - | - |
| Q48 | ✓ pass | 1.00x | 934→938ms | - | - | - |
| Q49 | ✓ pass | 1.02x | 534→526ms | ✓ pass | **1.05x** | 551→523ms |
| Q50 | ✓ pass | 0.91x | 1008→1106ms | - | - | - |
| Q51 | ✗ fail | 1.51x | 7935→5247ms | - | - | - |
| Q52 | ✓ pass | 1.08x | 239→222ms | ✓ pass | 0.96x | 223→231ms |
| Q53 | ✓ pass | 0.51x | 356→691ms | - | - | - |
| Q54 | ✓ pass | 1.03x | 389→377ms | ✓ pass | 1.00x | 377→375ms |
| Q55 | ✓ pass | 0.94x | 228→243ms | - | - | - |
| Q56 | ✓ pass | 0.92x | 349→378ms | - | - | - |
| Q57 | ✓ pass | 1.02x | 1317→1290ms | ✓ pass | 0.98x | 1239→1266ms |
| Q58 | ✓ pass | 1.06x | 269→254ms | ✓ pass | **1.09x** | 287→264ms |
| Q59 | ✗ fail | 1.86x | 2873→1541ms | - | - | - |
| Q60 | ✓ pass | 1.02x | 378→371ms | ✓ pass | 0.93x | 384→411ms |
| Q61 | ✓ pass | 0.40x | 19→48ms | - | - | - |
| Q62 | ✓ pass | **1.23x** | 414→336ms | ✓ pass | 0.99x | 337→341ms |
| Q63 | ✓ pass | 1.03x | 387→377ms | ✓ pass | 0.91x | 376→412ms |
| Q64 | ✓ pass | 1.01x | 3841→3819ms | ✓ pass | 1.04x | 3118→3005ms |
| Q65 | ✗ fail | 1.83x | 3548→1934ms | - | - | - |
| Q66 | ✓ pass | **1.23x** | 445→361ms | ✓ pass | 0.86x | 372→432ms |
| Q67 | ✗ error | - | timeout | - | - | - |
| Q68 | ✓ pass | 0.95x | 890→939ms | - | - | - |
| Q69 | ✓ pass | 1.03x | 441→427ms | ✓ pass | 0.83x | 410→494ms |
| Q70 | ✓ pass | 0.75x | 1300→1742ms | - | - | - |
| Q71 | ✓ pass | 0.96x | 579→606ms | - | - | - |
| Q72 | ✓ pass | 0.97x | 1467→1506ms | - | - | - |
| Q73 | ✓ pass | 1.03x | 450→437ms | ✓ pass | **1.24x** | 561→454ms |
| Q74 | ✓ pass | **1.36x** | 4130→3028ms | ✓ pass | **1.42x** | 4373→3070ms |
| Q75 | ✓ pass | 0.94x | 2845→3021ms | - | - | - |
| Q76 | ✓ pass | 1.10x | 513→465ms | ✓ pass | 1.04x | 514→493ms |
| Q77 | ✓ pass | 1.01x | 421→415ms | ✓ pass | **1.05x** | 447→425ms |
| Q78 | ✓ pass | 1.01x | 9002→8934ms | ✓ pass | **1.21x** | 12.7→10.5s |
| Q79 | ✓ pass | 1.05x | 940→893ms | ✓ pass | **1.06x** | 954→903ms |
| Q80 | ✓ pass | 1.03x | 1553→1506ms | ✓ pass | **1.24x** | 2057→1662ms |
| Q81 | ✗ fail | 4.38x | 355→81ms | - | - | - |
| Q82 | ✓ pass | 0.97x | 265→272ms | - | - | - |
| Q83 | ✓ pass | **1.24x** | 76→61ms | ✓ pass | 1.05x | 68→65ms |
| Q84 | ✓ pass | **1.22x** | 80→66ms | ✓ pass | 0.94x | 76→81ms |
| Q85 | ✓ pass | 1.00x | 460→461ms | - | - | - |
| Q86 | ✓ pass | 0.92x | 236→255ms | - | - | - |
| Q87 | ✓ pass | 0.86x | 1822→2118ms | - | - | - |
| Q88 | ✓ pass | 0.99x | 2070→2099ms | - | - | - |
| Q89 | ✓ pass | 0.60x | 521→865ms | - | - | - |
| Q90 | ✓ pass | **1.57x** | 109→70ms | ✓ pass | **1.84x** | 165→90ms |
| Q91 | ✓ pass | 0.66x | 43→65ms | - | - | - |
| Q92 | ✓ pass | 0.95x | 96→101ms | - | - | - |
| Q93 | ✓ pass | **2.73x** | 2861→1047ms | ✓ pass | **2.71x** | 2961→1093ms |
| Q94 | ✓ pass | 0.08x | 141→1671ms | - | - | - |
| Q95 | ✓ pass | **1.37x** | 5151→3755ms | ✓ pass | **1.36x** | 5198→3817ms |
| Q96 | ✓ pass | 1.01x | 253→252ms | ✓ pass | 1.02x | 297→292ms |
| Q97 | ✓ pass | 0.98x | 2643→2689ms | - | - | - |
| Q98 | ✓ pass | 0.96x | 385→402ms | - | - | - |
| Q99 | ✓ pass | 1.00x | 464→462ms | - | - | - |

---

## Success Pattern Analysis

### Top Performers (>1.5x on Full DB)

| Query | Speedup | Transform | Key Optimization |
|-------|---------|-----------|------------------|
| Q1 | 2.81x | decorrelate | Converted correlated subquery to pre-computed CTE with GROUP BY |
| Q15 | 2.67x | or_to_union | Split OR condition into UNION ALL branches + early date filter |
| Q93 | 2.71x | early_filter | Pushed dimension filter before fact table join |
| Q90 | 1.84x | early_filter | Early filtering on reason dimension |
| Q74 | 1.42x | pushdown | Year filter pushed into CTE |
| Q80 | 1.24x | early_filter | Store returns filter optimization |
| Q73 | 1.24x | pushdown | Date range filter pushdown |
| Q27 | 1.23x | early_filter | State filter pushed to dimension |
| Q78 | 1.21x | projection_prune | Eliminated unnecessary intermediate columns |

### Winning Transform Patterns

#### 1. **Decorrelation** (Highest Impact: ~2.8x)
- Convert correlated subqueries to non-correlated CTEs
- Pre-compute aggregates that are reused
- Example: Q1 - `ctr_total_return > (SELECT AVG(...) FROM ... WHERE store = ctr.store)` → pre-computed `store_avg_return` CTE

#### 2. **OR to UNION ALL** (Impact: ~2.7x)
- Split OR conditions on different columns into UNION ALL branches
- Each branch can use different indexes/partitions
- Example: Q15 - `WHERE ca_zip IN (...) OR ca_state IN (...) OR cs_sales_price > 500`

#### 3. **Early Filter Pushdown** (Impact: 1.2-2.7x)
- Push dimension filters before fact table joins
- Reduce rows entering expensive joins/aggregations
- Example: Q93 - `r_reason_desc = 'duplicate purchase'` pushed to reason table first

#### 4. **Projection Pruning** (Impact: 1.1-1.2x)
- Remove unused columns from intermediate CTEs
- Reduces memory and I/O
- Example: Q78 - Eliminated unused columns from web_sales join

---

## Failure Analysis

### Semantic Failures (Value Mismatch)

| Query | Issue | Root Cause |
|-------|-------|------------|
| Q2 | Values differ | Changed aggregation semantics in wswscs CTE |
| Q7 | Values differ | Incorrect join elimination |
| Q16 | 0.01x regression | Major semantic error in rewrite |
| Q26 | Values differ | Filter pushdown changed result set |
| Q35 | Values differ | Incorrect UNION ALL decomposition |
| Q51 | Values differ | Aggregate computation changed |
| Q59 | Values differ | Join order changed semantics |
| Q65 | Values differ | CTE reference error |
| Q81 | 4.38x but fails | Likely removed necessary rows |

### Syntax/Binder Errors

| Query | Issue |
|-------|-------|
| Q30 | Binder Error - column reference |
| Q44 | Binder Error - column reference |
| Q67 | Timeout (300s) |

### Major Regressions (Passed but >2x slower)

| Query | Slowdown | Cause |
|-------|----------|-------|
| Q94 | 0.08x (12x slower) | Added expensive unnecessary join |
| Q32 | 0.27x | Extra CTE materialization overhead |
| Q34 | 0.29x | Redundant CTE creation |
| Q61 | 0.40x | Over-decomposed simple query |
| Q9 | 0.42x | Added unnecessary intermediate steps |

---

## Knowledge Base Recommendations

### Patterns to Add

1. **Self-Join Optimization** (Q2, Q39)
   - When CTE is self-joined with different filters, pre-filter in CTE
   - Pattern: `SELECT ... FROM cte c1, cte c2 WHERE c1.d_moy = 1 AND c2.d_moy = 2`
   - Fix: Add `d_moy IN (1,2)` to CTE definition

2. **HAVING with Correlated Aggregate** (Q35, Q51)
   - HAVING COUNT(*) > (SELECT AVG(cnt) FROM ...)
   - Pre-compute the threshold as a scalar CTE

3. **Multi-Table OR Conditions** (Q7, Q26)
   - OR conditions spanning joined tables need careful handling
   - May not be valid for UNION ALL if join conditions differ

4. **Recursive CTE Optimization** (Q59)
   - Date range recursion patterns
   - Rolling window calculations

### Patterns to Avoid

1. **Over-decomposition of Simple Queries**
   - Don't add CTEs to queries under 5 lines
   - CTE materialization has overhead

2. **Changing IN to EXISTS on Small Sets**
   - Only beneficial when subquery is large (>1000 rows)
   - Small IN lists are often faster

3. **Breaking Date Continuity**
   - Don't split continuous date ranges into UNION ALL
   - Loses partition pruning benefits

---

## Token Usage

| Phase | Tokens In | Tokens Out |
|-------|-----------|------------|
| Q1-30 Collection | 60,206 | 183,990 |
| Q31-99 Collection | 136,687 | 394,836 |
| **Total** | **196,893** | **578,826** |

Estimated cost: ~$0.50 (Kimi via OpenRouter)

---

## Next Steps

1. **MCTS Retry on Failures** - Run MCTS agent mode on the 12 failed queries to attempt recovery
2. **Regression Analysis** - Investigate Q94, Q32, Q34 to understand what went wrong
3. **Knowledge Base Updates** - Add self-join and HAVING patterns
4. **Cross-Model Validation** - Run same queries with DeepSeek V3 for comparison

---

## Files

```
research/experiments/benchmarks/kimi_benchmark_20260202_221828/
├── summary.json              # Sample DB results
├── full_db_validation.json   # Full DB validation results
├── REPORT.md                 # This report
└── q{1-99}/                  # Per-query validation details
    ├── original.sql
    ├── optimized.sql
    └── validation.json
```
