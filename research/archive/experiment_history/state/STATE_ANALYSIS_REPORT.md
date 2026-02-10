# STATE ANALYSIS REPORT: TPC-DS Optimization Strategy

**Generated**: 2026-02-06

**Scope**: 99 TPC-DS Queries

**Strategy**: Prioritize by RUNTIME (absolute time savings), not speedup %

# EXECUTIVE DASHBOARD

## Progress Summary

### DSR1 (DeepSeek Reasoner, state_0)
- **WIN**: 30 (≥1.1x)
- **IMPROVED**: 8 (1.05-1.1x)
- **NEUTRAL**: 21 (0.95-1.05x)
- **REGRESSION**: 26 (<0.95x)
- **ERROR**: 14 (parse/execution failures)
- **Avg speedup**: 1.06x
- Top: Q88 3.32x, Q35 2.42x, Q59 1.68x, Q41 1.63x, Q65 1.60x

### Prior Best (Kimi + V2 + 3W Retry)
- WIN: 13
- IMPROVED: 16
- NEUTRAL: 70

## Complete Query Leaderboard (All 99 Queries by Runtime)

| Rank | Query | Runtime | Best | Prior | DSR1 | Status | Savings @2x |
|------|-------|---------|------|-------|------|--------|-------------|
| 1 | Q9 | 2206ms | 4.47x | 4.47x | 1.28x | WIN | 1103ms ⭐ TOP 20 |
| 2 | Q81 | 355ms | 4.38x | 4.38x | 1.20x | WIN | 177ms |
| 3 | Q63 | 387ms | 3.77x | 3.77x | 1.00x | WIN | 193ms |
| 4 | Q88 | 2069ms | 3.32x | 1.00x | 3.32x | WIN | 1034ms ⭐ TOP 20 |
| 5 | Q1 | 239ms | 2.92x | 2.92x | 0.71x | WIN | 119ms |
| 6 | Q15 | 150ms | 2.78x | 2.78x | 1.09x | WIN | 75ms |
| 7 | Q93 | 2860ms | 2.73x | 2.73x | 0.34x | WIN | 1430ms ⭐ TOP 20 |
| 8 | Q43 | 618ms | 2.71x | 2.71x | 1.10x | WIN | 309ms |
| 9 | Q35 | 1147ms | 2.42x | 1.51x | 2.42x | WIN | 573ms |
| 10 | Q29 | 692ms | 2.35x | 2.35x | 1.00x | WIN | 346ms |
| 11 | Q2 | 937ms | 2.10x | 2.10x | - | WIN | 468ms |
| 12 | Q26 | 166ms | 1.93x | 1.93x | 1.01x | WIN | 83ms |
| 13 | Q5 | 1169ms | 1.89x | 1.89x | 0.96x | WIN | 584ms |
| 14 | Q59 | 2873ms | 1.86x | 1.86x | 1.68x | WIN | 1436ms ⭐ TOP 20 |
| 15 | Q65 | 3547ms | 1.83x | 1.83x | 1.60x | WIN | 1773ms ⭐ TOP 20 |
| 16 | Q22 | 7654ms | 1.69x | 1.69x | 1.10x | WIN | 3827ms ⭐ TOP 20 |
| 17 | Q96 | 253ms | 1.64x | 1.64x | 0.98x | WIN | 126ms |
| 18 | Q41 | 19ms | 1.63x | 1.14x | 1.63x | WIN | 9ms |
| 19 | Q27 | 702ms | 1.58x | 1.01x | 1.58x | WIN | 351ms |
| 20 | Q73 | 450ms | 1.57x | 1.57x | 0.87x | WIN | 225ms |
| 21 | Q90 | 109ms | 1.57x | 1.57x | 0.59x | WIN | 54ms |
| 22 | Q51 | 7935ms | 1.51x | 1.51x | 0.87x | WIN | 3967ms ⭐ TOP 20 |
| 23 | Q61 | 19ms | 1.46x | 1.00x | 1.46x | WIN | 9ms |
| 24 | Q38 | 1598ms | 1.44x | 1.44x | 1.00x | WIN | 799ms |
| 25 | Q14 | 9210ms | 1.40x | 1.00x | 1.40x | WIN | 4605ms ⭐ TOP 20 |
| 26 | Q95 | 5151ms | 1.37x | 1.37x | 0.54x | WIN | 2575ms ⭐ TOP 20 |
| 27 | Q44 | 0ms | 1.37x | 1.00x | 1.37x | WIN | 0ms |
| 28 | Q74 | 4129ms | 1.36x | 1.36x | 0.68x | WIN | 2064ms ⭐ TOP 20 |
| 29 | Q6 | 418ms | 1.33x | 1.33x | 0.85x | WIN | 209ms |
| 30 | Q28 | 3730ms | 1.33x | 1.33x | 0.92x | WIN | 1865ms ⭐ TOP 20 |
| 31 | Q37 | 124ms | 1.30x | 1.30x | 1.10x | WIN | 62ms |
| 32 | Q80 | 1553ms | 1.30x | 1.03x | 1.30x | WIN | 776ms |
| 33 | Q12 | 110ms | 1.27x | 1.23x | 1.27x | WIN | 55ms |
| 34 | Q83 | 75ms | 1.24x | 1.24x | 1.16x | WIN | 37ms |
| 35 | Q46 | 859ms | 1.23x | 1.02x | 1.23x | WIN | 429ms |
| 36 | Q62 | 413ms | 1.23x | 1.23x | 1.00x | WIN | 206ms |
| 37 | Q66 | 444ms | 1.23x | 1.23x | 1.21x | WIN | 222ms |
| 38 | Q84 | 80ms | 1.22x | 1.22x | 1.10x | WIN | 40ms |
| 39 | Q57 | 1316ms | 1.20x | 1.02x | 1.20x | WIN | 658ms |
| 40 | Q45 | 192ms | 1.19x | 1.08x | 1.19x | WIN | 96ms |
| 41 | Q17 | 864ms | 1.19x | 1.19x | 0.90x | WIN | 432ms |
| 42 | Q82 | 264ms | 1.18x | 1.18x | - | WIN | 132ms |
| 43 | Q8 | 704ms | 1.16x | 1.03x | 1.16x | WIN | 352ms |
| 44 | Q56 | 349ms | 1.16x | 1.00x | 1.16x | WIN | 174ms |
| 45 | Q70 | 1299ms | 1.15x | 1.00x | 1.15x | WIN | 649ms |
| 46 | Q40 | 251ms | 1.15x | 1.07x | 1.15x | WIN | 125ms |
| 47 | Q30 | 0ms | 1.15x | 1.00x | 1.15x | WIN | 0ms |
| 48 | Q18 | 424ms | 1.14x | 1.14x | - | WIN | 212ms |
| 49 | Q69 | 441ms | 1.13x | 1.03x | 1.13x | WIN | 220ms |
| 50 | Q53 | 355ms | 1.12x | 1.00x | 1.12x | WIN | 177ms |
| 51 | Q4 | 10208ms | 1.12x | 1.03x | 1.12x | WIN | 5104ms ⭐ TOP 20 |
| 52 | Q99 | 464ms | 1.11x | 1.00x | 1.11x | WIN | 232ms |
| 53 | Q50 | 1007ms | 1.11x | 1.00x | 1.11x | WIN | 503ms |
| 54 | Q76 | 513ms | 1.10x | 1.10x | - | WIN | 256ms |
| 55 | Q33 | 338ms | 1.08x | 1.05x | 1.08x | IMPROVED | 169ms |
| 56 | Q78 | 9002ms | 1.08x | 1.01x | 1.08x | IMPROVED | 4501ms ⭐ TOP 20 |
| 57 | Q52 | 238ms | 1.08x | 1.08x | - | IMPROVED | 119ms |
| 58 | Q34 | 539ms | 1.08x | 1.00x | 1.08x | IMPROVED | 269ms |
| 59 | Q20 | 71ms | 1.07x | 1.07x | 1.01x | IMPROVED | 35ms |
| 60 | Q11 | 6017ms | 1.06x | 1.00x | 1.06x | IMPROVED | 3008ms ⭐ TOP 20 |
| 61 | Q23 | 24403ms | 1.06x | 1.06x | 1.02x | IMPROVED | 12201ms ⭐ TOP 20 |
| 62 | Q58 | 269ms | 1.06x | 1.06x | 0.78x | IMPROVED | 134ms |
| 63 | Q7 | 659ms | 1.05x | 1.00x | 1.05x | IMPROVED | 329ms |
| 64 | Q79 | 939ms | 1.05x | 1.05x | 0.98x | IMPROVED | 469ms |
| 65 | Q39 | 452ms | 1.05x | 1.00x | 1.05x | NEUTRAL | 226ms |
| 66 | Q3 | 296ms | 1.04x | 1.00x | 1.04x | NEUTRAL | 148ms |
| 67 | Q19 | 389ms | 1.04x | 1.04x | 0.99x | NEUTRAL | 194ms |
| 68 | Q31 | 661ms | 1.04x | 1.04x | 0.49x | NEUTRAL | 330ms |
| 69 | Q54 | 389ms | 1.03x | 1.03x | - | NEUTRAL | 194ms |
| 70 | Q91 | 43ms | 1.03x | 1.00x | 1.03x | NEUTRAL | 21ms |
| 71 | Q55 | 227ms | 1.03x | 1.00x | 1.03x | NEUTRAL | 113ms |
| 72 | Q10 | 290ms | 1.02x | 1.02x | 0.95x | NEUTRAL | 145ms |
| 73 | Q49 | 534ms | 1.02x | 1.02x | 0.98x | NEUTRAL | 267ms |
| 74 | Q60 | 378ms | 1.02x | 1.02x | - | NEUTRAL | 189ms |
| 75 | Q68 | 889ms | 1.02x | 1.00x | 1.02x | NEUTRAL | 444ms |
| 76 | Q13 | 980ms | 1.01x | 1.01x | - | NEUTRAL | 490ms |
| 77 | Q64 | 3841ms | 1.01x | 1.01x | - | NEUTRAL | 1920ms ⭐ TOP 20 |
| 78 | Q77 | 420ms | 1.01x | 1.01x | 0.99x | NEUTRAL | 210ms |
| 79 | Q16 | 40ms | 1.00x | 1.00x | 0.14x | NEUTRAL | 20ms |
| 80 | Q21 | 71ms | 1.00x | 1.00x | - | NEUTRAL | 35ms |
| 81 | Q24 | 780ms | 1.00x | 1.00x | - | NEUTRAL | 390ms |
| 82 | Q25 | 515ms | 1.00x | 1.00x | 0.50x | NEUTRAL | 257ms |
| 83 | Q32 | 17ms | 1.00x | 1.00x | 0.82x | NEUTRAL | 8ms |
| 84 | Q36 | 896ms | 1.00x | 1.00x | 0.91x | NEUTRAL | 448ms |
| 85 | Q42 | 217ms | 1.00x | 1.00x | 1.00x | NEUTRAL | 108ms |
| 86 | Q47 | 2705ms | 1.00x | 1.00x | 0.91x | NEUTRAL | 1352ms ⭐ TOP 20 |
| 87 | Q48 | 933ms | 1.00x | 1.00x | 0.90x | NEUTRAL | 466ms |
| 88 | Q67 | 0ms | 1.00x | 1.00x | 0.85x | NEUTRAL | 0ms |
| 89 | Q71 | 579ms | 1.00x | 1.00x | 0.89x | NEUTRAL | 289ms |
| 90 | Q72 | 1467ms | 1.00x | 1.00x | 0.77x | NEUTRAL | 733ms |
| 91 | Q75 | 2844ms | 1.00x | 1.00x | 0.97x | NEUTRAL | 1422ms ⭐ TOP 20 |
| 92 | Q85 | 459ms | 1.00x | 1.00x | 0.95x | NEUTRAL | 229ms |
| 93 | Q86 | 235ms | 1.00x | 1.00x | 0.98x | NEUTRAL | 117ms |
| 94 | Q87 | 1821ms | 1.00x | 1.00x | 0.97x | NEUTRAL | 910ms ⭐ TOP 20 |
| 95 | Q89 | 521ms | 1.00x | 1.00x | 0.94x | NEUTRAL | 260ms |
| 96 | Q92 | 95ms | 1.00x | 1.00x | 0.92x | NEUTRAL | 47ms |
| 97 | Q94 | 141ms | 1.00x | 1.00x | - | NEUTRAL | 70ms |
| 98 | Q97 | 2642ms | 1.00x | 1.00x | 0.90x | NEUTRAL | 1321ms ⭐ TOP 20 |
| 99 | Q98 | 384ms | 1.00x | 1.00x | 0.97x | NEUTRAL | 192ms |

## Transform Effectiveness

- prefetch_fact_join: 100% success (1/1), 3.77x avg
- union_cte_split: 100% success (1/1), 1.36x avg
- date_cte_isolate: 0% success (0/40), 4.00x avg
- decorrelate: 0% success (0/3), 2.92x avg
- dimension_cte_isolate: 0% success (0/2), 1.93x avg
- early_filter: 0% success (0/7), 4.00x avg
- intersect_to_exists: 0% success (0/1), 1.83x avg
- materialize_cte: 0% success (0/9), 1.37x avg
- multi_date_range_cte: 0% success (0/2), 2.35x avg
- multi_dimension_prefetch: 0% success (0/3), 2.71x avg


# TIER 1: HIGH-VALUE TARGETS (Priority > 70)


**Priority Score**: 95.0 (TOP_20%)

### Q88: Q88
**Classification**: NEUTRAL
**Runtime**: 2069.9ms baseline (TOP_20%)
**Time Savings Potential**: 1035ms at 2x, 1380ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 5.00x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.99x [none] neutral
- v2_standard: 1.00x [materialize_cte] success

**Transforms Tried** (learning record):
  No effect: materialize_cte

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **single_pass_aggregation** [CONFIDENCE: 65%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 4.47x on benchmark queries
   - Success rate: 0%

2. **or_to_union** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has or_condition structure
   - Verified: 3.17x on benchmark queries
   - Success rate: 0%

3. **pushdown** [CONFIDENCE: 31%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 2.11x on benchmark queries
   - Success rate: 0%

**Priority Score**: 87.5 (TOP_20%)

### Q59: Q59
**Classification**: NEUTRAL
**Runtime**: 2873.2ms baseline (TOP_20%)
**Time Savings Potential**: 1437ms at 2x, 1915ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 86.0 (TOP_20%)

### Q65: Q65
**Classification**: NEUTRAL
**Runtime**: 3547.9ms baseline (TOP_20%)
**Time Savings Potential**: 1774ms at 2x, 2365ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 85.0 (TOP_20%)

### Q97: Q97
**Classification**: NEUTRAL
**Runtime**: 2642.7ms baseline (TOP_20%)
**Time Savings Potential**: 1321ms at 2x, 1762ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.00x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.98x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **union_cte_split** [CONFIDENCE: 84%] [RISK: LOW]
   - Why: Query has union_year structure
   - Verified: 1.36x on benchmark queries
   - Success rate: 100%

**Priority Score**: 83.4 (TOP_20%)

### Q64: Q64
**Classification**: NEUTRAL
**Runtime**: 3841.1ms baseline (TOP_20%)
**Time Savings Potential**: 1921ms at 2x, 2561ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.70x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.01x [none] neutral
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **multi_date_range_cte** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has multi_date_alias structure
   - Verified: 2.35x on benchmark queries
   - Success rate: 0%

**Priority Score**: 82.5 (TOP_20%)

### Q47: Q47
**Classification**: NEUTRAL
**Runtime**: 2705.8ms baseline (TOP_20%)
**Time Savings Potential**: 1353ms at 2x, 1804ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.00x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 82.5 (TOP_20%)

### Q75: Q75
**Classification**: NEUTRAL
**Runtime**: 2844.6ms baseline (TOP_20%)
**Time Savings Potential**: 1422ms at 2x, 1896ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.94x [none] regression
- v2_standard: 1.00x [pushdown] success
- retry3w_2: 0.67x [none] regression

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **union_cte_split** [CONFIDENCE: 84%] [RISK: LOW]
   - Why: Query has union_year structure
   - Verified: 1.36x on benchmark queries
   - Success rate: 100%

3. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 82.3 (TOP_20%)

### Q78: Q78
**Classification**: NEUTRAL
**Runtime**: 9002.1ms baseline (TOP_20%)
**Time Savings Potential**: 4501ms at 2x, 6001ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.49x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.01x [none] neutral
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 81.7 (TOP_20%)

### Q28: Q28
**Classification**: IMPROVED
**Runtime**: 3730.7ms baseline (TOP_20%)
**Time Savings Potential**: 1865ms at 2x, 2487ms at 3x
**Current Best**: 1.33x (baseline)
**Gap to Expectation**: 4.47x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.33x [none] success
- v2_standard: 1.00x [semantic_rewrite] success

**Transforms Tried** (learning record):
  No effect: semantic_rewrite

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **single_pass_aggregation** [CONFIDENCE: 65%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 4.47x on benchmark queries
   - Success rate: 0%

2. **pushdown** [CONFIDENCE: 31%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 2.11x on benchmark queries
   - Success rate: 0%

**Priority Score**: 77.5 (TOP_20%)

### Q51: Q51
**Classification**: NEUTRAL
**Runtime**: 7935.1ms baseline (TOP_20%)
**Time Savings Potential**: 3968ms at 2x, 5290ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 0.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

**Priority Score**: 76.6 (TOP_20%)

### Q23: Q23
**Classification**: NEUTRAL
**Runtime**: 24403.6ms baseline (TOP_20%)
**Time Savings Potential**: 12202ms at 2x, 16269ms at 3x
**Current Best**: 1.06x (baseline)
**Gap to Expectation**: 0.44x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.06x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

**Priority Score**: 75.8 (TOP_20%)

### Q14: Q14
**Classification**: NEUTRAL
**Runtime**: 9210.5ms baseline (TOP_20%)
**Time Savings Potential**: 4605ms at 2x, 6140ms at 3x
**Current Best**: 0.95x (baseline)
**Gap to Expectation**: 0.05x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.95x [none] neutral

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **intersect_to_exists** [CONFIDENCE: 43%] [RISK: HIGH]
   - Why: Query has intersect structure
   - Verified: 1.83x on benchmark queries
   - Success rate: 0%

**Priority Score**: 75.3 (TOP_20%)

### Q11: Q11
**Classification**: NEUTRAL
**Runtime**: 6017.1ms baseline (TOP_20%)
**Time Savings Potential**: 3009ms at 2x, 4011ms at 3x
**Current Best**: 0.98x (baseline)
**Gap to Expectation**: 0.02x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.98x [none] neutral

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has correlated_subquery, date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

**Priority Score**: 74.7 (TOP_20%)

### Q4: Q4
**Classification**: NEUTRAL
**Runtime**: 10208.8ms baseline (TOP_20%)
**Time Savings Potential**: 5104ms at 2x, 6806ms at 3x
**Current Best**: 1.03x (W3)
**Gap to Expectation**: 0.03x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.03x [none] neutral
- retry3w_3: 0.35x [none] regression

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

2. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

3. **multi_date_range_cte** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has multi_date_alias structure
   - Verified: 2.35x on benchmark queries
   - Success rate: 0%

**Priority Score**: 71.9 (TOP_20%)

### Q95: Q95
**Classification**: IMPROVED
**Runtime**: 5151.0ms baseline (TOP_20%)
**Time Savings Potential**: 2576ms at 2x, 3434ms at 3x
**Current Best**: 1.37x (baseline)
**Gap to Expectation**: 2.13x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.37x [none] success
- v2_standard: 1.00x [semantic_rewrite] success

**Transforms Tried** (learning record):
  No effect: semantic_rewrite

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **materialize_cte** [CONFIDENCE: 24%] [RISK: HIGH]
   - Why: Query has exists_repeat structure
   - Verified: 1.37x on benchmark queries
   - Success rate: 0%


# TIER 2: INCREMENTAL OPPORTUNITIES (Priority 40-70)


**Priority Score**: 67.1 (TOP_20%)

### Q74: Q74
**Classification**: IMPROVED
**Runtime**: 4129.7ms baseline (TOP_20%)
**Time Savings Potential**: 2065ms at 2x, 2753ms at 3x
**Current Best**: 1.36x (baseline)
**Gap to Expectation**: 1.14x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.36x [pushdown] success
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  Worked: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **union_cte_split** [CONFIDENCE: 84%] [RISK: LOW]
   - Why: Query has union_year structure
   - Verified: 1.36x on benchmark queries
   - Success rate: 100%

3. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 62.5 (TOP_50%)

### Q36: Q36
**Classification**: NEUTRAL
**Runtime**: 896.6ms baseline (TOP_50%)
**Time Savings Potential**: 448ms at 2x, 598ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.96x [none] neutral
- v2_standard: 1.00x [multi_push_predicate] success

**Transforms Tried** (learning record):
  No effect: multi_push_predicate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 62.5 (TOP_50%)

### Q72: Q72
**Classification**: NEUTRAL
**Runtime**: 1467.3ms baseline (TOP_50%)
**Time Savings Potential**: 734ms at 2x, 978ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.97x [none] neutral
- v2_standard: 1.00x [semantic_rewrite] success

**Transforms Tried** (learning record):
  No effect: semantic_rewrite

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

2. **multi_date_range_cte** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has multi_date_alias structure
   - Verified: 2.35x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 62.5 (TOP_50%)

### Q7: Q7
**Classification**: NEUTRAL
**Runtime**: 660.0ms baseline (TOP_50%)
**Time Savings Potential**: 330ms at 2x, 440ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 2.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 62.4 (TOP_50%)

### Q27: Q27
**Classification**: NEUTRAL
**Runtime**: 703.0ms baseline (TOP_50%)
**Time Savings Potential**: 351ms at 2x, 469ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 2.49x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.01x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 61.9 (TOP_50%)

### Q31: Q31
**Classification**: NEUTRAL
**Runtime**: 661.3ms baseline (TOP_50%)
**Time Savings Potential**: 331ms at 2x, 441ms at 3x
**Current Best**: 1.04x (baseline)
**Gap to Expectation**: 2.46x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.04x [none] neutral
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 61.5 (TOP_20%)

### Q22: Q22
**Classification**: WIN
**Runtime**: 7654.7ms baseline (TOP_20%)
**Time Savings Potential**: 3827ms at 2x, 5103ms at 3x
**Current Best**: 1.69x (W2)
**Gap to Expectation**: 1.31x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.98x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success
- retry3w_2: 1.69x [none] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

**Priority Score**: 61.0 (TOP_50%)

### Q87: Q87
**Classification**: NEUTRAL
**Runtime**: 1821.9ms baseline (TOP_50%)
**Time Savings Potential**: 911ms at 2x, 1215ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.86x [none] regression
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 61.0 (TOP_50%)

### Q89: Q89
**Classification**: NEUTRAL
**Runtime**: 521.0ms baseline (TOP_50%)
**Time Savings Potential**: 261ms at 2x, 347ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 2.20x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.60x [none] regression
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 60.0 (TOP_50%)

### Q48: Q48
**Classification**: NEUTRAL
**Runtime**: 933.9ms baseline (TOP_50%)
**Time Savings Potential**: 467ms at 2x, 623ms at 3x
**Current Best**: 1.00x (W1)
**Gap to Expectation**: 2.00x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.00x [none] neutral
- v2_standard: 1.00x [or_to_union] success
- retry3w_1: 0.24x [none] regression

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

2. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

3. **dimension_cte_isolate** [CONFIDENCE: 39%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 1.93x on benchmark queries
   - Success rate: 0%

**Priority Score**: 59.8 (TOP_50%)

### Q13: Q13
**Classification**: NEUTRAL
**Runtime**: 980.7ms baseline (TOP_50%)
**Time Savings Potential**: 490ms at 2x, 654ms at 3x
**Current Best**: 1.01x (baseline)
**Gap to Expectation**: 1.97x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.01x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 59.5 (TOP_50%)

### Q80: Q80
**Classification**: NEUTRAL
**Runtime**: 1553.3ms baseline (TOP_50%)
**Time Savings Potential**: 777ms at 2x, 1036ms at 3x
**Current Best**: 1.03x (baseline)
**Gap to Expectation**: 1.97x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.03x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **union_cte_split** [CONFIDENCE: 84%] [RISK: LOW]
   - Why: Query has union_year structure
   - Verified: 1.36x on benchmark queries
   - Success rate: 100%

3. **multi_date_range_cte** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has multi_date_alias structure
   - Verified: 2.35x on benchmark queries
   - Success rate: 0%

**Priority Score**: 58.5 (TOP_50%)

### Q24: Q24
**Classification**: NEUTRAL
**Runtime**: 780.3ms baseline (TOP_50%)
**Time Savings Potential**: 390ms at 2x, 520ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.70x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.87x [none] regression
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

2. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q25: Q25
**Classification**: NEUTRAL
**Runtime**: 515.4ms baseline (TOP_50%)
**Time Savings Potential**: 258ms at 2x, 344ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.98x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success
- retry3w_2: 0.69x [none] regression

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_date_range_cte** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has multi_date_alias structure
   - Verified: 2.35x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q2: Q2
**Classification**: NEUTRAL
**Runtime**: 937.0ms baseline (TOP_50%)
**Time Savings Potential**: 469ms at 2x, 625ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [pushdown] success

**Transforms Tried** (learning record):
  No effect: pushdown

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q34: Q34
**Classification**: NEUTRAL
**Runtime**: 539.9ms baseline (TOP_50%)
**Time Savings Potential**: 270ms at 2x, 360ms at 3x
**Current Best**: 1.00x (W2)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.29x [none] regression
- v2_standard: 1.00x [or_to_union] success
- retry3w_2: 0.80x [none] regression

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q35: Q35
**Classification**: NEUTRAL
**Runtime**: 1147.5ms baseline (TOP_50%)
**Time Savings Potential**: 574ms at 2x, 765ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q50: Q50
**Classification**: NEUTRAL
**Runtime**: 1007.8ms baseline (TOP_50%)
**Time Savings Potential**: 504ms at 2x, 672ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.91x [none] regression
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q68: Q68
**Classification**: NEUTRAL
**Runtime**: 889.7ms baseline (TOP_50%)
**Time Savings Potential**: 445ms at 2x, 593ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.95x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.5 (TOP_50%)

### Q71: Q71
**Classification**: NEUTRAL
**Runtime**: 579.4ms baseline (TOP_50%)
**Time Savings Potential**: 290ms at 2x, 386ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.50x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.96x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.2 (TOP_50%)

### Q46: Q46
**Classification**: NEUTRAL
**Runtime**: 859.9ms baseline (TOP_50%)
**Time Savings Potential**: 430ms at 2x, 573ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.02x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 57.2 (TOP_50%)

### Q49: Q49
**Classification**: NEUTRAL
**Runtime**: 534.3ms baseline (TOP_50%)
**Time Savings Potential**: 267ms at 2x, 356ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.02x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **union_cte_split** [CONFIDENCE: 84%] [RISK: LOW]
   - Why: Query has union_year structure
   - Verified: 1.36x on benchmark queries
   - Success rate: 100%

**Priority Score**: 57.2 (TOP_50%)

### Q57: Q57
**Classification**: NEUTRAL
**Runtime**: 1316.8ms baseline (TOP_50%)
**Time Savings Potential**: 658ms at 2x, 878ms at 3x
**Current Best**: 1.02x (baseline)
**Gap to Expectation**: 1.48x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.02x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 56.8 (TOP_50%)

### Q79: Q79
**Classification**: NEUTRAL
**Runtime**: 939.8ms baseline (TOP_50%)
**Time Savings Potential**: 470ms at 2x, 626ms at 3x
**Current Best**: 1.05x (baseline)
**Gap to Expectation**: 1.45x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.05x [none] neutral
- v2_standard: 1.00x [or_to_union] success

**Transforms Tried** (learning record):
  No effect: or_to_union

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has multi_dim_filter, dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 56.4 (TOP_20%)

### Q93: Q93
**Classification**: WIN
**Runtime**: 2860.6ms baseline (TOP_20%)
**Time Savings Potential**: 1430ms at 2x, 1907ms at 3x
**Current Best**: 2.73x (baseline)
**Gap to Expectation**: 0.27x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 2.73x [early_filter] success
- v2_standard: 1.00x [decorrelate] success

**Transforms Tried** (learning record):
  Worked: early_filter
  No effect: decorrelate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - dimension_cte_isolate
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 55.0 (TOP_50%)

### Q70: Q70
**Classification**: NEUTRAL
**Runtime**: 1299.7ms baseline (TOP_50%)
**Time Savings Potential**: 650ms at 2x, 866ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 1.00x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.75x [none] regression
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

**Priority Score**: 55.0 (TOP_20%)

### Q9: Q9
**Classification**: WIN
**Runtime**: 2206.2ms baseline (TOP_20%)
**Time Savings Potential**: 1103ms at 2x, 1471ms at 3x
**Current Best**: 4.47x (W2)
**Gap to Expectation**: 3.47x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.42x [none] regression
- retry3w_2: 4.47x [none] success

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **single_pass_aggregation** [CONFIDENCE: 65%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 4.47x on benchmark queries
   - Success rate: 0%

2. **pushdown** [CONFIDENCE: 31%] [RISK: HIGH]
   - Why: Query has repeated_scan structure
   - Verified: 2.11x on benchmark queries
   - Success rate: 0%

**Priority Score**: 49.7 (TOP_50%)

### Q8: Q8
**Classification**: NEUTRAL
**Runtime**: 704.3ms baseline (TOP_50%)
**Time Savings Potential**: 352ms at 2x, 470ms at 3x
**Current Best**: 1.03x (baseline)
**Gap to Expectation**: 0.03x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 1.03x [none] neutral

**Gold Patterns NOT Tried** (candidates for next attempt):
  - date_cte_isolate
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **date_cte_isolate** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has correlated_subquery, date_filter structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **decorrelate** [CONFIDENCE: 44%] [RISK: HIGH]
   - Why: Query has correlated_subquery structure
   - Verified: 2.92x on benchmark queries
   - Success rate: 0%

**Priority Score**: 45.0 (BOTTOM_50%)

### Q98: Q98
**Classification**: NEUTRAL
**Runtime**: 385.0ms baseline (BOTTOM_50%)
**Time Savings Potential**: 192ms at 2x, 257ms at 3x
**Current Best**: 1.00x (baseline)
**Gap to Expectation**: 7.00x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.96x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter, dim_fact_chain structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **early_filter** [CONFIDENCE: 50%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 4.00x on benchmark queries
   - Success rate: 0%

3. **multi_dimension_prefetch** [CONFIDENCE: 42%] [RISK: HIGH]
   - Why: Query has dim_fact_chain structure
   - Verified: 2.71x on benchmark queries
   - Success rate: 0%

**Priority Score**: 40.9 (TOP_50%)

### Q38: Q38
**Classification**: IMPROVED
**Runtime**: 1598.8ms baseline (TOP_50%)
**Time Savings Potential**: 799ms at 2x, 1066ms at 3x
**Current Best**: 1.44x (W2)
**Gap to Expectation**: 1.06x

**Attempt History** (State 0 = baseline, current best = next starting point):
- baseline: 1.00x [none] success
- kimi: 0.99x [none] neutral
- v2_standard: 1.00x [date_cte_isolate] success
- retry3w_2: 1.44x [none] success

**Transforms Tried** (learning record):
  No effect: date_cte_isolate

**Gold Patterns NOT Tried** (candidates for next attempt):
  - decorrelate
  - dimension_cte_isolate
  - early_filter
  - intersect_to_exists
  - materialize_cte
  - multi_date_range_cte
  - multi_dimension_prefetch
  - or_to_union
  - prefetch_fact_join
  - pushdown
  - single_pass_aggregation
  - union_cte_split

**Recommended Next Moves**:

1. **prefetch_fact_join** [CONFIDENCE: 100%] [RISK: LOW]
   - Why: Query has date_filter structure
   - Verified: 3.77x on benchmark queries
   - Success rate: 100%

2. **intersect_to_exists** [CONFIDENCE: 43%] [RISK: HIGH]
   - Why: Query has intersect structure
   - Verified: 1.83x on benchmark queries
   - Success rate: 0%


# TIER 3: MATURE WINS (Priority < 40)


**54 queries with low priority** (mostly short-running or already optimized)

These queries are not recommended for immediate focus due to:
- Short baseline runtime (<500ms) → lower absolute time savings potential
- Already at or near expected speedup targets
- Limited remaining optimization opportunities



# APPENDIX: METHODOLOGY & INTERPRETATION GUIDE


## Priority Scoring Formula

`Priority = Runtime_Percentile(50pts) + Gap_To_Expectation(20pts) + Win_Potential(20pts) + Untried_Patterns(5pts) + Category_Bonus(15pts)`


### Runtime Percentile (50 points - DOMINANT FACTOR)

- **Top 20% by baseline runtime**: 50 points
- **Top 21-50% by baseline runtime**: 25 points
- **Bottom 50% by baseline runtime**: 0 points

**Key insight**: A 1.2x speedup on a 10,000ms query saves more absolute time than 3x speedup on a 100ms query.


### Time Savings Potential

Shown for each query:
- **At 2x speedup**: Original_ms / 2 seconds saved
- **At 3x speedup**: Original_ms * 2 / 3 seconds saved


### Confidence Scores

- **90-100%**: Very high confidence - proven pattern with high success rate
- **75-89%**: High confidence - successful pattern, likely to work
- **60-74%**: Good confidence - proven technique, moderate risk
- **40-59%**: Moderate confidence - less evidence but promising
- **<40%**: Low confidence - experimental, use as last resort


### Risk Assessment

- **LOW**: >80% historical success rate
- **MEDIUM**: 50-80% success rate
- **HIGH**: <50% success rate or untested pattern


## How to Use This Report

1. **Start with Tier 1** (highest priority scores) - these are longest-running queries with proven patterns
2. **Check Time Savings Potential** - focus on queries where potential savings are largest
3. **Review Top Recommendations** - follow highest-confidence transforms first
4. **Validate improvements** using 3-run (discard warmup, avg last 2) or 5-run trimmed mean methodology
5. **Move to Tier 2** only after exhausting high-value targets in Tier 1
