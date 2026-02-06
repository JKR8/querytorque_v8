# STATE ANALYSIS REPORT: TPC-DS Optimization Strategy

**Generated**: 2026-02-06

**Scope**: 99 TPC-DS Queries

**Strategy**: Prioritize by RUNTIME (absolute time savings), not speedup %

# EXECUTIVE DASHBOARD

## Progress Summary

- WIN: 13
- IMPROVED: 16
- NEUTRAL: 70
- REGRESSION: 0
- ERROR: 0
- GOLD_EXAMPLE: 0

## Complete Query Leaderboard (All 99 Queries by Runtime)

| Rank | Query | Runtime | Speedup | Status | Savings @2x |
|------|-------|---------|---------|--------|-------------|
| 1 | Q23 | 24404ms | 1.06x | NEUTRAL | 12202ms ⭐ TOP 20 |
| 2 | Q4 | 10209ms | 1.03x | NEUTRAL | 5104ms ⭐ TOP 20 |
| 3 | Q14 | 9211ms | 0.95x | NEUTRAL | 4605ms ⭐ TOP 20 |
| 4 | Q78 | 9002ms | 1.01x | NEUTRAL | 4501ms ⭐ TOP 20 |
| 5 | Q51 | 7935ms | 1.00x | NEUTRAL | 3968ms ⭐ TOP 20 |
| 6 | Q22 | 7655ms | 1.69x | WIN | 3827ms ⭐ TOP 20 |
| 7 | Q11 | 6017ms | 0.98x | NEUTRAL | 3009ms ⭐ TOP 20 |
| 8 | Q95 | 5151ms | 1.37x | IMPROVED | 2576ms ⭐ TOP 20 |
| 9 | Q74 | 4130ms | 1.36x | IMPROVED | 2065ms ⭐ TOP 20 |
| 10 | Q64 | 3841ms | 1.01x | NEUTRAL | 1921ms ⭐ TOP 20 |
| 11 | Q28 | 3731ms | 1.33x | IMPROVED | 1865ms ⭐ TOP 20 |
| 12 | Q65 | 3548ms | 1.00x | NEUTRAL | 1774ms ⭐ TOP 20 |
| 13 | Q59 | 2873ms | 1.00x | NEUTRAL | 1437ms ⭐ TOP 20 |
| 14 | Q93 | 2861ms | 2.73x | WIN | 1430ms ⭐ TOP 20 |
| 15 | Q75 | 2845ms | 1.00x | NEUTRAL | 1422ms ⭐ TOP 20 |
| 16 | Q47 | 2706ms | 1.00x | NEUTRAL | 1353ms ⭐ TOP 20 |
| 17 | Q97 | 2643ms | 1.00x | NEUTRAL | 1321ms ⭐ TOP 20 |
| 18 | Q9 | 2206ms | 4.47x | WIN | 1103ms ⭐ TOP 20 |
| 19 | Q88 | 2070ms | 1.00x | NEUTRAL | 1035ms ⭐ TOP 20 |
| 20 | Q87 | 1822ms | 1.00x | NEUTRAL | 911ms ⭐ TOP 20 |
| 21 | Q38 | 1599ms | 1.44x | IMPROVED | 799ms  |
| 22 | Q80 | 1553ms | 1.03x | NEUTRAL | 777ms  |
| 23 | Q72 | 1467ms | 1.00x | NEUTRAL | 734ms  |
| 24 | Q57 | 1317ms | 1.02x | NEUTRAL | 658ms  |
| 25 | Q70 | 1300ms | 1.00x | NEUTRAL | 650ms  |
| 26 | Q5 | 1169ms | 1.89x | WIN | 585ms  |
| 27 | Q35 | 1148ms | 1.00x | NEUTRAL | 574ms  |
| 28 | Q50 | 1008ms | 1.00x | NEUTRAL | 504ms  |
| 29 | Q13 | 981ms | 1.01x | NEUTRAL | 490ms  |
| 30 | Q79 | 940ms | 1.05x | NEUTRAL | 470ms  |
| 31 | Q2 | 937ms | 1.00x | NEUTRAL | 469ms  |
| 32 | Q48 | 934ms | 1.00x | NEUTRAL | 467ms  |
| 33 | Q36 | 897ms | 1.00x | NEUTRAL | 448ms  |
| 34 | Q68 | 890ms | 1.00x | NEUTRAL | 445ms  |
| 35 | Q17 | 864ms | 1.19x | IMPROVED | 432ms  |
| 36 | Q46 | 860ms | 1.02x | NEUTRAL | 430ms  |
| 37 | Q24 | 780ms | 1.00x | NEUTRAL | 390ms  |
| 38 | Q8 | 704ms | 1.03x | NEUTRAL | 352ms  |
| 39 | Q27 | 703ms | 1.01x | NEUTRAL | 351ms  |
| 40 | Q29 | 692ms | 2.35x | WIN | 346ms  |
| 41 | Q31 | 661ms | 1.04x | NEUTRAL | 331ms  |
| 42 | Q7 | 660ms | 1.00x | NEUTRAL | 330ms  |
| 43 | Q43 | 619ms | 2.71x | WIN | 309ms  |
| 44 | Q71 | 579ms | 1.00x | NEUTRAL | 290ms  |
| 45 | Q34 | 540ms | 1.00x | NEUTRAL | 270ms  |
| 46 | Q49 | 534ms | 1.02x | NEUTRAL | 267ms  |
| 47 | Q89 | 521ms | 1.00x | NEUTRAL | 261ms  |
| 48 | Q25 | 515ms | 1.00x | NEUTRAL | 258ms  |
| 49 | Q76 | 513ms | 1.10x | IMPROVED | 257ms  |
| 50 | Q99 | 464ms | 1.00x | NEUTRAL | 232ms  |
| 51 | Q85 | 460ms | 1.00x | NEUTRAL | 230ms  |
| 52 | Q39 | 452ms | 1.00x | NEUTRAL | 226ms  |
| 53 | Q73 | 450ms | 1.57x | WIN | 225ms  |
| 54 | Q66 | 445ms | 1.23x | IMPROVED | 222ms  |
| 55 | Q69 | 441ms | 1.03x | NEUTRAL | 221ms  |
| 56 | Q18 | 424ms | 1.14x | IMPROVED | 212ms  |
| 57 | Q77 | 421ms | 1.01x | NEUTRAL | 210ms  |
| 58 | Q6 | 419ms | 1.33x | IMPROVED | 209ms  |
| 59 | Q62 | 414ms | 1.23x | IMPROVED | 207ms  |
| 60 | Q19 | 389ms | 1.04x | NEUTRAL | 195ms  |
| 61 | Q54 | 389ms | 1.03x | NEUTRAL | 195ms  |
| 62 | Q63 | 387ms | 3.77x | WIN | 194ms  |
| 63 | Q98 | 385ms | 1.00x | NEUTRAL | 192ms  |
| 64 | Q60 | 378ms | 1.02x | NEUTRAL | 189ms  |
| 65 | Q53 | 356ms | 1.00x | NEUTRAL | 178ms  |
| 66 | Q81 | 355ms | 1.00x | NEUTRAL | 178ms  |
| 67 | Q56 | 349ms | 1.00x | NEUTRAL | 175ms  |
| 68 | Q33 | 339ms | 1.05x | NEUTRAL | 169ms  |
| 69 | Q3 | 296ms | 0.98x | NEUTRAL | 148ms  |
| 70 | Q10 | 290ms | 1.02x | NEUTRAL | 145ms  |
| 71 | Q58 | 269ms | 1.06x | NEUTRAL | 135ms  |
| 72 | Q82 | 265ms | 1.18x | IMPROVED | 132ms  |
| 73 | Q96 | 253ms | 1.64x | WIN | 127ms  |
| 74 | Q40 | 252ms | 1.07x | NEUTRAL | 126ms  |
| 75 | Q1 | 239ms | 2.92x | WIN | 120ms  |
| 76 | Q52 | 239ms | 1.08x | NEUTRAL | 119ms  |
| 77 | Q86 | 236ms | 1.00x | NEUTRAL | 118ms  |
| 78 | Q55 | 228ms | 1.00x | NEUTRAL | 114ms  |
| 79 | Q42 | 218ms | 1.00x | NEUTRAL | 109ms  |
| 80 | Q45 | 192ms | 1.08x | NEUTRAL | 96ms  |
| 81 | Q26 | 167ms | 1.93x | WIN | 83ms  |
| 82 | Q15 | 150ms | 2.78x | WIN | 75ms  |
| 83 | Q94 | 141ms | 1.00x | NEUTRAL | 71ms  |
| 84 | Q37 | 124ms | 1.30x | IMPROVED | 62ms  |
| 85 | Q12 | 110ms | 1.23x | IMPROVED | 55ms  |
| 86 | Q90 | 109ms | 1.57x | WIN | 55ms  |
| 87 | Q92 | 96ms | 1.00x | NEUTRAL | 48ms  |
| 88 | Q84 | 80ms | 1.22x | IMPROVED | 40ms  |
| 89 | Q83 | 76ms | 1.24x | IMPROVED | 38ms  |
| 90 | Q20 | 72ms | 1.07x | NEUTRAL | 36ms  |
| 91 | Q21 | 71ms | 1.00x | NEUTRAL | 36ms  |
| 92 | Q91 | 43ms | 1.00x | NEUTRAL | 22ms  |
| 93 | Q16 | 40ms | 1.00x | NEUTRAL | 20ms  |
| 94 | Q41 | 20ms | 1.14x | IMPROVED | 10ms  |
| 95 | Q61 | 19ms | 1.00x | NEUTRAL | 10ms  |
| 96 | Q32 | 17ms | 1.00x | NEUTRAL | 9ms  |

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
