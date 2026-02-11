# GOLD: Best-of-All-Time Optimization Results

> Built: 2026-02-11
> Every optimization we ever did, paired as original:optimized

## Contents

### DuckDB TPC-DS (SF10)
- **99 queries** total
- **97 paired** (original + optimized)
- Status: {'wins': 75, 'improved': 3, 'neutral': 12, 'regression': 8, 'no_data': 1}
- Sources: Kimi K2.5, V2 Evolutionary, 3-Worker Retry, 4-Worker Retry, DSR1

### PostgreSQL DSB (SF10)
- **50 queries** total
- **46 paired** (original + optimized)
- Status: {'wins': 24, 'improved': 6, 'neutral': 13, 'regression': 7}
- Sources: V2 Swarm (6 workers), Config Tuning, pg_hint_plan, Regression Retry

## Directory Structure

```
GOLD/
├── duckdb_tpcds/
│   ├── index.json            # Master index (sorted by speedup)
│   ├── all/{qN}/             # Every query flat
│   │   ├── original.sql
│   │   ├── optimized.sql
│   │   └── metadata.json
│   ├── wins/{qN}/            # WIN queries (>1.10x)
│   ├── improved/{qN}/        # IMPROVED (1.05x-1.10x)
│   ├── neutral/{qN}/         # NEUTRAL (0.95x-1.05x)
│   └── regression/{qN}/      # REGRESSION (<0.95x)
├── pg_dsb/
│   └── (same structure)
├── GOLD_LEADERBOARD_DUCKDB_TPCDS.csv
├── GOLD_LEADERBOARD_PG_DSB.csv
└── README.md
```

## DuckDB TPC-DS Full Leaderboard

| # | Query | Speedup | Status | Transform | Source | Orig (ms) | Opt (ms) |
|---|-------|---------|--------|-----------|--------|-----------|----------|
| 1 | Q88 | 5.25x | WIN | time_bucket_aggregation | Retry4W_W4 | 2070 | 394 |
| 2 | Q9 | 4.47x | WIN | single_pass_aggregation | Retry3W_W2 | 2206 | 494 |
| 3 | Q11 | 4.00x | WIN |  | Evo | 6017 | 1504 |
| 4 | Q63 | 3.77x | WIN | or_to_union | Retry3W_W2 | 387 | 103 |
| 5 | Q40 | 3.35x | WIN | date_cte_isolate | Retry4W_W2 | 252 | 75 |
| 6 | Q46 | 3.23x | WIN | or_to_union | Retry4W_W3 | 860 | 266 |
| 7 | Q15 | 3.17x | WIN | or_to_union | Evo | 150 | 47 |
| 8 | Q1 | 2.92x | WIN | decorrelate | Kimi | 239 | 82 |
| 9 | Q42 | 2.80x | WIN | date_cte_isolate | Retry4W_W3 | 218 | 78 |
| 10 | Q93 | 2.73x | WIN | decorrelate | Kimi | 2861 | 1047 |
| 11 | Q43 | 2.71x | WIN | early_filter | Retry3W_W2 | 619 | 228 |
| 12 | Q77 | 2.56x | WIN | date_cte_isolate | Retry4W_W4 | 421 | 164 |
| 13 | Q52 | 2.50x | WIN | date_cte_isolate | Retry4W_W3 | 239 | 96 |
| 14 | Q21 | 2.43x | WIN | date_cte_isolate | Retry4W_W2 | 71 | 29 |
| 15 | Q35 | 2.42x | WIN | date_cte_isolate | DSR1 | 214 | 89 |
| 16 | Q29 | 2.35x | WIN | date_cte_isolate | Retry3W_W1 | 693 | 295 |
| 17 | Q23 | 2.33x | WIN | date_cte_isolate | Retry4W_W1 | 24404 | 10474 |
| 18 | Q47 | 2.31x | WIN | or_to_union | Retry4W_W3 | 2706 | 1171 |
| 19 | Q99 | 2.30x | WIN | date_cte_isolate | Retry4W_W3 | 464 | 202 |
| 20 | Q80 | 2.06x | WIN | date_cte_isolate | Retry4W_W3 | 1553 | 754 |
| 21 | Q39 | 2.02x | WIN | date_cte_isolate | Retry4W_W4 | 452 | 224 |
| 22 | Q97 | 1.98x | WIN | date_cte_isolate | Retry4W_W4 | 2643 | 1335 |
| 23 | Q26 | 1.93x | WIN | or_to_union | Retry3W_W1 | 167 | 87 |
| 24 | Q69 | 1.92x | WIN | decorrelate | Retry4W_W2 | 441 | 230 |
| 25 | Q5 | 1.89x | WIN |  | Retry3W_W1 | 1169 | 619 |
| 26 | Q14 | 1.88x | WIN |  | Retry4W_W4 | 9211 | 4899 |
| 27 | Q85 | 1.83x | WIN | or_to_union | Retry4W_W3 | 460 | 251 |
| 28 | Q54 | 1.81x | WIN | date_cte_isolate | Retry4W_W3 | 389 | 215 |
| 29 | Q78 | 1.81x | WIN | pushdown | Retry4W_W2 | 9002 | 4973 |
| 30 | Q22 | 1.69x | WIN | date_cte_isolate | Retry3W_W2 | 7655 | 4530 |
| 31 | Q59 | 1.68x | WIN | pushdown | DSR1 | 353 | 210 |
| 32 | Q96 | 1.64x | WIN | early_filter | Retry3W_W2 | 253 | 154 |
| 33 | Q58 | 1.64x | WIN | materialize_cte | Retry4W_W1 | 269 | 164 |
| 34 | Q41 | 1.63x | WIN | or_to_union | DSR1 | 22 | 13 |
| 35 | Q65 | 1.60x | WIN | date_cte_isolate | DSR1 | 355 | 222 |
| 36 | Q27 | 1.58x | WIN | date_cte_isolate | DSR1 | 177 | 112 |
| 37 | Q90 | 1.57x | WIN | materialize_cte | Kimi | 109 | 69 |
| 38 | Q73 | 1.57x | WIN | or_to_union | Retry3W_W2 | 450 | 287 |
| 39 | Q36 | 1.56x | WIN | multi_push_predicate | Retry4W_W4 | 897 | 575 |
| 40 | Q61 | 1.46x | WIN | materialize_cte | DSR1 | 14 | 10 |
| 41 | Q38 | 1.44x | WIN | date_cte_isolate | Retry3W_W2 | 1599 | 1110 |
| 42 | Q68 | 1.42x | WIN | or_to_union | Retry4W_W2 | 890 | 627 |
| 43 | Q72 | 1.38x | WIN | semantic_rewrite | Retry4W_W1 | 1467 | 1063 |
| 44 | Q95 | 1.37x | WIN | semantic_rewrite | Kimi | 5151 | 3760 |
| 45 | Q44 | 1.37x | WIN | materialize_cte | DSR1 | 4 | 3 |
| 46 | Q74 | 1.36x | WIN | pushdown | Kimi | 4130 | 3037 |
| 47 | Q6 | 1.33x | WIN |  | Kimi | 419 | 315 |
| 48 | Q28 | 1.33x | WIN | semantic_rewrite | Kimi | 3731 | 2805 |
| 49 | Q31 | 1.33x | WIN | pushdown | Retry4W_W3 | 661 | 497 |
| 50 | Q10 | 1.32x | WIN | date_cte_isolate | Retry4W_W2 | 290 | 220 |
| 51 | Q37 | 1.30x | WIN | date_cte_isolate | Retry3W_W2 | 124 | 95 |
| 52 | Q12 | 1.27x | WIN |  | DSR1 | 36 | 28 |
| 53 | Q98 | 1.26x | WIN | date_cte_isolate | Retry4W_W2 | 385 | 306 |
| 54 | Q83 | 1.24x | WIN | materialize_cte | Kimi | 76 | 61 |
| 55 | Q62 | 1.23x | WIN | date_cte_isolate | Kimi | 414 | 337 |
| 56 | Q66 | 1.23x | WIN | date_cte_isolate | Kimi | 445 | 362 |
| 57 | Q84 | 1.22x | WIN | reorder_join | Kimi | 80 | 66 |
| 58 | Q81 | 1.20x | WIN | decorrelate | DSR1 | 92 | 77 |
| 59 | Q57 | 1.20x | WIN | date_cte_isolate | DSR1 | 218 | 182 |
| 60 | Q3 | 1.19x | WIN |  | Retry4W_W1 | 296 | 249 |
| 61 | Q17 | 1.19x | WIN |  | Kimi | 864 | 726 |
| 62 | Q45 | 1.19x | WIN | or_to_union | DSR1 | 76 | 64 |
| 63 | Q82 | 1.18x | WIN | date_cte_isolate | Retry3W_W1 | 265 | 225 |
| 64 | Q8 | 1.16x | WIN |  | DSR1 | 362 | 312 |
| 65 | Q19 | 1.16x | WIN | date_cte_isolate | Retry4W_W4 | 389 | 335 |
| 66 | Q56 | 1.16x | WIN | date_cte_isolate | DSR1 | 64 | 55 |
| 67 | Q30 | 1.15x | WIN | decorrelate | DSR1 | 63 | 55 |
| 68 | Q70 | 1.15x | WIN | date_cte_isolate | DSR1 | 207 | 180 |
| 69 | Q18 | 1.14x | WIN | date_cte_isolate | Kimi | 424 | 372 |
| 70 | Q92 | 1.14x | WIN | decorrelate | Retry4W_W1 | 96 | 84 |
| 71 | Q20 | 1.13x | WIN | date_cte_isolate | Retry4W_W3 | 72 | 64 |
| 72 | Q53 | 1.12x | WIN | or_to_union | DSR1 | 59 | 53 |
| 73 | Q4 | 1.12x | WIN |  | DSR1 | 1839 | 1641 |
| 74 | Q50 | 1.11x | WIN | date_cte_isolate | DSR1 | 153 | 138 |
| 75 | Q76 | 1.10x | WIN | pushdown | Kimi | 513 | 466 |
| 76 | Q33 | 1.08x | IMPROVED | materialize_cte | DSR1 | 49 | 45 |
| 77 | Q34 | 1.08x | IMPROVED | or_to_union | DSR1 | 88 | 81 |
| 78 | Q7 | 1.05x | IMPROVED | date_cte_isolate | DSR1 | 106 | 101 |
| 79 | Q79 | 1.05x | NEUTRAL | or_to_union | Kimi | 940 | 895 |
| 80 | Q55 | 1.03x | NEUTRAL | date_cte_isolate | DSR1 | 34 | 33 |
| 81 | Q49 | 1.02x | NEUTRAL | date_cte_isolate | Kimi | 534 | 524 |
| 82 | Q60 | 1.02x | NEUTRAL | date_cte_isolate | Kimi | 378 | 371 |
| 83 | Q13 | 1.01x | NEUTRAL | or_to_union | Kimi | 981 | 971 |
| 84 | Q64 | 1.01x | NEUTRAL | pushdown | Kimi | 3841 | 3803 |
| 85 | Q48 | 1.00x | NEUTRAL | or_to_union | Kimi | 934 | 934 |
| 86 | Q25 | 0.98x | NEUTRAL | date_cte_isolate | Kimi | 515 | 526 |
| 87 | Q86 | 0.98x | NEUTRAL | date_cte_isolate | DSR1 | 45 | 46 |
| 88 | Q87 | 0.97x | NEUTRAL | date_cte_isolate | DSR1 | 254 | 262 |
| 89 | Q75 | 0.97x | NEUTRAL | pushdown | DSR1 | 325 | 335 |
| 90 | Q71 | 0.96x | NEUTRAL | or_to_union | Kimi | 579 | 603 |
| 91 | Q89 | 0.94x | REGRESSION | or_to_union | DSR1 | 82 | 87 |
| 92 | Q91 | 0.89x | REGRESSION | or_to_union | Retry3W_W1 | 43 | 48 |
| 93 | Q24 | 0.87x | REGRESSION | pushdown | Kimi | 780 | 897 |
| 94 | Q51 | 0.87x | REGRESSION | date_cte_isolate | DSR1 | 1424 | 1637 |
| 95 | Q67 | 0.85x | REGRESSION | date_cte_isolate | DSR1 | 4509 | 5302 |
| 96 | Q32 | 0.82x | REGRESSION | decorrelate | DSR1 | 14 | 17 |
| 97 | Q94 | 0.24x | REGRESSION | date_cte_isolate | Retry3W_W2 | 141 | 588 |
| 98 | Q16 | 0.14x | REGRESSION | semantic_rewrite | DSR1 | 18 | 126 |
| 99 | Q2 | N/A | NO_DATA | pushdown | NO_DATA |  |  |

## PostgreSQL DSB Full Leaderboard

| # | Query | Speedup | Status | Transform | Source | Orig (ms) | Opt (ms) |
|---|-------|---------|--------|-----------|--------|-----------|----------|
| 1 | query092_multi | 4428.32x | WIN | decorrelate, date_cte_isolate, dimension_cte_isolate | swarm_w4_q092 | 300000 | 68 |
| 2 | query081_multi | 676.00x | WIN | rewrite+config | config_tuning | 300000 | 444 |
| 3 | query032_multi | 391.71x | WIN | decorrelate, date_cte_isolate, dimension_cte_isolate | swarm_w2_q032 | 300000 | 766 |
| 4 | query013_agg | 60.69x | WIN |  | swarm_w4 | 5106 | 84 |
| 5 | query014_multi | 30.37x | WIN | rewrite+config | config_tuning | 300000 | 9878 |
| 6 | query010_multi | 30.18x | WIN |  | revalidation | 1562 | 52 |
| 7 | query039_multi | 29.48x | WIN |  | revalidation | 6656 | 226 |
| 8 | query101_agg | 13.97x | WIN |  | swarm_w5 | 118464 | 8482 |
| 9 | query100_agg | 6.91x | WIN | config_only | config_tuning | 1407 | 204 |
| 10 | query059_multi | 4.12x | WIN |  | swarm_w1 | 27244 | 6616 |
| 11 | query072_spj_spj | 3.64x | WIN |  | swarm_w2 | 4732 | 1301 |
| 12 | query101_spj_spj | 2.91x | WIN |  | swarm_w3 | 117591 | 40442 |
| 13 | query102_agg | 2.22x | WIN | hint+config | hint_poc | 14312 | 6447 |
| 14 | query065_multi | 1.93x | WIN |  | swarm_w6 | 6284 | 3251 |
| 15 | query099_agg | 1.90x | WIN |  | swarm_w3 | 96 | 51 |
| 16 | query054_multi | 1.68x | WIN |  | swarm_w2 | 35 | 21 |
| 17 | query050_agg | 1.55x | WIN |  | swarm_w4 | 11981 | 7738 |
| 18 | query080_multi | 1.47x | WIN |  | swarm_w3 | 88 | 60 |
| 19 | query087_multi | 1.44x | WIN |  | swarm_w6 | 8826 | 6124 |
| 20 | query102_spj_spj | 1.36x | WIN |  | swarm_w5 | 15300 | 11290 |
| 21 | query069_multi | 1.33x | WIN |  | swarm_w5 | 2084 | 1564 |
| 22 | query038_multi | 1.15x | WIN |  | regression_retry | 18998 | 16520 |
| 23 | query084_agg | 1.14x | WIN |  | swarm_w3 | 832 | 729 |
| 24 | query084_spj_spj | 1.10x | WIN |  | swarm_w4 | 743 | 676 |
| 25 | query027_spj_spj | 1.08x | IMPROVED |  | swarm_w6 | 6294 | 5854 |
| 26 | query050_spj_spj | 1.08x | IMPROVED |  | swarm_w5 | 8528 | 7928 |
| 27 | query023_multi | 1.07x | IMPROVED |  | swarm_w4 | 9844 | 9230 |
| 28 | query064_multi | 1.06x | IMPROVED |  | swarm_w6 | 30318 | 28672 |
| 29 | query085_agg | 1.06x | IMPROVED |  | regression_retry | 3910 | 3688 |
| 30 | query085_spj_spj | 1.05x | IMPROVED |  | regression_retry | 3150 | 3000 |
| 31 | query099_spj_spj | 1.04x | NEUTRAL |  | swarm_w6 | 57 | 54 |
| 32 | query018_spj_spj | 1.04x | NEUTRAL |  | swarm_w6 | 4727 | 4559 |
| 33 | query091_spj_spj | 1.02x | NEUTRAL |  | swarm_w5 | 1765 | 1730 |
| 34 | query030_multi | 1.02x | NEUTRAL |  | swarm_w5 | 1202 | 1174 |
| 35 | query091_agg | 1.01x | NEUTRAL |  | swarm_w3 | 1808 | 1782 |
| 36 | query018_agg | 1.01x | NEUTRAL |  | swarm_w4 | 4300 | 4237 |
| 37 | query072_agg | 1.00x | NEUTRAL |  | swarm_w2 | 4823 | 4803 |
| 38 | query019_spj_spj | 1.00x | NEUTRAL |  | swarm_w6 | 915 | 913 |
| 39 | query094_multi | 1.00x | NEUTRAL |  | swarm_w2 | 1648 | 1651 |
| 40 | query031_multi | 1.00x | REGRESSION |  | swarm_w5 | 4479 | 5372 |
| 41 | query040_spj_spj | 0.99x | NEUTRAL |  | swarm_w1 | 1174 | 1188 |
| 42 | query040_agg | 0.98x | NEUTRAL |  | swarm_w6 | 1254 | 1281 |
| 43 | query027_agg | 0.97x | NEUTRAL |  | swarm_w3 | 3553 | 3650 |
| 44 | query058_multi | 0.95x | NEUTRAL |  | swarm_w2 | 1538 | 1617 |
| 45 | query019_agg | 0.93x | REGRESSION |  | swarm_w1 | 920 | 991 |
| 46 | query025_agg | 0.91x | REGRESSION |  | swarm_w1 | 3781 | 4166 |
| 47 | query013_spj_spj | 0.90x | REGRESSION |  | swarm_w3 | 4903 | 5450 |
| 48 | query025_spj_spj | 0.76x | REGRESSION |  | swarm_w3 | 2255 | 2959 |
| 49 | query100_spj_spj | 0.51x | REGRESSION |  | swarm_w4 | 18486 | 36504 |
| 50 | query083_multi | 0.49x | REGRESSION |  | swarm_w2 | 2148 | 4356 |

## Validation Rules

All speedups validated using one of:
1. **3x runs**: Run 3 times, discard 1st (warmup), average last 2
2. **5x trimmed mean**: Run 5 times, remove min/max, average remaining 3
3. **4x triage (1-2-1-2)**: Interleaved warmup+measure for drift control

**Single-run timing comparisons are never used.**
