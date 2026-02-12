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
- **52 queries** total (combined: SQL rewrites + config tuning)
- Status: {'WIN': 29, 'IMPROVED': 11, 'NEUTRAL': 12}
- Sources: V2 Swarm (76 sessions), Config Tuning (52 queries, 3-race validated), pg_hint_plan

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

| # | Query | Speedup | Status | Best Source | Config Type | Notes |
|---|-------|---------|--------|-------------|-------------|-------|
| 1 | query092_multi | 8043.91x | WIN | rewrite |  |  |
| 2 | query032_multi | 1465.16x | WIN | rewrite |  |  |
| 3 | query081_multi | 438.93x | WIN | rewrite | hint+config | both help |
| 4 | query010_multi | 30.18x | WIN | rewrite | config | both help |
| 5 | query039_multi | 29.48x | WIN | rewrite |  |  |
| 6 | query001_multi | 27.80x | WIN | rewrite |  |  |
| 7 | query101_spj_spj | 11.47x | WIN | rewrite |  |  |
| 8 | query101_agg | 10.92x | WIN | rewrite |  |  |
| 9 | query100_spj_spj | 9.09x | WIN | config | config | recovered from 0.61x |
| 10 | query083_multi | 8.56x | WIN | rewrite | hint+config | both help |
| 11 | query072_spj_spj | 7.18x | WIN | rewrite | hint+config | both help |
| 12 | query013_agg | 7.02x | WIN | rewrite |  |  |
| 13 | query102_spj_spj | 5.95x | WIN | config | config | recovered from 0.51x |
| 14 | query100_agg | 5.71x | WIN | config | hint+config | both help |
| 15 | query072_agg | 5.35x | WIN | config | hint |  |
| 16 | query064_multi | 3.81x | WIN | rewrite | hint | both help |
| 17 | query027_agg | 3.76x | WIN | config | config | recovered from 0.46x |
| 18 | query025_agg | 3.10x | WIN | rewrite |  |  |
| 19 | query014_multi | 3.02x | WIN | config | hint+config | both help |
| 20 | query058_multi | 2.51x | WIN | config | hint |  |
| 21 | query027_spj_spj | 2.35x | WIN | config | hint+config | recovered from 0.43x |
| 22 | query102_agg | 2.11x | WIN | config | config | both help |
| 23 | query059_multi | 2.07x | WIN | rewrite | config | both help |
| 24 | query069_multi | 1.98x | WIN | rewrite | config | both help |
| 25 | query030_multi | 1.86x | WIN | rewrite | config | both help |
| 26 | query075_multi | 1.85x | WIN | config | config | recovered from 0.30x |
| 27 | query031_multi | 1.79x | WIN | rewrite |  |  |
| 28 | query038_multi | 1.78x | WIN | rewrite |  |  |
| 29 | query065_multi | 1.75x | WIN | rewrite | hint+config | both help |
| 30 | query050_spj_spj | 1.39x | IMPROVED | config | config |  |
| 31 | query080_multi | 1.39x | IMPROVED | rewrite |  |  |
| 32 | query091_spj_spj | 1.21x | IMPROVED | config | config |  |
| 33 | query091_agg | 1.19x | IMPROVED | config | config | both help |
| 34 | query099_agg | 1.18x | IMPROVED | rewrite |  |  |
| 35 | query087_multi | 1.15x | IMPROVED | rewrite | config | both help |
| 36 | query050_agg | 1.09x | IMPROVED | rewrite |  |  |
| 37 | query084_spj_spj | 1.08x | IMPROVED | config | config | both help |
| 38 | query018_agg | 1.07x | IMPROVED | rewrite |  |  |
| 39 | query023_multi | 1.07x | IMPROVED | config | config | both help |
| 40 | query025_spj_spj | 1.06x | IMPROVED | rewrite |  |  |
| 41 | query018_spj_spj | 1.04x | NEUTRAL | rewrite |  |  |
| 42 | query085_agg | 1.04x | NEUTRAL | rewrite |  |  |
| 43 | query013_spj_spj | 1.03x | NEUTRAL | rewrite |  |  |
| 44 | query019_agg | 1.02x | NEUTRAL | rewrite |  |  |
| 45 | query019_spj_spj | 1.02x | NEUTRAL | rewrite |  |  |
| 46 | query094_multi | 1.02x | NEUTRAL | rewrite |  |  |
| 47 | query040_spj_spj | 1.01x | NEUTRAL | rewrite |  |  |
| 48 | query054_multi | 1.01x | NEUTRAL | rewrite |  |  |
| 49 | query040_agg | 1.00x | NEUTRAL | none |  |  |
| 50 | query084_agg | 1.00x | NEUTRAL | none |  |  |
| 51 | query085_spj_spj | 1.00x | NEUTRAL | none |  |  |
| 52 | query099_spj_spj | 1.00x | NEUTRAL | none |  |  |

