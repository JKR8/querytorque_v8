# DuckDB TPC-DS SF10 Leaderboard

**99 queries** | Engine: DuckDB | Scale Factor: 10 | Validation: 3-run (discard warmup, avg last 2)

## Summary

| Status | Count | % |
|--------|------:|--:|
| WIN (>=1.10x) | 30 | 30% |
| IMPROVED (1.05-1.10x) | 8 | 8% |
| NEUTRAL (0.95-1.05x) | 21 | 21% |
| REGRESSION (<0.95x) | 26 | 26% |
| ERROR | 14 | 14% |

**Average speedup (wins only): 1.37x**

## Top Winners

| # | Query | Speedup | Orig (ms) | Opt (ms) | Transform | SF10 Validated |
|--:|-------|--------:|----------:|---------:|-----------|:--------------:|
| 1 | q88 | **3.32x** | 251 | 75 | materialize_cte | yes (6.28x) |
| 2 | q35 | **2.42x** | 214 | 89 | date_cte_isolate |  |
| 3 | q59 | **1.68x** | 353 | 210 | pushdown |  |
| 4 | q41 | **1.63x** | 22 | 13 | or_to_union |  |
| 5 | q65 | **1.60x** | 355 | 222 | date_cte_isolate |  |
| 6 | q27 | **1.58x** | 177 | 112 | date_cte_isolate | yes (1.02x) |
| 7 | q61 | **1.46x** | 14 | 10 | materialize_cte |  |
| 8 | q14 | **1.40x** | 691 | 493 |  | yes (1.11x) |
| 9 | q44 | **1.37x** | 4 | 3 | materialize_cte |  |
| 10 | q80 | **1.30x** | 186 | 143 | date_cte_isolate | yes (1.96x) |
| 11 | q9 | **1.28x** | 798 | 625 |  |  |
| 12 | q12 | **1.27x** | 36 | 29 |  |  |
| 13 | q46 | **1.23x** | 184 | 149 | or_to_union | yes (0.89x) |
| 14 | q66 | **1.21x** | 67 | 55 | date_cte_isolate |  |
| 15 | q57 | **1.20x** | 218 | 183 | date_cte_isolate | yes (1.01x) |
| 16 | q81 | **1.20x** | 92 | 77 | decorrelate |  |
| 17 | q45 | **1.19x** | 76 | 64 | or_to_union | yes (1.35x) |
| 18 | q8 | **1.16x** | 362 | 311 |  | yes (0.98x) |
| 19 | q56 | **1.16x** | 64 | 56 | date_cte_isolate |  |
| 20 | q83 | **1.16x** | 25 | 22 | materialize_cte |  |
| 21 | q70 | **1.15x** | 207 | 180 | date_cte_isolate |  |
| 22 | q40 | **1.15x** | 51 | 44 | date_cte_isolate | yes (2.30x) |
| 23 | q30 | **1.15x** | 63 | 55 | decorrelate |  |
| 24 | q69 | **1.13x** | 96 | 85 | decorrelate | yes (1.13x) |
| 25 | q53 | **1.12x** | 59 | 53 | or_to_union |  |
| 26 | q4 | **1.12x** | 1839 | 1647 |  | yes (1.25x) |
| 27 | q99 | **1.11x** | 80 | 72 | date_cte_isolate | yes (1.07x) |
| 28 | q50 | **1.11x** | 153 | 138 | date_cte_isolate |  |
| 29 | q84 | **1.10x** | 22 | 20 | reorder_join |  |
| 30 | q22 | **1.10x** | 4230 | 3844 | date_cte_isolate |  |

## Improved (1.05-1.10x)

| Query | Speedup | Orig (ms) | Opt (ms) | Transform |
|-------|--------:|----------:|---------:|-----------|
| q43 | 1.10x | 86 | 78 | early_filter |
| q37 | 1.10x | 27 | 24 | date_cte_isolate |
| q15 | 1.09x | 51 | 47 | or_to_union |
| q33 | 1.08x | 49 | 45 | materialize_cte |
| q78 | 1.08x | 936 | 866 | pushdown |
| q34 | 1.08x | 88 | 81 | or_to_union |
| q11 | 1.06x | 953 | 896 |  |
| q7 | 1.05x | 106 | 101 | date_cte_isolate |

## Regressions

| Query | Speedup | Orig (ms) | Opt (ms) | Transform |
|-------|--------:|----------:|---------:|-----------|
| q16 | 0.14x | 18 | 126 | semantic_rewrite |
| q93 | 0.34x | 109 | 322 | early_filter |
| q31 | 0.49x | 99 | 201 | pushdown |
| q25 | 0.50x | 31 | 62 | date_cte_isolate |
| q95 | 0.54x | 390 | 728 | semantic_rewrite |
| q90 | 0.59x | 16 | 27 | early_filter |
| q74 | 0.68x | 493 | 724 | pushdown |
| q1 | 0.71x | 22 | 30 | decorrelate |
| q72 | 0.77x | 348 | 452 | semantic_rewrite |
| q58 | 0.78x | 46 | 59 | materialize_cte |
| q32 | 0.82x | 14 | 17 | decorrelate |
| q6 | 0.85x | 50 | 59 |  |
| q67 | 0.85x | 4509 | 5291 | date_cte_isolate |
| q73 | 0.87x | 112 | 128 | or_to_union |
| q51 | 0.87x | 1424 | 1629 | date_cte_isolate |
| q71 | 0.89x | 82 | 92 | or_to_union |
| q48 | 0.90x | 151 | 168 | or_to_union |
| q17 | 0.90x | 106 | 117 |  |
| q97 | 0.90x | 273 | 301 | date_cte_isolate |
| q47 | 0.91x | 415 | 456 | or_to_union |
| q36 | 0.91x | 567 | 620 | multi_push_predicate |
| q92 | 0.92x | 28 | 31 | decorrelate |
| q28 | 0.92x | 327 | 357 | semantic_rewrite |
| q89 | 0.94x | 82 | 87 | or_to_union |
| q85 | 0.95x | 82 | 86 | or_to_union |
| q10 | 0.95x | 59 | 62 | date_cte_isolate |

## Errors

| Query | Status | Transform |
|-------|--------|-----------|
| q2 | error | pushdown |
| q13 | error | or_to_union |
| q18 | error | date_cte_isolate |
| q21 | error | date_cte_isolate |
| q24 | error | pushdown |
| q52 | error | date_cte_isolate |
| q54 | error | date_cte_isolate |
| q60 | error | date_cte_isolate |
| q63 | error | or_to_union |
| q64 | error | pushdown |
| q76 | error | pushdown |
| q82 | parse_error | date_cte_isolate |
| q91 | wrong_results | or_to_union |
| q94 | error | date_cte_isolate |

## Full Results (all 99 queries)

| Query | Status | Speedup | Orig (ms) | Opt (ms) | Rows Match | Transform |
|-------|--------|--------:|----------:|---------:|:----------:|-----------|
| q1 | REGRESSION | 0.71x | 22 | 30 | yes | decorrelate |
| q2 | error | 0.00x | 0 | 0 | — | pushdown |
| q3 | NEUTRAL | 1.04x | 37 | 35 | yes |  |
| q4 | WIN | 1.12x | 1839 | 1647 | yes |  |
| q5 | NEUTRAL | 0.96x | 110 | 115 | yes |  |
| q6 | REGRESSION | 0.85x | 50 | 59 | yes |  |
| q7 | IMPROVED | 1.05x | 106 | 101 | yes | date_cte_isolate |
| q8 | WIN | 1.16x | 362 | 311 | yes |  |
| q9 | WIN | 1.28x | 798 | 625 | yes |  |
| q10 | REGRESSION | 0.95x | 59 | 62 | yes | date_cte_isolate |
| q11 | IMPROVED | 1.06x | 953 | 896 | yes |  |
| q12 | WIN | 1.27x | 36 | 29 | yes |  |
| q13 | error | 0.00x | 0 | 0 | — | or_to_union |
| q14 | WIN | 1.40x | 691 | 493 | yes |  |
| q15 | IMPROVED | 1.09x | 51 | 47 | yes | or_to_union |
| q16 | REGRESSION | 0.14x | 18 | 126 | yes | semantic_rewrite |
| q17 | REGRESSION | 0.90x | 106 | 117 | yes |  |
| q18 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q19 | NEUTRAL | 0.99x | 57 | 57 | yes | date_cte_isolate |
| q20 | NEUTRAL | 1.01x | 31 | 31 | yes | date_cte_isolate |
| q21 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q22 | WIN | 1.10x | 4230 | 3844 | yes | date_cte_isolate |
| q23 | NEUTRAL | 1.02x | 1854 | 1826 | yes | date_cte_isolate |
| q24 | error | 0.00x | 0 | 0 | — | pushdown |
| q25 | REGRESSION | 0.50x | 31 | 62 | yes | date_cte_isolate |
| q26 | NEUTRAL | 1.01x | 156 | 155 | yes | or_to_union |
| q27 | WIN | 1.58x | 177 | 112 | yes | date_cte_isolate |
| q28 | REGRESSION | 0.92x | 327 | 357 | yes | semantic_rewrite |
| q29 | NEUTRAL | 1.00x | 121 | 121 | yes | date_cte_isolate |
| q30 | WIN | 1.15x | 63 | 55 | yes | decorrelate |
| q31 | REGRESSION | 0.49x | 99 | 201 | yes | pushdown |
| q32 | REGRESSION | 0.82x | 14 | 17 | yes | decorrelate |
| q33 | IMPROVED | 1.08x | 49 | 45 | yes | materialize_cte |
| q34 | IMPROVED | 1.08x | 88 | 81 | yes | or_to_union |
| q35 | WIN | 2.42x | 214 | 89 | yes | date_cte_isolate |
| q36 | REGRESSION | 0.91x | 567 | 620 | yes | multi_push_predicate |
| q37 | IMPROVED | 1.10x | 27 | 24 | yes | date_cte_isolate |
| q38 | NEUTRAL | 1.00x | 174 | 175 | yes | date_cte_isolate |
| q39 | NEUTRAL | 1.05x | 234 | 223 | yes | pushdown |
| q40 | WIN | 1.15x | 51 | 44 | yes | date_cte_isolate |
| q41 | WIN | 1.63x | 22 | 13 | yes | or_to_union |
| q42 | NEUTRAL | 1.00x | 36 | 36 | yes | date_cte_isolate |
| q43 | IMPROVED | 1.10x | 86 | 78 | yes | early_filter |
| q44 | WIN | 1.37x | 4 | 3 | yes | materialize_cte |
| q45 | WIN | 1.19x | 76 | 64 | yes | or_to_union |
| q46 | WIN | 1.23x | 184 | 149 | yes | or_to_union |
| q47 | REGRESSION | 0.91x | 415 | 456 | yes | or_to_union |
| q48 | REGRESSION | 0.90x | 151 | 168 | yes | or_to_union |
| q49 | NEUTRAL | 0.98x | 86 | 88 | yes | date_cte_isolate |
| q50 | WIN | 1.11x | 153 | 138 | yes | date_cte_isolate |
| q51 | REGRESSION | 0.87x | 1424 | 1629 | yes | date_cte_isolate |
| q52 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q53 | WIN | 1.12x | 59 | 53 | yes | or_to_union |
| q54 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q55 | NEUTRAL | 1.03x | 34 | 33 | yes | date_cte_isolate |
| q56 | WIN | 1.16x | 64 | 56 | yes | date_cte_isolate |
| q57 | WIN | 1.20x | 218 | 183 | yes | date_cte_isolate |
| q58 | REGRESSION | 0.78x | 46 | 59 | yes | materialize_cte |
| q59 | WIN | 1.68x | 353 | 210 | yes | pushdown |
| q60 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q61 | WIN | 1.46x | 14 | 10 | yes | materialize_cte |
| q62 | NEUTRAL | 1.00x | 44 | 44 | yes | date_cte_isolate |
| q63 | error | 1.00x | 0 | 0 | — | or_to_union |
| q64 | error | 0.00x | 0 | 0 | — | pushdown |
| q65 | WIN | 1.60x | 355 | 222 | yes | date_cte_isolate |
| q66 | WIN | 1.21x | 67 | 55 | yes | date_cte_isolate |
| q67 | REGRESSION | 0.85x | 4509 | 5291 | yes | date_cte_isolate |
| q68 | NEUTRAL | 1.02x | 141 | 139 | yes | or_to_union |
| q69 | WIN | 1.13x | 96 | 85 | yes | decorrelate |
| q70 | WIN | 1.15x | 207 | 180 | yes | date_cte_isolate |
| q71 | REGRESSION | 0.89x | 82 | 92 | yes | or_to_union |
| q72 | REGRESSION | 0.77x | 348 | 452 | yes | semantic_rewrite |
| q73 | REGRESSION | 0.87x | 112 | 128 | yes | or_to_union |
| q74 | REGRESSION | 0.68x | 493 | 724 | yes | pushdown |
| q75 | NEUTRAL | 0.97x | 325 | 336 | yes | pushdown |
| q76 | error | 0.00x | 0 | 0 | — | pushdown |
| q77 | NEUTRAL | 0.99x | 58 | 59 | yes | date_cte_isolate |
| q78 | IMPROVED | 1.08x | 936 | 866 | yes | pushdown |
| q79 | NEUTRAL | 0.98x | 134 | 137 | yes | or_to_union |
| q80 | WIN | 1.30x | 186 | 143 | yes | date_cte_isolate |
| q81 | WIN | 1.20x | 92 | 77 | yes | decorrelate |
| q82 | parse_error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q83 | WIN | 1.16x | 25 | 22 | yes | materialize_cte |
| q84 | WIN | 1.10x | 22 | 20 | yes | reorder_join |
| q85 | REGRESSION | 0.95x | 82 | 86 | yes | or_to_union |
| q86 | NEUTRAL | 0.98x | 45 | 46 | yes | date_cte_isolate |
| q87 | NEUTRAL | 0.97x | 254 | 263 | yes | date_cte_isolate |
| q88 | WIN | 3.32x | 251 | 75 | yes | materialize_cte |
| q89 | REGRESSION | 0.94x | 82 | 87 | yes | or_to_union |
| q90 | REGRESSION | 0.59x | 16 | 27 | yes | early_filter |
| q91 | wrong_results | 1.03x | 31 | 30 | no | or_to_union |
| q92 | REGRESSION | 0.92x | 28 | 31 | yes | decorrelate |
| q93 | REGRESSION | 0.34x | 109 | 322 | yes | early_filter |
| q94 | error | 0.00x | 0 | 0 | — | date_cte_isolate |
| q95 | REGRESSION | 0.54x | 390 | 728 | yes | semantic_rewrite |
| q96 | NEUTRAL | 0.98x | 28 | 29 | yes | early_filter |
| q97 | REGRESSION | 0.90x | 273 | 301 | yes | date_cte_isolate |
| q98 | NEUTRAL | 0.97x | 97 | 100 | yes | date_cte_isolate |
| q99 | WIN | 1.11x | 80 | 72 | yes | date_cte_isolate |
