# R-Bot DSB Full Run Analysis (2026-02-13)

- Log source: `/tmp/rbot_logs/full_run_keep081/dsb`
- Total logs: **76** (complete: **76**)

## Headline
- Query-improvement ratio (best-per-query): **20/76 (26.3%)**.
- Paper DSB 10x (R-Bot GPT-4): **18/76 (23.7%)**.
- Delta vs paper on this metric: **+2.6 pp**.
- But aggregate cost moved in wrong direction: **+13.74%** (best view) and **+18.79%** (last view).
- Conclusion: matches/slightly exceeds paper only on *improvement count*, but does **not** match paper-level aggregate performance in this environment.

## Run Metrics (Best View)
- Wins: **20**
- Losses: **36**
- Ties: **20**
- Median % cost change: **0.00%**
- Mean % cost change: **30.03%**
- Geometric mean speedup: **0.946x**

## Threshold Counts (Best % Change)
- <=   0%: **40/76** (52.6%)
- <=  -1%: **12/76** (15.8%)
- <=  -5%: **9/76** (11.8%)
- <= -10%: **9/76** (11.8%)
- <= -20%: **6/76** (7.9%)
- <= -50%: **6/76** (7.9%)

## Top 10 Wins (Best)
| log_file | pct_change_best | speedup_best | input_cost | output_cost_best |
|---|---:|---:|---:|---:|
| query081_0.log | -95.33% | 21.40x | 3335835.66 | 155849.05 |
| query081_1.log | -94.01% | 16.69x | 2584675.27 | 154866.55 |
| query039_0_1.log | -87.96% | 8.31x | 1589352.93 | 191301.67 |
| query039_0_0.log | -87.96% | 8.31x | 1589347.12 | 191304.54 |
| query010_0.log | -82.46% | 5.70x | 8505868.94 | 1492163.72 |
| query010_1.log | -73.52% | 3.78x | 5633208.07 | 1491915.70 |
| query102_0.log | -15.76% | 1.19x | 297724.53 | 250793.55 |
| query054_0.log | -15.65% | 1.19x | 6573.89 | 5545.14 |
| query100_0.log | -11.56% | 1.13x | 802727.77 | 709908.16 |
| query101_0.log | -2.69% | 1.03x | 209777.10 | 204144.17 |

## Top 10 Regressions (Best)
| log_file | pct_change_best | speedup_best | input_cost | output_cost_best |
|---|---:|---:|---:|---:|
| query064_1.log | 293.93% | 0.25x | 675546.67 | 2661183.26 |
| query014_0.log | 276.38% | 0.27x | 2840064.50 | 10689546.02 |
| query064_0.log | 262.73% | 0.28x | 723124.85 | 2623003.22 |
| query102_1.log | 192.55% | 0.34x | 326941.77 | 956474.32 |
| query001_0.log | 169.74% | 0.37x | 196501.97 | 530037.65 |
| query001_1.log | 106.23% | 0.48x | 196785.20 | 405820.80 |
| query031_0.log | 103.21% | 0.49x | 942319.84 | 1914909.18 |
| query030_0.log | 102.66% | 0.49x | 43451.41 | 88057.37 |
| query030_1.log | 102.62% | 0.49x | 43451.44 | 88041.05 |
| query014_1.log | 101.52% | 0.50x | 2836493.59 | 5716178.60 |

## Output Files
- `paper/rbot_dsb_full_run_20260213/dsb_full_run_results_full_dimensions.csv`
- `paper/rbot_dsb_full_run_20260213/dsb_full_run_results_full_dimensions.jsonl`
- `paper/rbot_dsb_full_run_20260213/dsb_full_run_summary_metrics.json`
- `paper/rbot_dsb_full_run_20260213/DSB_FULL_RUN_ANALYSIS_REPORT.md`
