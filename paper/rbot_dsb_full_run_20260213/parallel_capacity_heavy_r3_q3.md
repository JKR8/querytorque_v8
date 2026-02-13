# Parallel Capacity Sweep

Queries: `['query023_multi', 'query081_multi', 'query101_spj_spj']`
Levels: `[1, 2, 4, 6, 8]`
Rounds: `3`
Threshold: `10.0%` worst-query p50 inflation vs level-1

**Max safe parallelism: 1**

| Level | Errors | Worst Degrade % | Median Degrade % |
|---|---:|---:|---:|
| 1 | 0 | 0.00 | 0.00 |
| 2 | 0 | 48.73 | 25.08 |
| 4 | 0 | 45.09 | 28.71 |
| 6 | 0 | 55.97 | 50.02 |
| 8 | 0 | 85.05 | 69.15 |

Baseline p50 (ms):
- `query023_multi`: `1132.91`
- `query081_multi`: `359.63`
- `query101_spj_spj`: `302.97`

Artifacts: `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/parallel_capacity_heavy_r3_q3.json`, `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/parallel_capacity_heavy_r3_q3.csv`
