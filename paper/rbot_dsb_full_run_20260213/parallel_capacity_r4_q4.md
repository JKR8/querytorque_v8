# Parallel Capacity Sweep

Queries: `['query010_multi', 'query023_multi', 'query069_multi', 'query081_multi']`
Levels: `[1, 2, 4, 6, 8]`
Rounds: `4`
Threshold: `10.0%` worst-query p50 inflation vs level-1

**Max safe parallelism: 1**

| Level | Errors | Worst Degrade % | Median Degrade % |
|---|---:|---:|---:|
| 1 | 0 | 0.00 | 0.00 |
| 2 | 0 | 19.98 | 6.68 |
| 4 | 0 | 24.97 | 13.83 |
| 6 | 0 | 42.58 | 29.67 |
| 8 | 0 | 131.75 | 57.47 |

Baseline p50 (ms):
- `query010_multi`: `40.98`
- `query023_multi`: `1270.73`
- `query069_multi`: `100.97`
- `query081_multi`: `403.82`

Artifacts: `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/parallel_capacity_r4_q4.json`, `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/parallel_capacity_r4_q4.csv`
