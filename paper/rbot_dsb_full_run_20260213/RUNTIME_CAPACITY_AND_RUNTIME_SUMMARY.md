# Runtime Capacity + DB Runtime Summary

## 1) Parallel Capacity (before degradation)

Method: synchronized concurrent execution against PostgreSQL `dsb_sf10`, compare p50 runtime inflation vs level-1 baseline.

### Sweep A (mixed set, 4 queries)
- Queries: `query010_multi`, `query023_multi`, `query069_multi`, `query081_multi`
- Levels: `1,2,4,6,8`; rounds: `4`
- Threshold: `<=10%` worst-query inflation
- Result: **max safe parallelism = 1**

Key points:
- Level 2 worst inflation: `+19.98%`
- Level 4 worst inflation: `+24.97%`
- Level 6 worst inflation: `+42.58%`
- Level 8 worst inflation: `+131.75%`

Artifacts:
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_r4_q4.md`
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_r4_q4.json`
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_r4_q4.csv`

### Sweep B (heavier set, 3 queries)
- Queries: `query023_multi`, `query081_multi`, `query101_spj_spj`
- Levels: `1,2,4,6,8`; rounds: `3`
- Threshold: `<=10%` worst-query inflation
- Result: **max safe parallelism = 1**

Key points:
- Level 2 worst inflation: `+48.73%`
- Level 4 worst inflation: `+45.09%`
- Level 6 worst inflation: `+55.97%`
- Level 8 worst inflation: `+85.05%`

Artifacts:
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_heavy_r3_q3.md`
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_heavy_r3_q3.json`
- `paper/rbot_dsb_full_run_20260213/parallel_capacity_heavy_r3_q3.csv`

Decision: run runtime benchmarks at **parallelism=1** to avoid degradation.

## 2) QT vs R-Bot DB Runtime Comparison (parallelism=1)

Method:
- Direct SQL execution on PostgreSQL.
- Compared common query numbers subset with practical runtime cap.
- Runs: `1` per SQL, warmup: `0`.
- Statement timeout: `60s`.
- Query nums compared: `25`.

Headline:
- QT median runtime speedup (baseline/optimized): **1.808x**
- R-Bot median runtime speedup (baseline/optimized): **1.144x**
- Optimized runtime winner counts: **QT 19**, **R-Bot 6**, ties 0

Notes:
- Two query numbers (`32`, `81`) hit baseline timeout/error for both systems under the 60s cap; optimized runtimes were still measured.

Artifacts:
- `paper/rbot_dsb_full_run_20260213/runtime_side_by_side_fastcap60_runs1.md`
- `paper/rbot_dsb_full_run_20260213/runtime_side_by_side_fastcap60_runs1.json`
- `paper/rbot_dsb_full_run_20260213/runtime_side_by_side_fastcap60_runs1.csv`
