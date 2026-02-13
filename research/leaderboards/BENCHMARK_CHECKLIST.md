# Benchmark Checklist (Final Architecture)

Canonical leaderboard artifacts:
- `research/leaderboards/20260209_duckdb_tpcds_v3_swarm.html`
- `research/leaderboards/20260212_pg_dsb_v2_combined.html`

## Must-Run Experiments

- [ ] Freeze one canonical run pack per engine (DuckDB TPC-DS, PostgreSQL DSB) with fixed commit/model/config.
- [ ] Re-run all reported winners and regressions with 5-run trimmed mean; publish raw timing vectors.
- [ ] Run PG correctness hardening: checksum/value-level validation for claimed wins (not row-count only).
- [ ] Run rewrite-only vs rewrite+config ablation on PG and report them separately.
- [ ] Run controlled R-Bot head-to-head on identical hardware, timeout, query set, and validation protocol.
- [ ] Run component ablations: `-DAG`, `-gold examples`, `-gap profile`, `single worker`, `reasoning vs standard model`.
- [ ] Run worker diversity attribution on one frozen run (best + unique wins per worker).
- [ ] Run timeout sensitivity analysis (with and without timeout-recovery outliers).
- [ ] Run cross-engine parity experiment (same pipeline/prompt/validator; engine profile swapped only).
- [ ] Run reproducibility drill from clean checkout and regenerate all paper tables/figures.

## Publication Gates (Pass Criteria)

- [ ] All paper `STUB` fields removed from `paper/querytorque.tex`.
- [ ] One source-of-truth leaderboard JSON/HTML pair per benchmark (no conflicting summaries).
- [ ] Validation protocol in paper matches implementation and run scripts.
- [ ] PG version, scale factor, and timeout in results match declared setup.
- [ ] Per-query artifact bundle published (`sql`, timings, status, transforms, provenance).
