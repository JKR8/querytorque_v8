# V1 Prompt Pack

Rendered examples of every prompt type in the QueryTorque V8 system.

## Version
- **Version**: V1_20260213
- **Generated**: 2026-02-13
- **Query**: TPC-DS Q88 (5.25x-6.24x, all 4 workers passed)
- **Generator**: `generate_sample.py --version V1_20260213`
- **Changes from V0**: + Q-Error routing (section 2b-i), + exploit algorithm (knowledge/duckdb.md), + EXPLAIN-first reasoning model (section 6)

## File Manifest

| # | File | Prompt Type | Builder |
|---|------|-------------|---------|
| 02 | `02_oneshot_query_88.md` | Oneshot (analyze + rewrite) | `build_analyst_briefing_prompt(mode="oneshot")` |
| 03 | `03_expert_analyst_query_88.md` | Expert analyst | `build_analyst_briefing_prompt(mode="expert")` |
| 04 | `04_expert_worker_query_88.md` | Expert worker | `build_worker_prompt()` |
| 05 | `05_swarm_analyst_query_88.md` | Swarm analyst (4-worker) | `build_analyst_briefing_prompt(mode="swarm")` |
| 06 | `06_fan_out_query_88.md` | Fan-out (lightweight) | `build_fan_out_prompt()` |
| 07 | `07_worker_query_88.md` | Swarm worker (W2) | `build_worker_prompt()` |
| 08 | `08_snipe_analyst_query_88.md` | Snipe analyst (post-validation) | `build_snipe_analyst_prompt()` |
| 09 | `09_sniper_iter1_query_88.md` | Sniper retry iter 1 | `build_sniper_prompt()` |
| 10 | `10_sniper_iter2_query_88.md` | Sniper retry iter 2 | `build_sniper_prompt(previous_sniper_result=...)` |
| 11 | `11_pg_tuner_query_88.md` | PG tuner | `build_pg_tuner_prompt()` |

## New in V1

### Q-Error Routing (section 2b-i)
Computed from EXPLAIN ANALYZE JSON. Provides direction (OVER_EST/UNDER_EST) + locus (JOIN/SCAN/etc.) that routes to candidate pathologies with 85% accuracy on validated wins. Magnitude/severity deliberately excluded (not predictive).

### Exploit Algorithm (section 4)
Full distilled knowledge playbook from `knowledge/duckdb.md` (299 lines, 10 pathologies P0-P9). Includes engine gaps, decision gates, regression registry.

### EXPLAIN-First Reasoning (section 6)
Step 2: Q-Error routing as primary signal. Step 3: Bottleneck hypothesis anchored on Q-Error, calibrated against pathology tree. Step 5: Intervention design (hypothesis-driven).

## Regenerate
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.prompts.samples.generate_sample query_88 --version V1_20260213
```
