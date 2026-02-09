# V0 Prompt Pack

Rendered examples of every prompt type in the QueryTorque V8 system.

## Version
- **Version**: V0
- **Generated**: 2026-02-10
- **Query**: TPC-DS Q88 (5.25x–6.24x, all 4 workers passed)
- **Script**: everyhousehold_deidentified.sql (1212-line enterprise pipeline)
- **Generator**: `generate_sample.py --version V0`

## File Manifest

| # | File | Prompt Builder | Source |
|---|------|---------------|--------|
| 01 | `01_oneshot_script_everyhousehold.md` | `build_script_oneshot_prompt()` | everyhousehold |
| 02 | `02_oneshot_query_88.md` | `build_analyst_briefing_prompt(mode="oneshot")` | Q88 |
| 03 | `03_expert_analyst_query_88.md` | `build_analyst_briefing_prompt(mode="expert")` | Q88 |
| 04 | `04_expert_worker_query_88.md` | `build_worker_prompt()` | Q88 |
| 05 | `05_swarm_analyst_query_88.md` | `build_analyst_briefing_prompt(mode="swarm")` | Q88 |
| 06 | `06_fan_out_query_88.md` | `build_fan_out_prompt()` | Q88 |
| 07 | `07_worker_query_88.md` | `build_worker_prompt()` | Q88 (swarm W2) |
| 08 | `08_snipe_analyst_query_88.md` | `build_snipe_analyst_prompt()` | Q88 |
| 09 | `09_sniper_iter1_query_88.md` | `build_sniper_prompt()` | Q88 |
| 10 | `10_sniper_iter2_query_88.md` | `build_sniper_prompt(previous_sniper_result=...)` | Q88 |
| 11 | `11_pg_tuner_query_88.md` | `build_pg_tuner_prompt()` | Q88 |

## Spec
See `../PROMPT_SPEC.md` for full input/output specification per prompt type.

## Regenerate
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.prompts.samples.generate_sample query_88 --version V0 --script paper/sql/everyhousehold_deidentified.sql
```
