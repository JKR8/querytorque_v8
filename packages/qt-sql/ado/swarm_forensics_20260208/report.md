# Swarm Forensics Report: Why New Swarm Did Not Beat Previous Efforts

- Comparison source: `packages/qt-sql/ado/benchmarks/duckdb_tpcds/swarm_comparison.json` (2026-02-08T02:08:43.025151+00:00)
- Scope: queries where comparison winner is `prev` (previous effort outperformed swarm).
- Artifact set generated in this folder: copied SQL + per-query forensic tables + root-cause summary.

## Executive Findings

- Previous efforts won **52** queries in this batch comparison.
- **50/52** prev-winning queries have significant speedup comparability mismatch (`|prev_speedup - baseline/prev_ms| >= 0.25`).
- **31/52** prev-winning queries would flip to swarm wins if previous speedup is recomputed on the same baseline used for swarm.
- **20/52** prev-winning queries show semantic-drift risk in previous SQL (literal/measure-column drift heuristics).
- **5/52** prev-winning queries come from weaker provenance labels (`unvalidated`, `state_0`, `analyst_mode`).
- **4/52** prev-winning queries had no valid swarm candidate, so previous best stood by default.

- Prev speedup mismatch magnitude: mean=1.057x, median=0.736x, max=12.283x.

## Why Swarm Lost (Root Causes)

- `selection_or_variance_gap`: 24 queries
- `principle_attempt_underperformed`: 12 queries
- `principle_attempt_regressed`: 11 queries
- `missing_swarm_result`: 4 queries
- `unlabeled_prev_principle`: 1 queries

Primary interpretation:
- A large portion of the gap comes from **non-comparable historical speedups** and **legacy SQL quality/provenance issues** in previous winners, not purely from swarm search failure.
- Where comparison is fair, swarm still misses or under-implements certain prior principles on some queries (especially `decorrelate`, `or_to_union`, and unlabeled structural rewrites).

## Principle Coverage (Prev-Winner Side)

| Principle Key | Count | Avg Prev Stored | Avg Prev Recomputed | Avg Swarm | Explored in Assignments | Explored in Reanalyze | Explored in Worker SQL | Baseline Mismatch | Semantic Drift Flagged |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `date_cte_isolate` | 23 | 1.85 | 1.6862 | 1.1447 | 23 | 2 | 23 | 23 | 13 |
| `decorrelate` | 4 | 2.9878 | 1.2395 | 1.4867 | 4 | 0 | 4 | 4 | 1 |
| `early_filter` | 2 | 2.175 | 1.3817 | 1.164 | 2 | 2 | 2 | 2 | 0 |
| `history_steered` | 2 | 2.365 | 1.2658 | 1.4449 | 0 | 0 | 2 | 2 | 0 |
| `materialize_cte` | 3 | 1.4151 | 0.9012 | 1.2079 | 3 | 0 | 3 | 3 | 0 |
| `or_to_union` | 9 | 2.2356 | 1.5438 | 1.1739 | 9 | 1 | 0 | 8 | 4 |
| `pushdown` | 5 | 1.4934 | 0.9143 | 1.0161 | 5 | 2 | 0 | 4 | 0 |
| `semantic_rewrite` | 2 | 1.355 | 0.5874 | 1.0915 | 0 | 0 | 2 | 2 | 1 |
| `single_pass_aggregation` | 1 | 4.47 | 2.7678 | 0.4245 | 1 | 1 | 1 | 1 | 1 |
| `unlabeled_structural_rewrite` | 1 | 1.19 | 0.5584 | 1.0385 | 0 | 0 | 0 | 1 | 0 |

## Top 20 Losses by Stored Delta (as shown in swarm_comparison)

| Query | Stored Delta | Prev Stored | Prev Recomputed | Swarm | Reason |
|---|---:|---:|---:|---:|---|
| `q9` | -4.05x | 4.4700x | 2.7678x | 0.4245x | `principle_attempt_regressed` |
| `q81` | -3.11x | 4.3813x | 1.6419x | 1.2667x | `selection_or_variance_gap` |
| `q11` | -2.73x | 4.0000x | 2.3524x | 1.2689x | `principle_attempt_underperformed` |
| `q63` | -2.61x | 3.7700x | 2.6748x | 1.1593x | `principle_attempt_regressed` |
| `q46` | -2.21x | 3.2300x | 2.1871x | 1.0187x | `principle_attempt_regressed` |
| `q42` | -1.77x | 2.8000x | 2.0540x | 1.0251x | `principle_attempt_underperformed` |
| `q77` | -1.49x | 2.5600x | 1.7286x | 1.0665x | `principle_attempt_underperformed` |
| `q52` | -1.43x | 2.5000x | 1.6066x | 1.0720x | `principle_attempt_underperformed` |
| `q43` | -1.39x | 2.7100x | 1.5608x | 1.3239x | `principle_attempt_underperformed` |
| `q29` | -1.32x | 2.3500x | 1.5325x | 1.0282x | `principle_attempt_underperformed` |
| `q1` | -1.30x | 2.9200x | 1.3101x | 1.6196x | `selection_or_variance_gap` |
| `q4` | -1.30x | 2.9100x | 1.4151x | 1.6144x | `selection_or_variance_gap` |
| `q21` | -1.30x | 2.4300x | 2.0653x | 1.1274x | `principle_attempt_underperformed` |
| `q47` | -1.24x | 2.3100x | 1.0845x | 1.0737x | `principle_attempt_regressed` |
| `q15` | -1.04x | 3.1700x | 1.9418x | 2.1307x | `principle_attempt_underperformed` |
| `q26` | -0.88x | 1.9300x | 2.4469x | 1.0520x | `principle_attempt_regressed` |
| `q69` | -0.88x | 1.9200x | 1.0027x | 1.0431x | `selection_or_variance_gap` |
| `q85` | -0.82x | 1.8300x | 0.9301x | 1.0075x | `principle_attempt_regressed` |
| `q78` | -0.80x | 1.8100x | 0.5586x | 1.0141x | `selection_or_variance_gap` |
| `q97` | -0.76x | 1.9800x | 0.8847x | 1.2160x | `selection_or_variance_gap` |

## Top 20 Losses on Same-Baseline Delta (Swarm - Recomputed Prev)

| Query | Same-Baseline Delta | Prev Recomputed | Swarm | Reason |
|---|---:|---:|---:|---|
| `q9` | -2.3433x | 2.7678x | 0.4245x | `principle_attempt_regressed` |
| `q63` | -1.5155x | 2.6748x | 1.1593x | `principle_attempt_regressed` |
| `q26` | -1.3949x | 2.4469x | 1.0520x | `principle_attempt_regressed` |
| `q46` | -1.1684x | 2.1871x | 1.0187x | `principle_attempt_regressed` |
| `q11` | -1.0835x | 2.3524x | 1.2689x | `principle_attempt_underperformed` |
| `q42` | -1.0289x | 2.0540x | 1.0251x | `principle_attempt_underperformed` |
| `q21` | -0.9379x | 2.0653x | 1.1274x | `principle_attempt_underperformed` |
| `q77` | -0.6621x | 1.7286x | 1.0665x | `principle_attempt_underperformed` |
| `q52` | -0.5346x | 1.6066x | 1.0720x | `principle_attempt_underperformed` |
| `q29` | -0.5043x | 1.5325x | 1.0282x | `principle_attempt_underperformed` |
| `q81` | -0.3752x | 1.6419x | 1.2667x | `selection_or_variance_gap` |
| `q43` | -0.2369x | 1.5608x | 1.3239x | `principle_attempt_underperformed` |
| `q22` | -0.2281x | 1.2639x | 1.0358x | `principle_attempt_underperformed` |
| `q96` | -0.1986x | 1.2027x | 1.0041x | `principle_attempt_underperformed` |
| `q54` | -0.1262x | 1.1758x | 1.0496x | `principle_attempt_underperformed` |
| `q31` | -0.0835x | 0.8896x | 0.8061x | `principle_attempt_regressed` |
| `q47` | -0.0108x | 1.0845x | 1.0737x | `principle_attempt_regressed` |
| `q73` | 0.0107x | 1.1091x | 1.1198x | `principle_attempt_underperformed` |
| `q69` | 0.0404x | 1.0027x | 1.0431x | `selection_or_variance_gap` |
| `q85` | 0.0774x | 0.9301x | 1.0075x | `principle_attempt_regressed` |

## Query-by-Query Forensic Ledger (Prev Winners)

| Query | Prev Principle | Explored Where | Why Not | Stored Prev | Recomp Prev | Swarm | Baseline Mismatch | Semantic Drift | Provenance Risk | SQL Evidence |
|---|---|---|---|---:|---:|---:|---|---|---|---|
| `q1` | `decorrelate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 2.9200x | 1.3101x | 1.6196x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q1.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q1.sql` |
| `q10` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3200x | 1.0033x | 1.1092x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q10.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q10.sql` |
| `q11` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.269x) was below previous result (2.352x on same-baseline when available). | 4.0000x | 2.3524x | 1.2689x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q11.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q11.sql` |
| `q12` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.2300x | 0.7089x | 1.0794x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q12.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q12.sql` |
| `q15` | `or_to_union` | `assignment` | Principle `or_to_union` was explored but best swarm attempt (1.283x) was below previous result (1.942x on same-baseline when available). | 3.1700x | 1.9418x | 2.1307x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q15.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q15.sql` |
| `q17` | `unlabeled_structural_rewrite` | `none` | Previous winning SQL has no explicit transform label, so swarm had weak guidance on the exact winning tactic. | 1.1900x | 0.5584x | 1.0385x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q17.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q17.sql` |
| `q19` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.1600x | 0.8468x | 1.0768x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q19.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q19.sql` |
| `q2` | `pushdown` | `assignment` | No valid swarm speedup recorded; previous best stood uncontested in this batch. | 2.0971x | 1.3576x | nanx | yes | no | yes | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q2.sql`<br>swarm: `(none)` |
| `q20` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.1300x | 0.5833x | 1.0687x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q20.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q20.sql` |
| `q21` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.127x) was below previous result (2.065x on same-baseline when available). | 2.4300x | 2.0653x | 1.1274x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q21.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q21.sql` |
| `q22` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.036x) was below previous result (1.264x on same-baseline when available). | 1.6900x | 1.2639x | 1.0358x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q22.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q22.sql` |
| `q23` | `date_cte_isolate` | `assignment,reanalyze,worker_sql` | No valid swarm speedup recorded; previous best stood uncontested in this batch. | 2.3300x | 1.2178x | nanx | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q23.sql`<br>swarm: `(none)` |
| `q24` | `pushdown` | `assignment,reanalyze` | No valid swarm speedup recorded; previous best stood uncontested in this batch. | 0.8700x | 1.0180x | nanx | no | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q24.sql`<br>swarm: `(none)` |
| `q25` | `date_cte_isolate` | `assignment,worker_sql` | Workers using principle `date_cte_isolate` regressed (best 0.952x). | 0.9800x | 0.1701x | 0.9515x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q25.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q25.sql` |
| `q26` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.513x). | 1.9300x | 2.4469x | 1.0520x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q26.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q26.sql` |
| `q28` | `semantic_rewrite` | `worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3300x | 0.5978x | 1.0520x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q28.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q28.sql` |
| `q29` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.028x) was below previous result (1.533x on same-baseline when available). | 2.3500x | 1.5325x | 1.0282x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q29.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q29.sql` |
| `q3` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.1900x | 0.7860x | 1.0433x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q3.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q3.sql` |
| `q31` | `pushdown` | `assignment,reanalyze` | Workers using principle `pushdown` regressed (best 0.757x). | 1.3300x | 0.8896x | 0.8061x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q31.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q31.sql` |
| `q37` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3000x | 0.8694x | 1.1340x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q37.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q37.sql` |
| `q39` | `date_cte_isolate` | `assignment,worker_sql` | No valid swarm speedup recorded; previous best stood uncontested in this batch. | 2.0200x | 14.3026x | nanx | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q39.sql`<br>swarm: `(none)` |
| `q4` | `history_steered` | `worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 2.9100x | 1.4151x | 1.6144x | yes | no | yes | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q4.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q4.sql` |
| `q42` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.025x) was below previous result (2.054x on same-baseline when available). | 2.8000x | 2.0540x | 1.0251x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q42.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q42.sql` |
| `q43` | `early_filter` | `assignment,reanalyze,worker_sql` | Principle `early_filter` was explored but best swarm attempt (1.324x) was below previous result (1.561x on same-baseline when available). | 2.7100x | 1.5608x | 1.3239x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q43.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q43.sql` |
| `q44` | `materialize_cte` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3654x | 0.9465x | 1.2160x | yes | no | yes | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q44.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q44.sql` |
| `q46` | `or_to_union` | `assignment,reanalyze` | Workers using principle `or_to_union` regressed (best 0.975x). | 3.2300x | 2.1871x | 1.0187x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q46.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q46.sql` |
| `q47` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.748x). | 2.3100x | 1.0845x | 1.0737x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q47.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q47.sql` |
| `q5` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.8900x | 0.7863x | 1.3618x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q5.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q5.sql` |
| `q51` | `history_steered` | `worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.8200x | 1.1165x | 1.2755x | yes | no | yes | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q51.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q51.sql` |
| `q52` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.072x) was below previous result (1.607x on same-baseline when available). | 2.5000x | 1.6066x | 1.0720x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q52.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q52.sql` |
| `q54` | `date_cte_isolate` | `assignment,reanalyze,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.050x) was below previous result (1.176x on same-baseline when available). | 1.8100x | 1.1758x | 1.0496x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q54.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q54.sql` |
| `q58` | `materialize_cte` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.6400x | 0.9762x | 1.1917x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q58.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q58.sql` |
| `q6` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3300x | 0.6299x | 1.2309x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q6.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q6.sql` |
| `q63` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.510x). | 3.7700x | 2.6748x | 1.1593x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q63.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q63.sql` |
| `q66` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.2300x | 0.5446x | 1.0818x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q66.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q66.sql` |
| `q68` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.674x). | 1.4200x | 0.8246x | 1.1716x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q68.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q68.sql` |
| `q69` | `decorrelate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.9200x | 1.0027x | 1.0431x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q69.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q69.sql` |
| `q72` | `semantic_rewrite` | `worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.3800x | 0.5771x | 1.1310x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q72.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q72.sql` |
| `q73` | `or_to_union` | `assignment` | Principle `or_to_union` was explored but best swarm attempt (1.068x) was below previous result (1.109x on same-baseline when available). | 1.5700x | 1.1091x | 1.1198x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q73.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q73.sql` |
| `q74` | `pushdown` | `assignment` | Workers using principle `pushdown` regressed (best 0.000x). | 1.3600x | 0.7478x | 1.2280x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q74.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q74.sql` |
| `q77` | `date_cte_isolate` | `assignment,worker_sql` | Principle `date_cte_isolate` was explored but best swarm attempt (1.066x) was below previous result (1.729x on same-baseline when available). | 2.5600x | 1.7286x | 1.0665x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q77.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q77.sql` |
| `q78` | `pushdown` | `assignment` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.8100x | 0.5586x | 1.0141x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q78.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q78.sql` |
| `q80` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 2.0600x | 0.9068x | 1.9122x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q80.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q80.sql` |
| `q81` | `decorrelate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 4.3813x | 1.6419x | 1.2667x | yes | no | yes | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q81.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q81.sql` |
| `q83` | `materialize_cte` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.2400x | 0.7810x | 1.2160x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q83.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q83.sql` |
| `q85` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.778x). | 1.8300x | 0.9301x | 1.0075x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q85.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q85.sql` |
| `q9` | `single_pass_aggregation` | `assignment,reanalyze,worker_sql` | Workers using principle `single_pass_aggregation` regressed (best 0.424x). | 4.4700x | 2.7678x | 0.4245x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q9.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q9.sql` |
| `q91` | `or_to_union` | `assignment` | Workers using principle `or_to_union` regressed (best 0.699x). | 0.8900x | 0.6954x | 0.8316x | no | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q91.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q91.sql` |
| `q93` | `decorrelate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 2.7300x | 1.0032x | 2.0173x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q93.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q93.sql` |
| `q96` | `early_filter` | `assignment,reanalyze,worker_sql` | Principle `early_filter` was explored but best swarm attempt (1.004x) was below previous result (1.203x on same-baseline when available). | 1.6400x | 1.2027x | 1.0041x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q96.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q96.sql` |
| `q97` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.9800x | 0.8847x | 1.2160x | yes | no | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q97.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q97.sql` |
| `q98` | `date_cte_isolate` | `assignment,worker_sql` | Principle was explored near/above prior level, but final swarm pick still under previous best. | 1.2600x | 0.7626x | 1.0995x | yes | yes | no | prev: `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/q98.sql`<br>swarm: `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/q98.sql` |

## Generated Artifacts

- `packages/qt-sql/ado/swarm_forensics_20260208/data/summary.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/data/prev_winner_forensics.csv`
- `packages/qt-sql/ado/swarm_forensics_20260208/data/prev_winner_forensics.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/data/top_stored_regressions.csv`
- `packages/qt-sql/ado/swarm_forensics_20260208/data/top_same_baseline_regressions.csv`
- `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/*.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/*.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/sql/original/*.sql`
