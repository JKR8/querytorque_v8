# R-Bot vs QueryTorque (Aligned Planner-Cost Metrics)

All numbers below use identical definitions from PostgreSQL `EXPLAIN (FORMAT JSON)` costs.

| Metric | R-Bot (n=76) | QueryTorque (n=46) |
|---|---:|---:|
| Wins (cost down) | 20 | 8 |
| Ties | 20 | 13 |
| Losses | 36 | 25 |
| Win rate % | 26.316 | 17.391 |
| Median % cost change | 0.000 | 6.603 |
| Mean % cost change | 30.025 | 456.967 |

| Metric (common 37 query numbers; best-of-each; valid n: R-Bot=37, QueryTorque=33) | R-Bot | QueryTorque |
|---|---:|---:|
| Wins (cost down) | 14 | 8 |
| Ties | 10 | 10 |
| Losses | 13 | 15 |
| Win rate % | 37.838 | 24.242 |
| Median % cost change | 0.000 | 0.000 |
| Mean % cost change | 19.508 | 422.716 |

Artifacts:
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY.csv`
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/RBT_QT_COMMON37_COST_SIDE_BY_SIDE.csv`
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/RBT_QT_COST_ALIGNED_SUMMARY.json`
