# R-Bot vs QueryTorque (Aligned Planner-Cost Metrics)

QT policy: `combined_best`
All numbers below use identical definitions from PostgreSQL `EXPLAIN (FORMAT JSON)` costs.

| Metric | R-Bot (n=76) | QueryTorque (n=50) |
|---|---:|---:|
| Wins (cost down) | 20 | 19 |
| Ties | 20 | 13 |
| Losses | 36 | 18 |
| Win rate % | 26.316 | 38.000 |
| Median % cost change | 0.000 | 0.000 |
| Mean % cost change | 30.025 | 110441.809 |

| Metric (common 37 query numbers; best-of-each; valid n: R-Bot=37, QueryTorque=35) | R-Bot | QueryTorque |
|---|---:|---:|
| Wins (cost down) | 14 | 15 |
| Ties | 10 | 8 |
| Losses | 13 | 12 |
| Win rate % | 37.838 | 42.857 |
| Median % cost change | 0.000 | 0.000 |
| Mean % cost change | 19.508 | 56.204 |

QT best_source counts: `{'config': 15, 'none': 4, 'rewrite': 33}`

Artifacts:
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/QUERYTORQUE_DSB_EXPLAIN_COST_REPLAY_COMBINED_BEST.csv`
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/RBT_QT_COMMON37_COST_SIDE_BY_SIDE_COMBINED_BEST.csv`
- `/mnt/c/Users/jakc9/Documents/QueryTorque_V8/paper/rbot_dsb_full_run_20260213/RBT_QT_COST_ALIGNED_SUMMARY_COMBINED_BEST.json`
