# Research Archive

Archived research from QueryTorque V8 development (Jan–Feb 2026).
Everything here is historical reference — the winning system is now in `packages/qt-sql/`.

## Directory Index

| Folder | Contents |
|--------|----------|
| `benchmark_results/` | DuckDB TPC-DS consolidated leaderboard, PG DSB results, retry runs, raw timing data |
| `prompt_development/` | Prompt iteration history, version snapshots, review notes, stale generators |
| `experiment_history/` | ML pipeline experiments, state analysis, evolutionary runs, pipeline iterations |
| `analysis_reports/` | Analyst session logs, research docs, grid system docs, loose reports/notes |
| `build_artifacts/` | Intermediate models, optimized query outputs, old packages |
| `adhoc_scripts/` | One-off test scripts, scratch work, payload comparisons |
| `ado/` | ADO learning system research (journal, analytics) |
| `papers/` | Reference papers (E2, R-Bot) |
| `knowledge_base/` | Early knowledge base experiments |
| `queries/` | Raw TPC-DS query files |

## Key Results (for reference)

- **DuckDB TPC-DS**: 34 WIN, 25 IMPROVED, 14 NEUTRAL, 15 REGRESSION (88 queries, 4 workers)
- **Top DuckDB**: Q88 5.25x, Q9 4.47x, Q40 3.35x, Q46 3.23x, Q42 2.80x
- **PG DSB**: 20 WIN, 4 IMPROVED, 13 NEUTRAL, 11 REGRESSION (50 queries)
- **Top PG**: Q092 4428x (timeout fix), Q065 3.93x, Q080 3.32x, Q099 2.28x

## Regenerating Prompt Pack

The only live prompt generator:
```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 -m qt_sql.prompts.samples.generate_sample query_88 --version V0
```
