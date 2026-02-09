# Script Oneshot Prompt — TODO

## Problem
`build_script_oneshot_prompt` is a skeleton missing all the battle-tested intelligence from `build_analyst_briefing_prompt`. It needs to reuse the existing section builders, not reinvent them.

## Missing from script prompt (exists in single-query prompt)

1. **EXPLAIN formatting** — `format_duckdb_explain_tree()` / `format_pg_explain_tree()` compress raw plans. Raw plans are 481K chars for the big query — must compress.
2. **Engine profiles** — optimizer gaps + strengths + field notes (DuckDB/PG JSON profiles)
3. **Gold examples** — tag-matched before/after SQL with speedups and adaptation reasoning
4. **Correctness constraints** — 4 validation gates (LITERAL_PRESERVATION, SEMANTIC_EQUIVALENCE, COMPLETE_OUTPUT, CTE_COLUMN_COMPLETENESS)
5. **Regression warnings** — causal failure rules with CAUSE/RULE format
6. **Strategy leaderboard** — per-archetype transform success rates from benchmark history
7. **DAG cost analysis** — per-node cost breakdown from CostAnalyzer
8. **Semantic intents** — pre-computed per-query + per-node intents
9. **Global knowledge** — principles + anti-patterns
10. **Resource envelope** — PG system profile for SET LOCAL tuning
11. **Exploit algorithm** — algorithmic optimization patterns (YAML)
12. **Plan scanner** — EXPLAIN plan scanning for optimizer blind spots
13. **Iteration history** — retry context from failed attempts
14. **Section validation checklist** — structured quality checks per section

## Approach
Refactor `build_script_oneshot_prompt` to reuse existing section builders from `build_analyst_briefing_prompt` (EXPLAIN compressor, engine profile formatter, example matcher, constraint injector, etc.) and layer pipeline-specific context on top (script DAG, lineage chains, cross-statement optimization opportunities).

Alternatively: add `mode="pipeline"` to `build_analyst_briefing_prompt` that injects pipeline context sections alongside all existing sections.

## EXPLAIN data collected
- 9 EXPLAIN ANALYZE plans saved to `explain_plans.json`
- Biggest: `tbl_address_portfolio` 481K chars raw — MUST use `format_duckdb_explain_tree()` to compress
