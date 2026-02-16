# PostgreSQL Dialect Knowledge

Source of truth for runtime decisions:
- Gap/strength authority: `constraints/engine_profile_postgresql.json`
- Canonical transforms: `knowledge/transforms.json`
- Canonical examples: `examples/postgres/*.json`
- Post-optimization tuning: `knowledge/config/postgresql.json` (not rendered in rewrite prompts)

This document is a compact human playbook aligned to canonical IDs.

## Canonical Transform Set (PostgreSQL)

| Family | Transform ID | Typical Use |
|---|---|---|
| A | `date_cte_explicit_join` | Fix comma-join plans and push selective date filters early |
| A/B | `early_filter_decorrelate` | Combine selective prefilter + decorrelation |
| B | `inline_decorrelate_materialized` | Correlated scalar aggregate subqueries |
| C | `materialized_dimension_fact_prefilter` | Non-equi joins with very large fact input |
| E/F | `dimension_prefetch_star` | Star-schema dimension prefetch before fact join |
| F | `pg_self_join_decomposition` | Self-join decomposition when repeated scans are expensive |

## Strengths (Do Not Fight)

- `BITMAP_OR_SCAN`: indexed OR predicates are already efficient. Avoid OR to UNION rewrites here.
- `SEMI_JOIN_EXISTS`: simple EXISTS/NOT EXISTS is usually optimal.
- `INNER_JOIN_REORDERING`: PostgreSQL reorders inner joins well; manual reorder often adds risk without gain.
- `INDEX_ONLY_SCAN`: small dimensions can be faster without CTE materialization.
- `PARALLEL_QUERY_EXECUTION`: unnecessary CTE fences can reduce useful parallelism.

## Gap-Driven Pathologies

### `COMMA_JOIN_WEAKNESS` (HIGH)
- Detect: comma-separated `FROM` with join predicates in `WHERE`.
- Preferred transforms: `date_cte_explicit_join`, `dimension_prefetch_star`.
- Notes: wins are strongest when explicit JOIN conversion and selective date/dimension filters are applied together.

### `CORRELATED_SUBQUERY_PARALYSIS` (HIGH)
- Detect: correlated scalar subquery with aggregate (`AVG/SUM/COUNT`) re-executed per outer row.
- Preferred transforms: `inline_decorrelate_materialized`, `early_filter_decorrelate`.
- Notes: highest-impact class. Preserve all correlation predicates and aggregate semantics exactly.

### `NON_EQUI_JOIN_INPUT_BLINDNESS` (HIGH)
- Detect: expensive non-equi join with large inputs and late selectivity.
- Preferred transforms: `materialized_dimension_fact_prefilter`.
- Notes: prefilter only when predicates are tight; loose prefilters can regress.

### `CROSS_CTE_PREDICATE_BLINDNESS` (MEDIUM)
- Detect: selective predicates applied after CTE materialization.
- Preferred transforms: `date_cte_explicit_join`, `early_filter_decorrelate`.
- Notes: in PostgreSQL this usually needs explicit JOIN cleanup with the filter push.

### `CTE_MATERIALIZATION_FENCE` (MEDIUM)
- Detect: large single-use CTE where outer filters cannot be pushed in.
- Preferred approach: minimize unnecessary CTE fencing; materialize only when reuse is real.
- Notes: duplicating large CTE bodies is a common regression source.

## Safety Gates

- Avoid CTE-heavy rewrites when baseline runtime is already small (overhead dominates).
- Never rewrite same-column indexed OR predicates into UNION ALL on PostgreSQL.
- Treat simple EXISTS/NOT EXISTS as protected patterns unless strong evidence says otherwise.
- For decorrelation, require true correlated scalar aggregate evidence from SQL + EXPLAIN.
- Limit deep CTE chains; each added fence can reduce parallel planning flexibility.

## Regression Patterns to Avoid

- Large UNION/CTE inlining that multiplies fact-table rescans.
- Over-materializing date/dimension CTEs in already efficient EXISTS paths.
- Applying star-prefetch patterns to self-joins or multi-fact join shapes.
- Forcing plan-shape changes without matching an engine gap signal.

## Config Tuning (Post-Rewrite)

Rewrite prompting and config tuning are separate systems.
- Rewrite guidance: this file + engine profile + transform catalog.
- Config guidance: `knowledge/config/postgresql.json`.

Use config tuning after semantic-safe SQL rewrites are established.
