# DSB76 Synthetic Validator: Deterministic MVROWS (AST-Only)

## Goal
Build synthetic data by solving query constraints directly from SQL AST, with:
- no LLM calls
- deterministic outputs
- minimum viable rows (MVROWS)

Success criterion per query:
- query executes on synthetic DB
- returns at least 1 row (configurable higher threshold later)

## Core Approach
1. Parse SQL to AST (`sqlglot`, DuckDB dialect target).
2. Extract constraint graph:
- table nodes
- join/equality edges
- filter predicates
- aggregate constraints (`HAVING`, correlated subqueries)
- anti-pattern edges (`NOT EXISTS`, `NOT IN`, `EXCEPT`)
3. Solve constraints to produce witness rows:
- equality: unify values across components
- inequalities/ranges: pick boundary-satisfying values
- aggregate thresholds: solve minimal cardinality/value
- arithmetic join offsets: satisfy with paired keys (example: `week2 = week1 + 52`)
4. Insert rows in FK-safe order.
5. Execute query probe; if zero rows, run deterministic fallback recipes for known hard templates.

## Implemented
- AST schema/column normalization and type promotion from filters.
- Join-component extraction and propagation in force seeding.
- Aggregate super-value solver for SUM/AVG/MAX/MIN thresholds.
- Temporal anchor detection for date-heavy predicates.
- Anti-pattern table skip logic (`NOT EXISTS`, `NOT IN`, `EXCEPT`).
- Deterministic MVROWS fallback entrypoint (`_apply_mvrows_recipe`) with template handlers for known holdout families.

## Determinism Rules
- No randomness.
- Stable numeric anchors from fixed variants.
- Fixed recipe key spaces per query family.
- Same SQL + same seed config => same inserted rows.

## Known Current Gaps
- Some query shapes still need pure-graph solving instead of family-specific recipe logic.
- Ambiguous query variants that are semantically contradictory should be marked UNSAT deterministically.
- Need wider arithmetic-constraint solver coverage (beyond current hardcoded patterns).

## Next Work (In Order)
1. Replace family recipes with generic DAG/constraint solver passes:
- branch solving for OR predicates
- correlated aggregate constraints as symbolic inequalities
- arithmetic equalities/offset relations across aliases
2. Add UNSAT classification and reporting.
3. Add tests:
- every DSB76 query should be `ROWS>=1` or explicit `UNSAT`.
- invariant test: rerun produces same witness rows for same query.
4. Remove recipe-specific logic after generic solver reaches equivalent or better coverage.

## How To Run
```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql \
python3 -m qt_sql.validation.build_dsb76_synthetic_db \
  --out-db /tmp/postgres_dsb_76_mvrows.duckdb \
  --report /tmp/postgres_dsb_76_mvrows.report.json \
  --force-seed-attempts 16 \
  --force-seed-rows 1 \
  --min-query-rows 1 \
  --preferred-query-rows 1
```

## Key File
- `packages/qt-sql/qt_sql/validation/build_dsb76_synthetic_db.py`
