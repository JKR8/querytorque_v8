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
5. Execute query probe; if zero rows, optional deterministic patch-pack fallback can be enabled.

## Implemented
- AST schema/column normalization and type promotion from filters.
- Join-component extraction and propagation in force seeding.
- Aggregate super-value solver for SUM/AVG/MAX/MIN thresholds.
- Temporal anchor detection for date-heavy predicates.
- Anti-pattern table skip logic (`NOT EXISTS`, `NOT IN`, `EXCEPT`).
- Patch-pack boundary for benchmark-specific fallback recipes (`--patch-pack dsb_mvrows`).
- Simple UNSAT detector for obvious contradictions (for example `x <> x` under single-literal domain constraints).

## Determinism Rules
- No randomness.
- Stable numeric anchors from fixed variants.
- Fixed recipe key spaces per query family.
- Same SQL + same seed config => same inserted rows.

## Predicate-Context Type Inference

Column types inferred from name heuristics can be overridden when predicate
context proves a different type:

- `BETWEEN CAST('...' AS DATE) AND CAST('...' AS DATE)` → column is DATE
- `col + INTERVAL '30' DAY > ...` → column is DATE
- `col = CAST('...' AS DATE)` → column is DATE

This fixes columns like `d_date` or `cal_date` being mistyped as DECIMAL.

## Generic Temporal Dimension Detection

The engine detects temporal dimension tables generically using
`_is_temporal_dimension(table_name, columns)` which checks for:
- Table name matches (date_dim, calendar, dim_date, etc.)
- OR: has a DATE column + 2 temporal integer columns (year, month, quarter, etc.)

No TPC-DS naming is required.

## Multi-Row Witness Generation

`MultiRowWitnessGenerator` (in `witness_generator.py`) generates additional
data sets to increase semantic recall:

1. **Clone witness**: Shifted surrogate keys (+10000), same values
2. **Boundary-fail witness**: Perturbs one BETWEEN boundary by epsilon

Usage: `validate_sql_pair(..., witness_mode="multi")`

## How To Run
```bash
PYTHONPATH=packages/qt-shared:packages/qt-sql \
python3 -m qt_sql.validation.build_dsb76_synthetic_db \
  --out-db /tmp/postgres_dsb_76_mvrows.duckdb \
  --report /tmp/postgres_dsb_76_mvrows.report.json \
  --force-seed-attempts 16 \
  --force-seed-rows 1 \
  --min-query-rows 1 \
  --preferred-query-rows 1 \
  --patch-pack none
```

Patch-pack options:
- `none` (default): core AST/general flow only.
- `dsb_mvrows`: enable DSB-specific witness recipes.

## Key File
- `packages/qt-sql/qt_sql/validation/build_dsb76_synthetic_db.py`
