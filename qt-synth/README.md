# qt-synth

DSB benchmark tooling and CLI wrappers for the synthetic SQL validation engine.

**Canonical implementation**: `packages/qt-sql/qt_sql/validation/`

All files in this directory are thin re-exports that delegate to the canonical
location. Use these as CLI entry points or import from `qt_sql.validation.*`
directly.

## Quick Start

```bash
# Single-query validation (file-based)
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/synthetic_validator.py your_query.sql --target-rows 1000

# SQL pair equivalence check (from code)
from qt_sql.validation.synthetic_validator import SyntheticValidator
v = SyntheticValidator(dialect="postgres")
result = v.validate_sql_pair(original_sql, optimized_sql, witness_mode="multi")

# DSB-76 batch synthetic DB build
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/build_dsb76_synthetic_db.py \
    --out-db /tmp/dsb76_mvrows.duckdb --force-seed-attempts 16

# MVROWS one-row equivalence eval
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/run_mvrows_one_row_eval.py --schema-mode merged
```

## Files

| File | Role |
|------|------|
| `synthetic_validator.py` | Re-export + CLI for `qt_sql.validation.synthetic_validator` |
| `build_dsb76_synthetic_db.py` | Re-export + CLI for DSB-76 batch builder |
| `build_minimal_synthetic_db.py` | Re-export + CLI for minimal witness DB |
| `repair_dsb76_synthetic_db.py` | Re-export + CLI for in-place DB repair |
| `patch_packs.py` | Re-export for benchmark patch-pack system |
| `validator.py` / `validator_v2.py` | Legacy compatibility aliases |
| `run_mvrows_one_row_eval.py` | Eval script for MVROWS one-row detection |
| `patches/` | DSB-specific witness recipes (behind patch-pack boundary) |

## Architecture

The engine is AST-first: all schema extraction, type inference, constraint
solving, and witness generation derive from the SQL parse tree (SQLGlot). No
raw SQL string heuristics.

Key features:
- **Predicate-context type inference**: BETWEEN/comparison with date expressions
  overrides name-based heuristics
- **Generic temporal dimension detection**: Works on any calendar/date table,
  not just TPC-DS `date_dim`
- **Multi-row witness generation**: Clone + boundary-fail witnesses for higher
  semantic recall (`witness_mode="multi"`)
- **Deterministic**: Same SQL + same config = identical synthetic data

See `packages/qt-sql/qt_sql/validation/README_MVROWS.md` for the MVROWS design.
