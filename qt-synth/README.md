# qt-synth — Synthetic Witness Engine

**Win condition**: A solved witness row for any new SQL query. Everything else is a bonus.

**Canonical implementation**: `packages/qt-sql/qt_sql/validation/`

All files in this directory are thin re-exports that delegate to the canonical
location. Use these as CLI entry points or import from `qt_sql.validation.*`
directly.

## How It Works

Given an original SQL query and an optimized rewrite, the engine synthesizes
a witness database — a minimal DuckDB instance where both queries can execute
and their results can be compared. If both return identical rows, the rewrite
is semantically equivalent. If not, the engine found a counterexample.

### Witness Pipeline (force-seed first)

The pipeline is deterministic-first. Random data is a last resort, not the default.

```
1. Force-seed    — Read query predicates via AST, insert exactly the values
                   needed to satisfy WHERE/JOIN/GROUP BY constraints.
                   Up to 6 seed variants tried per query.

2. Patch-pack    — Benchmark-specific witness recipes for known hard cases.

3. Random        — Only with --random-fallback. Spray random values and hope
                   some survive filter predicates. Slow and unreliable.

4. Unsat         — If nothing works, check if the query is provably
                   unsatisfiable (contradictory predicates).
```

Why force-seed first: it reads the query's AST to extract exact predicate
values, FK relationships, and temporal anchors, then inserts rows that are
guaranteed to pass all filters. Random generation sprays values hoping some
survive — for queries with narrow date ranges or specific dimension keys,
the hit rate is near zero.

## Quick Start

```bash
# Single-query pair validation (force-seed by default)
from qt_sql.validation.synthetic_validator import SyntheticValidator
v = SyntheticValidator(dialect="postgres")
result = v.validate_sql_pair(original_sql, optimized_sql)

# Disable force-seed (random only, not recommended)
result = v.validate_sql_pair(original_sql, optimized_sql, force_seed_first=False)

# DSB-76 batch build (force-seed only, no random)
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/build_dsb76_synthetic_db.py \
    --out-db /tmp/dsb76_witness.duckdb

# DSB-76 batch build with random fallback enabled
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/build_dsb76_synthetic_db.py \
    --out-db /tmp/dsb76_witness.duckdb \
    --random-fallback --random-base

# MVROWS one-row equivalence eval
PYTHONPATH=packages/qt-shared:packages/qt-sql:. \
  python3 qt-synth/run_mvrows_one_row_eval.py --schema-mode merged
```

## CLI Flags

### `build_dsb76_synthetic_db.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--random-base` | off | Populate random base data for all tables before per-query seeding |
| `--random-fallback` | off | Enable random top-up as last resort after force-seed fails |
| `--force-seed-attempts` | 8 | Number of deterministic seed variants tried per query |
| `--force-seed-rows` | 1 | Rows inserted per seed attempt per table |
| `--patch-pack` | none | Benchmark-specific witness recipes |

### `run_mvrows_one_row_eval.py`

| Flag | Default | Description |
|------|---------|-------------|
| `--random-fallback` | off | Enable random top-up as last resort after force-seed fails |
| `--seed-attempts` | 6 | Number of force-seed variants per query |
| `--patch-pack` | none | Benchmark-specific witness recipes |

### `validate_sql_pair()` API

| Parameter | Default | Description |
|-----------|---------|-------------|
| `force_seed_first` | True | Use deterministic force-seed before random fallback |
| `witness_mode` | "single" | "single" or "multi" (clone + boundary-fail witnesses) |
| `target_rows` | 100 | Rows per table for random fallback |

## Files

| File | Role |
|------|------|
| `synthetic_validator.py` | Re-export + CLI for `qt_sql.validation.synthetic_validator` |
| `build_dsb76_synthetic_db.py` | Re-export + CLI for DSB-76 batch witness builder |
| `build_minimal_synthetic_db.py` | Re-export + CLI for minimal witness DB |
| `repair_dsb76_synthetic_db.py` | Re-export + CLI for in-place DB repair |
| `patch_packs.py` | Re-export for benchmark patch-pack system |
| `run_mvrows_one_row_eval.py` | Eval script for MVROWS one-row detection |
| `patches/` | DSB-specific witness recipes (behind patch-pack boundary) |

## Architecture

The engine is AST-first: all schema extraction, type inference, constraint
solving, and witness generation derive from the SQL parse tree (SQLGlot).

Key capabilities:
- **Force-seed witness insertion**: Reads WHERE/JOIN/GROUP BY predicates from
  the AST, inserts exact values needed to produce witness rows
- **Join-component propagation**: FK values propagated across join graph so
  inserted rows satisfy multi-table join predicates
- **Predicate-context type inference**: BETWEEN/comparison with date expressions
  overrides name-based heuristics
- **Generic temporal dimension detection**: Works on any calendar/date table
- **Multi-row witness generation**: Clone + boundary-fail witnesses for higher
  semantic recall (`witness_mode="multi"`)
- **Deterministic**: Same SQL + same config = identical synthetic data

See `packages/qt-sql/qt_sql/validation/README_MVROWS.md` for the MVROWS design.
