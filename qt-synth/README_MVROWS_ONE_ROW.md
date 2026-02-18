# MVROWS One-Row Equivalence Eval

This evaluates whether single-witness-row synthetic validation can detect
non-semantic equivalence (NEQ) against SF100 oracle labels.

Representation contract: eval logic is AST-first, and witness construction
must be derivable from AST operators/predicates/aggregates.

## Script

- `qt-synth/run_mvrows_one_row_eval.py`

## Inputs

- Truth labels:
  - `qt-synth/equivalence_results.json`
  - Uses `sf100_match` as oracle (`True` = equivalent, `False` = non-equivalent).
- Baseline SQL:
  - `packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/baseline_queries/*.sql`
- Optimized SQL:
  - `research/ALL_OPTIMIZATIONS/postgres_dsb/<query>/swarm2_final/optimized.sql`

## Output

- Default report path:
  - `qt-synth/mvrows_one_row_equiv_eval.json`
- JSON includes:
  - `summary.one_row_metrics` (`recall`, `precision`, `accuracy`)
  - `summary.one_row_missed_non_equivalent`
  - per-query `results` with `pred`, `orig_rows`, `opt_rows`, `rows_probe`, `unsat`

## Run

From repo root:

```bash
python3 qt-synth/run_mvrows_one_row_eval.py
```

Optional flags:

```bash
python3 qt-synth/run_mvrows_one_row_eval.py \
  --schema-mode merged \
  --count-timeout-s 4 \
  --seed-attempts 6 \
  --patch-pack dsb_mvrows \
  --output-file qt-synth/mvrows_one_row_equiv_eval.latest.json
```

Patch-pack behavior:
- `--patch-pack none` (default): no benchmark-specific witness recipes.
- `--patch-pack dsb_mvrows`: enable DSB-specific fallback recipes.

## Metric Definition

- Positive class = **non-equivalent** (`sf100_match == False`)
- `recall` = fraction of true non-equivalent queries detected as `pred == NEQ`
- Product gate example:
  - pass if `summary.one_row_metrics.recall >= 0.80`

## Notes

- `--schema-mode original` is default and more stable.
- `--schema-mode merged` can capture extra columns from optimized SQL, but may
  hit parser edge cases for unusual aliases and fallback to baseline schema.
