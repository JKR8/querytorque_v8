# Review Result: q22

## Verdict
- Primary reason swarm lost: previous reported gain relies on shifted month window; equivalent rewrite space produced only modest (~1.04x) speedups.
- Secondary contributors: iter2 final worker failed to compile, so no late-stage improvement beyond iter0 result.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate date range in CTE then join inventory and item before `ROLLUP` aggregation.
- Evidence: `02_prev_winner.sql` uses `filtered_dates` followed by inventory join and rollup.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` includes prefetch and single-pass lanes; W3 selected as best in iter0.
- Reanalyze evidence: `reanalyze_parsed.json` suggests two-phase aggregation and explicit grouping-set decomposition.
- Worker SQL evidence: `03_swarm_best.sql` uses `filtered_dates` + `prejoined_inventory` then rollup over item dimensions.
- Conclusion: explored partially (date isolation implemented; stronger rollup decomposition from reanalyze not realized in valid candidate).

## Performance/Validity Outcome
- `benchmark_iter0.json`: W3 = 1.0358x (best swarm).
- `benchmark_iter1.json`: W5 = 1.0062x.
- `benchmark_iter2.json`: W6 parse error (`syntax error near UNION`), best_worker=0.
- Was the principle implemented correctly: yes for conservative rewrite.
- If slower, why: no successful advanced rollup decomposition candidate; previous comparator changes date literals.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `672203c1210c1b2c7a4afff62a434718083393364101f8f03cb17a6eaa74bbf0`
  - Prev checksum: `eb6ada895e242421caea5c983b872aef78de9ad01089e695f1ba42f19fb9ec79` (fail)
  - Swarm checksum: `672203c1210c1b2c7a4afff62a434718083393364101f8f03cb17a6eaa74bbf0` (pass)
- Validation source files/commands:
  - `packets/q22/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed:
  - Prev changes `d_month_seq` anchor from `1188` to `1200`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce one reanalyze-driven candidate that explicitly decomposes rollup/grouping sets, with parser pre-check before final promotion.
- Where to apply (fan-out, assignments, reanalyze, final selection): reanalyze -> final candidate pipeline.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/swarm_artifacts/benchmark_iter2.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q22/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
