# Review Result: q10

## Verdict
- Primary reason swarm did not beat prior headline: prior SQL changes year and county filters, causing larger result set and non-equivalent speedup.
- Secondary contributors: swarm best was only ~1.11x and later iterations regressed.

## Previous Winner Principle (manual SQL-derived)
- Principle: precompute customer sets per channel over filtered dates, then apply `EXISTS`/`OR EXISTS` against those sets.
- Evidence: `02_prev_winner.sql` uses `filtered_dates`, `store_sales_customers`, `web_sales_customers`, `catalog_sales_customers`.

## Swarm Exploration Trace
- Assignment evidence: workers include date/dimension isolation and decorrelation lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends filtered-customer-first semi-join approach.
- Worker SQL evidence: `03_swarm_best.sql` isolates date/address CTEs and keeps original correlated `EXISTS` structure with original literals.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.1092x (best swarm).
- `benchmark_iter1.json`: W5 = 0.9677x.
- `benchmark_iter2.json`: W6 = 0.8744x.
- Was the principle implemented correctly: yes.
- If slower, why: equivalent speedup limited; prior comparator benefits from changed predicate space.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 75 vs 100 (fail).
  - Original vs swarm: 75 vs 75 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `8829f4a7041283dc6fa9f8fae7e14ed0e042a594a533731be08493944cf5d9e1`
  - Prev checksum: `99ec7baf630ff9eb3de65b753dc3ce1d48c41a00fc35da346cd42d941c34f42b` (fail)
  - Swarm checksum: `8829f4a7041283dc6fa9f8fae7e14ed0e042a594a533731be08493944cf5d9e1` (pass)
- Validation source files/commands:
  - `packets/q10/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Year changed from `2001` to `2002`.
  - County list changed entirely.
  - Rowcount diverges (100 vs 75).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: keep equivalent plan and add stronger late-stage selection guard to avoid regressions replacing best iter0 candidate.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection policy.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q10/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
