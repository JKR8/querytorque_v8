# Review Result: q78

## Verdict
- Primary reason swarm lost on paper: previous score uses an older denominator and non-equivalent output schema; in current run prev is slower in absolute runtime than swarm.
- Secondary contributors: swarm search degraded after iter0 (iter1/iter2 regressions), so best valid candidate stayed ~1.01x.

## Previous Winner Principle (manual SQL-derived)
- Principle: push down year filter via date CTE and aggregate each channel before joining.
- Evidence: `02_prev_winner.sql` introduces `filtered_dates` + `filtered_ss/ws/cs` aggregates.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` includes prefetch + single-pass lanes (`prefetch_fact_join|multi_dimension_prefetch|single_pass_aggregation`).
- Reanalyze evidence: `reanalyze_parsed.json` calls for consolidated anti-join processing and channel consolidation.
- Worker SQL evidence: `03_swarm_best.sql` keeps filtered-date aggregates for each channel and final ratio join.
- Conclusion: partially explored (implemented stable baseline-like rewrite, but did not realize aggressive consolidation from reanalyze hint).

## Performance/Validity Outcome
- `benchmark_iter0.json`: W3 = 1.0141x (best swarm).
- `benchmark_iter1.json`: W5 = 0.6565x.
- `benchmark_iter2.json`: W6 = 0.3943x.
- Was the principle implemented correctly: yes for the conservative path.
- If slower, why: no successful aggressive rewrite; later iterations regressed substantially.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: fail (prev outputs 10 cols, original 8).
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `2c91c078cae370f8c25f7f26f81ed39de9ff36aeb4d11d9969cd8f20d8b02137`
  - Prev checksum: `f39f82cf9951d6d08881d60b68dc94f4de1132a8d21cf868636457dc8f699b44` (fail)
  - Swarm checksum: `2c91c078cae370f8c25f7f26f81ed39de9ff36aeb4d11d9969cd8f20d8b02137` (pass)
- Validation source files/commands:
  - `packets/q78/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Adds extra projected keys (`ss_sold_year`, `ss_customer_sk`) not in original output.
  - Produces checksum mismatch despite equal rowcount.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce reanalyze-plan execution fidelity (when consolidation strategy is selected, require one candidate that truly fuses channel scans).
- Where to apply (fan-out, assignments, reanalyze, final selection): assignments + reanalyze handoff.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q78/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; schema/checksum divergence is explicit.
