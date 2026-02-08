# Review Result: q93

## Verdict
- Primary reason swarm appears behind: comparison table uses legacy speedup denominator for prev; in current-run absolute terms swarm is materially faster.
- Secondary contributors: none substantial on SQL quality; this is mainly a provenance/metric normalization issue.

## Previous Winner Principle (manual SQL-derived)
- Principle: remove implicit cross join pattern by prefiltering `reason`, joining to `store_returns`, then joining to `store_sales` (decorrelation + join-chain normalization).
- Evidence: `02_prev_winner.sql` CTEs `filtered_reason`, `filtered_returns`, then single aggregated join.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` W3 lane explicitly targets prefetch + single-pass aggregation; W2 lane targets explicit join restructuring.
- Reanalyze evidence: `reanalyze_parsed.json` explicitly recommends cross-join elimination and inner-join chain rewrite.
- Worker SQL evidence: `03_swarm_best.sql` is the same normalized join-chain strategy with equivalent CASE aggregation.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W1 = 0.8130x.
- `benchmark_iter1.json`: W5 = 0.8015x.
- `benchmark_iter2.json`: W6 = 2.0173x (best swarm, rows_match=true, exited=true).
- Was the principle implemented correctly: yes.
- If slower, why: not slower in current-run absolute terms (`swarm_ms=521.1` vs `prev_ms=1047.82`); "loss" is due to mixed historical denominator (`prev` recorded against `original_ms=2860.55`).

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `48842c19d11635116ad6935709fef4b005f85adf3f1f7bfcb91a30a3191c6f67`
  - Prev checksum: `48842c19d11635116ad6935709fef4b005f85adf3f1f7bfcb91a30a3191c6f67` (pass)
  - Swarm checksum: `48842c19d11635116ad6935709fef4b005f85adf3f1f7bfcb91a30a3191c6f67` (pass)
- Validation source files/commands:
  - `packets/q93/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass

## Semantic Integrity Check
- Drift risks observed: none material; prev and swarm both checksum-identical to original.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: normalize leaderboard comparisons to a common baseline epoch before declaring wins/losses.
- Where to apply (fan-out, assignments, reanalyze, final selection): reporting/comparison layer.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/swarm_artifacts/benchmark_iter2.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q93/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
