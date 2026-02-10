# Review Result: q58

## Verdict
- Primary reason swarm did not beat prior headline: previous SQL changes the anchor week date (`2001-03-24` -> `2000-01-03`), producing a non-equivalent result set.
- Secondary contributors: swarm had parser/binder instability in non-winning iterations, limiting late exploration.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate target week once, compute per-channel item revenue CTEs, then join and ratio-filter near-equality bands.
- Evidence: `02_prev_winner.sql` uses `week_dates`, `ss_items/ws_items/cs_items`, then final ratio filter.

## Swarm Exploration Trace
- Assignment evidence: workers assigned pushdown/date isolation/prefetch and structural transform lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends single-scan multi-channel aggregation.
- Worker SQL evidence: `03_swarm_best.sql` keeps same week-date CTE pattern with original anchor date.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 = 1.0258x.
- `benchmark_iter1.json`: W5 = 1.1917x (best swarm).
- `benchmark_iter2.json`: W6 error (ambiguous `d_week_seq`).
- Was the principle implemented correctly: yes for winning swarm candidate.
- If slower, why: equivalent improvements are moderate; prior comparator relies on date-anchor drift.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 5 vs 31 (fail).
  - Original vs swarm: 5 vs 5 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `9a0b9107ce7d283d2fc4a1497460b54ee0998ca82fcb71bc59261fc52673f3b2`
  - Prev checksum: `8eb62862675e4d8a2f7273071d7ba2816367fc6d21e1a86da043a335dc346f96` (fail)
  - Swarm checksum: `9a0b9107ce7d283d2fc4a1497460b54ee0998ca82fcb71bc59261fc52673f3b2` (pass)
- Validation source files/commands:
  - `packets/q58/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Target week anchor changed from `2001-03-24` to `2000-01-03`.
  - Rowcount divergence (31 vs 5).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: add SQL lint to catch ambiguous column names (`d_week_seq`) before benchmark, preserving high-quality exploration.
- Where to apply (fan-out, assignments, reanalyze, final selection): fan-out syntax/binder precheck.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q58/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
