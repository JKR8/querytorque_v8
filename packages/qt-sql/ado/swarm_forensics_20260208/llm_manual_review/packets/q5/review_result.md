# Review Result: q5

## Verdict
- Primary reason swarm did not beat prior headline: prior SQL changes the date window and output alias (`returns_`), so its historical speedup is not an equivalent comparator.
- Secondary contributors: swarm peaked in iter0 and regressed afterward; no later candidate surpassed the initial equivalent rewrite.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate date range once, push it into each channel branch, then aggregate channel rollup.
- Evidence: `02_prev_winner.sql` introduces `filtered_dates` and reuses it in `ssr/csr/wsr`.

## Swarm Exploration Trace
- Assignment evidence: W2 lane (`date_cte_isolate|dimension_cte_isolate|shared_dimension_multi_channel`) aligns with prior principle.
- Reanalyze evidence: `reanalyze_parsed.json` suggests channel-wise filtering before union and aggregation.
- Worker SQL evidence: `03_swarm_best.sql` applies shared `filtered_dates` across all channel branches while preserving original literals.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.3618x (best swarm).
- `benchmark_iter1.json`: W5 = 1.1458x.
- `benchmark_iter2.json`: W6 = 0.9047x.
- Was the principle implemented correctly: yes.
- If slower, why: equivalent plan gains are moderate; prior score benefits from drifted date window and baseline epoch mismatch.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: fail (`returns` renamed `returns_`).
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `27f31e712c517aa0697789fac01e9f45ddfb82a307ba989c55e5bd74e758df58`
  - Prev checksum: `2161093f207efe45eb6f645b36bdaa652356c146bf74a1f3a0170bd2ec353b4c` (fail)
  - Swarm checksum: `27f31e712c517aa0697789fac01e9f45ddfb82a307ba989c55e5bd74e758df58` (pass)
- Validation source files/commands:
  - `packets/q5/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Date anchor moved from `2000-08-19` window to `2000-08-23` window.
  - Output alias drift (`returns` -> `returns_`).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: maintain current equivalent pattern; add final-stage guard against promoting literal-shifted historical comparators.
- Where to apply (fan-out, assignments, reanalyze, final selection): comparator ingestion + semantic gate.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q5/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
