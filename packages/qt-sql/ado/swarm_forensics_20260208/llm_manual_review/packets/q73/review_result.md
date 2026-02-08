# Review Result: q73

## Verdict
- Primary reason swarm did not beat prior headline: prior SQL changes date years, counties, and household potential literals, so its speedup is not semantically comparable.
- Secondary contributors: swarm improvement remained modest (~1.12x), with regression at iter2.

## Previous Winner Principle (manual SQL-derived)
- Principle: pre-filter date/store/household dimensions into CTEs and aggregate ticket/customer counts before joining customer.
- Evidence: `02_prev_winner.sql` uses `filtered_dates`, `filtered_stores`, `filtered_hd`, `grouped_sales`.

## Swarm Exploration Trace
- Assignment evidence: workers include early filter/date isolation/prefetch/structural lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends dimension-key pre-materialization and early fact filtering.
- Worker SQL evidence: `03_swarm_best.sql` mirrors grouped-sales CTE pattern while preserving original literals.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 = 1.0681x.
- `benchmark_iter1.json`: W5 = 1.1198x (best swarm).
- `benchmark_iter2.json`: W6 = 0.9252x.
- Was the principle implemented correctly: yes.
- If slower, why: equivalent rewrite gain ceiling; prior comparator uses drifted filters.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 15 vs 17 (fail).
  - Original vs swarm: 15 vs 15 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `badbde923d4e3ddb3d6ad8eb13169d3980d1a57ef46c29f17dbd5eccc9a54cd4`
  - Prev checksum: `f33dae9b6e6c77fa42be76982846ae7d39e77af2d69950b751dda8ef4eff7f63` (fail)
  - Swarm checksum: `badbde923d4e3ddb3d6ad8eb13169d3980d1a57ef46c29f17dbd5eccc9a54cd4` (pass)
- Validation source files/commands:
  - `packets/q73/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Years changed from `(2000,2001,2002)` to `(1999,2000,2001)`.
  - County set changed.
  - `hd_buy_potential` changed (`501-1000|Unknown` -> `Unknown|>10000`).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: keep current equivalent CTE design; prioritize runtime improvements in grouped fact pre-aggregation and sort reduction.
- Where to apply (fan-out, assignments, reanalyze, final selection): reanalyze strategy.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q73/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
