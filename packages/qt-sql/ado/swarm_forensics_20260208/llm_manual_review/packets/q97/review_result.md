# Review Result: q97

## Verdict
- Primary reason swarm appears behind: prior score is from an older baseline and non-equivalent month window; under strict equivalence swarm is the only valid fast candidate.
- Secondary contributors: swarm final iteration regressed, but iter1 already delivered a solid valid plan.

## Previous Winner Principle (manual SQL-derived)
- Principle: `date_cte_isolate` for month range + pre-aggregated store/catalog customer-item sets + full outer join counting.
- Evidence: `02_prev_winner.sql` uses `filtered_dates`, `ssci`, `csci`, then `FULL OUTER JOIN`.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` includes date isolation and join-structure lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends avoiding expensive full outer join via single-pass flags, but worker best remained conservative.
- Worker SQL evidence: `03_swarm_best.sql` reproduces the CTE + full outer join pattern while preserving original window.
- Conclusion: explored and implemented (conservative variant).

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.0556x.
- `benchmark_iter1.json`: W5 = 1.2160x (best swarm, rows_match=true).
- `benchmark_iter2.json`: W6 = 0.6033x.
- Was the principle implemented correctly: yes.
- If slower, why: not slower versus equivalent comparator; prior headline is inflated by month-window drift and legacy denominator.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 1 vs 1 (pass).
  - Original vs swarm: 1 vs 1 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `2a708727bccd82c74a9f213f4f1454ef82f2f96379154fd6eb5ca6ee8f5868a2`
  - Prev checksum: `86570644ed26c35de4eb7900f02631f773f7bd3be7212011a9f1828f9d2fa66d` (fail)
  - Swarm checksum: `2a708727bccd82c74a9f213f4f1454ef82f2f96379154fd6eb5ca6ee8f5868a2` (pass)
- Validation source files/commands:
  - `packets/q97/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Month sequence shifted from `1214..1225` to `1200..1211`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: keep current valid date-isolation rewrite; add explicit literal-lock checks for month windows before accepting historical baselines as comparators.
- Where to apply (fan-out, assignments, reanalyze, final selection): comparator hygiene + semantic gate.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q97/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q97/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q97/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q97/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q97/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
