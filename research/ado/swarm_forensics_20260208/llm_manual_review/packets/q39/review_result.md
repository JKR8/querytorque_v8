# Review Result: q39

## Verdict
- Primary reason swarm lost: swarm produced no valid final SQL for strict evaluation; comparison against previous winner is blocked on swarm side.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date CTE isolate for inventory covariance self-join.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` missing in this packet.
- Conclusion: not explored to a valid executable output (all swarm candidates invalid or missing final SQL).

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W0 = 0.0x.
- `benchmark_iter1.json`: best W0 = 0.0x.
- `benchmark_iter2.json`: best W0 = 0.0x.
- Was the principle implemented correctly: not to strict-valid parity.
- If slower, why: No valid swarm SQL produced. Prev severely drifts (schema and rowcount mismatch).

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=False, rowcount=False (52 vs 2433), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=False, rowcount=False (52 vs 2433), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `647b9fc4ff56809331cd85cb4f9181aebb81f21ce8738ba57c0422ef864b13cc`
- Prev checksum: `3be3914b22cfc231231c83ce800054db87c6920fc6fb2d4d0719c33be25d3bb5`
- Swarm checksum: `ERROR: missing_sql:03_swarm_best.sql`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: blocked
- If blocked, exact blocker: missing `03_swarm_best.sql` for this packet.

## Semantic Integrity Check
- Drift risks observed: No valid swarm SQL produced. Prev severely drifts (schema and rowcount mismatch).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q39/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
