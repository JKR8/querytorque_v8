# Review Result: q23

## Verdict
- Primary reason swarm lost: swarm produced no valid final SQL for strict evaluation; comparison against previous winner is blocked on swarm side.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date/item/customer prefilter CTEs for cross-channel sales.
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
- If slower, why: No valid swarm SQL produced; all swarm iterations failed row-match or compile. Prev also drifts (rowcount 5 -> 100).

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=False (5 vs 100), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=False (5 vs 100), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `5d9a66965bb8ec77e75843d9f03eda9127ec7891bf6bcf41c156a43b2559415d`
- Prev checksum: `9e008a8b56971026ac191ff6746b6a45f0faeacce90b3c404902a274023e849a`
- Swarm checksum: `ERROR: missing_sql:03_swarm_best.sql`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: blocked
- If blocked, exact blocker: missing `03_swarm_best.sql` for this packet.

## Semantic Integrity Check
- Drift risks observed: No valid swarm SQL produced; all swarm iterations failed row-match or compile. Prev also drifts (rowcount 5 -> 100).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q23/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
