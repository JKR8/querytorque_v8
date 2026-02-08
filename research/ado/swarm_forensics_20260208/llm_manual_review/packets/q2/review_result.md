# Review Result: q2

## Verdict
- Primary reason swarm lost: swarm produced no valid final SQL for strict evaluation; comparison against previous winner is blocked on swarm side.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: pushdown of date/day aggregation pipeline.
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
- If slower, why: No valid swarm SQL produced. Prev preserves rowcount but checksum drifts.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=True (2513 vs 2513), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=True (2513 vs 2513), checksum=False.
- Original vs swarm: blocked (left_ok=True, right_ok=False).
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `e4e1c18ffb415417c82123730bc874024e94f85551ca2eb79588a413f13d2811`
- Prev checksum: `7b547c269de372614cbae422944e7fb6faf74ec7798cdf9af9cf5f271a89f328`
- Swarm checksum: `ERROR: missing_sql:03_swarm_best.sql`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: blocked
- If blocked, exact blocker: missing `03_swarm_best.sql` for this packet.

## Semantic Integrity Check
- Drift risks observed: No valid swarm SQL produced. Prev preserves rowcount but checksum drifts.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q2/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
