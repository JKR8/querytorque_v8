# Review Result: q66

## Verdict
- Primary reason swarm lost: swarm rewrite failed strict equivalence while previous winner remained strict-valid.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date/time/ship-mode isolation with monthly conditional sums.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W3 = 1.0577424048651554x.
- `benchmark_iter1.json`: best W5 = 1.081787462681993x.
- `benchmark_iter2.json`: best W6 = 0.9970184406979152x.
- Was the principle implemented correctly: not to strict-valid parity.
- If slower, why: Swarm candidate has checksum drift despite same schema/rowcount; likely aggregation-associativity rewrite changed values.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=True (10 vs 10), checksum=True.
- Original vs swarm: schema=True, rowcount=True (10 vs 10), checksum=False.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=True (10 vs 10), checksum=True.
- Original vs swarm: schema=True, rowcount=True (10 vs 10), checksum=False.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `969140f2582d5370ba8acd56911d7137b4676be453a196a1a5374f13c3f0a639`
- Prev checksum: `969140f2582d5370ba8acd56911d7137b4676be453a196a1a5374f13c3f0a639`
- Swarm checksum: `010f044576796ff297563927a762e6450d74880a7bd8fb9e27a47ba0e0c17b99`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: fail
- If blocked, exact blocker: strict schema/rowcount/checksum parity not satisfied for swarm candidate.

## Semantic Integrity Check
- Drift risks observed: Swarm candidate has checksum drift despite same schema/rowcount; likely aggregation-associativity rewrite changed values.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q66/validation_strict.json`

## Confidence
- Confidence: medium
- Uncertainties:
- Checksum drift source may include numerical aggregation-order effects in addition to logical plan differences.
