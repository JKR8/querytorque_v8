# Review Result: q44

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: materialize threshold + rank ascending/descending join.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W4 = 1.2159670898043362x.
- `benchmark_iter1.json`: best W5 = 1.1694504884907846x.
- `benchmark_iter2.json`: best W6 = 0.6834313107635834x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev candidate does not compile (GROUP BY/binder error).

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: blocked (left_ok=True, right_ok=False).
- Original vs swarm: schema=True, rowcount=True (0 vs 0), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: blocked (left_ok=True, right_ok=False).
- Original vs swarm: schema=True, rowcount=True (0 vs 0), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Prev checksum: `ERROR: Binder Error: column profit_threshold must appear in the GROUP BY clause or be used in an aggregate function`
- Swarm checksum: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev candidate does not compile (GROUP BY/binder error).
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q44/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
