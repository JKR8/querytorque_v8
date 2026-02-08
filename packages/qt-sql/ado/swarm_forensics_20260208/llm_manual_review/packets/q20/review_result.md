# Review Result: q20

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date CTE isolate + category/class ranking rewrite.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W4 = 1.0687081989262024x.
- `benchmark_iter1.json`: best W5 = 1.009152389882472x.
- `benchmark_iter2.json`: best W6 = 0.7058878723464658x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev changes year from 2002 to 1999, causing checksum drift.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `841a1b1128f6c146bc83ed14b8a39e40bb92ec3c59236e9b42b0f85ede1a6214`
- Prev checksum: `e9c67b1bf07897a68b1813299ccaa7c1bf28436fc1acad370b1bae8f58447cd7`
- Swarm checksum: `841a1b1128f6c146bc83ed14b8a39e40bb92ec3c59236e9b42b0f85ede1a6214`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev changes year from 2002 to 1999, causing checksum drift.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q20/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
