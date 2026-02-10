# Review Result: q80

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date isolation around multi-channel rollup aggregation.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W2 = 1.9121582527989138x.
- `benchmark_iter1.json`: best W5 = 1.0082132779042419x.
- `benchmark_iter2.json`: best W6 = 1.3792819616907084x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev changes year from 1998 to 2000 and renames output column `returns` -> `returns_`.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=False, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=False, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `23d6e11db59c7262c3925f229d45ce5726ec6d1cf736bd7380345a655f1b0917`
- Prev checksum: `c14f5431125e1e5d9b5a7b51df1ac22c36f4ce4cf29b04f0e3544e82bb11147b`
- Swarm checksum: `23d6e11db59c7262c3925f229d45ce5726ec6d1cf736bd7380345a655f1b0917`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev changes year from 1998 to 2000 and renames output column `returns` -> `returns_`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q80/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
