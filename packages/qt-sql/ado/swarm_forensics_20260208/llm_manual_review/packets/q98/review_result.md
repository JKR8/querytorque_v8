# Review Result: q98

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date CTE isolation + pre-aggregation before rank/window logic.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W3 = 1.0995246408983421x.
- `benchmark_iter1.json`: best W5 = 1.04047740848008x.
- `benchmark_iter2.json`: best W0 = 0.0x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev changes year from 2002 to 1999; rowcount drops 15226 -> 15076.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=False (15226 vs 15076), checksum=False.
- Original vs swarm: schema=True, rowcount=True (15226 vs 15226), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=False (15226 vs 15076), checksum=False.
- Original vs swarm: schema=True, rowcount=True (15226 vs 15226), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `9c9a8076abb3a1a5f73154ddd782a2f8b8500220b9d95dd0d616bc84f63a34dd`
- Prev checksum: `f350a0d4032d4a3658012ae67a85f5ea40d29e628e1cd30e5236997241dd4a1b`
- Swarm checksum: `9c9a8076abb3a1a5f73154ddd782a2f8b8500220b9d95dd0d616bc84f63a34dd`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev changes year from 2002 to 1999; rowcount drops 15226 -> 15076.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q98/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
