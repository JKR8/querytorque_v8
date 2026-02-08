# Review Result: q19

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date CTE isolate + filtered grouping.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W1 = 1.076821298293995x.
- `benchmark_iter1.json`: best W5 = 0.972269704718516x.
- `benchmark_iter2.json`: best W6 = 0.9550407815346978x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev shifts temporal literals (1999/12 -> 1998/11), causing checksum drift.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `a87e1ba3add7e0387371e3de1fcd5b7289cd7bd7ef754b10c51ad0a8ec95b819`
- Prev checksum: `cb014bc4a9dc6ca3b9a4d89497052c00244ccfa11134e166e90cf37386aa193a`
- Swarm checksum: `a87e1ba3add7e0387371e3de1fcd5b7289cd7bd7ef754b10c51ad0a8ec95b819`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev shifts temporal literals (1999/12 -> 1998/11), causing checksum drift.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q19/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
