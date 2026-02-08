# Review Result: q3

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: filter/materialize rewrite around monthly sales query.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W3 = 1.0389274826521822x.
- `benchmark_iter1.json`: best W5 = 1.043321386571465x.
- `benchmark_iter2.json`: best W6 = 0.04952674768985023x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev expands rowcount 67 -> 100 with checksum drift.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=False (67 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (67 vs 67), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=False (67 vs 100), checksum=False.
- Original vs swarm: schema=True, rowcount=True (67 vs 67), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `6446e0b0cd312216d5f642679373aeca95912abe4d57eeaaaee6f64553e6bb06`
- Prev checksum: `77287a6b4088694dde2e66226796b42c69004b4b41ecc23333be97203992a082`
- Swarm checksum: `6446e0b0cd312216d5f642679373aeca95912abe4d57eeaaaee6f64553e6bb06`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev expands rowcount 67 -> 100 with checksum drift.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q3/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
