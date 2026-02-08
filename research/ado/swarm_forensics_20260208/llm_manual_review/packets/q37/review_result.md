# Review Result: q37

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: date CTE isolation with filtered fact aggregation.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W1 = 1.1340154958553943x.
- `benchmark_iter1.json`: best W0 = 0.0x.
- `benchmark_iter2.json`: best W6 = 0.02005576119057537x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev changes year anchor from 1999 to 2000; rowcount drops 3 -> 2.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=False (3 vs 2), checksum=False.
- Original vs swarm: schema=True, rowcount=True (3 vs 3), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=False (3 vs 2), checksum=False.
- Original vs swarm: schema=True, rowcount=True (3 vs 3), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `831a90ebd560d3f5ea106afa80ad2fa416a73288080debe54ceb2a73fcaa43fb`
- Prev checksum: `01a180b032081cf8a41de16f187b337f6e7e23b9b2e6386fcc239eb1c43cb2e8`
- Swarm checksum: `831a90ebd560d3f5ea106afa80ad2fa416a73288080debe54ceb2a73fcaa43fb`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev changes year anchor from 1999 to 2000; rowcount drops 3 -> 2.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q37/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
