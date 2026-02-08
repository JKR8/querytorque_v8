# Review Result: q91

## Verdict
- Primary reason swarm lost: previous headline advantage is non-equivalent under strict checks; swarm is the only strict-valid optimized path.
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: OR-to-UNION style filter decomposition.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W1 = 0.8316176495897339x.
- `benchmark_iter1.json`: best W5 = 0.6229034870731597x.
- `benchmark_iter2.json`: best W6 = 0.7657519193654806x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: Prev changes year from 2001 to 1998 and rowcount 21 -> 12.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=False (21 vs 12), checksum=False.
- Original vs swarm: schema=True, rowcount=True (21 vs 21), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=False (21 vs 12), checksum=False.
- Original vs swarm: schema=True, rowcount=True (21 vs 21), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `2e13dd749bada96128b52225949e4bcf06c841ca3ceb48da5e21bc0286d4090d`
- Prev checksum: `26ae3d890eebf213a742b39081251f1c7cd7305d3dc61a47313d44a29db00b76`
- Swarm checksum: `2e13dd749bada96128b52225949e4bcf06c841ca3ceb48da5e21bc0286d4090d`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: Prev changes year from 2001 to 1998 and rowcount 21 -> 12.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q91/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
