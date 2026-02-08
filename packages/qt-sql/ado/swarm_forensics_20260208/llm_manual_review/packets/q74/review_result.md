# Review Result: q74

## Verdict
- Primary reason swarm lost: both previous and swarm SQL are strict-equivalent; swarm lost on runtime only (no semantic edge available).
- Secondary contributors: swarm later iterations were unstable or regressive versus the best earlier candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: pushdown and pre-aggregation for customer-year stddev comparison.
- Evidence: derived from `02_prev_winner.sql` structure and CTE/fact-join layout.

## Swarm Exploration Trace
- Assignment evidence: see `swarm_artifacts/assignments.json` worker strategies and transform examples.
- Reanalyze evidence: see `swarm_artifacts/reanalyze_parsed.json` hint/failure analysis (where present).
- Worker SQL evidence: `03_swarm_best.sql` present and reviewed.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W4 = 1.215936679110929x.
- `benchmark_iter1.json`: best W5 = 1.2280077773449636x.
- `benchmark_iter2.json`: best W6 = 0.7846985263859467x.
- Was the principle implemented correctly: yes for strict-valid swarm candidate.
- If slower, why: No semantic drift detected; original/prev/swarm checksums identical.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=True.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Rowcount parity evidence (original vs candidate):
- Original vs prev: schema=True, rowcount=True (100 vs 100), checksum=True.
- Original vs swarm: schema=True, rowcount=True (100 vs 100), checksum=True.
- Checksum/hash parity evidence (original vs candidate):
- Original checksum: `fecebca05466f3ca26583f17f62fff8bde390fda4c1dd2436f77821b8eb36f42`
- Prev checksum: `fecebca05466f3ca26583f17f62fff8bde390fda4c1dd2436f77821b8eb36f42`
- Swarm checksum: `fecebca05466f3ca26583f17f62fff8bde390fda4c1dd2436f77821b8eb36f42`
- Validation source files/commands:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/validation_strict.json`
- strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass
- If blocked, exact blocker: N/A

## Semantic Integrity Check
- Drift risks observed: No semantic drift detected; original/prev/swarm checksums identical.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce strict checksum gate before promotion and reject non-equivalent comparator candidates.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection and comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/03_swarm_best.sql`
- Additional files:
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q74/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
- None material.
