# Review Result: q31

## Verdict
- Primary reason swarm did not beat prior score: prior SQL changes ordering key (`ca_county` instead of `web_q1_q2_increase`), yielding checksum drift and an invalid strict comparator.
- Secondary contributors: all swarm iterations underperformed (best 0.8061x), with no successful aggressive transform candidate.

## Previous Winner Principle (manual SQL-derived)
- Principle: pre-filter year/quarters in `date_dim`, aggregate `ss/ws` by county+quarter, then compute quarter-over-quarter ratios via self-joins.
- Evidence: `02_prev_winner.sql` uses `filtered_dates`, `ss`, `ws`, then 6-way joins for Q1/Q2/Q3 ratios.

## Swarm Exploration Trace
- Assignment evidence: workers covered early filter/date isolation/prefetch and structural transforms.
- Reanalyze evidence: `reanalyze_parsed.json` recommends quarter-pivoted county aggregates and direct ratio filtering.
- Worker SQL evidence: `03_swarm_best.sql` implements county-quarter aggregates and ratio filters with original year anchor.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 0.7711x.
- `benchmark_iter1.json`: W5 = 0.8061x (best swarm).
- `benchmark_iter2.json`: W6 = 0.6673x.
- Was the principle implemented correctly: yes (swarm).
- If slower, why: equivalent rewrite still expensive on this shape; prior comparator is not strictly equivalent due ordering drift.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 307 vs 307 (pass).
  - Original vs swarm: 307 vs 307 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `34dc9ebd677f9fc3daf208cc5211a27dccb07be3c5c3dba52a906a9ebe387151`
  - Prev checksum: `e2da4bb660ec914f6db5ec5c9515b34f6194fae8915b4435737d8ab4b94938f1` (fail)
  - Swarm checksum: `34dc9ebd677f9fc3daf208cc5211a27dccb07be3c5c3dba52a906a9ebe387151` (pass)
- Validation source files/commands:
  - `packets/q31/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Final ordering changed from `ORDER BY web_q1_q2_increase` to `ORDER BY ss1.ca_county`, altering deterministic top-100 sequence.
- Risk severity: medium.

## Minimal Fix for Swarm
- Tactical change needed: no semantic fix needed; focus on execution-plan improvements for quarter-join ratios (e.g., materialized pivot table and reduced join fanout).
- Where to apply (fan-out, assignments, reanalyze, final selection): reanalyze strategy and worker prompt constraints.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q31/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - Minor: checksum drift could include floating precision effects in addition to ordering, but ordering drift is explicit.
