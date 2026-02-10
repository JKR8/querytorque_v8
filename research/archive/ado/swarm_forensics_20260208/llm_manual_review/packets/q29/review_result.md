# Review Result: q29

## Verdict
- Primary reason swarm lost: swarm stayed semantically aligned and only achieved ~1.03x, while previous winner appears strongly non-comparable due changed month window and changed aggregation semantics.
- Secondary contributors: exploration quality was uneven (iter0 worker 1 compile error; iter2 collapse to 0.3042x).

## Previous Winner Principle (manual SQL-derived)
- Principle: nominally `date_cte_isolate`; structurally it prefilters three date windows and joins filtered fact tables.
- Evidence: `02_prev_winner.sql` uses `d1_dates/d2_dates/d3_dates` + filtered fact CTEs.

## Swarm Exploration Trace
- Assignment evidence: worker 2 explicitly assigned `date_cte_isolate` + `dimension_cte_isolate` + `multi_date_range_cte`.
- Reanalyze evidence: `reanalyze_parsed.json` pushes multi-stage pre-aggregation strategy.
- Worker SQL evidence: `03_swarm_best.sql` (W2) cleanly implements original month/year windows and AVG outputs.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 best at 1.0282x; W1 fails binder.
- `benchmark_iter1.json`: W5 ~1.0009x.
- `benchmark_iter2.json`: W6 = 0.3042x.
- Outcome: no branch approached reported prior headline.

## Semantic Integrity Check
- Drift risks observed in previous winner:
  - Original `d1.d_moy = 4`, `d2 between 4 and 7`; previous winner rewrites to month window starting at 9.
  - Original output uses `AVG(...)`; previous winner switches to `SUM(...)` for all three measures.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Hard semantic checks for aggregate function type (`AVG` vs `SUM`) and key temporal constants.
  - Keep date-CTE rewrites but reject candidates that alter aggregate semantics.
- Where to apply: SQL semantic equivalence validator before benchmark acceptance.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q29/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q29/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q29/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q29/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q29/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; semantic drift is explicit in SQL text.
