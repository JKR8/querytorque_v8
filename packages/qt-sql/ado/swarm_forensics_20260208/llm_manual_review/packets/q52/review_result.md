# Review Result: q52

## Verdict
- Primary reason swarm lost: swarm implemented the same general date/dimension isolation principle as previous runs, but only achieved small gains; reported prior edge is largely tied to non-comparable prior SQL constants.
- Secondary contributors: later swarm iterations degraded after the initial modest gain.

## Previous Winner Principle (manual SQL-derived)
- Principle: `date_cte_isolate` + `dimension_cte_isolate` (date/item prefilter then join fact).
- Evidence: `02_prev_winner.sql` uses `filtered_dates` and `filtered_items` CTEs before joining `store_sales`.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` includes date/dimension isolation for worker 2.
- Reanalyze evidence: `reanalyze_parsed.json` continues prefilter-first strategy.
- Worker SQL evidence: `03_swarm_best.sql` is the same structural pattern with correct `d_moy=12`, `d_year=2002`.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W2 = 1.0271x.
- `benchmark_iter1.json`: W5 = 1.0720x (best overall swarm).
- `benchmark_iter2.json`: W6 = 0.9391x.
- Outcome: small improvement ceiling for this query under current strategy.

## Semantic Integrity Check
- Drift risks observed in previous winner:
  - Original filters: `d_year=2002`, `d_moy=12`.
  - Previous winner switches to `d_year=2000`, `d_moy=11`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Enforce immutable date-constant checks for simple time-slice aggregations.
  - Add early stopping when best iteration is within a narrow band and later rewrites degrade.
- Where to apply: semantic guardrail + iteration controller.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q52/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None beyond historical-baseline comparability.
