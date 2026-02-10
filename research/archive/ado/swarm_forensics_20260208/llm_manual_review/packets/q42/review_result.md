# Review Result: q42

## Verdict
- Primary reason swarm lost: both sides use near-identical `date_cte_isolate` pattern, but the previous winner appears non-comparable due a changed target year.
- Secondary contributors: swarm produced only incremental plan improvements (best 1.0251x), so it could not overcome the reported prior headline.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate filtered date and item dimensions before joining to `store_sales`.
- Evidence: `02_prev_winner.sql` uses `filtered_dates` + `filtered_items` CTEs and then fact join.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` gives date/dimension isolation strategies (W1/W2/W3).
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` continues with prefilter + partial aggregation framing.
- Worker SQL evidence: `03_swarm_best.sql` directly implements filtered date/item CTE pattern with correct 2002 target.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W3 = 1.0251x.
- `benchmark_iter1.json`: W5 = 0.9744x.
- `benchmark_iter2.json`: W6 = 0.9460x.
- Outcome: only small gain from the best branch; later iterations degraded.

## Semantic Integrity Check
- Drift risks observed:
  - Original query targets `d_year = 2002` and `d_moy = 11`.
  - `02_prev_winner.sql` changes year to `2000`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add immutable-time-filter checks for simple slice queries (year/month constants must match source).
  - For this query class, keep rewrite minimal and avoid iterative over-engineering after first gain.
- Where to apply: semantic checker + iteration-stopping policy.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q42/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material beyond baseline/version comparability.
