# Review Result: q43

## Verdict
- Primary reason swarm lost: swarm explored the same principle and improved materially (final W6 = 1.3239x), but did not match previous efficiency in this comparison run.
- Secondary contributors: swarm final rewrite adds an extra pre-aggregation/pivot stage that likely introduces extra grouping/sort overhead relative to the simpler prior rewrite.

## Previous Winner Principle (manual SQL-derived)
- Principle: early date/store filtering then single-pass conditional day-of-week aggregation.
- Evidence: `02_prev_winner.sql` defines `filtered_dates` and `filtered_stores`, then directly computes 7 conditional SUMs.

## Swarm Exploration Trace
- Assignment evidence: workers assigned `early_filter`/`date_cte_isolate`/`single_pass_aggregation` variants.
- Reanalyze evidence: `reanalyze_parsed.json` targets fact pre-aggregation then pivot.
- Worker SQL evidence: `03_swarm_best.sql` (final worker) does two-stage approach (`pre_aggregated` then `pivoted`) and joins store late.
- Conclusion: explored and implemented; chosen variant is heavier than prior best.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best 1.0474x.
- `benchmark_iter1.json`: 1.0211x.
- `benchmark_iter2.json`: final worker reaches 1.3239x (best swarm).
- Outcome: swarm clearly improves but remains below prior score in this table.

## Semantic Integrity Check
- Drift risks observed:
  - `02_prev_winner.sql` appears semantically faithful to `01_original.sql` (same year, offset filter, weekday pivot fields).
  - `03_swarm_best.sql` also appears semantically faithful.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed:
  - Prefer lower-overhead direct conditional aggregation pattern for this query family; only use pre-aggregation/pivot when cardinality reduction is proven.
  - Add plan-cost heuristic penalizing extra aggregation stages when predicates are already highly selective.
- Where to apply: candidate ranking heuristics.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q43/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q43/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q43/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q43/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: medium-high
- Uncertainties:
  - Exact operator-level slowdown source (extra group-by vs join placement) would require explain-plan comparison.
