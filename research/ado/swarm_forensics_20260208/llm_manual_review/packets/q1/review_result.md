# Review Result: q1

## Verdict
- Primary reason swarm appears to lose in `swarm_comparison`: score comparability issue. On the current baseline used for swarm runs, swarm W3 is actually faster than the previous SQL.
- Secondary contributors: previous result was recorded against a different baseline context, so stored `2.92x` overstates gap versus current-run swarm.

## Previous Winner Principle (manual SQL-derived)
- Principle: decorrelate store-level threshold computation + push `s_state='SD'` filter into the fact prefilter path.
- Evidence: `02_prev_winner.sql` computes `store_avg_return` CTE and joins it, replacing correlated subquery.

## Swarm Exploration Trace
- Assignment evidence: worker 4 assigned explicit `decorrelate`; workers 2/3 assigned strong prefilter/prefetch patterns.
- Reanalyze evidence: `reanalyze_parsed.json` discusses decorrelation and store-average materialization concerns.
- Worker SQL evidence: `03_swarm_best.sql` computes store average via window over grouped returns and applies threshold filter.
- Conclusion: explored and implemented effectively.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W3 = 1.6196x (best).
- `benchmark_iter1.json`: W5 = 1.0501x.
- `benchmark_iter2.json`: W6 = 0.5847x.
- Current-baseline comparison:
  - recomputed prior = `baseline_ms / prev_ms = 107.35 / 81.94 = 1.31x`.
  - swarm best = `1.6196x`.
- Interpretation: swarm beat prior SQL on this baseline, but not the stored historical headline metric.

## Semantic Integrity Check
- Drift risks observed:
  - `02_prev_winner.sql` appears semantically faithful to `01_original.sql` (same year/state logic), with decorrelation/refactor only.
  - `03_swarm_best.sql` also appears semantically faithful.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed:
  - Normalize leaderboard comparison to same-baseline recomputation for all prior entries.
  - Distinguish `historical_speedup` from `current_baseline_speedup` in reports.
- Where to apply: comparison/leaderboard generation logic.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q1/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q1/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q1/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q1/swarm_artifacts/benchmark_iter0.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material beyond cross-run baseline normalization.
