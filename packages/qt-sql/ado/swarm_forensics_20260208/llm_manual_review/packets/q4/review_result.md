# Review Result: q4

## Verdict
- Primary reason swarm appears to lose in `swarm_comparison`: same comparability problem as q1. On the current baseline, swarm final worker outperforms the previous SQL.
- Secondary contributors: swarm spent early iterations on less effective variants; strongest result appears only in iter2.

## Previous Winner Principle (manual SQL-derived)
- Principle: avoid 6-way self-join explosion by channel/year pre-aggregation and customer-level pivot/ratio filtering.
- Evidence: `02_prev_winner.sql` computes channel/year totals then pivots into `s_1999/s_2000/c_1999/...` before final ratio predicates.

## Swarm Exploration Trace
- Assignment evidence: workers assigned date isolation, union splitting, multi-CTE prefetch, and single-pass transform patterns.
- Reanalyze evidence: `reanalyze_parsed.json` correctly identifies the self-join explosion bottleneck and recommends channel-wise pre-aggregation.
- Worker SQL evidence: `03_swarm_best.sql` (final worker) implements channel-specific conditional aggregates and direct ratio joins.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W1 = 1.5353x.
- `benchmark_iter1.json`: W5 = 1.2354x.
- `benchmark_iter2.json`: W6 = 1.6144x (best swarm).
- Current-baseline comparison:
  - recomputed prior = `4964.48 / 3508.19 = 1.415x`.
  - swarm best = `1.6144x`.
- Interpretation: swarm is stronger on current baseline despite stored delta showing `prev`.

## Semantic Integrity Check
- Drift risks observed:
  - `02_prev_winner.sql` and `03_swarm_best.sql` both appear semantically aligned with original year/channel ratio intent.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed:
  - Report dual metrics for prior entries: historical speedup and same-baseline recomputed speedup.
  - Keep existing q4 strategy family; it already solves core structural bottleneck.
- Where to apply: leaderboard/comparison pipeline, not query-generation prompting.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q4/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q4/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q4/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q4/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material beyond baseline-normalized ranking policy.
