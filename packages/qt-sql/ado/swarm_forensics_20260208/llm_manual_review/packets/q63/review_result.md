# Review Result: q63

## Verdict
- Primary reason swarm lost: swarm explored the tagged prior principle (`or_to_union`) but that branch regressed badly; best swarm output reverted to conservative filtering and only reached 1.1593x.
- Secondary contributors: the previous winner SQL appears to change the target month window, so the reported 3.77x prior result is not cleanly comparable.

## Previous Winner Principle (manual SQL-derived)
- Principle: practical effect is `date_cte_isolate` + filtered fact CTE, despite metadata label `or_to_union`.
- Evidence: `02_prev_winner.sql` uses `filtered_dates` and `filtered_sales`; no UNION decomposition of the OR predicate is present.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` gives worker 4 `or_to_union` strategy.
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` shifts away from OR-splitting and recommends early aggregation/single-pass ideas.
- Worker SQL evidence: `swarm_artifacts/worker_4_sql.sql` does explicit branch split + `UNION ALL` and regresses.
- Conclusion: explored, but rejected by performance.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 (`or_to_union`) = 0.5096x; best valid is W3 = 1.0956x.
- `benchmark_iter1.json`: snipe W5 = 1.1593x (best overall swarm for q63).
- `benchmark_iter2.json`: final worker collapses to 0.0638x.
- Root performance pattern: OR-to-UNION doubled branch work and lost to simpler filtered-join plan.

## Semantic Integrity Check
- Drift risks observed:
  - `01_original.sql` targets month sequence `1181..1192`.
  - `02_prev_winner.sql` switches to `1200..1211`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add literal-window guardrails (month/date constants must remain aligned unless explicitly justified).
  - Keep OR-split as an exploratory lane only; down-rank when branch duplication increases fact-table work.
- Where to apply: semantic checker + candidate-ranking heuristics.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/swarm_artifacts/worker_4_sql.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q63/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - Prior label/SQL mismatch (`or_to_union` tag vs no union in previous SQL) suggests metadata quality issues.
