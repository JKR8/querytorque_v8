# Review Result: q9

## Verdict
- Primary reason swarm lost: swarm did explore the core structural idea (collapse repeated scalar subqueries into one aggregate pass), but every valid implementation was materially slower on DuckDB for this query shape, and the final synthesis worker failed at runtime.
- Secondary contributors: the previous winner SQL appears to change core business semantics (measures and thresholds), so its 4.47x result is not clearly comparable to a semantics-preserving rewrite.

## Previous Winner Principle (manual SQL-derived)
- Principle: single-pass conditional aggregation in a pre-aggregated CTE replacing many repeated scalar subqueries.
- Evidence: `02_prev_winner.sql` uses one `store_sales_aggregates` CTE with `SUM/AVG(CASE WHEN ...)` buckets and then emits `bucket1..bucket5` from that single row.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` explicitly assigns `single_pass_aggregation` to worker 1.
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` recommends `single_pass_aggregation` + `union_cte_split`.
- Worker SQL evidence: `03_swarm_best.sql` and `swarm_artifacts/worker_2_sql.sql`/`worker_4_sql.sql` all implement one-pass conditional aggregation variants.
- Conclusion: explored (strongly).

## Performance/Validity Outcome
- What happened in benchmark iterations:
  - `benchmark_iter0.json`: best valid worker speedup 0.4245x (all W1-W4 valid but all slower).
  - `benchmark_iter1.json`: W5 speedup 0.4098x (still slower).
  - `benchmark_iter2.json`: W6 failed with error `unrecognized configuration parameter "enable_verification"`.
- Was the principle implemented correctly: yes, structurally yes; but it still regressed on this engine/workload.
- If slower, why: likely high per-row cost of evaluating many conditional aggregates in a single scan outweighed benefits here, while the original multiple subqueries may have been better optimized by DuckDB for this pattern.

## Semantic Integrity Check
- Drift risks observed:
  - `02_prev_winner.sql` changes threshold literals (e.g., `74129`, `122840`, ...) from original values in `01_original.sql` (e.g., `2972190`, `4505785`, ...).
  - `02_prev_winner.sql` swaps measures from `ss_ext_sales_price`/`ss_net_profit` to `ss_ext_discount_amt`/`ss_net_paid`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add hard semantic guardrails that block candidate SQL if threshold literals or measure columns deviate from source query intent.
  - Keep two explicit strategy lanes for q9-like shapes: (1) one-pass conditional aggregation, (2) independent range subqueries with explicit materialization; let benchmark select.
- Where to apply: fan-out constraints + semantic validator + final synthesis guardrails.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/reanalyze_parsed.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/benchmark_iter2.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q9/swarm_artifacts/final_worker_sql.sql`

## Confidence
- Confidence: medium-high
- Uncertainties:
  - The benchmark harness reports `rows_match=true`, but without inspecting validator internals we cannot prove whether it checks full value equivalence vs only row-count/shape in this path.
