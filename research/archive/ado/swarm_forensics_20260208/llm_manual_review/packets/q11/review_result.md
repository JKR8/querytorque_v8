# Review Result: q11

## Verdict
- Primary reason swarm lost: swarm explored the right structural direction (split by channel/year and avoid original 4-way self-join), but implemented it with heavier physical plans (repeated customer joins and wide grouping keys), so it never matched previous efficiency.
- Secondary contributors: final synthesis rewrite introduced a severe row-explosion pattern and collapsed to 0.091x.

## Previous Winner Principle (manual SQL-derived)
- Principle: pre-aggregate store/web sales by **customer surrogate key** and year in narrow CTEs, then perform ratio joins, then join to customer dimension once for final projection.
- Evidence: `02_prev_winner.sql` uses `store_sales_2001/2002` and `web_sales_2001/2002` grouped by `ss_customer_sk` / `ws_bill_customer_sk`, and only joins `customer` in final select.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` gives worker 3 aggressive multi-CTE restructure (`multi_date_range_cte`, `multi_dimension_prefetch`, `prefetch_fact_join`).
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` identifies self-join explosion and suggests single-pass alternatives.
- Worker SQL evidence:
  - `03_swarm_best.sql` (W3) splits by year/channel, but joins `customer` inside each CTE and groups by textual identity fields (`customer_id`, names, country).
  - `final_worker_sql.sql` attempts single-pass conditional aggregation with simultaneous LEFT joins to store/web/date, creating a high-cost plan (iter2: 0.091x).
- Conclusion: explored, but only partially matched the winning implementation principle.

## Performance/Validity Outcome
- What happened in benchmark iterations:
  - `benchmark_iter0.json`: best valid is W3 at 1.2689x.
  - `benchmark_iter1.json`: W5 at 1.2436x.
  - `benchmark_iter2.json`: W6 at 0.0910x.
- Was the principle implemented correctly: directionally yes, physically suboptimal.
- If slower, why:
  - repeated dimension joins in each aggregate CTE;
  - wide group keys (string attributes) instead of surrogate key aggregation;
  - late projection pushdown not preserved.

## Semantic Integrity Check
- Drift risks observed:
  - `03_swarm_best.sql` appears semantically faithful to `01_original.sql` (same years/channels/ratio logic).
  - `final_worker_sql.sql` is semantically risky due multi-fact LEFT join multiplication before aggregation, even though benchmark says `rows_match=true`.
- Risk severity: medium.

## Minimal Fix for Swarm
- Tactical change needed:
  - Enforce a key-first aggregation template for q11-like queries:
    1. aggregate facts by `{customer_sk, year, channel}` without customer text columns,
    2. apply positivity/ratio filters on compact key tables,
    3. join `customer` once at the end.
  - Add anti-pattern guard: reject rewrites that LEFT-join multiple fact tables prior to aggregation on the same grain.
- Where to apply: fan-out strategy constraints + synthesis-time lint checks.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/reanalyze_parsed.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/benchmark_iter2.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q11/swarm_artifacts/final_worker_sql.sql`

## Confidence
- Confidence: high
- Uncertainties:
  - Historical previous speedup is from a different baseline context (`prev_source=Evo`); relative method conclusions are strong, absolute speedup deltas are less stable.
