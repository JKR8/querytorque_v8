# Review Result: q21

## Verdict
- Primary reason swarm lost: swarm implemented the expected `date_cte_isolate` rewrite and got 1.1274x, but previous winner score is inflated by a different anchor date/window.
- Secondary contributors: swarm branches were conservative and close together; no branch introduced a step-change plan.

## Previous Winner Principle (manual SQL-derived)
- Principle: date/item prefilter CTEs + inventory aggregation with before/after conditional sums.
- Evidence: `02_prev_winner.sql` defines `filtered_dates`, `filtered_items`, `joined_facts`, then aggregated before/after quantities.

## Swarm Exploration Trace
- Assignment evidence: workers assigned early filter/date isolation and prefetch variants.
- Reanalyze evidence: `reanalyze_parsed.json` emphasizes two-phase aggregation and early pruning.
- Worker SQL evidence: `03_swarm_best.sql` preserves original structure with filtered date/item CTEs and aggregated ratio filter.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best 1.0532x.
- `benchmark_iter1.json`: W5 = 1.1274x (best swarm).
- `benchmark_iter2.json`: W6 = 1.0305x.
- Outcome: modest improvement, no major break-through.

## Semantic Integrity Check
- Drift risks observed in previous winner:
  - Original anchor date is `2002-02-27` with ±30-day window.
  - Previous winner rewrites to `2000-03-11` and corresponding 2000 date range.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Enforce immutable anchor-date checks in temporal inventory ratio queries.
  - Retain current swarm pattern; gains are reasonable for semantics-preserving rewrite.
- Where to apply: semantic equivalence checker for literal time anchors.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q21/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q21/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q21/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q21/swarm_artifacts/benchmark_iter1.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; semantic drift in prior SQL is explicit.
