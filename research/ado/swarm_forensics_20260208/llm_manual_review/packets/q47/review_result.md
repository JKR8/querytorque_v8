# Review Result: q47

## Verdict
- Primary reason swarm lost: stored previous score is inflated by non-equivalent SQL (year-window mutation + output schema expansion), while swarm stayed semantically aligned.
- Secondary contributors: swarm did not stabilize beyond ~1.07x; iter2 final worker regressed badly (0.18x), so no late-stage breakthrough.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate date filter in CTE, pre-aggregate monthly sales, then compute adjacent-month comparisons (originally via self-join around rank).
- Evidence: `02_prev_winner.sql` uses `filtered_dates`, `v1`, and `v2` with lag/lead-style adjacency logic.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` W4 explicitly targeted `or_to_union|intersect_to_exists|composite_decorrelate_union`; W3 targeted prefetch/deferred-window patterns.
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` calls out "single-pass aggregation" and explicit `LAG/LEAD` as the missed lever.
- Worker SQL evidence: `03_swarm_best.sql` implements `filtered_dates` + `LAG/LEAD` with original 2001/2000-12/2002-1 date intent.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W2 = 1.0447x (rows_match=true).
- `benchmark_iter1.json`: W5 = 1.0737x (best swarm, rows_match=true).
- `benchmark_iter2.json`: W6 = 0.1806x (severe regression).
- Was principle implemented correctly: yes (swarm aligns with intended structure).
- If slower, why: valid rewrites delivered incremental gains only; prior "winner" gained from changed semantics and non-comparable baseline provenance.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: **fail** (6 cols vs 10 cols; prev emits extra `i_category/i_brand/s_company_name/d_moy`).
  - Original vs swarm: **pass**.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `9d5202c21d6021a247cc59f5e66f0708bc4792ca0b2df678ca1a778a0e2b3dce`
  - Prev checksum: `d9d99dc54b99f59134204adc215fa371d278ffdb70b214e4e2d0c8ad0366e00b` (fail)
  - Swarm checksum: `9d5202c21d6021a247cc59f5e66f0708bc4792ca0b2df678ca1a778a0e2b3dce` (pass)
- Validation source files/commands:
  - `packets/q47/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed:
  - Prev changes temporal anchor from 2001-centered window to 1999-centered window.
  - Prev changes output schema shape (returns 10 columns instead of 6).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: keep current semantics-preserving `LAG/LEAD` strategy, but add stronger late-stage plan guardrails to prevent iter2-style regressions from being promoted as finalists.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection + regression gate.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/swarm_artifacts/assignments.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q47/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; strict validation clearly separates prev drift from swarm-equivalent output.
