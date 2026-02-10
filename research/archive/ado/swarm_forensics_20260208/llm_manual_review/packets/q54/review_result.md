# Review Result: q54

## Verdict
- Primary reason swarm lost: previous effort shifts anchor month and segment expression, so score advantage is not from equivalent optimization.
- Secondary contributors: swarm had worker instability in iter0 (2 binder errors), limiting breadth of validated candidates.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate customer-month date set, derive month-seq window for revenue dates, then aggregate/rebucket customer revenue.
- Evidence: `02_prev_winner.sql` uses `filtered_dates_customers`, `date_range_anchor`, `filtered_dates_revenue`, `my_customers`, `my_revenue`, `segments`.

## Swarm Exploration Trace
- Assignment evidence: W2 lane explicitly targets `date_cte_isolate|dimension_cte_isolate|shared_dimension_multi_channel`.
- Reanalyze evidence: `reanalyze_parsed.json` suggests single-pass customer qualification + shared date range.
- Worker SQL evidence: `03_swarm_best.sql` implements May-1998 anchor CTE and month-seq range expansion with shared dimensions.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.0496x (best swarm); W1/W3 compile errors.
- `benchmark_iter1.json`: W5 = 0.9307x.
- `benchmark_iter2.json`: W6 = 1.0113x.
- Was the principle implemented correctly: yes.
- If slower, why: equivalent rewrites are near 1.0-1.05x; prior headline depends on changed month anchor.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: fail (`segment` label changed to `SEGMENT`).
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 0 vs 1 (fail).
  - Original vs swarm: 0 vs 0 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
  - Prev checksum: `3f9dba6199bb94e5c7f482f83749f95b099887f6967b064ec6f7eb8feb1485e5` (fail)
  - Swarm checksum: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855` (pass)
- Validation source files/commands:
  - `packets/q54/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Customer anchor month changed from May 1998 to December 1998.
  - Segment expression changed to `ROUND(revenue/50)` before cast.
  - Rowcount diverges (0 vs 1).
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: preserve current valid approach; add compile pre-check for ambiguous column references seen in iter0 failures.
- Where to apply (fan-out, assignments, reanalyze, final selection): fan-out linting.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q54/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
