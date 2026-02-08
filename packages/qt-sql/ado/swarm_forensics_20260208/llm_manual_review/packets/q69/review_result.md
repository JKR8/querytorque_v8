# Review Result: q69

## Verdict
- Primary reason swarm lost: prior SQL's apparent advantage is largely from semantic drift (time window and state set changes), while swarm stayed equivalent.
- Secondary contributors: swarm peaked early at ~1.04x and then regressed in iter1/iter2.

## Previous Winner Principle (manual SQL-derived)
- Principle: decorrelate `EXISTS/NOT EXISTS` into precomputed distinct customer sets per channel using shared date CTE.
- Evidence: `02_prev_winner.sql` builds `filtered_store_sales`, `filtered_web_sales`, `filtered_catalog_sales` and applies existence checks.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` W3 assigned `multi_date_range_cte|composite_decorrelate_union|prefetch_fact_join`.
- Reanalyze evidence: `reanalyze_parsed.json` explicitly recommends early restricted customer sets and channel-wise consolidation.
- Worker SQL evidence: `03_swarm_best.sql` precomputes `store_customers/web_customers/catalog_customers` and uses left anti-join null checks.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W3 = 1.0431x (best swarm).
- `benchmark_iter1.json`: W5 = 0.9290x.
- `benchmark_iter2.json`: W6 = 0.7102x.
- Was the principle implemented correctly: yes.
- If slower, why: equivalent plan gains were small; previous gain relies on shifted predicates.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `523f389cd44868bf7e6f48041ade6afd4efddf5325420504612b2998c3ee9dc8`
  - Prev checksum: `29e5e5f931cbc0329be8b385aa2ec9d7bf6378d87d6ed1298ed478e4fdb6933c` (fail)
  - Swarm checksum: `523f389cd44868bf7e6f48041ade6afd4efddf5325420504612b2998c3ee9dc8` (pass)
- Validation source files/commands:
  - `packets/q69/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed:
  - Prev changes date filter from `d_year=2000, d_moy 1..3` to `d_year=2001, d_moy 4..6`.
  - Prev changes states from `TX/VA/MI` to `KY/GA/NM`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: preserve current decorrelation path but tighten reanalyze-to-final consistency so later regressions are not promoted.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/swarm_artifacts/assignments.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q69/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; mismatch is directly attributable to changed literals.
