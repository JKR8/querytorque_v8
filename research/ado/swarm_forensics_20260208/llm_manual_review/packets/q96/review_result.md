# Review Result: q96

## Verdict
- Primary reason swarm did not beat prior headline: previous SQL speed comes from changed filter literals (`t_hour`, `hd_dep_count`) rather than equivalent optimization.
- Secondary contributors: swarm delivered only marginal gain (~1.00x) and iter2 regressed sharply.

## Previous Winner Principle (manual SQL-derived)
- Principle: early selective dimension CTEs (`time`, `household_demographics`, `store`) joined into filtered fact count.
- Evidence: `02_prev_winner.sql` builds `filtered_time`, `filtered_hd`, `filtered_store`, then counts `filtered_sales`.

## Swarm Exploration Trace
- Assignment evidence: workers assigned early filter, date/dimension isolation, prefetch, and structural transforms.
- Reanalyze evidence: `reanalyze_parsed.json` suggests composite key precomputation to minimize repeated joins.
- Worker SQL evidence: `03_swarm_best.sql` implements early filtered CTEs and joins, preserving original literals.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 = 1.0041x (best swarm).
- `benchmark_iter1.json`: W5 = 0.9876x.
- `benchmark_iter2.json`: W6 = 0.1787x.
- Was the principle implemented correctly: yes.
- If slower, why: limited headroom for equivalent rewrite; prior comparator is semantically drifted.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 1 vs 1 (pass).
  - Original vs swarm: 1 vs 1 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `c2585399b68978a9c87b87abb60e6ed4ca2cd290a2dc26a69e4a8a9e9a56fc29`
  - Prev checksum: `1aee1c5296b02d5e4344941093f04f4925ad3d00ffb4ef05ddd1356d72cf7e5f` (fail)
  - Swarm checksum: `c2585399b68978a9c87b87abb60e6ed4ca2cd290a2dc26a69e4a8a9e9a56fc29` (pass)
- Validation source files/commands:
  - `packets/q96/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - `t_hour = 8` changed to `t_hour = 20`.
  - `hd_dep_count = 3` changed to `hd_dep_count = 7`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: keep current equivalent plan but hard-reject historical comparators that alter high-selectivity literal predicates.
- Where to apply (fan-out, assignments, reanalyze, final selection): semantic gate + comparator curation.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q96/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
