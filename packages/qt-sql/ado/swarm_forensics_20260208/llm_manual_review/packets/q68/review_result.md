# Review Result: q68

## Verdict
- Primary reason swarm did not beat prior headline: prior SQL rewrites changed years/cities/demographic predicates, so speedup is from non-equivalent filtering.
- Secondary contributors: swarm improvements were moderate (~1.17x) and did not match prior historical denominator.

## Previous Winner Principle (manual SQL-derived)
- Principle: split OR demographic condition into branch unions (`hd_dep_count` branch + `hd_vehicle_count` branch), with prefiltered dates/stores.
- Evidence: `02_prev_winner.sql` contains `branch1_sales`, `branch2_sales`, `union_sales`, then grouped join.

## Swarm Exploration Trace
- Assignment evidence: W4 had structural/decorrelation lane; W2/W3 had date/dimension/prefetch lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends early customer/address filtering and exists-based mismatch checks.
- Worker SQL evidence: `03_swarm_best.sql` uses filtered date/store/hd CTEs and single aggregated pass while preserving original literals.
- Conclusion: explored and implemented (without adopting prev’s literal-drifted branch split).

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.1065x.
- `benchmark_iter1.json`: W5 = 1.1716x (best swarm).
- `benchmark_iter2.json`: W6 = 1.0196x.
- Was the principle implemented correctly: yes for equivalent strategy.
- If slower, why: equivalent strategy has lower headroom; prior comparator is semantically drifted.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `468895509fa5e143087cc36247e5ebd3a2e80079af45c3f60d32b0f79e9fa769`
  - Prev checksum: `566d2828cc0da40a489458e0b58f8cd54013bf92ab5a77d74103382d0cb30b6c` (fail)
  - Swarm checksum: `468895509fa5e143087cc36247e5ebd3a2e80079af45c3f60d32b0f79e9fa769` (pass)
- Validation source files/commands:
  - `packets/q68/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - Years changed from `(1998,1999,2000)` to `(1999,2000,2001)`.
  - Store city set and household conditions changed.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: retain equivalent rewrite path; add hard literal-lock for year/city predicate families during comparator acceptance.
- Where to apply (fan-out, assignments, reanalyze, final selection): semantic gate/comparator ingestion.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q68/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
