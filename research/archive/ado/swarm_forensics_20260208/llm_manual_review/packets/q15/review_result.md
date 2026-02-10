# Review Result: q15

## Verdict
- Primary reason swarm appears to lose: scorecard mixes baselines from different runs; on current baseline swarm is actually faster in absolute time.
- Secondary contributors: strict schema identity fails for swarm due output alias casing (`SUM(...)` vs `sum(...)`), despite value checksum match.

## Previous Winner Principle (manual SQL-derived)
- Principle: OR decomposition into `UNION ALL` branches (zip branch, state branch, high-price branch), then regroup.
- Evidence: `02_prev_winner.sql` has three branch CTE union over shared joins and date filter.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` includes structural transform lane (`or_to_union`) and early-filter lanes.
- Reanalyze evidence: none present for this packet (no `reanalyze_parsed.json`).
- Worker SQL evidence: `03_swarm_best.sql` uses two-branch decomposition with `address_qualifies` guard to avoid duplicate rows across OR branches.
- Conclusion: explored and refined.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 = 1.2835x.
- `benchmark_iter1.json`: W5 = 2.1307x (best swarm, rows_match=true).
- Stored prev headline: 3.17x from older baseline (`original_ms=150.38`, `optimized_ms=47.44` in context metadata).
- Current-batch absolute timing: swarm `43.24ms` vs prev `47.44ms`; swarm is faster in raw runtime.
- Was the principle implemented correctly: yes.
- If slower, why: not actually slower in absolute runtime; comparison artifact is denominator drift across benchmark epochs.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: **fail** (`sum(cs_sales_price)` vs `SUM(cs_sales_price)` column label text).
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `2bfdac367534752d14addb7112478da2f65c076772f0cd4d8b56d155b7f62946`
  - Prev checksum: `2bfdac367534752d14addb7112478da2f65c076772f0cd4d8b56d155b7f62946` (pass)
  - Swarm checksum: `2bfdac367534752d14addb7112478da2f65c076772f0cd4d8b56d155b7f62946` (value pass)
- Validation source files/commands:
  - `packets/q15/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: fail (strict schema identity not met by swarm)
- If blocked, exact blocker: N/A.

## Semantic Integrity Check
- Drift risks observed: no value-level drift detected for prev or swarm; both match original value checksum.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: enforce deterministic output alias normalization to exact original label case/text for strict schema parity.
- Where to apply (fan-out, assignments, reanalyze, final selection): final SQL post-processor before validation.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q15/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q15/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q15/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q15/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q15/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material beyond policy choice on case-sensitive alias equivalence.
