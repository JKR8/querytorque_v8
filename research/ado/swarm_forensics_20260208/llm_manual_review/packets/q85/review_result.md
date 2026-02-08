# Review Result: q85

## Verdict
- Primary reason swarm lost: previous headline comes from non-equivalent SQL and a different baseline era; swarm equivalent candidate is near parity but not faster.
- Secondary contributors: large instability in swarm fan-out (3 binder failures, 1 wrong-result worker in iter0) reduced effective search quality.

## Previous Winner Principle (manual SQL-derived)
- Principle: prefilter addresses and rewrite complex OR blocks under an explicit join graph.
- Evidence: `02_prev_winner.sql` introduces `filtered_dates` and `filtered_addresses`, then applies demographic and address/profit OR predicates.

## Swarm Exploration Trace
- Assignment evidence: `assignments.json` spans early filter/date isolation/prefetch/structural lanes.
- Reanalyze evidence: `reanalyze_parsed.json` proposes staged fact filtering with computed predicate flags.
- Worker SQL evidence: `03_swarm_best.sql` exactly implements computed boolean flags (`demog_price_ok`, `addr_profit_ok`) over joined facts.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W1/W2/W3 compile errors (missing `ca_state`), W4 wrong results (row_count=4 vs baseline 13).
- `benchmark_iter1.json`: W5 pass but 0.2805x.
- `benchmark_iter2.json`: W6 pass at 1.0075x (best swarm).
- Was the principle implemented correctly: final worker yes; earlier worker quality was poor.
- If slower, why: valid implementation was conservative; prior score is inflated by semantic drift and historical denominator.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: fail.
  - Original vs swarm: **fail** (first column label text differs: `substr(...)` vs `substring(...)` rendering).
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 13 vs 15 (fail).
  - Original vs swarm: 13 vs 13 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `9ca946b5c0543722c4fa24f2ed3e2eb0438a736be2e4e5568d445f12f9a3c02c`
  - Prev checksum: `7f504d9dabbaf72080a9275278d3b9691ddf0d9ee181eba2ca9244325b7ee246` (fail)
  - Swarm checksum: `9ca946b5c0543722c4fa24f2ed3e2eb0438a736be2e4e5568d445f12f9a3c02c` (value pass)
- Validation source files/commands:
  - `packets/q85/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: fail (strict schema identity not met by swarm; prev also fails rowcount/checksum)

## Semantic Integrity Check
- Drift risks observed:
  - Prev changes demographic literals (`4 yr Degree` -> `Advanced Degree`, `Secondary` -> `College`, etc.).
  - Prev changes state sets substantially.
  - Prev output rowcount diverges from original.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add strict column-label normalization to match original output signature.
  - Add compile-time lint for alias provenance (`ca_state`) before benchmark.
- Where to apply (fan-out, assignments, reanalyze, final selection): fan-out SQL lint + final post-processing.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q85/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; rowcount/checksum evidence is conclusive.
