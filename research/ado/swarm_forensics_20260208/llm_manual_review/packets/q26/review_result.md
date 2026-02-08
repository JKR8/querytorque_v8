# Review Result: q26

## Verdict
- Primary reason swarm lost: previous SQL's speed comes from semantic drift (changed year and education predicate), not from a superior equivalent optimization.
- Secondary contributors: swarm stayed valid but only delivered ~1.05x, and iter2 final worker collapsed to 0.41x.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate selective dimensions (`date_dim`, `customer_demographics`, `promotion`) into CTEs, then join filtered fact rows and aggregate by item.
- Evidence: `02_prev_winner.sql` CTE stack (`filtered_dates`, `filtered_customer_demographics`, `filtered_promotions`, `joined_facts`).

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` includes early-filter, date/dimension isolation, and prefetch lanes.
- Reanalyze evidence: `reanalyze_parsed.json` pushes earlier fact pruning and selective-join ordering.
- Worker SQL evidence: `03_swarm_best.sql` applies the same CTE isolation pattern while preserving original literals.
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best W2 = 1.0398x.
- `benchmark_iter1.json`: W5 = 1.0520x (best swarm).
- `benchmark_iter2.json`: W6 = 0.4089x (major regression).
- Was the principle implemented correctly: yes.
- If slower, why: equivalent rewrite ceiling is modest; prior headline depends on changed filter semantics.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `e5071c9ddb6e0e90b8da4e6cc32dad56037395cfd30c8e689afc5c39e044e06f`
  - Prev checksum: `b66ce6d5072f62cdf2e6bc153f95f027705f6f9d2943ebb95f6853c3165d8b08` (fail)
  - Swarm checksum: `e5071c9ddb6e0e90b8da4e6cc32dad56037395cfd30c8e689afc5c39e044e06f` (pass)
- Validation source files/commands:
  - `packets/q26/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb` for original/prev/swarm triplet.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed:
  - Prev changes `d_year = 2001` to `d_year = 2000`.
  - Prev changes `cd_education_status = 'Unknown'` to `'College'`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: preserve current strategy, but add final-stage regression rejection (do not promote iter2-type regressions).
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection gate.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q26/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material; literal drift explains checksum mismatch directly.
