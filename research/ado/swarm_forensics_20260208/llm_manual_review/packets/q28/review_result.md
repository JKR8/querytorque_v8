# Review Result: q28

## Verdict
- Primary reason swarm did not beat previous effort: previous SQL preserves the original six-independent-branch aggregate layout, while swarm collapsed it into a single CASE-bucket pass that is slower on this shape.
- Secondary contributors: no semantic issue; this is a pure performance tradeoff between two valid plans.

## Previous Winner Principle (manual SQL-derived)
- Principle: keep six separate filtered aggregate subqueries (`B1`..`B6`) and cross-combine their scalar outputs in one row.
- Evidence: `02_prev_winner.sql` is the six-branch scalar-subquery template structure.

## Swarm Exploration Trace
- Assignment evidence: worker lanes included pushdown/materialization/single-pass and structural transforms.
- Reanalyze evidence: `reanalyze_parsed.json` explicitly pushed a single-pass bucketization strategy.
- Worker SQL evidence: `03_swarm_best.sql` implements one filtered scan with CASE bucket + grouped aggregates + pivot via MAX(CASE...).
- Conclusion: did **not** preserve prior winning principle; explored an alternate strategy.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W1 = 1.0520x (best swarm).
- `benchmark_iter1.json`: W5 = 0.6639x.
- `benchmark_iter2.json`: W6 = 0.5955x.
- Was the principle implemented correctly: swarm implemented the alternate single-pass plan correctly.
- If slower, why: CASE bucketization + pivoting increased compute overhead versus independent branch aggregates on DuckDB.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 1 vs 1 (pass).
  - Original vs swarm: 1 vs 1 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `3cdc5af2e0ef758bfa43e6068df6a63f16ad67ddd70dd424631915059da0a8a7`
  - Prev checksum: `3cdc5af2e0ef758bfa43e6068df6a63f16ad67ddd70dd424631915059da0a8a7` (pass)
  - Swarm checksum: `3cdc5af2e0ef758bfa43e6068df6a63f16ad67ddd70dd424631915059da0a8a7` (pass)
- Validation source files/commands:
  - `packets/q28/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass

## Semantic Integrity Check
- Drift risks observed: none; prev and swarm are both strict-equivalent to original.
- Risk severity: low.

## Minimal Fix for Swarm
- Tactical change needed: add plan-shape selection logic for this query family: keep independent branch aggregates when objective is raw runtime, do not force single-pass CASE pivot.
- Where to apply (fan-out, assignments, reanalyze, final selection): assignment strategy + final selection heuristic.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q28/validation_strict.json`

## Confidence
- Confidence: medium
- Uncertainties:
  - Absolute prev headline advantage may include historical baseline variance, but plan-shape gap is still visible in current batch.
