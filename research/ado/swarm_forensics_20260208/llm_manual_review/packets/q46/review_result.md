# Review Result: q46

## Verdict
- Primary reason swarm lost: swarm mostly produced semantics-preserving refactors with tiny gains (~1.0x), while the previous winner appears to win largely via semantic drift (different filter constants).
- Secondary contributors: intended novel `or_to_union` path was not meaningfully realized in the winning swarm branch; final synthesis failed with invalid PRAGMA.

## Previous Winner Principle (manual SQL-derived)
- Principle: nominally tagged `or_to_union`, but actual SQL is mainly dimension/date CTE isolation and join cleanup.
- Evidence: `02_prev_winner.sql` has no OR split into UNION branches.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` assigns worker 4 to `or_to_union`.
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` explicitly calls for OR-to-UNION but notes it was not actually delivered.
- Worker SQL evidence: best swarm SQL (`03_swarm_best.sql`, W5) is conservative filtered-CTE rewrite, not OR split.
- Conclusion: partially explored; effective branch selection stayed conservative.

## Performance/Validity Outcome
- `benchmark_iter0.json`: best 1.0097x.
- `benchmark_iter1.json`: best 1.0187x (W5, selected).
- `benchmark_iter2.json`: W6 errors (`force_parallelism` parameter unsupported).
- Outcome: swarm improvements are real but small; no branch approaches reported prior 3.23x.

## Semantic Integrity Check
- Drift risks observed in previous winner:
  - Store-city list changed from five cities in original to two cities in `02_prev_winner.sql`.
  - Household-demographic predicates changed from `(dep=6 OR vehicle=0)` to `(dep=4 OR vehicle=3)`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add strict constant-preservation checks for categorical filters and predicate literals.
  - Block unsupported engine pragmas during synthesis.
  - Keep OR-split attempts, but require demonstrated speedup over conservative baseline before promotion.
- Where to apply: semantic validator + final-worker SQL linting.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/swarm_artifacts/reanalyze_parsed.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q46/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - Historical prior speedup comes from different baseline context; direct speedup comparison is unstable.
