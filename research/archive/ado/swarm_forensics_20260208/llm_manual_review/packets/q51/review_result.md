# Review Result: q51

## Verdict
- Primary reason swarm did not beat prior effort: the prior winning SQL uses an aggressive semantic shortcut (inner join replacing full outer + window simplification), while swarm used a different rewrite that also failed strict equivalence.
- Secondary contributors: both prev and swarm fail checksum parity; neither should be accepted under strict policy.

## Previous Winner Principle (manual SQL-derived)
- Principle: collapse two-stage cumulative reconstruction into direct cumulative fields and replace `FULL OUTER JOIN` with `INNER JOIN` on `(item_sk, d_date)`.
- Evidence: `02_prev_winner.sql` outputs `web.cume_sales`/`store.cume_sales` directly and joins `web_v1` to `store_v1` via `INNER JOIN`.

## Swarm Exploration Trace
- Assignment evidence: workers included date/dimension isolation, deferred-window, and prefetch lanes.
- Reanalyze evidence: `reanalyze_parsed.json` recommends unifying date-item axes and single-pass cumulative computation.
- Worker SQL evidence: `03_swarm_best.sql` pre-aggregates daily sales and recomputes cumulatives over a `FULL OUTER JOIN`.
- Conclusion: partially explored (swarm did not adopt prev’s inner-join shortcut, but attempted an alternate cumulative rewrite).

## Performance/Validity Outcome
- `benchmark_iter0.json`: W2 = 1.2505x.
- `benchmark_iter1.json`: W5 = 1.2755x (best swarm).
- `benchmark_iter2.json`: W6 = 0.7722x.
- Was the principle implemented correctly: no strict-equivalent implementation found.
- If slower, why: swarm retained more expensive full-outer/window structure; prev’s faster path appears to gain from non-equivalent join semantics.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `45f42eb5cad4cdb4eb79a17dba260ebfdc382043739d4797f9d913b5eb301031`
  - Prev checksum: `ea90d619b49b6b308f6ca4e28019e4fb1a24262dfdfdf87f88c56722e8dfe5ea` (fail)
  - Swarm checksum: `714dbbc200866370bcc83c82cffecd83117cfe928ab417e936d3559c69a18657` (fail)
- Validation source files/commands:
  - `packets/q51/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: fail

## Semantic Integrity Check
- Drift risks observed:
  - Prev replaces full outer join with inner join, changing inclusion semantics for one-channel-only rows.
  - Swarm cumulative reconstruction differs from original `max(...) over (...)` layering.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: enforce checksum gate during swarm selection and force a candidate that preserves exact full-outer + cumulative semantics before optimization.
- Where to apply (fan-out, assignments, reanalyze, final selection): final selection hard gate + candidate generation constraints.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/swarm_artifacts/benchmark_iter1.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q51/validation_strict.json`

## Confidence
- Confidence: medium
- Uncertainties:
  - Exact logical delta between original and swarm cumulative layers is inferred from checksum mismatch and window rewrite structure.
