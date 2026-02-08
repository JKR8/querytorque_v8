# Review Result: q77

## Verdict
- Primary reason swarm lost: swarm explored broader structural rewrites and achieved only moderate gains (1.0665x), while previous winner score is amplified by semantic drift and a different time slice.
- Secondary contributors: final synthesis attempt failed binder checks; robust winner remained iter0 W4.

## Previous Winner Principle (manual SQL-derived)
- Principle: mostly date isolation per channel with channel-level aggregates; metadata says `date_cte_isolate`.
- Evidence: `02_prev_winner.sql` defines filtered date CTE and channel aggregates, then combines with `UNION ALL`.

## Swarm Exploration Trace
- Assignment evidence: W4 assigned `or_to_union`/structural transform in `assignments.json`.
- Reanalyze evidence: `reanalyze_parsed.json` suggests consolidated sales/returns processing and single-pass channel aggregation.
- Worker SQL evidence: `03_swarm_best.sql` (W4) builds per-channel aggregated CTEs and combines channels with `UNION ALL`.
- Conclusion: explored and implemented, but with limited runtime gain.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W4 = 1.0665x (best).
- `benchmark_iter1.json`: W5 = 1.0635x.
- `benchmark_iter2.json`: W6 fails binder (`sr_store_sk` missing), speedup 0.
- Outcome: reasonable but modest improvement; no approach reached reported prior score.

## Semantic Integrity Check
- Drift risks observed in previous winner:
  - Original date anchor: `1998-08-05`; previous winner moves to `2000-08-23`.
  - Original final ordering is by `channel,id`; previous winner injects `returns_ DESC` into ORDER BY.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed:
  - Freeze date anchors and ordering keys unless explicitly permitted.
  - Keep channel-consolidation rewrite, but require compile-time schema checks on synthesized aliases (to prevent iter2 binder errors).
- Where to apply: semantic/order checker + final synthesis validation.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q77/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q77/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q77/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q77/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q77/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - Prior run provenance is `Retry4W`; baseline normalization is still required for strict speedup ranking.
