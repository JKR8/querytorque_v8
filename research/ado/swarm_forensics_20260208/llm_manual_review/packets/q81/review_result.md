# Review Result: q81

## Verdict
- Primary reason swarm lost: swarm explored decorrelation, but its best valid SQL did not include the key early-pruning tactic present in the previous winner (filtering to customers whose current address is `CA` before heavy aggregation).
- Secondary contributors: fastest swarm attempt (W2, 2.277x) was invalid (`rows_match=false`) because it applied `CA` filtering to the wrong address role.

## Previous Winner Principle (manual SQL-derived)
- Principle: decorrelate the state-average subquery into an aggregate CTE + push the `current address = CA` customer filter into the aggregation pipeline before final threshold comparison.
- Evidence: `02_prev_winner.sql` has:
  - `state_avg_return` CTE replacing correlated AVG.
  - join to `customer` + `customer_address ca_current` with `ca_current.ca_state = 'CA'` inside `customer_total_return`, not only at final projection.

## Swarm Exploration Trace
- Assignment evidence: `swarm_artifacts/assignments.json` gives worker 4 `decorrelate`.
- Reanalyze evidence: `swarm_artifacts/reanalyze_parsed.json` calls out decorrelation and early filtering.
- Worker SQL evidence:
  - `03_swarm_best.sql` (W5) decorrelates via `state_thresholds`, but keeps `ca_state='CA'` only in final WHERE.
  - `worker_2_sql.sql` pushes `CA` early, but incorrectly filters `cr_returning_addr_sk` state instead of current customer address state.
- Conclusion: partially explored (decorrelation yes, decisive valid pruning pattern missed in best valid candidate).

## Performance/Validity Outcome
- What happened in benchmark iterations:
  - `benchmark_iter0.json`: W2 hits 2.2771x but `rows_match=false`; valid workers W1/W4 are <1.0x.
  - `benchmark_iter1.json`: W5 becomes best valid at 1.2667x.
  - `benchmark_iter2.json`: W6 is 0.9666x.
- Was the principle implemented correctly: partially.
- If slower, why: valid swarm SQL still aggregates broader customer population before final `CA` filter; previous winner reduces work earlier on the correct join path.

## Semantic Integrity Check
- Drift risks observed:
  - `worker_2_sql.sql` semantic bug: early `CA` filter is applied to return-address state (`cr_returning_addr_sk`) rather than current customer address (query requirement), explaining `rows_match=false`.
  - No obvious semantic drift in `02_prev_winner.sql` vs `01_original.sql` beyond decorrelation/order-preserving refactor.
- Risk severity: medium.

## Minimal Fix for Swarm
- Tactical change needed:
  - Add role-aware join constraints in prompt/checker: “`ca_state='CA'` must bind to `customer.c_current_addr_sk` path for q81.”
  - Preserve decorrelation, but enforce early customer-pruning CTE keyed by current address (as in prev winner), then aggregate returns.
- Where to apply: assignment hints + SQL lint/semantic checker before benchmark acceptance.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/03_swarm_best.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/assignments.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/reanalyze_parsed.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/worker_2_sql.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/benchmark_iter0.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/benchmark_iter1.json`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q81/swarm_artifacts/benchmark_iter2.json`

## Confidence
- Confidence: high
- Uncertainties:
  - Comparison row uses a different historical baseline metadata source (`prev_source=unvalidated`), so absolute cross-run speedup values should be interpreted cautiously.
