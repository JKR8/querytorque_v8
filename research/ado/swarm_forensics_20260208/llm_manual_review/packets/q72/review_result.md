# Review Result: q72

## Verdict
- Primary reason swarm did not beat prior headline: previous SQL changes key literals (`d_year`, marital status, buy potential), so its speedup is non-equivalent.
- Secondary contributors: swarm’s best equivalent plan was in iter0; later iterations failed/regressed.

## Previous Winner Principle (manual SQL-derived)
- Principle: isolate date/customer/household dimensions into filtered CTEs and join through catalog/inventory with promo counts.
- Evidence: `02_prev_winner.sql` uses `filtered_d1/d2/d3`, `filtered_cd`, `filtered_hd`.

## Swarm Exploration Trace
- Assignment evidence: workers covered early filter, date isolation, prefetch, and structural transforms.
- Reanalyze evidence: `reanalyze_parsed.json` emphasizes inequality join bottleneck and shared date constraints.
- Worker SQL evidence: `03_swarm_best.sql` applies the same CTE isolation pattern with original literals (`2002`, `W`, `501-1000`).
- Conclusion: explored and implemented.

## Performance/Validity Outcome
- `benchmark_iter0.json`: W1 = 1.1310x (best swarm).
- `benchmark_iter1.json`: W5 error (`Referenced table "d1" not found`).
- iter2 artifact absent for this packet.
- Was the principle implemented correctly: yes for W1.
- If slower, why: equivalent optimization ceiling; prior comparator is drifted.

## Validation Evidence (Required)
- Schema parity evidence (same columns, same order):
  - Original vs prev: pass.
  - Original vs swarm: pass.
- Rowcount parity evidence (original vs candidate):
  - Original vs prev: 100 vs 100 (pass).
  - Original vs swarm: 100 vs 100 (pass).
- Checksum/hash parity evidence (original vs candidate):
  - Original checksum: `261cb14db472b27c8f2ea6d3894856573ae58568de1511adb203cc6d6809f247`
  - Prev checksum: `b38b62537017d473b2e92bdc75e2c12ac6ddb31fe50a18d761e6ca4326f9cc30` (fail)
  - Swarm checksum: `261cb14db472b27c8f2ea6d3894856573ae58568de1511adb203cc6d6809f247` (pass)
- Validation source files/commands:
  - `packets/q72/validation_strict.json`
  - strict validator run over `/mnt/d/TPC-DS/tpcds_sf10_1.duckdb`.
- Validation status: pass (swarm); fail (prev)

## Semantic Integrity Check
- Drift risks observed in prev:
  - `d_year = 2002` changed to `1999`.
  - `cd_marital_status = 'W'` changed to `'D'`.
  - `hd_buy_potential = '501-1000'` changed to `'>10000'`.
- Risk severity: high.

## Minimal Fix for Swarm
- Tactical change needed: preserve equivalent W1-style plan and improve fallback robustness (avoid alias/binder mistakes in later iterations).
- Where to apply (fan-out, assignments, reanalyze, final selection): worker SQL lint + fallback selection.

## Evidence References
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/01_original.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/02_prev_winner.sql`
- `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/03_swarm_best.sql`
- Additional files:
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/swarm_artifacts/benchmark_iter0.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/swarm_artifacts/reanalyze_parsed.json`
  - `packages/qt-sql/ado/swarm_forensics_20260208/llm_manual_review/packets/q72/validation_strict.json`

## Confidence
- Confidence: high
- Uncertainties:
  - None material.
