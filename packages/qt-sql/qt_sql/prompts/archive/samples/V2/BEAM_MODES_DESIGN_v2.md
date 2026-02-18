# Beam Pipeline v3: Smart Dispatch → 12–16 Single-Transform Workers → Collated Table → Sniper

This version removes AST-based routing and instead uses an R1 dispatcher that chooses probes from plan evidence + a transform catalog.

---

## Fleet Terminology Mapping

| Fleet C2 | Beam Pipeline | Description |
|----------|--------------|-------------|
| **Fleet** | Batch | All queries in a benchmark run |
| **Mission** | Query session | One query’s full optimization lifecycle |
| **Sortie** | Iteration | One dispatch + worker set + validation pass |
| **Strike** | Worker probe | Single transform attempt (one LLM call) |
| **BDA** | Validation + benchmark + explain | Assess what worked |
| **Sniper** | Synthesis round | R1 composes winning insights into one best patch |

---

## Core Principle

- **Workers are narrow**: one transform, one precise target, produce a PatchPlan.
- **Sniper is wide**: reads a collated results table, then composes a compound patch safely.

This keeps the expensive reasoning in *two places*:
1) dispatch (choose the right 12–16 probes)
2) snipe (compose the best ideas once evidence exists)

---

## Pipeline (per Mission)

```
STEP 0: Baseline (cached or quick 1-run)
  - record original_ms, original_explain, original_sql_hash

STEP 1: SMART DISPATCH (R1, 1 call)
  Inputs: original_sql + explain + IR map + full transform catalog
  Output: 12–16 probe assignments (single-transform)

STEP 2: WORKERS (12–16, parallel; typically small models)
  Each worker receives:
    - shared hypothesis (from dispatch)
    - ONE probe assignment (transform + target + node contract)
    - original SQL + IR map (+ optional plan snippet)
    - patch ops + semantic rules
  Output: PatchPlan JSON

STEP 3: APPLY PATCH + TIER-1 GATE (instant)
  - Apply PatchPlan to IR
  - Structural validation (literals, column refs/count, parse, obvious alias issues)
  - Retry-once policy per probe with concrete error message (optional)

STEP 4: EQUIVALENCE GATE (full dataset)
  - Row count + checksum/MD5 against baseline query result
  - Retry-once policy with semantic failure notes (optional)

STEP 5: BENCHMARK (e.g., warmup + avg2)
  - Only for equivalence-passing candidates
  - Record speedup, variance

STEP 6: EXPLAIN COLLECTION
  - Capture EXPLAIN for passing candidates

STEP 7: COLLATE RESULTS TABLE (BDA)
  - One row per probe: status, speedup, explain delta summary, ops used, errors

STEP 8: SNIPER (R1, 1 call)
  Input: full BDA table + top PatchPlans + explain deltas
  Output: ONE best compound PatchPlan

STEP 9: RE-VALIDATE SNIPER PATCH
  - Steps 3–6 again on the sniper output
  - Final: best_speedup, best_patch_plan (and derived SQL)
```

---

## Collated Results Table (BDA schema)

Minimum fields (per probe):

- `probe_id`
- `transform_id`, `family`
- `status`: PASS | WIN | REGRESSION | FAIL_TIER1 | FAIL_EQUIV | ERROR
- `speedup` (if benchmarked)
- `benchmark_ms` (optional)
- `ops_used` (e.g., insert_cte, replace_from, ...)
- `explain_delta`: 1–2 lines (what changed: operator replaced, loops reduced, rows reduced)
- `failure_reason` (if any)

This table is the only thing the Sniper *must* read to reason safely.

---

## Prompt Caching Discipline

All prompts must be structured like:

1) **Static header** (dialect rules, patch ops, families, registry, schemas)
2) `## Cache Boundary`
3) **Dynamic tail** (SQL, plan, IR hashes, catalog, BDA)

This makes caching stable across missions and reduces token waste.

Recommended convention:
- Put any tables, family cards, regression registries, schemas in the static section.
- Put *only* query-specific artifacts below the boundary.

---

## Files / Prompts

- `beam_dispatcher_v2.txt` — Smart dispatch prompt (R1)
- `beam_worker_v2.txt` — Single-transform worker PatchPlan prompt
- `beam_sniper_v2.txt` — Evidence-driven compound patch prompt (R1)
