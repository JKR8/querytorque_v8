# Beam Modes Design v4 — Probe Swarm + Reasoning Mode

This document defines two complementary modes:

1) **Probe Swarm Mode**: Smart dispatch → 4–16 single-transform probes → BDA → Sniper (one best patch)
2) **Reasoning Mode**: Analyst → 4 diverse global-goal workers (1–3 plans each) → optional Sniper/Analyst Round 2 (1–3 plans)

Both modes share the same validation harness and PatchPlan IR ops.

---

## Shared Inputs

- Original SQL
- EXPLAIN / EXPLAIN ANALYZE
- IR structure + anchor hashes (whitespace independent)
- ★ Importance (1–3) from the DB/fleet system

---

## ★ Importance drives effort

- ★★★: hardest / highest cost → deeper search
- ★★: medium
- ★: easy

This signal influences probe count (swarm) and how aggressive the second-round sniper can be.

---

## Mode 1: Probe Swarm Mode (v3)

### Flow

1) SMART DISPATCH (R1)
   - Uses plan evidence + full transform catalog
   - Outputs adaptive `probe_count` and probe list (single-transform)
2) WORKERS (4–16, parallel)
   - Each outputs one PatchPlan per probe
3) VALIDATION + BENCHMARK
4) BDA TABLE
5) SNIPER (R1)
   - Evidence-informed analyst
   - Outputs ONE best compound PatchPlan (may include novel transform)

### When to use
- You have a transform catalog and want broad, targeted coverage.
- You want deterministic attribution per transform.

---

## Mode 2: Reasoning Mode (Analyst → 4W global briefs)

This mode is the “most successful” reasoning pattern:
- One analyst provides 4 intentionally diverse global optimization goals,
- Each worker explores a broad strategy (two families),
- Workers may emit multiple candidate patch plans,
- If no winner, a second analyst/sniper round produces targeted attempts.

### Step 1: Analyst (R1)

Outputs:
- hypothesis + cost spine + hotspots
- **exactly 4 worker briefs**
  - each with primary+secondary family
  - global goal
  - targets + guardrails + success criteria
- output limits: 1–3 plans per worker

### Step 2: 4 Workers (parallel)

Each worker outputs:
- **at least 1 PatchPlan**
- **up to 3 PatchPlans**
- Each plan includes expected explain delta + risks

Diversity requirement:
- family pairs should be minimally overlapping across the 4 workers.

### Step 3: Apply + validate + benchmark

Evaluate each plan like normal:
- Tier-1 structural gate
- Equivalence gate
- Benchmark + explain capture for passers

### Step 4 (optional): Reasoning Sniper / Analyst Round 2 (R1)

Triggered when:
- no clear winner, OR
- best plan is incomplete, OR
- equivalence failures indicate a different tactic is needed

Outputs:
- updated hypothesis grounded in evidence
- **1–3 targeted PatchPlans**
- may incorporate worker wins, but is free to introduce new transforms

### When to use
- You want maximum creativity/diversity with only 4 worker calls.
- You want workers to explore broad, non-overlapping strategies.

---

## Prompt caching discipline (both modes)

All prompts must be structured:
1) Static header (dialect rules, bans, schemas, ops)
2) `## Cache Boundary`
3) Dynamic tail (★ score, SQL, plan, IR, catalog, BDA)

---

## Patch IR + Anchor Hashes (robustness)

- Anchor hashes must be deterministic and formatting independent.
- Workers/sniper must never invent hashes.
- If anchors are missing/ambiguous, prefer coarse ops (`replace_where_predicate`) or no-op.

---

## Files

### Probe Swarm Mode
- `beam_dispatcher_v3.txt`
- `beam_worker_v3.txt`
- `beam_sniper_v3.txt`

### Reasoning Mode
- `beam_reasoning_analyst_v1.txt`
- `beam_reasoning_worker_v1.txt`
- `beam_reasoning_sniper_v1.txt`
