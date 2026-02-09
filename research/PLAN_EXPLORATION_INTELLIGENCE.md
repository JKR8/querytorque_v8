# Plan-Space Exploration: EXPLAIN Intelligence vs Wall-Clock Requirements

**Generated**: Feb 9, 2026
**Data Source**: 76 DSB queries on PG14.3 SF10, 54-second EXPLAIN-only scan + 9 wall-clock calibration queries

---

## Part 1: What EXPLAIN-Only Exploration Gives Us (FREE — 54 seconds, zero execution)

### 1.1 Vulnerability Classification

Every query gets a vulnerability fingerprint from EXPLAIN plan structure comparison alone.

**Corpus statistics (76 queries):**
| Vulnerability Type | Affected | % |
|---|---|---|
| JOIN_TYPE_TRAP | 74/76 | 97% |
| JOIN_ORDER_TRAP | 74/76 | 97% |
| SCAN_TYPE_TRAP | 74/76 | 97% |
| MEMORY_SENSITIVITY | 46/76 | 61% |
| PLAN_LOCKED | 2/76 | 3% |

**What each tells the exploit algorithm:**

**JOIN_TYPE_TRAP** — The optimizer's join method choice is fragile. Disabling one join method forces a completely different plan. This tells the LLM: *"The optimizer is choosing nested loops but hash joins are reachable. Rewrite SQL to naturally guide toward the alternative join method."*

Evidence: Q001 uses 4x Nested Loop. `no_nestloop` forces Hash Join with completely different join order and scan types. Q014 same pattern — `force_hash` produced 1.41x wall-clock speedup (confirmed).

**JOIN_ORDER_TRAP** — The optimizer's table access order is unstable. Small perturbations flip the entire join ordering. This tells the LLM: *"Join enumeration is on a knife edge. Explicit JOINs, CTEs, or subquery restructuring can steer the order."*

Evidence: Q013 baseline order [date_dim → store_sales → household_demographics → store → customer_address → customer_demographics] flips completely to [customer_demographics → customer_address → store_sales → date_dim → household_demographics → store] when nested loops disabled.

**SCAN_TYPE_TRAP** — Scan method selection (Index Scan vs Seq Scan vs Bitmap Scan) changes under plan perturbation. This tells the LLM: *"Adding pre-filters, CTEs, or explicit predicates could shift the optimizer from sequential scans to index scans or vice versa."*

Evidence: Q092 has 30 combos that change scan types. Baseline uses Seq Scan(date_dim) + Seq Scan(item) + Bitmap Heap Scan(web_sales). Multiple alternative plans available with different scan strategies.

**MEMORY_SENSITIVITY** — The plan structure changes when `ssd_plus_mem` (work_mem=256MB + hash_mem_multiplier=4 + SSD cost model) is applied. This signals hash/sort spill in the baseline. Tells the LLM: *"SET LOCAL work_mem is a candidate intervention. The baseline is likely spilling to disk."*

Evidence: 46/76 queries (61%) have this flag. Q001 wall-clock confirmed: work_mem_256mb = **1.35x speedup** with same plan (spill elimination).

**PLAN_LOCKED** — Only 2/76 queries produce zero plan changes across all 17 combos + pairwise pairs. Tells the LLM: *"Don't bother with plan-shape attacks. Focus on reducing cardinality or computation within the fixed plan."*

### 1.2 Plan-Space Catalog

For each query, EXPLAIN tells us the exact number of distinct reachable plans:

| Metric | Value |
|---|---|
| Avg distinct plans per query | 14.1 |
| Max distinct plans | 23 (Q080, Q092) |
| Min distinct plans | 1 (2 locked queries) |
| Avg plan-changing combos | 5.9 of 17 |

This is a **search space map**. The LLM now knows which queries have room for plan improvement and which are stuck.

### 1.3 Specific Plan-Change Intelligence

For every combo that changes the plan, EXPLAIN tells us EXACTLY what changed:

- **Which join types appeared/disappeared**: `Nested Loop(Inner) → Hash Join(Inner)`
- **How the table order shifted**: `[A, B, C, D] → [C, D, B, A]`
- **Which scans flipped**: `Index Scan(item,pkey) → Seq Scan(item)`
- **What new operators appeared**: `Bitmap Heap Scan + Bitmap Index Scan` (not in baseline)

This is **actionable attack intel**. If `no_nestloop` produces a better plan shape (more hash joins, better table order), the LLM should write SQL that makes nested loops unattractive to the optimizer — e.g., enlarging the inner relation via CTE, removing correlated predicates, or adding explicit hash-friendly join conditions.

### 1.4 Combinatorial Emergence (Pairwise Discovery)

Phase 2 of the exploration tests pairwise combinations of plan-changing flags. This discovers **emergent plans** that don't appear from any single flag alone.

**Corpus statistics:**
- 74/76 queries produce novel pairwise plans
- Average novel pair combos per query: ~8

Example: Q092 has 15 novel pair combos. `no_hashjoin+ssd_costs` produces a completely different plan from either `no_hashjoin` alone or `ssd_costs` alone. This reveals **interaction effects** in the optimizer that single-knob exploration misses.

### 1.5 Baseline Plan Characterization

EXPLAIN gives us a complete structural fingerprint of the baseline plan:
- Root node type (Limit, Aggregate, Sort, etc.)
- All join types used (Nested Loop, Hash Join, Merge Join) with inner/outer labels
- All scan types with specific index names
- Table access order
- Presence of CTE scans, subquery scans, bitmap scans

This is the **starting position** for the exploit algorithm. The LLM sees exactly what the optimizer chose and what alternatives exist.

---

## Part 2: What Requires Wall-Clock Timing (CANNOT be determined from EXPLAIN)

### 2.1 Same-Plan Performance Differences (INVISIBLE to EXPLAIN)

**This is the single biggest blind spot.** Some configs produce identical EXPLAIN plans but radically different wall-clock performance.

**Evidence — Q001:**
| Config | Plan Change? | EXPLAIN Cost | Wall-Clock | Speedup |
|---|---|---|---|---|
| baseline | — | 63,553 | 8,478ms | 1.00x |
| work_mem_256mb | NO | 63,553 | 6,642ms | **1.35x** |
| work_mem_1gb | NO | 63,553 | 6,702ms | **1.33x** |
| work_mem_2gb | NO | 63,553 | 6,592ms | **1.33x** |

EXPLAIN reports **identical plan, identical cost**. But wall-clock shows 35% improvement because hash tables fit in memory instead of spilling to disk. The cost model doesn't account for spill.

**Evidence — Q013_spj_i1:**
| Config | Plan Change? | Wall-Clock | Speedup |
|---|---|---|---|
| ssd_costs | YES (plan change) | 625ms | **1.07x** |
| work_mem_1gb | NO | 685ms | 1.06x |
| max_parallel | NO | 630ms | 1.05x |

The ssd_costs plan change is visible, but work_mem and parallelism gains are invisible.

**Bottom line**: Work_mem tuning, JIT toggling, and parallelism adjustments often produce NO plan change but significant wall-clock improvement. EXPLAIN cannot detect these. Wall-clock is the only source of truth.

### 2.2 Direction of Plan Changes (EXPLAIN Lies About Whether "Different" = "Better")

EXPLAIN can detect that `no_nestloop` produces a different plan. But it CANNOT predict whether that plan is faster or slower.

**Evidence — Plan change direction is unpredictable:**

| Query | Combo | Plan Changed? | EXPLAIN Cost Ratio | Wall-Clock Speedup |
|---|---|---|---|---|
| Q014 | force_hash | YES | 0.26x (cost says worse) | **1.41x** (actually faster!) |
| Q010 | no_reorder | YES | 0.006x (cost says catastrophic) | **1.49x** (actually fastest!) |
| Q010 | no_nestloop | YES | 0.0001x (cost says disaster) | 0.15x (actually disaster) |
| Q065 | no_nestloop | YES | 0.15x (cost says worse) | 0.25x (cost was right-ish) |
| Q013 | no_nestloop | YES | 0.19x (cost says worse) | 0.27-0.28x (cost was right-ish) |

**Q010 is the killer example**: `no_reorder` (join_collapse_limit=1) has a cost ratio of 0.006x — the cost model says it should be **155x slower**. Wall-clock says it's **1.49x faster** — the best combo for this query. The cost model is inverted.

Meanwhile `no_nestloop` on the same query has cost ratio 0.0001x AND wall-clock 0.15x — both agree it's terrible.

**Conclusion**: EXPLAIN cost ratios have **r=0.44 correlation** with wall-clock speedups. This is below the 0.80 threshold for reliability. You cannot use cost estimates to predict direction. You can only use them as a very noisy prior.

### 2.3 Magnitude of Improvement

Even when we correctly predict the direction, EXPLAIN cannot predict the magnitude.

**Evidence**: Q014 plan exploration shows `force_hash` produces a different plan with cost_ratio 0.26x (cost says 3.9x slower). Wall-clock shows 1.41x faster. The magnitude is off by **5.5x in the wrong direction**.

For the paper, we need precise speedup numbers (1.41x, not "plan changed"). Only wall-clock provides this.

### 2.4 JIT Compilation Overhead (Completely Invisible)

JIT compilation cost is a runtime phenomenon with no EXPLAIN representation.

**Evidence — Q065:**
| Config | Plan Changed? | Wall-Clock | Speedup |
|---|---|---|---|
| no_jit | NO | 2,041ms | **1.07x** |
| baseline | — | 2,055ms | 1.00x |

JIT is the best single combo for Q065, but EXPLAIN shows identical plans. The JIT compiler spends compilation time that exceeds execution savings for fast queries. The EXPLAIN cost model includes no JIT overhead term.

**From pg_config_validation**: Q010 achieves **29.9x speedup** with `jit=off` applied to an LLM-rewritten query. JIT killed a 50ms query by spending 1400ms compiling.

### 2.5 Parallelism Effectiveness (Magnitude Unknown)

EXPLAIN can detect when `no_parallel` changes the plan (Gather node disappears). But the actual parallelism benefit varies wildly:

| Query | no_parallel Wall-Clock | Speedup Impact | Notes |
|---|---|---|---|
| Q014 | 22,068ms (2x regression) | Parallelism helps a lot | 11.4s → 22s |
| Q013_spj | 1,365ms (0.51x) | Parallelism helps 2x | 766ms → 1365ms |
| Q013_agg | 1,396ms (0.47x) | Same pattern | 699ms → 1396ms |
| Q065 | 3,459ms (0.62x) | Parallelism helps ~60% | 2055ms → 3459ms |
| Q001 | 9,465ms (0.97x) | Parallelism barely helps | 8478ms → 9465ms |

For Q001, disabling parallelism barely matters (3% slower). For Q013, it's catastrophic (2x slower). EXPLAIN can detect the Gather node presence/absence, but cannot predict whether removing it costs 3% or 100%.

### 2.6 Cost Model Inversions (SSD/Cache Settings)

Changing `random_page_cost` and `effective_cache_size` alters the cost model's cost estimates but may not reflect reality. The optimizer may choose a "cheaper" plan that's actually slower.

**Evidence**: Q013_spj_i1 shows `ssd_costs` at 1.07x speedup with a plan change. But Q001 shows `ssd_costs` at 0.985x (slight regression) — the "optimized" cost model chose poorly.

---

## Part 3: Integration Strategy — What Goes Where

### 3.1 EXPLAIN-Only Intelligence → Analyst Prompt (Section 3.5)

**Already implemented** in `plan_scanner.py` → `analyst_briefing.py` → `swarm_session.py`.

Feed the analyst:
1. **Vulnerability summary**: "JOIN_TYPE_TRAP (8 combos), JOIN_ORDER_TRAP (8 combos), SCAN_TYPE_TRAP (11 combos)"
2. **Specific plan-change details**: "no_nestloop: Nested Loop(Inner) → Hash Join(Inner), table order flips"
3. **Memory sensitivity flag**: "MEMORY_SENSITIVITY detected — spill likely in baseline"
4. **Plan diversity score**: "10 distinct plans from 17 combos — optimizer choice is fragile"
5. **Best cost-model alternative**: "ssd_costs achieves 1.70x cost reduction" (with caveat that cost ≠ speed)

The analyst uses this to:
- Assign workers to exploit specific vulnerabilities (W1: guide away from nested loops, W2: restructure join order, W3: SET LOCAL work_mem)
- Skip queries with PLAN_LOCKED (redirect to computation-reduction strategies)
- Prioritize high-diversity queries (more plans = more room for improvement)

### 3.2 Wall-Clock Timing → Two Specific Use Cases

**Use Case A: SET LOCAL Config Validation (per-worker, already in pipeline)**

Workers emit SET LOCAL commands. The validation step already benchmarks these with 4x triage timing. No change needed — the wall-clock is already in the validation loop.

What EXPLAIN exploration tells us: which configs CHANGE the plan (targeting information).
What wall-clock tells us: whether that change is actually faster (ground truth).

**Use Case B: Offline Config Sweep (ceiling discovery)**

Run the 17-combo wall-clock sweep on all 76 queries (currently 9/76 done). This gives:
- Absolute ceiling per query (best achievable without SQL changes)
- Specific config for each ceiling (to pass to workers as a hint)
- Detection of same-plan wins (work_mem, JIT) that EXPLAIN misses entirely

**Estimated time**: ~90 minutes for remaining 67 queries at 30s timeout.

### 3.3 Decision Matrix

| Signal | Source | Cost | Actionable For |
|---|---|---|---|
| Plan has alternative join types | EXPLAIN-only | 0.7s/query | LLM worker targeting |
| Plan has alternative join order | EXPLAIN-only | 0.7s/query | LLM worker targeting |
| Plan has memory sensitivity | EXPLAIN-only | 0.7s/query | SET LOCAL work_mem hint |
| Plan is locked (no alternatives) | EXPLAIN-only | 0.7s/query | Skip plan-shape attacks |
| How many distinct plans exist | EXPLAIN-only | 0.7s/query | Priority ranking |
| Which specific configs help | Wall-clock | ~70s/query | SET LOCAL generation |
| Same-plan speedups (work_mem, JIT) | Wall-clock ONLY | ~70s/query | Config-only wins |
| Actual speedup magnitude | Wall-clock ONLY | ~70s/query | Paper numbers |
| Cost model reliability per query | Wall-clock + EXPLAIN | ~70s/query | Trust calibration |

---

## Part 4: Key Findings and Lessons

### 4.1 EXPLAIN Cost ≠ Wall-Clock Performance

**Pearson r = 0.44** between EXPLAIN cost-model speedup predictions and actual wall-clock speedups across all combos. Even filtering to only plan-changing combos: **r = 0.78** (still below 0.80 threshold).

The PostgreSQL cost model is systematically wrong about:
- `random_page_cost` effects (changes cost but not reality when data is cached)
- `join_collapse_limit=1` (cost model penalizes, but removing reordering sometimes helps)
- Hash/sort spill (not modeled in cost — same cost whether spilling or not)
- JIT overhead (zero cost model term for compilation time)

### 4.2 Three Categories of Wins

From our 9 wall-clock calibration queries + the DSB-76 LLM benchmark results:

1. **Plan-change wins** (EXPLAIN-detectable as "different plan", direction unknown):
   - Q014: force_hash → 1.41x (EXPLAIN saw plan change)
   - Q010: no_reorder → 1.49x (EXPLAIN saw plan change)
   - LLM wins: Q065 3.93x, Q080 3.32x, Q099 2.28x (SQL rewrites caused plan changes)

2. **Same-plan wins** (EXPLAIN-invisible, wall-clock only):
   - Q001: work_mem_256mb → 1.35x (identical plan, spill elimination)
   - Q010: jit=off → 29.9x on LLM rewrite (identical plan, JIT overhead)
   - Q100: work_mem → 6.9x (identical plan, spill elimination)

3. **Hybrid wins** (plan change + config):
   - Q102: HashJoin hints + work_mem + jit_off → 2.22x (neither alone achieves this)
   - Q014: SQL rewrite + config → 30.37x (SQL=18.9x, config adds 1.6x multiplicative)

### 4.3 The Memory Sensitivity Blind Spot

46/76 queries (61%) show MEMORY_SENSITIVITY in EXPLAIN exploration. **But this flag is incomplete.**

MEMORY_SENSITIVITY detects when ssd_plus_mem changes the plan **structure**. It does NOT detect when work_mem eliminates spill **within** the same plan. These are two different phenomena:

| Phenomenon | EXPLAIN Detection | Example | Wall-Clock Impact |
|---|---|---|---|
| Plan-shape sensitivity | YES (MEMORY_SENSITIVITY flag) | Q013: ssd_costs → different plan | 1.07x |
| Spill elimination (same plan) | NO (identical fingerprint) | Q001: work_mem_256mb → same plan | **1.35x** |

**Q001 is the killer counterexample**: It does NOT trigger MEMORY_SENSITIVITY (ssd_plus_mem produces identical plan hash). Yet work_mem_256mb gives 1.35x speedup — the biggest config ceiling in our calibration set. The hash tables fit in memory instead of spilling, but EXPLAIN can't see this because the plan structure is identical.

**Heuristic workaround**: If the baseline plan contains **Hash Join** or **Sort** operators, treat it as a spill candidate regardless of the MEMORY_SENSITIVITY flag. This catches both phenomena:
- MEMORY_SENSITIVITY queries (plan changes with memory) → 46/76
- Spill candidates (Hash Join or Sort in baseline) → ~70/76

For ALL spill candidates, workers should try `SET LOCAL work_mem = '256MB'` as a companion config. Only wall-clock (Layer 2) can confirm the actual impact.

### 4.4 The 2 Locked Queries

Only 2/76 queries have completely locked plans. These are the hardest to optimize — the optimizer found the globally preferred plan and no amount of flag toggling changes it. For these queries:
- Plan-shape attacks (join reordering, scan changes) will fail
- Only **computation reduction** (eliminating redundant work, reducing cardinality) can help
- SET LOCAL work_mem is still worth trying (same plan, less spill)

---

## Appendix: Cross-Reference Table (9 Calibration Queries)

| Query | Baseline(ms) | Plan Explore | Wall-Clock Ceiling | Ceiling Combo | Plan Changed? |
|---|---|---|---|---|---|
| Q001_multi | 8,478 | 10 distinct plans, 4 changers | **1.35x** | work_mem_256mb | NO |
| Q010_multi | 1,343 | 10 distinct plans, 4 changers | **1.49x** | no_reorder | YES |
| Q013_agg_i1 | 699 | 13 distinct plans, 6 changers | 1.04x | work_mem_2gb | NO |
| Q013_agg_i2 | - | 13 distinct plans, 6 changers | ~1.04x | work_mem_2gb | NO |
| Q013_spj_i1 | 766 | 19 distinct plans, 7 changers | **1.07x** | ssd_costs | YES |
| Q013_spj_i2 | - | 13 distinct plans, 6 changers | ~1.06x | work_mem_1gb | NO |
| Q014_multi | 11,467 | see plan_explore | **1.41x** | force_hash | YES |
| Q065_multi | 2,055 | 17 distinct plans, 6 changers | 1.07x | no_jit | NO |
| Q010_multi_i2 | - | 10 distinct plans, 4 changers | ~1.04x | work_mem_1gb | NO |

**Pattern**: 3/9 ceiling wins come from plan changes (detectable by EXPLAIN). 6/9 come from same-plan improvements (invisible to EXPLAIN, wall-clock only). The ratio may shift with more data, but the message is clear: **both signals matter, and EXPLAIN alone misses the majority of config-only wins**.

---

## Appendix B: Can We Predict Which Queries Need Wall-Clocks?

**Partially, with heuristics — but not definitively.**

From EXPLAIN alone, we can triage:

| Signal | Heuristic | Priority |
|---|---|---|
| High plan diversity (>15 plans) | More alternatives = higher ceiling chance | Tier 1 |
| MEMORY_SENSITIVITY + Hash/Sort operators | Spill elimination + plan-shape wins | Tier 1 |
| Medium diversity + Hash Join operators | Potential config wins | Tier 2 |
| PLAN_LOCKED (2 queries) | No plan alternatives to test | Tier 3 (deprioritize) |

**What we CANNOT predict from EXPLAIN:**
- Whether a plan change is faster or slower (r=0.44)
- Whether work_mem eliminates spill (no spill info without ANALYZE)
- JIT overhead magnitude (zero EXPLAIN signal)
- Actual speedup numbers for the paper

**Recommendation**: Run wall-clocks on ALL queries (skip >30s baselines). Prioritize by exploration score but don't skip any — same-plan wins are unpredictable.

---

## Appendix C: Algorithm Reference

The full exploit algorithm is defined in `research/EX_ALGO.yaml`:
- **Layer 1**: EXPLAIN-only exploration (54s for 76 queries, cached)
- **Layer 2**: Wall-clock calibration (17 combos × 4x triage per query, cached)
- **Layer 3**: LLM swarm (uses Layer 1+2 as targeting intel)
