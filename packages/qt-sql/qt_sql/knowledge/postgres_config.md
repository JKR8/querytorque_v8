# PostgreSQL Config Tuning Playbook
# DSB SF10 + TPC-DS field intelligence — PG 14.3

## HOW TO USE THIS DOCUMENT

You are tuning ONLY via SET LOCAL parameters and pg_hint_plan directives.
The SQL text is FIXED — you cannot rewrite it. Your interventions are:

  1. SET LOCAL parameter changes (work_mem, enable_*, random_page_cost, etc.)
  2. pg_hint_plan hints (HashJoin, NoNestLoop, Leading, Rows, etc.)
  3. Combinations of 1 + 2

Work the EXPLAIN ANALYZE plan, not the SQL. Each pathology has:
  - EXPLAIN signal: what operator pattern to look for
  - Mechanism: why the default plan is suboptimal
  - Intervention: specific SET LOCAL / hint to apply
  - Evidence: validated speedups from DSB benchmark (3-race or 5-run trimmed mean)
  - Risk: known regressions with root cause

CRITICAL: EXPLAIN cost gaps do NOT predict runtime gains. 6 false positives caught
where EXPLAIN showed 38-84% improvement but 3-race runtime showed 0% or regression.
Reason about I/O, cache, and parallelism — not plan cost numbers.

Combo patterns beat single changes. Always consider C1+C3, C2+C3, C2+C4, etc.

---

## CONFIG PATHOLOGIES

### C1: Merge join with expensive sort [HIGHEST IMPACT — 6 wins]

  Mechanism: Planner picks Merge Join because its cost model underestimates
  sort overhead relative to hash-build cost. When both inputs are large and
  unsorted, the sort dominates wall-clock time. Disabling merge join forces
  hash join which builds a hash table (O(N)) and probes (O(M)) — no sort.

  EXPLAIN signal:
  - Merge Join node with Sort child node
  - Sort input rows > 10K
  - No pre-existing index order on the sort key

  Intervention: SET LOCAL enable_mergejoin = 'off'
  Also valid: /*+ Set(enable_mergejoin off) */

  Decision gates:
  - MUST: Merge Join present with Sort below it on large unsorted inputs
  - MUST: Equi-join condition exists (hash join is viable alternative)
  - STOP: Data is pre-sorted (index scan feeding merge join) — MJ is optimal
  - STOP: Sort input < 10K rows — overhead is negligible

  Evidence: 6 wins validated (3-race):
    +82.5%, +68.2%, +66.9%, +60.2%, +17.1%, +8.6%
    Average: +50.6%. Tight variance (e.g., 68.1-68.4% across races).
    Highest impact single-parameter change in our benchmark.

  Risk: LOW when Sort+MJ visible in EXPLAIN.
  Regressions: 0 observed across 52 queries.

  Combos:
  - C1 + C2 (rpc+cache): +82.5% — sort elimination + index scan tips
  - C1 + C6 (sort_off): +68.2% — eliminates both sort and merge overhead
  - C1 + C3 (par4): enhances hash join with parallel hash build

### C2: Cost model undervaluing index scans [HIGHEST RECOVERY — 6 wins]

  Mechanism: Default random_page_cost=4.0 assumes spinning disk. On SSD the
  actual random-vs-sequential cost ratio is ~1.1. This makes the planner
  systematically prefer sequential scans over index scans on fact tables.
  Combined with effective_cache_size telling the planner how much OS page
  cache is available, the optimizer tips from seq scan to index scan.
  These two parameters have a NONLINEAR interaction — neither alone works.

  EXPLAIN signal:
  - Seq Scan on fact table (store_sales, web_sales, catalog_sales, etc.)
  - Btree index exists on the join/filter columns (check pg_indexes)
  - Large fact table (>100K rows in scan)

  Intervention:
    SET LOCAL random_page_cost = '1.1';
    SET LOCAL effective_cache_size = '48GB'

  Decision gates:
  - MUST: Storage is SSD (not spinning disk)
  - MUST: Seq Scan on fact table despite btree index on filter/join columns
  - MUST: Both params together — neither alone sufficient (nonlinear)
  - CHECK: Buffer cache warm (first query after restart may not benefit)

  Evidence: 6 wins validated (3-race):
    +89.0%, +83.2%, +73.4%, +82.5%, +52.5%, +46.0%
    Average: +71.1%. Rescued 3 rewrite regressions.
    Q100: 6.82x additive gain (rewrite 1.01x → config 6.91x — config was the WIN, not rewrite).

  Risk: LOW on SSD. Zero regressions across 52 queries.
  This is our safest high-impact intervention.

  Combos:
  - C2 + C1 (MJ_off): +82.5% — index scan + sort elimination
  - C2 + C3 (par4): +73.4%, +52.5% — index scan + parallel remaining scans
  - C2 + C4 (work_mem): additive when hash spills present alongside seq scans

### C3: Parallelism underutilized on large scans [MOST VERSATILE — 5 wins]

  Mechanism: PG's parallel cost model (parallel_setup_cost=1000, parallel_tuple_cost=0.1)
  is conservative — it underestimates the benefit of parallel workers on large scans.
  Reducing setup cost and tuple transfer cost triggers parallel execution on plans
  that would otherwise run single-threaded. Combined with max_parallel_workers_per_gather=4,
  this activates parallel seq scan + parallel hash join.

  EXPLAIN signal:
  - Large Seq Scan (>100K rows) WITHOUT Gather or Parallel node above it
  - Query execution time > 500ms (CRITICAL threshold)
  - "Workers Planned: 0" or no parallel nodes at all

  Intervention:
    SET LOCAL max_parallel_workers_per_gather = '4';
    SET LOCAL parallel_setup_cost = '100';
    SET LOCAL parallel_tuple_cost = '0.001'

  Decision gates:
  - MUST: Seq Scan > 100K rows without parallel workers
  - MUST: Query baseline > 500ms
  - DANGER: NEVER on queries < 500ms — 7.34x REGRESSION observed on 244ms query
  - DANGER: par4-alone on hash-heavy queries regresses (-15.3%) — combine with C4
  - PREFER: Cost reduction (setup=100, tuple=0.001) over max_workers forcing alone

  Evidence: 5 standalone wins (3-race):
    +28.2%, +17.4%, +12.5%, +7.0%, +6.2%
    Average: +14.3%. Also present in 10+ combo wins.

  Risk: MEDIUM.
    CATASTROPHIC regression: 7.34x on 244ms query (worker startup overhead dominates)
    par4-alone: -15.3% when hash ops spill under parallel execution
    Rule: ALWAYS combine with work_mem (C4) when hash/sort operations visible

  Combos:
  - C3 + C5 (NL_off): +57.5%, +42.5% — hint alone insufficient, par adds 12-35%
  - C3 + C4 (work_mem): +41.5%, +6.2% — par4 alone regresses without work_mem
  - C3 + C2 (rpc+cache): +73.4%, +52.5% — index tips + parallel remaining scans

### C4: Hash/sort spilling to disk [TARGETED — 4 wins]

  Mechanism: Default work_mem=4MB is often insufficient for large hash tables
  and sort operations. When work_mem is exceeded, PG spills to disk (temp files)
  which is orders of magnitude slower. work_mem is allocated PER-OPERATION —
  a query with 6 hash joins allocates 6 × work_mem.

  EXPLAIN signal:
  - "Batches: N" where N > 1 on Hash nodes (hash spill)
  - "Sort Method: external merge" or "Sort Space Type: Disk" (sort spill)
  - "Peak Memory" or "Memory Usage" approaching work_mem limit
  - Temp I/O: read/written blocks in Buffers line

  Intervention: Size by operation count:
    ≤2 sort+hash ops → SET LOCAL work_mem = '512MB'
    3-5 sort+hash ops → SET LOCAL work_mem = '256MB'
    6+ sort+hash ops  → SET LOCAL work_mem = '128MB'

  Decision gates:
  - MUST: Hash Batches > 1 OR Sort Space = 'Disk' in EXPLAIN
  - COUNT: sort + hash nodes in plan before sizing (per-operation allocation)
  - PREFER: combine with C3 (par4) to realize full benefit

  Evidence: 4 wins validated (3-race):
    +41.5% (wm512+par), +17.9% (wm256+par), +16.0% (wm256+par), +11.4% (wm256 alone)
    Average: +21.7%. Often needs par4 to realize full benefit.

    Q100: work_mem=512MB + effective_cache_size=48GB → 6.82x (sort spill eliminated)
    Q059: work_mem=256MB + par4 + jit_off → 1.43x (LLM-driven win)

  Risk: LOW. work_mem is per-operation — count nodes before sizing.
  Regressions: 0 observed when properly sized.
    BUT: hash_mem_multiplier=8.0 on Q064 → 0.56x REGRESSION (too much memory
    per hash op caused memory pressure). Keep hash_mem_multiplier ≤ 2.0.

### C5: Nested loop on large equi-join inputs [HIGH IMPACT — 3 wins]

  Mechanism: PG sometimes picks nested loop for equi-joins when both inputs
  are large (>10K rows). NL is O(N×M) — correct for correlated subqueries
  and small inner tables, but catastrophic for large equi-joins where
  hash join O(N+M) is vastly superior.

  EXPLAIN signal:
  - Nested Loop node with both inputs > 10K rows
  - Equi-join condition (= predicate) exists between the two sides
  - No correlation (no SubPlan reference)

  Intervention: SET LOCAL enable_nestloop = 'off'
  Also valid: /*+ Set(enable_nestloop off) */ or /*+ NoNestLoop(t1 t2) */

  Decision gates:
  - MUST: Nested Loop with >10K rows on BOTH sides
  - MUST: Equi-join condition exists (hash join is viable)
  - STOP: Correlated subquery → NL is correct (use SQL decorrelation instead)
  - STOP: Small inner side (<100 rows) → NL with index lookup is optimal
  - STOP: Dimension table lookup → NL + index is typically fastest

  Evidence: 3 wins validated (3-race):
    +81.3% (Q072, NL_off alone), +57.5% (with par4), +42.5% (with par4)
    One query was completely config-resistant before NL_off unlocked it.

    Q102 PoC: HashJoin(cd)+HashJoin(ca)+work_mem+jit_off → 2.22x
    Neither hints alone (1.0x) nor config alone achieved 2.22x — MUST combine.

    Additional archive evidence:
    Q083: force_nestloop + ssd_costs → 1.62x WIN (164ms vs 265ms)
    Q023: enable_hashjoin=off + max_parallel=8 → 2.44x WIN (774ms vs 1886ms)

  Risk: HIGH.
    CATASTROPHIC: -1454% regression on one query where NL was correct.
    Rule: NEVER on correlated subqueries. NEVER when NL is the right plan shape.
    Targeted hints (NoNestLoop(t1 t2)) are SAFER than global enable_nestloop=off.

  Combos:
  - C5 + C3 (par4): +57.5%, +42.5% — hint alone insufficient
  - C5 + C4 (work_mem) + jit_off: Q102 PoC 2.22x — additive stack

### C6: Sort overhead on pre-ordered data [RARE — 2 wins]

  Mechanism: Planner inserts a Sort node even when data is already ordered
  by an index, or when hash-based aggregation would be cheaper than
  sort-based aggregation. Disabling sort forces hash-based execution.

  EXPLAIN signal:
  - Sort node with input from Index Scan (already ordered)
  - Sort node where hash aggregation is viable (GROUP BY with few groups)
  - Sort cost dominates query time

  Intervention: SET LOCAL enable_sort = 'off'

  Decision gates:
  - MUST: Sort node in EXPLAIN with plausible hash alternative
  - CHECK: High variance observed (3.2-7.7%) — validate carefully
  - PREFER: combine with C1 (MJ_off) for maximum sort elimination

  Evidence: 2 wins validated (3-race):
    +68.2% (with MJ_off), +4.7%
    Average: +36.5%.

  Risk: MEDIUM.
    Forces hash-based execution for ALL operations — may hurt ORDER BY.
    Validate carefully due to high variance.

### C7: JIT compilation overhead on short queries [CLEANUP — 4 wins]

  Mechanism: JIT compiles expressions into native code — setup cost ~50-100ms.
  On queries < 500ms, JIT compilation time can exceed the savings from
  compiled execution. JIT is never harmful on long queries (>2s).

  EXPLAIN signal:
  - "JIT:" section present in EXPLAIN
  - Execution Time < 500ms
  - JIT Time > 5% of Execution Time

  Intervention: SET LOCAL jit = 'off'

  Decision gates:
  - MUST: JIT active AND query < 500ms
  - OR: JIT overhead > 5% of execution time
  - STOP: Query > 2s — JIT amortizes well on long queries
  - SAFE: Always safe to disable — worst case neutral

  Evidence: 4 additive wins (always combined with other params):
    Q010: jit_off alone → 1.066x additive
    Q094: jit_off alone → 1.051x additive
    Q059: jit_off + wm256 + par4 → 1.43x total
    Q081: jit_off + wm256 + par4 → 1.09x additive

  Risk: LOWEST. Zero regressions observed. Always safe as add-on.

---

## PROVEN COMBO PATTERNS

Combos beat singles. The optimizer's plan is a system — changing one parameter
shifts the equilibrium. These combos have proven additive (validated 3-race):

| Combo | Evidence | Mechanism |
|-------|----------|-----------|
| C2 + C1 (rpc+cache + MJ_off) | +82.5% | Index scan tips + sort elimination |
| C5 + C3 (NL_off + par4) | +57.5%, +42.5% | Hint alone insufficient, par adds 12-35% |
| C4 + C3 (wm512 + par4) | +41.5%, +6.2% | CRITICAL: par4 alone regresses without work_mem |
| C1 + C6 (MJ_off + sort_off) | +68.2% | Eliminates both sort and merge overhead |
| C2 + C3 (rpc+cache + par4) | +73.4%, +52.5% | Index scan + parallel remaining scans |
| C5 + C4 + C7 (NL_off + wm + jit_off) | 2.22x (PoC) | Plan shape + memory + cleanup |

Anti-combos (DO NOT use together):
- par4 alone without work_mem on hash-heavy plans → -15.3%
- enable_nestloop=off on correlated subqueries → -1454%
- hash_mem_multiplier=8.0 → memory pressure regression (0.56x on Q064)
- rpc=1.1 + cache=48GB on queries with many NL dimension lookups → can force bad index scan

Per-pair hint warning:
  Per-pair pg_hint_plan hints (e.g., HashJoin(t1 t2) for a specific join pair) show
  3-13% improvement on EXPLAIN but 0% on 3-race validation. Only global Set()-based
  hints (Set(enable_mergejoin off), Set(enable_nestloop off)) produce real runtime gains.
  Targeted NoNestLoop/HashJoin hints on specific table pairs are unreliable.

---

## SAFETY RANKING

From safest to most dangerous:

| Rank | Pattern | Regr. | Worst | Notes |
|------|---------|-------|-------|-------|
| 1 | C2: rpc+cache (SSD fix) | 0 | — | Always apply on SSD. Zero regressions in 52 queries |
| 2 | C7: jit_off | 0 | — | Always safe as add-on for queries < 500ms |
| 3 | C4: work_mem | 0 | — | Zero regr when sized by op count. hash_mem_multiplier ≤ 2.0 |
| 4 | C1: MJ_off | 0 | — | Only when Sort+MJ visible. Highest single-param impact |
| 5 | C6: sort_off | 0 | — | Rare. High variance — validate carefully |
| 6 | C3: par4 | 1 | 7.34x | NEVER on queries < 500ms. Always combine with work_mem |
| 7 | C5: NL_off | 1 | -1454% | ONLY on large equi-joins. Never on correlated subqueries |

---

## PRUNING GUIDE

Skip pathologies the EXPLAIN rules out:

| EXPLAIN shows | Skip |
|---|---|
| No Merge Join nodes | C1 (MJ_off has nothing to fix) |
| No Seq Scan on fact tables with indexes | C2 (rpc+cache irrelevant) |
| Parallel workers already active | C3 (parallelism already engaged) |
| No Hash Batches > 1 and no Sort Disk spill | C4 (work_mem sufficient) |
| No Nested Loop on large inputs | C5 (NL_off has nothing to fix) |
| No Sort node or no hash alternative | C6 (sort_off irrelevant) |
| No JIT section or query > 2s | C7 (jit_off won't help) |
| Baseline < 500ms | C3, C5 (too fast for par4, NL_off risky) |

---

## REGRESSION REGISTRY

Every known config regression with root cause. Match against before proposing.

| Severity | Config | Result | Query | Root cause |
|----------|--------|--------|-------|------------|
| CATASTROPHIC | enable_nestloop=off | -1454% | DSB Q | NL was correct plan for correlated subquery |
| CATASTROPHIC | par4 on 244ms query | 7.34x regr | DSB Q | Worker startup + coordination > query time |
| MAJOR | par4 alone (no work_mem) | -15.3% | DSB Q | Hash spill under parallel execution |
| MAJOR | hash_mem_multiplier=8.0 | 0.56x | Q064 | Memory pressure from oversized hash ops |
| MAJOR | geqo_off | -254% | DSB Q | Exhaustive planner found "better" cost plan on 19 joins but cardinality errors made it catastrophic |
| MAJOR | rpc=1.1+cache=48GB | 6x regr | Q102_i2 | Tipped to index scan on NL-heavy plan where seq scan was faster |
| MODERATE | work_mem=1GB | 0.25x | Q064 PoC | Complex multi-join + excessive memory per op |
| NEUTRAL | 10 assorted hints | 0% | DSB Q | EXPLAIN showed +15% cost improvement → runtime 0%. EXPLAIN ≠ runtime |
| NEUTRAL | geqo_off | 0% | DSB Q | EXPLAIN showed +38% → runtime -254%. Cost gaps are unreliable |
| NEUTRAL | "ALL 12 configs" | 0% | DSB Q | EXPLAIN showed +84% → runtime 0%. Config is not a substitute for SQL rewrite |

---

## EXPLAIN FALSE POSITIVE WARNING

This applies to ALL config tuning decisions. EXPLAIN ANALYZE cost estimates do NOT
predict runtime gains. 6 validated false positives:

1. geqo_off: EXPLAIN +38% → runtime **-254%** (catastrophic regression)
2. All 12 configs: EXPLAIN +84% → runtime **0%** (complete false positive)
3. One query: EXPLAIN +81% → runtime **-1.3%** (slight regression)
4. One query: EXPLAIN +74% → runtime **-2.4%** (slight regression)
5. 10 hints: EXPLAIN +15% → runtime **0%** (no effect)
6. par4: EXPLAIN +25% → runtime **-15.3%** (regression)

The mechanism: EXPLAIN measures plan cost (CPU + I/O estimates).
Runtime is affected by buffer cache state, OS page cache, parallel worker
coordination overhead, and memory pressure — none visible in EXPLAIN.

Rule: Never trust EXPLAIN cost improvements alone. Always benchmark.

---

## HYPOTHESIS TEMPLATE

When proposing candidates, use this reasoning structure:

1. **Identify bottleneck operator**: Which node dominates wall-clock time?
2. **Match to pathology**: Which C1-C7 pattern does the EXPLAIN signal match?
3. **Check decision gates**: All gates pass? If any gate fails → skip.
4. **Check regression registry**: Does this combo match a known regression? → avoid.
5. **Propose combo**: Which patterns compose well? (see combo table)
6. **Size parameters**: work_mem by op count, par by baseline latency.
7. **Predict mechanism**: HOW will the plan change? (not just "faster")

Bad hypothesis: "Increase work_mem to make it faster"
Good hypothesis: "Hash Batches=4 on store_sales join → spilling to disk.
  3 hash+sort ops → work_mem=256MB eliminates spill. Combined with par4
  (baseline 2.4s > 500ms threshold) for parallel hash build."

---

## RECOVERY POTENTIAL

Config tuning can RECOVER rewrite regressions. 5 validated cases where SQL rewrite
regressed but config rescued the query:

| Query | Rewrite | Config Recovery | Mechanism |
|-------|---------|----------------|-----------|
| Q100_spj | 0.61x | +89% → net WIN | rpc+cache tipped to index scan |
| Q102_spj | 0.51x | +83% → net WIN | rpc+cache same mechanism |
| Q027_agg | 0.46x | +73% → net WIN | rpc+cache+par4 |
| Q027_spj | 0.43x | +58% → net WIN | NL_off+par4 (hint+config) |
| Q075 | 0.30x | +46% → net WIN | rpc+cache |

Config acts as a SAFETY NET for aggressive SQL rewrites. If a rewrite changes plan
shape adversely, config can often recover by adjusting cost model parameters.
