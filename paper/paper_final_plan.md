# FINAL PAPER EXECUTION PLAN
## "Smaller Box, Same Workload: Plan-Grounded Query Rewriting for Infrastructure Right-Sizing"

**Target**: VLDB 2026 Industry Track
**Rating target**: 9.8–9.9 (no hand-waving, no gaps, every claim backed by a table or figure)
**Status**: System exists, baselines reproduced, competitive advantage confirmed. This document is the build plan for the paper, not the system.

---

## THE THESIS (memorise this)

> We right-size real SQL workloads by diagnosing engine-specific plan failure modes
> and applying catalog-gated treatments — achieving reliable one-tier infrastructure
> downsizing, where latency-optimised generic rewriting is brittle under resource budgets.

**20-second pitch**: Query optimizers have systematic, documented failure modes that differ by engine. We catalog these, detect them in execution plans, and apply gated treatments to rewrite SQL so the same workload runs on a smaller (cheaper) machine. Generic LLM rewriting optimizes for latency, which is the wrong target — faster queries that use more memory fail on smaller instances. We optimize for the right thing: workload fit under resource budgets.

**What's novel (three things, no more)**:
1. Plan failure mode catalogs as structured, engine-specific knowledge for LLM-constrained rewriting
2. Resource-budget feasibility as the optimization objective (not latency)
3. Fleet-level workload right-sizing economics at deployment scale

---

## COMPETITIVE POSITION

### What we have that nobody else has

```
                        Rbot/LLM-R2          Us
                        ───────────          ──
Knowledge source        LLM pretraining      Empirical engine-specific catalogs
Reasoning from          SQL text              Execution plan gaps
Optimization target     Query latency         Workload fit (tier feasibility)
Regression control      None (hope)           Gated treatments + anti-pattern blacklist
Fleet deployment        Not addressed         Triage + shared optimizations
Validation              Run and check         Logic oracle pre-screens every candidate
```

### The evidence we already hold

1. **Rbot reproduced on our test server.** We have their actual runtime numbers — not just their published claims. We can show parity on their metric (latency) and dominance on ours (resource budget feasibility).

2. **We are significantly better on single-query rewrites.** Head-to-head on same benchmark, same hardware, same conditions. This is not a reproduction gap — this is a methodology gap.

3. **We have their unpublished runtime metrics.** Spill bytes, temp IO, memory pressure — the data they didn't report because they weren't measuring for it. We can show that their "wins" are often resource-hostile: faster but bigger.

4. **We have a business-cost benchmark.** An overnight pipeline simulation on TPC-DS that measures what a customer actually pays: total workload completion time × instance cost. This is the metric that matters commercially and the metric nobody else reports.

---

## VOCABULARY LOCK (use these words, not others)

### USE
- **Plan failure mode** (not "blind spot" or "optimizer weakness")
- **Catalog-gated treatment** (not "rewrite" or "optimization")
- **Tier feasibility** (not "can it run on a smaller box")
- **Resource-budget feasibility** (not "memory usage")
- **Reference plan shape** (not "optimal plan")
- **Workload fit** (not "performance")
- **Right-sizing** (not "cost optimization")
- **Plan-grounded** (not "EXPLAIN-based")
- **Spill avoidance** (not "memory reduction")
- **Regression control** (not "safety")

### NEVER USE
- "Deterministic" (overclaims, triggers skepticism)
- "Neuro-symbolic" (buzzword, invites comparison to formal methods)
- "Eliminates hallucinations" (unprovable, red flag)
- "Optimal plan" (prove it's optimal or don't say it)
- "AI-powered" (adds nothing, triggers eye-rolls)
- "Novel" in the abstract (let the reviewer decide)

---

## PAPER STRUCTURE

### Abstract (~250 words)

Hook: Cloud customers pay for infrastructure tiers, not query latency. Right-sizing — running the same workload on a smaller compute tier — is the optimization that directly reduces cost. Yet SQL rewriting research, including recent LLM-based approaches, targets per-query latency, which is a proxy metric that diverges from tier feasibility under resource budgets.

System: We present [SYSTEM], which diagnoses engine-specific plan failure modes in execution plans and applies catalog-gated treatments to rewrite SQL for workload fit. The catalog encodes empirically validated failure modes with detection criteria, gated treatments (with preconditions), and documented anti-patterns (with measured regressions).

Results (fill in measured numbers):
- Head-to-head vs Rbot: [X]% win rate vs [Y]% on single-query latency (their metric), using same model and matched token budget
- On resource-budget feasibility: [A]% of our rewrites are tier-feasible on the smaller instance vs [B]% for Rbot — the gap widens under resource pressure
- Infrastructure right-sizing: full TPC-DS workload completes on one-tier-smaller instance after fleet optimization, reducing infrastructure cost by ~50%
- On [N] queries where latency-optimal and tier-feasible rewrites diverge, Rbot's rewrites timeout on the smaller tier while ours complete
- Fleet triage achieves [85]%+ of per-query benefit at [25]% of LLM compute cost

---

### §1. Introduction (2 pages)

**Page 1: The problem**

Open with economics. One concrete number:

> "A production SQL workload running on a Postgres db.m5.xlarge ($0.342/hr) costs
> $3,000/year. The same workload, after plan-grounded rewriting, completes on a
> db.m5.large ($0.171/hr) — saving $1,500/year. The rewriting cost: $0.12 in LLM
> API tokens, applied once."

Then the systems insight: why doesn't generic LLM rewriting achieve this?

Introduce the divergence with one vivid example:

> "Query Q67 runs in 180s on xlarge. Rbot rewrites it to 95s — a 1.9× speedup.
> But on the target tier (large, 8GB RAM), Rbot's rewrite spills 4.1GB to disk
> and times out. The rewrite is faster but bigger — it traded latency for memory.
> Our system rewrites Q67 to 145s on xlarge (modest speedup) but with zero spill,
> completing in 195s on large. By the latency metric, Rbot wins. By the metric
> that determines the customer's bill, it fails."

This one example makes the entire thesis concrete before any formalism.

**Page 2: Our approach + contributions**

Three-sentence system description:
1. We maintain empirically validated catalogs of engine-specific plan failure modes
2. Given a query + execution plan, we diagnose plan-gap divergences and match to catalog entries
3. Treatments are gated: preconditions must hold, anti-patterns are blacklisted, resource-budget constraints are enforced

Contributions list (same four as before, with updated vocabulary):
1. Plan failure mode catalogs as structured knowledge (§3)
2. Plan-grounded diagnosis outperforms SQL-level reasoning (§5.1)
3. Tier feasibility as optimization objective — where it diverges from latency (§5.2)
4. Fleet-level right-sizing economics (§5.3)

---

### §2. Background and Related Work (1.5 pages)

**§2.1 Query Optimizer Limitations**
Leis et al. (2015) — cardinality estimation errors. Frame: optimizer failures are systematic and engine-specific. This is the foundation for our catalogs.

**§2.2 Learned Query Optimization**
Bao, Neo, Balsa, Lero. Inside the optimizer. Complementary — we rewrite SQL externally. Key distinction: they need per-workload training; our catalogs transfer across workloads on the same engine.

**§2.3 LLM-Based SQL Rewriting**
Rbot, LLM-R, LLM-R2, DB-GPT, GPTuner. Position precisely:
- Same framing (LLM for SQL optimization)
- Different knowledge source (empirical catalogs vs pretrained knowledge)
- Different reasoning (plan-gap diagnosis vs SQL-level intuition)
- Different target (tier feasibility vs latency)

Note: "We reproduced Rbot on identical hardware and benchmark conditions (§5.1). Our comparison uses measured results from the same test server, not published numbers from different environments."

**§2.4 Cloud Cost Optimization**
Right-sizing tools (AWS Compute Optimizer, Snowflake Resource Monitor). These choose instance size for a given workload. We change the workload so a smaller instance suffices. Opposite direction, complementary.

---

### §3. Plan Failure Mode Catalogs (2 pages)

This is the core intellectual contribution. The formalism that makes it research, not "bag of tricks."

**§3.1 Definition**

> A plan failure mode is a class of query structures where a specific engine's
> query optimizer systematically produces plans that diverge from the reference
> plan shape, due to a documented limitation in plan enumeration, cardinality
> estimation, or transformation rules.

Properties (each must hold for a catalog entry):
- **Engine-specific**: verified to exist on engine E, verified absent (or different) on engine E'
- **Detectable**: observable symptoms in EXPLAIN output, specified as detection criteria
- **Treatable**: at least one SQL restructuring that causes the optimizer to find a better plan, with measured improvement
- **Gated**: treatment preconditions (when it applies) and anti-patterns (when it harms), both with measured evidence
- **Reproducible**: manifests across multiple queries sharing the structural pattern

**§3.2 Catalog Entry Structure**

```
FAILURE MODE: [name]
ENGINE: [engine + version range]
DETECTION:
  Plan symptom: [observable in EXPLAIN]
  Query pattern: [SQL structural signature]
  Gate conditions: [when treatment applies]
TREATMENTS:
  [name]: [SQL transform] → [measured speedup range] | [resource impact]
ANTI-PATTERNS:
  [name]: [SQL transform] → [measured regression] | CAUSE: [why]
RESOURCE IMPACT:
  Spill change: [increase/decrease/neutral]
  Peak memory: [increase/decrease/neutral]
  Intermediate size: [increase/decrease/neutral]
```

The RESOURCE IMPACT section is new — it's what makes the catalog infrastructure-aware. A treatment that's 2× faster but doubles spill is tagged. The system can then GATE that treatment out when optimizing for a resource-constrained tier.

**§3.3 Catalog Construction**

Methodology: benchmark-driven empirical process.
1. Execute benchmark suite with EXPLAIN ANALYZE + resource telemetry
2. Identify plan-vs-reference divergences (automated detection + manual root cause)
3. Classify by engine limitation mechanism
4. Design treatments, measure results INCLUDING resource impact
5. Document anti-patterns from treatments that regressed
6. Cross-query validation: confirm same failure mode across multiple queries

Quantify the effort: "The Postgres 16 catalog contains [N] failure modes documented from [M] benchmark runs over [T] weeks. The same catalog applies unchanged to all workloads on Postgres 16."

**§3.4 Engine Specificity (the proof)**

Table showing the SAME query pattern requiring DIFFERENT treatments (or no treatment) on different engines:

| Failure Mode | Postgres 16 | DuckDB 1.1 |
|---|---|---|
| Correlated subquery | Failure mode: nested loop semi. Treatment: decorrelate → 2.4× | Not a failure mode: auto-decorrelated. Treatment: none needed |
| Cross-CTE predicate propagation | Moderate: some cases optimized | Strong failure mode: predicates never propagate. Treatment: isolate → 1.3–1.9× |
| Redundant scans (subquery CSE) | Partial: some CSE applied | Strong failure mode: no CSE. Treatment: single-pass aggregation → 1.5–6.2× |

This table is the evidence that engine-specific knowledge matters. A generic rewriter can't make these distinctions.

**§3.5 Catalog Coverage and Concentration**

Figure 6: Bar chart — wins per failure mode category, stacked by engine.

Key finding: "[K] failure modes account for [P]% of all wins. The catalog does not need to be exhaustive — the top failure modes per engine capture most of the opportunity."

Table: Coverage — what fraction of TPC-DS queries match at least one catalog entry.

| Engine | Queries with ≥1 match | Queries with no match | Avg matches per query |
|---|---|---|---|
| Postgres 16 | [A]/99 | [B]/99 | [C] |
| DuckDB 1.1 | [D]/99 | [E]/99 | [F] |

**§3.6 Catalog Stability**

If available: "We verified [N/M] Postgres 16 catalog entries against Postgres 15. [N-K] entries applied unchanged; [K] entries required minor revision due to optimizer improvements in v16."

If not available: acknowledge as future work, but note that failure modes correspond to architectural limitations (not version-specific bugs), so stability is expected.

---

### §4. System Architecture (2 pages)

Brief. The system supports the thesis but IS NOT the thesis. Don't over-describe.

**§4.1 Diagnostic Reasoning Pipeline**

```
INPUT: SQL + EXPLAIN ANALYZE + resource telemetry + engine catalog

DIAGNOSIS:
  1. Parse plan → extract cost-dominant operators + resource pressure points
  2. Analyze query structure → filter selectivities, join ratios, directions
  3. Construct reference plan shape from query structure
  4. Compare actual plan to reference → identify divergences
  5. Match each divergence to catalog failure mode (or flag as novel)
  6. Select treatments: gated by preconditions, constrained by resource budget

OUTPUT: Rewritten SQL + predicted resource impact
```

Key design principle: "Steps 1–4 are engine-independent diagnostic reasoning. Step 5 is where engine-specific knowledge enters. Step 6 is where resource-budget constraints enter. The same reasoning framework operates across engines — only the catalog changes."

**§4.2 Gated Treatment Selection**

This is the "why we don't regress" argument. Explain the gating mechanism:

- Treatment T for failure mode F has preconditions P₁..Pₙ
- If any Pᵢ fails, treatment is blocked (not attempted)
- Anti-patterns are checked: if the query matches a documented anti-pattern for T, treatment is blocked
- Resource budget is checked: if T's documented resource impact would exceed the target tier's budget, treatment is blocked or an alternative treatment is selected

"This is why our regression rate is [X]% vs Rbot's [Y]%. We don't attempt treatments that have been measured to fail in similar conditions."

**§4.3 Multi-Agent Rewriting**

Brief description. 4 workers, family-diversified (filter placement, join restructuring, aggregation, subquery/set). Validation via logic oracle (mini-DuckDB/SF1). Sniper refinement.

Include the one empirical finding worth reporting: "Diversifying workers by structural transform family (which part of the plan they target) outperforms diversifying by aggressiveness (how much they change). Family-diversified workers produce uncorrelated results; intensity-diversified workers produce correlated failures."

**§4.4 Logic Oracle Validation**

"Each candidate rewrite is validated for semantic equivalence by executing both original and rewrite on a small-scale DuckDB instance (TPC-DS SF1). Result sets are compared row-by-row. This catches [X]% of semantic errors before any production-tier benchmark, reducing wasted compute. When validation fails, the precise diff (which rows differ, which columns) feeds back to the retry mechanism as a targeted error signal."

This is a practical contribution that costs one paragraph to describe and saves significant benchmark compute.

---

### §5. Evaluation (4 pages — the core of the paper)

#### §5.1 Experiment 1: Plan-Grounded vs Generic Rewriting (their metric — latency)

**Purpose**: Beat Rbot on THEIR metric first. Establish credibility before introducing ours.

**Setup**:
- TPC-DS SF100, Postgres 16, identical test server for both systems
- Rbot: reproduced on our hardware, verified against their published numbers
- [SYSTEM]: same base LLM, matched candidate count, matched token budget
- Baseline: no rewrite

**Baseline parity documentation** (one paragraph, critical for reviewer trust):
"We reproduced Rbot on identical hardware (spec). On [N] queries where Rbot publishes results, our reproduction achieves within [±X]% of their reported speedups, confirming faithful reproduction. All comparisons below use measured results from the same test server under identical conditions."

**Results table**:

| System | Win Rate (>1.1×) | Avg Speedup (winners) | Regression Rate (<0.9×) | Avg Regression |
|---|---|---|---|---|
| Rbot (reproduced) | [Y]% | [A]× | [B]% | [C]× |
| [SYSTEM] | [X]% | [D]× | [E]% | [F]× |
| [SYSTEM] − catalog (ablation) | [G]% | [H]× | [I]% | [J]× |

**Ablation narrative**: "Removing the engine catalog from [SYSTEM] — retaining the diagnostic reasoning pipeline but without failure mode knowledge — reduces win rate from [X]% to [G]%. This [X-G] percentage point gap is the measured contribution of engine-specific knowledge. Note that the ablated system ([G]%) performs comparably to Rbot ([Y]%), suggesting that generic LLM rewriting and our system without catalogs operate at similar effectiveness."

This is the clean proof that engine-specific knowledge is the decisive factor.

**Per-failure-mode breakdown** (table or figure):

| Failure Mode | Queries Affected | [SYSTEM] Wins | Rbot Wins | Neither |
|---|---|---|---|---|
| Correlated subquery | [N] | [a] | [b] | [c] |
| Cross-CTE propagation | [N] | [a] | [b] | [c] |
| Redundant scans | [N] | [a] | [b] | [c] |
| Aggregate-below-join | [N] | [a] | [b] | [c] |
| ... | ... | ... | ... | ... |

"[SYSTEM]'s wins concentrate on queries matching catalog failure modes. Rbot's wins are distributed without systematic pattern — consistent with generic reasoning that occasionally finds improvements but cannot reliably target known failure modes."

#### §5.2 Experiment 2: Infrastructure Right-Sizing (our metric — tier feasibility)

**THE HEADLINE EXPERIMENT.**

**Setup**:
- TPC-DS SF100, Postgres 16
- Target tier: one size below minimum viable baseline
- Resource telemetry: spill bytes (temp IO), peak buffer usage, per-operator timing
- Feasibility threshold: query completes in <300s with spill below [threshold]

**Phase A — Establish the cliff**:

| Instance Tier | Baseline Completion | Timeouts | Spill Events | Total Workload Time |
|---|---|---|---|---|
| db.m5.xlarge (16GB) | [~95]% | [~5] | [few] | [T₁] |
| db.m5.large (8GB) | [~70]% | [~30] | [many] | [T₂] |

"The workload is feasible on xlarge but NOT feasible on large: [~30] queries timeout due to resource pressure (spill to disk exceeding memory budget)."

**Phase B — Right-size** (Figure 2 — THE figure that wins the room):

| Instance | System | Completion Rate | Timeouts | Spill Events | Cost/hr |
|---|---|---|---|---|---|
| xlarge | Baseline | [95]% | [5] | [few] | $0.342 |
| large | Baseline | [70]% | [30] | [many] | $0.171 |
| large | Rbot | [??]% | [??] | [??] | $0.171 |
| **large** | **[SYSTEM]** | **[~95]%** | **[~5]** | **[minimal]** | **$0.171** |

"[SYSTEM] on large achieves comparable feasibility to the unoptimized workload on xlarge — at half the infrastructure cost. Rbot on large achieves [??]% feasibility: some rewrites help, but [N] queries that Rbot 'improved' by latency on xlarge now timeout on large due to increased resource pressure."

**Phase C — The overnight pipeline simulation**:

"To simulate a realistic deployment scenario, we execute all 99 queries as a serial pipeline (simulating an overnight batch workload) and measure total wall-clock completion time and total resource cost."

| Instance | System | Pipeline Completes? | Wall-Clock Time | Effective Cost |
|---|---|---|---|---|
| xlarge | Baseline | Yes | [T₁] | [T₁ × $0.342] |
| large | Baseline | No ([N] timeouts) | DNF | — |
| large | Rbot | Partial ([M] timeouts) | DNF | — |
| **large** | **[SYSTEM]** | **Yes** | **[T₂]** | **[T₂ × $0.171]** |

"Effective cost of the optimised pipeline on large: $[X]. Baseline on xlarge: $[Y]. Reduction: [Z]%."

This is the number a CTO reads.

#### §5.3 Experiment 3: Latency vs Feasibility Divergence (the thesis proof)

**Purpose**: Show SPECIFIC queries where latency-optimal ≠ tier-feasible. This is the systems insight that survives even if a reviewer dislikes LLMs.

**3–5 case studies**, each in a compact box:

```
┌─────────────────────────────────────────────────────────────────┐
│ CASE STUDY: Query Q67                                           │
│                                                                 │
│ Plan failure mode: Aggregate-below-join                         │
│ Original: 180s on xlarge | TIMEOUT on large (spill: 3.2GB)     │
│                                                                 │
│ Rbot rewrite:                                                   │
│   Approach: Materialized CTE + parallel hash join               │
│   xlarge: 95s (1.9× faster) ✓                                  │
│   large:  TIMEOUT (spill: 4.1GB — WORSE) ✗                     │
│   Resource impact: +28% peak memory, +0.9GB spill              │
│                                                                 │
│ [SYSTEM] rewrite:                                               │
│   Approach: Pre-aggregate before join (catalog treatment T₃)    │
│   xlarge: 120s (1.5× faster)                                   │
│   large:  145s (spill: 0GB) ✓                                  │
│   Resource impact: −62% peak memory, eliminated spill           │
│                                                                 │
│ WHY THEY DIVERGE: Rbot's materialized CTE accumulates the full │
│ join result (2.3M rows × 47 cols) before aggregating. On xlarge │
│ this fits in memory. On large it spills. [SYSTEM] pre-aggregates│
│ (reducing to 12K rows) BEFORE joining, so the join result never │
│ exceeds memory budget.                                          │
│                                                                 │
│ By latency: Rbot wins (95s vs 120s on xlarge)                   │
│ By tier feasibility: [SYSTEM] wins (completes on large; Rbot    │
│ does not)                                                       │
└─────────────────────────────────────────────────────────────────┘
```

**Figure 4: Quadrant scatter plot**

X-axis: latency change (speedup ratio, log scale)
Y-axis: spill bytes change (positive = more spill, negative = less)

```
              more spill
                  │
    BRITTLE       │      WASTEFUL
    (faster but   │      (slower AND
     spills more) │       spills more)
                  │
  ────────────────┼──────────────────
                  │
    IDEAL         │      CONSERVATIVE
    (faster AND   │      (slower but
     spills less) │       spills less)
                  │
              less spill
```

Plot Rbot points and [SYSTEM] points. Expected pattern: [SYSTEM] clusters in lower-left (ideal). Rbot splits between lower-left (when it works) and upper-left (brittle — faster but more spill). The upper-left points are the queries that fail on the smaller tier.

"[N] of Rbot's rewrites fall in the 'brittle' quadrant: faster on the unconstrained tier, but with increased resource pressure that prevents tier feasibility. [M] of [SYSTEM]'s rewrites fall in this quadrant — the gating mechanism blocks treatments with documented resource-hostile impact."

#### §5.4 Experiment 4: Fleet Triage Economics

**Setup**: All 99 queries, fleet pipeline.

**Show the three tiers**:

| Tier | Action | Queries Routed | Tokens Used | Improvement Captured |
|---|---|---|---|---|
| 1: Fleet-level | Config + indexes + clustering | All (shared) | [~80K] (1 analysis) | [~30]% of total |
| 2: Light per-query | Single-pass diagnosis + direct gold example | [~60] queries | [~300K] | [~35]% of total |
| 3: Deep per-query | Full pipeline (4W + sniper) | [~15] queries | [~700K] | [~35]% of total |
| **Total** | | **99 queries** | **~1.1M tokens** | **~100%** |
| Comparison: all tier 3 | Full pipeline for all | 99 queries | ~4.5M tokens | ~100% |

**Figure 5: Diminishing returns curve**

X-axis: cumulative tokens spent
Y-axis: cumulative improvement achieved (% of total possible)

Show the steep early curve (fleet actions + light optimization) flattening as deep optimization handles the long tail. Mark the tier boundaries on the curve.

"Fleet triage achieves [85]% of per-query optimization quality at [25]% of the LLM compute cost. For a deployment with [N] queries, the total optimization cost is approximately $[X] in API tokens."

#### §5.5 Experiment 5: Multi-Engine Generalization

**Setup**: Repeat Experiments 1–2 on DuckDB with `SET memory_limit` as the tier analog.

| Engine | [SYSTEM] Win Rate | Rbot Win Rate | Top Failure Modes |
|---|---|---|---|
| Postgres 16 | [X]% | [Y]% | decorrelate, date_cte_isolate, ... |
| DuckDB 1.1 | [A]% | [B]% | single_pass_aggregation, date_cte_isolate, ... |

"The same pipeline architecture with different engine catalogs achieves comparable effectiveness across engines, but the winning failure modes differ — confirming that engine-specific knowledge, not the pipeline, is the primary value driver."

**DuckDB tier feasibility** (using memory_limit):

| Memory Limit | Baseline Completion | [SYSTEM] Completion |
|---|---|---|
| 8GB (comfortable) | ~99% | ~99% |
| 4GB (constrained) | ~75% | [~95]% |
| 2GB (tight) | ~50% | [~80]% |

#### §5.6 Experiment 6: Ablation Study

| Configuration | Win Rate | Tier Feasibility (large) | Regression Rate |
|---|---|---|---|
| Full system | [X]% | [A]% | [E]% |
| − engine catalog | [G]% | [??]% | [I]% |
| − plan diagnosis (SQL-only) | [??]% | [??]% | [??]% |
| − resource gating | [X]% (same wins) | [lower]% | [same]% |
| − worker family diversity | [??]% | [??]% | [??]% |
| − validation oracle | [X]% (same wins) | [A]% | [higher]% |
| − sniper refinement | [lower]% | [??]% | [??]% |

Critical rows:
- **− catalog**: proves engine knowledge is decisive (expect largest drop)
- **− resource gating**: wins stay the same but tier feasibility drops (proves gating is what makes right-sizing work — without it, we win on latency but lose on feasibility, same as Rbot)
- **− plan diagnosis (SQL-only)**: proves reasoning from plans beats reasoning from SQL

The **− resource gating** ablation is the one that proves the thesis most directly. It shows our system WITHOUT resource awareness becomes Rbot — same wins on latency, same brittleness on smaller tiers.

---

### §6. The Concurrency Question (1 page)

**Separate from the main experiments. Framed as robustness validation, not a core contribution.**

"Infrastructure right-sizing assumes the optimized workload remains feasible under concurrent load. We validate this with a stress test: [10] randomly-selected TPC-DS queries executed in parallel on the target tier."

| Instance | System | Concurrent Streams | Completion Rate | Spill Events |
|---|---|---|---|---|
| large | Baseline | 4 parallel | [??]% | [many] |
| large | [SYSTEM] | 4 parallel | [??]% | [??] |
| large | [SYSTEM] | 10 parallel | [??]% | [??] |

"Under 4-way concurrency on the target tier, [SYSTEM]'s rewrites maintain [??]% feasibility. Memory pressure increases linearly with concurrency — the optimized queries have sufficient headroom due to spill avoidance. [N] queries require concurrency-specific treatment (typically: reduce parallel worker count to stay within shared memory budget)."

Note: "Concurrency-aware optimization — where treatments consider not just the query's own resource budget but contention with concurrent queries — is future work. The current system optimizes queries independently; the concurrency test validates that independent optimization provides sufficient headroom."

This is honest, addresses the reviewer concern, and positions future work without over-claiming.

---

### §7. Discussion (1.5 pages)

**§7.1 When Latency and Feasibility Diverge**

Characterize the query properties: large intermediate results, materialization-heavy plans, parallel-worker memory multiplication. Quantify: "[N]/99 queries show divergence. On these queries, latency-optimal rewrites fail tier feasibility [M]% of the time."

**§7.2 Catalog Maintenance and Scalability**

Construction cost: "[N] failure modes from [M] benchmark runs over [T] person-weeks."
Reuse: "Same catalog covers all customer workloads on the same engine version."
Stability: "[evidence if available, future work if not]."
Concentration: "Top [K] failure modes account for [P]% of wins. The catalog does not need to be exhaustive."

**§7.3 Negative Results**

Be explicit about where the system doesn't help:
- Queries with no matching failure mode ([B]/99 on Postgres)
- Queries where the optimizer already produces a near-reference plan
- Queries where the bottleneck is I/O, not plan quality
- Edge cases where catalog gating is too conservative (blocks a treatment that would have helped)

"On [B] queries with no catalog match, [SYSTEM] achieves [??]% win rate — comparable to generic rewriting. The catalog's value is concentrated on queries with matching failure modes, where win rate jumps from [??]% to [X]%."

**§7.4 Limitations**

- TPC-DS is synthetic. We do not claim direct transfer to production workloads. The contribution is the methodology and the tier-feasibility objective, both of which apply to any workload.
- Catalogs require upfront investment per engine. Construction cost is documented (§3.3).
- Concurrency is validated but not optimized for (§6).
- The logic oracle (mini-DuckDB) may miss dialect-specific semantic differences for non-standard SQL.
- Fleet triage requires query frequency/cost data. Cold-start deployments lack this signal.

---

### §8. Conclusion (0.5 pages)

Restate thesis. Restate key numbers. Future work:
- Automated catalog construction (reduce manual analysis)
- Concurrency-aware treatment selection
- Catalog transfer across engine versions
- Production deployment study

---

## FIGURES AND TABLES (storyboard)

Every figure has ONE message. If a reviewer glances at it for 5 seconds, they get the point.

| Figure | Message | Type |
|---|---|---|
| Fig 1 | Generic rewriting reasons from SQL; we reason from plans + catalogs | Side-by-side architecture contrast |
| Fig 2 | **Our system makes the workload feasible on a smaller tier; Rbot doesn't** | Bar chart: completion rate by tier × system. THE headline figure. |
| Fig 3 | One concrete example: plan failure → catalog match → gated treatment → fixed plan | Annotated EXPLAIN walkthrough |
| Fig 4 | Rbot rewrites cluster "faster but more spill"; ours cluster "faster AND less spill" | Quadrant scatter: latency change vs spill change |
| Fig 5 | Fleet triage: 65% of benefit from 20% of tokens | Diminishing returns curve with tier boundaries |
| Fig 6 | A few failure modes account for most wins; catalog doesn't need to be huge | Bar chart: wins per failure mode, stacked by engine |

| Table | Content |
|---|---|
| Table 1 | Head-to-head vs Rbot: win rate, speedup, regressions (Experiment 1) |
| Table 2 | **Infrastructure right-sizing: completion rate by tier × system** (Experiment 2) |
| Table 3 | Fleet triage: tokens vs improvement by tier (Experiment 4) |
| Table 4 | Multi-engine: win rates + top failure modes per engine (Experiment 5) |
| Table 5 | Ablation: component removal impact (Experiment 6) |
| Table 6 | Catalog entries: structure + cross-engine comparison (Section 3) |

---

## REVIEWER ANTICIPATION + RESPONSES

| Concern | Response | Evidence |
|---|---|---|
| "TPC-DS is synthetic" | Industry standard benchmark, same as Rbot/LLM-R. Methodology transfers. Overnight pipeline simulation adds realism. | §5.2 Phase C |
| "Bag of tricks / doesn't generalize" | Concentration chart: top K failure modes → P% wins. Coverage table. Cross-engine replication. | §3.5, §5.5 |
| "LLMs hallucinate / unreliable" | Catalog GATES treatments. Anti-patterns blacklisted. Regression rate: [E]% vs Rbot's [B]%. Validation oracle catches semantic errors pre-benchmark. | §4.2, §4.4 |
| "Comparison is unfair / strawman" | Rbot reproduced on identical hardware. Published numbers matched within ±X%. Baseline parity documented. | §5.1 preamble |
| "Catalog requires manual work / expensive" | [N] failure modes, [T] person-weeks. One-time per engine version. Top K entries capture P% of value. | §3.3, §7.2 |
| "Concurrency breaks the story" | Validated under 4-way and 10-way concurrent load. Headroom from spill avoidance. Concurrency-aware selection is future work. | §6 |
| "Just use a bigger machine" | Right-sizing is the economics of cloud. 50% cost reduction × 365 days × N instances. The ROI is >10,000×. | §1 opening |
| "Resource gating is just threshold checking" | The insight isn't the mechanism, it's the OBJECTIVE. Optimizing for tier feasibility produces different rewrites than optimizing for latency. The divergence is empirically demonstrated. | §5.3, §5.6 (−gating ablation) |

---

## EXECUTION TIMELINE

```
WEEK 1-2: FOUNDATIONS
  ☐ Vocabulary lock — all team members aligned
  ☐ Claims table — each claim paired with metric + experiment + falsification
  ☐ Tier boundary identification — which Postgres instance sizes give clean cliff
  ☐ Resource telemetry instrumentation — spill bytes, temp IO, buffer usage
  ☐ Verify Rbot reproduction numbers are solid
  ☐ Rbot runtime metrics extracted (the unpublished data)

WEEK 2-4: CORE EXPERIMENTS  
  ☐ Experiment 1: head-to-head latency (Table 1) — beat them on their metric
  ☐ Experiment 2: tier feasibility (Table 2, Figure 2) — THE result
  ☐ Overnight pipeline simulation (Table 2, Phase C)
  ☐ Divergence case studies × 5 (§5.3 boxes)
  ☐ Figure 4: quadrant scatter (latency vs spill)

WEEK 3-5: DEPTH EXPERIMENTS (parallel)
  ☐ Experiment 4: fleet triage (Table 3, Figure 5)
  ☐ Experiment 5: DuckDB replication (Table 4) — SET memory_limit tiering
  ☐ Experiment 6: ablation study (Table 5) — especially −catalog and −gating
  ☐ Concentration chart: failure mode → wins (Figure 6)
  ☐ Coverage table: catalog match rate across TPC-DS (§3.5)

WEEK 4-5: CONCURRENCY + ROBUSTNESS
  ☐ Experiment 6 (§6): 4-way and 10-way concurrent stress test
  ☐ Repeatability: re-run Experiments 1-2 three times, report variance
  ☐ Negative results collection: where we don't help (§7.3)

WEEK 5-7: WRITING
  ☐ Figure storyboard: all 6 figures drafted with captions
  ☐ §1 Introduction: opening hook + Q67 example + contributions
  ☐ §3 Catalogs: formalism + cross-engine table + coverage
  ☐ §5 Evaluation: all experiments written up with tables/figures
  ☐ §6 Concurrency: stress test results
  ☐ §7 Discussion: divergence analysis + limitations (honest)

WEEK 7-8: HARDENING
  ☐ Hostile reviewer pass — adversarial read, attack every claim
  ☐ Claims-to-evidence audit — every claim has a table/figure citation
  ☐ Baseline parity documentation — one paragraph, airtight
  ☐ Reproducibility appendix — configs, scripts, parameters
  ☐ Final figures: professional quality, single-message each

WEEK 8: SUBMIT
  ☐ Final proofread
  ☐ Supplementary material (full per-query results, catalog listings)
  ☐ Artifact package (if required by venue)
```

---

## THE KNOCKOUT CHECKLIST (all must be YES before submission)

```
CORE THESIS
  ☐ Downsizing result is clean and repeatable (not a one-off)
  ☐ At least 3 divergence case studies are undeniable
  ☐ −catalog ablation shows material drop (proves engine knowledge is decisive)
  ☐ −gating ablation shows feasibility drop (proves resource awareness matters)

COMPETITIVE
  ☐ Beat Rbot on their metric (latency) — not just ours
  ☐ Baseline reproduction is documented and defensible
  ☐ Rbot's unpublished resource metrics show the brittleness we claim

ROBUSTNESS  
  ☐ Spill/temp IO measurement is robust (not just peak RSS)
  ☐ Concurrency stress test shows headroom
  ☐ Repeatability: variance across 3 runs is tight
  ☐ Negative results documented honestly

SCALABILITY
  ☐ Catalog concentration: top K failure modes → P% of wins
  ☐ Coverage table shows non-trivial match rate
  ☐ Fleet triage economics are compelling (85% benefit at 25% cost)
  ☐ Multi-engine (DuckDB) shows framework generalizes

PRESENTATION
  ☐ Figure 2 (downsizing bars) wins the room in 5 seconds
  ☐ Figure 4 (quadrant scatter) makes divergence visual
  ☐ Every claim in the abstract has a table/figure citation
  ☐ Vocabulary is consistent throughout (no "blind spots", no "optimal")
  ☐ Limitations section is honest — builds trust, doesn't undermine
```

---

## THE ONE SENTENCE THAT SELLS THE PAPER

For the intro, for the talk, for the tweet:

> "For the cost of a single API call, we make a $3,000/year database workload
> run on a $1,500/year machine — and we can prove that latency-optimized
> rewriting can't."
