# QueryTorque V1 Prompt Pack — Engineering Review

**Version:** V1_20260213  
**Reviewer:** External architecture review  
**Date:** 2026-02-13  
**Scope:** All 10 prompt types rendered against TPC-DS Q88  
**Context:** 78 DuckDB wins, 30 PG wins across production benchmarks  

---

## Status: SOTA. The job is to stay there.

The system works. The data shows it:

- Fan-out (4 workers) finds the winner 87% of the time
- No single worker dominates — every slot contributes, validating the 4-worker architecture
- W4 exploration produces the highest ceilings (5.25x DuckDB, 4428x PG)
- Sniper adds 13% overall but 38% on PG, earning its cost on harder queries
- DSR1 (single-worker single-worker mode) produces 27% of all DuckDB wins — more than any individual swarm worker

The prompt architecture is the competitive advantage. This review identifies incremental improvements that protect and extend what's working, not structural changes that risk breaking it.

---

## Section 1: Swarm Analyst (05) — The Critical Path

**Rating: Strong.** 87% of wins originate from the swarm analyst's strategy assignments.

### 1.1 Reasoning Chain (§6) — The Best Part

The 6-step CLASSIFY → EXPLAIN ANALYSIS → BOTTLENECK HYPOTHESIS → AGGREGATION TRAP CHECK → INTERVENTION DESIGN → LOGICAL TREE DESIGN chain is excellent. Three things make it work:

1. **Hypothesis-driven, not pattern-matching.** The prompt teaches reasoning, not lookup. The DIAGNOSE → HYPOTHESIZE → CALIBRATE flow in step 3 gives the LLM a first-principles scaffold rather than a decision tree.

2. **The CALIBRATE three-way branch (3c) prevents both over-application and paralysis.** Documented gap → USE evidence. Documented strength → STOP. No match → tag UNVERIFIED. This is exactly the right meta-reasoning structure.

3. **AGGREGATION TRAP CHECK as a standalone gate (step 4)** between hypothesis and implementation catches correctness errors before they propagate into worker briefings.

### 1.2 Improvements — Low risk, incremental

**1.2a — Q-Error graceful degradation (Priority: HIGH, Effort: LOW)**

The reasoning process references Q-Error routing 7+ times across §2, §4, and §6 as the "primary hypothesis anchor." But §2b-i can be absent (it's absent for Q88 in this sample). When it's missing, there's no fallback instruction — the LLM either skips its primary anchor silently or hallucinates.

Proposed fix (add to §6, Step 2, after the Q-Error routing paragraph):

```
If §2b-i Q-Error routing is not available for this query, use Path B
(Structural routing — §4 SYMPTOM ROUTING table) as your primary
hypothesis anchor. The structural symptoms are less precise but still
directional. Proceed through the same DIAGNOSE → HYPOTHESIZE →
CALIBRATE flow using EXPLAIN tree inspection instead of quantitative
Q-Error locus.
```

**1.2b — EXPLAIN tree compression for repeated-subtree queries (Priority: MEDIUM, Effort: MEDIUM)**

Q88's EXPLAIN tree produces 8 near-identical subtrees rendered as wide ASCII art (~250+ chars per line). The right-side children get truncated. The LLM cannot parse what it cannot see.

For queries with N repeated subtrees (detected by the tree formatter), consider a compressed representation:

```
EXPLAIN shows 8 nearly-identical subtrees connected via CROSS_PRODUCT.
Representative subtree (s1):
  UNGROUPED_AGGREGATE (count_star)
    └── HASH_JOIN (ss_hdemo_sk = hd_demo_sk) → ~30,905 rows
        ├── HASH_JOIN (ss_store_sk = s_store_sk) → ~154,525 rows
        │   ├── HASH_JOIN (ss_sold_time_sk = t_time_sk) → ~1,404,7xx rows
        │   │   ├── SEQ_SCAN store_sales
        │   │   └── FILTER time_dim (t_hour = 8 AND t_minute >= 30)
        │   └── FILTER store (s_store_sk <= 100)
        └── FILTER household_demographics ((hd_dep_count = 4) OR ...)

Variant across 8 subtrees: time_dim filter conditions only
  s1: t_hour=8, t_minute>=30    s2: t_hour=9, t_minute<30
  s3: t_hour=9, t_minute>=30    s4: t_hour=10, t_minute<30
  ...
  s8: t_hour=12, t_minute<30
```

This gives the LLM the full structural information in a parseable format. The current wide ASCII is visually impressive but functionally broken for this query shape.

**1.2c — CLASSIFY archetype list expansion (Priority: LOW, Effort: LOW)**

The current list of 7 archetypes doesn't cleanly match Q88's structure ("repeated independent star-joins with variant dimension filters"). The "repeated fact scan" archetype is close but misses the star-join aspect.

Add to the archetype list:

```
repeated independent star-join (N subqueries, same fact+dims, variant filter)
```

This is the P1 pattern expressed as a query archetype. The connection helps the analyst route to P1 faster.

### 1.3 Exploit Algorithm (§4) — Leave It Alone

The pathology catalog (P0–P9) with decision gates, win/regression data, safety rankings, and the pruning guide is the single strongest asset in the system. It encodes institutional knowledge in a format the LLM can use.

Two small additions that don't change existing content:

**1.3a — Composition patterns (Priority: LOW, Effort: LOW)**

The system says "composition is allowed and encouraged" but doesn't document which compositions have been tested. The best Q88 worker (6.24x) used P0+P1 (dimension isolation + single-pass aggregation). Add a brief section:

```
## TESTED COMPOSITIONS

| Combo | Query | Result | Pattern |
|-------|-------|--------|---------|
| P0+P1 | Q88   | 6.24x  | Isolate dims into CTEs + consolidate to single scan |
| P0+P3 | Q22   | 42.9x  | Push dims early + pre-aggregate before join |
| ...   | ...   | ...    | ... |
```

Populated from existing win data. No new experiments needed.

**1.3b — Global Guard #9 evidence (Priority: LOW, Effort: LOW)**

Guard #9 ("Convert comma joins to explicit JOIN...ON") is the only guard without a regression citation. Either add evidence or annotate it as a hygiene rule:

```
9. Convert comma joins to explicit JOIN...ON [HYGIENE — improves plan readability
   and enables PG optimizer join ordering; no measured regression from comma joins
   on DuckDB but PG's COMMA_JOIN_WEAKNESS makes this defensive best practice]
```

---

## Section 2: Worker Prompt (04, 07) — Tight and Effective

**Rating: Strong.** The role framing ("rewrite engine, not architect") correctly prevents strategy second-guessing.

### 2.1 Literal Arithmetic Ambiguity (Priority: HIGH, Effort: LOW)

The original query uses `hd_vehicle_count <= -1+2`. Different workers produce different forms:

- W2: `hd_vehicle_count <= -1 + 2` (preserves original arithmetic)
- W1: `hd_vehicle_count <= 1` (pre-computed, with comment `-- -1 + 2 = 1`)
- Worker prompt NODE_CONTRACTS: `hd_vehicle_count <= 1` (pre-computed, no comment)

The LITERAL_PRESERVATION constraint says "copy ALL literal values EXACTLY from the original" but doesn't address whether `-1+2` is a literal or an expression. Both workers pass validation (same result), so this isn't a correctness issue — but it's an inconsistency in the specification.

Proposed fix: Add to §3a LITERAL_PRESERVATION:

```
Arithmetic expressions in the original (e.g., <= -1+2, <= 4+2) SHOULD be
preserved in their original form. The optimizer evaluates constant expressions
at plan time, so there is no performance difference. Preserving the original
form makes the rewrite auditable against the source query.
```

This also aligns the NODE_CONTRACTS (which currently pre-compute) with the constraint (which says "copy exactly").

### 2.2 FILTER Clause Hazard Flag — Verify or Remove (Priority: LOW, Effort: LOW)

The worker prompt says "Do NOT use FILTER clause with COUNT — use COUNT(CASE WHEN ... THEN 1 END)." But the same prompt's preamble says "FILTER clause is native" for DuckDB.

If there's a known bug or version-specific issue, document it:
```
- Do NOT use FILTER clause: [DuckDB issue #XXXX / version X.Y incompatibility]
```

If this is defensive caution without empirical basis, consider removing it — FILTER is often more readable and the optimizer handles it identically to CASE WHEN.

### 2.3 Example Trimming — Long-Term (Priority: LOW, Effort: MEDIUM)

Workers receive full before/after SQL for 3 examples. For Q88, the `shared_dimension_multi_channel` example is ~100 lines showing a 3-channel ROLLUP pattern that's structurally distant from Q88's pattern. The worker must mentally extract the relevant sub-pattern (shared dimension CTEs) and ignore the rest.

Consider a "pattern extract" mode that shows only the relevant structural element:

```
### shared_dimension_multi_channel — RELEVANT PATTERN ONLY
BEFORE: Each channel CTE independently joins date_dim, item, promotion
AFTER:  Shared filtered_dates, filtered_items, filtered_promotions CTEs
        referenced by all channel CTEs

[Only the CTE definitions + one channel reference shown, not full query]
```

This is a prompt-generation-time optimization, not a prompt-content change.

---

## Section 3: Expert / DSR1 (03) — Highest ROI Improvement Target

**Rating: Good, but undertreated.** DSR1 produces 27% of all DuckDB wins — more than any individual swarm worker — yet its prompt is a minimal diff from the swarm analyst.

### 3.1 The Problem

The single-worker analyst prompt inherits the full swarm infrastructure:
- Transform catalog with diversity guidelines designed for 4 workers
- Strategy selection rules including "MAXIMIZE DIVERSITY" (changed to "MAXIMIZE EXPECTED VALUE" but everything else stays)
- Worker 4 exploration rules (irrelevant — there's no Worker 4)
- All 14 matched examples (the analyst must pick 1–3, but sees 14)

The single-worker analyst doesn't need to reason about diversity, exploration, or worker role differentiation. It needs to reason about depth: which single compound strategy has the highest expected value?

### 3.2 Proposed Expert-Specific Additions

Add to §6 reasoning process (single-worker mode only):

```
EXPERT MODE: You are assigning a single worker. Your reasoning budget should
prioritize DEPTH over BREADTH:

- Spend 60% of your analysis on the primary bottleneck: what is the single
  highest-cost operation, why does the optimizer handle it suboptimally, and
  what is the most effective intervention?
- Spend 30% on composition: does combining 2-3 transforms from different
  categories create compound benefit? The biggest wins (42.9x Q22, 6.24x Q88)
  are compound strategies.
- Spend 10% on risk assessment: what is the specific regression risk for
  your chosen strategy on THIS query?

Do NOT enumerate alternative strategies. Select the single best approach and
invest your full reasoning depth in specifying it precisely.
```

Add to §5a (single-worker mode only):

```
Select the 3 most applicable examples. Relevance over coverage — 3 closely
matched examples are better than 3 diverse but loosely matched ones.
```

**Effort: LOW.** These are conditional paragraphs gated on `mode="oneshot"`.

---

## Section 4: Fan-Out (06) — Working Well

**Rating: Strong.** The lightweight coordinator correctly allocates diverse strategies.

### 4.1 Minor: Link Regression Warnings to Strategies (Priority: LOW)

Current regression warnings are listed generically. Add explicit routing:

```
## Regression Warnings — Strategy-Specific

- regression_q90_materialize_cte: RELEVANT TO Worker 4 (Novel) — do not split
  same-column OR into UNION ALL
- regression_q25_date_cte_isolate: RELEVANT TO Worker 2/3 — do not pre-filter
  fact tables when 3+ fact joins exist
- regression_q95_semantic_rewrite: RELEVANT TO Worker 4 (Novel) — do not
  decompose correlated EXISTS pairs
```

---

## Section 5: Snipe Analyst (08) — Strong with One Gap

**Rating: Strong.** The post-hoc diagnostic framework extracts genuine signal from empirical results.

### 5.1 Handle the All-Pass Case (Priority: MEDIUM, Effort: LOW)

The diagnostic task says: "Why did the best worker achieve Xx instead of the 2.0x target?" This is framed for failure analysis. When all 4 workers pass (as in Q88: 5.27x–6.24x), the prompt's failure-oriented framing becomes awkward — there are no failures to diagnose.

Add to the task description:

```
If all workers reached target:
1. DIAGNOSE the spread — why did the best (6.24x) outperform the worst (5.27x)?
   What structural difference in their approach caused the gap?
2. IDENTIFY ceiling — is there remaining headroom beyond the best result?
   Look for: dimensions not yet pre-filtered, joins not yet reordered,
   scans not yet eliminated.
3. SYNTHESIZE micro-optimizations — if the macro structure is solved,
   guide the sniper toward: join order tuning, column projection reduction,
   CTE inlining vs materialization trade-offs.
```

---

## Section 6: Sniper (09, 10) — Well-Designed Iteration

**Rating: Strong.** "FULL FREEDOM" + best foundation + empirical context is the right combination.

### 6.1 Neutral Framing for Converged Cases (Priority: LOW, Effort: LOW)

Sniper iter2 receives: "Limited headroom for further improvement. A retry would focus on micro-optimizations." This can cause premature convergence — the sniper may produce a minimal-effort output.

Reframe:

```
Previous attempts converged in the 5.3x–6.2x range using similar structural
approaches (dimension isolation + single-pass aggregation). Evaluate whether:
(a) A fundamentally different structural approach exists that wasn't tried
(b) Micro-optimizations (join order, CTE inlining, column projection) could
    push beyond 6.2x
(c) The 6.2x represents the effective ceiling for this query structure
State your assessment explicitly before proceeding.
```

---

## Section 7: PG Tuner (11) — Underrated, High Potential

**Rating: Good but limited by missing inputs.** Config tuning alone produced 676x on PG — this prompt deserves more investment.

### 7.1 No-EXPLAIN Fallback (Priority: HIGH, Effort: LOW)

The prompt says "EXPLAIN ANALYZE Plan: Not available. Recommend parameters based on query structure." But all 8 analysis instructions reference specific EXPLAIN artifacts ("Look for 'Sort Method: external merge'", "If you see sequential scans").

Add a structural analysis mode:

```
## When EXPLAIN is Not Available

Analyze the query structure to infer likely plan characteristics:

1. COUNT comma-join tables per subquery: Q88 has 4 tables in comma-join × 8
   subqueries. PostgreSQL's COMMA_JOIN_WEAKNESS means the planner may produce
   suboptimal join orderings. Consider: from_collapse_limit, join_collapse_limit.

2. ESTIMATE total join operations: 8 subqueries × 3 joins each = 24 hash/sort
   operations. Size work_mem accordingly: 24 ops at current 64MB = 1.5GB peak.
   This is within the 2GB shared_buffers budget but aggressive.

3. CHECK for GEQO risk: 4 tables per subquery is below default geqo_threshold (12),
   so GEQO should not trigger. But if the planner flattens the 8 scalar subqueries
   into a single planning unit, the effective FROM count could be higher.

4. ASSESS parallel opportunity: 8 independent scalar subqueries could each
   benefit from parallel scans on store_sales. Consider max_parallel_workers_per_gather.

5. EVALUATE JIT overhead: 8 subqueries with similar expressions = high expression
   count. JIT compilation overhead may exceed benefit on sub-2s queries.
```

### 7.2 Comma-Join Specific Guidance (Priority: MEDIUM, Effort: LOW)

Q88 uses comma-separated FROM tables exclusively. Add to the analysis section:

```
COMMA JOIN DETECTED: This query uses implicit comma joins (FROM t1, t2, t3
WHERE ...). PostgreSQL's cost model is significantly weaker on comma-joins.
Consider:
- from_collapse_limit: increase to ensure all subquery tables are flattened
  into the main planning context (current default may be too low for 4-table
  comma joins inside scalar subqueries)
- join_collapse_limit: increase to match, ensuring the planner considers all
  join orderings
- random_page_cost: with 8 repeated fact table scans, index vs. sequential
  scan choice is amplified 8×. If the system uses SSD, 1.0-1.5 is appropriate.
```

---

## Section 8: Cross-Cutting Observations

### 8.1 Worker Role Calibration Based on Win Data

The win distribution data suggests two calibration opportunities:

**W1 (Conservative) wins 22% within swarm — lowest.** The "proven patterns, low risk" framing produces safe but unremarkable results. Consider reframing W1 as the **minimal-change baseline**: explicit JOINs only, no CTE restructuring, no scan consolidation. Strategy selection rule 6 already describes this role. Making it W1's default gives the swarm a regression-safe control that also occasionally wins (simple queries where overhead is the enemy).

**W2 (Moderate) wins 37% within swarm — highest by a wide margin.** This validates that dimension isolation + CTE restructuring is the sweet spot. No changes needed — W2's current positioning is empirically optimal.

The fan-out prompt (06) hardcodes the diversity spectrum as Conservative/Moderate/Aggressive/Novel. The data suggests the spectrum could be rebalanced:

```
Current:  Conservative | Moderate | Aggressive | Novel
Consider: Minimal-change | Moderate-A | Moderate-B | Exploration
```

Where Moderate-A and Moderate-B are two different moderate-risk approaches (e.g., dimension isolation vs. single-pass aggregation). This gives the sweet spot more coverage without sacrificing the exploration slot.

**Caution:** This is the highest-risk suggestion in this review. The current 4-worker architecture produces wins from all slots. Rebalancing could reduce W3's 24% contribution. Only consider this after A/B testing on a representative query set.

### 8.2 Validation on Unseen Queries

Q88 is a strong demonstration query but the exploit algorithm contains "Q88 6.24x" in its win list. The LLM has the answer key for this query. The prompt architecture's real value — its ability to reason about novel queries — can't be assessed from this sample alone.

**Recommendation:** Generate a second sample pack for a query that exercises the "NO MATCH — First-Principles Reasoning" path in §4. This would demonstrate how the reasoning chain performs without a pre-encoded answer. Candidates: any query not in the pathology win lists, ideally one with a non-obvious bottleneck.

### 8.3 Token Budget Alignment

Two minor inconsistencies in the current prompts:

| Location | Says | Should be |
|----------|------|-----------|
| §7a output format | SEMANTIC_CONTRACT: 80–150 tokens | Align with checklist |
| §7b validation checklist | SEMANTIC_CONTRACT: 30–250 tokens | Align with format |

Pick one range and use it in both places. 50–200 tokens accommodates both simple queries (Q88) and complex ones.

### 8.4 Negative Example — Consider Adding One

The system provides extensive positive examples (before/after SQL for wins) but no rendered negative example showing what a regression looks like in practice. The regression registry describes failures textually, but showing a concrete before/after where the "after" is slower could help workers develop better intuition for anti-patterns.

Candidate: The Q90 regression (0.59x) where a same-column OR was split into UNION ALL. The before/after SQL is short, the mistake is clear, and it directly reinforces Global Guard #2.

This is a low-priority enrichment, not a structural change.

---

## Prioritized Action Items

Items are ordered by (impact × confidence / effort). All are additive — none require changing existing content that works.

| # | Item | Section | Risk | Effort | Expected Impact |
|---|------|---------|------|--------|-----------------|
| 1 | Q-Error graceful degradation | §6 Step 2 | None | 3 lines | Prevents silent reasoning failure when §2b-i is absent |
| 2 | Literal arithmetic standardization | §3a | None | 2 lines | Eliminates inconsistency between constraint and NODE_CONTRACTS |
| 3 | PG tuner no-EXPLAIN fallback | PG tuner | None | 15 lines | Enables config recommendations when EXPLAIN unavailable |
| 4 | single-worker mode depth-over-breadth guidance | §6 single-worker | None | 8 lines | May improve DSR1's already-leading 27% win rate |
| 5 | EXPLAIN tree compression for repeated subtrees | §2b formatter | Low | Code change | Gives LLM parseable structure instead of truncated ASCII |
| 6 | Snipe analyst all-pass handling | Snipe §3 | None | 6 lines | Better diagnostic framing when all workers succeed |
| 7 | Composition patterns table | §4 | None | Table addition | Documents tested transform combinations |
| 8 | SEMANTIC_CONTRACT token budget alignment | §7a/§7b | None | 1 line | Removes spec inconsistency |
| 9 | Regression warnings → strategy routing in fan-out | Fan-out | None | 3 lines | Directs warnings to the workers they apply to |
| 10 | Validate on an unseen query | Testing | None | Generate sample | Confirms reasoning chain works without answer key |

---

*This review is based on the V1_20260213 prompt pack rendered against Q88, the ARCHITECTURE.md design documentation, and the production win distribution data across 78 DuckDB and 30 PostgreSQL benchmarked queries.*
