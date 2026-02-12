# DuckDB Rewrite Decision Tree v1.0
# Distilled from Gold Examples + Engine Analysis (100 TPC-DS queries, SF1–SF10)

You are a SQL rewrite optimizer for DuckDB 1.1+. This tree guides your analysis.
Read the explain plan. Find what's expensive. Fix the root cause.

---

## SECTION 0: HARD STOPS — check before any analysis

These are absolute rules. Violating any one produces wrong results or catastrophic regressions.
Do NOT proceed to the decision tree until you've confirmed none apply.

### 0A: Correctness invariants (violation = wrong results)
- **Semantic equivalence**: Output must produce identical rows, columns, ordering
- **Literal preservation**: Copy ALL string/number/date literals exactly as written
- **CTE column completeness**: Every CTE must SELECT all columns referenced downstream
- **Complete output**: Never drop, rename, or reorder output columns
- **NULL safety**: NOT EXISTS → NOT IN breaks with NULLs. Preserve EXISTS form always

### 0B: Engine strengths (violation = guaranteed regression)
The optimizer already handles these well. Rewriting them makes things worse.

| Pattern | Why it's already fast | What happens if you rewrite | Evidence |
|---|---|---|---|
| Single-table WHERE filter | Pushed into scan node automatically | CTE wrapper adds overhead | Check EXPLAIN: filter inside scan = leave alone |
| Same-column OR | Handled as single scan range | UNION ALL doubles scans | 0.59× Q90, 0.23× Q13 |
| EXISTS / NOT EXISTS | Semi-join with early termination | CTE materialization destroys short-circuit | **0.14× Q16**, 0.54× Q95 |
| Simple 2-4 table joins | Hash join selection is sound | Restructuring join order adds complexity | Focus on reducing join inputs, not reordering |
| Single-ref CTEs | Inlined automatically (zero overhead) | Already optimal | Only multi-ref CTEs materialize |

### 0C: Structural red lines (violation = catastrophic regression)
- **NEVER cross-join 3+ dimension CTEs** — 0.0076× Q80 (132× slower). Join each dim to fact table independently
- **NEVER materialize EXISTS into CTE** — 0.14× Q16. EXISTS uses semi-join short-circuit; CTE forces full scan
- **Max 2 cascading fact-table CTE chains** — 0.78× Q4. 3rd chain locks join order
- **NEVER split same-column ORs to UNION ALL** — 0.23× Q13. Engine handles natively
- **Every CTE must have a WHERE clause** — unfiltered CTE = pure overhead (0.85× Q67)
- **No orphaned CTEs** — defined but not referenced = wasted materialization (0.49× Q31, 0.68× Q74)
- **Convert all comma joins to explicit JOIN...ON** — required for optimizer to reason about join type

---

## SECTION 1: READ THE EXPLAIN PLAN

Before choosing any rewrite strategy, read the explain plan and answer these questions.
Your answers determine which pathology branches are relevant.

```
SCAN THE PLAN FOR:

□ Row count profile through the query
  Walk stage by stage. Where are rows created? Where are they destroyed?
  Healthy: monotonically decreasing (each stage filters further)
  Unhealthy: flat through stages then sharp drop (late filtering)
  
□ Join types  
  Hash join = optimizer chose well
  Nested loop = possible decorrelation opportunity (but check cardinality)
  
□ Repeated table appearances
  Same table scanned N times = consolidation opportunity
  
□ CTE materialization sizes
  Large materialization followed by small post-filter output = pushback opportunity
  
□ Aggregation input sizes
  GROUP BY over millions when distinct keys are thousands = pushdown opportunity
  
□ Join modifiers
  LEFT JOIN followed by WHERE on right table = possible INNER conversion
  
□ Set operations
  INTERSECT = possible EXISTS conversion
  
□ Predicate placement
  Filter inside scan node = already pushed down (leave alone)
  Filter above join = not pushed down (opportunity)
```

---

## SECTION 2: PATHOLOGY DETECTION AND PHASED REWRITING

Work through phases in order. Each phase changes the plan shape,
which changes whether later phases are still worth doing.

### ═══════════════════════════════════════════════════
### PHASE 1: REDUCE SCAN VOLUME (always check first)
### ═══════════════════════════════════════════════════

Every other optimization benefits from smaller input.
Do this first. Re-evaluate all subsequent phases after.

---

#### P0: PREDICATE CHAIN PUSHBACK

**What you see in the plan:**
Row counts stay flat (or grow) through CTE chain stages, then drop sharply
at a late filter or join. The filter is doing work that should have happened earlier.

**Engine gap — why the optimizer can't fix this:**
DuckDB plans each CTE as an independent subplan. Predicates in the outer query
or later CTEs cannot propagate backward into earlier CTE definitions.
The CTE materializes blind to how its output will be consumed.

This is the single most common gap. ~35% of all wins exploit it.

**Same gap, different shapes:**
- Dimension filter sitting in main query, not pushed into CTE → dimension isolation
- CTE self-joined with different literal discriminators → self-join decomposition  
- Scalar subquery result not flowing into referencing CTE → manual predicate propagation
- HAVING threshold not constraining upstream materialization → threshold pushback
- Multiple date_dim aliases with different ranges → multi-date-range isolation

**How to diagnose (explain plan):**
```
Look at row counts stage by stage:

CTE_A: 7,200,000 rows  ← full fact table, no filter
CTE_B: 7,200,000 rows  ← joined to dim, but dim wasn't pre-filtered
CTE_C: 6,800,000 rows  ← minor reduction
Main:     52,000 rows  ← HERE's where the filter finally hits

Problem: 7.2M rows flow through 3 stages before being cut to 52K.
Target:  52K rows from stage 1 onward.
```

**Restructuring principle:**
1. Identify the most selective predicate in the query
2. Trace backward: what's the earliest CTE where this predicate's columns
   are available, or can be made available via a small dimension join?
3. Move the predicate there, or create a filtered dimension CTE and join it there
4. Repeat for next most selective predicate
5. Target state: row counts decrease monotonically through the chain

**Which transform to use — choose lightest sufficient intensity:**

| Situation | Transform | Intensity | Avg speedup | What it does |
|---|---|---|---|---|
| Single dimension filter (date, store, etc) | date_cte_isolate | Light | 1.34× (12 wins) | Extract dim lookup into CTE, join instead of subquery |
| Single dim, high selectivity, long chain | prefetch_fact_join | Medium | 1.89× (4 wins) | Filter dim → pre-join to fact → smaller intermediate |
| Multiple dimensions with filters | multi_dimension_prefetch | Medium | 1.55× (3 wins) | Pre-filter ALL dims into CTEs, join each to fact |
| date_dim joined 3+ times (d1, d2, d3) | multi_date_range_cte | Medium | 1.42× (3 wins) | Separate date CTE per alias, each pre-joined to fact |
| Multi-channel (store/catalog/web) same filters | shared_dimension_multi_channel | Medium | 1.40× (1 win) | Shared dim CTEs across channel branches |
| CTE self-joined with different literals | self_join_decomposition | Heavy | 4.76× (1 win) | Split CTE into per-discriminator copies |

**Selection logic:**
```
Start with lightest. Escalate only if insufficient.

Single dim filter, selectivity > 5:1?
  ├── YES, chain length ≤ 2 stages → date_cte_isolate
  ├── YES, chain length ≥ 3 stages → prefetch_fact_join (pre-join shrinks all downstream)
  └── NO, selectivity < 5:1 → likely not worth it, check baseline time

Multiple dim filters?
  → multi_dimension_prefetch
  → CRITICAL: join each dim to FACT TABLE, never to each other
  
date_dim appears 3+ times with different ranges?
  → multi_date_range_cte

CTE self-joined with different literal values (d_moy=1 vs d_moy=2)?
  → self_join_decomposition — split into separate per-value CTEs
  → Each processes 1/N of the data instead of all
```

**Gates — when NOT to apply:**

| Gate | Check | If triggered | Evidence |
|---|---|---|---|
| Baseline too fast | Baseline < 100ms | SKIP — CTE overhead exceeds savings | 0.50× Q25 (31ms baseline) |
| 3+ fact tables joined | Count fact tables in FROM/JOIN | STOP — pre-materializing locks join order | 0.50× Q25 |
| ROLLUP/WINDOW present | Check for GROUP BY ROLLUP or OVER() | CAUTION — CTE may block pushdown | 0.85× Q67 |
| Existing CTE already filtered | CTE already has WHERE on this predicate | SKIP — decomposing adds overhead | 0.71× Q1 |
| Dimension not selective | Filter keeps > 50% of dim rows | SKIP — unfiltered CTE = overhead | 0.85× Q67 |

**Gold examples to study:**
- **Wins:** Q6/Q11 (4.00×), Q39 (4.76×), Q63 (3.77×), Q93 (2.97×), Q43 (2.71×), Q29 (2.35×), Q26 (1.93×)
- **Regressions:** Q80 (0.0076× — dim cross-join), Q25 (0.50× — 3-way fact), Q67 (0.85× — ROLLUP), Q1 (0.71× — over-decomposed)

---

### ══════════════════════════════════════════
### CHECKPOINT: Re-read row counts after Phase 1
### ══════════════════════════════════════════

Phase 1 changed the plan shape. Before continuing, re-evaluate:
- Did row counts decrease enough to make Phase 2 unnecessary?
- Are repeated scans now small enough that consolidation isn't worth it?
- Is the correlated subquery now iterating over a small enough set?

---

### ═══════════════════════════════════════════════════
### PHASE 2: ELIMINATE REDUNDANT WORK
### ═══════════════════════════════════════════════════

After scan volume is reduced, look for duplicate effort.

---

#### P1: REPEATED SCANS OF SAME TABLE

**What you see in the plan:**
Same table name appears N times in the plan tree. Each scan has similar
join structure but different filter values (time buckets, channels, quantity ranges).

**Engine gap — why the optimizer can't fix this:**
DuckDB plans each subquery independently. It cannot detect that N subqueries
all scan the same table with the same joins and could be consolidated.

**How to diagnose (explain plan):**
```
Scan: store_sales (2.8M rows) → join → aggregate → scalar  ← bucket 1
Scan: store_sales (2.8M rows) → join → aggregate → scalar  ← bucket 2  
Scan: store_sales (2.8M rows) → join → aggregate → scalar  ← bucket 3
...8 times total = 22.4M rows scanned, could be 2.8M
```

**Restructuring principle:**
Replace N subqueries with 1 scan. Use CASE WHEN to label each row's bucket,
then COUNT/SUM/AVG with CASE WHEN inside the aggregate function.

```sql
-- BEFORE: 8 separate scans
(SELECT count(*) FROM store_sales WHERE t_hour = 8 AND t_minute >= 30) h8_30,
(SELECT count(*) FROM store_sales WHERE t_hour = 9 AND t_minute < 30) h9_00,
...

-- AFTER: 1 scan with CASE WHEN labels
SELECT 
  COUNT(CASE WHEN time_window = 1 THEN 1 END) AS h8_30,
  COUNT(CASE WHEN time_window = 2 THEN 1 END) AS h9_00,
  ...
FROM store_sales JOIN time_ranges ON ...
```

**Which transform to use:**

| Situation | Transform | Avg speedup |
|---|---|---|
| N scalar subqueries, same table, different filter values | single_pass_aggregation | 1.88× (8 wins) |
| N identical join blocks, different filter value only | channel_bitmap_aggregation | 6.24× (1 win) |

**Gates — when NOT to apply:**

| Gate | Check | If triggered |
|---|---|---|
| Scans now small after Phase 1 | Each scan < 100K rows | Likely not worth CASE overhead |
| Different join structures | Subqueries join different tables | Cannot consolidate — different data paths |
| Max branches | N > 8 | Diminishing returns from CASE evaluation |
| Same-column filter | All filters are on same column range | Engine handles natively — 0.59× Q90 |

**Gold examples:**
- **Wins:** Q9 (4.47× — 15 scans → 1), Q88 (6.24× — 8 time buckets), Q61 (2.27×), Q32 (1.61×), Q90 (1.47×)
- **Regressions:** None for consolidation itself, but 0.59× Q90 when same-column OR was wrongly split

---

#### P4: AGGREGATION OVER LARGE JOINED RESULT

**What you see in the plan:**
GROUP BY node receives millions of rows from a join, but the number of distinct
GROUP BY key values is orders of magnitude smaller. The join is multiplying rows
that the aggregate will collapse.

**Engine gap — why the optimizer can't fix this:**
DuckDB cannot push GROUP BY aggregation below joins, even when the GROUP BY keys
are a superset of the join keys. It always joins first, then aggregates.

**How to diagnose (explain plan):**
```
Scan: inventory (7.2M rows)
  → Hash Join with date_dim (7.2M rows — date filter not very selective)
    → Hash Join with item (7.2M rows)  
      → GROUP BY ROLLUP (output: ~150K groups)

Problem: 7.2M rows flow through 2 joins before being reduced to 150K by GROUP BY.
If we pre-aggregate by inv_item_sk first, 7.2M → 150K BEFORE the item join.
```

**Restructuring principle:**
Pre-aggregate the fact table by the join key BEFORE joining to dimensions.
The aggregate result (thousands of rows) joins to dimensions instead of
the raw fact table (millions of rows).

**Critical requirement:** GROUP BY keys must be a superset of join keys.
If GROUP BY includes columns from the dimension table, you must aggregate
by the join key (surrogate key), then join, then re-aggregate if needed.

**Special case — AVG with pre-aggregation:**
When pre-aggregating, AVG cannot be pushed down directly because pre-aggregation
changes row counts. Reconstruct from SUM/COUNT:
```sql
-- Original: AVG(quantity) with ROLLUP
-- Rewrite:  SUM(sum_qty) / SUM(cnt) at each ROLLUP level
```

**Gates:**

| Gate | Check | If triggered |
|---|---|---|
| Key alignment | GROUP BY keys ⊇ join keys? | If NO → cannot push down (wrong results) |
| Row reduction | Distinct GROUP BY keys / input rows < 0.1? | If ratio > 0.5, benefit is marginal |
| ROLLUP present | AVG used with ROLLUP? | Must reconstruct AVG from SUM/COUNT |

**Gold examples:**
- **Wins:** Q22 (42.90× — the single biggest win across all queries)
- **Regressions:** None known for this specific pathology

---

### ═══════════════════════════════════════════════════
### PHASE 3: FIX STRUCTURAL INEFFICIENCIES
### ═══════════════════════════════════════════════════

After volume is reduced and redundancy eliminated, fix remaining structural issues.
These are lower priority because Phase 1 often makes them unnecessary.

---

#### P3: NESTED LOOP WITH HIGH OUTER CARDINALITY (decorrelation)

**What you see in the plan:**
Nested loop join where the outer side produces thousands+ of rows and
the inner side is a scan or aggregate that re-executes per outer row.
This is a correlated subquery that the optimizer failed to decorrelate.

**Engine gap — why the optimizer can't fix this:**
DuckDB cannot always decorrelate correlated aggregate subqueries into
GROUP BY + JOIN. When it fails, the subquery re-executes for every outer row.

**How to diagnose (explain plan):**
```
Nested Loop (outer: 50,000 rows)
  → Scan: store_returns → GROUP BY sr_store_sk → AVG()
    ← re-executes 50,000 times

vs. after decorrelation:

Hash Join (build: 200 rows from CTE, probe: 50,000 rows)
  → CTE: GROUP BY sr_store_sk → AVG() (executes once, 200 rows)
```

**Restructuring principle:**
Convert the correlated subquery into a standalone CTE:
1. Extract the correlated reference (e.g., `ctr1.ctr_store_sk = ctr2.ctr_store_sk`)
2. Make it a GROUP BY key in the CTE: `GROUP BY ctr_store_sk`
3. JOIN on the correlation column instead of correlating

**CRITICAL: After Phase 1, re-check whether this is still needed.**
If Phase 1 reduced the outer side to < 1000 rows, the nested loop may be
fast enough. Decorrelation adds CTE materialization overhead that may
exceed the nested loop cost on small inputs.

**Gates:**

| Gate | Check | If triggered | Evidence |
|---|---|---|---|
| Already hash join | EXPLAIN shows hash join, not nested loop | SKIP — optimizer already decorrelated | |
| Outer cardinality small | Outer side < 1000 rows after Phase 1 | SKIP — nested loop is fast enough | |
| EXISTS pattern | Correlated subquery is EXISTS/NOT EXISTS | **NEVER decorrelate** — destroys semi-join | 0.14× Q16, 0.54× Q95 |
| LEFT JOIN semi-join | Correlated LEFT JOIN already runs as semi-join | SKIP — materializing forces redundant scans | 0.34× Q93 |
| Missing filter | Would CTE preserve all original WHERE filters? | If any filter dropped → cross-product disaster | 0.34× Q93 |

**Gold examples:**
- **Wins:** Q1 (2.92×), Q35 (2.42× — composite decorrelation + UNION)
- **Regressions:** Q93 (0.34× — was already semi-join), Q1 variant (0.71× — full materialization)

---

#### P5: LEFT JOIN WITH NULL-ELIMINATING WHERE

**What you see in the plan:**
LEFT JOIN preserving NULL rows from right table, but a WHERE clause
immediately filters on a right-table column, eliminating all NULL rows.
The LEFT JOIN is semantically an INNER JOIN.

**Engine gap — why the optimizer can't fix this:**
DuckDB cannot infer that a WHERE clause on a right-table column after
a LEFT JOIN means the LEFT can be converted to INNER. LEFT JOIN also
constrains join reordering — the optimizer cannot move selective filters
before a LEFT JOIN.

**How to diagnose:**
```sql
-- This pattern:
FROM store_sales LEFT JOIN store_returns ON (sr_item_sk = ss_item_sk)
WHERE sr_reason_sk = r_reason_sk  -- eliminates all NULL rows from LEFT

-- Is semantically:
FROM store_sales INNER JOIN store_returns ON (sr_item_sk = ss_item_sk)
WHERE sr_reason_sk = r_reason_sk
```

**Restructuring principle:**
Convert LEFT JOIN to INNER JOIN. Optionally pre-filter the right table
into a CTE to further reduce join input.

**Gate:**
- Check: does a CASE WHEN reference IS NULL on the right table column?
  If yes → NULL branch is semantically meaningful → DO NOT convert
- Evidence: Q93 (3.44× win when WHERE eliminates NULLs)

**Gold examples:**
- **Wins:** Q93 (3.44×)
- **Regressions:** None known

---

#### P6: CROSS-COLUMN OR FORCING FULL SCAN

**What you see in the plan:**
Sequential scan with complex OR predicate spanning different columns.
The optimizer cannot use targeted access for each branch independently.

**Engine gap — why the optimizer can't fix this:**
DuckDB can handle OR on the SAME column efficiently (single scan range),
but OR across DIFFERENT columns (e.g., `zip = X OR state = Y OR price > Z`)
forces a full scan evaluating all conditions.

**How to diagnose:**
```sql
-- Different columns in OR → optimizer can't target each branch
WHERE ca_zip IN ('10603', ...) 
   OR ca_state IN ('IL', 'TN', ...) 
   OR cs_sales_price > 500
```

**Restructuring principle:**
Split into UNION ALL branches, each with a single targeted predicate.
Each branch scans only the rows matching its specific condition.

**DANGER: This is the most volatile transform. Best win 6.28×, worst regression 0.23×.**

**Gates — apply ALL before proceeding:**

| Gate | Check | If triggered | Evidence |
|---|---|---|---|
| Same-column OR | Are all OR branches on the same column? | **NEVER split** — engine handles natively | 0.59× Q90, 0.23× Q13 |
| Branch count | How many UNION ALL branches needed? | **Max 3.** 6+ is lethal (9 branches = 0.23×) | 0.23× Q13 |
| Self-join present | Is there a self-join in the query? | **NEVER split** — each branch re-does the self-join | 0.51× Q23 |
| Nested ORs | OR of (A AND B) OR (C AND D) = multiplicative branches? | Count total branches after expansion. Max 3. | 0.23× Q13 |

**Gold examples:**
- **Wins:** Q15 (3.17× — 3 branches on different columns)
- **Regressions:** Q13 (0.23× — 9 branches), Q90 (0.59× — same-column), Q23 (0.51× — self-join)

---

#### P7: SET OPERATIONS AS FULL MATERIALIZATION

**What you see in the plan:**
INTERSECT or EXCEPT operations that materialize complete result sets from
both sides, sort them, then compare. The result is used as a filter
(membership check), not as a data source.

**Engine gap — why the optimizer can't fix this:**
INTERSECT must compute complete result sets before intersecting.
When the INTERSECT result is used only for membership checking,
EXISTS with correlated predicates short-circuits at the first match.

**Restructuring principle:**
Replace INTERSECT with correlated EXISTS subqueries.
Each EXISTS stops scanning as soon as one match is found per row.

```sql
-- BEFORE: INTERSECT materializes full sets
SELECT brand_id, class_id, category_id
FROM store_sales JOIN item ... INTERSECT 
SELECT brand_id, class_id, category_id
FROM catalog_sales JOIN item ... INTERSECT ...

-- AFTER: EXISTS short-circuits per row
WHERE EXISTS (SELECT 1 FROM store_sales JOIN item ... 
              WHERE iss.i_brand_id = i.i_brand_id AND ...)
  AND EXISTS (SELECT 1 FROM catalog_sales JOIN item ... 
              WHERE ics.i_brand_id = i.i_brand_id AND ...)
```

**Gate:**
- If INTERSECT operates on < 1000 rows, materialization cost is negligible — skip
- If correlation columns are not indexed, correlated probe may be slower

**Gold examples:**
- **Wins:** Q14 (2.39×), Q38 (1.83×)
- **Regressions:** None known

---

#### P8: WINDOW FUNCTIONS IN CTEs BEFORE JOIN

**What you see in the plan:**
Window functions (SUM OVER, RANK OVER) computed inside CTEs, then the
CTE results are joined. The window functions process more rows than
necessary because the join will filter the result.

**Engine gap — why the optimizer can't fix this:**
DuckDB executes window functions inside CTEs before the CTE output
is consumed. It cannot defer the window computation to after a
downstream join reduces the dataset.

**Restructuring principle:**
Remove window functions from CTEs. Keep only GROUP BY for daily/unit
aggregates. Join the reduced results, then compute window functions
once on the joined output.

**Gate:**
- Only applies when window is monotonically accumulating (SUM, COUNT)
- Does NOT apply to AVG, non-monotonic windows, or when CTE window result
  is consumed by multiple downstream references
- SUM() OVER() naturally skips NULLs, which handles FULL OUTER JOIN gaps

**Gold examples:**
- **Wins:** Q51 (1.36× — 3 window passes reduced to 1)
- **Regressions:** None known

---

#### P9: ROLLUP BLOCKING OPTIMIZATION

**What you see in the plan:**
GROUP BY ROLLUP computing all hierarchy levels in a single pass over
a large dataset. Each level could be computed more efficiently from
pre-aggregated results.

**Restructuring principle:**
Replace ROLLUP with explicit UNION ALL at each hierarchy level.
Pre-aggregate base data once, then compute each level from the
pre-aggregated CTE (not from raw data).

**Gate:**
- Only beneficial when ROLLUP operates on large fact table joins
- Small dimensions, few groups → ROLLUP is already efficient
- Must ensure UNION ALL branches cover ALL levels ROLLUP would produce

**Gold examples:**
- **Wins:** Q36 (2.47×)
- **Regressions:** None known, but CTE-based rewrites near ROLLUP can backfire (Q67 0.85×)

---

### ═══════════════════════════════════════════════════
### PHASE 4: VERIFY THE REWRITE
### ═══════════════════════════════════════════════════

Before finalizing, run these checks:

#### 4A: Row count trace
Walk through your rewritten query stage by stage.
Row counts should decrease monotonically.
Any flat or increasing stage = missed pushback opportunity.

#### 4B: Structural red line check (re-check Section 0C)
- [ ] No orphaned CTEs (every CTE referenced downstream)
- [ ] No unfiltered CTEs (every CTE has WHERE)
- [ ] No cross-joined dimension CTEs (each dim joins to fact)
- [ ] EXISTS still uses EXISTS (not materialized to CTE)
- [ ] Same-column ORs still intact (not split to UNION)
- [ ] All original WHERE filters preserved in CTEs
- [ ] Max 2 cascading fact-table CTE chains
- [ ] All comma joins converted to explicit JOIN...ON
- [ ] Original UNION eliminated if UNION_CTE_SPLIT applied

#### 4C: Regression pattern check
Does your rewrite match any known regression pattern?

| Pattern | Risk | Evidence |
|---|---|---|
| Pre-materialized fact in 3+ fact join | 0.50× | Q25 — locked join order |
| Dimension CTE with no selective filter | 0.85× | Q67 — pure overhead |
| Decorrelated EXISTS | 0.14× | Q16 — destroyed semi-join |
| 9+ UNION ALL branches | 0.23× | Q13 — multiplicative scans |
| CTE cross-joining dimensions | 0.0076× | Q80 — Cartesian product |
| Decorrelated LEFT JOIN that was semi-join | 0.34× | Q93 — redundant scans |
| Over-decomposed existing CTE | 0.71× | Q1 — added overhead |
| Correlated EXISTS pairs broken apart | 0.54× | Q95 — cardinality estimate severed |

#### 4D: Composition sanity
If you applied transforms from multiple phases:
- Phase 1 (pushback) should be reflected FIRST in the CTE chain
- Phase 2 (consolidation/pushdown) operates on the Phase 1-reduced result
- Phase 3 (decorrelation, conversion) operates on the Phase 1+2-reduced result
- Verify: does each later transform still make sense given what earlier phases did?

---

## SECTION 3: DECISION SUMMARY FORMAT

After analysis, output your plan in this format for workers:

```
QUERY ANALYSIS:
  Baseline: [time]ms
  Plan signature: [key explain plan observations]

PHASE 1 — Scan reduction:
  Pathology: [P0 description]
  Transform: [name] ([intensity])
  Rationale: [why this transform, why this intensity]
  Row count impact: [before] → [after]
  Gold examples: [win IDs] | Regression warnings: [regression IDs]
  Confidence: [high/medium/low] — [why]

PHASE 2 — Redundancy elimination (if applicable after Phase 1):
  [same format, or "SKIP — Phase 1 reduced scans below threshold"]

PHASE 3 — Structural fixes (if applicable after Phase 1+2):
  [same format, or "SKIP — [reason]"]

RED LINE CHECK: [pass/fail with details]
REGRESSION PATTERN CHECK: [pass/fail with details]
```

---

## SECTION 4: NOVEL PATTERNS (no pathology matched)

If no pathology in Phase 1-3 matches the explain plan, record:

```
NO MATCH REPORT:
  Pathologies checked: [list each, why it didn't match]
  Nearest miss: [closest pathology + which gate blocked it]
  
  Explain plan features present:
    - [list structural features: join types, scan sizes, 
       aggregation patterns, window usage, set operations]
  
  Row count profile:
    Stage 1: [N] rows → Stage 2: [N] rows → ... → Output: [N] rows
    Bottleneck stage: [which stage has most cost]
  
  Manual reasoning:
    What's expensive: [describe the cost center]
    Why the optimizer can't fix it: [hypothesize the engine limitation]  
    Proposed restructuring: [describe approach without named transform]
    Risk assessment: [what could go wrong]
```

This report feeds back into the gold example pipeline when the rewrite
is validated. If successful, it becomes a new gold example attached
to either an existing pathology or a new one.

---

## SECTION 5: PREDICATE PUSHBACK DECISION FRAMEWORK

This is the structured decision process for the single most valuable pathology
(P0 — 35% of all wins). Use these gates in order to decide WHETHER to push,
HOW HARD to push, and WHICH transform to select.

### 5A: Structural gate (required)

Both conditions must be true to proceed:
- **2+ stage CTE chain** (or subquery nesting)
- **At least one predicate** that applies late but references columns available
  (or joinable) earlier

If either is missing → SKIP. This pathology does not apply.

### 5B: Explain gate (row count shape)

Walk the chain stage by stage. Classify the shape:

```
Unhealthy (pushback opportunity):
  Stage 1: 7M rows → Stage 2: 7M rows → Stage 3: 6.8M rows → Main: 50K rows
  That 7M→7M→6.8M→50K shape = predicate at the end should be at the beginning.

Healthy (predicates already well-placed):
  Stage 1: 50K rows → Stage 2: 48K → Stage 3: 45K → Main: 44K
  Monotonically decreasing = no pushback needed.
```

If row counts are already monotonically decreasing → SKIP.

### 5C: Cardinality gate (is the predicate selective enough?)

Compute: rows before filter / rows after filter.

| Ratio    | Signal   | Action |
|----------|----------|--------|
| > 5:1    | Strong   | Proceed — high confidence of win |
| 2:1–5:1  | Moderate | Proceed if baseline > 200ms |
| < 2:1    | Weak     | Likely not worth CTE overhead — SKIP |

### 5D: Multi-fact gate (regression risk)

Count fact tables in the chain:

| Fact tables | Action |
|-------------|--------|
| 1           | Safe to push back aggressively |
| 2           | Push back but don't pre-materialize the join |
| 3+          | **STOP** — pre-materialization locks join order (Q25: 0.50×) |

### 5E: Ordering decision

If multiple predicates can be pushed back:
1. Push the most selective predicate first
2. Check if pushing the first makes the second redundant (dimension filters
   sometimes overlap)
3. If pushing back requires adding a dimension join to an early CTE, verify
   the dimension table is small (< 100K rows)

### 5F: Transform selection (lightest sufficient intensity)

| Situation | Transform | Intensity | Avg speedup |
|-----------|-----------|-----------|-------------|
| Single dim filter, selectivity > 5:1, chain ≤ 2 stages | date_cte_isolate | Light | 1.34× |
| Single dim filter, selectivity > 10:1, chain ≥ 3 stages | prefetch_fact_join | Medium | 1.89× |
| 2+ dimension filters | multi_dimension_prefetch | Medium | 1.55× |
| date_dim joined 3+ times (d1, d2, d3) | multi_date_range_cte | Medium | 1.42× |

Start with lightest. Escalate only if lighter version doesn't achieve target
row reduction at the bottleneck stage.

### 5G: Sequencing and composition

Predicate pushback is **always Phase 1** (first). Every other optimization
benefits from smaller input.

After applying pushback, re-evaluate:
- **P1 (repeated scans)**: may no longer be worth consolidating if each scan is now small
- **P3 (decorrelation)**: may no longer be needed if correlated loop is now over small set
- **P4 (aggregate pushdown)**: may become MORE valuable because pre-aggregation on smaller set is cheaper

Common compositions:
- **pushback + aggregate_pushdown**: push predicate back, THEN push aggregation below join
- **pushback + decorrelation**: push predicates back to shrink base, THEN decorrelate

### 5H: Confidence output

| Level  | When |
|--------|------|
| High   | Row count flat through 2+ stages then drops 10:1+, single fact table, clear dimension predicate |
| Medium | Row count drops 3:1–10:1, or predicate involves a subquery result not a simple literal |
| Low    | Row count drops < 3:1, or 3+ fact tables, or existing CTE structure already has filters |

---

## SECTION 6: PRUNING GUIDE (for context efficiency)

When the explain plan clearly rules out a pathology, skip that section entirely.

| If the plan shows | Skip | Reason |
|---|---|---|
| No nested loop joins | P3 (decorrelation) | Optimizer already using hash joins |
| Each table appears once | P1 (repeated scans) | No redundant scans to consolidate |
| No CTE materialization nodes | P0 partial (self-join decomp) | No CTE to decompose |
| No LEFT JOIN | P5 (INNER conversion) | Not applicable |
| No OR predicates in filters | P6 (OR decomposition) | Not applicable |
| No GROUP BY | P4 (aggregate pushdown) | Not applicable |
| No WINDOW/OVER | P8 (deferred window) | Not applicable |
| No INTERSECT/EXCEPT | P7 (set operation rewrite) | Not applicable |
| No ROLLUP/CUBE | P9 (ROLLUP decomposition) | Not applicable |
| Baseline < 50ms | ALL CTE-based transforms | Overhead exceeds any possible savings |
| Row counts already monotonically decreasing | P0 (predicate pushback) | Predicates already well-placed |

---

## APPENDIX: COMPLETE REGRESSION REGISTRY

Every known regression with root cause, sorted by severity.
Check your rewrite against this list before finalizing.

| Severity | Query | Transform | Speedup | Root cause |
|---|---|---|---|---|
| CATASTROPHIC | Q80 | dimension_cte_isolate | 0.0076× | Cross-joined 3 dim CTEs: 30×200×20 = 120K Cartesian |
| CATASTROPHIC | Q16 | materialize_cte | 0.14× | Materialized EXISTS → destroyed semi-join short-circuit |
| SEVERE | Q13 | or_to_union | 0.23× | Nested ORs expanded to 9 UNION ALL branches = 9 fact scans |
| SEVERE | Q93 | decorrelate | 0.34× | Correlated LEFT JOIN was already semi-join; CTE forced redundant scans |
| MAJOR | Q31 | union_cte_split | 0.49× | Original UNION kept alongside split CTEs = double materialization |
| MAJOR | Q25 | date_cte_isolate | 0.50× | Pre-materialized fact in 3-way fact join, locked optimizer join order |
| MAJOR | Q23 | or_to_union | 0.51× | Self-join present; each UNION branch re-executed the self-join |
| MAJOR | Q95 | semantic_rewrite | 0.54× | Correlated EXISTS pairs broken into independent CTEs, lost cardinality |
| MODERATE | Q90 | or_to_union | 0.59× | Split same-column OR that engine handles natively |
| MODERATE | Q74 | union_cte_split | 0.68× | Original UNION kept alongside split CTEs |
| MODERATE | Q1 | decorrelate | 0.71× | Pre-aggregated ALL stores when only SD stores needed |
| MODERATE | Q4 | prefetch_fact_join | 0.78× | 3rd cascading fact-table CTE chain |
| MINOR | Q67 | date_cte_isolate | 0.85× | CTE blocked ROLLUP/window pushdown optimization |
| MINOR | Q72 | multi_dimension_prefetch | 0.77× | Forced suboptimal join order on complex multi-table query |

---

*Version 1.0 — Distilled from 100 TPC-DS queries across SF1-SF10.*
*Pipeline: Trial JSON → Gold Examples → Pathology Cards → Decision Tree → This document.*
*Update protocol: New gold example → update relevant pathology → regenerate this tree.*
