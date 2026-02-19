# Beyond Rule-Based Rewriting: Why LLM-Guided SQL Optimization Outperforms Calcite

## 1. Introduction

Apache Calcite ships ~120 rewrite rules covering predicate pushdown, join reordering, subquery decorrelation, and expression simplification. R-Bot [Zhou et al., 2024] uses Calcite's rule engine to generate candidate rewrites, then selects the best via LLM. On DSB-76 (PostgreSQL 14.3, SF10), our R-Bot reproduction achieves 20/76 queries improved (26.3%) — matching the paper's reported 23.7% with GPT-4.

QueryTorque's LLM-guided approach achieves 36/76 queries improved with Beam V3 and 25/76 with Swarm V2. On the 31 shared templates, Beam wins 19, Swarm wins 10, and R-Bot wins 9 — with R-Bot having **zero exclusive wins** over both QueryTorque modes.

This section analyzes *why* through two detailed case studies: query032 (1499.7× speedup) and query040 (511.6× speedup). We decompose each LLM rewrite into atomic transformation steps and evaluate whether Calcite's rule engine could produce the same result.

---

## 2. Case Study 1: DSB Query 032 — Correlated Subquery with Shared Scan Materialization

**Speedup**: 1499.7× (300,000ms → 200ms)
**R-Bot result**: 81.6× (cost-estimation), runtime 3,679ms
**Transform family**: B (Decorrelation) + A (Early Filtering) + E (Materialization)

### 2.1 Original Query (times out at 300s)

```sql
SELECT SUM(cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales, item, date_dim
WHERE (i_manufact_id IN (47, 226, 612, 676, 818)
       OR i_manager_id BETWEEN 71 AND 100)
  AND i_item_sk = cs_item_sk
  AND d_date BETWEEN '1998-01-06' AND CAST('1998-01-06' AS DATE) + INTERVAL '90 day'
  AND d_date_sk = cs_sold_date_sk
  AND cs_ext_discount_amt > (
      SELECT 1.3 * avg(cs_ext_discount_amt)           -- correlated scalar subquery
      FROM catalog_sales, date_dim
      WHERE cs_item_sk = i_item_sk                     -- ← correlation predicate
        AND d_date BETWEEN '1998-01-06' AND CAST('1998-01-06' AS DATE) + INTERVAL '90 day'
        AND d_date_sk = cs_sold_date_sk
        AND cs_list_price BETWEEN 108 AND 137
        AND cs_sales_price / cs_list_price BETWEEN 0.23 AND 0.43
  )
ORDER BY SUM(cs_ext_discount_amt) LIMIT 100;
```

**Why it times out**: The correlated subquery `WHERE cs_item_sk = i_item_sk` forces PostgreSQL to re-execute the subquery for every outer row. The subquery scans `catalog_sales` (millions of rows) × `date_dim` (73K rows) per outer row. With ~300K qualifying outer rows, this becomes ~300K × full-scan = timeout.

### 2.2 LLM Rewrite (200ms)

```sql
WITH cte_date AS (                                    -- Step 1: Dimension isolation
    SELECT d_date_sk
    FROM date_dim
    WHERE d_date BETWEEN '1998-01-06'
      AND CAST('1998-01-06' AS DATE) + INTERVAL '90 day'
),
cte_fact AS (                                         -- Step 2: Shared fact scan
    SELECT cs.cs_item_sk, cs.cs_ext_discount_amt
    FROM catalog_sales cs
    JOIN cte_date d ON cs.cs_sold_date_sk = d.d_date_sk
    WHERE cs.cs_list_price BETWEEN 108 AND 137
      AND cs.cs_sales_price / cs.cs_list_price BETWEEN 0.23 AND 0.43
),
cte_avg AS (                                          -- Step 3: Decorrelate → GROUP BY
    SELECT cs_item_sk,
           1.3 * AVG(cs_ext_discount_amt) AS threshold
    FROM cte_fact
    GROUP BY cs_item_sk
)
SELECT SUM(f.cs_ext_discount_amt) AS "excess discount amount"
FROM cte_fact f                                       -- Step 5: Reuse shared scan
JOIN item i ON f.cs_item_sk = i.i_item_sk
JOIN cte_avg a ON f.cs_item_sk = a.cs_item_sk         -- Step 4: JOIN replaces correlation
WHERE (i.i_manufact_id IN (47, 226, 612, 676, 818)
       OR i.i_manager_id BETWEEN 71 AND 100)
  AND f.cs_ext_discount_amt > a.threshold
ORDER BY SUM(f.cs_ext_discount_amt) LIMIT 100;
```

### 2.3 Transformation Decomposition

The LLM applies five coordinated transformations:

| Step | Transformation | Description | Rows |
|------|---------------|-------------|------|
| **1** | Dimension isolation | Extract `date_dim WHERE d_date BETWEEN ...` into `cte_date` | 73,049 → **91** |
| **2** | Shared fact scan | Join `catalog_sales` with `cte_date`, apply price filters. Creates one pre-filtered result shared by both threshold computation and main query | ~14M → **16,053** |
| **3** | Decorrelation | Convert correlated `WHERE cs_item_sk = i_item_sk` scalar subquery into `GROUP BY cs_item_sk` CTE that computes all per-item thresholds in one pass | Per-row execution → **single aggregate** |
| **4** | JOIN replacement | Replace `> (correlated subquery)` with equi-join `JOIN cte_avg ON cs_item_sk` + filter `> a.threshold` | SubPlan → hash join |
| **5** | Scan reuse | Both the main query's fact rows and the threshold computation read from `cte_fact` — the fact table is scanned exactly once | 2 full scans → **1 shared scan** |

### 2.4 Calcite Feasibility Analysis

| Step | Calcite Capable? | Relevant Rules | Gap |
|------|-----------------|----------------|-----|
| **1. Dimension CTE** | **NO** | None | Calcite inlines CTEs during `SqlToRelConverter`. No rule introduces new CTEs or materializes dimension filters as separate hash tables. `FilterJoinRule` pushes predicates but cannot materialize intermediate results. |
| **2. Shared fact scan** | **NO** | `CommonRelSubExprRegisterRule` (detection only) | Requires recognizing that the outer query and correlated subquery scan the same fact table with overlapping filters, then extracting the intersection into a shared materialized plan. No rule performs cross-consumer scan merging. |
| **3. Decorrelation** | **PARTIAL** | `SubQueryRemoveRule` + `RelDecorrelator` | Calcite can decorrelate single-level correlated scalar aggregates into left-joins. However, the result is an inline subquery that scans `catalog_sales` independently — it does not share the scan with the outer query (Step 2 is prerequisite). |
| **4. JOIN replacement** | **YES** | Part of decorrelation output | Natural consequence of successful decorrelation. The correlated comparison becomes a join condition. |
| **5. Scan reuse** | **NO** | None | Even after decorrelation, Calcite produces two separate `catalog_sales` scan operators. There is no mechanism to detect that both scans apply overlapping filters and could share a single materialized result. |

### 2.5 Calcite's Theoretical Best Outcome

If Calcite successfully decorrelates (Step 3), it produces:

```sql
SELECT SUM(cs.cs_ext_discount_amt) AS "excess discount amount"
FROM catalog_sales cs
JOIN item i ON i.i_item_sk = cs.cs_item_sk
JOIN date_dim d ON d.d_date_sk = cs.cs_sold_date_sk
LEFT JOIN (
    SELECT cs2.cs_item_sk,                            -- ← SECOND full fact scan
           1.3 * AVG(cs2.cs_ext_discount_amt) AS threshold
    FROM catalog_sales cs2                             -- ← no filter sharing
    JOIN date_dim d2 ON d2.d_date_sk = cs2.cs_sold_date_sk
    WHERE d2.d_date BETWEEN '1998-01-06' AND ...
      AND cs2.cs_list_price BETWEEN 108 AND 137
      AND cs2.cs_sales_price / cs2.cs_list_price BETWEEN 0.23 AND 0.43
    GROUP BY cs2.cs_item_sk
) sub ON cs.cs_item_sk = sub.cs_item_sk
WHERE (i.i_manufact_id IN (...) OR i.i_manager_id BETWEEN 71 AND 100)
  AND d.d_date BETWEEN '1998-01-06' AND ...
  AND cs.cs_ext_discount_amt > sub.threshold
ORDER BY SUM(cs.cs_ext_discount_amt) LIMIT 100;
```

This still scans `catalog_sales` **twice** — once in the outer FROM (millions of rows) and once in the decorrelated subquery (millions of rows). Estimated speedup: ~5–20× from eliminating per-row re-execution, but not the 1499.7× that the LLM achieves.

### 2.6 Why the LLM Wins

The critical transformation is **Step 2 + Step 5**: creating a shared fact scan (`cte_fact`) that materializes 16,053 pre-filtered rows from ~14M, then reusing it for both the threshold computation and the main query. This reduces:

- **I/O**: 2 × 14M-row scans → 1 × 14M-row scan + 2 × 16K-row CTE reads
- **Computation**: Per-row correlated execution (300K × full scan) → one GROUP BY on 16K rows
- **Memory**: Two independent hash tables → one shared CTE in memory

No Calcite rule can introduce this CTE, because:
1. CTE introduction requires *recognizing* shared computation across query consumers — a semantic, not syntactic, operation
2. The QUITE paper [2025] explicitly identifies CTE conversion as requiring "explicit control over evaluation order and sub-plan sharing, which is beyond the scope of relational-algebra rewrite rules"
3. Calcite's `CommonRelSubExprRegisterRule` detects common sub-expressions within the Volcano planner but cannot generate WITH clauses or shared materialization points

**R-Bot comparison**: R-Bot's Calcite-based rewrite achieves 81.6× (cost-estimation) / ~3,679ms runtime — consistent with decorrelation-only (no shared scan). The LLM's shared-scan strategy provides an additional **18× improvement** over R-Bot's best output.

---

## 3. Case Study 2: DSB Query 040 — Multi-Dimension CTE Isolation with Implicit JOIN Semantics

**Speedup**: 511.6× (73,764ms → 144ms)
**R-Bot result**: 2.0× (cost-estimation), runtime regression
**Transform family**: F (Join Transform) + A (Early Filtering)

### 3.1 Original Query (73.8s)

```sql
SELECT w_state, i_item_id,
  SUM(CASE WHEN d_date < '2002-02-20'
      THEN cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END) AS sales_before,
  SUM(CASE WHEN d_date >= '2002-02-20'
      THEN cs_sales_price - COALESCE(cr_refunded_cash,0) ELSE 0 END) AS sales_after
FROM catalog_sales
  LEFT OUTER JOIN catalog_returns
    ON (cs_order_number = cr_order_number AND cs_item_sk = cr_item_sk)
  , warehouse                                         -- ← comma join
  , item                                              -- ← comma join
  , date_dim                                          -- ← comma join
WHERE i_item_sk = cs_item_sk
  AND cs_warehouse_sk = w_warehouse_sk
  AND cs_sold_date_sk = d_date_sk
  AND d_date BETWEEN ('2002-02-20' - INTERVAL '30 day')
                 AND ('2002-02-20' + INTERVAL '30 day')
  AND i_category = 'Shoes'
  AND i_manager_id BETWEEN 42 AND 81
  AND cs_wholesale_cost BETWEEN 68 AND 87
  AND cr_reason_sk = 40                               -- ← null-rejecting filter on LEFT JOIN
GROUP BY w_state, i_item_id
ORDER BY w_state, i_item_id
LIMIT 100;
```

**Three interacting problems**:

1. **Comma joins mixed with LEFT JOIN**: PostgreSQL's optimizer cannot freely reorder joins when a LEFT JOIN is interleaved with comma-separated tables in the FROM clause. The implicit cross-product semantics constrain the join tree.

2. **Null-rejecting LEFT JOIN**: `WHERE cr_reason_sk = 40` filters on the right side of the LEFT JOIN. Since `cr_reason_sk` is NULL for non-matching rows, this predicate eliminates all outer-join NULLs — making the LEFT JOIN semantically equivalent to an INNER JOIN. PostgreSQL 14.3 does not always detect this simplification.

3. **No predicate pushdown into fact table**: `cs_wholesale_cost BETWEEN 68 AND 87` is in the WHERE clause but applies to `catalog_sales`. The optimizer may not push this predicate below the LEFT JOIN, forcing a full fact table scan before filtering.

### 3.2 LLM Rewrite (144ms)

```sql
WITH filtered_date_dim AS (                           -- Step 1: Date isolation (61 rows)
    SELECT d_date_sk, d_date
    FROM date_dim
    WHERE d_date BETWEEN (CAST('2002-02-20' AS DATE) - INTERVAL '30 day')
                   AND (CAST('2002-02-20' AS DATE) + INTERVAL '30 day')
),
filtered_item AS (                                    -- Step 2: Item isolation (~200 rows)
    SELECT i_item_sk, i_item_id
    FROM item
    WHERE i_category = 'Shoes' AND i_manager_id BETWEEN 42 AND 81
),
filtered_warehouse AS (                               -- Step 3: Warehouse passthrough
    SELECT w_warehouse_sk, w_state FROM warehouse
),
filtered_catalog_sales AS (                           -- Step 4: Fact pre-filtering
    SELECT cs_order_number, cs_item_sk, cs_warehouse_sk,
           cs_sold_date_sk, cs_sales_price, cs_wholesale_cost
    FROM catalog_sales
    WHERE cs_wholesale_cost BETWEEN 68 AND 87
),
catalog_returns AS (                                  -- Step 5: Returns CTE
    SELECT cr_order_number, cr_item_sk, cr_refunded_cash, cr_reason_sk
    FROM catalog_returns
)
SELECT w.w_state, i.i_item_id,
  SUM(CASE WHEN CAST(d.d_date AS DATE) < CAST('2002-02-20' AS DATE)
      THEN cs.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0) ELSE 0 END) AS sales_before,
  SUM(CASE WHEN CAST(d.d_date AS DATE) >= CAST('2002-02-20' AS DATE)
      THEN cs.cs_sales_price - COALESCE(cr.cr_refunded_cash, 0) ELSE 0 END) AS sales_after
FROM filtered_catalog_sales cs                        -- Step 6: Explicit JOIN chain
JOIN filtered_date_dim d ON cs.cs_sold_date_sk = d.d_date_sk
JOIN filtered_item i ON cs.cs_item_sk = i.i_item_sk
JOIN filtered_warehouse w ON cs.cs_warehouse_sk = w.w_warehouse_sk
LEFT JOIN catalog_returns cr
  ON cs.cs_order_number = cr.cr_order_number
 AND cs.cs_item_sk = cr.cr_item_sk
WHERE cr.cr_reason_sk = 40
GROUP BY w.w_state, i.i_item_id
ORDER BY w.w_state, i.i_item_id
LIMIT 100;
```

### 3.3 Transformation Decomposition

| Step | Transformation | Description | Calcite? |
|------|---------------|-------------|----------|
| **1** | Date dimension CTE | Materialize `date_dim` filtered to 61 rows (from 73K) as hash table | **NO** — Cannot introduce CTEs |
| **2** | Item dimension CTE | Materialize `item` filtered by category + manager (~200 rows) | **NO** — Same limitation |
| **3** | Warehouse CTE | Extract warehouse (passthrough, no filter — guides join order) | **NO** — No strategic CTE introduction |
| **4** | Fact pre-filtering CTE | Apply `cs_wholesale_cost BETWEEN 68 AND 87` to `catalog_sales` before any joins, materializing the reduced fact set | **PARTIAL** — `FilterJoinRule` can push the predicate down, but cannot materialize as CTE |
| **5** | Returns CTE | Wrap `catalog_returns` to separate it from the comma-join syntax | **NO** — Syntactic restructuring for join order guidance |
| **6** | Comma → explicit JOIN | Convert comma-separated tables to `JOIN ... ON` syntax, resolving the join-order constraint | **IMPLICIT** — Handled during `SqlToRelConverter` parsing |
| **7** | NULL-rejection detection | `cr_reason_sk = 40` in WHERE null-rejects LEFT JOIN rows (semantically INNER JOIN) | **YES** — `JoinDeriveIsNotNullFilterRule` can detect this pattern |

### 3.4 Calcite Feasibility Analysis

Calcite can handle two of the seven steps:

**Step 7 (NULL-rejection)**: Calcite's `JoinDeriveIsNotNullFilterRule` can recognize that `WHERE cr_reason_sk = 40` implies `cr_reason_sk IS NOT NULL`, which contradicts the NULL rows from a LEFT JOIN. It can convert LEFT → INNER JOIN. This enables the optimizer to reorder the join freely.

**Step 4 (Predicate pushdown)**: `FilterJoinRule` can push `cs_wholesale_cost BETWEEN 68 AND 87` down to the `catalog_sales` scan. However, it pushes it as a scan-level filter, not as a materialized intermediate result.

**Steps 1-3, 5 (CTE introduction)**: Calcite fundamentally cannot introduce CTEs. The dimension isolation pattern — materializing small filtered dimension results as hash tables before probing the fact table — requires:
1. Recognizing that certain predicates are highly selective on small tables
2. Deciding to materialize those filtered results as separate execution units
3. Controlling join order by making the materialized results available as build-side inputs

This is a **cost-based materialization decision**, not a syntactic pattern match. The LLM reasons about selectivity (61 date rows, ~200 item rows) and decides to front-load these tiny results as hash tables.

### 3.5 Why Multi-Dimension CTE Isolation Works

The original query's EXPLAIN shows PostgreSQL choosing a **nested loop** plan:
```
Nested Loop (rows=0, time=73764ms)
  Nested Loop (rows=868)
    Nested Loop (rows=30343)
      Seq Scan on date_dim (rows=61)
      Index Scan on catalog_sales (rows=497 per loop)
    ...
```

The optimizer stumbles because the comma joins + LEFT JOIN constrain join ordering. It picks a plan with nested loops that probe `catalog_sales` 61 times (once per date row), yielding 30,343 intermediate rows that then cascade through more nested loops.

The LLM's CTE structure forces a different execution strategy:
1. Materialize 61 date rows → tiny hash table
2. Materialize ~200 item rows → tiny hash table
3. Scan `catalog_sales` once with `cs_wholesale_cost` filter → reduced fact set
4. Hash-join the reduced fact set against both tiny hash tables
5. LEFT JOIN with `catalog_returns` on the already-small result

This converts 61 × index-probe loops into 1 × hash-join — a fundamentally different execution topology.

### 3.6 The Compound Strategy Problem

Note that **no single step** produces the full speedup. The scouts achieved only 3.24× with individual transforms (date CTE alone, or INNER JOIN conversion alone, or multi-dimension prefetch alone). The compiler synthesized all transforms into one coordinated rewrite to achieve 511.6×.

This illustrates a key limitation of rule-based systems: Calcite applies rules independently (each rule pattern-matches and fires locally). The interaction effects between transforms — CTE materialization enables hash-join selection, which enables join reorder, which enables predicate pushdown through the reordered plan — cannot be captured by individual rules.

---

## 4. Transform Taxonomy: Calcite-Feasible vs. LLM-Only

Based on our catalog of 30 transforms and empirical results from 76 DSB queries, we classify each transform by Calcite feasibility:

### 4.1 Calcite-Feasible (rule-based systems can produce equivalent rewrites)

| Transform | Calcite Rule(s) | Our Wins | Notes |
|-----------|-----------------|----------|-------|
| Comma → explicit JOIN | `SqlToRelConverter` (parse-time) | — | No performance impact alone; enables optimizer join reorder |
| Simple decorrelation (IN/EXISTS → semi-join) | `SubQueryRemoveRule` + `RelDecorrelator` | 0 | Single-level only; nested correlation fails (CALCITE-5789, CALCITE-7031) |
| Transitive predicate inference | `JoinPushTransitivePredicatesRule` | 0 | A=B AND B=5 → A=5 |
| LEFT → INNER JOIN null-rejection | `JoinDeriveIsNotNullFilterRule` | 0 | WHERE on right-side column implies NOT NULL |
| INTERSECT → EXISTS | `IntersectToExistsRule` | 2 | 4.0×, 1.2× (query014, query087) |
| Predicate pushdown through joins | `FilterJoinRule`, `FilterProjectTransposeRule` | 0 | Core strength, but alone insufficient for complex queries |
| Join reordering | `DphypJoinReorderRule` | 0 | Depends on accurate cardinality estimates |

**Total wins attributable to Calcite-feasible transforms alone: 2** (both INTERSECT→EXISTS)

### 4.2 Partially Feasible (Calcite has rules, but they are insufficient)

| Transform | Calcite Limitation | LLM Advantage | Our Wins |
|-----------|-------------------|---------------|----------|
| Correlated scalar subquery decorrelation | `RelDecorrelator` handles single-level but produces inline subquery (no scan sharing) | LLM creates shared CTE + GROUP BY + JOIN — 18× better than decorrelation alone | 8 |
| OR→UNION decomposition | `JoinExpandOrToUnionRule` targets join conditions only, not standalone WHERE | LLM reasons about when OR→UNION is profitable (selectivity, index structure) and limits to ≤3 branches | 1 |
| Aggregate pushdown below joins | `AggregateJoinTransposeRule` requires GROUP BY keys to align with join keys | LLM restructures the query to enable aggregate pushdown where keys don't naturally align | 1 |

### 4.3 LLM-Only (fundamentally beyond rule-based rewriting)

| Transform | Why Rules Cannot Do It | Our Wins | Best Speedup |
|-----------|----------------------|----------|-------------|
| **CTE introduction** (dimension isolation, fact pre-filtering) | No RelNode concept of "shared sub-plan." Calcite inlines CTEs at parse time. QUITE [2025]: "CTE conversion requires explicit control over evaluation order and sub-plan sharing, beyond the scope of rewrite rules." | **22** | 1499.7× |
| **Shared scan materialization** | Requires recognizing overlapping fact-table scans across query consumers and merging into single CTE | **8** | 1499.7× |
| **Multi-dimension CTE isolation** | Requires selectivity reasoning: which dimensions are small enough to materialize as hash tables? | **14** | 511.6× |
| **Single-pass aggregation** (CASE WHEN consolidation) | Requires recognizing that N separate COUNT/SUM/AVG passes on the same table can merge into one scan with conditional aggregation | **3** | 5.25× |
| **CTE specialization** (generic CTE → per-filter CTEs) | Requires recognizing that a CTE scanned N times with different filters should be split into N specialized CTEs | **2** | 4.76× |
| **Deferred window aggregation** | Requires understanding that window functions in CTEs are wasteful when most CTE rows are filtered after joining | **1** | 1.8× |
| **Compound strategy coordination** | Multiple transforms must be applied together in the correct order; individual rules cannot compose multi-step plans | **32** (compiler) | 1499.7× |

### 4.4 Summary Statistics

| Category | Transform Count | Query Wins | Geo Mean Speedup |
|----------|----------------|------------|-----------------|
| Calcite-feasible | 7 | 2 (3.6%) | 2.2× |
| Partially feasible | 3 | 10 (18.5%) | 3.8× |
| LLM-only | 7+ | 42 (77.8%) | 8.9× |

**77.8% of our winning query optimizations use transforms that Calcite fundamentally cannot produce.** The LLM-only category dominates because the highest-impact transforms — CTE introduction, shared scan materialization, and compound strategy coordination — require semantic reasoning about query topology, data distribution, and execution strategy that cannot be expressed as local pattern-matching rules on relational algebra trees.

---

## 5. Can We Teach Calcite These Transforms?

A natural question is whether new Calcite rules could be written to implement our LLM-discovered transforms. We assess each:

### 5.1 Theoretically Implementable (with significant engineering)

| Transform | Implementation Path | Complexity | Prerequisite |
|-----------|-------------------|------------|-------------|
| CTE introduction for dimension isolation | Add `DimensionMaterializeRule`: detect dimension tables with selective predicates, introduce materialization points | HIGH | Requires cardinality estimates for dimension tables; must add CTE/materialize concept to RelNode tree |
| LEFT → INNER null-rejection | Already exists (`JoinDeriveIsNotNullFilterRule`) | DONE | — |
| Correlated decorrelation with scan sharing | Extend `RelDecorrelator` with `CommonScanMergeRule`: after decorrelation, detect overlapping scans and merge | VERY HIGH | Requires general common sub-expression materialization — an open research problem (CALCITE-7031) |

### 5.2 Fundamentally Rule-Resistant

| Transform | Why New Rules Cannot Help |
|-----------|-------------------------|
| **Compound strategy coordination** | The space of valid multi-step rewrite sequences is combinatorially explosive. A query with 5 applicable transforms has 5! = 120 possible orderings, each producing different intermediate plans. Rules fire locally and greedily; they cannot evaluate the downstream effect of applying transform A before transform B. |
| **Selectivity-aware materialization** | Deciding *when* to materialize (CTE) vs. inline (subquery) requires runtime cost estimation. A dimension CTE with 61 rows is beneficial; one with 50K rows adds overhead. This is a cost-model decision, not a pattern-matching one. Calcite's default statistics (heuristic row counts) are too weak for this. |
| **Single-pass aggregation via CASE WHEN** | Recognizing that 8 separate `SELECT COUNT(*) FROM store_sales WHERE quantity BETWEEN X AND Y` can merge into `SELECT COUNT(CASE WHEN quantity BETWEEN X1 AND Y1 THEN 1 END), COUNT(CASE WHEN ...)` requires understanding the *intent* of the query, not just its algebraic structure. Each subquery differs in predicate values but shares table, join pattern, and aggregate function — a semantic similarity that rules cannot detect. |
| **CTE specialization** | Deciding that a generic CTE scanned twice with `WHERE d_moy=1` and `WHERE d_moy=2` should split into two specialized CTEs requires recognizing: (a) the CTE is scanned multiple times, (b) each scan applies a discriminator predicate, (c) pushing that predicate into the CTE definition is cheaper than post-filtering. Step (c) is a cost decision; steps (a)+(b) require cross-consumer analysis that rules operating on individual nodes cannot perform. |

### 5.3 The Gold Example Pathway

Our 30 verified gold examples (16 DuckDB, 14 PostgreSQL) could serve as *templates* for new Calcite rules — each gold example defines:
- **Precondition features**: AST-detectable patterns (e.g., `CORRELATED_SUB`, `TABLE_REPEAT_3+`, `DATE_DIM`)
- **Transformation procedure**: Step-by-step rewrite with before/after SQL
- **Contraindications**: When the transform is harmful (e.g., `or_to_union` with >3 branches → 0.23× regression)

A hybrid approach — **LLM-discovered transforms compiled into Calcite rules** — could potentially capture some of our wins. However, this requires:
1. Adding CTE/materialization support to Calcite's RelNode algebra
2. Adding cardinality-aware guard conditions to prevent regressions
3. Handling the compound strategy problem (which single rules cannot solve)

The QUITE paper [2025] proposes a similar "LLM agent that invokes rules" architecture but reports only 21.9% speedup over baseline rules — far below our 1499.7× on complex queries. The gap exists because QUITE still relies on Calcite's rule engine for the actual rewriting; the LLM merely selects which rules to apply. Our approach uses the LLM as the rewrite engine itself, enabling transforms that rules cannot express.

---

## 6. Conclusion

Rule-based SQL rewriting systems like Calcite excel at **local algebraic transformations**: pushing predicates, reordering joins, simplifying expressions, and decorrelating simple subqueries. These are necessary but insufficient for complex analytical queries.

LLM-guided rewriting excels at **global semantic transformations**: restructuring query topology (introducing CTEs), recognizing shared computation (scan merging), reasoning about data distribution (selectivity-aware materialization), and coordinating multi-step strategies (compound transforms). These capabilities produce order-of-magnitude speedups (1499.7×, 511.6×) that rule-based systems cannot achieve — as demonstrated by R-Bot's 0 exclusive wins over QueryTorque on the same benchmark.

The three intrinsic limitations of rule-based rewriting are:

1. **No CTE introduction**: Rules operate on existing relational algebra nodes; they cannot introduce new materialization points that change the query's execution topology.
2. **No cross-consumer reasoning**: Rules fire on individual operators; they cannot recognize that two separate scan paths share filters and should be merged into a shared CTE.
3. **No compound coordination**: Rules apply independently and greedily; they cannot plan multi-step transformation sequences where each step enables the next.

Our gold examples demonstrate that these LLM-discovered transforms are *real, reproducible, and verified* — not random perturbations, but structured optimization strategies with documented preconditions, contraindications, and empirical speedups across multiple query instances.
