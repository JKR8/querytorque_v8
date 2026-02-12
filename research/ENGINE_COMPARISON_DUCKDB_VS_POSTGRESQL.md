# DuckDB vs PostgreSQL: Optimizer Gap Analysis

## 1. Benchmark Overview

| Metric | DuckDB (TPC-DS) | PostgreSQL (DSB) |
|--------|-----------------|------------------|
| Queries tested | 88 (SF1-SF10) | 76 (SF10) |
| Total wins | 34 | 31 WIN + 21 IMPROVED |
| Success rate | ~67% | 68.4% |
| Pathologies found | 10 (P0-P9) | 7 (P1-P7) |
| Regressions catalogued | 14 | 7 |
| Biggest win | Q22 **42.90x** | Q92 **8044x** |
| Zero-regression pathologies | 5 of 10 | 4 of 7 |

The headline numbers look similar in success rate, but the *character* of the wins is completely different.

---

## 2. Shared Optimizer Gaps (Both Engines Fail)

### Correlated Subquery Paralysis
Both engines cannot decorrelate complex correlated aggregate subqueries. The fix is identical: extract to GROUP BY CTE + JOIN.

**But the impact is wildly different:**
- **DuckDB**: Low priority. 3 wins, avg 2.45x. DuckDB's in-process columnar execution means even a nested loop on millions of rows completes in seconds.
- **PostgreSQL**: Highest impact. 11 wins including **8044x, 1465x, 439x**. PG's process-per-connection model + disk I/O makes correlated nested loops catastrophically slow. Queries literally timeout.

**Takeaway**: Same bug, 1000x difference in severity. PG's row-oriented disk-based execution amplifies the nested-loop penalty exponentially.

### Cross-CTE Predicate Blindness
Both engines plan CTEs independently — predicates can't propagate backward.

**But the response strategy differs:**
- **DuckDB**: The #1 productive gap (~35% of wins). CTEs are cheap (inlined when single-ref, columnar materialization). Push predicates into CTEs freely.
- **PostgreSQL**: Double-edged sword. CTE materialization is a **hard optimization fence** (single-threaded, blocks parallelism). Must combine with explicit JOIN conversion to be effective. CTE alone often regresses.

### Repeated Scans / Common Subexpression Elimination
Neither engine detects N scans of the same table across subquery boundaries.

**But the fix pattern varies:**
- **DuckDB**: FILTER clause is native syntax — consolidate into single scan with `COUNT(*) FILTER (WHERE ...)`. Clean, zero-overhead. 8 wins, avg 1.88x, **zero regressions**.
- **PostgreSQL**: Same consolidation works but PG lacks native FILTER syntax (uses CASE WHEN). Self-join decomposition and pivot patterns are the PG-specific variants. 3 wins, avg 2.53x, zero regressions.

---

## 3. Engine-Specific Gaps (One Has It, The Other Doesn't)

### DuckDB-Only Gaps

| Gap | Why PG doesn't have it |
|-----|----------------------|
| **P4: Cross-column OR** (or_to_union) | PG has **BitmapOr** — handles multi-branch ORs natively via bitmap index combination. Splitting to UNION on PG is lethal (0.21x). |
| **P5: LEFT JOIN + NULL-eliminating WHERE** | PG doesn't have this gap to the same degree — its cost-based optimizer handles LEFT→INNER inference better in practice. |
| **P8: Window functions in CTEs before join** | Not observed as a significant gap on PG in our DSB benchmark. |
| **P9: Shared subexpression (CSE)** | PG's CTE materialization actually helps here — it IS the CSE mechanism. The problem is unique to DuckDB where CTE inlining re-executes. |

### PostgreSQL-Only Gaps

| Gap | Why DuckDB doesn't have it |
|-----|--------------------------|
| **P1: Comma join weakness** | DuckDB handles comma joins fine — columnar execution makes join order less critical. PG's row-oriented cost model is significantly weaker on implicit joins. |
| **P4: Non-equi join input blindness** | DuckDB's columnar execution is less sensitive to non-equi join input sizes. PG's nested-loop fallback on BETWEEN/range is devastating. |
| **P6: Date dim redundancy** | DuckDB inlines single-ref date CTEs (zero cost). PG materializes them (fence + overhead). But pre-filtering date_dim on PG is the most reliable transform (92% success). |
| **P7: Multi-dim prefetch** | DuckDB's parallel columnar scans make dimension prefetching less impactful. PG's single-threaded CTE materialization makes this a careful tradeoff. |
| **CTE Materialization Fence** | DuckDB doesn't have this gap at all — CTEs are inlined or cheaply materialized in-memory. PG's CTE fence blocks parallelism, predicate pushdown, and semi-join optimization. |

---

## 4. Architectural Root Causes

| Property | DuckDB | PostgreSQL |
|----------|--------|------------|
| Storage | Columnar, in-memory | Row-oriented, disk-backed |
| CTE behavior | Inlined (single-ref), cheap materialize | Hard materialization fence |
| Parallelism | Thread-based, always on | Process-based, CTE blocks it |
| Index support | None (scans only) | B-tree, bitmap, hash |
| OR handling | Full scan evaluates all | BitmapOr combines indexes |
| Join fallback | Hash join (fast on cols) | Nested loop (slow on rows) |
| Execution model | Vectorized, morsel-driven | Volcano (tuple-at-a-time + JIT) |

These architectural differences explain why:
1. **CTE strategies are cheap on DuckDB, expensive on PG** — DuckDB materializes in columnar memory chunks; PG writes to temp tuplestore, blocks parallelism.
2. **Correlated subqueries are annoying on DuckDB but catastrophic on PG** — vectorized execution vs tuple-at-a-time nested loop.
3. **OR decomposition works on DuckDB but is lethal on PG** — DuckDB has no indexes (must scan), PG has BitmapOr.

---

## 5. Safety Profiles

### Safest transforms (zero regressions on both):
- **Repeated scan consolidation** (single_pass_aggregation) — universally safe
- **INTERSECT → EXISTS** — safe on both engines

### Safest per-engine:
- **DuckDB**: P1 (repeated scan), P3 (agg pushdown), P5 (LEFT→INNER), P6 (INTERSECT), P8 (deferred window) — all zero regressions
- **PostgreSQL**: P6 (date CTE, 92% success), P3 (repeated scans), P4 (non-equi prefilter), P1 (comma join) — all zero regressions

### Most dangerous:
- **DuckDB**: P4 or_to_union (0.23x worst) and P9 materialize_cte on EXISTS (0.14x worst)
- **PostgreSQL**: P7 multi-dim prefetch on self-joins (0.25x worst) and CTE inlining of UNION bodies (0.16x worst)

---

## 6. Recommended Approach by Engine

### DuckDB Strategy: "Push Predicates, Consolidate Scans"
1. **Always first**: Count table scans — if same table appears N times, consolidate (P1). Zero risk.
2. **Then**: Push selective predicates into CTEs (P0). Check all gates (max 2 fact chains, no dim cross-joins, every CTE filtered).
3. **Then**: Check for agg-below-join (P3) and LEFT→INNER (P5). Both zero risk.
4. **Avoid unless certain**: or_to_union (P4) — only cross-column, max 3 branches. Never touch same-column OR.
5. **Never**: Materialize EXISTS. Ever.

### PostgreSQL Strategy: "Fix the SQL, Then Decorrelate"
1. **Always first**: Convert comma joins to explicit JOIN + date CTE (P1+P6). This combo is the #1 reliable move on PG (92% success).
2. **Then**: Decorrelate correlated subqueries (P2). This is where the monster wins live (8044x). Always use AS MATERIALIZED.
3. **Then**: Consolidate repeated scans (P3) and prefilter non-equi join inputs (P4). Both zero risk.
4. **Carefully**: Set operations (P5) — INTERSECT→EXISTS is safe, but materializing EXISTS is dangerous.
5. **Avoid unless star-schema only**: Multi-dim prefetch (P7) — self-join and multi-fact patterns regress hard.
6. **Layer on**: SET LOCAL tuning (work_mem for sort spills, JIT off for medium queries). Additive 1.3x average.

### Cross-Engine Principle
The universal algorithm is the same: **reduce input rows as early as possible, eliminate redundant work, then fix structural issues**. But the *implementation* of each step is engine-specific:

- "Reduce input rows" on DuckDB = push predicates into CTEs (cheap). On PG = explicit JOIN conversion + date CTE (must avoid fence).
- "Eliminate redundant work" on DuckDB = FILTER clause consolidation. On PG = MATERIALIZED CTE for shared scans.
- "Fix structural issues" on DuckDB = or_to_union, decorrelate. On PG = decorrelate (massive impact), comma→explicit JOIN.

---

## 7. Key Insight for the Paper

The same optimizer limitation (e.g., failure to decorrelate) manifests at radically different severity depending on the execution engine:

- DuckDB's columnar vectorized execution **absorbs** optimizer mistakes better — a bad plan on columnar data is still tolerable because scan overhead is low.
- PostgreSQL's row-oriented execution **amplifies** optimizer mistakes — a correlated nested loop becomes a timeout because every row requires a disk seek + tuple reconstruction.

This means **the value of SQL rewriting is inversely proportional to the engine's execution efficiency on unoptimized plans**. PG needs it more and rewards it more (8044x vs 42.9x best wins), but the rewrites must be more conservative because PG's CTE fence and parallelism constraints create more ways to regress.
