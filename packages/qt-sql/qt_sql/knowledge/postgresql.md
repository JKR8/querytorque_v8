# PostgreSQL Rewrite Playbook
# DSB SF10 field intelligence

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **BITMAP_OR_SCAN**: Multi-branch ORs on indexed columns handled via bitmap combination in one scan. Splitting ORs to UNION ALL is lethal (0.21x observed).
2. **EXISTS semi-join**: Uses early termination. Converting a single EXISTS to a materializing CTE caused 0.50x, 0.75x — semi-join destroyed. **Exception**: When 3+ correlated NOT EXISTS channels scan different fact tables (e.g., Q069 17.48x), pre-materializing each channel into DISTINCT CTEs with LEFT JOIN IS NULL anti-pattern eliminates repeated Materialize node re-scans.
3. **INNER JOIN reordering**: Freely reorders INNER JOINs by selectivity estimates. Do NOT manually restructure INNER JOIN order.
4. **Index-only scan**: Reads only index when covering all requested columns. Small dimension lookups may not need CTEs.
5. **Parallel query execution**: Large scans and aggregations parallelized across workers. CTEs block parallelism (materialization is single-threaded).
6. **JIT compilation**: JIT-compiles complex expressions for long-running queries (>100ms).

## GLOBAL GUARDS

1. OR conditions on indexed columns → never split to UNION ALL (0.21x observed)
2. Single EXISTS → never materialize into CTE (0.50x, 0.75x — semi-join destroyed). Exception: 3+ NOT EXISTS channels → pre-materialize each into DISTINCT CTE + LEFT JOIN IS NULL (17.48x observed)
3. INNER JOIN order → never restructure (optimizer handles reordering)
4. Small dimensions (< 10K rows) → index-only scan may be faster than CTE
5. Baseline < 100ms → skip CTE-based rewrites (overhead exceeds savings)
6. CTEs block parallel execution — only use when benefit outweighs parallelism loss
7. Use AS MATERIALIZED when CTE must not be inlined (decorrelation, shared scans)
8. Preserve efficient existing CTEs — don't decompose working patterns
9. Verify NULL semantics in NOT IN conversions
10. ROLLUP/window in same query → CTE may prevent pushdown optimizations
11. Never inline a large UNION CTE — re-execution multiplied per reference (0.16x — 6 fact scans re-executed)
12. Max 2 cascading fact-table CTE chains — deeper chains block parallelism
13. EXPLAIN cost gaps ≠ runtime gains for config tuning — 6 false positives caught (up to 84% EXPLAIN gap → 0% runtime). Always 3-race validate config changes.

---

## DOCUMENTED CASES

Cases ordered by safety (zero-regression cases first, then by decreasing risk).

**P6: Multiple Date_dim Aliases with Overlapping Filters** (SMALLEST SET FIRST) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | 2+ date_dim aliases in FROM with similar year/month_seq/moy predicates. |
| Gates | 2+ date_dim instances with overlapping date predicates. Selectivity < 1% of date_dim (always true for year+month). Combine with explicit JOIN conversion when comma joins present. |
| Treatments | date_consolidation (1 win, 3.10x), date_cte_isolate (3 wins + 7 improved). Apply first — date CTE is smallest, most reliable transform. |
| Failures | None observed. |

**P3: Same Fact+Dimension Scan Repeated Across Subquery Boundaries** (DON'T REPEAT WORK) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | Identical scan subtrees appearing 2+ times in EXPLAIN with similar costs. Same fact table joined to same dimensions in multiple subqueries, or self-join with different GROUP BY granularity. |
| Gates | 2+ subqueries scanning same fact table with identical filters. COUNT/SUM/AVG/MIN/MAX only (not STDDEV/PERCENTILE). Self-join → consolidate into single CTE. 3 channel scans → single_pass_aggregation. |
| Treatments | single_pass_aggregation (1 win, 1.98x), self_join_pivot (1 win, 1.79x) |
| Failures | None observed. |

**P4: Non-Equi Join Without Prefiltering** (MINIMIZE ROWS TOUCHED) — ZERO REGRESSIONS

| Aspect | Detail |
|---|---|
| Detect | Expensive non-equi join (BETWEEN, <, >) in EXPLAIN with large inputs. Neither side filtered. |
| Gates | Non-equi join predicate exists. Both join inputs > 10K rows. At least one side has selective dimension filter available. |
| Treatments | pg_materialized_dimension_fact_prefilter (1 win, 12.07x). Apply after P1 and P6. |
| Failures | None observed. |

**P1: Comma Join Confusing Cardinality Estimation** (ARM THE OPTIMIZER)

| Aspect | Detail |
|---|---|
| Detect | FROM t1, t2, t3 WHERE t1.key = t2.key (comma joins, no explicit JOIN). Hash/nested-loop join with poor row estimates in EXPLAIN. |
| Gates | Multiple tables in comma-separated FROM with equi-join predicates. Dimension filters available. 1-2 fact tables only (3+ → join order lock). Max 3-4 dimension CTEs. Stop if all JOINs already explicit → skip to P6/P7. |
| Treatments | pg_date_cte_explicit_join (4 wins, 2.1x avg), pg_dimension_prefetch_star (3 wins, 2.8x avg), explicit_join_materialized (2 wins, 5.9x avg) |
| Failures | 0.88x (explicit join overhead on simple query) |

**P5: Set Operation Materializing Full Result Sets** (SETS OVER LOOPS)

| Aspect | Detail |
|---|---|
| Detect | INTERSECT/EXCEPT between large result sets. Correlated EXISTS on 3+ channels (store, web, catalog). |
| Gates | INTERSECT with 10K+ rows → convert to EXISTS. Correlated NOT EXISTS on 3+ channels → materialize channel sets. Simple EXISTS (single channel) → KEEP EXISTS. NOT EXISTS already hash anti-join in EXPLAIN → STOP. |
| Treatments | intersect_to_exists (1 win, 1.78x), set_operation_materialization (1 win, 17.48x) |
| Failures | 0.75x (over-materialized date CTE in EXISTS path) |

**P2: Correlated Subquery Executing Per Outer Row** (SETS OVER LOOPS) — HIGHEST IMPACT

| Aspect | Detail |
|---|---|
| Detect | Nested loop in EXPLAIN, inner side re-executes aggregate per outer row. SQL: WHERE col > (SELECT AGG(...) FROM ... WHERE outer.key = inner.key). If EXPLAIN shows hash join on correlation key → already decorrelated → STOP. |
| Gates | Correlated scalar subquery with aggregate (AVG, SUM, COUNT). NOT EXISTS: NEVER decorrelate (destroys semi-join, 0.50x). Inner = outer table → extract common scan to shared CTE. ALWAYS use AS MATERIALIZED. 1-2 fact tables safe, 3+ → STOP. |
| Treatments | inline_decorrelate_materialized (3 wins, avg 500x), decorrelate (8 wins, avg 3.2x), shared_scan + decorrelate (2 wins, avg 7000x) |
| Failures | 0.51x (multi-fact join lock), 0.75x (EXISTS materialized) |

**P7: Multi-Dimension Prefetch for Star-Schema Aggregation** (SMALLEST SET FIRST) — CAUTION

| Aspect | Detail |
|---|---|
| Detect | Large fact table scan followed by late dimension filter in EXPLAIN. Star schema with 3+ dimension filters in WHERE. |
| Gates | 3+ selective dimension filters, each < 10% of dimension table. Single fact table, NOT self-join or multi-fact. Stop if self-join → use P3 (0.25x). Stop if multi-fact → join order lock (0.51x). |
| Treatments | pg_dimension_prefetch_star (2 wins, 2.5x avg), multi_dimension_prefetch (1 win, 2.50x) |
| Failures | 0.25x (self-join), 0.51x (multi-fact) |

---

## CONFIG TUNING PATTERNS

Config tuning is ADDITIVE to SQL rewrite — not a substitute. Apply after SQL rewrite.
Evidence: 52 queries benchmarked, 25 config wins, 3-race validated (PG 14.3, SF10).
CRITICAL: EXPLAIN ANALYZE cost gaps do NOT predict runtime gains. 6 false positives
caught where EXPLAIN showed 38-84% improvement but runtime showed 0% or regression.
Always 3-race validate config changes.

**C1: Merge Join Forcing Suboptimal Plan** — HIGHEST IMPACT hint

| Aspect | Detail |
|---|---|
| Detect | EXPLAIN shows Merge Join with Sort node below it on large unsorted inputs (both > 10K rows). |
| Config | `/*+ Set(enable_mergejoin off) */` |
| Evidence | 6 wins (+8.6%–+82.5%, avg +50.6%). |
| Risk | LOW when Sort+MJ visible. Do NOT disable on pre-sorted data. |

**C2: Cost Model Undervaluing Index Scans on SSD** — HIGHEST RECOVERY

| Aspect | Detail |
|---|---|
| Detect | Seq Scan on fact tables despite btree indexes on join/filter columns. |
| Config | `SET LOCAL random_page_cost = '1.1'; SET LOCAL effective_cache_size = '48GB'` |
| Evidence | 6 wins (+46.0%–+89.0%, avg +71.1%). Rescued 3 rewrite regressions. Nonlinear interaction — neither alone sufficient. |
| Risk | LOW on SSD. Zero regressions observed. |

**C3: Parallelism Underutilized on Large Scans** — MOST VERSATILE

| Aspect | Detail |
|---|---|
| Detect | Large Seq Scan (>100K rows) without Gather/Parallel node, query > 500ms. |
| Config | `SET LOCAL max_parallel_workers_per_gather = '4'; SET LOCAL parallel_setup_cost = '100'; SET LOCAL parallel_tuple_cost = '0.001'` |
| Evidence | 5 standalone wins (+6.2%–+28.2%, avg +14.3%). Also in 10+ combo wins. |
| Risk | MEDIUM. 7.34x REGRESSION on 244ms query. NEVER on queries < 500ms. par4-alone -15.3% — must include work_mem. |

**C4: Hash/Sort Spilling to Disk** — TARGETED

| Aspect | Detail |
|---|---|
| Detect | Hash Batches > 1 or Sort Space Type = 'Disk' in EXPLAIN ANALYZE. |
| Config | work_mem sized by op count: ≤2 ops → 512MB, 3-5 → 256MB, 6+ → 128MB |
| Evidence | 4 wins (+11.4%–+41.5%, avg +21.7%). Often needs par4. |
| Risk | LOW. work_mem is per-operation — count sort+hash ops before sizing. |

**C5: Nested Loop on Large Join Inputs** — HIGH IMPACT hint

| Aspect | Detail |
|---|---|
| Detect | Nested Loop in EXPLAIN with >10K rows on both sides, equi-join condition exists. |
| Config | `/*+ Set(enable_nestloop off) */` |
| Evidence | 3 wins (+42.5%–+81.3%, avg +60.4%). |
| Risk | HIGH. -1454% regression observed. NEVER on correlated subqueries (use P2 instead). |

**C6: Sort Overhead on Pre-Ordered Data** — RARE

| Aspect | Detail |
|---|---|
| Detect | Sort node on index-ordered data or where hash aggregation is viable. |
| Config | `SET LOCAL enable_sort = 'off'` |
| Evidence | 2 wins (+4.7%–+68.2%, avg +36.5%). High variance (3.2-7.7%). |
| Risk | MEDIUM. Forces hash-based execution. Validate carefully. |

---

## PRUNING GUIDE

| Plan shows | Skip |
|---|---|
| No comma joins (all explicit JOINs) | P1 (comma join fix) |
| No nested loops on large tables | P2 (decorrelation) |
| Each table appears once | P3 (repeated scans) |
| No non-equi joins (BETWEEN, <, >) | P4 (non-equi prefilter) |
| No INTERSECT/EXCEPT and no correlated multi-channel EXISTS | P5 (set operation) |
| Single date_dim reference | P6 (date consolidation) |
| No GROUP BY or only 1 dimension filter | P7 (multi-dim prefetch) |
| Baseline < 100ms | ALL CTE-based transforms |
| Bitmap OR scan present | OR→UNION rewrites |
| Parallel workers active + query fast | CTE-heavy transforms |

## REGRESSION REGISTRY

| Severity | Transform | Result | Root cause |
|----------|-----------|--------|------------|
| CATASTROPHIC | cte_inlining | 0.16x | Inlined large UNION CTE → 6 fact scans re-executed 2x each |
| SEVERE | multi_dim_prefetch | 0.15x | CTEs blocked date-predicate pushdown on 90-day interval join |
| SEVERE | dimension_prefetch | 0.25x | Applied star-schema pattern to 6-way self-join → parallelism destroyed |
| MAJOR | cte_materialization | 0.30x | Multi-scan CTE overhead similar to above cte_inlining pattern |
| MAJOR | early_fact_filtering | 0.51x | Disabled nestloop too aggressively + DISTINCT forced hash spill |
| MAJOR | date_cte_prefetch | 0.75x | Over-materialized date CTE in EXISTS path → destroyed semi-join |
| MODERATE | explicit_join | 0.88x | Explicit join conversion overhead exceeded benefit on simple query |
| CATASTROPHIC | forced_parallelism (C3) | 7.34x regr | Worker startup + coordination overhead on 244ms query. NEVER force par on < 500ms |
| CATASTROPHIC | enable_nestloop_off (C5) | -1454% | NL was correct plan. Disabling forced catastrophic merge/hash on unsuitable query |
| MAJOR | geqo_off | -254% | Exhaustive planner found "better" cost plan on 19 joins but cardinality errors made it catastrophic |
| MAJOR | par4_without_wm | -15.3% | Parallelism without sufficient work_mem causes hash spill under parallel execution |
