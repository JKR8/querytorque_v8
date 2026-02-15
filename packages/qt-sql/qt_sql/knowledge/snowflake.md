# Snowflake Rewrite Playbook
# TPC-DS SF10TCL empirical evidence | X-Small warehouse

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **Micro-partition pruning**: Filters on clustered columns skip micro-partitions. DO NOT wrap filter columns in functions (kills pruning).
2. **Column pruning through CTEs**: Reads only columns referenced by final query. Automatic.
3. **Predicate pushdown**: Filters pushed to storage layer, including through single-ref CTEs. Also does predicate MIRRORING across join sides. DO NOT manually duplicate filters already applied to the same table. NOTE: Does NOT push date_sk ranges through UNION ALL CTEs or across comma joins to fact tables — see P4.
4. **Correlated subquery decorrelation (simple)**: Transforms simple correlated subqueries into hash joins. DOES NOT handle correlated scalar subqueries with aggregation (see P3). Check EXPLAIN for nested loop before manual decorrelation of simple EXISTS/IN patterns.
5. **EXISTS/NOT EXISTS semi-join**: Early termination. SemiJoin node in plan. NEVER materialize EXISTS into CTEs.
6. **Join filtering (bloom filters)**: JoinFilter nodes push bloom filters from build side to probe-side TableScan. 77/99 TPC-DS queries show JoinFilter. DO NOT restructure joins that already have JoinFilter.
7. **Cost-based join ordering**: Usually correct. DO NOT force join order unless evidence of a flipped join.
8. **QUALIFY clause**: Native window-function filtering, more efficient than subquery.

## GLOBAL GUARDS

1. EXISTS/NOT EXISTS → never materialize into CTEs (kills SemiJoin early termination).
2. UNION ALL → limit to ≤3 branches (each = separate scan pipeline).
3. CTEs referenced once → inline. CTEs referenced 2+ times → keep.
4. Do NOT restructure joins that have JoinFilter.
5. Do NOT wrap filter columns in functions → prevents micro-partition pruning.
6. NOT IN → NOT EXISTS for NULL safety.
7. Baseline < 100ms → skip structural rewrites.

---

## DOCUMENTED CASES

**P3: Correlated Scalar Subquery with Aggregation** (DECORRELATE) — 100% success (2/2)

| Aspect | Detail |
|---|---|
| Detect | WHERE col > (SELECT agg(col) FROM fact WHERE key = outer.key). Correlated scalar subquery with AVG/SUM/COUNT that re-scans the same or different fact table per outer row. |
| Gates | REQUIRED: correlated scalar subquery with aggregate function. REQUIRED: inner query joins fact table. Works on any fact table (catalog_sales, web_sales, store_sales). |
| Treatments | Decompose into CTEs: (1) dimension filter, (2) date-filtered fact rows, (3) per-key aggregate threshold via GROUP BY. Final query JOINs threshold CTE. If inner and outer scan the SAME fact table with SAME filters, use shared-scan variant (single CTE for both). |
| Failures | None observed. |

Evidence table — wins (MEDIUM warehouse, 3x3 validation):

| Example | Orig_ms | Opt_ms | Speedup | Pattern |
|---------|---------|--------|---------|---------|
| inline_decorrelate | 69,415 | 2,996 | 23.17x | 3 CTEs: dim filter + date-filtered fact + per-key threshold |
| shared_scan_decorrelate | 8,025 | 1,026 | 7.82x | Shared-scan variant: common fact CTE reused for threshold + outer rows |

---

**P4: Predicate Transitivity Failure — SK Range Pushdown** (SK_PUSHDOWN) — 100% success (2/2)

| Aspect | Detail |
|---|---|
| Detect | Fact table(s) joined to date_dim via comma join. Date filter on date_dim columns (d_year, d_moy, d_quarter_name) but NO explicit sold_date_sk range on the fact table. EXPLAIN shows full or near-full partition scan on fact table(s). Especially impactful through UNION ALL CTEs. |
| Gates | REQUIRED: date filter exists on date_dim. REQUIRED: fact table joined via sold_date_sk = d_date_sk (comma or explicit). REQUIRED: fact table partition scan ratio > 50%. Does NOT help if query is compute-bound (e.g. ROLLUP). |
| Treatments | (1) Look up date_sk range: SELECT MIN(d_date_sk), MAX(d_date_sk) FROM date_dim WHERE <date_filter>. (2) Add explicit sold_date_sk BETWEEN <min> AND <max> on each fact table. (3) Convert comma joins to explicit JOINs. For UNION ALL CTEs: push the BETWEEN inside each branch. |
| Failures | Q17 NEUTRAL (0.97x) — Snowflake already optimizes after warmup when SK ranges are wide (274 values). Q67 TIMEOUT — ROLLUP over 8 columns is compute-bound, not I/O-bound. |

Evidence table — wins (X-Small warehouse, 5x trimmed mean):

| Example | Orig_ms | Opt_ms | Speedup | Pattern |
|---------|---------|--------|---------|---------|
| sk_pushdown_union_all (Q2) | 229,847 | 107,982 | 2.13x | BETWEEN pushed into UNION ALL branches (web_sales + catalog_sales) |
| sk_pushdown_3fact (Q56) | 10,234 | 8,730 | 1.17x | BETWEEN on 3 fact tables (store_sales + catalog_sales + web_sales) |

---

## PRUNING GUIDE

| Plan shows | Skip |
|---|---|
| No correlated scalar subquery with aggregate | P3 (decorrelation) |
| Simple EXISTS/IN correlation (no aggregate) | P3 (Snowflake handles these natively) |
| No date_dim join or no date filter | P4 (SK pushdown) |
| Fact table partition scan < 50% | P4 (already pruning well) |
| Query is compute-bound (ROLLUP, massive GROUP BY) | P4 (SK pushdown won't help) |
| Baseline < 100ms | ALL structural rewrites |

## REGRESSION REGISTRY

No regressions observed.

Neutrals (not regressions, but no win):
- Q17 P4 SK pushdown: 0.97x — wide date range (274 values), Snowflake handles after warmup
- Q67 P4 SK pushdown: both timeout — ROLLUP over 8 columns is compute-bound
