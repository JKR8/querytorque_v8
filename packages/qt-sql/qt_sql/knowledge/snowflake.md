# Snowflake Rewrite Playbook
# TPC-DS SF10TCL empirical evidence | X-Small warehouse

## ENGINE STRENGTHS — do NOT rewrite these patterns

1. **Micro-partition pruning**: Filters on clustered columns skip micro-partitions. DO NOT wrap filter columns in functions (kills pruning).
2. **Column pruning through CTEs**: Reads only columns referenced by final query. Automatic.
3. **Predicate pushdown**: Filters pushed to storage layer, including through single-ref CTEs. Also does predicate MIRRORING across join sides. DO NOT manually duplicate filters.
4. **Correlated subquery decorrelation**: Transforms correlated subqueries into hash joins. DO NOT decorrelate unless EXPLAIN shows a nested loop.
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

**P1: Comma Join Prevents Date-Based Partition Pruning** (SMALLEST SET FIRST) — 63% success

| Aspect | Detail |
|---|---|
| Detect | Fact table: all partitions assigned in EXPLAIN, date filter exists on date_dim, comma join between fact and date_dim. |
| Gates | REQUIRED: date range/year/month filter exists on date_dim. REQUIRED: fact table has >1000 partitions. Narrower date windows prune more: 30d→1s, 1yr→55-250s. Multiple date_dim aliases (d1, d2, d3): create separate CTEs for each. |
| Treatments | Date CTE isolation + explicit JOINs. Works for single-date, multi-date, EXISTS subqueries, UNION branches. 22 wins (all TIMEOUT→completes). |
| Failures | No date filter (Q9/Q88/Q93 → N/A). CTE self-join bottleneck (Q47/Q57). FULL OUTER JOIN (Q51/Q97). 3-way fact join + wide date (Q17/Q29). Structure too heavy for X-Small (Q35/Q67/Q72/Q85/Q89). EXCEPT branches (Q87). |

Evidence table — wins (all were TIMEOUT >300s):

| Query | After | Date window | Detail |
|-------|-------|-------------|--------|
| Q21 | 0.7s | 60 days | Single fact, tight window |
| Q12 | 1.1s | 30 days | Single fact + item filter |
| Q55 | 3.5s | 1 month | Simple 3-table join |
| Q32 | 4.2s | 90 days | + correlated subquery reuses CTE |
| Q77 | 10.1s | 30 days | 6 channel CTEs, 1 shared date CTE |
| Q61 | 11.9s | 1 month | 3-channel promotions |
| Q16 | 17.3s | 60 days | EXISTS/NOT EXISTS preserved |
| Q71 | 20.3s | 1 month | 3 channel UNION, 1 date CTE |
| Q53 | 31.4s | 12 months | 4-table, wider window |
| Q10 | 51.0s | 1 year EXISTS | 4 EXISTS subqueries, shared date CTE |
| Q79 | 55.0s | 1 year | Customer+ticket grouping |
| Q5 | 56.1s | 14 days | 3-channel sales+returns UNION |
| Q80 | 57.2s | 30 days | LEFT JOIN returns, item+promo |
| Q36 | 69.7s | Full year | + store filter, ROLLUP |
| Q49 | 71.4s | 1 month | 3 UNION + returns + RANK |
| Q25 | 96.1s | 1mo/7mo/7mo | 3 fact tables, 3 date CTEs |
| Q48 | 139.6s | Full year | Multi-OR demographics |
| Q70 | 143.2s | 1 year | ROLLUP + ranking, borderline |
| Q69 | 182.7s | 1 year EXISTS | 4 EXISTS with NOT EXISTS |
| Q66 | 200.9s | 12 months | 3-channel, wider window |
| Q65 | 231.0s | 12 months | Borderline (run 2 timed out) |
| Q50 | 250.6s | 12 months | Borderline, inventory join |

**P2: Repeated Identical Subqueries** (DON'T REPEAT WORK) — needs more evidence

| Aspect | Detail |
|---|---|
| Detect | Same fact table scanned N times with similar filters. |
| Gates | Without date pruning: insufficient on huge tables (Q9 failed — 73K partitions, no date filter). Must preserve exact aggregation semantics. |
| Treatments | Consolidate into single scan with conditional aggregation (CASE WHEN). |
| Failures | Q9 TIMEOUT — 73K parts unavoidable without date filter. |

---

## PRUNING GUIDE

| Plan shows | Skip |
|---|---|
| No date_dim comma join | P1 (date CTE pruning) |
| Date filter already in fact scan | P1 (already pruning) |
| Each table appears once | P2 (scan consolidation) |
| Baseline < 100ms | ALL structural rewrites |

## REGRESSION REGISTRY

No regressions observed. All 22 wins produced strict improvements (TIMEOUT → completes).

Failure summary by category:
- No date filter (P1 N/A): Q9, Q88, Q93
- CTE self-join bottleneck: Q47, Q57
- FULL OUTER JOIN: Q51, Q97
- 3-way fact join + wide date: Q17, Q29
- Structure too heavy for X-Small: Q35, Q67, Q72, Q85, Q89
- EXCEPT branches: Q87
