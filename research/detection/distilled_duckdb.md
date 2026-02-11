**ENGINE STRENGTHS**
1. **INTRA_SCAN_PREDICATE_PUSHDOWN**: Pushes WHERE filters directly into SEQ_SCAN. **Do NOT** create a CTE to push a filter already inside the scan node.
2. **SAME_COLUMN_OR**: OR on the same column uses a single scan. **Do NOT** split same-column ORs into UNION ALL — duplicates fact scans.
3. **HASH_JOIN_SELECTION**: Automatically selects hash joins. **Do NOT** restructure simple join orders — focus on input reduction.
4. **CTE_INLINING**: Single‑reference CTEs are inlined; multi‑reference may be materialized. **Do NOT** assume CTEs are optimization fences.
5. **COLUMNAR_PROJECTION**: Reads only referenced columns. **Do NOT** ignore column selection in CTEs — fewer columns = less memory.
6. **PARALLEL_AGGREGATION**: Parallel scans and efficient perfect‑hash GROUP BY. **Do NOT** restructure aggregations unless reducing input rows.
7. **EXISTS_SEMI_JOIN**: EXISTS uses semi‑join with early termination. **Do NOT** convert EXISTS to a materialized CTE — forces full scan.

**CORRECTNESS RULES**
- Row count must be identical.
- NULL handling semantics must be preserved.
- ORDER BY must be kept unless the query has no LIMIT/ORDER requirement.
- LIMIT must be preserved.

**OPTIMIZER GAPS**

## Gap: CORRELATED_SUBQUERY_PARALYSIS
Correlated subqueries re‑execute per outer row. Opportunity: decorrelate into CTEs that execute once.
**decorrelate** [W3] — HIGH reliability, 1 win, avg 2.92x  
Convert correlated subqueries to standalone CTEs with GROUP BY, then JOIN.
- ✓ Q1: 2.92x — pushed s_state='SD' filter early
- Guard: Preserve all WHERE filters. Check EXPLAIN — if hash join, optimizer already decorrelated.
**composite_decorrelate_union** [W3] — HIGH reliability, 1 win, avg 2.42x  
For multiple correlated EXISTS, extract shared dimensions once, decorrelate each into DISTINCT CTEs, then UNION.
- ✓ Q35: 2.42x — shared date filter CTE, each EXISTS became SELECT DISTINCT customer_sk
- Guard: (none)

## Gap: CROSS_COLUMN_OR_DECOMPOSITION
OR on different columns prevents optimal scan path. Opportunity: split into UNION ALL with focused predicates.
**or_to_union** [W2] — HIGH reliability, 1 win, avg 3.17x  
Split OR conditions on different columns into separate UNION ALL branches.
- ✓ Q15: 3.17x — three OR branches became UNION ALL
- ✗ Q90: 0.59x — split same‑column time range, doubled fact scans
- Guard: Max 3 UNION branches. Never split same‑column ORs. Avoid on self‑joins.

## Gap: CROSS_CTE_PREDICATE_BLINDNESS
Predicates aren’t pushed across CTE boundaries. Opportunity: pre‑filter dimensions into CTEs.
**date_cte_isolate** [W1] — HIGH reliability, 2 wins, avg 4.00x  
Extract date dimension lookups into CTE. Join instead of scalar subquery.
- ✓ Q6/Q11: 4.00x — date filter into CTE
- ✗ Q31: 0.49x — baseline <100ms, CTE overhead exceeded savings
- Guard: Skip if baseline <100ms. Don't decompose efficient existing CTEs.
**early_filter** [W1] — HIGH reliability, 2 wins, avg 4.00x  
Filter small dimension tables first, then join to fact tables.
- ✓ Q11/Q93: 4.00x — filtered reason table first
- Guard: (none)
**prefetch_fact_join** [W1] — HIGH reliability, 1 win, avg 3.77x  
Build CTE chain: filter dimension, pre‑join with fact, then join remaining dimensions.
- ✓ Q63: 3.77x — filtered date_dim first, pre‑joined with store_sales
- ✗ Q25: 0.50x — baseline <50ms, CTE overhead dominated
- Guard: Max 2 cascading fact‑table CTE chains.
**multi_dimension_prefetch** [W1] — HIGH reliability, 1 win, avg 2.71x  
Pre‑filter multiple dimension tables into CTEs before fact join.
- ✓ Q43: 2.71x — pre‑filtered date_dim and store
- ✗ Q67: 0.85x — unfiltered dimension CTEs added overhead
- Guard: Every CTE must have a WHERE clause.
**multi_date_range_cte** [W1] — HIGH reliability, 1 win, avg 2.35x  
When same dimension is joined multiple times, create separate filtered CTEs per alias.
- ✓ Q29: 2.35x — separate date CTEs for d1, d2, d3
- Guard: (none)
**pushdown** [W1] — HIGH reliability, 1 win, avg 2.11x  
Consolidate multiple subqueries scanning same table into a single CTE.
- ✓ Q9: 2.11x — 15+ scalar subqueries became one CTE
- Guard: (none)
**dimension_cte_isolate** [W1] — MEDIUM reliability, 1 win, avg 1.93x  
Pre‑filter dimension tables into CTEs returning only surrogate keys.
- ✓ Q26: 1.93x — isolated date, demographics, promotions
- ✗ Q26: 0.0076x — cross‑joined 3+ dimension CTEs, Cartesian explosion
- Guard: Never cross‑join 3+ dimension CTEs. Every CTE must have a WHERE clause.
**shared_dimension_multi_channel** [W1] — MEDIUM reliability, 1 win, avg 1.30x  
Extract shared dimension filters into common CTEs for multiple channels.
- ✓ Q80: 1.30x — shared date, item, promotion filters extracted once
- Guard: (none)

## Gap: REDUNDANT_SCAN_ELIMINATION
Repeated scans of same table waste I/O. Opportunity: consolidate into single scan.
**channel_bitmap_aggregation** [W2] — HIGH reliability, 1 win, avg 6.24x  
Consolidate repeated fact scans into one scan with CASE WHEN and conditional aggregation.
- ✓ Q88: 6.24x — 8 scans → 1 scan with CASE labels
- Guard: Not for >8 buckets or structurally different joins.
**single_pass_aggregation** [W2] — HIGH reliability, 1 win, avg 4.47x  
Consolidate scalar subqueries into one CTE with CASE inside aggregates.
- ✓ Q9: 4.47x — 15 subqueries → one CTE
- Guard: (none)

## Gap: UNION_CTE_SELF_JOIN_DECOMPOSITION
Generic CTEs scanned multiple times with different filters. Opportunity: split into specialized CTEs.
**rollup_to_union_windowing** [W4] — HIGH reliability, 1 win, avg 2.47x  
Replace GROUP BY ROLLUP with UNION ALL of pre‑aggregated CTEs per level.
- ✓ Q36: 2.47x — explicit UNION ALL allowed per‑level optimization
- Guard: Not when ROLLUP is efficient (small dimensions, few groups).
**union_cte_split** [W4] — MEDIUM reliability, 1 win, avg 1.36x  
Split generic CTE scanned multiple times into specialized CTEs that embed the filter.
- ✓ Q74: 1.36x — generic wswscs CTE split by year
- ✗ Q74: 0.49x — kept both original UNION and specialized CTEs
- Guard: Original UNION must be eliminated.

**STANDALONE TRANSFORMS**
**multi_intersect_exists_cte** [W3] — HIGH reliability, 1 win, avg 2.39x  
Convert cascading INTERSECT to correlated EXISTS with pre‑materialized date/channel CTEs.
- ✓ Q14: 2.39x — EXISTS short‑circuits vs. full materialization
- Guard: Not for small result sets (<1000 rows).
**intersect_to_exists** [W3] — MEDIUM reliability, 1 win, avg 1.83x  
Replace INTERSECT with EXISTS to avoid full materialization and sorting.
- ✓ Q14: 1.83x — semi‑join short‑circuit
- Guard: (none)
**materialize_cte** [W3] — MEDIUM reliability, 1 win, avg 1.37x  
Extract repeated subquery patterns into CTEs to avoid recomputation.
- ✓ Q95: 1.37x — multi‑warehouse order detection into CTE
- ✗ Q16: 0.14x — converted EXISTS to materialized CTE, forced full scan
- Guard: Never convert EXISTS/NOT EXISTS used as filter into materialized CTE.
**deferred_window_aggregation** [W3] — MEDIUM reliability, 1 win, avg 1.36x  
Delay window functions until after joins reduce the dataset.
- ✓ Q51: 1.36x — removed WINDOW from CTEs, computed SUM once after join
- Guard: Not when CTE window is referenced by multiple consumers.

**GLOBAL GUARD RAILS**
1. Never split same‑column ORs — engine handles natively. Caused 0.59x on Q90.
2. Never cross‑join 3+ dimension CTEs — Cartesian explosion. Caused 0.0076x on Q26.
3. Never convert EXISTS/NOT EXISTS used as filter into materialized CTE — destroys semi‑join short‑circuit. Caused 0.14x on Q16.
4. Max 3 UNION branches — 6+ duplicates fact scans. Caused 0.23x on Q13.
5. Skip transform if baseline <100ms — CTE overhead dominates. Caused 0.50x on Q25.
6. Every dimension CTE must have a WHERE clause — unfiltered CTE = pure overhead. Caused 0.85x on Q67.
7. Preserve all WHERE filters when decorrelating — missing filter causes cross‑product (0.34x).
8. Check EXPLAIN before decorrelating — if hash join, optimizer already decorrelated.
9. Avoid on self‑joins — each UNION branch re‑does the self‑join. Caused 0.51x.
10. Avoid on window‑function‑dominated queries — filtering not the bottleneck. Caused 0.87x on Q51.