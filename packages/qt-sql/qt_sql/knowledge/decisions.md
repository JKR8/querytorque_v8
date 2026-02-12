# Query Rewrite Decision Cards
# Distilled from 22 gold wins + 10 regressions across TPC-DS & DSB benchmarks
#
# Each card starts from the PATHOLOGY — what you see in the execution plan
# that signals an optimization opportunity. The reasoning flows:
# pathology → surface cost → engine gap → implications → detection →
# restructuring → risk calibration → evidence.

---

## PATHOLOGY 1: Large scan feeding into join — filter not pushed down

SURFACE COST:
  Millions of fact-table rows enter a join, most discarded by a filter
  that appears ABOVE the join in the plan. The join processes 10-100x
  more rows than the final result needs.

ENGINE GAP: CROSS_CTE_PREDICATE_BLINDNESS
  DuckDB plans each CTE as an independent subplan. Predicates in the
  outer query cannot propagate backward into CTE definitions. The CTE
  doesn't "know" that its output will be filtered.

IMPLICATION: This affects ANY pattern where information from one query
  scope needs to constrain another:
  - Dimension filters not reaching fact CTEs
  - Scalar subquery results not reaching referencing CTEs
  - WHERE clauses not reaching into CTE GROUP BYs
  - Self-join discriminators not reaching CTE materialization

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ SEQ_SCAN on fact table producing millions of rows
  ✓ Filter node ABOVE the scan/join, discarding 80%+ of rows
  ✓ Dimension table joined AFTER fact scan (filter in wrong position)
  ✓ Multiple CTEs where outer CTE could benefit from inner CTE's WHERE

  In SQL, look for:
  ✓ Fact table joined to 1+ dimension tables with WHERE on dimension
  ✓ Star-join topology (1 fact + 3+ dimensions)
  ✓ CTE definitions lacking filters that the outer query applies

RESTRUCTURING:
  Move the selective filter INTO a CTE, then JOIN the small CTE result
  to the fact table. The filter executes first, producing a tiny hash
  table that probes the fact scan.

  dim_filter → CTE (WHERE selective_predicate) — small result (N rows)
  fact_table JOIN dim_cte ON key → reduced from M to N rows
  → remaining joins and aggregation on reduced set

  Multi-channel variant:
    shared_dim → CTE (once)
    channel_1_fact JOIN shared_dim → aggregate
    channel_2_fact JOIN shared_dim → aggregate
    UNION ALL channels

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Single dim, selective      │ 1.3-4.0x  │ —            │ N/M < 0.2, baseline  │
  │ filter, baseline > 100ms   │           │              │ > 100ms              │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Multiple dims, each joined │ 1.5-2.7x  │ 0.0076x(Q80) │ Check: dims joined   │
  │ to fact independently      │           │              │ to EACH OTHER or     │
  │                            │           │              │ each to FACT?        │
  │                            │           │              │ Cross-dim = Cartesian│
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ ROLLUP/CUBE/window present │ 0.85-1.0x │ 0.85x (Q67)  │ CTE creates barrier  │
  │                            │           │              │ blocking ROLLUP      │
  │                            │           │              │ pushdown             │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 3+ fact tables joined      │ DON'T     │ 0.50x (Q25)  │ Pre-materializing    │
  │                            │           │              │ one fact kills       │
  │                            │           │              │ cross-fact pushdown  │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Baseline < 100ms           │ DON'T     │ 0.50x (Q25)  │ CTE overhead (5-20ms)│
  │                            │           │              │ exceeds savings      │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Orphaned CTE (original     │ DON'T     │ 0.49x (Q31)  │ Original unfiltered  │
  │ path kept alongside CTE)   │           │              │ CTE still in WITH    │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  Guard rules:
  - Every CTE MUST have a WHERE clause — unfiltered CTEs are pure overhead
  - Multiple dim CTEs: join each to fact sequentially, NEVER cross-join dims
  - Limit cascading fact-table CTE chains to 2 levels max
  - Never create a CTE AND keep the original unfiltered path

TRANSFORMS: date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch,
  shared_dimension_multi_channel, prefetch_fact_join, early_filter, pushdown

GOLD EXAMPLES:
  Win: Q6 (4.00x), Q93 (2.97x), Q63 (3.77x), Q43 (2.71x), Q29 (2.35x), Q26 (1.93x)
  Win (PG): Q080 (3.32x), Q072 (2.68x), Q099 (2.28x)
  Regression: Q80 (0.0076x Cartesian), Q25 (0.50x low baseline), Q31 (0.49x orphaned CTE),
              Q67 (0.85x ROLLUP blocked), Q51 (0.87x window blocked)

---

## PATHOLOGY 2: Same fact table scanned N times with identical joins

SURFACE COST:
  The plan shows N separate sequential scans of the same large fact table
  (e.g., store_sales). Each scan applies the same dimension joins but a
  different bucket filter. Total I/O is N× what a single scan would cost.
  With N=8, the query runs 8× slower than necessary.

ENGINE GAP: REDUNDANT_SCAN_ELIMINATION
  The optimizer cannot detect that the same fact table is scanned N times
  with similar filters across subquery boundaries. Each subquery is an
  independent plan unit. The optimizer has no Common Subexpression
  Elimination (CSE) across scalar subquery boundaries.

IMPLICATION: This gap fires whenever a query computes multiple aggregates
  over the same base data with different filter slices:
  - N time-bucket subqueries (count per hour) → N fact scans
  - N scalar subqueries computing statistics per category → N fact scans
  - N channel aggregates with shared dimensions → N fact scans
  - Cross-tab / pivot patterns where each cell is a separate subquery

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ N separate SEQ_SCAN nodes on the same table
  ✓ Each scan has similar row counts and similar join structure
  ✓ Total execution time roughly N× a single scan's time

  In SQL, look for:
  ✓ Same fact table name appearing N times (N >= 3) in FROM clauses
  ✓ Each subquery has identical dimension joins but different WHERE bucket
  ✓ Each computes a simple aggregate (COUNT, SUM, AVG) over its bucket

RESTRUCTURING:
  Merge all N subqueries into a single scan. Use CASE WHEN to label
  each row's bucket, then aggregate with conditional expressions.

  dim_filter_1 → CTE (pre-filter shared dimension, compute bucket labels)
  dim_filter_2 → CTE (pre-filter shared dimension)
  fact_table JOIN dim_ctes → single scan, one row per fact row
  → SELECT COUNT(CASE WHEN bucket = 1 THEN 1 END) AS bucket_1,
           COUNT(CASE WHEN bucket = 2 THEN 1 END) AS bucket_2, ...

  DuckDB variant (cleaner syntax):
  → SELECT COUNT(*) FILTER (WHERE bucket = 1) AS bucket_1, ...

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 3-8 identical-join         │ 1.5-6.2x  │ —            │ Count subqueries     │
  │ subqueries on same fact    │           │              │ referencing same     │
  │                            │           │              │ fact table           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 15 scalar subqueries       │ 4.5x      │ —            │ Correlated scalars   │
  │ on same fact               │           │              │ on same base table   │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ STDDEV/VARIANCE/PERCENTILE │ DON'T     │ wrong results│ Grouping-sensitive   │
  │                            │           │              │ aggregates can't be  │
  │                            │           │              │ merged via CASE      │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safest pathology to fix.

  Guard rules:
  - Each subquery must have structurally identical joins (same tables, same keys)
  - Only safe for COUNT, SUM, AVG, MIN, MAX — NOT STDDEV, VARIANCE, PERCENTILE
  - Bucket labels can be computed in a dimension CTE (e.g., time_dim → bucket mapping)
  - Tested up to 8 branches; beyond 8 is untested

TRANSFORMS: single_pass_aggregation, channel_bitmap_aggregation

GOLD EXAMPLES:
  Win: Q88 (6.24x), Q9 (4.47x), Q61 (2.27x), Q32 (1.61x), Q4 (1.53x), Q90 (1.47x)
  Regression: none

---

## PATHOLOGY 3: Nested loop executing correlated subquery per outer row

SURFACE COST:
  EXPLAIN shows a nested loop where the inner side is a subquery that
  re-executes for every row of the outer table. If the outer produces
  100K rows and the inner scans a fact table, total work is 100K × fact
  scan cost. The subquery computes an aggregate (AVG, SUM) that could
  be computed once for all groups.

ENGINE GAP: CORRELATED_SUBQUERY_PARALYSIS
  The optimizer cannot automatically decorrelate correlated aggregate
  subqueries into GROUP BY + hash join. When a scalar subquery references
  an outer column (WHERE outer.key = inner.key), the optimizer falls back
  to nested-loop re-execution instead of recognizing the GROUP BY + JOIN
  equivalence.

IMPLICATION: This gap fires whenever:
  - WHERE col > (SELECT AVG(...) FROM ... WHERE outer.key = inner.key)
  - SELECT (SELECT SUM(...) FROM ... WHERE outer.key = inner.key) AS val
  - Multiple correlated EXISTS checking overlapping conditions
  - Any correlated aggregate comparison (>, <, =, HAVING)

  It does NOT fire for:
  - EXISTS/NOT EXISTS (uses semi-join short-circuit — already fast)
  - Uncorrelated subqueries (computed once, no per-row cost)
  - Correlated subqueries the optimizer already decorrelates (check EXPLAIN)

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ Nested loop with inner side showing subquery re-execution
  ✓ Inner side includes an aggregate function (AVG, SUM, COUNT)
  ✓ Total actual time dominated by the nested loop node

  If EXPLAIN shows hash join on the correlation key:
  → Optimizer ALREADY decorrelated it → no benefit (most common false positive)

  In SQL, look for:
  ✓ WHERE col > (SELECT AGG(...) FROM ... WHERE outer.key = inner.key)
  ✓ Correlated scalar subquery with GROUP BY on correlation column

RESTRUCTURING:
  Extract the correlated aggregate into a standalone CTE with GROUP BY
  on the correlation key, then JOIN the result back.

  filtered_base → GROUP BY correlation_key, AGG(measure) → CTE
  outer_query JOIN cte ON correlation_key
  → WHERE outer.measure > cte.threshold

  Multiple correlated EXISTS variant:
    shared_dims → CTE (extract shared dimension filters)
    exists_1 → SELECT DISTINCT key FROM ... → CTE
    exists_2 → SELECT DISTINCT key FROM ... → CTE
    outer JOIN (exists_1 UNION exists_2) ON key

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Correlated aggregate,      │ 1.5-2.9x  │ —            │ EXPLAIN: nested loop │
  │ many groups, nested loop   │           │              │ with subquery        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Optimizer already          │ DON'T     │ 0.71x (Q1)   │ EXPLAIN: hash join   │
  │ decorrelated (hash join)   │           │              │ on correlation key   │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ EXISTS/NOT EXISTS pattern  │ DON'T     │ 0.34x (Q93)  │ EXISTS keyword in    │
  │                            │           │              │ subquery — semi-join │
  │                            │           │              │ destroyed by CTE     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Few correlation groups     │ DON'T     │ 0.71x (Q1)   │ Outer WHERE filters  │
  │ (1-5 after outer filter)   │           │              │ to small set —       │
  │                            │           │              │ re-execution is cheap│
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  Guard rules:
  - NEVER decorrelate EXISTS/NOT EXISTS — semi-join short-circuit is faster
  - MUST preserve ALL WHERE filters from original subquery in the CTE
  - Check EXPLAIN first: if hash join, optimizer already handled it
  - On PostgreSQL: use AS MATERIALIZED to prevent CTE inlining

TRANSFORMS: decorrelate, composite_decorrelate_union,
  early_filter_decorrelate (PG), inline_decorrelate_materialized (PG)

GOLD EXAMPLES:
  Win: Q1 (2.92x), Q35 (2.42x)
  Win (PG): Q032 (timeout rescue), Q001 (1.13x)
  Regression: Q93 (0.34x semi-join destroyed), Q1 (0.71x incremental lost)

---

## PATHOLOGY 4: Aggregation after join — fact table fan-out before GROUP BY

SURFACE COST:
  The plan joins a large fact table (M rows) to dimension tables first,
  producing M joined rows (possibly more with fan-out), THEN aggregates
  with GROUP BY. The GROUP BY keys are actually fact-table columns that
  could be aggregated BEFORE the join, reducing M to K groups (K << M).
  The join processes M rows when it only needs K.

ENGINE GAP: AGGREGATE_BELOW_JOIN_BLINDNESS
  The optimizer cannot push GROUP BY aggregation below joins even when
  the aggregation keys align with the join keys. It always executes:
  scan → join → aggregate. It cannot recognize the algebraically
  equivalent: scan → aggregate → join (which processes far fewer rows
  through the join).

IMPLICATION: This fires when:
  - GROUP BY is on fact-table columns that are join keys
  - Dimension tables are only used for labels (name, category) in SELECT
  - Fact table has high fan-out per join key (many rows per key)
  - The join is purely for decorating the result with dimension attributes

  It does NOT fire when:
  - Dimension columns appear in WHERE (need join BEFORE aggregation)
  - GROUP BY keys don't align with join keys (can't push down)

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ Large fact scan → hash join with dimension → aggregate node
  ✓ Aggregate input rows ≈ fact table size (join didn't reduce rows)
  ✓ Aggregate output rows << input rows (high fan-out per group)

  In SQL, look for:
  ✓ GROUP BY on fact-table columns (e.g., GROUP BY item_sk)
  ✓ Same columns are join keys (fact.item_sk = dim.item_sk)
  ✓ Dimension columns only in SELECT, not WHERE
  ✓ GROUP BY keys are a SUPERSET of join keys (strict requirement)

RESTRUCTURING:
  Pre-aggregate the fact table by the join key BEFORE the dimension join.
  The join then processes K aggregated rows instead of M raw rows.

  dim_filter → CTE (optional, if dimension has WHERE)
  fact_table [JOIN dim_filter] → GROUP BY join_key, SUM(measure), COUNT(*) → CTE
  agg_cte JOIN dimension ON join_key → add label columns (name, category)
  → ROLLUP / ORDER BY / LIMIT on labeled result

  AVG with ROLLUP special case:
  Pre-aggregation changes row count per group, so AVG cannot be directly
  pre-aggregated. Must split: SUM(val) + COUNT(val) pre-aggregated,
  then reconstruct AVG = SUM(sum_col) / SUM(cnt_col) at the ROLLUP level.

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ High fan-out, keys align,  │ 5-43x     │ —            │ GROUP BY keys ⊇      │
  │ dims are labels only       │           │              │ join keys, dims in   │
  │                            │           │              │ SELECT not WHERE     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Keys don't align           │ WRONG     │ wrong results│ GROUP BY keys ⊅      │
  │                            │ RESULTS   │              │ join keys — grain    │
  │                            │           │              │ collapsed, data lost │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Low fan-out (1-2 rows/key) │ ~1.0x     │ —            │ EXPLAIN row counts   │
  │                            │           │              │ before/after GROUP   │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded.

  Guard rules:
  - Key alignment is a CORRECTNESS gate — GROUP BY keys MUST be ⊇ join keys
  - No amount of performance justifies wrong results
  - When AVG + ROLLUP: split into SUM + COUNT, reconstruct after ROLLUP

TRANSFORMS: aggregate_pushdown

GOLD EXAMPLES:
  Win: Q22 (42.90x — single biggest win in entire benchmark)
  Regression: none

---

## PATHOLOGY 5: Cross-column OR evaluated as single scan with row-by-row filter

SURFACE COST:
  A WHERE clause with OR conditions spanning different columns forces the
  optimizer into a single sequential scan that evaluates every OR branch
  for every row. With 3 OR branches each selecting 5% of rows, the scan
  reads 100% of rows but only keeps 15%.

ENGINE GAP: CROSS_COLUMN_OR_DECOMPOSITION
  The optimizer can efficiently handle same-column ORs (via BitmapOr /
  index union) but cannot decompose ORs that span different columns into
  independent targeted scans. It must evaluate all branches in a single
  pass because it has no mechanism to split the scan by column family.

IMPLICATION: This gap fires ONLY for cross-column ORs:
  - WHERE zip IN (...) OR state IN (...) OR price > threshold
  - WHERE (dim_a = X) OR (dim_b = Y) — different dimensions
  - WHERE (t_hour = 8 AND t_minute >= 30) OR (t_hour = 9 AND t_minute < 30)

  It does NOT fire for:
  - WHERE col = 1 OR col = 2 OR col = 3 (same column — engine handles natively)
  - WHERE col IN (1, 2, 3) (equivalent to same-column OR)

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ Single SEQ_SCAN with filter showing OR condition
  ✓ Filter discards 70%+ of scanned rows
  ✓ OR branches reference different columns or different tables

  In SQL, look for:
  ✓ OR conditions on DIFFERENT columns (critical distinction)
  ✓ Each OR branch has low selectivity (< 20% of fact rows)
  ✓ Maximum 3 top-level OR branches
  ✓ No nested ORs (A OR B) AND (C OR D) — expansion is multiplicative

  STOP signals in SQL:
  ✗ Same column in all OR arms → engine handles this, don't split
  ✗ > 3 branches → N UNION branches = N fact scans
  ✗ Self-join present → each UNION branch re-does the self-join
  ✗ Nested OR → multiplicative expansion (2×3 = 6 branches)

RESTRUCTURING:
  Split into UNION ALL branches, one per OR condition. Each branch
  does a targeted scan on its specific predicate.

  shared_dims → CTE (factor out common dimension filters)
  UNION ALL (
    fact JOIN shared_dims WHERE branch_1_predicate,
    fact JOIN shared_dims WHERE branch_2_predicate,
    fact JOIN shared_dims WHERE branch_3_predicate
  ) → deduplicate if semantically required → aggregation

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 2-3 cross-column OR        │ 1.3-3.2x  │ —            │ Different columns    │
  │ branches, no self-join     │           │              │ in OR arms           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Same-column OR             │ DON'T     │ 0.59x (Q90)  │ Same column in all   │
  │                            │           │              │ OR arms — engine     │
  │                            │           │              │ handles natively     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ > 3 branches               │ DON'T     │ 0.23x (Q13)  │ Each branch rescans  │
  │                            │           │              │ fact: 9 = 9× I/O    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Nested OR (multiplicative) │ DON'T     │ 0.23x (Q13)  │ (A OR B) AND (C OR  │
  │                            │           │              │ D) = 4 branches      │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Self-join present          │ DON'T     │ 0.51x (Q23)  │ Table joined to      │
  │                            │           │              │ itself — each branch │
  │                            │           │              │ re-does self-join    │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  HIGHEST VARIANCE family. Our biggest wins AND worst regressions.

  Guard rules:
  - ALWAYS extract shared dimension filters into a CTE BEFORE the split
  - UNION ALL (not UNION) unless deduplication is semantically required
  - Hard limit: 3 branches maximum

TRANSFORMS: or_to_union

GOLD EXAMPLES:
  Win: Q15 (3.17x), Q88 (6.28x via time-range OR), Q10 (1.49x), Q45 (1.35x)
  Regression: Q13 (0.23x 9 branches), Q48 (0.41x nested OR),
              Q90 (0.59x same-col), Q23 (0.51x self-join)

---

## PATHOLOGY 6: LEFT JOIN preserving NULL rows that WHERE immediately discards

SURFACE COST:
  The plan executes a LEFT JOIN, preserving all left-side rows (including
  those with no match → NULL on right side). Then a WHERE filter on a
  right-table column immediately discards all NULL rows. The LEFT JOIN
  did unnecessary work preserving rows that were guaranteed to be thrown
  away. Worse, the optimizer can't reorder LEFT JOINs (they're not
  commutative), so a selective dimension filter that SHOULD run first
  is stuck in the wrong position.

ENGINE GAP: LEFT_JOIN_FILTER_ORDER_RIGIDITY
  The optimizer cannot infer that a WHERE filter on the right table makes
  a LEFT JOIN semantically equivalent to an INNER JOIN. It also cannot
  reorder LEFT JOINs to apply selective filters first, because LEFT JOIN
  is not commutative. These two limitations compound: the query is stuck
  with both the wrong join type AND the wrong join order.

IMPLICATION: This fires whenever:
  - LEFT JOIN ... WHERE right_table.col = value (proves right side non-null)
  - LEFT JOIN ... WHERE right_table.col IS NOT NULL (explicit)
  - LEFT JOIN chain where selective filter is on a late-joined table

  It does NOT fire when:
  - CASE WHEN right_col IS NULL (NULL branch is semantically needed)
  - COALESCE(right_col, default) (relies on NULL preservation)
  - No WHERE filter on right-table columns

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ LEFT JOIN node producing many rows
  ✓ Filter node immediately above discarding NULL-right rows
  ✓ Join order doesn't match selectivity order

  In SQL, look for:
  ✓ LEFT JOIN followed by WHERE on right-table column
  ✓ No IS NULL / COALESCE logic on right-table columns

RESTRUCTURING:
  Convert LEFT JOIN to INNER JOIN. Optionally, pre-filter the right table
  into a CTE and join early.

  right_table_filter → CTE (selective WHERE on right table)
  left_table INNER JOIN right_cte ON key
  → remaining joins and aggregation

  Once INNER, the optimizer can:
  - Reorder joins freely (INNER is commutative)
  - Choose a completely different plan with better selectivity estimation

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ LEFT JOIN + right-table    │ 1.5-3.4x  │ —            │ WHERE on right-table │
  │ WHERE, no NULL logic       │           │              │ column after LEFT    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ CASE WHEN IS NULL present  │ DON'T     │ wrong results│ IS NULL / COALESCE   │
  │                            │           │              │ on right-table col   │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe pathology to fix.

TRANSFORMS: inner_join_conversion

GOLD EXAMPLES:
  Win: Q93 (3.44x), Q80 (1.89x)
  Regression: none

---

## PATHOLOGY 7: INTERSECT materializing both sides before comparison

SURFACE COST:
  The plan fully materializes both sides of an INTERSECT — computing the
  complete result set of each branch — then compares them row by row.
  If each side produces 100K+ rows from fact-table joins, the
  materialization cost dominates. An EXISTS semi-join would stop at the
  first match per row.

ENGINE GAP: none (general INTERSECT implementation)
  INTERSECT is implemented as set materialization + comparison. The
  optimizer doesn't recognize that a semi-join (EXISTS) is algebraically
  equivalent and can short-circuit.

IMPLICATION: This fires for:
  - INTERSECT between two large computed result sets
  - EXCEPT (same mechanism, convert to NOT EXISTS)
  - Multiple chained INTERSECTs

  It does NOT fire for:
  - INTERSECT on small result sets (< 1000 rows — materialization is cheap)

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ Two large materialization nodes feeding an INTERSECT operator
  ✓ Each side produces 10K+ rows from fact-table joins

  In SQL, look for:
  ✓ INTERSECT keyword between two queries
  ✓ Each side joins to fact tables or produces large intermediate results

RESTRUCTURING:
  Replace INTERSECT with EXISTS semi-join.

  base_query → CTE (if referenced multiple times)
  SELECT ... FROM side_1
  WHERE EXISTS (SELECT 1 FROM side_2 WHERE side_2.key = side_1.key)

  Multiple INTERSECTs:
  WHERE EXISTS (...) AND EXISTS (...)

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Large INTERSECT, keys are  │ 1.8-2.7x  │ —            │ INTERSECT + large    │
  │ join-friendly              │           │              │ result sets          │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Small INTERSECT (< 1K)     │ ~1.0x     │ —            │ Both sides small     │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe pathology to fix.

TRANSFORMS: intersect_to_exists, multi_intersect_exists_cte

GOLD EXAMPLES:
  Win: Q14 (2.72x SF10), Q14 (2.39x multi_intersect variant)
  Regression: none

---

## PATHOLOGY 8: Self-joined CTE materialized for all values, post-filtered per arm

SURFACE COST:
  A CTE computes an aggregate for ALL months/years/categories, then is
  self-joined with different discriminator filters per arm (e.g.,
  WHERE month = 1 on left, WHERE month = 2 on right). The CTE
  materializes the full result, and each self-join arm discards 11/12ths
  of the data. The CTE does 12× the work it needs to.

ENGINE GAP: UNION_CTE_SELF_JOIN_DECOMPOSITION + CROSS_CTE_PREDICATE_BLINDNESS
  The optimizer materializes the CTE once for all values. It cannot push
  the self-join discriminator filters backward into the CTE definition,
  because CTE predicates don't propagate. Each arm post-filters the full
  materialized result instead of computing only its needed partition.

IMPLICATION: This fires whenever:
  - A CTE is joined to itself with different WHERE on each arm
  - A UNION aggregates CTEs that share structure but differ by partition key
  - ROLLUP generates all grouping levels when only specific levels are needed
  - Year-over-year / month-over-month comparison queries

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ CTE materialization producing large result (all partitions)
  ✓ Two+ hash joins or nested loops on the same CTE
  ✓ Each join has a filter discarding most CTE rows

  In SQL, look for:
  ✓ WITH cte AS (...) SELECT ... FROM cte a JOIN cte b ON key WHERE a.period = 1 AND b.period = 2
  ✓ UNION of queries with identical structure but different filter constants
  ✓ ROLLUP that could be decomposed into targeted per-level aggregates

RESTRUCTURING:
  Split the CTE into per-partition CTEs, each computing only its needed
  partition's data.

  partition_1 → CTE (WHERE discriminator = val_1, GROUP BY key, AGG(...))
  partition_2 → CTE (WHERE discriminator = val_2, GROUP BY key, AGG(...))
  partition_1 JOIN partition_2 ON shared_key
  → comparison / computation across partitions

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Self-join with 2-4         │ 1.4-4.8x  │ —            │ CTE joined to itself │
  │ discriminator values,      │           │              │ with different WHERE │
  │ original CTE removed       │           │              │ per arm              │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Original CTE kept (orphan) │ DON'T     │ 0.49x (Q31)  │ Original CTE still   │
  │                            │           │              │ in WITH clause after │
  │                            │           │              │ split — double       │
  │                            │           │              │ materialization      │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 5+ discriminator values    │ CAUTION   │ diminishing  │ Too many partition   │
  │                            │           │ returns      │ CTEs, complexity     │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  Guard rules:
  - CRITICAL: remove the original combined CTE after splitting
  - Redirect ALL references to appropriate partition CTE
  - Limit to 2-4 partitions (diminishing returns beyond)
  - On PostgreSQL: CTE materialization is default → even more effective

TRANSFORMS: self_join_decomposition, union_cte_split, rollup_to_union_windowing

GOLD EXAMPLES:
  Win: Q39 (4.76x), Q36 (2.47x), Q74 (1.57x), PG-Q065 (3.93x)
  Regression: Q74 (0.68x orphaned CTE), Q31 (0.49x duplicated CTEs)

---

## PATHOLOGY 9: Window functions computed in CTEs before join — N window passes

SURFACE COST:
  Multiple CTEs each compute their own window function (SUM OVER,
  ROW_NUMBER OVER) before being joined together. The plan shows N
  separate window computation nodes. If the window is over the same
  ordering, it could be computed once after the join instead of N times
  before it.

ENGINE GAP: none (general optimization principle)
  The optimizer doesn't recognize that a window function can be deferred
  past a join when the partition/ordering is preserved. It computes the
  window in the CTE because that's where the SQL places it.

IMPLICATION: This fires when:
  - 2+ CTEs each have window functions (SUM OVER, cumulative aggregates)
  - The CTEs are then joined (especially FULL OUTER JOIN)
  - The window function's PARTITION BY aligns with the join key

  It does NOT fire when:
  - Window uses LAG, LEAD (depends on pre-join row ordering)
  - Window uses ROWS BETWEEN with specific frame (semantics change after join)
  - CTE's window result is referenced by other consumers

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ N separate WINDOW nodes inside CTE scans
  ✓ All windows use the same ORDER BY key
  ✓ CTEs are joined on that same key

  In SQL, look for:
  ✓ SUM(...) OVER (ORDER BY date) inside CTE definition
  ✓ CTE is then joined to another CTE with similar window

RESTRUCTURING:
  Remove window functions from CTEs (keep only GROUP BY for base
  aggregates). Join the reduced CTEs. Compute the window once on
  the joined result.

  base_1 → GROUP BY key (daily totals only, NO window) → CTE
  base_2 → GROUP BY key (daily totals only, NO window) → CTE
  cte_1 FULL OUTER JOIN cte_2 ON date_key
  → SUM() OVER (ORDER BY date) on joined result (one window pass)

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 2+ CTEs with windows       │ 1.3-1.4x  │ —            │ WINDOW inside CTE    │
  │ joined on window key       │           │              │ that feeds a JOIN    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ LAG/LEAD in CTE            │ DON'T     │ wrong results│ LAG/LEAD depends on  │
  │                            │           │              │ pre-join row order   │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe pathology to fix.

  Guard rules:
  - SUM() naturally skips NULLs from FULL OUTER JOIN — no COALESCE needed
  - Only safe when join doesn't change window's row ordering semantics

TRANSFORMS: deferred_window_aggregation

GOLD EXAMPLES:
  Win: Q51 (1.36x)
  Regression: none

---

## PATHOLOGY 10: Shared subexpression executed multiple times (no CSE)

SURFACE COST:
  The same expensive subquery (fact-table join + aggregate) appears twice
  in the query and the plan executes it twice. The plan shows two
  identical subtrees with identical costs. One execution could serve both
  consumers.

ENGINE GAP: none (general CSE limitation)
  The optimizer may or may not CSE identical subqueries across different
  query branches. When it doesn't, the cost is 2× (or N×) what it could
  be. Manual CTE materialization forces single execution.

IMPLICATION: This fires when:
  - Same subquery text appears 2+ times in the query
  - The subquery is expensive (joins large tables, aggregates)
  - The optimizer didn't recognize the duplication

  CRITICAL — it does NOT fire for:
  - EXISTS/NOT EXISTS subqueries — these use semi-join with early
    termination. Materializing forces full scan, destroying the
    optimization. This is the #1 regression cause in the entire system.
    (Q16: 0.14x — materialized EXISTS on full fact table before filtering)

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ Two subtrees with identical structure and similar costs
  ✓ Both scanning the same tables with same joins

  In SQL, look for:
  ✓ Same subquery text appearing 2+ times
  ✓ NOT an EXISTS/NOT EXISTS pattern

  HARD STOP signals:
  ✗ EXISTS or NOT EXISTS in the repeated expression → NEVER materialize
  ✗ Subquery is trivial (single table, few rows) → overhead wasted
  ✗ Outer query pushes filters through inline subquery → CTE blocks this

RESTRUCTURING:
  Extract the shared subexpression into a CTE.

  shared_expr → CTE (WITH shared AS (...))
  query_arm_1 references shared
  query_arm_2 references shared

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Shared expensive join/agg  │ 1.3-1.4x  │ —            │ Same subquery text   │
  │ referenced 2+ times        │           │              │ appears 2+ times     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ EXISTS/NOT EXISTS           │ DON'T     │ 0.14x (Q16)  │ EXISTS keyword in    │
  │ materialized               │ EVER      │              │ the shared expr —    │
  │                            │           │              │ semi-join destroyed  │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Correlated EXISTS split    │ DON'T     │ 0.54x (Q95)  │ Decomposing          │
  │ into independent CTEs      │           │              │ correlated EXISTS    │
  │                            │           │              │ severs cardinality   │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ CTE without WHERE clause   │ DON'T     │ overhead     │ No WHERE in CTE def  │
  │                            │           │              │ = unfiltered scan    │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  Guard rules:
  - NEVER materialize EXISTS/NOT EXISTS — #1 regression cause
  - Every CTE MUST have a WHERE clause — unfiltered = pure overhead
  - On PostgreSQL: CTEs are materialized by default
  - PostgreSQL: AS MATERIALIZED / AS NOT MATERIALIZED to control behavior

TRANSFORMS: materialize_cte

GOLD EXAMPLES:
  Win: Q95 (1.43x), PG-Q065 (3.93x)
  Regression: Q16 (0.14x EXISTS materialized), Q95 (0.54x cardinality severed)

---

## PATHOLOGY 11: Predicate appears late in CTE chain — upstream CTEs process unfiltered rows

SURFACE COST:
  Predicate appears late in CTE chain (or main query). All upstream CTEs
  process full unfiltered row sets. Most rows are eventually discarded by
  the late predicate. Work wasted: all upstream materialization of rows
  that won't survive the filter.

ENGINE GAP: CROSS_CTE_PREDICATE_BLINDNESS
  DuckDB plans each CTE independently. A WHERE clause in CTE_C or the
  main query cannot propagate backward into CTE_A or CTE_B. Each CTE
  materializes its full result set regardless of how downstream consumers
  filter it.

IMPLICATION: This is the GENERAL CASE of dimension isolation.
  date_cte_isolate, early_filter, prefetch_fact_join,
  multi_dimension_prefetch are all specific instances where the predicate
  being pushed back is a dimension filter. But the principle applies to
  ANY selective predicate anywhere in the chain:
  - Dimension filters (most common)
  - HAVING thresholds that imply row constraints
  - JOIN conditions that are effectively filters
  - Subquery results that constrain fact rows
  The rule is always the same: find the most selective predicate, find the
  earliest CTE where it CAN apply, put it there.

DETECTION:
  In EXPLAIN ANALYZE, look for:
  ✓ CTE scan produces N rows
  ✓ Downstream node (join, filter, or next CTE) reduces to N/K
  ✓ This pattern repeats across multiple chain stages
  ✓ Filter node appears 2+ stages below where it could apply

  In SQL, look for:
  ✓ WHERE clause in main query references column available in an earlier CTE
  ✓ JOIN ON condition uses a key that could pre-filter an upstream CTE
  ✓ Long CTE chain (3+ stages) with filters only at the end

  Row count signal:
  Compare row counts stage by stage through the chain. If rows stay flat
  (or grow) through stages 1-3 then drop sharply at stage 4, the filter
  at stage 4 should have been at stage 1.

RESTRUCTURING:
  1. Identify the most selective predicate in the chain
  2. Trace backward: what's the earliest CTE where this predicate's columns
     are available (or can be made available via a dimension join)?
  3. If columns aren't available: create a small filtered dimension CTE and
     join it at the earliest stage
  4. If columns are available: move the WHERE clause there
  5. Repeat for next most selective predicate

  Target state: rows should decrease monotonically through the CTE chain.
  If they stay flat or increase at any stage, a pushback opportunity exists.

  Ordering principle:
  Push the MOST SELECTIVE predicate first. Selectivity compounds — once the
  first filter reduces 7M to 50K, every subsequent join and filter operates
  on 50K.

  When multiple predicates exist:
  - Dimension filter on date (usually most selective) → first
  - Dimension filter on geography/category → second
  - Aggregate threshold / HAVING → after dimension filters
  - Correlated predicate → last (after base is small)

RISK:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Single predicate pushed    │ 1.3-4.0x  │ —            │ Monotonic row count  │
  │ back, selective filter     │           │              │ violation in chain   │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Multiple pushbacks         │ compounds │ —            │ 1.5x * 1.5x = 2.25x │
  │                            │ (multiply)│              │                      │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 3+ fact tables joined      │ DON'T     │ 0.50x (Q25)  │ Pushback locks join  │
  │                            │           │              │ reorder opportunity  │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Predicate selectivity < 2:1│ DON'T     │ overhead     │ Overhead exceeds     │
  │                            │           │              │ savings              │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ CTE already filtered       │ DON'T     │ 0.71x (Q1)   │ Decomposing further  │
  │                            │           │              │ = overhead           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ ROLLUP/WINDOW downstream   │ CAUTION   │ 0.85x (Q67)  │ May block further    │
  │                            │           │              │ optimization         │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  Guard rules:
  - Every CTE MUST have a WHERE clause — unfiltered CTEs are pure overhead
  - Multiple dim CTEs: join each to fact, NEVER cross-join dims
  - Limit cascading fact-table CTE chains to 2 levels max

TRANSFORMS: date_cte_isolate, prefetch_fact_join, multi_dimension_prefetch,
  multi_date_range_cte, early_filter, aggregate_pushdown (compound)

GOLD EXAMPLES:
  Win: Q6/Q11 (4.00x date pushed to first CTE), Q93 (2.97x dim filter before LEFT JOIN chain),
       Q63 (3.77x date filter pushed then fact pre-joined), Q43 (2.71x two predicates simultaneously),
       Q29 (2.35x three date predicates each pushed to their fact), Q22 (42.90x pushback + pre-agg)
  Regression: Q25 (0.50x pushback into 3-way fact join locked order),
              Q67 (0.85x pushback blocked by ROLLUP), Q1 (0.71x over-decomposed already-efficient CTE)

---

## SAFETY RANKING

  ┌──────┬─────────────────────────────────┬──────────┬────────────┬───────────────────────────┐
  │ Rank │ Pathology                       │ Regr.    │ Worst case │ Recommendation            │
  ├──────┼─────────────────────────────────┼──────────┼────────────┼───────────────────────────┤
  │ 1    │ Repeated scans (scan_merge)     │ 0        │ —          │ Always fix                │
  │ 2    │ Agg after join (agg_pushdown)   │ 0        │ —          │ Always fix (verify keys)  │
  │ 3    │ LEFT→INNER (join_strengthen)    │ 0        │ —          │ Always fix                │
  │ 4    │ INTERSECT (set_rewrite)         │ 0        │ —          │ Always fix                │
  │ 5    │ Pre-join windows (window_defer) │ 0        │ —          │ Always fix                │
  │ 6    │ Self-join CTE (decompose)       │ 1 indir. │ 0.49x      │ Check orphan CTE          │
  │ 7    │ Filter not pushed (isolate)     │ 4        │ 0.0076x    │ All gates must pass       │
  │ 8    │ Correlated nested loop (decor.) │ 2        │ 0.34x      │ Check EXPLAIN first       │
  │ 9    │ Shared expr (materialize)       │ 3        │ 0.14x      │ Never on EXISTS           │
  │ 10   │ Cross-col OR (decompose)        │ 4        │ 0.23x      │ Max 3, cross-column only  │
  └──────┴─────────────────────────────────┴──────────┴────────────┴───────────────────────────┘
