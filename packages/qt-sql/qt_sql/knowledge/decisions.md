# Query Rewrite Decision Cards
# Distilled from 22 gold wins + 10 regressions across TPC-DS & DSB benchmarks
#
# Each card is a sequential decision tree: structural gate → explain check →
# cardinality calibration → risk table → evidence. Walk the gates in order.
# Stop at the first NO.

---

## DECISION 1: Should I isolate dimension filters into CTEs?

FAMILY: predicate_isolation
TRANSFORMS: date_cte_isolate, dimension_cte_isolate, multi_dimension_prefetch,
            shared_dimension_multi_channel, prefetch_fact_join, early_filter, pushdown
GAP: CROSS_CTE_PREDICATE_BLINDNESS

STRUCTURAL GATE (from SQL):
  ✓ Query joins fact table to 1+ dimension tables
  ✓ Dimension tables have WHERE filters
  → Proceed to explain plan check

EXPLAIN PLAN CHECK:
  Look at: Where does the dimension filter appear in the plan?

  IF filter is INSIDE the scan node → optimizer already pushed it down
     → LIKELY NO BENEFIT (this is what "Check EXPLAIN first" actually means)
     → But still possible if 3+ dimensions compound selectivity

  IF filter is ABOVE the join → optimizer missed the pushdown
     → LIKELY BENEFIT from CTE isolation

CARDINALITY CHECK (the calibration):
  Look at: Row counts at each stage

  Dimension CTE will produce N rows
  Fact table has M rows

  IF N/M > 0.5 → dimension isn't selective enough, CTE overhead wasted
  IF N < 1000 AND M > 100K → strong signal, tiny hash table probes huge fact
  IF baseline < 100ms → CTE materialization overhead exceeds any savings
     (Q25: 31ms baseline → 0.50x regression)

MULTI-FACT GATE:
  IF query has 3+ fact tables joined together → STOP
     Pre-materializing one fact kills cross-fact filter pushdown
     (Q25: 0.50x — locked join order by materializing early)

MULTI-DIMENSION GATE:
  IF creating 3+ dimension CTEs:
     Are they joined to EACH OTHER or each to FACT?
     Cross-dim join = Cartesian explosion → STOP
     (Q80: 0.0076x — 3 dim CTEs cross-joined = 132x slower)

     Each dim joined independently to fact → OK, proceed

ROLLUP/WINDOW GATE:
  IF query has ROLLUP, CUBE, or window functions:
     CTE may create materialization barrier that prevents pushdown
     (Q67: 0.85x — CTE blocked ROLLUP pushdown)
     → Proceed with caution, prefer leaving ROLLUP path untouched

IDEAL PLAN SHAPE (when all gates pass):
  dim_filter → CTE (WHERE selective_predicate) — small result
  fact_table JOIN dim_cte ON key → reduced fact rows
  → remaining joins and aggregation on reduced set

  Multi-channel variant:
    shared_dim → CTE (once)
    channel_1_fact JOIN shared_dim → aggregate
    channel_2_fact JOIN shared_dim → aggregate
    UNION ALL channels

COMPOSITION NOTES:
- Multiple dimension CTEs: JOIN each to fact sequentially, NEVER cross-join dims
- Limit cascading fact-table CTE chains to 2 levels max
- Every CTE MUST have a WHERE clause — unfiltered CTEs are pure overhead
- Never create a CTE AND keep the original unfiltered path (orphaned CTE: Q31 0.49x)

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Single dim, selective      │ 1.3-4.0x  │ 0.85x (Q67)  │ ROLLUP/window in     │
  │ filter, no ROLLUP          │           │              │ same query blocks    │
  │                            │           │              │ pushdown             │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Multiple dims, all joined  │ 1.5-2.7x  │ 0.0076x(Q80) │ Are dims joined to   │
  │ to fact independently      │           │              │ EACH OTHER or each   │
  │                            │           │              │ to FACT? Cross-dim   │
  │                            │           │              │ = Cartesian          │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 3+ fact tables joined      │ DON'T     │ 0.50x (Q25)  │ Count fact tables    │
  │                            │           │              │ in FROM/JOIN         │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Already has efficient      │ DON'T     │ 0.71x (Q1)   │ Existing CTE with    │
  │ CTE structure              │           │              │ filters present      │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Baseline < 100ms           │ DON'T     │ 0.50x (Q25)  │ Check baseline       │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

GOLD EXAMPLES FOR WORKERS:
  Win: Q6 (4.00x), Q93 (2.97x), Q63 (3.77x), Q43 (2.71x), Q29 (2.35x), Q26 (1.93x)
  Win (PG): Q080 (3.32x), Q072 (2.68x), Q099 (2.28x)
  Regression: Q80 (0.0076x), Q25 (0.50x), Q31 (0.49x), Q67 (0.85x), Q51 (0.87x)

---

## DECISION 2: Should I merge repeated fact-table scans into a single pass?

FAMILY: scan_merge
TRANSFORMS: single_pass_aggregation, channel_bitmap_aggregation
GAP: REDUNDANT_SCAN_ELIMINATION

STRUCTURAL GATE (from SQL):
  ✓ Same fact table appears N times (N >= 3) in separate subqueries
  ✓ Each subquery has the same dimension joins but different bucket filters
  ✓ Each computes an aggregate (COUNT, SUM, AVG) over its bucket
  → Proceed to explain plan check

  IF each subquery has structurally DIFFERENT joins → STOP, not mergeable

EXPLAIN PLAN CHECK:
  Look at: Does the plan show N separate sequential scans of the same table?

  IF yes → confirmed redundant scanning, strong signal for merge
  IF optimizer already merged them (rare) → no benefit

AGGREGATE COMPATIBILITY CHECK:
  Look at: What aggregates are used?

  COUNT, SUM, AVG, MIN, MAX → safe to merge with CASE-WHEN
  STDDEV_SAMP, VARIANCE, PERCENTILE_CONT → NOT safe
     These are grouping-sensitive — CASE branches compute per-group
     differently than separate per-branch queries

BRANCH COUNT CHECK:
  IF N >= 3 → benefit scales linearly with N (N scans → 1 scan)
  IF N = 2 → marginal, overhead of CASE evaluation may eat savings
  Tested successfully up to N = 8; beyond 8 is untested

IDEAL PLAN SHAPE (when all gates pass):
  dim_filter_1 → CTE (small, pre-filter each shared dimension)
  dim_filter_2 → CTE (small)
  fact_table JOIN dim_ctes → single scan
  → SELECT COUNT(CASE WHEN bucket = 1 THEN 1 END) AS bucket_1,
           COUNT(CASE WHEN bucket = 2 THEN 1 END) AS bucket_2, ...

  DuckDB variant (cleaner):
  → SELECT COUNT(*) FILTER (WHERE bucket = 1) AS bucket_1, ...

COMPOSITION NOTES:
- Naturally combines with predicate_isolation (pre-filter shared dims into CTEs)
- The CASE-WHEN labels can be computed in a dimension CTE (e.g., time bucket mapping)
- DuckDB's FILTER clause is cleaner than CASE WHEN — prefer it

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 3-8 identical-join         │ 1.5-6.2x  │ —            │ Count subqueries     │
  │ subqueries on same fact    │           │              │ referencing same     │
  │                            │           │              │ fact table           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 15 scalar subqueries       │ 4.5x      │ —            │ Same as above but    │
  │ on same fact               │           │              │ correlated scalars   │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded for this family. Safest transform.

GOLD EXAMPLES FOR WORKERS:
  Win: Q88 (6.24x), Q9 (4.47x), Q61 (2.27x), Q32 (1.61x), Q4 (1.53x), Q90 (1.47x)
  Regression: none

---

## DECISION 3: Should I decorrelate a correlated subquery into a CTE?

FAMILY: correlation_break
TRANSFORMS: decorrelate, composite_decorrelate_union
GAP: CORRELATED_SUBQUERY_PARALYSIS

STRUCTURAL GATE (from SQL):
  ✓ Subquery references outer table's column (WHERE outer.key = inner.key)
  ✓ Subquery computes an aggregate (AVG, SUM, COUNT) per correlation group
  → Proceed to explain plan check

  IF subquery is EXISTS/NOT EXISTS → STOP
     EXISTS uses semi-join short-circuit. Decorrelating destroys it.
     (Q93: 0.34x — decorrelated what optimizer ran as semi-join)

EXPLAIN PLAN CHECK:
  Look at: How does the optimizer execute the correlation?

  IF nested loop with subquery re-execution per outer row
     → optimizer FAILED to decorrelate → strong signal for manual decorrelation

  IF hash join on correlation key
     → optimizer ALREADY decorrelated → no benefit, STOP
     (this is the most common false positive)

SELECTIVITY CHECK:
  Look at: How many distinct correlation key values does the outer query produce?

  IF outer query filters to 1-5 groups → correlated re-execution is cheap
     → LIKELY NO BENEFIT (materialization overhead > re-execution cost)
     (Q1 regression: 0.71x — pre-aggregated when incremental was cheaper)

  IF outer query produces 100+ groups → decorrelation wins
     The CTE computes all groups once; hash join probes O(1) per outer row

MULTIPLE CORRELATED EXISTS GATE:
  IF query has 2+ correlated EXISTS on different tables:
     Consider composite decorrelation — extract shared dims once,
     decorrelate each EXISTS into a DISTINCT key CTE,
     replace OR(EXISTS a, EXISTS b) with UNION of key sets
     (Q35: 2.42x — composite approach)

IDEAL PLAN SHAPE (when all gates pass):
  filtered_base → GROUP BY correlation_key, AGG(measure) → CTE
  outer_query JOIN cte ON correlation_key
  → WHERE outer.measure > cte.threshold (or similar comparison)

COMPOSITION NOTES:
- Often combines with early_filter (push dimension filters into the base CTE)
- Never combine with EXISTS materialization — destroys semi-join
- If correlated path is inside a UNION, decorrelate each branch independently
- On PostgreSQL: use AS MATERIALIZED to prevent CTE inlining
- MUST preserve ALL WHERE filters from original subquery (Q93: 0.34x when filters dropped)

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Correlated aggregate,      │ 1.5-2.9x  │ —            │ EXPLAIN: nested loop │
  │ many groups, nested loop   │           │              │ + subquery           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Correlated aggregate,      │ DON'T     │ 0.71x (Q1)   │ EXPLAIN: hash join   │
  │ optimizer decorrelated     │           │              │ on correlation key   │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ EXISTS/NOT EXISTS pattern  │ DON'T     │ 0.34x (Q93)  │ EXISTS keyword in    │
  │                            │           │              │ subquery             │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Few correlation groups     │ DON'T     │ 0.71x (Q1)   │ Outer WHERE filters  │
  │ (1-5 after outer filter)   │           │              │ to small set         │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

GOLD EXAMPLES FOR WORKERS:
  Win: Q1 (2.92x), Q35 (2.42x)
  Win (PG): Q032 (timeout rescue), Q001 (1.13x)
  Regression: Q93 (0.34x semi-join destroyed), Q1 (0.71x incremental lost)

---

## DECISION 4: Should I push GROUP BY aggregation below the joins?

FAMILY: aggregate_pushdown
TRANSFORMS: aggregate_pushdown
GAP: AGGREGATE_BELOW_JOIN_BLINDNESS

STRUCTURAL GATE (from SQL):
  ✓ Query has GROUP BY on columns from the fact table
  ✓ Fact table is joined to 1+ dimension tables
  ✓ Dimensions are used for labeling (in SELECT) not filtering (in WHERE)
  → Proceed to key alignment check

  IF dimension columns appear in WHERE → STOP
     Need dimension join BEFORE aggregation; can't push agg below

KEY ALIGNMENT CHECK (correctness gate — strict):
  Look at: GROUP BY keys vs join keys

  GROUP BY keys MUST be a SUPERSET of the join keys.

  Example (correct):
    GROUP BY item_sk          — join key is item_sk ✓
    GROUP BY item_sk, date_sk — join keys are item_sk, date_sk ✓

  Example (WRONG — will produce incorrect results):
    GROUP BY category         — join key is item_sk ✗
    The pre-aggregation collapses item_sk values, losing the join grain

  IF keys don't align → HARD STOP, wrong results guaranteed

FAN-OUT CHECK (the calibration):
  Look at: How many fact rows per join key?

  IF fact has 100+ rows per join key → huge reduction from pre-agg (Q22: 7M → 150K)
  IF fact has 1-2 rows per join key → pre-aggregation saves nothing

AVG + ROLLUP GATE:
  IF AVG is used with ROLLUP or CUBE:
     Pre-aggregation changes row count per group
     Must reconstruct: split AVG into SUM + COUNT,
     pre-aggregate both, then AVG = SUM(sum_col) / SUM(cnt_col) at ROLLUP level
     → Correctness risk, but mechanically sound if done right

IDEAL PLAN SHAPE (when all gates pass):
  dim_filter → CTE (optional, if dimension has WHERE)
  fact_table [JOIN dim_filter ON key] → GROUP BY join_key, SUM(measure), COUNT(*) → CTE
  agg_cte JOIN dimension ON join_key → add label columns (name, category, etc.)
  → ROLLUP / ORDER BY / LIMIT on labeled result

COMPOSITION NOTES:
- Naturally combines with predicate_isolation (filter dimensions first)
- This is the single highest-impact transform (42.90x) but has a strict correctness gate
- The key alignment check is non-negotiable — no amount of performance justifies wrong results

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ High fan-out, keys align,  │ 5-43x     │ —            │ GROUP BY keys ⊇      │
  │ dims are labels only       │           │              │ join keys, dims in   │
  │                            │           │              │ SELECT not WHERE     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Keys don't align           │ WRONG     │ wrong results│ GROUP BY keys ⊅      │
  │                            │ RESULTS   │              │ join keys            │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Low fan-out (1-2 rows/key) │ ~1.0x     │ —            │ EXPLAIN row counts   │
  │                            │           │              │ before/after join    │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded (but wrong results if key alignment violated).

GOLD EXAMPLES FOR WORKERS:
  Win: Q22 (42.90x)
  Regression: none

---

## DECISION 5: Should I split cross-column OR conditions into UNION ALL?

FAMILY: or_decomposition
TRANSFORMS: or_to_union
GAP: CROSS_COLUMN_OR_DECOMPOSITION

STRUCTURAL GATE (from SQL):
  ✓ WHERE clause has OR conditions
  → Check: are they on the SAME column or DIFFERENT columns?

  IF same column (e.g., col = 1 OR col = 2) → STOP
     Engine handles this natively via BitmapOr / index union
     (Q90: 0.59x — split same-column OR that engine already optimized)

  IF different columns (e.g., zip IN (...) OR state IN (...)) → proceed

BRANCH COUNT GATE (hard limit):
  Count the number of top-level OR branches.

  IF branches <= 3 → proceed
  IF branches > 3 → STOP
     Each UNION branch rescans the fact table.
     N branches = N fact table scans.
     (Q13: 0.23x — 9 branches = 9× fact scans)

NESTED OR GATE:
  IF OR conditions are nested (e.g., (A OR B) AND (C OR D OR E)):
     Expansion = 2 × 3 = 6 branches → exceeds limit → STOP
     (Q13: 0.23x, Q48: 0.41x — Cartesian OR explosion)

SELF-JOIN GATE:
  IF query contains a self-join → STOP
     Each UNION branch must independently re-do the self-join
     (Q23: 0.51x — self-join replicated across branches)

SELECTIVITY CHECK:
  Look at: What fraction of fact rows does each branch select?

  IF each branch selects < 20% → UNION branches are targeted, net I/O reduced
  IF branches overlap heavily or each selects > 30% → UNION wastes I/O

IDEAL PLAN SHAPE (when all gates pass):
  shared_dims → CTE (factor out common dimension filters)
  UNION ALL (
    fact JOIN shared_dims WHERE branch_1_predicate,
    fact JOIN shared_dims WHERE branch_2_predicate,
    fact JOIN shared_dims WHERE branch_3_predicate
  ) → deduplicate if semantically required → aggregation

COMPOSITION NOTES:
- Always extract shared dimension filters into a CTE BEFORE the UNION split
- UNION ALL (not UNION) unless deduplication is semantically required
- If branches share identical joins, factor those into the shared CTE

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 2-3 cross-column OR        │ 1.3-3.2x  │ —            │ Different columns    │
  │ branches, no self-join     │           │              │ in OR arms           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Same-column OR             │ DON'T     │ 0.59x (Q90)  │ Same column in all   │
  │                            │           │              │ OR arms              │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ > 3 branches               │ DON'T     │ 0.23x (Q13)  │ Count OR branches    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Nested OR (multiplicative) │ DON'T     │ 0.23x (Q13)  │ OR inside AND(OR)    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Self-join present          │ DON'T     │ 0.51x (Q23)  │ Table joined to      │
  │                            │           │              │ itself               │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  HIGHEST VARIANCE family. Our biggest wins AND worst regressions.

GOLD EXAMPLES FOR WORKERS:
  Win: Q15 (3.17x), Q88 (6.28x via time-range OR), Q10 (1.49x), Q45 (1.35x)
  Regression: Q13 (0.23x), Q48 (0.41x), Q90 (0.59x), Q23 (0.51x)

---

## DECISION 6: Should I convert LEFT JOIN to INNER JOIN?

FAMILY: join_strengthening
TRANSFORMS: inner_join_conversion
GAP: LEFT_JOIN_FILTER_ORDER_RIGIDITY

STRUCTURAL GATE (from SQL):
  ✓ Query has a LEFT JOIN
  ✓ WHERE clause filters on a column from the RIGHT (nullable) table
  → Proceed to NULL-dependency check

  IF no WHERE filter on right-table columns → STOP
     LEFT JOIN semantics are genuinely needed

NULL-DEPENDENCY CHECK:
  Look at: Does the query use the NULL rows that LEFT JOIN produces?

  IF CASE WHEN right_col IS NULL → STOP
     The NULL branch is semantically meaningful

  IF COALESCE(right_col, default) → STOP
     Relies on NULL preservation

  IF right_col is only in SELECT/GROUP BY (never checked for NULL) → safe to convert

EXPLAIN PLAN CHECK:
  Look at: What does the optimizer do with the LEFT JOIN?

  IF optimizer already inferred INNER JOIN → no benefit, but conversion is still safe
     (Some optimizers are smarter about this than others)

  IF optimizer preserves LEFT JOIN semantics → conversion unlocks:
     - Join reordering (INNER JOINs are commutative, LEFT JOINs aren't)
     - Potentially different plan with better selectivity estimation

IDEAL PLAN SHAPE (when all gates pass):
  right_table_filter → CTE (selective WHERE on right table)
  left_table INNER JOIN right_cte ON key
  → remaining joins and aggregation

COMPOSITION NOTES:
- Naturally combines with predicate_isolation (filter right table into CTE first)
- Once converted to INNER, optimizer may choose a completely different join order
- This often reveals FURTHER optimization opportunities downstream

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ LEFT JOIN + right-table    │ 1.5-3.4x  │ —            │ WHERE on right-      │
  │ WHERE, no NULL logic       │           │              │ table column after   │
  │                            │           │              │ LEFT JOIN            │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ CASE WHEN IS NULL present  │ DON'T     │ wrong results│ IS NULL check on     │
  │                            │           │              │ right-table column   │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe transform.

GOLD EXAMPLES FOR WORKERS:
  Win: Q93 (3.44x), Q80 (1.89x)
  Regression: none

---

## DECISION 7: Should I replace INTERSECT with EXISTS?

FAMILY: set_rewrite
TRANSFORMS: intersect_to_exists, multi_intersect_exists_cte
GAP: none (general INTERSECT materialization overhead)

STRUCTURAL GATE (from SQL):
  ✓ Query uses INTERSECT between two result sets
  → Proceed to size check

  IF query uses EXCEPT → similar logic applies (convert to NOT EXISTS)

SIZE CHECK:
  Look at: How large are the two sides of the INTERSECT?

  IF both sides < 1000 rows → STOP
     Materialization overhead is trivial, EXISTS won't help

  IF either side is large (10K+ rows from fact table joins)
     → EXISTS enables semi-join: stops at first match per row
     → Avoids materializing entire result set

INDEX CHECK:
  Look at: Would the EXISTS correlation be on indexed columns?

  IF correlation key is indexed → hash join or index lookup, fast
  IF correlation key is not indexed → nested loop with full scan per row → STOP

IDEAL PLAN SHAPE (when all gates pass):
  base_query → CTE (if referenced multiple times)
  SELECT ... FROM side_1
  WHERE EXISTS (SELECT 1 FROM side_2 WHERE side_2.key = side_1.key)

  Multiple INTERSECTs:
  WHERE EXISTS (...) AND EXISTS (...)

COMPOSITION NOTES:
- Pre-filter shared dimensions (e.g., date_dim) into a CTE to avoid repeated scans
- EXISTS enables semi-join optimization (stops at first match per row)
- Can chain multiple INTERSECT → AND EXISTS conditions

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Large INTERSECT, keys      │ 1.8-2.7x  │ —            │ INTERSECT keyword    │
  │ are indexed/join-friendly  │           │              │ + large result sets  │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Small INTERSECT (<1K rows) │ ~1.0x     │ —            │ Both sides small     │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe transform.

GOLD EXAMPLES FOR WORKERS:
  Win: Q14 (2.72x SF10), Q14 (2.39x multi_intersect variant)
  Regression: none

---

## DECISION 8: Should I split a self-joined CTE into per-partition CTEs?

FAMILY: self_join_decomposition
TRANSFORMS: self_join_decomposition, union_cte_split, rollup_to_union_windowing
GAP: UNION_CTE_SELF_JOIN_DECOMPOSITION + CROSS_CTE_PREDICATE_BLINDNESS

STRUCTURAL GATE (from SQL):
  ✓ A CTE (or subquery) is joined to ITSELF
  ✓ Each arm of the self-join applies a different discriminator filter
    (e.g., WHERE month = 1 on left, WHERE month = 2 on right)
  → Proceed to cardinality check

  IF CTE is referenced by other consumers besides the self-join → CAUTION
     Splitting may orphan those references

DISCRIMINATOR CARDINALITY CHECK:
  Look at: How many distinct values does the discriminator have?

  IF 2-4 values → good, create one CTE per partition
  IF 5+ values → diminishing returns, too many CTEs

EXPLAIN PLAN CHECK:
  Look at: Does the plan materialize the full CTE then filter per arm?

  IF yes → optimizer computes ALL months/years then post-filters
     → Each partition CTE aggregates only its own slice (e.g., 1/12th of data)
     (Q39: 4.76x from processing 1/12th)

  IF optimizer already pushes discriminator into CTE scan → less benefit

ORPHAN CTE GATE (critical):
  After splitting, does the original combined CTE still exist?

  IF yes → DOUBLE MATERIALIZATION — the original CTE is still computed
     → MUST remove original CTE and redirect all references
     (Q74: 0.68x — kept both original and split versions)
     (Q31: 0.49x — same orphan problem)

IDEAL PLAN SHAPE (when all gates pass):
  partition_1 → CTE (WHERE discriminator = val_1, GROUP BY key, AGG(...))
  partition_2 → CTE (WHERE discriminator = val_2, GROUP BY key, AGG(...))
  partition_1 JOIN partition_2 ON shared_key
  → comparison / computation across partitions

COMPOSITION NOTES:
- CRITICAL: remove the original combined CTE after splitting
- Redirect ALL references to appropriate partition CTE
- On PostgreSQL: CTE materialization is default → even more effective (PG-Q065: 3.93x)
- ROLLUP variant: when ROLLUP generates all grouping levels, explicit UNION
  of per-level aggregates with targeted GROUP BY can be faster (Q36: 2.47x)

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Self-join with 2-4         │ 1.4-4.8x  │ —            │ CTE joined to itself │
  │ discriminator values,      │           │              │ with different WHERE │
  │ original CTE removed       │           │              │ per arm              │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Original CTE kept (orphan) │ DON'T     │ 0.49x (Q31)  │ Original CTE still   │
  │                            │           │              │ in WITH clause after │
  │                            │           │              │ split                │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

GOLD EXAMPLES FOR WORKERS:
  Win: Q39 (4.76x), Q36 (2.47x), Q74 (1.57x), PG-Q065 (3.93x)
  Regression: Q74 (0.68x orphaned CTE), Q31 (0.49x duplicated CTEs)

---

## DECISION 9: Should I defer window functions until after joins?

FAMILY: window_deferral
TRANSFORMS: deferred_window_aggregation
GAP: none (general optimization principle)

STRUCTURAL GATE (from SQL):
  ✓ Window function (SUM OVER, ROW_NUMBER OVER) computed inside a CTE
  ✓ That CTE is then joined to another CTE or table
  ✓ Multiple CTEs each compute their own window before being joined
  → Proceed to semantic check

  IF only one CTE has a window function → less benefit (1 pass → 1 pass)

SEMANTIC CHECK:
  Look at: Can the window function be computed AFTER the join
  without changing semantics?

  IF window PARTITION BY aligns with join key → safe to defer
  IF window uses SUM() OVER (ORDER BY date) and join is FULL OUTER
     → SUM naturally skips NULLs from FULL OUTER → safe

  IF window uses LAG, LEAD → depends on row ordering, NOT safe after join
  IF window uses ROWS BETWEEN with specific frame → may change after join expansion

CONSUMER CHECK:
  IF the CTE's window result is referenced by another consumer (not the join)
     → cannot remove window from CTE

IDEAL PLAN SHAPE (when all gates pass):
  base_1 → GROUP BY key (daily totals only, NO window) → CTE
  base_2 → GROUP BY key (daily totals only, NO window) → CTE
  cte_1 FULL OUTER JOIN cte_2 ON date_key
  → SUM() OVER (ORDER BY date) on the joined result (one window pass)

COMPOSITION NOTES:
- Reduces N window passes to 1
- SUM() naturally skips NULLs from FULL OUTER JOIN — no COALESCE needed
- Only safe when join doesn't change the window's row ordering semantics

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ 2+ CTEs with windows       │ 1.3-1.4x  │ —            │ WINDOW inside CTE    │
  │ joined together            │           │              │ that feeds a JOIN    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ LAG/LEAD in CTE            │ DON'T     │ wrong results│ LAG/LEAD keyword     │
  │                            │           │              │ inside CTE           │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

  ZERO REGRESSIONS recorded. Safe transform.

GOLD EXAMPLES FOR WORKERS:
  Win: Q51 (1.36x)
  Regression: none

---

## DECISION 10: Should I materialize a shared subexpression into a CTE?

FAMILY: cte_materialization
TRANSFORMS: materialize_cte
GAP: none (shared computation principle)

STRUCTURAL GATE (from SQL):
  ✓ Same subquery or expression appears 2+ times in the query
  ✓ The repeated subquery is expensive (joins large tables, aggregates many rows)
  → Proceed to EXISTS check

  IF repeated expression is trivial (single table, few rows) → STOP, overhead wasted

EXISTS GATE (critical — most dangerous false positive):
  IF the repeated expression is an EXISTS or NOT EXISTS subquery → HARD STOP
     EXISTS uses semi-join with early termination.
     Materializing into a CTE forces full scan of the entire result set.
     (Q16: 0.14x — materialized EXISTS on entire fact table before filtering)
     (Q95: 0.54x — decomposed correlated EXISTS into independent CTEs)

  This is the single most dangerous regression pattern in the entire system.

FILTER PUSHDOWN CHECK:
  IF materializing would prevent the outer query from pushing filters down
     → the optimizer may currently push filters THROUGH the subquery inline
     → CTE creates a materialization barrier that blocks this
     → STOP unless the CTE itself has a WHERE clause

EXPLAIN PLAN CHECK:
  Look at: Is the subquery actually executed multiple times?

  IF plan shows shared scan (optimizer already CSE'd it) → no benefit
  IF plan shows the subquery executed N times → materializing saves N-1 executions

IDEAL PLAN SHAPE (when all gates pass):
  shared_expr → CTE (WITH shared AS (...))
  query_arm_1 references shared
  query_arm_2 references shared
  → shared computed once, both arms read from materialized result

COMPOSITION NOTES:
- NEVER materialize EXISTS/NOT EXISTS — this is the #1 regression cause
- On PostgreSQL: CTEs are materialized by default (use NOT MATERIALIZED to opt out)
- Every materialized CTE MUST have a WHERE clause — unfiltered = pure overhead
- PostgreSQL: AS MATERIALIZED forces materialization; AS NOT MATERIALIZED forces inlining

RISK CALIBRATION:
  ┌────────────────────────────┬───────────┬──────────────┬──────────────────────┐
  │ Scenario                   │ Expected  │ Worst seen   │ How to detect        │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Shared expensive join/agg  │ 1.3-1.4x  │ —            │ Same subquery text   │
  │ referenced 2+ times        │           │              │ appears 2+ times     │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ EXISTS/NOT EXISTS           │ DON'T     │ 0.14x (Q16)  │ EXISTS keyword in    │
  │ materialized               │ EVER      │              │ the repeated expr    │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ Correlated EXISTS split    │ DON'T     │ 0.54x (Q95)  │ Correlated EXISTS    │
  │ into independent CTEs      │           │              │ decomposed           │
  ├────────────────────────────┼───────────┼──────────────┼──────────────────────┤
  │ CTE without WHERE clause   │ DON'T     │ overhead     │ No WHERE in CTE def  │
  └────────────────────────────┴───────────┴──────────────┴──────────────────────┘

GOLD EXAMPLES FOR WORKERS:
  Win: Q95 (1.43x), PG-Q065 (3.93x)
  Regression: Q16 (0.14x EXISTS materialized), Q95 (0.54x cardinality severed)

---

## SAFETY RANKING

  ┌──────┬─────────────────────────┬─────────────┬────────────┬───────────────────────────┐
  │ Rank │ Family                  │ Regressions │ Worst case │ Recommendation            │
  ├──────┼─────────────────────────┼─────────────┼────────────┼───────────────────────────┤
  │ 1    │ scan_merge              │ 0           │ —          │ Always try                │
  │ 2    │ aggregate_pushdown      │ 0           │ —          │ Always try (verify keys)  │
  │ 3    │ join_strengthening      │ 0           │ —          │ Always try                │
  │ 4    │ set_rewrite             │ 0           │ —          │ Always try                │
  │ 5    │ window_deferral         │ 0           │ —          │ Always try                │
  │ 6    │ self_join_decomposition │ 1 indirect  │ 0.49x      │ Check orphan CTE          │
  │ 7    │ predicate_isolation     │ 4           │ 0.0076x    │ All gates must pass       │
  │ 8    │ correlation_break       │ 2           │ 0.34x      │ Check EXPLAIN first       │
  │ 9    │ cte_materialization     │ 3           │ 0.14x      │ Never on EXISTS           │
  │ 10   │ or_decomposition        │ 4           │ 0.23x      │ Max 3, cross-column only  │
  └──────┴─────────────────────────┴─────────────┴────────────┴───────────────────────────┘
