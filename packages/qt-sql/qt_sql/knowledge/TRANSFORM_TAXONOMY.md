# Transform Taxonomy

Authoritative catalog of every SQL rewrite transform in the QueryTorque system.
Each transform has a unique ID, a family classification, engine applicability,
empirical stats from validated benchmarks, query pair references showing the
pattern in action, and known failure modes with root cause analysis.

Last updated: 2026-02-14

---

## Family Classification

Every transform belongs to one of 7 families. Families group transforms by
the optimizer blindspot they exploit, not by the syntactic change they make.

| Family | Code | What it attacks | Principle |
|--------|------|-----------------|-----------|
| **Scan Reduction** | `SR` | Redundant or over-broad table scans | Filter early, scan less. Pre-filter dimension tables into CTEs so fact table joins probe tiny hash tables. |
| **Subquery Elimination** | `SE` | Correlated subqueries, per-row re-execution | Sets over loops. Convert correlated subqueries to CTEs with GROUP BY + JOIN. |
| **Set Operation** | `SO` | INTERSECT/EXCEPT materializing both sides | Semi-join short-circuit. Replace full materialization with EXISTS or targeted CTE. |
| **Scan Consolidation** | `SC` | Multiple scans of the same table | Don't repeat work. Merge N separate scans into 1 pass using CASE expressions or shared CTEs. |
| **Predicate Restructure** | `PR` | OR conditions, join reordering | Arm the optimizer. Restructure predicates so the optimizer can use different access paths. |
| **Aggregation Rewrite** | `AR` | Aggregation after large joins, deferred windows | Minimize rows touched. Push aggregation below joins or defer windows to after filtering. |
| **Join Restructure** | `JR` | Comma joins, join order, LEFT->INNER | Arm the optimizer. Give the planner explicit join structure and better cardinality estimates. |

---

## Transform Catalog

### Family: Scan Reduction (SR)

These transforms exploit CROSS_CTE_PREDICATE_BLINDNESS: the optimizer cannot
push predicates backward from outer query into CTE definitions. CTEs are
planned as independent subplans with no data lineage tracing through boundaries.
~35% of all DuckDB wins exploit this gap.

---

#### `date_cte_isolate`

Extract date dimension lookups into a CTE so they materialize once. Subsequent
fact table joins probe a tiny hash table (~365 rows for a year, ~30 for a
month) instead of scanning 73K date_dim rows. The most frequently successful
transform in the system.

**Why it works**: Date dimension is always tiny after filtering (30-365 rows
out of 73K). By materializing the filter result first, the subsequent fact
table hash join probes against a minuscule build side. The optimizer cannot
do this itself because it treats CTE boundaries as optimization fences.

**Pattern** (from gold example, TPC-DS Q6, 4.00x on DuckDB):
```sql
-- BEFORE: date filter buried in main query, full 73K date_dim scanned per join
SELECT ...
FROM customer_address ca, customer c, store_sales s, date_dim d, item i
WHERE d.d_month_seq = 1205 AND s.ss_sold_date_sk = d.d_date_sk ...

-- AFTER: date filter extracted, only matching date keys enter fact join
WITH target_month AS (
    SELECT d_date_sk FROM date_dim WHERE d_month_seq = 1205
),
category_avg AS (
    SELECT i_category, AVG(i_current_price) * 1.2 AS threshold
    FROM item GROUP BY i_category
)
SELECT ...
FROM customer_address ca
JOIN customer c ON ca.ca_address_sk = c.c_current_addr_sk
JOIN store_sales s ON c.c_customer_sk = s.ss_customer_sk
JOIN target_month tm ON s.ss_sold_date_sk = tm.d_date_sk  -- probes ~30 keys
JOIN item i ON s.ss_item_sk = i.i_item_sk
JOIN category_avg ca2 ON i.i_category = ca2.i_category
WHERE i.i_current_price > ca2.threshold
```

- **Engines**: DuckDB, PostgreSQL, Snowflake
- **DuckDB stats**: 93 verified speedups across 53 queries, 1.86x avg, best 9.50x
- **PG stats**: 3.10x (date_consolidation variant, DSB). Must combine with explicit JOINs.
- **Snowflake stats**: 13/15 wins on timeout queries. Enables RUNTIME partition pruning (static EXPLAIN still shows all partitions — this is expected).
- **Regressions**: 0.50x (3-way fact join locks optimizer order), 0.85x (CTE blocks ROLLUP pushdown)
- **Gates**: Baseline > 100ms. Max 2 cascading CTE chains. Every CTE MUST have WHERE clause. Skip if query has ROLLUP/WINDOW immediately downstream.
- **AST detection**: New CTE referencing date_dim with date filter columns (d_year, d_moy, d_month_seq)
- **Gold examples**: `duckdb/date_cte_isolate.json` (Q6, 4.00x), `postgres/date_consolidation.json` (3.10x)
- **Pathology**: DuckDB P0, PG P6, Snowflake P1

---

#### `dimension_cte_isolate`

Pre-filter non-date dimension tables (item, customer, store, customer_demographics,
household_demographics, promotion) into CTEs returning only surrogate keys
before joining with fact tables.

**Why it works**: Each dimension CTE reduces to a tiny hash table. When the
fact table join probes this hash table, only matching rows survive. The compound
effect of multiple dimension CTEs is multiplicative — if item CTE keeps 5% and
store CTE keeps 10%, the fact scan effectively touches 0.5% of rows.

**Pattern** (from gold example, TPC-DS Q26, 1.93x on DuckDB):
```sql
-- BEFORE: dimension filters applied AFTER full fact join
SELECT i_item_id, AVG(ss_quantity), AVG(ss_list_price), ...
FROM store_sales, customer_demographics, date_dim, item, promotion
WHERE ss_cdemo_sk = cd_demo_sk AND ...
  AND cd_gender = 'F' AND cd_education_status = 'Unknown'
  AND i_category IN ('Books')
  AND p_channel_email = 'N'

-- AFTER: each dimension pre-filtered, fact join probes tiny tables
WITH filtered_dates AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2000
),
filtered_demographics AS (
    SELECT cd_demo_sk FROM customer_demographics
    WHERE cd_gender = 'F' AND cd_education_status = 'Unknown'
),
filtered_items AS (
    SELECT i_item_sk, i_item_id FROM item WHERE i_category IN ('Books')
),
filtered_promos AS (
    SELECT p_promo_sk FROM promotion WHERE p_channel_email = 'N'
)
SELECT fi.i_item_id, AVG(ss_quantity), ...
FROM store_sales ss
JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_demographics fc ON ss.ss_cdemo_sk = fc.cd_demo_sk
JOIN filtered_items fi ON ss.ss_item_sk = fi.i_item_sk
JOIN filtered_promos fp ON ss.ss_promo_sk = fp.p_promo_sk
```

- **Engines**: DuckDB
- **DuckDB stats**: 7 verified speedups, 2.05x avg, best 6.24x
- **Regressions**: **0.0076x CATASTROPHIC** (cross-joined 3 dim CTEs without fact table — Cartesian product explosion), 0.85x (unfiltered CTE with no WHERE clause adds pure overhead)
- **Gates**: NEVER cross-join 3+ dimension CTEs directly — each must join through the fact table. Every CTE MUST have a selective WHERE clause. If dimension table has < 1K rows total, CTE overhead exceeds benefit.
- **Regression mechanism (0.0076x)**: Three dimension CTEs were joined to each other instead of through the fact table. 5K x 200 x 300 = 300M row Cartesian product. The fix is always `fact JOIN dim1 ON ... JOIN dim2 ON ...`, never `dim1 CROSS JOIN dim2`.
- **AST detection**: New CTE referencing dimension table (not date_dim, not fact) with WHERE clause
- **Gold example**: `duckdb/dimension_cte_isolate.json` (Q26, 1.93x)
- **Pathology**: DuckDB P0

---

#### `multi_dimension_prefetch`

When multiple dimension tables have selective filters, pre-filter ALL into CTEs
before the fact join. Combined selectivity compounds — each dimension CTE
further reduces the fact scan.

**Why it works**: Star-schema queries typically filter on 2-3 dimensions (date +
store + item). By pre-filtering all of them into CTEs before touching the fact
table, the hash join sees tiny probe sets from every dimension. This is the
compound version of `dimension_cte_isolate`.

**Pattern** (from gold example, TPC-DS Q43, 2.71x on DuckDB):
```sql
-- AFTER: pre-filter date (30 rows) + store (200 rows), then fact join
WITH filtered_dates AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2000
),
filtered_stores AS (
    SELECT s_store_sk, s_store_name FROM store WHERE s_gmt_offset = -5
)
SELECT fs.s_store_name, SUM(ss_net_profit)
FROM store_sales ss
JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
JOIN filtered_stores fs ON ss.ss_store_sk = fs.s_store_sk
GROUP BY fs.s_store_name
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: 3 wins, 1.55x avg, best 2.71x
- **PG stats**: 2.5x avg, best 3.32x (pg_dimension_prefetch_star variant). On PG, MUST also convert comma joins to explicit JOINs.
- **Regressions**: 0.77x (5+ tables forced suboptimal join order — optimizer lost reordering freedom), 0.85x (unfiltered dimension CTEs add overhead without reducing cardinality)
- **Gates**: Max 4 dimension CTEs. Single fact table only. Each dimension CTE must filter to < 10% of its table. STOP if self-join detected (0.25x observed on PG).
- **AST detection**: 2+ new CTEs referencing dimension tables
- **Gold examples**: `duckdb/multi_dimension_prefetch.json` (Q43, 2.71x), `postgres/dimension_prefetch_star.json` (3.32x)
- **Pathology**: DuckDB P0, PG P7

---

#### `multi_date_range_cte`

When query joins the same date_dim table multiple times with different filters
(d1 for d_year=2001, d2 for d_year=2002, d3 for d_year=2003), create separate
CTEs for each date range and pre-join each with the fact table.

**Why it works**: Each date alias scans 73K rows separately. By extracting each
into a CTE (~365 rows per year), the 3 separate full scans become 3 tiny lookups.
The pre-join with fact table further reduces rows entering the main query.

**Pattern** (from gold example, TPC-DS Q29, 2.35x on DuckDB):
```sql
-- AFTER: separate date CTEs for different date ranges, pre-joined to facts
WITH d1_filtered AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 9 AND 9+3
),
d2_filtered AS (
    SELECT d_date_sk FROM date_dim WHERE d_moy BETWEEN 9 AND 9+3  -- same range, different join
),
filtered_store_sales AS (
    SELECT ss_customer_sk, ss_item_sk, ss_ticket_number, ss_quantity
    FROM store_sales JOIN d1_filtered ON ss_sold_date_sk = d_date_sk
),
filtered_store_returns AS (
    SELECT sr_customer_sk, sr_item_sk, sr_ticket_number, sr_return_quantity
    FROM store_returns JOIN d2_filtered ON sr_returned_date_sk = d_date_sk
)
SELECT ... FROM filtered_store_sales
JOIN filtered_store_returns ON ...
```

- **Engines**: DuckDB
- **DuckDB stats**: 3 wins, 1.42x avg, best 2.35x
- **Regressions**: None documented. Zero regression risk.
- **AST detection**: 2+ new date CTEs with different date filters
- **Gold example**: `duckdb/multi_date_range_cte.json` (Q29, 2.35x)
- **Pathology**: DuckDB P0

---

#### `prefetch_fact_join`

Staged join pipeline: first CTE filters dimension, second CTE pre-joins the
filtered dimension with the fact table to materialize a small intermediate
result, subsequent CTEs join remaining dimensions against the reduced dataset.

**Why it works**: Instead of letting the optimizer decide join order with all
tables at once (which it often gets wrong on star schemas), this forces a
staged reduction. The pre-joined fact+dimension CTE is dramatically smaller
than the raw fact table, so all subsequent joins are fast.

**Pattern** (from gold example, TPC-DS Q63, 3.77x on DuckDB):
```sql
-- AFTER: staged pipeline — filter date, pre-join to fact, then remaining dims
WITH filtered_dates AS (
    SELECT d_date_sk, d_month_seq
    FROM date_dim WHERE d_month_seq BETWEEN 1212 AND 1212+11
),
prefetched_sales AS (
    SELECT ss.ss_item_sk, ss.ss_store_sk, ss.ss_ext_sales_price
    FROM store_sales ss
    JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
    -- This CTE is MUCH smaller than raw store_sales
)
SELECT i.i_brand, AVG(ps.ss_ext_sales_price)
FROM prefetched_sales ps
JOIN item i ON ps.ss_item_sk = i.i_item_sk
JOIN store s ON ps.ss_store_sk = s.s_store_sk
WHERE i.i_category IN ('Books') AND i.i_class IN ('fiction')
GROUP BY i.i_brand
```

- **Engines**: DuckDB
- **DuckDB stats**: 18 verified speedups, 1.82x avg, best 5.23x across 18 queries
- **Regressions**: 0.78x (3rd cascading CTE chain — too many stages blocks parallelism), 0.50x (fast baseline < 50ms — CTE overhead dominates)
- **Gates**: Max 2 cascading CTE chains (dim→fact→main). Baseline > 50ms. Single fact table only.
- **Regression mechanism (0.78x)**: Three cascading CTEs (dim→fact→intermediate→final) created a serial dependency chain that prevented DuckDB's parallel execution. Two stages is the sweet spot.
- **AST detection**: CTE that joins fact table with pre-filtered dimension (has_fact + has_dim/date_dim)
- **Gold example**: `duckdb/prefetch_fact_join.json` (Q63, 3.77x)
- **Pathology**: DuckDB P0

---

#### `early_filter`

Push selective dimension filters to execute before expensive fact table joins.
Extract dimension filters into CTEs so they reduce cardinality before the fact
table is scanned.

**Why it works**: Most star-schema queries filter dimensions in the WHERE clause,
but the optimizer may scan the full fact table first and filter late. By
materializing the dimension filter into a CTE, we guarantee the fact table join
only touches rows matching the dimension predicate.

**Pattern** (from gold example, TPC-DS Q93, 4.00x on DuckDB):
```sql
-- BEFORE: all tables joined, then filtered
SELECT ss_customer_sk, SUM(act_sales)
FROM store_sales, store_returns, reason
WHERE sr_reason_sk = r_reason_sk AND ss_item_sk = sr_item_sk ...
  AND r_reason_desc = 'reason 66'

-- AFTER: filter reason first (tiny table), then returns, then fact
WITH filtered_reason AS (
    SELECT r_reason_sk FROM reason WHERE r_reason_desc = 'reason 66'  -- ~1 row
),
filtered_returns AS (
    SELECT sr_item_sk, sr_ticket_number, ...
    FROM store_returns sr
    JOIN filtered_reason fr ON sr.sr_reason_sk = fr.r_reason_sk  -- massive reduction
)
SELECT ss_customer_sk, SUM(act_sales)
FROM store_sales ss
JOIN filtered_returns fr ON ss.ss_item_sk = fr.sr_item_sk ...
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: Broad contributor across many wins. Best 4.00x (Q93).
- **PG stats**: 27.80x when combined with decorrelate (early_filter_decorrelate, DSB Q001). On PG, early filtering is especially critical because comma joins prevent the optimizer from reordering tables.
- **Regressions**: None documented standalone. When combined with other transforms, can contribute to overhead on fast baselines (< 200ms).
- **AST detection**: New CTEs with WHERE clauses; existing CTEs gain new WHERE conditions
- **Gold examples**: `duckdb/early_filter.json` (Q93, 4.00x), `postgres/early_filter_decorrelate.json` (Q001, 27.80x)
- **Pathology**: DuckDB P0, PG P1

---

### Family: Subquery Elimination (SE)

These transforms exploit CORRELATED_SUBQUERY_PARALYSIS: the optimizer cannot
decorrelate complex correlated aggregate subqueries into GROUP BY + JOIN.
On PostgreSQL, this is the HIGHEST IMPACT family — responsible for 9 of 31
total wins including 8044x, 1465x, and 439x timeout rescues.

---

#### `decorrelate`

Convert correlated subqueries to standalone CTEs with GROUP BY on the
correlation key, then JOIN back. Replaces per-row re-execution (O(N*M)
nested loop) with a single pre-computed hash join (O(N+M)).

**Why it works**: A correlated subquery like `WHERE x > (SELECT AVG(y) FROM t
WHERE t.key = outer.key)` re-executes the inner SELECT for every row of the
outer query. By computing the aggregate once with GROUP BY and joining, the
work is done once. This is the single most impactful transform on PostgreSQL
because PG's decorrelation engine cannot handle complex aggregate correlations.

**Pattern** (from gold example, TPC-DS Q1, 2.92x on DuckDB):
```sql
-- BEFORE: correlated subquery re-executes per customer
SELECT c_customer_id
FROM customer c, store_returns sr, date_dim d
WHERE sr.sr_returned_date_sk = d.d_date_sk AND ...
  AND sr.ctr_total_return >
      (SELECT AVG(sr2.ctr_total_return) * 1.2
       FROM store_returns sr2
       WHERE sr2.ctr_store_sk = sr.ctr_store_sk)  -- re-runs per outer row!

-- AFTER: threshold computed once, joined back
WITH store_thresholds AS (
    SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS threshold
    FROM store_returns
    GROUP BY ctr_store_sk  -- one row per store
)
SELECT c.c_customer_id
FROM customer c
JOIN store_returns sr ON ...
JOIN store_thresholds st ON sr.ctr_store_sk = st.ctr_store_sk
WHERE sr.ctr_total_return > st.threshold  -- simple comparison, no re-execution
```

**PG variant** (from gold example, DSB Q092, 8044x timeout rescue):
```sql
-- BEFORE: correlated subquery times out (>300s)
WHERE ss_ext_sales_price >
    (SELECT AVG(ss2.ss_ext_sales_price) * 1.2
     FROM store_sales ss2
     WHERE ss2.ss_item_sk = ss.ss_item_sk)  -- re-scans 28B rows PER ITEM

-- AFTER: shared scan CTE + threshold CTE (completes in <1s)
WITH common_scan AS MATERIALIZED (  -- AS MATERIALIZED prevents PG re-inlining!
    SELECT ss_item_sk, ss_ext_sales_price
    FROM store_sales JOIN date_dim ON ...
),
item_thresholds AS MATERIALIZED (
    SELECT ss_item_sk, AVG(ss_ext_sales_price) * 1.2 AS threshold
    FROM common_scan GROUP BY ss_item_sk
)
SELECT ... FROM common_scan cs
JOIN item_thresholds it ON cs.ss_item_sk = it.ss_item_sk
WHERE cs.ss_ext_sales_price > it.threshold
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: 38 verified speedups, 1.52x avg, best 4.38x across 32 queries
- **PG stats**: 8 wins at 3.2x avg for standard cases. Timeout rescues: 8044x (Q092, shared_scan variant), 1465x (Q032, inline variant), 439x (Q081, state_avg variant)
- **Regressions**: DuckDB 0.34x (LEFT JOIN was already semi-join — decorrelation destroyed it), 0.71x (over-decomposed: pre-aggregated ALL stores when only subset needed). PG 0.51x (multi-fact join lock), 0.75x (EXISTS materialized)
- **Gates**: Preserve ALL WHERE filters from original subquery (0.34x regression from missing filters). Check EXPLAIN for existing hash join on correlation key — if present, already decorrelated, STOP. NEVER materialize EXISTS/NOT EXISTS (0.14x-0.75x from destroying semi-join short-circuit). On PG: ALWAYS use AS MATERIALIZED to prevent optimizer from re-inlining CTE back into correlated form.
- **Regression mechanism (0.34x on DuckDB)**: The LEFT JOIN already had an efficient semi-join plan. Decorrelating it into a GROUP BY + JOIN removed the semi-join optimization and forced full materialization of all groups, even those not needed.
- **AST detection**: Subquery count drops while CTE/JOIN count increases
- **Gold examples**: `duckdb/decorrelate.json` (Q1, 2.92x), `postgres/early_filter_decorrelate.json` (Q001, 27.80x), `postgres/shared_scan_decorrelate.json` (Q092, 8044x), `postgres/state_avg_decorrelate.json` (Q081, 439x), `postgres/inline_decorrelate_materialized.json` (Q032, 1465x)
- **Pathology**: DuckDB P2, PG P2

---

#### `inline_decorrelate_materialized` (PG only)

Inline decorrelation with AS MATERIALIZED CTEs. When WHERE contains correlated
scalar subquery with aggregate, decompose into 3 MATERIALIZED CTEs:
(1) dimension prefilter, (2) date-filtered fact scan, (3) per-key threshold
via GROUP BY + JOIN.

**Why it works**: PostgreSQL will re-inline non-MATERIALIZED CTEs back into
correlated form, undoing the decorrelation. The AS MATERIALIZED keyword is
mandatory on PG 12+ to force the CTE to evaluate once. Without it, PG's
optimizer "helpfully" pushes the CTE back into each reference site, recreating
the correlated subquery.

**Critical insight**: This is the PG-specific version of decorrelate. On PG,
all decorrelation CTEs MUST use AS MATERIALIZED. On DuckDB, CTEs are inlined
by default but the planner still benefits from the structural hint.

- **Engines**: PostgreSQL (PG 12+ for AS MATERIALIZED support)
- **PG stats**: 1465x (DSB Q032, timeout rescue), 8044x (shared_scan variant, Q092), 439x (state_avg variant, Q081)
- **Regressions**: None documented — all applications were timeout rescues where any improvement is a win
- **Gates**: MUST use AS MATERIALIZED keyword. Only for truly correlated subqueries with aggregates. Verify correlation is on a column with many distinct values (low cardinality correlation = GROUP BY produces few rows = minimal benefit).
- **Gold examples**: `postgres/inline_decorrelate_materialized.json` (Q032, 1465x), `postgres/shared_scan_decorrelate.json` (Q092, 8044x), `postgres/state_avg_decorrelate.json` (Q081, 439x)
- **Pathology**: PG P2

---

### Family: Set Operation (SO)

These transforms exploit INTERSECT_MATERIALIZATION: the optimizer implements
INTERSECT/EXCEPT as full materialization + sort + comparison, rather than
recognizing the algebraic equivalence to EXISTS semi-join which can short-circuit.

---

#### `intersect_to_exists`

Replace INTERSECT with EXISTS to avoid full materialization and sorting of both
sides. EXISTS stops at first match per row, enabling semi-join optimization.

**Why it works**: INTERSECT materializes both result sets, sorts them, and
compares row by row. EXISTS checks one row at a time and stops at the first
match — on average examining only ~50% of the inner set. With index support,
EXISTS can be O(N log M) vs INTERSECT's O(N log N + M log M).

**Pattern** (from gold example, TPC-DS Q14, 1.83x on DuckDB):
```sql
-- BEFORE: INTERSECT materializes and sorts both sides
SELECT i_brand_id, i_class_id FROM store_sales, item, date_dim WHERE ...
INTERSECT
SELECT i_brand_id, i_class_id FROM catalog_sales, item, date_dim WHERE ...
INTERSECT
SELECT i_brand_id, i_class_id FROM web_sales, item, date_dim WHERE ...

-- AFTER: first query is base, others become EXISTS checks
WITH store_brands AS (
    SELECT DISTINCT i_brand_id, i_class_id
    FROM store_sales JOIN item ON ... JOIN date_dim ON ...
)
SELECT sb.i_brand_id, sb.i_class_id
FROM store_brands sb
WHERE EXISTS (
    SELECT 1 FROM catalog_sales cs JOIN item i ON ...
    WHERE i.i_brand_id = sb.i_brand_id AND i.i_class_id = sb.i_class_id
)
AND EXISTS (
    SELECT 1 FROM web_sales ws JOIN item i ON ...
    WHERE i.i_brand_id = sb.i_brand_id AND i.i_class_id = sb.i_class_id
)
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: 1 win at 1.83x (Q14), multi-intersect variant: 2.7x. Zero regressions.
- **PG stats**: 1.78x. Zero regressions.
- **Regressions**: None documented for the basic transform. Blackboard shows 5 queries with regressions (0.37x-0.91x) when over-applied, but these were compound rewrites, not pure intersect_to_exists.
- **Gates**: Both INTERSECT sides should produce > 1K rows. Smaller sets don't benefit because materialization is cheap. Correlation columns should have indexes or hash join support.
- **AST detection**: INTERSECT count drops while EXISTS count increases
- **Gold examples**: `duckdb/intersect_to_exists.json` (Q14, 1.83x), `duckdb/multi_intersect_exists_cte.json` (2.7x), `postgres/intersect_to_exists.json` (1.78x)
- **Pathology**: DuckDB P6, PG P5

---

#### `set_operation_materialization` (PG only)

Convert large INTERSECT/EXCEPT operations on 3+ correlated channels
(store/catalog/web) to pre-materialized channel CTEs with EXISTS/NOT EXISTS
checks. Avoids full result set materialization for multi-channel set operations.

**Why it works**: When EXISTS/NOT EXISTS is applied across 3+ channels, PG
re-evaluates the correlated subquery for each outer row × each channel. By
pre-materializing DISTINCT customer sets per channel into MATERIALIZED CTEs,
each check becomes a hash lookup instead of a correlated re-evaluation.

**Pattern** (from gold example, DSB, 17.48x on PG):
```sql
-- BEFORE: 3 correlated EXISTS/NOT EXISTS per customer row
WHERE EXISTS (SELECT 1 FROM store_sales WHERE ss_customer_sk = c.c_customer_sk ...)
  AND EXISTS (SELECT 1 FROM web_sales WHERE ws_bill_customer_sk = c.c_customer_sk ...)
  AND NOT EXISTS (SELECT 1 FROM catalog_sales WHERE cs_bill_customer_sk = c.c_customer_sk ...)

-- AFTER: pre-materialized channel sets, simple JOIN/anti-JOIN
WITH store_customers AS MATERIALIZED (
    SELECT DISTINCT ss_customer_sk FROM store_sales JOIN date_dim ON ...
),
web_customers AS MATERIALIZED (
    SELECT DISTINCT ws_bill_customer_sk FROM web_sales JOIN date_dim ON ...
),
catalog_customers AS MATERIALIZED (
    SELECT DISTINCT cs_bill_customer_sk FROM catalog_sales JOIN date_dim ON ...
)
SELECT c.*
FROM customer c
INNER JOIN store_customers sc ON c.c_customer_sk = sc.ss_customer_sk
INNER JOIN web_customers wc ON c.c_customer_sk = wc.ws_bill_customer_sk
LEFT JOIN catalog_customers cc ON c.c_customer_sk = cc.cs_bill_customer_sk
WHERE cc.cs_bill_customer_sk IS NULL  -- NOT EXISTS via anti-join
```

- **Engines**: PostgreSQL
- **PG stats**: 17.48x
- **Regressions**: 0.75x (over-materialized date CTE in EXISTS path when set operation was already efficient)
- **Gold example**: `postgres/set_operation_materialization.json` (17.48x)
- **Pathology**: PG P5

---

### Family: Scan Consolidation (SC)

These transforms exploit REDUNDANT_SCAN_ELIMINATION: the optimizer cannot
detect when the same fact table is scanned N times with similar filters across
subquery boundaries. Common Subexpression Elimination does not cross scalar
subquery boundaries. ~37% of DuckDB benchmark wins exploit this gap. This
family has ZERO REGRESSIONS on DuckDB — the safest family of transforms.

---

#### `single_pass_aggregation`

Consolidate multiple scalar subqueries scanning the same table into a single
CTE using CASE expressions inside aggregate functions. Reduces N separate
table scans to 1 pass.

**Why it works**: When a query has 5 subqueries each scanning store_sales with
different WHERE filters, the optimizer runs 5 separate full table scans.
By combining them into one scan with CASE WHEN routing, we do 1 scan with
conditional aggregation. DuckDB's native FILTER clause makes this even more
efficient: `COUNT(*) FILTER (WHERE condition)`.

**Pattern** (from gold example, TPC-DS Q9, 4.47x on DuckDB):
```sql
-- BEFORE: 15 separate subqueries (5 quantity ranges x 3 aggregates each)
SELECT
  (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) cnt1,
  (SELECT AVG(ss_ext_discount_amt) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) avg1,
  (SELECT AVG(ss_net_profit) FROM store_sales WHERE ss_quantity BETWEEN 1 AND 20) prof1,
  (SELECT COUNT(*) FROM store_sales WHERE ss_quantity BETWEEN 21 AND 40) cnt2,
  ... -- 15 separate scans total

-- AFTER: single scan with CASE-routed aggregation
WITH quantity_stats AS (
    SELECT
        COUNT(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN 1 END) AS cnt1,
        AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_ext_discount_amt END) AS avg1,
        AVG(CASE WHEN ss_quantity BETWEEN 1 AND 20 THEN ss_net_profit END) AS prof1,
        COUNT(CASE WHEN ss_quantity BETWEEN 21 AND 40 THEN 1 END) AS cnt2,
        ...  -- all 15 aggregates in 1 pass
    FROM store_sales
)
SELECT * FROM quantity_stats, reason  -- single scan!
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: 4 wins, 2.72x avg, best 5.85x (Q88). **ZERO regressions.**
- **PG stats**: 1.98x. Zero regressions.
- **Regressions**: None documented. This is the SAFEST transform in the system. The blackboard shows 6 queries with regressions (0.09x-0.94x), but analysis shows these were compound rewrites where single_pass_aggregation was combined with other harmful transforms — the consolidation itself was not the cause.
- **Gates**: All subqueries must scan the same table with the same joins. Only COUNT, SUM, AVG, MIN, MAX (not STDDEV, PERCENTILE — these require cross-row state). Max ~8 buckets (beyond 8, CASE evaluation overhead exceeds scan savings).
- **AST detection**: CASE inside COUNT/SUM/AVG increases; original had multiple scalar subqueries on same table
- **Gold examples**: `duckdb/single_pass_aggregation.json` (Q9, 4.47x), `postgres/single_pass_aggregation.json` (1.98x)
- **Pathology**: DuckDB P1, PG P3

---

#### `channel_bitmap_aggregation`

Variant of single_pass_aggregation for channel-pattern queries. When query
computes separate aggregates for different "channels" (web_sales, catalog_sales,
store_sales) or time buckets, merge into one pass with CASE-tagged aggregation.

**Why it works**: Channel queries (TPC-DS Q88 pattern) scan the same fact table
8 times — once per time bucket. Each scan touches the full table. Consolidating
into 1 scan with 8 CASE WHEN conditions reduces I/O by 8x.

**Pattern** (from gold example, TPC-DS Q88, 6.24x on DuckDB):
```sql
-- BEFORE: 8 separate subqueries, one per time bucket
(SELECT COUNT(*) FROM store_sales, time_dim WHERE t_hour=8 AND t_minute>=30) h8_30,
(SELECT COUNT(*) FROM store_sales, time_dim WHERE t_hour=9 AND t_minute<30) h9_00,
(SELECT COUNT(*) FROM store_sales, time_dim WHERE t_hour=9 AND t_minute>=30) h9_30,
... -- 8x table scans

-- AFTER: single consolidated scan with bitmap routing
WITH time_bucket_counts AS (
    SELECT
        COUNT(CASE WHEN t.t_hour = 8 AND t.t_minute >= 30 THEN 1 END) AS h8_30,
        COUNT(CASE WHEN t.t_hour = 9 AND t.t_minute < 30 THEN 1 END) AS h9_00,
        COUNT(CASE WHEN t.t_hour = 9 AND t.t_minute >= 30 THEN 1 END) AS h9_30,
        ...  -- all 8 buckets, 1 scan
    FROM store_sales ss
    JOIN household_demographics hd ON ss.ss_hdemo_sk = hd.hd_demo_sk
    JOIN time_dim t ON ss.ss_sold_time_sk = t.t_time_sk
    WHERE hd.hd_dep_count = 5
)
```

- **Engines**: DuckDB
- **DuckDB stats**: 1 win, 6.24x (Q88). Zero regressions.
- **Regressions**: None documented
- **Gate**: Different joins per subquery = STOP (can't consolidate if join structure differs)
- **Gold example**: `duckdb/channel_bitmap_aggregation.json` (Q88, 6.24x)
- **Pathology**: DuckDB P1

---

#### `self_join_decomposition`

When a CTE is joined to itself with different WHERE clauses (self-join with
discriminator filters), split into specialized CTEs so each processes only
its subset. Eliminates redundant computation of rows that are immediately
filtered out.

**Why it works**: A self-join on CTE `yearly_agg` with `a.d_moy=1` on one side
and `b.d_moy=2` on the other materializes ALL months into `yearly_agg`, then
discards most rows at the join. By splitting into `month1_agg` and `month2_agg`,
each CTE only processes its month's data.

**Pattern** (from gold example, TPC-DS Q39, 4.76x on DuckDB):
```sql
-- BEFORE: single CTE materialized with ALL months, self-joined
WITH inv AS (
    SELECT w_warehouse_name, i_item_id, d_moy,
           STDDEV(inv_quantity_on_hand) AS stdev, AVG(inv_quantity_on_hand) AS mean
    FROM inventory, warehouse, item, date_dim
    WHERE ... AND d_year = 2001
    GROUP BY w_warehouse_name, i_item_id, d_moy
)
SELECT inv1.w_warehouse_name, inv1.i_item_id
FROM inv inv1 JOIN inv inv2
  ON inv1.i_item_id = inv2.i_item_id AND inv1.w_warehouse_name = inv2.w_warehouse_name
WHERE inv1.d_moy = 1 AND inv2.d_moy = 2  -- 90% of CTE rows wasted!

-- AFTER: split by discriminator, each CTE only computes its month
WITH month1_stats AS (
    SELECT w_warehouse_name, i_item_id, STDDEV(...) AS stdev, AVG(...) AS mean
    FROM inventory, warehouse, item, date_dim
    WHERE ... AND d_year = 2001 AND d_moy = 1  -- only month 1
    GROUP BY w_warehouse_name, i_item_id
),
month2_stats AS (
    SELECT w_warehouse_name, i_item_id, STDDEV(...) AS stdev, AVG(...) AS mean
    FROM inventory, warehouse, item, date_dim
    WHERE ... AND d_year = 2001 AND d_moy = 2  -- only month 2
    GROUP BY w_warehouse_name, i_item_id
)
SELECT m1.w_warehouse_name, m1.i_item_id
FROM month1_stats m1 JOIN month2_stats m2
  ON m1.i_item_id = m2.i_item_id AND m1.w_warehouse_name = m2.w_warehouse_name
```

- **Engines**: DuckDB, PostgreSQL
- **DuckDB stats**: 1 win, 4.76x (Q39)
- **PG stats**: 3.93x (pg_self_join_decomposition)
- **Regressions**: 0.49x (orphaned original CTE — double materialization), 0.68x (original CTE kept alongside split variant)
- **Gates**: 2-4 discriminator values only. MUST delete the original generic CTE after splitting. Redirect ALL references to the split CTEs.
- **Regression mechanism (0.49x)**: The split CTEs were created but the original combined CTE was left in the query. Both the original AND the splits were materialized, doubling the work. The fix is to DELETE the original CTE and update all references.
- **AST detection**: CTE joined to itself detected via table name repetition
- **Gold examples**: `duckdb/self_join_decomposition.json` (Q39, 4.76x), `postgres/self_join_decomposition.json` (3.93x)
- **Pathology**: DuckDB P7, PG P3

---

#### `union_cte_split`

When a generic CTE is scanned multiple times with different filters (e.g., by
year), split into specialized CTEs embedding the filter in the definition.
Each specialized CTE processes only the rows it needs.

**Why it works**: Same principle as self_join_decomposition but for UNION-based
CTEs. The generic CTE materializes ALL years, then each reference filters to
one year. By splitting, each specialized CTE only computes its year.

**Pattern** (from gold example, TPC-DS Q74, 1.36x on DuckDB):
```sql
-- BEFORE: generic CTE scanned 2x with different year filters
WITH yearly_totals AS (
    SELECT c_customer_id, c_first_name, d_year, SUM(ss_net_paid) AS year_total
    FROM customer, store_sales, date_dim WHERE ... GROUP BY ...
)
SELECT t1.*, t2.*
FROM yearly_totals t1 JOIN yearly_totals t2 ON ...
WHERE t1.d_year = 2001 AND t2.d_year = 2002  -- CTE materializes ALL years

-- AFTER: split by year, each CTE only computes its year
WITH totals_2001 AS (
    SELECT c_customer_id, c_first_name, SUM(ss_net_paid) AS year_total
    FROM customer, store_sales, date_dim
    WHERE ... AND d_year = 2001  -- embedded filter
    GROUP BY ...
),
totals_2002 AS (
    SELECT c_customer_id, c_first_name, SUM(ss_net_paid) AS year_total
    FROM customer, store_sales, date_dim
    WHERE ... AND d_year = 2002  -- embedded filter
    GROUP BY ...
)
SELECT t1.*, t2.* FROM totals_2001 t1 JOIN totals_2002 t2 ON ...
-- No more original CTE — DELETED
```

- **Engines**: DuckDB
- **DuckDB stats**: 2 wins, 1.72x avg
- **Regressions**: 0.49x (orphaned original CTE → double materialization), 0.68x (same mechanism)
- **Gates**: MUST eliminate original CTE after splitting. Redirect all references. 2-4 splits only. Same regression mechanism as self_join_decomposition.
- **AST detection**: UNION ALL present in optimized but not in original
- **Gold example**: `duckdb/union_cte_split.json` (Q74, 1.36x)
- **Pathology**: DuckDB P7

---

#### `self_join_pivot` (PG only)

Convert self-join comparison patterns to pivoted CASE aggregation within a
single pass. When 6 self-join aliases compute metrics for different quarters
or channels, pivot into one scan.

**Why it works**: Self-joins on the same table with different filters (quarterly
data) scan the table N times. CASE WHEN pivoting computes all quarters in one
pass: `MAX(CASE WHEN d_qoy=1 THEN sales END) AS q1_sales`.

**Pattern** (from gold example, 1.79x on PG):
```sql
-- BEFORE: 6 self-join aliases (3 quarters x 2 channels)
FROM store_sales ss1, store_sales ss2, store_sales ss3, ...
WHERE ss1.d_qoy = 1 AND ss2.d_qoy = 2 AND ss3.d_qoy = 3 ...

-- AFTER: single scan with CASE pivoting
WITH ss_all AS MATERIALIZED (
    SELECT ss_store_sk, d_qoy, SUM(ss_net_profit) AS profit
    FROM store_sales JOIN date_dim ON ...
    GROUP BY ss_store_sk, d_qoy
)
SELECT ss_store_sk,
    MAX(CASE WHEN d_qoy = 1 THEN profit END) AS q1,
    MAX(CASE WHEN d_qoy = 2 THEN profit END) AS q2,
    MAX(CASE WHEN d_qoy = 3 THEN profit END) AS q3
FROM ss_all GROUP BY ss_store_sk
```

- **Engines**: PostgreSQL
- **PG stats**: 1.79x. Zero regressions.
- **Gate**: Do NOT add dimension prefetch to self-join patterns (0.25x regression observed)
- **Gold example**: `postgres/self_join_pivot.json` (1.79x)
- **Pathology**: PG P3

---

### Family: Predicate Restructure (PR)

These transforms exploit CROSS_COLUMN_OR_DECOMPOSITION: the optimizer cannot
decompose OR conditions spanning different columns into independent targeted
scans. This family has the HIGHEST VARIANCE — the same transform that produces
6.28x on one query produces 0.23x on another. Requires careful query-specific
analysis before applying.

---

#### `or_to_union`

Split OR conditions on DIFFERENT columns into UNION ALL branches. Each branch
gets focused predicates allowing the optimizer to use different access paths
per branch.

**Why it works**: `WHERE a.cat='X' OR b.state='TX'` forces the optimizer to
evaluate both conditions on every row. By splitting into UNION ALL branches,
branch 1 can use an index on `cat` and branch 2 can use an index on `state`,
each scanning only relevant rows.

**Pattern** (from gold example, TPC-DS Q15, 3.17x on DuckDB):
```sql
-- BEFORE: OR across different columns forces full scan
SELECT ca_zip, SUM(cs_sales_price)
FROM catalog_sales, customer, customer_address, date_dim
WHERE ...
  AND (ca_zip IN ('85669', '86197', ...)    -- zip-based filter
       OR ca_state IN ('CA', 'WA', 'GA')    -- state-based filter
       OR cs_sales_price > 500)             -- price-based filter

-- AFTER: 3 branches, each with focused predicate
SELECT ca_zip, SUM(cs_sales_price) FROM (
    -- Branch 1: zip filter only
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales JOIN customer ON ... JOIN customer_address ON ...
    WHERE ca_zip IN ('85669', '86197', ...)

    UNION ALL

    -- Branch 2: state filter only
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales JOIN customer ON ... JOIN customer_address ON ...
    WHERE ca_state IN ('CA', 'WA', 'GA')

    UNION ALL

    -- Branch 3: price filter only
    SELECT ca_zip, cs_sales_price
    FROM catalog_sales JOIN customer ON ... JOIN customer_address ON ...
    WHERE cs_sales_price > 500
) combined
GROUP BY ca_zip
```

- **Engines**: DuckDB (NEVER on PG — BITMAP_OR_SCAN handles OR natively on indexed columns, 0.21x observed when split)
- **DuckDB stats**: 21 verified speedups, 2.44x avg, best 9.09x across 16 queries
- **Regressions**: 0.23x (nested OR expansion: 3 x 3 = 9 branches = 9 fact table scans), 0.59x (same-column OR split — engine handles natively), 0.51x (self-join re-executed per UNION branch), 0.41x (nested OR variant)
- **Gates**: Max 3 UNION branches. Cross-column ONLY — NEVER split OR on same column (DuckDB handles same-column OR natively). No nested OR (multiplicative branch expansion). No self-join in source query. Count resulting branches BEFORE committing.
- **Regression mechanism (0.23x)**: Nested OR like `(a OR b) AND (c OR d OR e)` expanded to 2 x 3 = 6 branches. Each branch independently scans the full fact table. On TPC-DS Q13/Q48, this became 9 branches = 9x the fact table scan cost. The fix is to count branches before committing and abort if > 3.
- **EXTREME VARIANCE WARNING**: Same transform produced 6.28x on Q88 and 0.23x on Q13. Query-specific analysis is mandatory.
- **AST detection**: UNION count increases while OR count decreases
- **Gold example**: `duckdb/or_to_union.json` (Q15, 3.17x)
- **Pathology**: DuckDB P4

---

#### `pushdown`

Push predicates from outer queries into CTEs/subqueries. When multiple
subqueries scan the same table, consolidate them into CTEs computing all
needed aggregates in fewer passes.

**Why it works**: Predicates in the outer query cannot benefit from indexes or
early filtering when the CTE doesn't include them. By moving the predicate
into the CTE definition, the CTE materializes fewer rows.

**Pattern** (from gold example, TPC-DS Q9, 2.11x on DuckDB):
```sql
-- BEFORE: 15 scalar subqueries scan store_sales independently

-- AFTER: consolidated into CTEs with pushed-down filters
WITH quantity_1_20 AS (
    SELECT COUNT(*) AS cnt, AVG(ss_ext_discount_amt) AS avg_disc,
           AVG(ss_net_profit) AS avg_prof
    FROM store_sales
    WHERE ss_quantity BETWEEN 1 AND 20  -- filter pushed INTO CTE
),
...
```

- **Engines**: DuckDB
- **DuckDB stats**: Broad contributor across many wins. Best 2.11x (Q9).
- **Regressions**: Blackboard shows 14 queries with regressions (0.27x-0.94x) when combined with other transforms. Standalone pushdown has no documented regressions.
- **AST detection**: WHERE count increases in existing CTEs
- **Gold example**: `duckdb/pushdown.json` (Q9, 2.11x)
- **Pathology**: DuckDB P0

---

### Family: Aggregation Rewrite (AR)

These transforms exploit AGGREGATE_BELOW_JOIN_BLINDNESS: the optimizer
cannot push GROUP BY below joins when aggregation keys align with join keys.
This family produced the single largest individual win in all benchmarks:
42.90x. Zero regressions documented.

---

#### `aggregate_pushdown`

Push aggregation below joins. When GROUP BY output has far fewer rows than
JOIN input, aggregate first on the fact table, then join dimensions.

**Why it works**: The optimizer joins 7M fact rows with dimensions, THEN
aggregates down to 150K groups. By pre-aggregating the fact table to 150K
rows FIRST, the subsequent dimension join processes 50x fewer rows. This
only works when GROUP BY keys are a superset of the join keys (correctness
requirement).

**Pattern** (from gold example, TPC-DS, 42.90x on DuckDB):
```sql
-- BEFORE: join 7M rows, then aggregate
SELECT i_product_name, i_brand, SUM(inv_quantity_on_hand) ...
FROM inventory          -- 7M rows
JOIN item ON ...        -- 300K rows
JOIN date_dim ON ...    -- 73K rows
GROUP BY i_product_name, i_brand, ...  -- only 150K groups

-- AFTER: aggregate first, then join
WITH inventory_agg AS (
    SELECT inv_item_sk, inv_date_sk,
           SUM(inv_quantity_on_hand) AS total_qty,
           COUNT(*) AS inv_count
    FROM inventory
    GROUP BY inv_item_sk, inv_date_sk  -- 150K rows (from 7M)
)
SELECT i_product_name, i_brand, ia.total_qty ...
FROM inventory_agg ia  -- 150K rows, not 7M!
JOIN item ON ia.inv_item_sk = i_item_sk
JOIN date_dim ON ia.inv_date_sk = d_date_sk
```

- **Engines**: DuckDB
- **DuckDB stats**: 3 wins, 15.3x avg, best 42.90x. **ZERO regressions.**
- **Gates**: GROUP BY keys MUST be superset of join keys (correctness). When using ROLLUP, reconstruct AVG from SUM/COUNT (AVG cannot be pre-aggregated). Must pre-aggregate BEFORE first join that doesn't break GROUP BY keys.
- **Gold example**: `duckdb/aggregate_pushdown.json` (42.90x)
- **Pathology**: DuckDB P3

---

#### `deferred_window_aggregation`

Defer window functions to after the main join and aggregation. When window
functions are computed inside CTEs, they process more rows than needed.

**Why it works**: Window functions in CTEs compute over ALL rows in the CTE,
including rows that will be filtered out by subsequent joins. By deferring
the window to after the join, it processes only the surviving rows. Works
because SUM() OVER() naturally skips NULLs from FULL OUTER JOINs.

**Pattern** (from gold example, 1.36x on DuckDB):
```sql
-- BEFORE: window computed in CTE over all rows
WITH monthly AS (
    SELECT store, month, SUM(sales) AS total,
           SUM(SUM(sales)) OVER (PARTITION BY store) AS yearly  -- computed on ALL months
    FROM store_sales GROUP BY store, month
)
SELECT * FROM monthly m1 FULL OUTER JOIN ...

-- AFTER: window deferred to post-join (processes fewer rows)
WITH monthly AS (
    SELECT store, month, SUM(sales) AS total
    FROM store_sales GROUP BY store, month
    -- NO window here
)
SELECT *, SUM(total) OVER (PARTITION BY store) AS yearly  -- computed on joined result
FROM monthly m1 FULL OUTER JOIN ...
```

- **Engines**: DuckDB
- **DuckDB stats**: 1 win, 1.36x. Zero regressions.
- **Gates**: NOT LAG/LEAD (depends on pre-join row order). NOT ROWS BETWEEN with specific frame. Only monotonic aggregates (SUM, COUNT, MIN, MAX — not AVG, which needs SUM/COUNT).
- **Gold example**: `duckdb/deferred_window_aggregation.json` (1.36x)
- **Pathology**: DuckDB P8

---

#### `rollup_to_union_windowing`

Convert ROLLUP-based aggregation patterns to explicit UNION ALL with window
functions for cases where the optimizer can't push predicates through ROLLUP.

**Why it works**: ROLLUP computes all hierarchy levels in one pass, but the
optimizer can't push predicates through it. By explicitly computing each
hierarchy level as a separate UNION ALL branch, each branch can have targeted
filters and the result can use window functions for ranking.

- **Engines**: DuckDB
- **DuckDB stats**: 2.47x. Limited data.
- **Regressions**: 0.85x (CTE prevents ROLLUP pushdown — only apply when ROLLUP is the bottleneck, not when it's efficient)
- **Gold example**: `duckdb/rollup_to_union_windowing.json` (2.47x)

---

### Family: Join Restructure (JR)

These transforms give the optimizer better structural information about joins.
On DuckDB, this means converting LEFT to INNER when NULLs are eliminated.
On PostgreSQL, this means converting comma joins to explicit JOIN syntax,
exploiting the COMMA_JOIN_WEAKNESS gap — the most reliable PG optimization.

---

#### `inner_join_conversion`

Convert LEFT JOIN to INNER JOIN when a downstream WHERE clause eliminates NULLs
anyway (null-eliminating filter).

**Why it works**: LEFT JOIN must preserve all left rows and produce NULLs for
non-matching right rows. But if the WHERE clause filters on a right-table column
(e.g., `WHERE sr_reason_sk = r_reason_sk`), NULLs are eliminated anyway. The
optimizer cannot infer this, so it maintains the expensive LEFT JOIN semantics.
Converting to INNER lets the optimizer freely reorder the join.

**Pattern** (from gold example, TPC-DS, 3.44x on DuckDB):
```sql
-- BEFORE: LEFT JOIN preserved but WHERE eliminates NULLs
FROM store_sales ss
LEFT JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk
WHERE sr.sr_reason_sk = r.r_reason_sk  -- this eliminates NULLs!

-- AFTER: INNER JOIN (safe because WHERE proves non-null)
FROM store_sales ss
INNER JOIN store_returns sr ON ss.ss_item_sk = sr.sr_item_sk
WHERE sr.sr_reason_sk = r.r_reason_sk
```

- **Engines**: DuckDB
- **DuckDB stats**: 2 wins, 1.9x and 3.4x. **ZERO regressions.**
- **Gates**: No CASE WHEN IS NULL on right-table column (the NULL branch is semantic, not accidental). No COALESCE on right-table column. WHERE must be simple equality or comparison.
- **Gold example**: `duckdb/inner_join_conversion.json` (3.44x)
- **Pathology**: DuckDB P5

---

#### `pg_date_cte_explicit_join` (PG only)

Combine dimension CTE isolation with conversion from comma joins to explicit
JOIN...ON syntax. Both are required together on PG — CTE alone may not help,
explicit JOINs alone may not help, but the combination enables hash join with
a tiny probe side.

**Why it works**: PostgreSQL's optimizer handles comma-separated FROM tables
as implicit cross products with WHERE-based filter. This limits the planner's
join reordering and cardinality estimation. Converting to explicit JOIN...ON
gives the optimizer structural hints about join relationships, and adding
dimension CTEs gives it accurate row count estimates for the tiny filtered
sets. The combination is more powerful than either alone.

**Pattern** (from gold example, DSB Q081, 2.28x on PG):
```sql
-- BEFORE: comma joins, no structural hints
SELECT ... FROM store_sales, date_dim, item, store
WHERE ss_sold_date_sk = d_date_sk AND ss_item_sk = i_item_sk
  AND d_year = 2000 AND i_category = 'Music'

-- AFTER: date CTE + explicit JOINs
WITH filtered_dates AS (
    SELECT d_date_sk FROM date_dim WHERE d_year = 2000  -- 730 rows from 73K
)
SELECT ...
FROM store_sales ss
INNER JOIN filtered_dates fd ON ss.ss_sold_date_sk = fd.d_date_sk
INNER JOIN item i ON ss.ss_item_sk = i.i_item_sk
INNER JOIN store s ON ss.ss_store_sk = s.s_store_sk
WHERE i.i_category = 'Music'
```

- **Engines**: PostgreSQL
- **PG stats**: 4 wins, 2.1x avg, best 2.28x. Most reliable PG optimization.
- **Regressions**: 0.88x (explicit join overhead on simple query with baseline < 100ms)
- **Gap**: COMMA_JOIN_WEAKNESS — 5+ comma-separated tables is the sweet spot
- **Field note**: Win usually comes from explicit JOINs + CTE together, not CTE alone. CTE alone on PG can even hurt because it creates a materialization fence.
- **Gold example**: `postgres/date_cte_explicit_join.json` (2.28x)
- **Pathology**: PG P1

---

#### `pg_materialized_dimension_fact_prefilter` (PG only)

Staged reduction for non-equi joins (BETWEEN, <, >). Reduce BOTH dimension
and fact table sizes via MATERIALIZED CTEs before the expensive inequality
join. Combined selectivity dramatically cuts the search space.

**Why it works**: Non-equi joins cannot use hash join — they fall back to
nested loop O(N*M). Reducing N from 10M to 100K and M from 300K to 5K
reduces the search space from 3 trillion comparisons to 500 million — a
6000x reduction in work, yielding the observed 12x speedup.

**Pattern** (from gold example, DSB, 12.07x on PG):
```sql
-- BEFORE: non-equi join on full tables
SELECT ... FROM catalog_sales cs, item i
WHERE cs.cs_wholesale_cost BETWEEN i.i_wholesale_cost AND i.i_wholesale_cost * 1.5
-- 10M x 300K = nested loop nightmare

-- AFTER: pre-filter both sides, THEN non-equi join
WITH filtered_sales AS MATERIALIZED (
    SELECT cs_item_sk, cs_wholesale_cost, ...
    FROM catalog_sales JOIN date_dim ON ...
    WHERE d_year = 2001  -- reduces 10M → 100K
),
filtered_items AS MATERIALIZED (
    SELECT i_item_sk, i_wholesale_cost, ...
    FROM item WHERE i_category = 'Books'  -- reduces 300K → 5K
)
SELECT ... FROM filtered_sales cs
JOIN filtered_items i ON cs.cs_wholesale_cost BETWEEN i.i_wholesale_cost AND ...
-- 100K x 5K = manageable nested loop
```

- **Engines**: PostgreSQL
- **PG stats**: 12.07x (V2 upgrade from 2.68x V1)
- **Regressions**: None with tight, selective filters. 0.79x observed with loose UNION/OR superset filters that don't actually reduce cardinality.
- **Gap**: NON_EQUI_JOIN_INPUT_BLINDNESS
- **Gate**: Both join inputs must be > 10K rows. At least one side must have selective filter available. Loose superset filters are harmful — filter must actually reduce cardinality significantly.
- **Gold example**: `postgres/materialized_dimension_fact_prefilter.json` (12.07x)
- **Pathology**: PG P4

---

#### `pg_explicit_join_materialized` (PG only)

Convert comma joins to explicit INNER JOIN + pre-materialized dimension CTEs.
The explicit JOINs give the planner accurate cardinality estimates at each
join step, and the materialized dimensions prevent CTE re-inlining.

- **Engines**: PostgreSQL
- **PG stats**: 2 wins, 5.9x avg, best 8.56x
- **Field note**: 5+ table comma joins are the sweet spot. Planner sees accurate row counts at each step instead of guessing from the cross product.
- **Gold example**: `postgres/explicit_join_materialized.json` (8.56x)
- **Pathology**: PG P1

---

### Utility Transforms

Valid transforms not tied to a specific optimizer blindspot. Used as catch-all
labels when the primary transform doesn't fit a named pattern.

| ID | What it does | Engines | Notes |
|----|-------------|---------|-------|
| `flatten_subquery` | Convert EXISTS/IN to JOINs | DuckDB, PG | Use with caution — EXISTS semi-join may be optimal |
| `reorder_join` | Reorder joins for selectivity | DuckDB, PG | Usually the optimizer handles this; only for comma-join queries |
| `multi_push_predicate` | Push predicates through multiple CTE layers | DuckDB | Compound version of pushdown |
| `inline_cte` | Inline single-use CTEs to avoid materialization overhead | DuckDB, PG | Opposite of materialize_cte; use when CTE is referenced once |
| `remove_redundant` | Remove unnecessary DISTINCT/ORDER BY | DuckDB, PG | Safe — removing redundant operations never regresses |
| `materialize_cte` | Extract repeated subqueries into CTE (fallback label) | DuckDB | DANGEROUS for EXISTS (0.14x). Only use for truly repeated expensive subqueries. |
| `semantic_rewrite` | Catch-all for valid optimizations not matching above | DuckDB, PG | Applied when AST inference can't identify the specific transform |

---

## Documented Regressions

10 regression patterns documented in `examples/duckdb/regressions/`:

| Regression | Worst ratio | Root cause | NEVER rule |
|------------|-------------|------------|------------|
| `regression_exists_materialization` | 0.14x | Materialized GROUP BY HAVING on unfiltered fact table — destroyed semi-join short-circuit | NEVER materialize EXISTS/NOT EXISTS |
| `regression_left_join_decorrelation` | 0.34x | LEFT JOIN already had efficient semi-join plan; decorrelation removed it | Check EXPLAIN for existing hash join before decorrelating |
| `regression_orphaned_cte_pushdown` | 0.49x | Split CTEs created but original CTE left behind — double materialization, 1000x+ cardinality error | ALWAYS delete original CTE after splitting |
| `regression_fast_baseline_cte_overhead` | 0.50x | CTE overhead on fast baseline (< 50ms) exceeded any scan reduction benefit | Skip CTE-based rewrites when baseline < 100ms |
| `regression_exists_pair_broken` | 0.54x | Decomposed tightly-correlated EXISTS/NOT EXISTS pairs — broke the semi-join interaction | Don't decompose correlated EXISTS pairs |
| `regression_same_column_or_split` | 0.59x | Split OR on same column into UNION — doubled fact table scan; engine handles same-column OR natively | NEVER split same-column OR |
| `regression_orphaned_union_cte` | 0.68x | UNION CTE split by year but original kept — redundant materialization | MUST remove original after splitting |
| `regression_over_decomposed_cte` | 0.71x | Pre-aggregated ALL stores when only a subset was needed — more work, not less | Don't decompose when original CTE is already efficient |
| `regression_rollup_pushdown_blocked` | 0.85x | CTE creation blocked ROLLUP pushdown optimization path | Don't wrap ROLLUP/CUBE queries in CTEs |
| `regression_window_bottleneck_cte` | 0.87x | Materialized cumulative windows before joins that filter on them — processed unnecessary rows | Don't materialize window computations before filtering joins |

---

## Query Category Classification

Queries are classified by `tag_index.py::classify_category()` based on their
dominant SQL patterns. This determines which transform family to try first.

| Category | Trigger tags | Primary transforms | Safety |
|----------|-------------|-------------------|--------|
| `scan_consolidation` | repeated_scan, self_join | single_pass_aggregation, self_join_decomposition | SAFEST — zero regressions |
| `set_operations` | intersect, except, union | intersect_to_exists, set_operation_materialization | SAFE |
| `aggregation_rewrite` | rollup, cube, grouping | aggregate_pushdown, deferred_window_aggregation | SAFE |
| `subquery_elimination` | correlated_subquery, exists+subquery | decorrelate, inline_decorrelate_materialized | HIGH IMPACT, medium risk |
| `filter_pushdown` | cte + filter-heavy | date_cte_isolate, dimension_cte_isolate, early_filter | MEDIUM risk |
| `join_reorder` | 2+ join types | inner_join_conversion, pg_date_cte_explicit_join | SAFE on DuckDB |
| `general` | none of above | any applicable transform | Explore mode |

---

## Cross-Engine Behavior

The same transform can behave very differently across engines. These differences
are critical for selecting the right approach:

| Transform | DuckDB | PostgreSQL | Snowflake |
|-----------|--------|------------|-----------|
| `date_cte_isolate` | CTE inlined by optimizer but structural hint still helps predicate pushdown. 1.86x avg. | MUST use AS MATERIALIZED or optimizer re-inlines. Combine with explicit JOINs. 3.10x. | Enables RUNTIME partition pruning. Static EXPLAIN still shows all partitions — this is expected and correct. 13/15 win rate on timeouts. |
| `decorrelate` | Usually effective. Check EXPLAIN for existing hash join (already decorrelated = STOP). 1.52x avg. | HIGHEST IMPACT transform on PG. 100-8000x on timeouts. MUST use AS MATERIALIZED. ALWAYS try first on correlated subqueries. | Untested. |
| `intersect_to_exists` | Semi-join optimization kicks in. 1.83x. Zero regressions. | Same mechanism. 1.78x. Zero regressions. | Untested. |
| `single_pass_aggregation` | Zero regressions. Safest transform. 2.72x avg. | Same mechanism, same safety. 1.98x. | Untested. |
| `early_filter` | Works well, engine sometimes does it already. 4.00x best. | Critical for comma-join queries where optimizer can't reorder tables. 27.80x combined. | Critical for date pruning failures. |
| `or_to_union` | HIGHEST VARIANCE: 0.23x-9.09x. Needs careful analysis. | NEVER on PG — BITMAP_OR_SCAN handles OR natively (0.21x regression). | Untested. |
| `materialize_cte` | DANGEROUS for EXISTS (0.14x). Safe for truly shared subexpressions. | PG auto-materializes multi-use CTEs. Use AS MATERIALIZED explicitly for decorrelation. | Cloud warehouse — materialization semantics differ. |

---

## Regression Safety Ranking

Transforms ranked by regression risk (safest first). Use this to prioritize
which transforms to try first on unknown queries.

| Rank | Transform | Regressions | Worst ratio | Safety | Try order |
|------|-----------|-------------|-------------|--------|-----------|
| 1 | `single_pass_aggregation` | 0 | — | SAFE | Always try first if repeated scans detected |
| 2 | `channel_bitmap_aggregation` | 0 | — | SAFE | Variant of #1 for channel patterns |
| 3 | `aggregate_pushdown` | 0 | — | SAFE | Try when GROUP BY << JOIN input |
| 4 | `inner_join_conversion` | 0 | — | SAFE | Try when LEFT + null-eliminating WHERE |
| 5 | `intersect_to_exists` | 0 | — | SAFE | Try when INTERSECT on 1K+ rows |
| 6 | `multi_date_range_cte` | 0 | — | SAFE | Try when 2+ date_dim aliases |
| 7 | `deferred_window_aggregation` | 0 | — | SAFE | Try when windows inside CTEs |
| 8 | `early_filter` | 0 standalone | — | SAFE | Try when dimension filters available |
| 9 | `pushdown` | 0 standalone | — | SAFE | Try when predicates in outer query |
| 10 | `self_join_decomposition` | 2 | 0.49x | MEDIUM | Orphaned CTE risk — must delete original |
| 11 | `union_cte_split` | 2 | 0.49x | MEDIUM | Same orphaned CTE risk |
| 12 | `date_cte_isolate` | 2 | 0.50x | MEDIUM | Skip if 3+ fact joins or ROLLUP |
| 13 | `prefetch_fact_join` | 2 | 0.50x | MEDIUM | Max 2 cascading chains |
| 14 | `dimension_cte_isolate` | 2 | 0.0076x | HIGH | Never cross-join 3+ CTEs |
| 15 | `decorrelate` | 4 | 0.34x | HIGH | Never on EXISTS; preserve all filters |
| 16 | `or_to_union` | 3 | 0.23x | HIGH | Max 3 branches; cross-column only |
| 17 | `materialize_cte` | 2 | 0.14x | DANGEROUS | NEVER on EXISTS/NOT EXISTS |

---

## AST Detection Patterns

How `sql_rewriter.py::infer_transforms_from_sql_diff()` identifies transforms
from before/after SQL comparison (no LLM needed, deterministic):

| Transform | Detection signal | Specificity |
|-----------|-----------------|-------------|
| `decorrelate` | Subquery count drops, CTE/JOIN count increases | High — clear structural change |
| `or_to_union` | UNION count up, OR count down | High |
| `intersect_to_exists` | INTERSECT count down, EXISTS count up | High |
| `single_pass_aggregation` | CASE inside COUNT/SUM/AVG increases | High |
| `date_cte_isolate` | New CTE with date_dim + date filter columns (d_year, d_moy, d_month_seq) | Medium — need column name check |
| `dimension_cte_isolate` | New CTE with dimension table (not date_dim, not fact) with WHERE | Medium |
| `multi_dimension_prefetch` | 2+ new dimension CTEs | Medium — composite of #6 |
| `multi_date_range_cte` | 2+ date CTEs with different date filters | Medium — composite of #5 |
| `prefetch_fact_join` | CTE joining fact table with pre-filtered dimension (has_fact + has_dim) | Medium |
| `union_cte_split` | UNION ALL present in optimized but not in original | Medium |
| `early_filter` | New CTEs with WHERE, or existing CTEs gain WHERE conditions | Low — generic |
| `pushdown` | WHERE conditions move from outer to inner queries | Low — generic |
| `materialize_cte` | CTE count increases, no more specific pattern matched | Fallback — assigned when nothing else matches |

---

## DuckDB Blackboard Stats (TPC-DS, all sources aggregated)

From `duckdb_tpcds.json` global blackboard — empirical data across 200+ swarm
runs covering Kimi K2.5, V2 Standard, V2 Evolutionary, 3-Worker Retry, and
4-Worker Retry batches:

| Transform principle | Win queries | Avg speedup | Best speedup | Regression queries | Win:Regression ratio |
|--------------------|-------------|-------------|--------------|-------------------|---------------------|
| `date_cte_isolate` | 53 | 1.86x | 9.50x | 53 | 1:1 (high variance) |
| `decorrelate` | 32 | 1.52x | 4.38x | 19 | 1.7:1 |
| `or_to_union` | 16 | 2.44x | 9.09x | 42 | 0.4:1 (more regressions than wins!) |
| `prefetch_fact_join` | 18 | 1.82x | 5.23x | 36 | 0.5:1 (high risk) |
| `dimension_cte_isolate` | 7 | 2.05x | 6.24x | 27 | 0.3:1 (high risk) |
| `single_pass_aggregation` | 4 | 2.72x | 5.85x | 22* | N/A (regressions from compound rewrites) |
| `early_filter` | broad | — | 4.00x | few | High |
| `union_cte_split` | 2 | 1.72x | — | 1 | 2:1 |
| `intersect_to_exists` | 2 | 1.83x | 2.70x | 5* | N/A (regressions from compound rewrites) |
| `multi_dimension_prefetch` | 3 | 1.55x | 2.71x | 7 | 0.4:1 |
| `multi_date_range_cte` | 3 | 1.42x | 2.35x | 2 | 1.5:1 |
| `pushdown` | broad | — | 2.11x | 14 | — |
| `materialize_cte` | 1 | 1.37x | — | 7 | 0.1:1 (AVOID) |

\* Regressions marked with * were from compound rewrites where the named
transform was combined with other harmful transforms — the consolidation
itself was not the cause.

**Key insight from blackboard**: Win:Regression ratio is the most important
metric. `or_to_union` has 2.44x avg speedup but 0.4:1 win ratio — it
regresses more often than it wins. `single_pass_aggregation` has 2.72x avg
with zero standalone regressions. Always prefer high-ratio transforms.

---

## Recommended Transform Order by Engine

Based on safety ranking, win rate, and impact:

### DuckDB — try in this order:
1. **P1 Scan Consolidation** (single_pass_aggregation) — ZERO regressions, 2.72x avg
2. **P3 Aggregation Rewrite** (aggregate_pushdown) — ZERO regressions, 42.90x best
3. **P5/P6 Join+Set** (inner_join_conversion, intersect_to_exists) — ZERO regressions
4. **P0 Scan Reduction** (date_cte_isolate, prefetch_fact_join) — high volume, medium risk
5. **P7 Self-Join Split** (self_join_decomposition) — 4.76x but orphaned CTE risk
6. **P2 Decorrelate** — high impact, check EXPLAIN first
7. **P4 OR Decomposition** (or_to_union) — LAST, query-specific analysis mandatory

### PostgreSQL — try in this order:
1. **P2 Correlated Subquery** — HIGHEST IMPACT (100-8000x timeout rescues). Try FIRST.
2. **P1 Comma Joins** (pg_date_cte_explicit_join) — most reliable PG optimization
3. **P6 Date Consolidation** — ZERO regressions
4. **P4 Non-equi Prefilter** — 12.07x with tight filters
5. **P3 Repeated Scans** (single_pass_aggregation, self_join_pivot) — ZERO regressions
6. **P5 Set Operations** — 17.48x
7. **P7 Multi-dim Prefetch** — CAUTION, 0.25x observed on self-joins

---

## File Locations

| What | Path |
|------|------|
| This document | `qt_sql/knowledge/TRANSFORM_TAXONOMY.md` |
| Transform catalog (JSON, 30 entries) | `qt_sql/knowledge/transforms.json` |
| DuckDB gold examples (23 files) | `qt_sql/examples/duckdb/` |
| PG gold examples (14 files) | `qt_sql/examples/postgres/` |
| DuckDB regressions (10 files) | `qt_sql/examples/duckdb/regressions/` |
| AST inference engine | `qt_sql/sql_rewriter.py::infer_transforms_from_sql_diff()` |
| Tag classification | `qt_sql/tag_index.py::classify_category()` |
| DuckDB playbook (10 pathologies) | `qt_sql/knowledge/duckdb.md` |
| PG playbook (7 pathologies) | `qt_sql/knowledge/postgresql.md` |
| Snowflake playbook (9 pathologies) | `qt_sql/knowledge/snowflake.md` |
| Global blackboard (14 principles, 29 anti-patterns) | `qt_sql/knowledge/duckdb_tpcds.json` |
| Engine profiles (structured fallback) | `qt_sql/constraints/engine_profile_{dialect}.json` |
| Allowed list (V1 path) | `qt_sql/dag.py::ALLOWED_TRANSFORMS` |
