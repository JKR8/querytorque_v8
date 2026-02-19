# EverSQL Q14a Benchmark — PG14.3 DSB SF10

## EverSQL's Only Change
- `IN (SELECT ...) → EXISTS (SELECT 1 FROM ... WHERE ...)`
- Added explicit table qualifiers (`"store_sales".ss_item_sk` etc.) — cosmetic only

## 3x Benchmark Results (discard warmup, avg last 2)

| Version  | Warmup  | Run 2    | Run 3    | Avg (2-3) |
|----------|---------|----------|----------|-----------|
| Original | 95.42s  | 45.32s   | 44.45s   | **44.89s** |
| EverSQL  | 77.02s  | 45.07s   | 44.48s   | **44.77s** |

## Result
- **Speedup: 1.00x — NEUTRAL**
- PG14 optimizer converts `IN (subquery)` to semi-join identically to `EXISTS`
- EverSQL's IN→EXISTS transformation has zero effect on modern PostgreSQL

## Our Best DuckDB Rewrite (2.39x)
Key transforms:
1. Date CTE isolation (filtered_dates + nov2002_dates)
2. INTERSECT → 3x correlated EXISTS on item table
3. Per-channel CTEs (store_sales_data, catalog_sales_data, web_sales_data)
4. Explicit JOIN syntax replacing comma joins

## QueryTorque Rewrite on PG14.3 (3x benchmark)

| Version  | Warmup  | Run 2    | Run 3    | Avg (2-3) |
|----------|---------|----------|----------|-----------|
| Original | 95.42s  | 45.32s   | 44.45s   | **44.89s** |
| EverSQL  | 77.02s  | 45.07s   | 44.48s   | **44.77s** |
| **QT**   | 13.72s  | 11.93s   | 11.97s   | **11.95s** |

- **QT Speedup: 3.76x WIN** (44.89s → 11.95s)
- **EverSQL Speedup: 1.00x NEUTRAL**

Transforms applied:
1. Date CTE isolation (filtered_dates reused 6x)
2. INTERSECT → 3x correlated EXISTS
3. Nov 2002 date CTE isolation
4. Per-channel CTEs

## Verdict
EverSQL optimizes based on static schema metadata (table/index/column catalog).
Without EXPLAIN ANALYZE data, it can only apply generic rules like IN→EXISTS
which modern optimizers already handle identically.

QueryTorque's EXPLAIN-first approach identifies actual bottlenecks (repeated date_dim scans,
expensive INTERSECT set operation) and applies targeted structural rewrites.
Result: 3.76x on PG vs EverSQL's 1.00x — on the same query.
