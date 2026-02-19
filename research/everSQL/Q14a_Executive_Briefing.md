# QueryTorque vs EverSQL: TPC-DS Q14a Head-to-Head

**Database**: PostgreSQL 14.3, TPC-DS SF10 (~28.8B rows store_sales)
**Query**: TPC-DS Q14a — cross-channel item analysis with INTERSECT + ROLLUP
**Validation**: 3x runs (discard warmup, average last 2), MD5 checksum verified identical results

---

## Results at a Glance

| Tool | Runtime | Speedup | Verdict |
|------|---------|---------|---------|
| **Original SQL** | 44.89s | — | baseline |
| **EverSQL** | 44.77s | 1.00x | NEUTRAL |
| **QueryTorque** | 11.95s | **3.76x** | **WIN** |

**QueryTorque delivers a 3.76x speedup. EverSQL delivers nothing.**

---

## What EverSQL Did

EverSQL's optimizer made exactly one change:

```sql
-- Original
WHERE ss_item_sk IN (SELECT ss_item_sk FROM cross_items)

-- EverSQL
WHERE EXISTS (SELECT 1 FROM cross_items WHERE ss_item_sk = cross_items.ss_item_sk)
```

This is the textbook `IN → EXISTS` conversion. On PostgreSQL 9.5 and earlier, this
could matter. On PostgreSQL 14, the optimizer already converts both forms into the
same **semi-join** execution plan internally. The change has zero performance impact.

**Why EverSQL can't do better**: EverSQL collects only static schema metadata — table
names, column types, index definitions. It never sees the EXPLAIN plan, never knows
where time is actually spent, and never measures whether its changes helped. It applies
generic rewrite rules blindly and hopes one sticks.

---

## What QueryTorque Did

QueryTorque reads the EXPLAIN ANALYZE plan and identifies the actual bottlenecks:

### Bottleneck 1: Repeated date_dim Scans (6x)

The original query joins `date_dim` **six separate times** — once in each INTERSECT
branch (3x) and once in each channel subquery (3x). Each scan filters the same
`d_year BETWEEN 2000 AND 2002` predicate.

**Fix — Date CTE Isolation**:
```sql
WITH filtered_dates AS (
  SELECT d_date_sk FROM date_dim WHERE d_year BETWEEN 2000 AND 2002
)
```
One scan, one filter, reused 6 times via CTE reference. PostgreSQL materializes
this small result set (~1,100 rows) and hash-joins it into each subsequent scan.

### Bottleneck 2: Expensive Triple INTERSECT

The original `cross_items` CTE uses a three-way INTERSECT to find items sold across
all three channels (store, catalog, web). INTERSECT requires:
- Full materialization of each branch
- Sorting or hashing each branch
- Set intersection comparison

This processes millions of (brand_id, class_id, category_id) tuples three times.

**Fix — INTERSECT → 3x Correlated EXISTS**:
```sql
FROM item
WHERE EXISTS (SELECT 1 FROM store_sales   JOIN item iss ... WHERE iss.i_brand_id = item.i_brand_id ...)
  AND EXISTS (SELECT 1 FROM catalog_sales JOIN item ics ... WHERE ics.i_brand_id = item.i_brand_id ...)
  AND EXISTS (SELECT 1 FROM web_sales     JOIN item iws ... WHERE iws.i_brand_id = item.i_brand_id ...)
```
Instead of materializing three full result sets and intersecting them, this probes
each channel once per item and short-circuits on the first match. The optimizer can
use index-backed semi-joins, and items that fail the first EXISTS never check the
remaining two.

### Bottleneck 3: Redundant Date Filter in Channel Queries

Each channel subquery (store/catalog/web) independently filters `d_year = 2002 AND d_moy = 11`.

**Fix — Nov 2002 Date CTE**:
```sql
WITH nov2002_dates AS (
  SELECT d_date_sk FROM date_dim WHERE d_year = 2002 AND d_moy = 11
)
```
Pre-computes the ~30 matching date keys once. Each channel query hash-joins against
this tiny set instead of scanning date_dim (73K rows) three more times.

### Bottleneck 4: Per-Channel CTE Decomposition

Each channel subquery is extracted into its own CTE (`store_sales_data`,
`catalog_sales_data`, `web_sales_data`), enabling the optimizer to plan each
independently and reuse date/item joins more efficiently.

---

## Why EverSQL Cannot Compete

| Capability | EverSQL | QueryTorque |
|-----------|---------|-------------|
| **Reads EXPLAIN ANALYZE** | No | Yes — identifies operator-level bottlenecks |
| **Knows where time is spent** | No | Yes — sees actual ms per plan node |
| **Structural rewrites** | No — only IN/EXISTS, index hints | Yes — CTE isolation, INTERSECT elimination, join reordering |
| **Validates speedup** | No — ships blind | Yes — 3x benchmark + MD5 checksum |
| **Adapts to engine version** | No — applies same rules to PG9 and PG16 | Yes — engine-specific gap profiles |
| **Multi-candidate evaluation** | No — single rewrite | Yes — beam search with parallel candidate racing |

The fundamental difference: **EverSQL looks at the blueprint (schema). QueryTorque
looks at the X-ray (EXPLAIN ANALYZE).** You can't optimize what you can't see.

---

## The Numbers

- **44.89s → 11.95s** = 32.94 seconds saved per execution
- At 100 executions/day = **55 minutes saved daily**
- At 1,000 executions/day = **9.2 hours saved daily**
- Checksum: `f8d775146a9c417338bf4aa12d10a70e` — **identical results, zero semantic risk**

---

*Benchmark conducted 2026-02-19 on PostgreSQL 14.3, TPC-DS SF10, 3x validation protocol.*
