# Snowflake Warehouse Downsizing: Findings

## Date: 2026-02-14

## Hypothesis

Query rewrite + data clustering + QAS enables 2-4x warehouse downsizing
on TPC-DS workloads, saving 50-75% on compute credits.

## What We Proved

### 1. Equivalence: VERIFIED (All 3 Queries)

All rewrites produce **identical results** to the originals.

| Query | Rows | Columns | MD5 Checksum |
|-------|------|---------|-------------|
| Q21 | 100 | 4 (w_warehouse_name, i_item_id, inv_before, inv_after) | `795228c014ebe2380f20df61f00e4166` |
| Q36 | 100 | 5 (gross_margin, i_category, i_class, lochierarchy, rank_within_parent) | `730bb61757092fc05a4f527296f7dd20` |
| Q88 | 1 | 8 (h8_30_to_9 through h12_to_12_30) | Verified (values match) |

Row-by-row MD5 checksum comparison after canonical sorting. Zero diffs.

### 2. Warehouse Sizing: Q36 Performance (Date CTE Isolation)

| Size | Credits/hr | Original | Optimized | Speedup |
|------|-----------|----------|-----------|---------|
| X-Small | 1 | 66.0s | 62.6s | 1.06x |
| Small | 2 | 32.8s | 31.6s | 1.04x |
| Medium | 4 | 15.7s | 14.1s | 1.12x |

### 3. Q36 ROLLUP Decomposition (DuckDB 1.56x Winner Adapted)

| Size | Credits/hr | Original | Optimized | Speedup |
|------|-----------|----------|-----------|---------|
| X-Small | 1 | 66.1s | 63.4s | 1.04x |
| Small | 2 | 33.2s | 31.8s | 1.04x |
| Medium | 4 | 15.6s | 13.9s | 1.12x |

### 4. Q88 Single-Pass Scan Consolidation (DuckDB 5.25x Winner Adapted)

| Size | Credits/hr | Original | Optimized | Speedup |
|------|-----------|----------|-----------|---------|
| Medium | 4 | 40.1s | 47.9s | **0.84x (REGRESSION)** |

## Why DuckDB/PostgreSQL Speedups Don't Transfer to Snowflake

### Critical Discovery: Snowflake's Optimizer Is Smarter

We tested three distinct optimization techniques that produced big wins on DuckDB.
**All three failed on Snowflake** because the optimizer already performs these
transformations automatically:

#### Technique 1: Date CTE Isolation (Q36)
- **DuckDB weakness**: Optimizer can't push date predicates through comma joins
- **Snowflake reality**: Runtime partition pruning works regardless of join syntax
  on pre-clustered data. Both original and optimized scan the same partitions.
- **Result**: 1.04-1.12x (noise)

#### Technique 2: ROLLUP-to-UNION Decomposition (Q36)
- **DuckDB weakness**: GroupingSets operator is slow; manual UNION ALL on
  pre-aggregated data saves 1.56x
- **Snowflake reality**: Native GroupingSets operator is already optimized.
  The ROLLUP on ~300 rows is negligible either way. The bottleneck is the
  28.8B row scan, which is identical in both plans.
- **Result**: 1.04-1.12x (noise)

#### Technique 3: Scalar Subquery Consolidation (Q88)
- **DuckDB weakness**: 8 independent subqueries execute as 8 sequential full
  scans. Single-pass CTE with COUNT(CASE) reduces to 1 scan → 5.25x.
- **Snowflake reality**: **Optimizer automatically consolidates** the 8
  subqueries into a single scan with conditional aggregation (condAggr).
  EXPLAIN plan shows identical structure for both original and optimized.
  The CTE overhead in our rewrite actually makes it *slower*.
- **Result**: 0.84x (our rewrite is worse)

### EXPLAIN Plan Evidence (Q88)

**Original Q88** — Snowflake's auto-consolidated plan:
```
Row  0: Result | COUNT(*) × 8
Row  1: Aggregate | condAggr(0)..condAggr(7)  ← 8 conditional counts in ONE pass
Row 17: TableScan STORE_SALES | 72718/72718 partitions  ← SINGLE SCAN
```

**Optimized Q88** — Our explicit single-pass:
```
Row  0: Result | COUNT(CASE slot=1)..COUNT(CASE slot=8)
Row  1: Aggregate | condAggr(0)..condAggr(7)  ← same structure
Row 17: TableScan STORE_SALES | 72718/72718 partitions  ← same single scan
```

**Both plans are functionally identical.** Snowflake already does what we're
trying to do manually.

## Root Cause Analysis

### Why Snowflake doesn't have the same weaknesses as DuckDB

| Weakness | DuckDB | Snowflake | Why |
|----------|--------|-----------|-----|
| Repeated scans (P1) | Can't consolidate scalar subqueries | Auto-consolidates with condAggr | Cloud-scale optimizer |
| Predicate pushback (P0) | Order-dependent join planning | Runtime partition pruning | Micro-partition metadata |
| Slow ROLLUP (Q36) | GroupingSets operator is slow | Native optimized operator | Purpose-built for analytics |
| Cross-column OR (P4) | Forces full scans | Handles natively | Bloom filters + JoinFilter |

### The ONLY confirmed Snowflake weakness

**P1: Comma Join Preventing Date-Based Partition Pruning** — but ONLY on
unclustered data where micro-partitions span multiple date ranges. On
SNOWFLAKE_SAMPLE_DATA (pre-clustered, average_depth=1.05), this pathology
does not manifest.

## What Would Prove the Theory

To properly test warehouse downsizing, we need:

1. **Our own database** with TPC-DS data loaded in RANDOM ORDER (not pre-clustered)
2. **Baseline run**: Original Q36 on Small → should spill/timeout
3. **Rewrite run**: Optimized Q36 on Small → should be fast (runtime pruning)
4. **Clustering**: `ALTER TABLE STORE_SALES CLUSTER BY (SS_SOLD_DATE_SK)` → verify depth drops
5. **Post-clustering**: Both queries should be fast, but on a SMALLER warehouse

### The Real Test Matrix

```
                  | Unclustered Data  | Clustered Data    |
                  | Original | Rewrite| Original | Rewrite|
X-Small (1 cr/hr) | TIMEOUT  | FAST   | MODERATE | FAST   |
Small   (2 cr/hr) | SLOW     | FAST   | FAST     | FAST   |
Medium  (4 cr/hr) | MODERATE | FAST   | FAST     | FAST   |
```

If this matrix holds, the business pitch becomes:
"We cluster your data once (fixed cost) and rewrite your queries (consulting fee),
then you drop from Large to Small = 75% savings ongoing."

## Tactical Inventory

### Available Levers (from research)

| # | Lever | Status | Impact |
|---|-------|--------|--------|
| 1 | **Query Rewrite** (date CTE isolation) | TESTED — no effect on pre-clustered | Potentially 100x+ on unclustered |
| 2 | **Query Rewrite** (scan consolidation) | TESTED — Snowflake auto-optimizes | No effect (optimizer already does this) |
| 3 | **Query Rewrite** (ROLLUP decomposition) | TESTED — Snowflake ROLLUP is fast | No effect (native operator efficient) |
| 4 | **Data Clustering** (ALTER TABLE CLUSTER BY) | NOT TESTABLE on SNOWFLAKE_SAMPLE_DATA (read-only) | Expected: massive on unclustered data |
| 5 | **QAS** (Query Acceleration Service) | NOT AVAILABLE on this account tier | Expected: 2-5x on outlier queries |
| 6 | **Snowpark-Optimized Warehouse** | NOT TESTED | 16x memory per node, prevents disk spilling |
| 7 | **APPROX_COUNT_DISTINCT** | N/A for tested queries | 5-10x on COUNT(DISTINCT) queries |
| 8 | **CTE Materialization** (temp tables) | NOT TESTED | Prevents memory explosions on self-joins |

### Account Limitations Discovered

- QAS: `feature 'Query Acceleration Service' not enabled` → likely needs Enterprise+ tier
- Clustering: Can't ALTER tables in SNOWFLAKE_SAMPLE_DATA (read-only)
- Metrics: `INFORMATION_SCHEMA.QUERY_HISTORY()` not returning execution details

## Key Insight for Paper

**Snowflake's optimizer is categorically more sophisticated than DuckDB's for
these patterns.** The techniques that produce 3-5x speedups on DuckDB (scan
consolidation, predicate pushdown, ROLLUP decomposition) are already performed
automatically by Snowflake's optimizer. This means:

1. **Rewrite-only optimization has diminishing returns on mature cloud engines**
2. **The real opportunity on Snowflake is data organization** (clustering,
   materialized views) — not query rewriting
3. **Our system's value on Snowflake is in the P1 pathology** — helping queries
   that timeout due to unclustered data, which can't be fixed by the optimizer alone

## Harness Status

The `prove_q21.py` script is parameterized and ready for any query:
```bash
QT_QUERY=q36 python3 research/snowflake/prove_q21.py
QT_QUERY=q88 python3 research/snowflake/prove_q21.py
QT_QUERY=q21 python3 research/snowflake/prove_q21.py
```

Includes:
- 3-run benchmark protocol (warmup + 2 measure runs)
- MD5 row-level equivalence verification
- EXPLAIN plan capture
- Spill/pruning/compilation metrics (when available)
- Multi-size warehouse testing (XSMALL → MEDIUM)
- Auto-restore to XSMALL on exit (cost protection)
- Result cache disabled (accurate timings)
- Configurable timeout: `QT_TIMEOUT=300` env var
