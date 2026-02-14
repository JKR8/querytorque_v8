# Snowflake Warehouse Downsizing Experiment

## Theory

Standard recommendations (e.g., 2XL for SF10000) assume brute-force scans on unoptimized data.
By combining **query rewrites + data clustering + QAS**, we can run the same workload on
**2-4x smaller warehouses**, saving 50-75% on compute credits.

If proven, this is a repeatable consulting deliverable:
1. Diagnose spilling + pruning failures
2. Apply clustering on date keys
3. Rewrite queries (date CTE isolation)
4. Enable QAS for outlier queries
5. Downsize warehouse

## Business Model

| Before | After | Savings |
|--------|-------|---------|
| X-Large (16 credits/hr) | Medium (4 credits/hr) | 75% |
| Large (8 credits/hr) | Small (2 credits/hr) | 75% |
| Medium (4 credits/hr) | X-Small (1 credit/hr) | 75% |

## Proof of Concept: Q21

**Query 21**: Inventory analysis, 60-day window, joins inventory+warehouse+item+date_dim.

- **Original**: Comma joins, date filter in WHERE clause -> no partition pruning -> TIMEOUT on X-Small
- **Optimized**: Date CTE isolation + explicit JOINs -> runtime partition pruning -> 0.7s on X-Small

## Process (Repeatable)

### Step 1: Check Prerequisites

```bash
cd /mnt/c/Users/jakc9/Documents/QueryTorque_V8
```

### Step 2: Run Diagnostics

```bash
python research/snowflake/setup_clustering.py --check
```

Look for `average_overlap_depth`. If > 5 on fact tables, clustering will help.

### Step 3: Apply Clustering (one-time, costs credits)

```bash
python research/snowflake/setup_clustering.py --apply
```

Wait for `--check` to show `overlap_depth < 5` before proceeding.

### Step 4: Run Single Query Proof

```bash
# Test Q21 on XSMALL, SMALL, MEDIUM with and without QAS
python research/snowflake/runner.py --query q21

# Or specific sizes
python research/snowflake/runner.py --query q21 --sizes XSMALL,MEDIUM

# Without QAS (just rewrite + clustering)
python research/snowflake/runner.py --query q21 --no-qas
```

### Step 5: Review Results

Results saved to `results/q21_YYYYMMDD_HHMMSS.json` with:
- Per-run timings (3x validation protocol)
- Spill bytes (local + remote)
- Partition pruning stats (scanned vs total)
- Row counts for correctness

### Step 6: Scale to All Queries

```bash
# Add more queries to queries/ directory (same naming convention)
# q55_original.sql, q55_optimized.sql, etc.

# Run all at once
python research/snowflake/runner.py --all
```

## Adding New Queries

1. Create `queries/{qid}_original.sql` — standard TPC-DS SQL
2. Create `queries/{qid}_optimized.sql` — rewritten SQL
3. Run: `python research/snowflake/runner.py --query {qid}`

## Test Matrix

Each query is tested across:

| Dimension | Values |
|-----------|--------|
| Warehouse Size | XSMALL, SMALL, MEDIUM |
| QAS | ON, OFF |
| Query Variant | original, optimized |

Total: 3 sizes x 2 QAS x 2 variants = **12 configurations per query**

## Validation Protocol

Per MEMORY.md rules: **3 runs, discard 1st (warmup), average last 2**.
Never use single-run timing comparisons.

## Key Metrics Captured

- **avg_time_s**: Average of measure runs (excludes warmup)
- **mb_spilled_local**: Memory spilled to local SSD (bad)
- **mb_spilled_remote**: Memory spilled to S3 (very bad)
- **partitions_scanned/total**: Pruning efficiency (lower ratio = better)
- **compile_seconds**: Query compilation time
- **queued_seconds**: Time waiting for warehouse resources

## File Structure

```
research/snowflake/
├── README.md              # This file
├── runner.py              # Benchmark runner (main tool)
├── setup_clustering.py    # One-time clustering + QAS setup
├── queries/               # SQL files (original + optimized pairs)
│   ├── q21_original.sql
│   └── q21_optimized.sql
└── results/               # JSON output (auto-generated)
    └── q21_YYYYMMDD_HHMMSS.json
```

## Warehouse Size Reference

| Scale Factor | Data Size | Standard Rec | Downsized Target |
|-------------|-----------|-------------|-----------------|
| SF100 | 100 GB | Small-Medium | X-Small |
| SF1000 | 1 TB | Large-XLarge | Small-Medium |
| SF10000 | 10 TB | 2XL-3XL | Large-XLarge |

## Three Levers for Downsizing

1. **Query Rewrites** (free): Date CTE isolation enables partition pruning
2. **Data Clustering** (one-time cost): Sorts micro-partitions by date_sk
3. **QAS** (per-query cost): Offloads heavy scans to shared compute pool
