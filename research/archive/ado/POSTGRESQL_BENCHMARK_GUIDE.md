# PostgreSQL DSB Benchmark Guide

## Overview

Benchmarks 53 ADO-optimized PostgreSQL DSB queries against their originals using proper statistical validation.

**Methodology**: 3-run (discard warmup, average last 2)
- Run each query 3 times
- Discard first run (warmup)
- Average the remaining 2 runs

## Scripts

### 1. Preview What Will Run

```bash
python3 /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/preview_postgresql_benchmark.py
```

Shows:
- All 53 queries to be benchmarked
- Transforms applied to each
- Expected runtime
- Benchmark configuration

### 2. Run Full Benchmark

```bash
bash /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/RUN_POSTGRESQL_BENCHMARK.sh
```

This will:
- ✅ Verify PostgreSQL connectivity
- 🔄 Run 3 iterations per query (original + optimized)
- 📊 Calculate 3-run speedups (discard warmup, avg last 2)
- 💾 Save detailed results to JSON

**Expected Duration**: ~1 hour
- 53 queries × 6 runs (3 original + 3 optimized) = 318 query runs
- Timeout: 5 minutes per query

### 3. Manual Run (Advanced)

```bash
python3 /mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/validate_postgresql_dsb.py
```

## Configuration

**Database**: `postgres://jakc9:jakc9@127.0.0.1:5433/dsb_sf10`
- Host: 127.0.0.1
- Port: 5433
- Database: dsb_sf10
- User: jakc9

**Validation Rules**:
- ✅ **WIN**: speedup ≥ 1.3x
- ⚪ **PASS**: speedup 0.95x - 1.3x (neutral, no regression)
- ❌ **REGRESSION**: speedup < 0.95x

## Output

Results saved to:
```
/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/ado/validation_results/postgresql_dsb_validation.json
```

Sample output structure:
```json
{
  "timestamp": "2026-02-06T15:30:45",
  "validation_method": "3-run (discard warmup, avg last 2)",
  "total_queries": 53,
  "results": [
    {
      "query_id": "query001_multi",
      "status": "PASS",
      "classification": "WIN",
      "speedup": 1.45,
      "methodology": "3-run method (discard warmup, avg last 2): 150.23ms → 103.58ms",
      "original_times_ms": [145.2, 149.1, 151.3],
      "optimized_times_ms": [101.2, 103.5, 105.2],
      "row_count": 12500
    }
  ],
  "summary": {
    "wins": 15,
    "passes": 25,
    "regressions": 10,
    "errors": 3,
    "validated": 50
  }
}
```

## Query Distribution

**By Transform Strategy**:
- **date_cte_isolate**: 16 queries (30%)
  - Pre-filter date dimension into CTE
- **early_filter**: 6 queries (11%)
  - Push filters into CTEs early
- **semantic_rewrite**: 3 queries (5%)
  - Complex logic-based rewrites
- **decorrelate**: 2 queries (3%)
  - Convert correlated subqueries to JOINs
- **or_to_union**: 2 queries (3%)
  - Convert OR to UNION ALL
- **Multi-transform**: 14 queries (26%)
  - Combinations of 2+ transforms

**By Query Type**:
- **AGG** (aggregate-focused): ~20 queries
- **MULTI** (complex multi-joins): ~25 queries
- **SPJ_SPJ** (select-project-join): ~8 queries

## Prerequisites

### Required Packages
```bash
pip install psycopg2-binary
```

### PostgreSQL Must Be Running
```bash
# Check if PostgreSQL is responding
psql -h 127.0.0.1 -p 5433 -U jakc9 -d dsb_sf10 -c "SELECT version();"
```

If not running, start PostgreSQL:
```bash
/usr/lib/postgresql/16/bin/pg_ctl -D /mnt/d/pgdata -l /mnt/d/pgdata/logfile start
```

## Interpreting Results

### Win Rate
```
Wins (≥1.3x) = 15/53 = 28%
```
Good result if > 20% of optimizations show meaningful speedup.

### Average Speedup
```
avg = (1.45 + 1.12 + 0.92 + ...) / 50 = 1.08x
```
Overall, queries run 1.08x faster on average.

### Classification Breakdown
- **Regressions** (< 0.95x): Optimizations hurt performance
  - May indicate:
    - Transform not suitable for this query shape
    - PostgreSQL cost model mismatch
    - Query complexity increase
- **Neutral** (0.95-1.3x): No meaningful change
  - Optimization didn't help but didn't hurt
- **Wins** (≥ 1.3x): Clear improvement
  - Good candidates for production use

## Troubleshooting

### "Connection refused"
PostgreSQL not running. Start it:
```bash
/usr/lib/postgresql/16/bin/pg_ctl -D /mnt/d/pgdata start
```

### "Database does not exist"
Check database name and credentials:
```bash
psql -h 127.0.0.1 -p 5433 -l -U jakc9
```

### "Query timeout"
Some queries may exceed 5 minutes. You can:
- Increase timeout in script (change `TIMEOUT_SECS`)
- Run on smaller scale (use `dsb_sf10_sample` for 1% of data)

### "Result mismatch"
Original and optimized queries returned different row counts.
- Check optimized SQL validity
- Verify transforms didn't change query semantics

## Next Steps

1. **Run Preview**: Understand what will be tested
2. **Run Benchmark**: Collect actual speedup data
3. **Analyze Results**: Identify which transforms work best
4. **Compare**: Against DuckDB TPC-DS baseline (88 queries)
5. **Update Gold Examples**: Based on PostgreSQL-specific results

## References

- **Validation Rules**: `/home/jakc9/.claude/projects/-mnt-c-Users-jakc9-Documents-QueryTorque-V8/memory/MEMORY.md`
- **ADO System**: `research/ado/`
- **PostgreSQL Lessons**: Memory notes on cost model, window functions, multi-scan rewrites
