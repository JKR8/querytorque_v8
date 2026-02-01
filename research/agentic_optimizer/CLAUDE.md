# SQL Query Optimizer Agent

You are an agent that optimizes SQL queries for performance.

## Your Task

Given a SQL query, find optimizations that produce **≥2x speedup** while maintaining **identical results**.

---

## Databases

| Database | Path | Purpose |
|----------|------|---------|
| Sample (1%) | `/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb` | Fast iteration (~0.3s/query) |
| Full SF100 | `/mnt/d/TPC-DS/tpcds_sf100.duckdb` | Final validation (~25s/query) |

---

## Workflow

### 1. Analyze the Query

Run EXPLAIN to understand what's expensive:

```python
import duckdb
conn = duckdb.connect("/mnt/d/TPC-DS/tpcds_sf100.duckdb", read_only=True)
conn.execute("PRAGMA enable_profiling='json'")
conn.execute("PRAGMA profiling_output='/tmp/profile.json'")
conn.execute(query).fetchall()
conn.close()

# Read /tmp/profile.json for operator costs
```

Look for:
- Which operators have highest cost %?
- Which tables are scanned without filters?
- Are there repeated scans of the same table?

### 2. Apply an Optimization

Try one of the known patterns (see KNOWLEDGE.md) or discover a new one.

### 3. Test on Sample DB

```python
from test_optimization import test_optimization

result = test_optimization(
    original_sql=original,
    optimized_sql=optimized,
    db_path="/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
)

print(f"Speedup: {result['speedup']:.2f}x")
print(f"Correct: {result['correct']}")
```

### 4. Iterate or Promote

- **If speedup < 2x**: Try a different optimization
- **If semantics wrong**: Fix or revert, try again
- **If speedup ≥ 2x and correct**: Test on full SF100

### 5. Validate on Full DB

```python
result = test_optimization(
    original_sql=original,
    optimized_sql=optimized,
    db_path="/mnt/d/TPC-DS/tpcds_sf100.duckdb"
)
```

---

## Success Criteria

| Metric | Requirement |
|--------|-------------|
| Speedup | ≥2x on full SF100 |
| Semantics | Identical results (row-for-row match) |
| Syntax | Query executes without error |

---

## Files in This Folder

| File | Purpose |
|------|---------|
| `CLAUDE.md` | This file - your instructions |
| `KNOWLEDGE.md` | Optimization patterns that work |
| `test_optimization.py` | Script to test speedup + correctness |
| `queries/` | Input queries to optimize |

---

## Tips

1. **Start with the execution plan** - it shows where time is spent
2. **One change at a time** - easier to debug if it breaks
3. **Test frequently** - sample DB is fast, use it
4. **Read KNOWLEDGE.md** - these patterns are proven to work
5. **Document what you try** - helps avoid repeating failed attempts
