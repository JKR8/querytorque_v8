# TPC-DS Queries for Optimization

These queries are from TPC-DS SF100 converted for DuckDB.

## Usage

```python
# Read a query
with open("queries/q1.sql") as f:
    original_sql = f.read()

# Profile it
import duckdb
conn = duckdb.connect("/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb", read_only=True)
conn.execute("PRAGMA enable_profiling='json'")
conn.execute("PRAGMA profiling_output='/tmp/profile.json'")
conn.execute(original_sql).fetchall()
conn.close()

# Read profile
import json
with open('/tmp/profile.json') as f:
    plan = json.load(f)
```

## Full Query Set

All 99 TPC-DS queries are at: `/mnt/d/TPC-DS/queries_duckdb_converted/`

Copy more here as needed:
```bash
cp /mnt/d/TPC-DS/queries_duckdb_converted/query_N.sql queries/qN.sql
```
