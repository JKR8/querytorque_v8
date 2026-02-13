# Snowflake TPC-DS SF10 Benchmark

## Connection
- **Account**: CVRYJTF-AW47074
- **Warehouse**: COMPUTE_WH (X-Small)
- **Database**: SNOWFLAKE_SAMPLE_DATA.TPCDS_SF10TCL
- **Scale Factor**: 10 (~28.8B store_sales rows)
- **Collected**: 2026-02-13 (result cache disabled, 3-run pattern)

## Summary

| Status | Count | Description |
|--------|-------|-------------|
| Collected | 44 | Full EXPLAIN + baseline timing + operator stats |
| Timeout | 59 | >300s on X-Small — **critical optimization targets** |
| Errors | 0 | All syntax issues fixed |
| **Total** | **103** | 99 standard + 4 split (14a/b, 24a/b, 39a/b) |

57% of TPC-DS queries timeout on X-Small. These are prime targets for SQL rewrites.

## Baseline Timings (44 collected)

| Range | Count | Queries |
|-------|-------|---------|
| Fast (<5s) | 2 | query_41 (0.4s), query_1 (3.1s) |
| Mid (5–60s) | 5 | query_6 (6.7s), query_8 (7.4s), query_98 (13.3s), query_3 (19.2s), query_7 (58.1s) |
| Slow (60–300s) | 37 | query_60 through query_46 (81s–209s) |

- **Min**: 413ms (query_41)
- **Max**: 208,675ms (query_46)
- **Median**: 145s
- **Mean**: 127s

### Full Timing Table

| Query | Baseline (s) | Rows | Operators |
|-------|-------------|------|-----------|
| query_41 | 0.4 | 100 | 14 |
| query_1 | 3.1 | 100 | 27 |
| query_6 | 6.7 | 52 | 37 |
| query_8 | 7.4 | 11 | 36 |
| query_98 | 13.3 | 60,471 | 17 |
| query_3 | 19.2 | 100 | 16 |
| query_7 | 58.1 | 100 | 22 |
| query_60 | 81.2 | 100 | 70 |
| query_58 | 87.1 | 0 | 59 |
| query_56 | 90.8 | 100 | 70 |
| query_42 | 95.1 | 13 | 15 |
| query_45 | 95.5 | 100 | 26 |
| query_2 | 95.6 | 2,513 | 28 |
| query_18 | 124.4 | 100 | 30 |
| query_90 | 126.6 | 1 | 20 |
| query_15 | 129.3 | 100 | 18 |
| query_86 | 131.7 | 100 | 16 |
| query_30 | 134.5 | 100 | 29 |
| query_19 | 138.0 | 100 | 29 |
| query_99 | 138.6 | 100 | 20 |
| query_96 | 139.6 | 1 | 19 |
| query_33 | 142.7 | 100 | 73 |
| query_22 | 148.0 | 100 | 14 |
| query_84 | 149.0 | 100 | 21 |
| query_40 | 149.1 | 100 | 18 |
| query_82 | 149.8 | 32 | 22 |
| query_91 | 150.8 | 54 | 34 |
| query_92 | 151.5 | 1 | 25 |
| query_83 | 157.2 | 100 | 65 |
| query_39a | 157.4 | 16,948 | 23 |
| query_39b | 157.5 | 417 | 23 |
| query_43 | 157.7 | 100 | 16 |
| query_37 | 158.6 | 8 | 22 |
| query_68 | 160.6 | 100 | 31 |
| query_73 | 161.7 | 2,467 | 25 |
| query_20 | 164.1 | 100 | 17 |
| query_13 | 181.8 | 1 | 26 |
| query_26 | 182.0 | 100 | 21 |
| query_27 | 185.6 | 100 | 23 |
| query_34 | 194.7 | 408,443 | 25 |
| query_62 | 196.8 | 100 | 20 |
| query_52 | 197.3 | 100 | 15 |
| query_63 | 207.5 | 100 | 21 |
| query_46 | 208.7 | 100 | 28 |

## Timeout Queries (59) — Critical Optimization Targets

These queries exceed 300s on X-Small warehouse. They have EXPLAIN plans (estimated)
but no actual execution stats or baselines. The optimization goal: rewrite SQL so
these complete on X-Small, or significantly reduce runtime on a larger warehouse.

```
query_4    query_5    query_9    query_10   query_11   query_12
query_14a  query_14b  query_16   query_17   query_21   query_23a
query_23b  query_24a  query_24b  query_25   query_28   query_29
query_31   query_32   query_35   query_36   query_38   query_44
query_47   query_48   query_49   query_50   query_51   query_53
query_54   query_55   query_57   query_59   query_61   query_64
query_65   query_66   query_67   query_69   query_70   query_71
query_72   query_74   query_75   query_76   query_77   query_78
query_79   query_80   query_81   query_85   query_87   query_88
query_89   query_93   query_94   query_95   query_97
```

## Directory Structure

```
snowflake_tpcds/
  config.json           # DSN, engine, validation settings
  README.md             # This file
  queries/              # 103 TPC-DS .sql files (Snowflake-adapted)
  explains/             # 103 .json files — EXPLAIN + baselines + operator stats
  knowledge/            # Snowflake-specific optimization knowledge
  prepared/             # Deterministic prompt snapshots (qt prepare)
  swarm_sessions/       # Swarm run artifacts (analyst + worker outputs)
  learning/             # Per-query learning records from optimization attempts
  runs/                 # Run metadata
```

## Data per Query (explains/*.json)

Each explain file contains:

| Field | Description |
|-------|-------------|
| `execution_time_ms` | Baseline timing (avg of 2 measures, cache disabled). null for timeouts. |
| `row_count` | Result set size |
| `plan_text` | Rendered EXPLAIN (operator tree with partitions, bytes) |
| `plan_json` | Structured plan from SYSTEM$EXPLAIN_PLAN_JSON (GlobalStats + Operations) |
| `operator_stats` | Actual per-operator stats from GET_QUERY_OPERATOR_STATS (rows, partitions, bytes, spill, timing) |
| `operator_stats_text` | Compact text rendering of operator stats for prompt embedding |
| `error` | "TIMEOUT" for timeout queries, null for collected |

## Snowflake SQL Adaptations

TPC-DS standard SQL required these Snowflake-specific fixes:

1. **INTERVAL syntax**: `INTERVAL 14 DAY` → `INTERVAL '14 DAY'` (15 queries)
2. **Multi-statement splits**: queries 14, 23, 24, 39 split into a/b variants (matching TPC-DS convention)
3. **Column name**: `c_last_review_date_sk` → `c_last_review_date` in query_30 (Snowflake sample data uses date value, not surrogate key)

## Collection Method

```bash
# Collect all (skips already-collected, resumable)
qt collect-explains snowflake_tpcds --parallel 10 --timeout 300

# Re-collect specific queries
qt collect-explains snowflake_tpcds -q query_1 -q query_2 --force

# Higher parallelism (Snowflake handles it)
qt collect-explains snowflake_tpcds --parallel 20 --timeout 300
```

Each query collection runs: EXPLAIN → SYSTEM$EXPLAIN_PLAN_JSON → warmup + 2 measures
→ GET_QUERY_OPERATOR_STATS. Result caching disabled via ALTER SESSION.
