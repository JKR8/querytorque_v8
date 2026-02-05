# DSB Query Optimization - Input Collection Guide

This document specifies exactly what inputs to provide to the LLM optimizer and the SQL commands to gather them.

---

## Required Inputs Checklist

| Input | Required | Format | Purpose |
|-------|----------|--------|---------|
| PostgreSQL Version | ✓ | Text | Determines available features (CTE inlining, etc.) |
| Configuration Settings | ✓ | Key-value | Explains optimizer decisions |
| Schema DDL | ✓ | SQL | Enables join/predicate reasoning |
| Table Statistics | ✓ | Structured text | Explains cardinality estimates |
| Original Query | ✓ | SQL | The optimization target |
| Execution Plan | ✓ | EXPLAIN ANALYZE output | Identifies bottlenecks |
| Extended Statistics | Optional | SQL | Shows existing correlation handling |
| Query Intent | Optional | Natural language | Helps verify semantic equivalence |

---

## 1. PostgreSQL Version & Configuration

### Gather Version
```sql
SELECT version();
```

### Gather Relevant Settings
```sql
SELECT name, setting, unit 
FROM pg_settings 
WHERE name IN (
    'work_mem',
    'shared_buffers', 
    'effective_cache_size',
    'random_page_cost',
    'seq_page_cost',
    'cpu_tuple_cost',
    'cpu_index_tuple_cost',
    'cpu_operator_cost',
    'parallel_tuple_cost',
    'parallel_setup_cost',
    'min_parallel_table_scan_size',
    'min_parallel_index_scan_size',
    'max_parallel_workers_per_gather',
    'join_collapse_limit',
    'from_collapse_limit',
    'geqo',
    'geqo_threshold',
    'geqo_effort',
    'enable_hashjoin',
    'enable_mergejoin',
    'enable_nestloop',
    'enable_seqscan',
    'enable_indexscan',
    'enable_bitmapscan',
    'enable_sort',
    'enable_hashagg',
    'default_statistics_target',
    'jit'
)
ORDER BY name;
```

**Example Output Format:**
```
PostgreSQL 15.4 on x86_64-pc-linux-gnu

work_mem = 256MB
effective_cache_size = 24GB
random_page_cost = 1.1
join_collapse_limit = 8
from_collapse_limit = 8
geqo_threshold = 12
default_statistics_target = 100
max_parallel_workers_per_gather = 4
```

---

## 2. Schema DDL

### Option A: Extract from Database
```sql
-- For each table in the query, run:
\d+ table_name

-- Or use pg_dump for clean DDL:
pg_dump -s -t 'schema.table_pattern' dbname
```

### Option B: Generate DDL Script
```sql
-- Tables with columns and constraints
SELECT 
    'CREATE TABLE ' || schemaname || '.' || tablename || ' (' ||
    string_agg(
        column_name || ' ' || data_type || 
        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
        ', '
    ) || ');'
FROM information_schema.columns c
JOIN pg_tables t ON c.table_name = t.tablename AND c.table_schema = t.schemaname
WHERE t.tablename IN ('table1', 'table2', ...)  -- Tables from your query
GROUP BY schemaname, tablename;
```

### Required Schema Elements

```sql
-- 1. Table definitions with types
CREATE TABLE store_sales (
    ss_sold_date_sk         integer,
    ss_sold_time_sk         integer,
    ss_item_sk              integer NOT NULL,
    ss_customer_sk          integer,
    ss_store_sk             integer,
    ss_quantity             integer,
    ss_sales_price          decimal(7,2),
    ss_net_profit           decimal(7,2)
);

-- 2. Primary keys
ALTER TABLE store_sales ADD PRIMARY KEY (ss_item_sk, ss_sold_date_sk);

-- 3. Foreign keys (critical for join semantics)
ALTER TABLE store_sales ADD FOREIGN KEY (ss_sold_date_sk) 
    REFERENCES date_dim(d_date_sk);
ALTER TABLE store_sales ADD FOREIGN KEY (ss_store_sk) 
    REFERENCES store(s_store_sk);

-- 4. Indexes
CREATE INDEX ss_date_idx ON store_sales(ss_sold_date_sk);
CREATE INDEX ss_store_idx ON store_sales(ss_store_sk);
CREATE INDEX ss_customer_idx ON store_sales(ss_customer_sk);
```

---

## 3. Table Statistics

### Basic Table Stats
```sql
SELECT 
    schemaname,
    relname as table_name,
    n_live_tup as row_count,
    n_dead_tup as dead_rows,
    pg_size_pretty(pg_relation_size(relid)) as table_size,
    pg_size_pretty(pg_indexes_size(relid)) as index_size,
    last_analyze,
    last_autoanalyze
FROM pg_stat_user_tables
WHERE relname IN ('table1', 'table2', ...);  -- Tables from your query
```

### Column Statistics
```sql
SELECT 
    tablename,
    attname as column_name,
    n_distinct,
    null_frac,
    avg_width,
    correlation,
    most_common_vals::text as mcv,
    most_common_freqs::text as mcf,
    histogram_bounds::text as histogram
FROM pg_stats
WHERE tablename IN ('table1', 'table2', ...)  -- Tables from your query
  AND attname IN ('col1', 'col2', ...);       -- Key columns (join keys, filter columns)
```

### Extended Statistics
```sql
SELECT 
    stxname as stat_name,
    stxnamespace::regnamespace as schema,
    stxrelid::regclass as table_name,
    stxkeys,
    stxkind,  -- 'd'=dependencies, 'f'=functional deps, 'm'=mcv, 'n'=ndistinct
    stxstattarget
FROM pg_statistic_ext
WHERE stxrelid IN ('table1'::regclass, 'table2'::regclass, ...);
```

**Example Output Format:**
```
Table: store_sales
  Row count: 287,997,024
  Table size: 34 GB
  Index size: 12 GB
  Last analyzed: 2024-01-15 03:00:00
  
  Column statistics:
    - ss_sold_date_sk: ndistinct=1823, null_frac=0.0, correlation=0.98
    - ss_store_sk: ndistinct=402, null_frac=0.001, correlation=0.02
    - ss_customer_sk: ndistinct=2,000,000, null_frac=0.05, correlation=0.001
    - ss_sales_price: avg=52.34, null_frac=0.0
    
Table: date_dim
  Row count: 73,049
  
  Column statistics:
    - d_date_sk: ndistinct=-1 (unique), null_frac=0.0, correlation=1.0
    - d_year: ndistinct=6, null_frac=0.0, mcv=[2019,2020,2021,2022,2023,2024]
    - d_moy: ndistinct=12, null_frac=0.0

Extended statistics:
  - date_dim_year_qtr: type=dependencies, columns=(d_year, d_qoy)
```

---

## 4. Execution Plan

### Generate Full Plan
```sql
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, TIMING, FORMAT TEXT)
<your_query_here>;
```

### For Long-Running Queries (estimate only)
```sql
-- First get estimate without execution:
EXPLAIN (COSTS, VERBOSE, FORMAT TEXT)
<your_query_here>;

-- If acceptable, run with ANALYZE:
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, TIMING, FORMAT TEXT)
<your_query_here>;
```

### Key Elements to Capture

```
                                    QUERY PLAN
----------------------------------------------------------------------------------
Hash Join  (cost=1234.56..5678.90 rows=1000 width=100) 
           (actual time=10.5..250.3 rows=15234 loops=1)
           ↑                          ↑         ↑        ↑
           |                          |         |        └── Loops (critical for correlated)
           |                          |         └── Actual rows (compare to estimate)
           |                          └── Estimated rows
           └── Cost estimate
           
  Hash Cond: (a.id = b.aid)
  Buffers: shared hit=1234 read=567    ← I/O patterns
  ->  Seq Scan on large_table a (...)
        Filter: (region = 'West')
        Rows Removed by Filter: 500000  ← Selectivity info
  ->  Hash (...)
        Buckets: 16384  Batches: 1  Memory Usage: 1234kB  ← Memory pressure
        ->  Index Scan on ...
        
Planning Time: 5.2 ms
Execution Time: 255.7 ms                ← Total time
```

---

## 5. Complete Input Template

Copy and fill in this template:

```markdown
## PostgreSQL Environment

**Version:** 
PostgreSQL X.Y on platform

**Configuration:**
```
work_mem = 
effective_cache_size = 
random_page_cost = 
join_collapse_limit = 
from_collapse_limit = 
geqo_threshold = 
default_statistics_target = 
```

## Schema

```sql
-- Paste DDL here
CREATE TABLE ...
```

## Statistics

```
Table: table_name
  Row count: 
  Column statistics:
    - column: ndistinct=, null_frac=, correlation=
```

## Query to Optimize

```sql
-- Paste query here
SELECT ...
```

## Current Execution Plan

```
-- Paste EXPLAIN ANALYZE output here
```

## Additional Context (Optional)

**Query intent:** What business question does this answer?

**Performance requirement:** Target execution time or current pain point

**Constraints:** Any rewrites to avoid (e.g., must preserve CTE for readability)
```

---

## 6. Quick Collection Script

Save and run this script to gather all inputs automatically:

```bash
#!/bin/bash
# collect_query_inputs.sh

DB_NAME="your_database"
QUERY_FILE="query.sql"
OUTPUT_FILE="optimization_input.md"

# Tables extracted from query (update this list)
TABLES="store_sales,date_dim,store,customer"

psql -d $DB_NAME << EOF > $OUTPUT_FILE
\echo '## PostgreSQL Environment'
\echo ''
\echo '**Version:**'
SELECT version();

\echo ''
\echo '**Configuration:**'
\echo '\`\`\`'
SELECT name || ' = ' || setting FROM pg_settings 
WHERE name IN ('work_mem','effective_cache_size','random_page_cost',
               'join_collapse_limit','from_collapse_limit','geqo_threshold',
               'default_statistics_target');
\echo '\`\`\`'

\echo ''
\echo '## Table Statistics'
\echo ''
SELECT 'Table: ' || relname || E'\n  Row count: ' || n_live_tup
FROM pg_stat_user_tables WHERE relname IN ($(echo $TABLES | sed "s/,/','/g" | sed "s/^/'/" | sed "s/$/'/"));

\echo ''
\echo '## Execution Plan'
\echo ''
\echo '\`\`\`'
\i $QUERY_FILE
EXPLAIN (ANALYZE, BUFFERS, COSTS, VERBOSE, FORMAT TEXT)
\i $QUERY_FILE
\echo '\`\`\`'
EOF

echo "Inputs collected to $OUTPUT_FILE"
```

---

## 7. DSB-Specific Considerations

For DSB benchmark queries, also capture:

### Scale Factor
```sql
-- Infer from row counts
SELECT 
    (SELECT COUNT(*) FROM store_sales) / 2879970.24 as scale_factor;
```

### Data Skew Indicators
```sql
-- Check for DSB's exponential distributions
SELECT 
    tablename, attname,
    most_common_freqs[1] as top_freq,  -- High = skewed
    n_distinct
FROM pg_stats 
WHERE tablename IN ('store_sales', 'catalog_sales', 'web_sales')
  AND attname LIKE '%_sk';
```

### Query Template Number
DSB queries are numbered (e.g., dsb_q01, dsb_q17). Include this if known, as certain templates have known optimization patterns.
