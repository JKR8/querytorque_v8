# V5 Optimization Prompts (ARCHIVED)

> **Note:** These are ARCHIVED stub templates that were never fully implemented.
> The `guided_dag.txt` and `guided_full_sql.txt` contain only placeholders (`{examples}`, `{dag_context}`)
> without the actual prompt content. They are kept here for reference only.
>
> **Active templates are in:** `prompts/templates/`
> - `dag_rewriter.txt` - Main DAG JSON template (extracted from dag_v2.py)
> - `full_sql_postgres.txt` - PostgreSQL direct SQL (was explore_full_sql_postgres.txt)
> - `full_sql_duckdb.txt` - DuckDB direct SQL (was explore_full_sql_duckdb.txt)

## Original Prompt Matrix (Historical Reference)

| Mode | Examples | Output Format | Use Case |
|------|----------|---------------|----------|
| `guided_dag.txt` | 3x ML gold | DAG JSON | Large queries (150+ lines), multi-CTE |
| `guided_full_sql.txt` | 3x ML gold | Full SQL | Small-medium queries, simple structure |
| `explore_dag.txt` | None | DAG JSON | Discovery mode, large queries |
| `explore_full_sql_postgres.txt` | None | Full SQL | PostgreSQL with full schema/stats context |
| `explore_full_sql_duckdb.txt` | None | Full SQL | DuckDB pattern discovery |

## Template Variables

### Guided Prompts
- `{examples}` - ML-recommended gold examples with verified speedups
- `{sql}` - Original SQL query
- `{execution_plan}` - EXPLAIN ANALYZE output
- `{dag_context}` - DAG node contracts, target nodes, detected opportunities

### PostgreSQL Explore Prompt
The PostgreSQL explore prompt uses `pg_context_builder.py` to dynamically collect:
- `{postgres_version}` - PostgreSQL version string
- `{postgres_settings}` - Optimization-relevant settings (work_mem, effective_cache_size, etc.)
- `{schema_ddl}` - CREATE TABLE DDL for tables referenced in query
- `{table_statistics}` - Row counts and column stats (ndistinct, null_frac) from pg_stats
- `{original_query}` - Input SQL query
- `{explain_analyze_output}` - Full EXPLAIN ANALYZE output

## Mode Selection Logic

```
if query_lines > 150 or cte_count > 3:
    output_format = "dag"
else:
    output_format = "full_sql"

if have_verified_examples_for_db:
    mode = "guided"
else:
    mode = "explore"

prompt = f"{mode}_{output_format}_{db_type}.txt"
```

## Database-Specific Explore Prompts

### PostgreSQL (`explore_full_sql_postgres.txt`)

**Full-context prompt** with 9 rewrite patterns (A-I):

1. **Pattern A**: Correlated Subquery → JOIN (100-3000x)
2. **Pattern B**: NOT IN → NOT EXISTS (anti-join)
3. **Pattern C**: IN Subquery → EXISTS or JOIN
4. **Pattern D**: Correlated Comparison → Window Function
5. **Pattern E**: CTE Materialization Control (PG12+)
6. **Pattern F**: Join Order Optimization
7. **Pattern G**: UNION ALL → Single Scan with CASE
8. **Pattern H**: DISTINCT Elimination
9. **Pattern I**: Predicate Pushdown Through Outer Joins

**Context collection** (via `pg_context_builder.py`):
- Parses SQL to extract base tables using `sql_parser`
- Queries `pg_settings` for optimization-relevant configuration
- Generates DDL for query-relevant tables only
- Fetches column stats from `pg_stats` (ndistinct, null_frac, correlation)

### DuckDB (`explore_full_sql_duckdb.txt`)
4-phase protocol covering:
1. Columnar Scan Optimization
2. Join Optimization (Hash Join strengths)
3. Aggregation Optimization
4. DuckDB-Specific Features (CTE materialization, parallel execution, ASOF joins)

## Adding New Database Support

1. Create `explore_full_sql_{db}.txt` with DB-specific optimization protocol
2. Run exploration on benchmark queries
3. Collect verified speedups
4. Create `examples/{db}/` with gold examples
5. Update ML recommender to select DB-specific examples
