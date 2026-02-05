# Sample Prompt Outputs

This directory contains fully populated examples of each active template,
showing exactly what the LLM receives when processing Q1 (TPC-DS Query 1).

## Template → Sample Mapping

| Template | Sample File | Used By | Code Location |
|----------|-------------|---------|---------------|
| `dag_rewriter.txt` | `dag_rewriter_sample.txt` | DAG v2 pipeline | `dag_v2.py:DagV2PromptBuilder` |
| `full_sql_postgres.txt` | `full_sql_postgres_sample.txt` | Adaptive rewriter v5 | `adaptive_rewriter_v5.py:_build_postgres_prompt()` |
| `full_sql_duckdb.txt` | `full_sql_duckdb_sample.txt` | DuckDB optimization | (future) |

## How to Generate New Samples

### PostgreSQL (adaptive_rewriter_v5.py)

```python
from qt_sql.optimization.adaptive_rewriter_v5 import _build_postgres_prompt

prompt = _build_postgres_prompt(
    sql="SELECT ...",
    sample_db="postgres://user:pass@localhost:5432/db",
    full_explain_plan="EXPLAIN ANALYZE output..."
)
# prompt is now the fully populated template
```

### DAG Rewriter (dag_v2.py)

```python
from qt_sql.optimization.dag_v2 import DagV2PromptBuilder
from qt_sql.optimization.dag_v3 import build_prompt_with_examples

# Build DAG from SQL
dag = QueryDag.from_sql(sql)

# Build prompt with examples
prompt = build_prompt_with_examples(
    sql=sql,
    dag_context=dag.to_prompt_context(),
    execution_plan=explain_output
)
```

## Placeholder Reference

### dag_rewriter.txt
- `{examples}` - ML-selected gold examples (from examples/*.json)
- `{transforms}` - Allowed transform list
- `{dag_context}` - DAG node contracts and opportunities
- `{execution_plan}` - EXPLAIN ANALYZE output

### full_sql_postgres.txt
- `{postgres_version}` - PostgreSQL version string
- `{postgres_settings}` - work_mem, effective_cache_size, etc.
- `{schema_ddl}` - CREATE TABLE DDL for referenced tables
- `{table_statistics}` - Row counts, pg_stats column stats
- `{original_query}` - Input SQL query
- `{explain_analyze_output}` - Full EXPLAIN ANALYZE plan

### full_sql_duckdb.txt
- `{duckdb_version}` - DuckDB version string
- `{duckdb_settings}` - Memory, thread settings
- `{data_format}` - Parquet/CSV source info
- `{schema_ddl}` - CREATE TABLE DDL
- `{table_statistics}` - Table sizes, cardinalities
- `{sql}` - Input SQL query
- `{plan_summary}` - Execution plan analysis
