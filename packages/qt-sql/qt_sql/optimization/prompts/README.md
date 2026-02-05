# SQL Optimization Prompts

This directory contains all prompts, templates, and examples used by the QueryTorque SQL optimization system.

## Directory Structure

```
prompts/
├── README.md                    # This file
│
├── templates/                   # ACTIVE: Templates used by code
│   ├── dag_rewriter.txt         # Main DAG JSON template (from dag_v2.py)
│   ├── full_sql_postgres.txt    # PostgreSQL direct SQL optimization
│   └── full_sql_duckdb.txt      # DuckDB direct SQL optimization
│
├── batch/                       # TPC-DS benchmark prompts (99 queries)
│   ├── manifest.json            # Query metadata and configuration
│   └── q{N}_prompt.txt          # Pre-generated prompts for each query
│
├── experimental/                # DSB/research prompts (useful for exploration)
│   ├── dsb_q1_*.txt             # DSB Q1 variations
│   ├── q1_explore_prompt.txt    # Q1 exploration prompt
│   ├── v5_q1_prompt_fresh.txt   # V5 fresh prompt for Q1
│   └── q2/                      # Q2 multi-worker experiments
│       ├── mode1_retry.txt
│       ├── mode2_worker_*.txt
│       └── mode3_evolutionary_*.txt
│
├── historical/                  # Old versions (reference only)
│   ├── v1/                      # Original DAG v1 archive
│   │   └── dag_v1_optimizer_archive.py
│   ├── v2/                      # V2 prompts
│   │   ├── v2_q1_prompt.txt
│   │   └── v2_system_prompt.txt
│   ├── v5_stubs/                # Broken v5 templates (kept for reference)
│   │   ├── guided_dag.txt       # Placeholder-only (never implemented)
│   │   ├── guided_full_sql.txt
│   │   ├── explore_dag.txt
│   │   └── README.md
│   └── archived/                # Other archived prompts
│       ├── q1_prompt_v2.txt
│       └── q1_prompt_v3.txt
│
└── sample_output/               # Fully populated template examples
    ├── README.md                # Mapping: template → sample → code location
    ├── dag_rewriter_sample.txt  # dag_rewriter.txt fully populated
    ├── full_sql_postgres_sample.txt  # full_sql_postgres.txt fully populated
    └── full_sql_duckdb_sample.txt    # full_sql_duckdb.txt fully populated
```

## Related Directories

The following directories contain data used WITH prompts but are not prompts themselves:

```
optimization/
├── examples/                    # ACTIVE: Verified gold examples (JSON)
│   ├── decorrelate.json         # Q1 (2.92x) - CANONICAL
│   ├── or_to_union.json         # Q15 (2.98x)
│   ├── early_filter.json        # Q93 (2.71x)
│   ├── pushdown.json            # Q39 (2.44x)
│   ├── date_cte_isolate.json    # Q6 (4.00x)
│   ├── materialize_cte.json
│   ├── intersect_to_exists.json
│   ├── union_cte_split.json
│   ├── postgres/                # DB-specific examples
│   │   └── early_filter_decorrelate.json
│   └── unverified/              # Candidate examples (not yet proven)
│
└── constraints/                 # ACTIVE: Learned failure patterns
    ├── or_to_union_limit.json   # Limit OR→UNION to ≤3 branches
    └── literal_preservation.json # Never change literal values
```

## How Templates Work

### DAG Rewriter (dag_rewriter.txt)

Used for node-level DAG transformations. Placeholders:
- `{examples}` - ML-selected gold examples from examples/*.json
- `{transforms}` - Allowed transform list
- `{dag_context}` - DAG node contracts, target nodes, opportunities
- `{execution_plan}` - EXPLAIN ANALYZE output

### Full SQL Templates (full_sql_*.txt)

Used for direct SQL-to-SQL rewriting. Database-specific with full context.

**PostgreSQL placeholders:**
- `{postgres_version}` - Version string
- `{postgres_settings}` - work_mem, effective_cache_size, etc.
- `{schema_ddl}` - CREATE TABLE DDL for referenced tables
- `{table_statistics}` - Row counts, column stats from pg_stats
- `{original_query}` - Input SQL
- `{explain_analyze_output}` - Full EXPLAIN ANALYZE

**DuckDB placeholders:**
- `{duckdb_version}` - Version string
- `{duckdb_settings}` - Memory, thread settings
- `{data_format}` - Parquet/CSV source format
- `{schema_ddl}` - CREATE TABLE DDL
- `{table_statistics}` - Table sizes, cardinalities
- `{sql}` - Input SQL
- `{plan_summary}` - Execution plan analysis

## Example Loading (dag_v3.py)

The ML pipeline selects relevant examples based on detected patterns:

```python
from qt_sql.optimization.dag_v3 import load_all_examples, build_prompt_with_examples

# Load all gold examples
examples = load_all_examples()

# Build prompt with ML-selected examples
prompt = build_prompt_with_examples(
    sql="SELECT ...",
    dag_context="...",
    execution_plan="..."
)
```

## Transform Types

| Transform | Description | Example Speedup |
|-----------|-------------|-----------------|
| decorrelate | Correlated subquery → JOIN | 2.92x |
| pushdown | Predicate pushdown to base tables | 2.44x |
| or_to_union | OR conditions → UNION ALL | 2.98x |
| early_filter | Apply filters earlier in execution | 2.71x |
| date_cte_isolate | Extract date filtering to CTE | 4.00x |
| materialize_cte | Force CTE materialization | varies |
| intersect_to_exists | INTERSECT → EXISTS | varies |
| union_cte_split | Split complex UNIONs into CTEs | varies |

## Code References

- Template loading: `adaptive_rewriter_v5.py:_load_prompt_template()`
- Example loading: `dag_v3.py:load_all_examples()`
- Prompt building: `dag_v3.py:build_prompt_with_examples()`
- PostgreSQL context: `pg_context_builder.py:build_pg_optimization_context()`
