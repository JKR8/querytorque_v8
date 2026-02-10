# DAG-Based SQL Optimizer

**Status**: Ready for integration
**Created**: 2026-02-02
**Validated**: TPC-DS Q1 @ SF100 - 2.36x speedup, semantically correct

## Overview

A new optimization approach that parses SQL into a proper DAG (Directed Acyclic Graph) and enables **node-level rewrites** instead of full SQL replacement. This reduces token usage, prevents drift in unchanged code, and makes correlated subqueries visible as first-class optimization targets.

## Why DAG vs Full SQL?

| Aspect | Old (Full SQL) | New (DAG) |
|--------|----------------|-----------|
| Output | Entire rewritten query | `{"rewrites": {"node_id": "SELECT ..."}}` |
| Token usage | Scales with query size | Scales with change size |
| Unchanged code | May drift/change | Preserved exactly |
| Correlated subqueries | Hidden in WHERE clause | Visible as nodes |
| Large queries | Problematic | Handles well |

## Code Location

```
packages/qt-sql/qt_sql/optimization/
├── sql_dag.py              # NEW - DAG parser and rewrite engine
├── dspy_optimizer.py       # Updated - added DAG-based signatures
└── __init__.py             # Updated - exports new components
```

### Key Components

#### `sql_dag.py`

| Class/Function | Description |
|----------------|-------------|
| `SQLDag` | Main class - parses SQL into DAG |
| `DagNode` | Node representing CTE, subquery, or main_query |
| `DagEdge` | Edge showing data flow between nodes |
| `build_dag_prompt()` | Generates LLM prompt with DAG structure |

#### `dspy_optimizer.py` (new additions)

| Class/Function | Description |
|----------------|-------------|
| `SQLDagOptimizer` | DSPy signature for node-level rewrites |
| `SQLDagOptimizerWithFeedback` | Retry signature with failure feedback |
| `DagOptimizationPipeline` | Full pipeline with validation + retries |
| `DagOptimizationResult` | Result dataclass |
| `optimize_with_dag()` | Convenience function |

## Usage

### Basic: Build DAG and Generate Prompt

```python
from qt_sql.optimization import SQLDag, build_dag_prompt

sql = "WITH cte AS (...) SELECT ... FROM cte WHERE ..."
dag = SQLDag.from_sql(sql)

# View structure
print(dag.nodes)  # {'cte': DagNode(...), 'main_query': DagNode(...)}
print(dag.edges)  # [DagEdge(source='cte', target='main_query', ...)]

# Generate prompt for LLM
prompt = build_dag_prompt(sql, plan_summary={"top_operators": [...], "scans": [...]})
```

### Apply Rewrites

```python
# LLM returns JSON like:
rewrites = {
    "customer_total_return": "SELECT ... (new CTE body)",
    "main_query": "SELECT ... (new main query)"
}

# Apply to get new SQL
new_sql = dag.apply_rewrites(rewrites)
```

### Full Pipeline with Validation

```python
from qt_sql.optimization import DagOptimizationPipeline, create_duckdb_validator

validator = create_duckdb_validator("/path/to/db.duckdb")
pipeline = DagOptimizationPipeline(
    validator_fn=validator,
    max_retries=2,
    model_name="deepseek",
    db_name="duckdb"
)

result = pipeline(sql=sql, plan=plan_summary)
print(result.optimized_sql)
print(result.rewrites)  # Which nodes were changed
print(result.correct)   # Validation passed?
```

## Prompt Format

The DAG prompt looks like:

```
Optimize this SQL query by rewriting specific nodes.

## Execution Plan
**Operators by cost:**
- SEQ_SCAN (customer): 73.4% cost, 1,999,335 rows
...

## Query DAG
```
Nodes:
  [customer_total_return] type=cte tables=['store_returns', 'date_dim']
  [main_query] type=main_query tables=['store', 'customer'] refs=['customer_total_return']
  [subquery_1] type=subquery refs=['customer_total_return'] CORRELATED

Edges:
  customer_total_return → subquery_1
  customer_total_return → main_query
```

## Node SQL
### customer_total_return
```sql
SELECT sr_customer_sk AS ctr_customer_sk, ...
```
...

## Output Format
Return JSON:
{
  "rewrites": {"node_id": "SELECT ..."},
  "explanation": "..."
}
```

## Expected LLM Response

```json
{
  "rewrites": {
    "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(SR_FEE) AS ctr_total_return, AVG(SUM(SR_FEE)) OVER (PARTITION BY sr_store_sk) * 1.2 AS ctr_avg_threshold FROM store_returns JOIN date_dim ON sr_returned_date_sk = d_date_sk JOIN store ON sr_store_sk = s_store_sk WHERE d_year = 2000 AND s_state = 'SD' GROUP BY sr_customer_sk, sr_store_sk",
    "main_query": "SELECT c_customer_id FROM customer_total_return AS ctr1 JOIN customer ON ctr1.ctr_customer_sk = c_customer_sk WHERE ctr1.ctr_total_return > ctr1.ctr_avg_threshold ORDER BY c_customer_id LIMIT 100"
  },
  "explanation": "Correlated → Window + Filter pushdown"
}
```

## Validated Results

### TPC-DS Q1 @ SF100

| Metric | Original | Optimized |
|--------|----------|-----------|
| Time | 0.268s | 0.114s |
| Speedup | - | **2.36x** |
| Rows | 100 | 100 |
| Validation | - | ✓ exact match |

**Optimizations applied:**
1. Correlated subquery → window function (`AVG(...) OVER (PARTITION BY ...)`)
2. Filter pushdown (store join + `s_state='SD'` into CTE)

## Node Types

| Type | Description | Example ID |
|------|-------------|------------|
| `cte` | WITH clause CTE | `customer_total_return` |
| `main_query` | Final SELECT | `main_query` |
| `subquery` | Nested subquery | `subquery_1`, `subquery_2` |
| `union_branch` | UNION branch | `main_query.union[0]` |
| `derived_table` | Derived table in FROM | `derived_1` |

## Edge Types

| Type | Description |
|------|-------------|
| `ref` | CTE reference (FROM clause) |
| `correlated` | Correlated reference to outer scope |

## Integration TODO

1. [ ] Add to DSPy test scripts in `research/scripts/`
2. [ ] Create `test_dag_optimizer.py` benchmark script
3. [ ] Update `iterative_optimizer.py` to use DAG mode
4. [ ] Add DAG mode to CLI (`qt-sql optimize --dag`)
5. [ ] Performance comparison: DAG vs full-SQL on all TPC-DS queries

## Files Changed

```
packages/qt-sql/qt_sql/optimization/sql_dag.py       # NEW (520 lines)
packages/qt-sql/qt_sql/optimization/dspy_optimizer.py # +200 lines
packages/qt-sql/qt_sql/optimization/__init__.py      # +15 lines exports
```
