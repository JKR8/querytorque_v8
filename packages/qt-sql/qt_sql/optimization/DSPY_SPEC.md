# DSPy Prompt System Specification

## Overview

The DSPy optimizer uses structured prompts to request SQL optimizations from LLMs. This document specifies the exact format and validation requirements for the DSPy prompt system used in QueryTorque V8.

**Version**: v5 (DAG-based with example batching)
**Last Updated**: 2026-02-04

---

## Architecture

```
Input SQL → DagBuilder → QueryDag
                            ↓
            ┌──────────────────────────────┐
            │ Prompt Components            │
            ├──────────────────────────────┤
            │ 1. query_dag (structure)     │ ← build_dag_structure_string()
            │ 2. node_sql (code)           │ ← build_node_sql_string()
            │ 3. execution_plan (analysis) │ ← analyze_plan_for_optimization()
            │ 4. optimization_hints (KB)   │ ← detect_knowledge_patterns()
            │ 5. constraints (rules)       │ ← (optional)
            └──────────────────────────────┘
                            ↓
            DSPy Signature (SQLDagOptimizer)
                            ↓
            LLM generates rewrites JSON
                            ↓
            DagV2Pipeline.apply_response()
                            ↓
            Optimized SQL
```

---

## DSPy Signature Definition

**File**: `packages/qt-sql/qt_sql/optimization/dspy_optimizer.py`

```python
class SQLDagOptimizer(dspy.Signature):
    """Optimize SQL by rewriting individual DAG nodes."""

    # INPUTS (from prompt builders)
    query_dag: str = dspy.InputField(
        desc="DAG structure showing query decomposition"
    )
    node_sql: str = dspy.InputField(
        desc="SQL for each node (CTEs and main query)"
    )
    execution_plan: str = dspy.InputField(
        desc="EXPLAIN ANALYZE summary with costs"
    )
    optimization_hints: str = dspy.InputField(
        desc="Knowledge base patterns detected"
    )
    constraints: str = dspy.InputField(
        desc="Constraints and requirements"
    )

    # OUTPUTS (to DagV2Pipeline)
    rewrites: str = dspy.OutputField(
        desc="JSON map of node_id -> optimized SQL"
    )
    explanation: str = dspy.OutputField(
        desc="Rationale for optimizations"
    )
```

---

## Prompt Component Specifications

### 1. query_dag (DAG Structure)

**Builder**: `build_dag_structure_string(dag: QueryDag) -> str`
**File**: `packages/qt-sql/qt_sql/optimization/dag_prompts.py`

**Format**:
```
Nodes:
  [node_id] type=<cte|main> tables=[t1,t2] refs=[ref1] CORRELATED

Edges:
  src → dst
```

**Requirements**:
- ✅ Nodes MUST be in topological order (dependencies first)
- ✅ Each node MUST specify `type` (cte or main)
- ✅ `tables` list MUST include all base tables referenced
- ✅ `refs` list MUST include all CTE references
- ✅ `CORRELATED` flag MUST be present if node contains correlated subquery
- ✅ Edges MUST use arrow notation (`→`)

**Example**:
```
Nodes:
  [cte1] type=cte tables=['store_sales','date_dim']
  [cte2] type=cte tables=['item'] refs=['cte1'] CORRELATED
  [main_query] type=main refs=['cte1','cte2']

Edges:
  cte1 → cte2
  cte1 → main_query
  cte2 → main_query
```

**Validation**: Test in `test_dag_prompts.py::test_build_dag_structure_string`

---

### 2. node_sql (Node SQL Code)

**Builder**: `build_node_sql_string(dag: QueryDag) -> str`
**File**: `packages/qt-sql/qt_sql/optimization/dag_prompts.py`

**Format**:
```
### node_id
```sql
SELECT ...
```
```

**Requirements**:
- ✅ Nodes MUST be in topological order (same as query_dag)
- ✅ Each node MUST use `### node_id` header (markdown H3)
- ✅ SQL MUST be in triple-backtick code block with `sql` language tag
- ✅ SQL MUST be stripped of leading/trailing whitespace
- ✅ Nodes without SQL (e.g., references only) MUST be omitted

**Example**:
```
### cte1
```sql
SELECT ss_item_sk, SUM(ss_sales_price) as revenue
FROM store_sales
JOIN date_dim ON ss_sold_date_sk = d_date_sk
WHERE d_year = 2000
GROUP BY ss_item_sk
```

### main_query
```sql
WITH cte1 AS (...) SELECT * FROM cte1
```
```

**Validation**: Test in `test_dag_prompts.py::test_build_node_sql_string`

---

### 3. execution_plan (EXPLAIN Analysis)

**Builder**: `_format_plan_summary(ctx: OptimizationContext) -> str`
**File**: `packages/qt-sql/qt_sql/optimization/adaptive_rewriter_v5.py`

**Format**:
```
Table Scans: store_sales(x3), date_dim(x2), item(x1)
Expensive Ops: Hash Join (cost=5000), Aggregate (cost=2000)
Filters: d_year=2000, ss_sales_price>100
```

**Requirements**:
- ✅ Deduplicate table scans and show count
- ✅ List operators with cost > threshold
- ✅ Extract filter predicates
- ✅ Highlight nested loops and sorts

**Validation**: Covered by `analyze_plan_for_optimization()` tests

---

### 4. optimization_hints (Knowledge Base)

**Builder**: `detect_knowledge_patterns(sql: str, dag: QueryDag) -> str`
**File**: `packages/qt-sql/qt_sql/optimization/dspy_optimizer.py`

**Format**:
```
Detected Patterns:
- CTE Materialization: cte1, cte2 (used multiple times)
- Subquery Pullup: Scalar subquery in WHERE clause
- Predicate Pushdown: Filter on d_year can push to CTE

Relevant Rules:
- Rule #47: Materialize CTEs used 2+ times
- Rule #12: Push filters before joins
```

**Requirements**:
- ✅ Pattern names MUST match knowledge base taxonomy
- ✅ Affected nodes MUST be listed
- ✅ Rule numbers MUST reference actual KB rules

**Validation**: Covered by knowledge_base tests

---

### 5. constraints (Optional)

**Format**: Free-form text

**Common Constraints**:
- Preserve result ordering
- Maintain all GROUP BY columns
- Keep LIMIT/OFFSET unchanged
- No modification to UNION/INTERSECT

---

## LLM Response Format

### rewrites (Required)

**Format**: JSON object mapping node IDs to optimized SQL

**Schema**:
```json
{
  "node_id": "optimized SQL string",
  "node_id_2": "optimized SQL string"
}
```

**Requirements**:
- ✅ MUST be valid JSON (parseable by `json.loads()`)
- ✅ Keys MUST be existing node IDs from query_dag
- ✅ Values MUST be syntactically valid SQL
- ✅ Empty object `{}` is valid (no optimizations)
- ✅ Partial rewrites allowed (only modified nodes)

**Example**:
```json
{
  "cte1": "SELECT ss_item_sk, SUM(ss_sales_price) as revenue FROM store_sales WHERE ss_sold_date_sk IN (SELECT d_date_sk FROM date_dim WHERE d_year = 2000) GROUP BY ss_item_sk",
  "main_query": "WITH cte1 AS (...) SELECT * FROM cte1 WHERE revenue > 1000"
}
```

**Validation**: `DagV2Pipeline.apply_response()` parses JSON and applies rewrites

---

### explanation (Required)

**Format**: Free-form text explaining optimizations

**Requirements**:
- ✅ MUST reference specific node IDs
- ✅ SHOULD cite rule numbers or pattern names
- ✅ SHOULD explain why optimization improves performance

**Example**:
```
Applied Rule #12 (Predicate Pushdown) to cte1:
Pushed d_year=2000 filter into subquery to reduce join cardinality.
This eliminates 99% of date_dim rows before the expensive hash join.
```

---

## Example Batching (V5 Innovation)

### Worker Configuration

**Workers 1-4**: Coverage workers (each gets different example batch)
- Batch 1: Gold examples 1-3 (highest KB match scores)
- Batch 2: Gold examples 4-6
- Batch 3: Gold examples 7-9
- Batch 4: Gold examples 10-12

**Worker 5**: Explore worker
- No examples (empty batch `[]`)
- Extended hints with full EXPLAIN plan
- "Be adversarial" instruction

### Example Format

**Source**: `packages/qt-sql/qt_sql/optimization/dag_v3.py`

Each `GoldExample` contains:
```python
{
  "input_slice": "DAG structure snippet",
  "opportunity": "Pattern description",
  "output": {"node_id": "optimized SQL"},
  "key_insight": "Why this works"
}
```

**Conversion to DSPy Demo**:
```python
demo = dspy.Example(
    query_dag=ex.example.get("input_slice", ""),
    node_sql="",  # Not needed for demos
    execution_plan="",
    optimization_hints=ex.example.get("opportunity", ""),
    constraints="",
    rewrites=json.dumps(ex.example.get("output", {})),
    explanation=ex.example.get("key_insight", "")
).with_inputs(...)
```

---

## Validation Pipeline

### 1. Prompt Building Validation

**Test**: `test_dag_prompts.py`

```bash
pytest packages/qt-sql/tests/test_dag_prompts.py -v
```

**Checks**:
- ✅ Topological order is correct
- ✅ DAG structure format matches spec
- ✅ Node SQL format matches spec
- ✅ All node IDs are present

### 2. Response Parsing Validation

**Function**: `DagV2Pipeline.apply_response()`

**Checks**:
- ✅ `rewrites` is valid JSON
- ✅ All keys are valid node IDs
- ✅ All SQL values parse without syntax errors

### 3. Semantic Validation

**Function**: `SQLValidator.validate()`

**Checks**:
- ✅ Optimized SQL runs without errors
- ✅ Row count matches original
- ✅ Column names/types match original
- ✅ Result ordering preserved (if required)

---

## Inspector Tool

**Script**: `scripts/inspect_dspy_call.py`

**Usage**:
```bash
export DEEPSEEK_API_KEY=$(cat DeepseekV3.txt)
python scripts/inspect_dspy_call.py <query_file.sql>
```

**Output**:
- ✅ Shows all 5 input components
- ✅ Shows LLM response (rewrites + explanation)
- ✅ Validates JSON format
- ✅ Reports parsing errors

---

## Implementation Checklist

### Required Modules

- ✅ `dag_v2.py` - DAG structure (QueryDag, DagNode, DagBuilder)
- ✅ `dag_v3.py` - Gold examples (GoldExample, get_matching_examples)
- ✅ `dag_prompts.py` - Prompt builders (NEW)
- ✅ `dspy_optimizer.py` - DSPy signatures and LM config
- ✅ `adaptive_rewriter_v5.py` - Main optimizer with example batching

### Required Tests

- ✅ `test_dag_prompts.py` - Prompt builder unit tests
- ✅ `test_dag_v2.py` - DAG builder tests
- ✅ `test_adaptive_rewriter_v5.py` - Integration tests

### Required Scripts

- ✅ `scripts/inspect_dspy_call.py` - DSPy call inspector
- ✅ `scripts/benchmark_v5_correct.py` - Full benchmark runner

---

## Common Issues and Fixes

### Issue: ModuleNotFoundError: sql_dag

**Cause**: Old code importing removed `sql_dag.py`
**Fix**: Replace with `DagBuilder` from `dag_v2.py`

```python
# OLD (WRONG)
from qt_sql.optimization.sql_dag import SQLDag
dag = SQLDag.from_sql(sql)

# NEW (CORRECT)
from qt_sql.optimization.dag_v2 import DagBuilder
dag = DagBuilder(sql).build()
```

### Issue: JSON decode error in rewrites

**Cause**: LLM returned non-JSON text
**Fix**: Check with inspector script, add retry with feedback

```bash
python scripts/inspect_dspy_call.py query.sql
# Shows exact LLM response
```

### Issue: All workers get same examples

**Cause**: Examples loaded inside worker instead of batched outside
**Fix**: Use example batching in `optimize_v5_dspy`

```python
# Get examples and split into batches
examples = get_matching_examples(sql)
batches = _split_example_batches(examples, batch_size=3)

# Pass different batch to each worker
for i, batch in enumerate(batches[:4]):
    pool.submit(_worker_dspy, i+1, sql, ..., batch, ...)
```

---

## Performance Characteristics

### Expected API Calls per Query

- **v5 DSPy**: 5 parallel calls (1 per worker)
- **v5 JSON**: 5 parallel calls (1 per worker)
- **Retry on failure**: +1 call with feedback

### Expected Success Rate

- **Validation Pass**: 60-80% (varies by query complexity)
- **JSON Parse**: 95%+ (DSPy structured output is reliable)
- **Speedup ≥1.2x**: 40-60% of passing queries

### Timing Budget

- **Sample DB validation**: 1-5 seconds per candidate
- **Main DB benchmark**: 5× runs with trimmed mean (10-60 seconds total)
- **Full 99 query suite**: 2-4 hours (depends on provider rate limits)

---

## Version History

### v5 (Current - 2026-02-04)
- DAG-based prompts with `dag_prompts.py`
- Example batching (5 parallel workers with different examples)
- Clean modular architecture
- Full test coverage

### v4 (Deprecated)
- Used `sql_dag.py` prototype (broken)
- Single example set for all workers
- No test coverage

### v3 (Legacy)
- Text-based prompts without DAG structure
- No knowledge base integration
- Lower success rate

---

## References

- **DAG Builder**: `packages/qt-sql/qt_sql/optimization/dag_v2.py`
- **Gold Examples**: `packages/qt-sql/qt_sql/optimization/dag_v3.py`
- **Knowledge Base**: `packages/qt-sql/qt_sql/optimization/knowledge_base.py`
- **DSPy Docs**: https://github.com/stanfordnlp/dspy
- **TPC-DS Benchmark**: http://www.tpc.org/tpcds/

---

## Glossary

- **DAG**: Directed Acyclic Graph - represents query structure with CTEs as nodes
- **CTE**: Common Table Expression - WITH clause subquery
- **DSPy**: Framework for structured LLM prompts and demos
- **KB**: Knowledge Base - collection of optimization patterns and rules
- **Topological Order**: Dependency-respecting node ordering (parents before children)
- **Worker**: Parallel optimization attempt with different examples/strategies
- **Trimmed Mean**: Average after removing outliers (highest/lowest values)
