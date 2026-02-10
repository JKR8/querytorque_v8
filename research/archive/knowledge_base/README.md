# QueryTorque Knowledge Base

Per-database pattern library for SQL query optimization.

## Structure

```
knowledge_base/
├── core/           # Universal patterns (shared across databases)
├── duckdb/         # DuckDB-specific patterns (current)
├── postgres/       # PostgreSQL patterns (future)
└── _archive/       # Old files (reference only)
```

## Gold Examples (Confirmed Optimizations)

**DuckDB (TPC-DS SF100)**:

| Query | Speedup | Pattern |
|-------|---------|---------|
| Q15 | 2.98x | OR to UNION + Date CTE |
| Q39 | 2.44x | Filter pushdown |
| Q23 | 2.33x | EXISTS instead of IN |
| Q45 | 1.8x | OR decomposition |
| Q95 | 1.7x | CTE with DISTINCT |

Files: `duckdb/gold_examples.py`, `duckdb/dag_gold_examples.py`

## Usage

```python
# Load transforms (database-agnostic)
from qt_sql.optimization.knowledge_base import TRANSFORM_REGISTRY, detect_opportunities

# Load gold examples (database-specific)
from research.knowledge_base.duckdb.gold_examples import get_gold_examples
examples = get_gold_examples(3)
```

## Adding a New Database

1. Create `knowledge_base/{db_name}/`
2. Copy `duckdb/gold_examples.py` as template
3. Run benchmarks on target database
4. Replace examples with validated patterns
5. Update loader to select by DB type

## High-Value Patterns

1. `or_to_union` - 2.98x (split OR into UNION ALL)
2. `correlated_to_cte` - 2.81x (decorrelate subqueries)
3. `date_cte_isolate` - 2.67x (early date filtering)
4. `push_pred` - 2.71x (predicate pushdown)
5. `consolidate_scans` - 1.84x (combine table scans)

## Code References

- Transform registry: `packages/qt-sql/qt_sql/optimization/knowledge_base.py`
- Agent instructions: `AGENT_INSTRUCTIONS.md`
