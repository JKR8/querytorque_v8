# QueryTorque Semantic SQL Rewriters

This package provides the **semantic transformation layer** for QueryTorque. While detection rules identify anti-patterns, **rewriters actually transform the SQL** into optimized equivalents.

## Architecture

```
Detection Rule (e.g., SQL-DUCK-001)
         ↓ identifies anti-pattern
Registry.get_rewriter_for_rule("SQL-DUCK-001")
         ↓ returns appropriate rewriter
SemanticRewriter.rewrite(ast_node)
         ↓ transforms SQL
RewriteResult (rewritten_sql, safety_checks, confidence)
         ↓ validated by
DuckDB Harness (equivalence check)
```

## Quick Start

```python
from qt_sql.rewriters import get_rewriter_for_rule, SchemaMetadata, TableMetadata

# When detection rule fires, get the corresponding rewriter
rewriter = get_rewriter_for_rule("SQL-DUCK-001")

# Optionally provide schema metadata for safer rewrites
metadata = SchemaMetadata(tables={
    "employees": TableMetadata(
        name="employees",
        primary_key=["id"],
        foreign_keys={"dept_id": ("departments", "id")},
    )
})
rewriter.metadata = metadata

# Perform the rewrite
result = rewriter.rewrite(ast_node)

if result.success and result.all_safety_checks_passed:
    print(f"Optimized SQL: {result.rewritten_sql}")
    print(f"Confidence: {result.confidence}")
else:
    print(f"Rewrite failed: {result.explanation}")
    for check in result.safety_checks:
        if not check.passed:
            print(f"  - {check.name}: {check.message}")
```

## Implemented Rewriters

### High-Impact Semantic Rewrites

| Rewriter ID | Pattern | Transformation | Confidence |
|------------|---------|----------------|------------|
| `or_chain_to_in` | `x=1 OR x=2 OR x=3` | `x IN (1,2,3)` | HIGH |
| `correlated_subquery_to_join` | Scalar subquery | LEFT JOIN | MEDIUM |
| `self_join_to_window` | Self-join max/min | ROW_NUMBER() | MEDIUM |
| `repeated_subquery_to_cte` | Duplicate subqueries | WITH clause | HIGH |
| `subquery_to_qualify` | Window filter subquery | QUALIFY (DuckDB) | HIGH |
| `manual_pivot_to_pivot` | CASE pivot pattern | PIVOT (DuckDB) | MEDIUM |
| `union_to_unpivot` | UNION unpivot | UNPIVOT (DuckDB) | MEDIUM |

### Detection Rule to Rewriter Mapping

```python
# DuckDB-specific rules
"SQL-DUCK-001" → SubqueryToQualifyRewriter
"SQL-DUCK-007" → ManualPivotToPivotRewriter  
"SQL-DUCK-008" → UnionToUnpivotRewriter

# General rules
"SQL-WHERE-010" → OrChainToInRewriter
"SQL-SEL-008"   → CorrelatedSubqueryToJoinRewriter
"SQL-JOIN-005"  → SelfJoinToWindowRewriter
"SQL-CTE-003"   → RepeatedSubqueryToCTERewriter
```

## Taxonomy Coverage

Based on the **Taxonomy of Semantic SQL Optimization Patterns**:

| # | Pattern | Status | Rewriter |
|---|---------|--------|----------|
| 1 | Double-Dip (Repeated Subqueries) | ✅ Implemented | `repeated_subquery_to_cte` |
| 2 | Row-by-Row Subquery | ✅ Implemented | `correlated_subquery_to_join` |
| 3 | Manual Pivot | ✅ Implemented | `manual_pivot_to_pivot` |
| 4 | Greatest-N Per-Group | ✅ Implemented | `self_join_to_window` |
| 5 | Unnecessary DISTINCT | 🚧 Planned | - |
| 6 | OR Chain | ✅ Implemented | `or_chain_to_in` |
| 7 | Redundant Join | 🚧 Planned | - |
| 8 | Implicit Cross Join | 🚧 Planned | - |

## Safety Model

Every rewrite includes **safety checks** with three result levels:

- `PASSED` - Safe to apply
- `WARNING` - May change semantics in edge cases
- `FAILED` - Should not apply without manual review
- `SKIPPED` - Missing metadata to verify

### Confidence Levels

- `HIGH` - Proven equivalent (e.g., OR → IN)
- `MEDIUM` - Likely equivalent, needs validation
- `LOW` - May change semantics
- `UNSAFE` - Known semantic change

### Metadata Requirements

Some rewrites require schema metadata for safety:

```python
# Correlated subquery to JOIN needs uniqueness info
metadata = SchemaMetadata(tables={
    "lookup_table": TableMetadata(
        name="lookup_table",
        primary_key=["id"],  # Ensures 1:1 relationship
    )
})
```

## Integration with Detection

```python
from qt_sql.analyzers.ast_detector import ASTDetector
from qt_sql.rewriters import get_rewriter_for_rule

# Detect issues
detector = ASTDetector(dialect="duckdb")
issues = detector.detect(sql)

# For each issue, try to rewrite
for issue in issues:
    rewriter = get_rewriter_for_rule(issue.rule_id)
    if rewriter:
        result = rewriter.rewrite(issue.node)
        if result.success:
            # Validate with harness before accepting
            pass
```

## Adding New Rewriters

1. Create rewriter class extending `BaseRewriter`
2. Implement `rewrite()` method
3. Define `linked_rule_ids` tuple
4. Add `@register_rewriter` decorator

```python
from qt_sql.rewriters import BaseRewriter, register_rewriter, RewriteConfidence

@register_rewriter
class MyNewRewriter(BaseRewriter):
    rewriter_id = "my_new_rewriter"
    name = "My New Pattern"
    description = "Converts X to Y"
    linked_rule_ids = ("SQL-XXX-001",)
    default_confidence = RewriteConfidence.MEDIUM
    
    def can_rewrite(self, node, context=None):
        # Pre-check if pattern matches
        return isinstance(node, exp.Select) and ...
    
    def rewrite(self, node, context=None):
        # Perform transformation
        result = self._create_result(
            success=True,
            original_sql=node.sql(),
            rewritten_sql=transformed.sql(),
            explanation="Converted X to Y",
        )
        
        # Add safety checks
        result.add_safety_check(
            name="semantic_equivalence",
            result=SafetyCheckResult.PASSED,
            message="Transformation preserves semantics",
        )
        
        return result
```

## Directory Structure

```
qt_sql/rewriters/
├── __init__.py           # Package exports
├── base.py               # BaseRewriter, RewriteResult, SchemaMetadata
├── registry.py           # @register_rewriter, get_rewriter_for_rule
└── semantic/
    ├── __init__.py
    ├── or_chain.py           # OR → IN
    ├── correlated_subquery.py # Subquery → JOIN
    ├── self_join_to_window.py # Self-join → ROW_NUMBER
    ├── repeated_subquery.py   # Duplicate → CTE
    └── duckdb_specific.py     # QUALIFY, PIVOT, UNPIVOT
```

## Next Steps

1. **Expand Coverage**: Implement remaining patterns from taxonomy
2. **Adversarial Validation**: Integrate with benchmark harness
3. **LLM Fallback**: For complex patterns that resist AST transformation
4. **Effectiveness Tracking**: Score rewriters by real-world improvement

## References

- [Taxonomy of Semantic SQL Optimization Patterns](docs/taxonomy.pdf)
- [DuckDB Optimizer Gaps](docs/duckdb_gaps.pdf)
- [LLM-Driven Query Rewrite Patterns](docs/llm_rewrite_patterns.pdf)
