"""SQL Semantic Rewriters Package.

This package provides the transformation layer for QueryTorque.
Detection rules identify anti-patterns; rewriters transform them.

Architecture:
    Detection Rule (e.g., SQL-DUCK-001)
           ↓
    Registry.get_rewriter_for_rule()
           ↓
    SemanticRewriter.rewrite(ast_node)
           ↓
    RewriteResult (rewritten_sql, safety_checks, confidence)
           ↓
    Validation Harness (equivalence check)

Usage:
    from qt_sql.rewriters import get_rewriter_for_rule, SchemaMetadata
    
    # Get rewriter for a detected issue
    rewriter = get_rewriter_for_rule("SQL-WHERE-007")
    if rewriter:
        result = rewriter.rewrite(ast_node)
        if result.success and result.all_safety_checks_passed:
            print(result.rewritten_sql)

Semantic Rewriter Categories:
    - Correlated subquery → JOIN
    - Self-join → Window function
    - Repeated subquery → CTE
    - DISTINCT elimination
    - OR chain → IN
    - Manual pivot → PIVOT/UNPIVOT
    - Greatest-N-per-group → ROW_NUMBER
"""

from .base import (
    BaseRewriter,
    CompositeRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheck,
    SafetyCheckResult,
    SchemaMetadata,
    TableMetadata,
)

from .registry import (
    register_rewriter,
    get_rewriter_for_rule,
    get_rewriter_by_id,
    has_rewriter,
    list_registered_rules,
    list_registered_rewriters,
    get_coverage_stats,
    discover_rewriters,
    RewriterChain,
)

__all__ = [
    # Base classes
    "BaseRewriter",
    "CompositeRewriter",
    "RewriteResult",
    "RewriteConfidence",
    "SafetyCheck",
    "SafetyCheckResult",
    "SchemaMetadata",
    "TableMetadata",
    # Registry
    "register_rewriter",
    "get_rewriter_for_rule",
    "get_rewriter_by_id",
    "has_rewriter",
    "list_registered_rules",
    "list_registered_rewriters",
    "get_coverage_stats",
    "discover_rewriters",
    "RewriterChain",
]

# Auto-discover semantic rewriters on import
discover_rewriters()
