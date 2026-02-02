"""SQL Semantic Rewriters Package.

This package provides SQL transformation/rewriting capabilities.
Detection rules identify anti-patterns; rewriters transform them.

Architecture:
    Detection Rule (e.g., SQL-DUCK-001)
           ↓
    Registry.get_rewriter_for_rule()
           ↓
    SemanticRewriter.rewrite(ast_node)
           ↓
    RewriteResult (rewritten_sql, safety_checks, confidence)

Usage:
    from qt_sql.rewriters import get_rewriter_for_rule

    # Get rewriter for a detected issue
    rewriter = get_rewriter_for_rule("SQL-WHERE-010")
    if rewriter:
        result = rewriter.rewrite(ast_node)
        if result.success and result.all_safety_checks_passed:
            print(result.rewritten_sql)
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
    RewriterChain,
)

# Import rewriter modules to trigger registration
# NOTE: Import order matters! LLM rewriters are imported first, then rule-based
# fallbacks are imported after to take precedence when LLM is unavailable.
from . import or_chain
from . import correlated_subquery
from . import self_join_to_window
from . import repeated_subquery
from . import duckdb_specific
from . import null_semantics
from . import join_patterns
from . import filter_sargability
from . import cte_optimizer
from . import llm_rewriter  # LLM-based rewriters (may fail without LLM)
from . import aggregate_optimizer  # Rule-based fallbacks (import AFTER llm_rewriter)
from . import in_subquery
from . import simplification
from . import join_conversion
from . import boolean_optimizer
from . import subquery_flattener

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
    "RewriterChain",
]
