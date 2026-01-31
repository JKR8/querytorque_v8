"""AST-based SQL anti-pattern detection using sqlglot."""

from .base import ASTContext, ASTRule, RuleMatch, ASTDetector
from .registry import (
    get_all_rules,
    get_categories,
    get_rule_by_id,
    get_rule_count,
    get_rules_by_category,
)

__all__ = [
    "ASTContext",
    "ASTRule",
    "RuleMatch",
    "ASTDetector",
    "get_all_rules",
    "get_categories",
    "get_rule_by_id",
    "get_rule_count",
    "get_rules_by_category",
]
