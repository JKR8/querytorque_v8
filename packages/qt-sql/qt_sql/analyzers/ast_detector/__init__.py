"""AST-based SQL anti-pattern detection using sqlglot."""

from typing import Optional

from .base import ASTContext, ASTRule, RuleMatch, ASTDetector
from .registry import (
    get_all_rules,
    get_categories,
    get_rule_by_id,
    get_rule_count,
    get_rules_by_category,
    get_opportunity_rules,
)


def detect_antipatterns(sql: str, dialect: str = "snowflake") -> list:
    """Convenience function to detect SQL anti-patterns.

    Args:
        sql: The SQL query to analyze
        dialect: SQL dialect (snowflake, postgres, duckdb, etc.)

    Returns:
        List of SQLIssue objects representing detected issues
    """
    detector = ASTDetector(dialect=dialect)
    return detector.detect(sql)


def detect_opportunities(sql: str, dialect: str = "duckdb") -> list:
    """Detect optimization opportunities based on empirical TPC-DS wins.

    These patterns have been proven to produce 1.2x-3x speedups
    in TPC-DS SF100 benchmarks on DuckDB.

    Args:
        sql: The SQL query to analyze
        dialect: SQL dialect (duckdb, postgres, snowflake, etc.)

    Returns:
        List of SQLIssue objects representing optimization opportunities

    Example speedups from TPC-DS:
        - QT-OPT-001 (OR to UNION ALL): 2-3x (q15: 2.98x)
        - QT-OPT-002 (Early date filtering): 1.5-2.5x (q39: 2.44x)
        - QT-OPT-003 (Materialized CTE): 1.2-2x (q95: 2.25x)
    """
    detector = ASTDetector(dialect=dialect)
    all_issues = detector.detect(sql)
    # Filter to only optimization_opportunity category
    return [i for i in all_issues if i.rule_id.startswith("QT-OPT-")]


__all__ = [
    "ASTContext",
    "ASTRule",
    "RuleMatch",
    "ASTDetector",
    "detect_antipatterns",
    "detect_opportunities",
    "get_all_rules",
    "get_categories",
    "get_rule_by_id",
    "get_rule_count",
    "get_rules_by_category",
    "get_opportunity_rules",
]
