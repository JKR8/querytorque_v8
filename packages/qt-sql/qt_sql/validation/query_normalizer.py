"""Query normalizer for handling LIMIT/ORDER BY issues and comment stripping."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None
    exp = None

from .schemas import LimitStrategy


@dataclass
class CommentStrippingResult:
    """Result of stripping SQL comments."""

    sql: str
    comments: List[str]
    had_comments: bool


@dataclass
class NormalizationResult:
    """Result of query normalization."""

    sql: str
    was_modified: bool
    had_limit_without_order: bool
    strategy_applied: Optional[LimitStrategy] = None
    error: Optional[str] = None
    stripped_comments: List[str] = field(default_factory=list)


class QueryNormalizer:
    """Normalizes SQL queries for deterministic comparison.

    Handles:
    - LIMIT without ORDER BY (non-deterministic results)
    - SQL comments (strip for execution, preserve for analysis)
    """

    def __init__(self, dialect: str = "duckdb"):
        """Initialize normalizer.

        Args:
            dialect: SQL dialect for parsing (duckdb, postgres, etc.)
        """
        self.dialect = dialect
        if sqlglot is None:
            raise ImportError("sqlglot is required for query normalization")

    def strip_comments(self, sql: str) -> CommentStrippingResult:
        """Strip SQL comments from query using sqlglot.

        Removes both block comments (/* */) and line comments (--).
        Preserves comments for later analysis.
        Uses sqlglot to safely handle comments inside string literals.

        Args:
            sql: SQL query with potential comments.

        Returns:
            CommentStrippingResult with clean SQL and extracted comments.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            # Extract all comments from the AST
            comments = self._extract_comments(parsed)

            # Strip comments by setting to None
            self._clear_comments(parsed)

            # Regenerate clean SQL
            clean_sql = parsed.sql(dialect=self.dialect)

            return CommentStrippingResult(
                sql=clean_sql,
                comments=comments,
                had_comments=len(comments) > 0,
            )
        except Exception:
            # Fallback: return original SQL if parsing fails
            return CommentStrippingResult(
                sql=sql,
                comments=[],
                had_comments=False,
            )

    def _extract_comments(self, node: exp.Expression) -> List[str]:
        """Recursively extract all comments from AST."""
        comments = []
        if hasattr(node, 'comments') and node.comments:
            comments.extend(node.comments)
        for child in node.iter_expressions():
            comments.extend(self._extract_comments(child))
        return comments

    def _clear_comments(self, node: exp.Expression) -> None:
        """Recursively clear all comments from AST."""
        if hasattr(node, 'comments'):
            node.comments = None
        for child in node.iter_expressions():
            self._clear_comments(child)

    def strip_comments_pair(
        self, original_sql: str, optimized_sql: str
    ) -> Tuple[CommentStrippingResult, CommentStrippingResult]:
        """Strip comments from both queries.

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.

        Returns:
            Tuple of (original_result, optimized_result).
        """
        return (
            self.strip_comments(original_sql),
            self.strip_comments(optimized_sql),
        )

    def detect_limit_without_order(self, sql: str) -> bool:
        """Detect if query has LIMIT without ORDER BY.

        Args:
            sql: SQL query to check.

        Returns:
            True if query has LIMIT without ORDER BY.
        """
        try:
            parsed = sqlglot.parse_one(sql, dialect=self.dialect)
            has_limit = parsed.find(exp.Limit) is not None
            has_order = parsed.find(exp.Order) is not None
            return has_limit and not has_order
        except Exception:
            # If parsing fails, assume no issue
            return False

    def _get_select_column_count(self, parsed: exp.Expression) -> int:
        """Get the number of columns in the outermost SELECT.

        Returns:
            Number of columns, or -1 if SELECT * is used.
        """
        select = parsed.find(exp.Select)
        if select and hasattr(select, "expressions"):
            # Count non-star expressions
            count = 0
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    # Can't determine column count for SELECT *
                    return -1
                count += 1
            return count
        return -1

    def _has_select_star(self, parsed: exp.Expression) -> bool:
        """Check if query uses SELECT *."""
        select = parsed.find(exp.Select)
        if select and hasattr(select, "expressions"):
            for expr in select.expressions:
                if isinstance(expr, exp.Star):
                    return True
        return False

    def normalize_for_comparison(
        self, sql: str, strategy: LimitStrategy = LimitStrategy.ADD_ORDER
    ) -> NormalizationResult:
        """Normalize query for deterministic comparison.

        Args:
            sql: SQL query to normalize.
            strategy: How to handle LIMIT without ORDER BY.

        Returns:
            NormalizationResult with normalized SQL.
        """
        try:
            # First check if normalization is needed
            has_issue = self.detect_limit_without_order(sql)
            if not has_issue:
                return NormalizationResult(
                    sql=sql,
                    was_modified=False,
                    had_limit_without_order=False,
                )

            parsed = sqlglot.parse_one(sql, dialect=self.dialect)

            if strategy == LimitStrategy.REMOVE_LIMIT:
                return self._remove_limit(parsed, sql)
            else:  # ADD_ORDER
                return self._add_order_by(parsed, sql)

        except Exception as e:
            return NormalizationResult(
                sql=sql,
                was_modified=False,
                had_limit_without_order=False,
                error=str(e),
            )

    def _remove_limit(
        self, parsed: exp.Expression, original_sql: str
    ) -> NormalizationResult:
        """Remove LIMIT clause from query."""
        try:
            limit_node = parsed.find(exp.Limit)
            if limit_node:
                limit_node.pop()

            normalized_sql = parsed.sql(dialect=self.dialect)
            return NormalizationResult(
                sql=normalized_sql,
                was_modified=True,
                had_limit_without_order=True,
                strategy_applied=LimitStrategy.REMOVE_LIMIT,
            )
        except Exception as e:
            return NormalizationResult(
                sql=original_sql,
                was_modified=False,
                had_limit_without_order=True,
                error=f"Failed to remove LIMIT: {e}",
            )

    def _add_order_by(
        self, parsed: exp.Expression, original_sql: str
    ) -> NormalizationResult:
        """Add ORDER BY clause to query with LIMIT but no ORDER BY.

        For SELECT * queries, we use ORDER BY 1 only since we don't know
        the column count. This provides deterministic ordering.
        """
        try:
            # Get column count for ORDER BY
            col_count = self._get_select_column_count(parsed)

            if col_count < 0:
                # SELECT * - we don't know column count
                # Use ORDER BY 1 as a safe default (always valid)
                # This provides determinism for comparison purposes
                order_cols = "1"
            else:
                # Build ORDER BY 1, 2, 3, ... up to the column count
                order_cols = ", ".join(str(i) for i in range(1, col_count + 1))

            # Find the LIMIT clause and insert ORDER BY before it
            limit_node = parsed.find(exp.Limit)
            if limit_node:
                # Create ORDER BY clause
                order_by = sqlglot.parse_one(
                    f"SELECT 1 ORDER BY {order_cols}", dialect=self.dialect
                ).find(exp.Order)

                if order_by:
                    # Insert ORDER BY before LIMIT
                    # Find the parent (usually the Select)
                    select = parsed.find(exp.Select)
                    if select:
                        # Set the order on the select
                        select.set("order", order_by)

            normalized_sql = parsed.sql(dialect=self.dialect)
            return NormalizationResult(
                sql=normalized_sql,
                was_modified=True,
                had_limit_without_order=True,
                strategy_applied=LimitStrategy.ADD_ORDER,
            )
        except Exception as e:
            return NormalizationResult(
                sql=original_sql,
                was_modified=False,
                had_limit_without_order=True,
                error=f"Failed to add ORDER BY: {e}",
            )

    def normalize_pair(
        self,
        original_sql: str,
        optimized_sql: str,
        strategy: LimitStrategy = LimitStrategy.ADD_ORDER,
    ) -> tuple[NormalizationResult, NormalizationResult]:
        """Normalize both queries for comparison.

        Applies the same normalization strategy to both queries if either
        has LIMIT without ORDER BY.

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.
            strategy: How to handle LIMIT without ORDER BY.

        Returns:
            Tuple of (original_result, optimized_result).
        """
        original_has_issue = self.detect_limit_without_order(original_sql)
        optimized_has_issue = self.detect_limit_without_order(optimized_sql)

        # If either has the issue, normalize both
        if original_has_issue or optimized_has_issue:
            return (
                self.normalize_for_comparison(original_sql, strategy),
                self.normalize_for_comparison(optimized_sql, strategy),
            )

        # Neither needs normalization
        return (
            NormalizationResult(sql=original_sql, was_modified=False, had_limit_without_order=False),
            NormalizationResult(sql=optimized_sql, was_modified=False, had_limit_without_order=False),
        )
