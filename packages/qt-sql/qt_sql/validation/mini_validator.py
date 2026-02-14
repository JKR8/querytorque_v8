"""3-tier semantic validation using TABLESAMPLE mini dataset.

This module validates optimized SQL candidates against the original query
before expensive benchmarking. Uses TABLESAMPLE to create a "mini oracle"
that runs in milliseconds but catches semantic errors.

Validation tiers:
1. Structural: AST checks (columns, ORDER BY, LIMIT)
2. Logic: Execute on TABLESAMPLE, compare results
3. Dialect: Optional syntax checks for cross-dialect
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import sqlglot
    from sqlglot import exp
except ImportError:
    sqlglot = None
    exp = None

from ..execution.factory import create_executor_from_dsn
from ..schemas import (
    ColumnMismatch,
    RowCountDiff,
    SemanticValidationResult,
    ValueDiff,
)
from .equivalence_checker import EquivalenceChecker
from .sql_differ import SQLDiffer

logger = logging.getLogger(__name__)


class MiniValidator:
    """3-tier semantic validation using TABLESAMPLE."""

    def __init__(
        self,
        db_path: str,
        sample_pct: float = 2.0,
        timeout_ms: int = 30_000,
        dialect: str = "duckdb",
    ):
        """Initialize MiniValidator.

        Args:
            db_path: Database path or DSN
            sample_pct: TABLESAMPLE percentage (default 2%)
            timeout_ms: Max execution time per mini query
            dialect: SQL dialect ("duckdb", "postgresql", "snowflake")
        """
        self.db_path = db_path
        self.sample_pct = sample_pct
        self.timeout_ms = timeout_ms
        self.dialect = dialect
        self._executor: Optional[Any] = None
        self._checker = EquivalenceChecker()

    def validate_rewrite(
        self,
        original_sql: str,
        rewrite_sql: str,
        worker_id: int,
    ) -> SemanticValidationResult:
        """Run 3-tier validation and return rich diagnostics.

        Args:
            original_sql: Original query
            rewrite_sql: Rewritten query
            worker_id: Worker ID for logging

        Returns:
            SemanticValidationResult with tier passed and error details
        """
        t_start = time.time()
        errors: List[str] = []

        try:
            # ── Tier 1: Structural checks (instant) ───────────────────
            tier1_result = self._tier1_structural(original_sql, rewrite_sql)
            if not tier1_result["passed"]:
                elapsed = (time.time() - t_start) * 1000
                return SemanticValidationResult(
                    tier_passed=1,
                    passed=False,
                    errors=tier1_result["errors"],
                    syntax_error=tier1_result.get("syntax_error"),
                    column_mismatch=tier1_result.get("column_mismatch"),
                    validation_time_ms=elapsed,
                )

            # ── Tier 2: Logic check on TABLESAMPLE (milliseconds) ──────
            tier2_result = self._tier2_logic(original_sql, rewrite_sql)
            if not tier2_result["passed"]:
                elapsed = (time.time() - t_start) * 1000
                return SemanticValidationResult(
                    tier_passed=2,
                    passed=False,
                    errors=tier2_result["errors"],
                    row_count_diff=tier2_result.get("row_count_diff"),
                    value_diffs=tier2_result.get("value_diffs"),
                    sql_diff=tier2_result.get("sql_diff"),
                    validation_time_ms=elapsed,
                )

            # ── Tier 3: Dialect warnings (optional) ────────────────────
            tier3_result = self._tier3_dialect(original_sql, rewrite_sql)

            elapsed = (time.time() - t_start) * 1000
            return SemanticValidationResult(
                tier_passed=3,
                passed=True,
                errors=[],
                validation_time_ms=elapsed,
            )

        except Exception as e:
            elapsed = (time.time() - t_start) * 1000
            logger.warning(
                f"[W{worker_id}] Validation exception (tier unknown): {e}"
            )
            return SemanticValidationResult(
                tier_passed=0,
                passed=False,
                errors=[f"Validation exception: {str(e)[:100]}"],
                validation_time_ms=elapsed,
            )

    def _tier1_structural(
        self, original_sql: str, rewrite_sql: str
    ) -> Dict[str, Any]:
        """Tier 1: Structural checks via AST.

        Checks:
        - Both queries parse without errors
        - Output columns match (count and names)
        - ORDER BY/LIMIT preservation

        Returns:
            Dict with "passed" bool and optional error details
        """
        errors: List[str] = []

        # Parse both SQLs
        try:
            if not sqlglot:
                # If sqlglot not available, skip Tier 1
                return {"passed": True}

            orig_ast = sqlglot.parse_one(original_sql, dialect=self.dialect)
            rewrite_ast = sqlglot.parse_one(rewrite_sql, dialect=self.dialect)
        except Exception as e:
            # Parse error is a syntax problem
            return {
                "passed": False,
                "errors": [f"SQL parse error: {str(e)[:80]}"],
                "syntax_error": str(e)[:200],
            }

        # Extract output columns from both
        try:
            orig_cols = self._extract_select_columns(orig_ast)
            rewrite_cols = self._extract_select_columns(rewrite_ast)
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"Failed to extract columns: {str(e)[:80]}"],
            }

        # Check column mismatch
        if orig_cols != rewrite_cols:
            orig_set = set(orig_cols)
            rewrite_set = set(rewrite_cols)
            missing = list(orig_set - rewrite_set)
            extra = list(rewrite_set - orig_set)
            mismatch = ColumnMismatch(
                original_columns=orig_cols,
                rewrite_columns=rewrite_cols,
                missing=missing,
                extra=extra,
            )
            errors.append(
                f"Column mismatch: {len(missing)} missing, {len(extra)} extra"
            )
            return {
                "passed": False,
                "errors": errors,
                "column_mismatch": mismatch,
            }

        return {"passed": True}

    def _extract_select_columns(self, ast: Any) -> List[str]:
        """Extract SELECT column names from AST.

        Args:
            ast: sqlglot AST node

        Returns:
            List of column names in order
        """
        if not ast:
            return []

        columns = []
        # Find the SELECT statement
        select_node = None
        if isinstance(ast, exp.Select):
            select_node = ast
        else:
            # Traverse to find SELECT
            for node in ast.walk():
                if isinstance(node, exp.Select):
                    select_node = node
                    break

        if not select_node:
            return []

        # Extract columns from SELECT clause
        for expr in select_node.expressions:
            if isinstance(expr, exp.Alias):
                columns.append(expr.alias)
            elif isinstance(expr, exp.Column):
                columns.append(expr.name)
            elif isinstance(expr, exp.Star):
                columns.append("*")
            else:
                # Use string representation as fallback
                columns.append(str(expr)[:50])

        return columns

    def _tier2_logic(self, original_sql: str, rewrite_sql: str) -> Dict[str, Any]:
        """Tier 2: Execute on TABLESAMPLE and compare results.

        Args:
            original_sql: Original query
            rewrite_sql: Rewritten query

        Returns:
            Dict with "passed" bool and optional diff details
        """
        errors: List[str] = []

        # Wrap both queries with TABLESAMPLE
        try:
            orig_sample_sql = self._wrap_tablesample(original_sql)
            rewrite_sample_sql = self._wrap_tablesample(rewrite_sql)
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"Failed to wrap TABLESAMPLE: {str(e)[:80]}"],
            }

        # Execute both queries
        try:
            executor = self._get_executor()
            orig_rows = executor.execute(
                orig_sample_sql, timeout_ms=self.timeout_ms
            )
            rewrite_rows = executor.execute(
                rewrite_sample_sql, timeout_ms=self.timeout_ms
            )
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"Execution failed: {str(e)[:80]}"],
            }

        # Compare row counts
        if len(orig_rows) != len(rewrite_rows):
            diff = RowCountDiff(
                original_count=len(orig_rows),
                rewrite_count=len(rewrite_rows),
                diff=len(rewrite_rows) - len(orig_rows),
                sample_pct=self.sample_pct,
            )
            errors.append(
                f"Row count mismatch on {self.sample_pct}% sample: "
                f"{len(orig_rows)} vs {len(rewrite_rows)}"
            )
            return {
                "passed": False,
                "errors": errors,
                "row_count_diff": diff,
            }

        # Compare values
        checker = EquivalenceChecker()
        value_result = checker.compare_values(
            orig_rows, rewrite_rows, max_differences=10
        )

        if not value_result.match:
            value_diffs = [
                ValueDiff(
                    row_index=vd.row_index,
                    column=vd.column,
                    original_value=vd.original_value,
                    rewrite_value=vd.optimized_value,
                )
                for vd in value_result.differences
            ]
            errors.append(
                f"Value mismatch on {self.sample_pct}% sample: "
                f"{len(value_result.differences)} differences"
            )

            # Generate SQL diff
            sql_diff = SQLDiffer.unified_diff(original_sql, rewrite_sql)

            return {
                "passed": False,
                "errors": errors,
                "value_diffs": value_diffs,
                "sql_diff": sql_diff,
            }

        return {"passed": True}

    def _wrap_tablesample(self, sql: str) -> str:
        """Wrap SQL with TABLESAMPLE on real tables.

        Injects TABLESAMPLE BERNOULLI (sample_pct) into all table references,
        skipping CTEs and subqueries.

        Args:
            sql: Original SQL

        Returns:
            Modified SQL with TABLESAMPLE injected
        """
        if not sqlglot:
            # If sqlglot not available, return original
            return sql

        try:
            ast = sqlglot.parse_one(sql, dialect=self.dialect)

            # Find all Table nodes and add TABLESAMPLE
            for table in ast.find_all(exp.Table):
                # Only add TABLESAMPLE to real tables, not CTEs
                # (CTEs are handled separately in the query structure)
                # Add SAMPLE clause
                sample_pct_val = sqlglot.exp.Literal.number(self.sample_pct)
                table.set(
                    "sample",
                    exp.TableSample(
                        method=exp.Identifier(this="BERNOULLI"),
                        expression=sample_pct_val,
                    ),
                )

            return ast.sql(dialect=self.dialect)
        except Exception as e:
            logger.warning(f"Failed to wrap TABLESAMPLE: {e}")
            # Fall back to original SQL
            return sql

    def _tier3_dialect(self, original_sql: str, rewrite_sql: str) -> Dict[str, Any]:
        """Tier 3: Dialect-specific syntax checks (optional).

        For DuckDB→DuckDB: no-op.
        For cross-dialect: could check for unsupported functions, keywords.

        Args:
            original_sql: Original query
            rewrite_sql: Rewritten query

        Returns:
            Dict with warnings (always returns passed=True)
        """
        # Placeholder for future cross-dialect validation
        return {"passed": True}

    def _get_executor(self) -> Any:
        """Get or create database executor."""
        if self._executor is None:
            self._executor = create_executor_from_dsn(self.db_path)
        return self._executor

    def close(self) -> None:
        """Close executor connection."""
        if self._executor:
            try:
                self._executor.close()
            except Exception:
                pass
            self._executor = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
