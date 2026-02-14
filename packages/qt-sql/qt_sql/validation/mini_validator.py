"""3-tier semantic validation using a deterministic mini dataset.

This module validates optimized SQL candidates against the original query
before expensive benchmarking.

Validation tiers:
1. Structural: AST checks (column-shape compatibility)
2. Logic: Execute both queries against the same sampled base tables
3. Dialect: Placeholder for future cross-dialect checks
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

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
    """3-tier semantic validation on a deterministic sampled slice."""

    def __init__(
        self,
        db_path: str,
        sample_pct: float = 2.0,
        timeout_ms: int = 30_000,
        dialect: str = "duckdb",
    ):
        self.db_path = db_path
        self.sample_pct = sample_pct
        self.timeout_ms = timeout_ms
        self.dialect = dialect
        self._executor: Optional[Any] = None

    def validate_rewrite(
        self,
        original_sql: str,
        rewrite_sql: str,
        worker_id: int,
    ) -> SemanticValidationResult:
        """Run 3-tier validation and return diagnostics."""
        t_start = time.time()

        try:
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

            _tier3_result = self._tier3_dialect(original_sql, rewrite_sql)

            elapsed = (time.time() - t_start) * 1000
            return SemanticValidationResult(
                tier_passed=3,
                passed=True,
                errors=[],
                validation_time_ms=elapsed,
            )

        except Exception as e:
            elapsed = (time.time() - t_start) * 1000
            logger.warning("[W%s] Validation exception: %s", worker_id, e)
            return SemanticValidationResult(
                tier_passed=0,
                passed=False,
                errors=[f"Validation exception: {str(e)[:120]}"],
                validation_time_ms=elapsed,
            )

    def _tier1_structural(self, original_sql: str, rewrite_sql: str) -> Dict[str, Any]:
        """Tier 1: structural checks via AST."""
        try:
            if not sqlglot:
                return {"passed": True}

            orig_ast = sqlglot.parse_one(original_sql, dialect=self.dialect)
            rewrite_ast = sqlglot.parse_one(rewrite_sql, dialect=self.dialect)
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"SQL parse error: {str(e)[:80]}"],
                "syntax_error": str(e)[:200],
            }

        try:
            orig_cols = self._extract_select_columns(orig_ast)
            rewrite_cols = self._extract_select_columns(rewrite_ast)
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"Failed to extract columns: {str(e)[:80]}"],
            }

        if orig_cols != rewrite_cols:
            orig_set = set(orig_cols)
            rewrite_set = set(rewrite_cols)
            mismatch = ColumnMismatch(
                original_columns=orig_cols,
                rewrite_columns=rewrite_cols,
                missing=sorted(orig_set - rewrite_set),
                extra=sorted(rewrite_set - orig_set),
            )
            return {
                "passed": False,
                "errors": [
                    f"Column mismatch: {len(mismatch.missing)} missing, {len(mismatch.extra)} extra"
                ],
                "column_mismatch": mismatch,
            }

        return {"passed": True}

    def _extract_select_columns(self, ast: Any) -> List[str]:
        """Extract top-level output columns in order."""
        if not ast:
            return []

        select_exprs = []

        # Prefer top-level output expressions when available.
        if hasattr(ast, "selects") and ast.selects:
            select_exprs = list(ast.selects)
        elif isinstance(ast, exp.Select):
            select_exprs = list(ast.expressions)
        else:
            select_node = None
            for node in ast.walk():
                if isinstance(node, exp.Select):
                    select_node = node
                    break
            if select_node is not None:
                select_exprs = list(select_node.expressions)

        cols: List[str] = []
        for expression in select_exprs:
            if isinstance(expression, exp.Alias):
                cols.append(expression.alias_or_name)
                continue
            if isinstance(expression, exp.Column):
                cols.append(expression.name)
                continue
            if isinstance(expression, exp.Star):
                cols.append("*")
                continue

            output_name = getattr(expression, "output_name", None)
            if output_name:
                cols.append(output_name)
            else:
                cols.append(expression.sql(dialect=self.dialect)[:80])

        return cols

    def _tier2_logic(self, original_sql: str, rewrite_sql: str) -> Dict[str, Any]:
        """Tier 2: execute both queries over the same sampled base tables."""
        executor = self._get_executor()
        cleanup_tables: List[str] = []

        try:
            sampled_original, sampled_rewrite, cleanup_tables = self._prepare_sampled_queries(
                executor,
                original_sql,
                rewrite_sql,
            )

            orig_rows = self._execute_sql(executor, sampled_original, with_timeout=True)
            rewrite_rows = self._execute_sql(executor, sampled_rewrite, with_timeout=True)
        except Exception as e:
            return {
                "passed": False,
                "errors": [f"Execution failed: {str(e)[:120]}"],
            }
        finally:
            for table_name in cleanup_tables:
                try:
                    self._execute_sql(
                        executor,
                        f"DROP TABLE IF EXISTS {table_name}",
                        with_timeout=False,
                    )
                except Exception:
                    pass

        if len(orig_rows) != len(rewrite_rows):
            diff = RowCountDiff(
                original_count=len(orig_rows),
                rewrite_count=len(rewrite_rows),
                diff=len(rewrite_rows) - len(orig_rows),
                sample_pct=self.sample_pct,
            )
            return {
                "passed": False,
                "errors": [
                    f"Row count mismatch on {self.sample_pct}% sample: "
                    f"{len(orig_rows)} vs {len(rewrite_rows)}"
                ],
                "row_count_diff": diff,
            }

        checker = EquivalenceChecker()
        value_result = checker.compare_values(orig_rows, rewrite_rows, max_differences=10)
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
            return {
                "passed": False,
                "errors": [
                    f"Value mismatch on {self.sample_pct}% sample: "
                    f"{len(value_result.differences)} differences"
                ],
                "value_diffs": value_diffs,
                "sql_diff": SQLDiffer.unified_diff(original_sql, rewrite_sql),
            }

        return {"passed": True}

    def _prepare_sampled_queries(
        self,
        executor: Any,
        original_sql: str,
        rewrite_sql: str,
    ) -> Tuple[str, str, List[str]]:
        """Build deterministic sampled-table rewrites for both queries.

        Creates one sampled temp table per base table and rewrites both queries to
        reference those exact temp tables so both sides run on identical data.
        """
        if not sqlglot:
            return original_sql, rewrite_sql, []

        orig_ast = sqlglot.parse_one(original_sql, dialect=self.dialect)
        rewrite_ast = sqlglot.parse_one(rewrite_sql, dialect=self.dialect)

        table_map: Dict[str, str] = {}
        cleanup_tables: List[str] = []
        run_token = int(time.time() * 1000) % 100_000_000

        for ast in (orig_ast, rewrite_ast):
            cte_names = self._collect_cte_names(ast)
            for table in ast.find_all(exp.Table):
                if self._is_cte_reference(table, cte_names):
                    continue
                key = self._table_key(table)
                if key in table_map:
                    continue

                sampled_name = f"__qt_sem_{run_token}_{len(table_map) + 1}"
                create_sql = self._sample_create_sql(sampled_name, key)
                self._execute_sql(executor, create_sql, with_timeout=False)
                table_map[key] = sampled_name
                cleanup_tables.append(sampled_name)

        sampled_original = self._rewrite_with_sample_tables(orig_ast, table_map)
        sampled_rewrite = self._rewrite_with_sample_tables(rewrite_ast, table_map)

        return sampled_original, sampled_rewrite, cleanup_tables

    def _collect_cte_names(self, ast: Any) -> Set[str]:
        names: Set[str] = set()
        for cte in ast.find_all(exp.CTE):
            alias = cte.alias_or_name
            if alias:
                names.add(alias.lower())
        return names

    def _is_cte_reference(self, table: Any, cte_names: Set[str]) -> bool:
        name = getattr(table, "name", "")
        return bool(name and name.lower() in cte_names)

    def _table_key(self, table: Any) -> str:
        table_copy = table.copy()
        table_copy.set("alias", None)
        table_copy.set("sample", None)
        return table_copy.sql(dialect=self.dialect)

    def _rewrite_with_sample_tables(self, ast: Any, table_map: Dict[str, str]) -> str:
        cte_names = self._collect_cte_names(ast)
        for table in ast.find_all(exp.Table):
            if self._is_cte_reference(table, cte_names):
                continue
            key = self._table_key(table)
            sampled_name = table_map.get(key)
            if not sampled_name:
                continue
            table.set("this", exp.to_identifier(sampled_name))
            table.set("db", None)
            table.set("catalog", None)
            table.set("sample", None)
        return ast.sql(dialect=self.dialect)

    def _sample_create_sql(self, sampled_name: str, source_sql: str) -> str:
        norm = self.dialect.lower()
        if norm in ("postgres", "postgresql"):
            return (
                f"CREATE TEMP TABLE {sampled_name} AS "
                f"SELECT * FROM {source_sql} "
                f"TABLESAMPLE BERNOULLI ({self.sample_pct}) REPEATABLE (42)"
            )
        if norm == "duckdb":
            return (
                f"CREATE TEMP TABLE {sampled_name} AS "
                f"SELECT * FROM {source_sql} "
                f"USING SAMPLE BERNOULLI ({self.sample_pct} PERCENT) REPEATABLE (42)"
            )
        if norm == "snowflake":
            return (
                f"CREATE TEMP TABLE {sampled_name} AS "
                f"SELECT * FROM {source_sql} "
                f"SAMPLE BERNOULLI ({self.sample_pct}) SEED (42)"
            )
        # Fallback for unknown dialects.
        return (
            f"CREATE TEMP TABLE {sampled_name} AS "
            f"SELECT * FROM {source_sql}"
        )

    def _execute_sql(self, executor: Any, sql: str, *, with_timeout: bool) -> List[Dict[str, Any]]:
        if with_timeout:
            try:
                return executor.execute(sql, timeout_ms=self.timeout_ms)
            except TypeError:
                return executor.execute(sql)
        return executor.execute(sql)

    def _tier3_dialect(self, original_sql: str, rewrite_sql: str) -> Dict[str, Any]:
        """Tier 3 placeholder for future dialect-specific checks."""
        return {"passed": True}

    def _get_executor(self) -> Any:
        if self._executor is None:
            self._executor = create_executor_from_dsn(self.db_path)
        return self._executor

    def close(self) -> None:
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
