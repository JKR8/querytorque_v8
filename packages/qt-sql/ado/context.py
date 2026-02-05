"""Context building for ADO.

This module builds context bundles for SQL queries using the qt_sql
execution layer for database connections and EXPLAIN plans.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ContextBundle:
    """Structured, engine-adapted context for a query."""
    query_id: str
    original_sql: str
    plan_summary: str = ""
    plan_text: str = ""
    plan_context: Optional[Any] = None
    table_stats: dict[str, Any] = field(default_factory=dict)
    heuristics: dict[str, Any] = field(default_factory=dict)
    join_graph: dict[str, Any] = field(default_factory=dict)
    scan_counts: dict[str, Any] = field(default_factory=dict)
    misestimates: list[dict[str, Any]] = field(default_factory=list)


def _summarize_plan(plan_text: str, max_lines: int = 30) -> str:
    """Summarize execution plan to first N lines.

    Args:
        plan_text: Full execution plan text
        max_lines: Maximum lines to keep

    Returns:
        Truncated plan text
    """
    if not plan_text:
        return ""

    lines = plan_text.split("\n")
    if len(lines) <= max_lines:
        return plan_text

    return "\n".join(lines[:max_lines]) + f"\n... ({len(lines) - max_lines} more lines)"


class ContextBuilder:
    """Build a ContextBundle from SQL + database connection.

    Uses qt_sql execution factory to support DuckDB and PostgreSQL.
    """

    def __init__(self, engine: str = "postgres"):
        """Initialize context builder.

        Args:
            engine: Database engine type (duckdb, postgres)
        """
        self.engine = engine

    def build(self, query_id: str, sql: str, sample_db: str) -> ContextBundle:
        """Build context bundle for a query.

        Args:
            query_id: Query identifier (e.g., 'q1', 'query_15')
            sql: The SQL query text
            sample_db: Database connection string or path

        Returns:
            ContextBundle with query context
        """
        # Create base context
        context = ContextBundle(
            query_id=query_id,
            original_sql=sql,
        )

        # Try to get execution plan
        try:
            from qt_sql.execution.factory import create_executor_from_dsn

            with create_executor_from_dsn(sample_db) as executor:
                # Get EXPLAIN plan
                try:
                    plan_result = executor.explain(sql, analyze=False)

                    # Handle different return formats
                    if isinstance(plan_result, dict):
                        context.plan_text = plan_result.get("plan_text", "")
                        if not context.plan_text and "plan" in plan_result:
                            # Some executors return plan as a list
                            plan_data = plan_result.get("plan", [])
                            if isinstance(plan_data, list):
                                context.plan_text = "\n".join(
                                    str(row) for row in plan_data
                                )
                    elif isinstance(plan_result, str):
                        context.plan_text = plan_result
                    else:
                        context.plan_text = str(plan_result)

                    context.plan_summary = _summarize_plan(context.plan_text)

                except Exception as e:
                    logger.debug(f"Could not get EXPLAIN plan: {e}")
                    context.plan_summary = f"(EXPLAIN failed: {e})"

                # Try to get table stats for referenced tables
                try:
                    schema_info = executor.get_schema_info(include_row_counts=True)
                    if schema_info and "tables" in schema_info:
                        # Extract stats for tables in the query
                        sql_lower = sql.lower()
                        for table in schema_info["tables"]:
                            table_name = table.get("name", "").lower()
                            if table_name and table_name in sql_lower:
                                context.table_stats[table_name] = {
                                    "row_count": table.get("row_count", 0),
                                    "columns": table.get("columns", []),
                                }
                except Exception as e:
                    logger.debug(f"Could not get table stats: {e}")

        except ImportError as e:
            logger.warning(f"qt_sql execution module not available: {e}")
        except Exception as e:
            logger.warning(f"Failed to build context for {query_id}: {e}")
            context.plan_summary = f"(context build failed: {e})"

        return context

    def build_minimal(self, query_id: str, sql: str) -> ContextBundle:
        """Build minimal context without database connection.

        Args:
            query_id: Query identifier
            sql: The SQL query text

        Returns:
            ContextBundle with just the SQL (no plan)
        """
        return ContextBundle(
            query_id=query_id,
            original_sql=sql,
            plan_summary="(no database connection)",
        )
