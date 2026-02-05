"""PostgreSQL optimization context builder.

Builds complete context for PostgreSQL optimization prompts by extracting
schema, statistics, and settings relevant to the query being optimized.
"""

from __future__ import annotations

from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)


def build_pg_optimization_context(
    executor: Any,
    sql: str,
    explain_output: str,
) -> dict[str, str]:
    """Build full context for PostgreSQL optimization prompt.

    Extracts only the tables referenced in the query and builds formatted
    context sections matching the prompt template placeholders.

    Args:
        executor: PostgresExecutor instance (connected)
        sql: Original SQL query to optimize
        explain_output: EXPLAIN ANALYZE output text

    Returns:
        Dictionary with keys matching prompt placeholders:
        - postgres_version: Version string
        - postgres_settings: Formatted settings block
        - schema_ddl: CREATE TABLE statements for relevant tables
        - table_statistics: Formatted statistics block
        - original_query: The input SQL
        - explain_analyze_output: The input EXPLAIN output
    """
    # Extract tables from SQL
    tables = _extract_tables_from_sql(sql)
    logger.info(f"Extracted {len(tables)} tables from query: {tables}")

    # Get PostgreSQL version
    postgres_version = executor.get_version()

    # Get PostgreSQL settings
    settings = executor.get_settings()
    postgres_settings = _format_settings(settings)

    # Get schema DDL for relevant tables only
    schema_ddl = _build_schema_ddl(executor, tables)

    # Get table statistics for relevant tables only
    table_statistics = _build_table_statistics(executor, tables)

    return {
        "postgres_version": postgres_version,
        "postgres_settings": postgres_settings,
        "schema_ddl": schema_ddl,
        "table_statistics": table_statistics,
        "original_query": sql,
        "explain_analyze_output": explain_output,
    }


def _extract_tables_from_sql(sql: str) -> list[str]:
    """Extract base table names from SQL using sql_parser.

    Args:
        sql: SQL query

    Returns:
        List of base table names (not CTEs)
    """
    try:
        from qt_sql.sql_parser import SQLParser

        parser = SQLParser(dialect="postgres")
        graph = parser.parse(sql)
        return graph.base_tables
    except Exception as e:
        logger.warning(f"Failed to parse SQL for table extraction: {e}")
        # Fallback: simple regex extraction
        return _extract_tables_regex(sql)


def _extract_tables_regex(sql: str) -> list[str]:
    """Fallback table extraction using regex.

    Args:
        sql: SQL query

    Returns:
        List of potential table names
    """
    import re

    tables = set()

    # Match FROM table and JOIN table patterns
    from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
    join_pattern = r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'

    for pattern in [from_pattern, join_pattern]:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            table = match.group(1).lower()
            # Skip common SQL keywords
            if table not in {'select', 'where', 'and', 'or', 'on', 'as', 'in', 'not'}:
                tables.add(table)

    return list(tables)


def _format_settings(settings: dict[str, str]) -> str:
    """Format PostgreSQL settings as readable text.

    Args:
        settings: Dictionary of setting name -> value

    Returns:
        Formatted settings block
    """
    if not settings:
        return "(settings not available)"

    lines = []
    # Order settings by importance for query optimization
    priority_order = [
        'work_mem',
        'effective_cache_size',
        'shared_buffers',
        'random_page_cost',
        'seq_page_cost',
        'join_collapse_limit',
        'from_collapse_limit',
        'geqo_threshold',
        'default_statistics_target',
        'max_parallel_workers_per_gather',
        'jit',
    ]

    for name in priority_order:
        if name in settings:
            lines.append(f"{name} = {settings[name]}")

    # Add any remaining settings not in priority list
    for name, value in sorted(settings.items()):
        if name not in priority_order:
            lines.append(f"{name} = {value}")

    return "\n".join(lines)


def _build_schema_ddl(executor: Any, tables: list[str]) -> str:
    """Build CREATE TABLE DDL for specified tables.

    Args:
        executor: PostgresExecutor instance
        tables: List of table names

    Returns:
        Combined DDL statements
    """
    if not tables:
        return "(no tables found in query)"

    ddl_parts = []
    for table in sorted(tables):
        try:
            ddl = executor.get_table_ddl(table)
            if ddl and not ddl.startswith("-- Could not"):
                ddl_parts.append(ddl)
        except Exception as e:
            logger.warning(f"Failed to get DDL for table {table}: {e}")

    if not ddl_parts:
        return "(schema not available)"

    return "\n\n".join(ddl_parts)


def _build_table_statistics(executor: Any, tables: list[str]) -> str:
    """Build formatted table statistics for specified tables.

    Args:
        executor: PostgresExecutor instance
        tables: List of table names

    Returns:
        Formatted statistics block
    """
    if not tables:
        return "(no tables found in query)"

    stats_parts = []

    for table in sorted(tables):
        try:
            # Get basic table stats
            table_stats = executor.get_table_stats(table)
            row_count = table_stats.get("row_count", 0)

            lines = [
                f"Table: {table}",
                f"  Row count: {row_count:,}",
            ]

            # Get column statistics from pg_stats
            col_stats = executor.get_pg_column_stats(table)
            if col_stats:
                lines.append("  Column statistics:")
                for cs in col_stats:
                    col_name = cs["column_name"]
                    ndistinct = cs.get("ndistinct", "N/A")
                    null_frac = cs.get("null_frac", 0)

                    # Format ndistinct
                    if isinstance(ndistinct, float):
                        if ndistinct < 0:
                            # Negative means fraction of rows
                            ndistinct_str = f"{abs(ndistinct):.2f} (fraction)"
                        else:
                            ndistinct_str = f"{int(ndistinct):,}"
                    else:
                        ndistinct_str = str(ndistinct)

                    lines.append(
                        f"    - {col_name}: ndistinct={ndistinct_str}, "
                        f"null_frac={null_frac:.4f}"
                    )

            stats_parts.append("\n".join(lines))

        except Exception as e:
            logger.warning(f"Failed to get stats for table {table}: {e}")
            stats_parts.append(f"Table: {table}\n  (statistics not available)")

    if not stats_parts:
        return "(statistics not available)"

    return "\n\n".join(stats_parts)


def format_explain_text(plan_json: dict) -> str:
    """Format EXPLAIN JSON into readable text format.

    Args:
        plan_json: EXPLAIN output as dict (from executor.explain())

    Returns:
        Text representation of the plan
    """
    if not plan_json or "error" in plan_json:
        return str(plan_json.get("error", "(plan not available)"))

    lines = []

    def _format_node(node: dict, depth: int = 0) -> None:
        indent = "  " * depth
        node_type = node.get("Node Type", "Unknown")

        # Build node description
        parts = [node_type]

        if "Relation Name" in node:
            parts.append(f"on {node['Relation Name']}")
        if "Alias" in node and node["Alias"] != node.get("Relation Name"):
            parts.append(f"({node['Alias']})")
        if "Index Name" in node:
            parts.append(f"using {node['Index Name']}")

        # Add cost and rows info
        cost_parts = []
        if "Actual Total Time" in node:
            cost_parts.append(f"time={node['Actual Total Time']:.3f}ms")
        if "Actual Rows" in node:
            actual = node["Actual Rows"]
            planned = node.get("Plan Rows", actual)
            if actual != planned:
                cost_parts.append(f"rows={actual} (est: {planned})")
            else:
                cost_parts.append(f"rows={actual}")
        if "Actual Loops" in node and node["Actual Loops"] > 1:
            cost_parts.append(f"loops={node['Actual Loops']}")

        if cost_parts:
            parts.append(f"({', '.join(cost_parts)})")

        lines.append(f"{indent}-> {' '.join(parts)}")

        # Add filter info
        if "Filter" in node:
            lines.append(f"{indent}   Filter: {node['Filter']}")
            if "Rows Removed by Filter" in node:
                lines.append(
                    f"{indent}   Rows Removed: {node['Rows Removed by Filter']}"
                )

        # Recurse into children
        for child in node.get("Plans", []):
            _format_node(child, depth + 1)

    plan = plan_json.get("Plan")
    if plan:
        _format_node(plan)

    # Add timing summary
    if "Planning Time" in plan_json:
        lines.append(f"\nPlanning Time: {plan_json['Planning Time']:.3f} ms")
    if "Execution Time" in plan_json:
        lines.append(f"Execution Time: {plan_json['Execution Time']:.3f} ms")

    return "\n".join(lines)
