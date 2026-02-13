"""Database utilities for SQL optimization.

Functions for running EXPLAIN ANALYZE and fetching schema with stats.
Supports both DuckDB (file paths) and PostgreSQL (DSN strings).
"""

import logging
import re
from pathlib import Path
from typing import Any, Optional, Set, Union

logger = logging.getLogger(__name__)


def _detect_db_type(database_path: str) -> str:
    """Detect database type from path or DSN.

    Args:
        database_path: Database path or connection string

    Returns:
        "duckdb" or "postgres"
    """
    path_lower = database_path.lower()
    if path_lower.startswith("postgres://") or path_lower.startswith("postgresql://"):
        return "postgres"
    if path_lower.startswith("snowflake://"):
        return "snowflake"
    if path_lower.startswith("duckdb://"):
        return "duckdb"
    # Default to DuckDB for file paths
    return "duckdb"


def _get_executor(database_path: str):
    """Get the appropriate executor for a database path/DSN.

    Args:
        database_path: Database path or connection string

    Returns:
        Executor instance (DuckDBExecutor or PostgresExecutor)
    """
    from .factory import create_executor_from_dsn
    return create_executor_from_dsn(database_path)


def extract_table_names(sql: str) -> Set[str]:
    """Extract table names referenced in a SQL query.

    Uses regex patterns to find table names in FROM, JOIN, and other clauses.

    Args:
        sql: SQL query string

    Returns:
        Set of lowercase table names referenced in the query
    """
    tables: Set[str] = set()

    # Try sqlglot first for accurate parsing
    try:
        import sqlglot
        from sqlglot import exp

        parsed = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.IGNORE)
        for statement in parsed:
            if statement is None:
                continue
            for table in statement.find_all(exp.Table):
                table_name = table.name
                if table_name:
                    tables.add(table_name.lower())

        if tables:
            return tables
    except Exception as e:
        logger.debug(f"sqlglot parsing failed, falling back to regex: {e}")

    # Fallback: regex-based extraction
    sql_normalized = re.sub(r'\s+', ' ', sql.upper())

    # Pattern for FROM clause
    from_pattern = r'\bFROM\s+([A-Za-z_][A-Za-z0-9_]*(?:\s*,\s*[A-Za-z_][A-Za-z0-9_]*)*)'
    from_matches = re.findall(from_pattern, sql_normalized, re.IGNORECASE)
    for match in from_matches:
        for table in match.split(','):
            table = table.strip().split()[0]
            if table and table.upper() not in ('SELECT', 'WHERE', 'AND', 'OR'):
                tables.add(table.lower())

    # Pattern for JOIN clauses
    join_pattern = r'\bJOIN\s+([A-Za-z_][A-Za-z0-9_]*)'
    join_matches = re.findall(join_pattern, sql_normalized, re.IGNORECASE)
    for table in join_matches:
        tables.add(table.lower())

    return tables


def fetch_schema_with_stats(
    database_path: str,
    sql: Optional[str] = None,
) -> Optional[dict]:
    """Fetch schema with stats (row counts, indexes, primary keys).

    Supports both DuckDB file paths and PostgreSQL DSN strings.

    Args:
        database_path: Path to DuckDB database file or PostgreSQL DSN
        sql: Optional SQL query to filter tables by

    Returns:
        Schema dict with structure:
        {
            "tables": [
                {
                    "name": "customers",
                    "columns": [...],
                    "row_count": 5000,
                    "indexes": [...],
                    "primary_key": [...]
                },
                ...
            ]
        }
    """
    # Extract referenced tables from SQL if provided
    referenced_tables: Optional[Set[str]] = None
    if sql:
        referenced_tables = extract_table_names(sql)
        if referenced_tables:
            logger.debug(f"Filtering schema to tables: {referenced_tables}")

    try:
        db = _get_executor(database_path)
        db_type = _detect_db_type(database_path)

        with db:
            # Get base schema
            schema_info = db.get_schema_info(include_row_counts=True)

            # Filter and enrich tables
            tables_with_stats = []
            for table in schema_info.get("tables", []):
                table_name = table.get("name") or table.get("table_name")
                if not table_name:
                    continue

                # Filter by referenced tables if SQL was provided
                if referenced_tables is not None:
                    if table_name.lower() not in referenced_tables:
                        continue

                tables_with_stats.append({
                    "name": table_name,
                    "columns": table.get("columns", []),
                    "row_count": table.get("row_count", 0),
                    "indexes": table.get("indexes", []),
                    "primary_key": table.get("primary_key", []),
                })

            return {
                "tables": tables_with_stats,
                "source": db_type,
            }

    except Exception as e:
        logger.warning(f"Failed to fetch schema with stats: {e}")
        return None


def run_explain_analyze(
    database_path: str,
    sql: str,
) -> Optional[dict]:
    """Run EXPLAIN ANALYZE on SQL and return execution plan analysis.

    Supports both DuckDB file paths and PostgreSQL DSN strings.

    Args:
        database_path: Path to DuckDB database file or PostgreSQL DSN
        sql: SQL query to explain

    Returns:
        Execution plan dict with structure:
        {
            "execution_time_ms": 12.5,
            "plan_text": "...",  # Raw EXPLAIN output
            "plan_json": {...},  # Parsed JSON plan
            "actual_rows": ...,
        }
    """
    db_type = _detect_db_type(database_path)

    if db_type == "postgres":
        return _run_explain_analyze_postgres(database_path, sql)
    elif db_type == "snowflake":
        return _run_explain_analyze_snowflake(database_path, sql)
    else:
        return _run_explain_analyze_duckdb(database_path, sql)


def _run_explain_analyze_snowflake(dsn: str, sql: str) -> Optional[dict]:
    """Run EXPLAIN on Snowflake (no ANALYZE available)."""
    try:
        from .factory import SnowflakeConfig

        config = SnowflakeConfig.from_dsn(dsn)
        executor = config.get_executor()

        with executor:
            result = {
                "execution_time_ms": None,
                "plan_text": None,
                "plan_json": None,
                "actual_rows": None,
            }

            try:
                plan_data = executor.explain(sql, analyze=False)
                result["plan_text"] = plan_data.get("plan", "")
            except Exception as e:
                logger.warning(f"Snowflake EXPLAIN failed: {e}")

            return result

    except Exception as e:
        logger.warning(f"Failed to run Snowflake EXPLAIN: {e}")
        return None


def _run_explain_analyze_duckdb(database_path: str, sql: str) -> Optional[dict]:
    """Run EXPLAIN ANALYZE on DuckDB."""
    try:
        from .duckdb_executor import DuckDBExecutor
        import duckdb

        with DuckDBExecutor(database_path, read_only=True) as db:
            conn = db._ensure_connected()

            result = {
                "execution_time_ms": None,
                "plan_text": None,
                "plan_json": None,
                "actual_rows": None,
            }

            # Get text plan first (always works)
            try:
                text_result = conn.execute(f"EXPLAIN {sql}").fetchall()
                result["plan_text"] = "\n".join(
                    row[1] if len(row) > 1 else row[0]
                    for row in text_result
                )
            except Exception as e:
                logger.warning(f"Failed to get text plan: {e}")

            # Try to get JSON plan with ANALYZE
            try:
                json_result = conn.execute(
                    f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
                ).fetchall()

                for plan_type, plan_json in json_result:
                    import json
                    parsed = json.loads(plan_json)

                    if plan_type == "analyzed_plan" and isinstance(parsed, dict):
                        result["plan_json"] = parsed
                        result["execution_time_ms"] = parsed.get("latency", 0) * 1000
                        result["actual_rows"] = parsed.get("rows_returned", 0)
                        break
            except Exception as e:
                logger.debug(f"JSON EXPLAIN ANALYZE failed: {e}")

            return result

    except Exception as e:
        logger.warning(f"Failed to run DuckDB EXPLAIN: {e}")
        return None


def _run_explain_analyze_postgres(database_path: str, sql: str) -> Optional[dict]:
    """Run EXPLAIN ANALYZE on PostgreSQL."""
    try:
        from .factory import PostgresConfig
        import json as json_mod

        config = PostgresConfig.from_dsn(database_path)
        db = config.get_executor()

        with db:
            result = {
                "execution_time_ms": None,
                "plan_text": None,
                "plan_json": None,
                "actual_rows": None,
            }

            # Get JSON plan with ANALYZE
            try:
                explain_result = db.explain(sql, analyze=True)

                if explain_result:
                    result["plan_json"] = explain_result.get("Plan")
                    result["execution_time_ms"] = explain_result.get("Execution Time", 0)
                    result["actual_rows"] = explain_result.get("rows_returned", 0)

                    # Build text representation
                    plan = explain_result.get("Plan", {})
                    result["plan_text"] = _plan_to_text(plan)

            except Exception as e:
                logger.warning(f"Failed to get PostgreSQL JSON plan: {e}")

                # Fallback to text-only EXPLAIN
                try:
                    text_rows = db.execute(f"EXPLAIN {sql}")
                    result["plan_text"] = "\n".join(
                        row.get("QUERY PLAN", str(row)) for row in text_rows
                    )
                except Exception as text_e:
                    logger.warning(f"Text EXPLAIN also failed: {text_e}")

            return result

    except Exception as e:
        logger.warning(f"Failed to run PostgreSQL EXPLAIN: {e}")
        return None


def _plan_to_text(plan: dict, indent: int = 0) -> str:
    """Convert PostgreSQL JSON plan to text representation."""
    lines = []
    prefix = "  " * indent

    node_type = plan.get("Node Type", "Unknown")
    relation = plan.get("Relation Name", "")
    alias = plan.get("Alias", "")

    # Build node description
    node_desc = f"{prefix}{node_type}"
    if relation:
        node_desc += f" on {relation}"
    if alias and alias != relation:
        node_desc += f" ({alias})"

    # Add cost/rows info
    rows = plan.get("Actual Rows", plan.get("Plan Rows", "?"))
    time = plan.get("Actual Total Time", plan.get("Total Cost", "?"))
    node_desc += f"  (rows={rows}, time={time})"

    lines.append(node_desc)

    # Recursively add children
    for child in plan.get("Plans", []):
        lines.append(_plan_to_text(child, indent + 1))

    return "\n".join(lines)


def run_explain_text(
    database_path: str,
    sql: str,
) -> Optional[str]:
    """Run EXPLAIN and return the text plan only (fast, no execution).

    Supports both DuckDB file paths and PostgreSQL DSN strings.

    Args:
        database_path: Path to DuckDB database file or PostgreSQL DSN
        sql: SQL query to explain

    Returns:
        EXPLAIN plan as text string
    """
    db_type = _detect_db_type(database_path)

    try:
        db = _get_executor(database_path)

        with db:
            if db_type == "postgres":
                # PostgreSQL EXPLAIN
                rows = db.execute(f"EXPLAIN {sql}")
                return "\n".join(
                    row.get("QUERY PLAN", str(row)) for row in rows
                )
            else:
                # DuckDB EXPLAIN
                conn = db._ensure_connected()
                text_result = conn.execute(f"EXPLAIN {sql}").fetchall()
                return "\n".join(
                    row[1] if len(row) > 1 else row[0]
                    for row in text_result
                )
    except Exception as e:
        logger.warning(f"Failed to get EXPLAIN: {e}")
        return None


def get_duckdb_engine_info(database_path: str) -> Optional[dict]:
    """Get DuckDB engine metadata.

    Args:
        database_path: Path to DuckDB database file

    Returns:
        Engine info dict with version, threads, memory settings
    """
    try:
        from .duckdb_executor import DuckDBExecutor
        import os

        with DuckDBExecutor(database_path, read_only=True) as db:
            conn = db._ensure_connected()

            version = conn.execute("SELECT version()").fetchone()[0]

            # Get thread count
            try:
                threads = conn.execute("SELECT current_setting('threads')").fetchone()[0]
            except Exception:
                threads = os.cpu_count() or 1

            # Get memory limit
            try:
                memory_limit = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            except Exception:
                memory_limit = "auto"

            return {
                "name": "duckdb",
                "version": version,
                "execution_mode": "IN_MEMORY" if database_path == ":memory:" else "PERSISTENT",
                "threads": int(threads) if str(threads).isdigit() else threads,
                "memory_limit": str(memory_limit),
            }
    except Exception as e:
        logger.warning(f"Failed to get engine info: {e}")
        return None
