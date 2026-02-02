"""Database utilities for SQL optimization.

Functions for running EXPLAIN ANALYZE and fetching schema with stats.
Adapted for CLI usage with direct database paths.
"""

import logging
import re
from pathlib import Path
from typing import Any, Optional, Set

logger = logging.getLogger(__name__)


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

    Args:
        database_path: Path to DuckDB database file
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
        from .duckdb_executor import DuckDBExecutor

        with DuckDBExecutor(database_path, read_only=True) as db:
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
                "source": "database",
            }

    except Exception as e:
        logger.warning(f"Failed to fetch schema with stats: {e}")
        return None


def run_explain_analyze(
    database_path: str,
    sql: str,
) -> Optional[dict]:
    """Run EXPLAIN ANALYZE on SQL and return execution plan analysis.

    Args:
        database_path: Path to DuckDB database file
        sql: SQL query to explain

    Returns:
        Execution plan dict with structure:
        {
            "execution_time_ms": 12.5,
            "plan_text": "...",  # Raw EXPLAIN output
            "plan_json": {...},  # Parsed JSON plan
            "row_estimates": {...},
        }
    """
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
        logger.warning(f"Failed to run EXPLAIN: {e}")
        return None


def run_explain_text(
    database_path: str,
    sql: str,
) -> Optional[str]:
    """Run EXPLAIN and return the text plan only (fast, no execution).

    Args:
        database_path: Path to DuckDB database file
        sql: SQL query to explain

    Returns:
        EXPLAIN plan as text string
    """
    try:
        from .duckdb_executor import DuckDBExecutor

        with DuckDBExecutor(database_path, read_only=True) as db:
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
