"""Snowflake database executor for QueryTorque.

This module provides a Snowflake executor implementing the DatabaseExecutor protocol.
Supports both username/password and JWT token authentication.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

logger = logging.getLogger(__name__)


class SnowflakeExecutor:
    """Snowflake database executor.

    Implements the DatabaseExecutor protocol for Snowflake.
    Supports JWT token and username/password authentication.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: str,
        database: str,
        schema: str = "PUBLIC",
        role: str = "",
    ):
        """Initialize Snowflake executor.

        Args:
            account: Snowflake account identifier (e.g., xy12345.us-east-1)
            user: Username or "JWT_TOKEN" for JWT auth
            password: Password or JWT token
            warehouse: Warehouse name
            database: Database name
            schema: Schema name (default: PUBLIC)
            role: Role name (optional)
        """
        # Import here to defer the error until actual use
        try:
            import snowflake.connector
            self.snowflake = snowflake
            self.DatabaseError = snowflake.connector.errors.DatabaseError
            self.ProgrammingError = snowflake.connector.errors.ProgrammingError
        except ImportError as e:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake support. "
                "Install it with: pip install snowflake-connector-python"
            ) from e

        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.connection = None
        self._auto_connect = True  # auto-connect on first execute()

    def _ensure_connected(self) -> None:
        """Auto-connect if not already connected."""
        if not self.connection and self._auto_connect:
            self.connect()
        if not self.connection:
            raise RuntimeError("Not connected to Snowflake")

    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            # Prepare connection parameters
            conn_params = {
                "account": self.account,
                "warehouse": self.warehouse,
                "database": self.database,
                "schema": self.schema,
            }

            conn_params["user"] = self.user
            conn_params["password"] = self.password
            logger.info(f"Connecting to Snowflake as {self.user} on {self.account}")

            if self.role:
                conn_params["role"] = self.role

            self.connection = self.snowflake.connector.connect(**conn_params)
            logger.info(f"Connected to Snowflake {self.account}.{self.database}.{self.schema}")
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def close(self) -> None:
        """Close the Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("Snowflake connection closed")

    def rollback(self) -> None:
        """Rollback current transaction (no-op for Snowflake autocommit)."""
        pass

    def execute(self, sql: str, params: tuple[Any, ...] = (), timeout_ms: int | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Args:
            sql: SQL query to execute
            params: Query parameters (tuple)
            timeout_ms: Optional query timeout in milliseconds

        Returns:
            List of result rows as dictionaries
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Set statement timeout if requested
            if timeout_ms is not None:
                timeout_s = max(1, timeout_ms // 1000)
                cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {timeout_s}")

            # Execute query
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Fetch results
            columns = [desc[0].lower() for desc in cursor.description] if cursor.description else []
            rows = []
            for row in cursor.fetchall():
                rows.append(dict(zip(columns, row)))

            cursor.close()
            return rows

        except (self.DatabaseError, self.ProgrammingError) as e:
            logger.error(f"Snowflake query error: {e}")
            raise
        except Exception as e:
            logger.error(f"Snowflake execution error: {e}")
            raise

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script.

        Args:
            sql_script: SQL script with multiple statements
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Split by semicolon and execute each statement
            statements = [s.strip() for s in sql_script.split(";") if s.strip()]
            for statement in statements:
                cursor.execute(statement)

            cursor.close()
        except (self.DatabaseError, self.ProgrammingError) as e:
            logger.error(f"Snowflake script error: {e}")
            raise

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan as dict.

        Args:
            sql: SQL query to explain
            analyze: Whether to use EXPLAIN ANALYZE (default: True)

        Returns:
            Execution plan as dictionary with 'plan' and 'timing' keys
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Snowflake EXPLAIN syntax
            explain_sql = f"EXPLAIN {sql}"
            cursor.execute(explain_sql)

            # Fetch the plan — Snowflake EXPLAIN returns multiple columns,
            # join all non-None values from each row
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            lines = []
            for row in rows:
                parts = [str(v) for v in row if v is not None]
                if parts:
                    lines.append(" | ".join(parts))
            plan_text = "\n".join(lines) if lines else str(rows)

            cursor.close()

            return {
                "plan": plan_text,
                "format": "snowflake",
                "type": "explain_analyze" if analyze else "explain",
            }

        except (self.DatabaseError, self.ProgrammingError) as e:
            logger.error(f"Snowflake EXPLAIN error: {e}")
            raise

    def get_schema_info(self, include_row_counts: bool = True) -> dict[str, Any]:
        """Get schema information (tables, columns).

        Args:
            include_row_counts: Whether to include row counts

        Returns:
            Schema information dictionary
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Get tables
            cursor.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{self.schema}' AND table_type = 'BASE TABLE'"
            )
            tables = {row[0]: {} for row in cursor.fetchall()}

            # Get columns for each table
            for table_name in tables:
                cursor.execute(
                    f"SELECT column_name, data_type FROM information_schema.columns "
                    f"WHERE table_schema = '{self.schema}' AND table_name = '{table_name}'"
                )
                columns = {row[0]: {"type": row[1]} for row in cursor.fetchall()}
                tables[table_name]["columns"] = columns

            # Get row counts if requested
            if include_row_counts:
                for table_name in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    tables[table_name]["row_count"] = row_count

            cursor.close()

            return {
                "database": self.database,
                "schema": self.schema,
                "tables": tables,
            }

        except (self.DatabaseError, self.ProgrammingError) as e:
            logger.error(f"Snowflake schema info error: {e}")
            raise

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Get table statistics (row counts, indexes).

        Args:
            table_name: Table name

        Returns:
            Table statistics dictionary
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            # Get table size (bytes)
            cursor.execute(
                f"SELECT bytes FROM information_schema.table_storage_metrics "
                f"WHERE table_schema = '{self.schema}' AND table_name = '{table_name.upper()}'"
            )
            result = cursor.fetchone()
            bytes_used = result[0] if result else None

            cursor.close()

            return {
                "table_name": table_name,
                "row_count": row_count,
                "bytes_used": bytes_used,
            }

        except (self.DatabaseError, self.ProgrammingError) as e:
            logger.error(f"Snowflake table stats error: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
