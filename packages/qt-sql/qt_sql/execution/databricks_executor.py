"""Databricks (Spark SQL) database executor for QueryTorque.

This module provides a Databricks executor implementing the DatabaseExecutor protocol.
Uses the databricks-sql-connector package for connectivity.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class DatabricksExecutor:
    """Databricks database executor.

    Implements the DatabaseExecutor protocol for Databricks SQL warehouses.
    Connects via the databricks-sql-connector using server hostname + HTTP path.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str = "hive_metastore",
        schema: str = "default",
    ):
        """Initialize Databricks executor.

        Args:
            server_hostname: Databricks workspace hostname (e.g., dbc-xxx.cloud.databricks.com)
            http_path: SQL warehouse HTTP path (e.g., /sql/1.0/warehouses/abc123)
            access_token: Personal access token or OAuth token
            catalog: Unity Catalog name (default: hive_metastore)
            schema: Schema/database name (default: default)
        """
        try:
            from databricks import sql as databricks_sql
            self._databricks_sql = databricks_sql
        except ImportError as e:
            raise ImportError(
                "databricks-sql-connector is required for Databricks support. "
                "Install it with: pip install databricks-sql-connector"
            ) from e

        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self.connection = None
        self._auto_connect = True

    def _ensure_connected(self) -> None:
        """Auto-connect if not already connected."""
        if not self.connection and self._auto_connect:
            self.connect()
        if not self.connection:
            raise RuntimeError("Not connected to Databricks")

    def connect(self) -> None:
        """Establish connection to Databricks SQL warehouse."""
        try:
            self.connection = self._databricks_sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
                catalog=self.catalog,
                schema=self.schema,
            )
            logger.info(
                f"Connected to Databricks {self.server_hostname} "
                f"catalog={self.catalog} schema={self.schema}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Databricks: {e}")
            raise

    def close(self) -> None:
        """Close the Databricks connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Databricks connection closed")

    def rollback(self) -> None:
        """Rollback current transaction (no-op — Databricks is autocommit)."""
        pass

    def execute(
        self, sql: str, params: tuple[Any, ...] = (), timeout_ms: int | None = None
    ) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Args:
            sql: SQL query to execute
            params: Query parameters (tuple)
            timeout_ms: Optional query timeout in milliseconds

        Returns:
            List of result rows as dictionaries with lowercase column names
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            # Databricks SQL connector doesn't support per-statement timeout
            # directly; the warehouse-level timeout is configured in config.json.

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            # Fetch results with lowercase column names
            columns = (
                [desc[0].lower() for desc in cursor.description]
                if cursor.description
                else []
            )
            rows = []
            for row in cursor.fetchall():
                rows.append(dict(zip(columns, row)))

            cursor.close()
            return rows

        except Exception as e:
            logger.error(f"Databricks query error: {e}")
            raise

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script.

        Splits on semicolons and executes each statement sequentially.

        Args:
            sql_script: SQL script with multiple statements
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            statements = [s.strip() for s in sql_script.split(";") if s.strip()]
            for statement in statements:
                cursor.execute(statement)

            cursor.close()
        except Exception as e:
            logger.error(f"Databricks script error: {e}")
            raise

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan.

        Databricks/Spark SQL supports EXPLAIN EXTENDED (no ANALYZE keyword).

        Args:
            sql: SQL query to explain
            analyze: Ignored — Databricks always returns extended plan

        Returns:
            Execution plan dict with 'plan', 'format', 'type' keys
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            cursor.execute(f"EXPLAIN EXTENDED {sql}")

            rows = cursor.fetchall()
            plan_text = "\n".join(str(row[0]) for row in rows if row[0])

            cursor.close()

            return {
                "plan": plan_text,
                "format": "databricks",
                "type": "explain_extended",
            }

        except Exception as e:
            logger.error(f"Databricks EXPLAIN error: {e}")
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

            # List tables in the current catalog.schema
            cursor.execute(f"SHOW TABLES IN {self.catalog}.{self.schema}")
            table_rows = cursor.fetchall()
            # SHOW TABLES returns columns: database, tableName, isTemporary
            table_names = [row[1] for row in table_rows]

            tables = {}
            for table_name in table_names:
                fq = f"{self.catalog}.{self.schema}.{table_name}"
                cursor.execute(f"DESCRIBE TABLE {fq}")
                col_rows = cursor.fetchall()
                columns = {}
                for col_row in col_rows:
                    # DESCRIBE returns: col_name, data_type, comment
                    col_name = col_row[0]
                    if col_name.startswith("#") or col_name == "":
                        break  # partition info section
                    columns[col_name] = {"type": col_row[1]}

                table_info: dict[str, Any] = {"columns": columns}

                if include_row_counts:
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {fq}")
                        table_info["row_count"] = cursor.fetchone()[0]
                    except Exception as e:
                        logger.warning(f"Failed to count {table_name}: {e}")
                        table_info["row_count"] = None

                tables[table_name] = table_info

            cursor.close()

            return {
                "catalog": self.catalog,
                "schema": self.schema,
                "tables": tables,
            }

        except Exception as e:
            logger.error(f"Databricks schema info error: {e}")
            raise

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Get table statistics.

        Args:
            table_name: Table name

        Returns:
            Table statistics dictionary
        """
        self._ensure_connected()

        try:
            cursor = self.connection.cursor()

            fq = f"{self.catalog}.{self.schema}.{table_name}"

            # Row count
            cursor.execute(f"SELECT COUNT(*) FROM {fq}")
            row_count = cursor.fetchone()[0]

            # DESCRIBE DETAIL for size info
            detail = {}
            try:
                cursor.execute(f"DESCRIBE DETAIL {fq}")
                detail_row = cursor.fetchone()
                if detail_row and cursor.description:
                    detail_cols = [d[0].lower() for d in cursor.description]
                    detail = dict(zip(detail_cols, detail_row))
            except Exception as e:
                logger.debug(f"DESCRIBE DETAIL failed for {table_name}: {e}")

            cursor.close()

            return {
                "table_name": table_name,
                "row_count": row_count,
                "size_bytes": detail.get("sizeinbytes"),
                "num_files": detail.get("numfiles"),
                "format": detail.get("format"),
            }

        except Exception as e:
            logger.error(f"Databricks table stats error: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
