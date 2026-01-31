"""PostgreSQL database executor for SQL analysis."""

from __future__ import annotations

import json
import os
from typing import Any, Optional

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError as e:
    raise ImportError(
        "psycopg2 is not installed. Install with: pip install psycopg2-binary"
    ) from e


class PostgresExecutor:
    """PostgreSQL database executor for SQL analysis and execution plan generation.

    Provides connection management, query execution, and EXPLAIN plan generation
    using PostgreSQL's JSON EXPLAIN output.

    Usage:
        with PostgresExecutor(host="localhost", database="test_db") as db:
            db.execute_script("CREATE TABLE t (x INT); INSERT INTO t VALUES (1);")
            plan = db.explain("SELECT * FROM t")

    Environment variables (used as defaults):
        - QT_POSTGRES_HOST: PostgreSQL host
        - QT_POSTGRES_PORT: PostgreSQL port
        - QT_POSTGRES_DATABASE: Database name
        - QT_POSTGRES_USER: Username
        - QT_POSTGRES_PASSWORD: Password

    Args:
        host: PostgreSQL server hostname.
        port: PostgreSQL server port.
        database: Database name.
        user: Username for authentication.
        password: Password for authentication.
        schema: Schema to use (default: public).
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        schema: str = "public",
    ):
        self.host = host or os.getenv("QT_POSTGRES_HOST", "localhost")
        self.port = port or int(os.getenv("QT_POSTGRES_PORT", "5432"))
        self.database = database or os.getenv("QT_POSTGRES_DATABASE", "postgres")
        self.user = user or os.getenv("QT_POSTGRES_USER", "postgres")
        self.password = password or os.getenv("QT_POSTGRES_PASSWORD", "postgres")
        self.schema = schema
        self._conn: Optional[psycopg2.extensions.connection] = None

    def connect(self) -> None:
        """Open connection to PostgreSQL."""
        if self._conn is not None and not self._conn.closed:
            return  # Already connected

        self._conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )
        # Set schema search path
        with self._conn.cursor() as cur:
            cur.execute(f"SET search_path TO {self.schema}, public")
        self._conn.commit()

    def close(self) -> None:
        """Close connection to PostgreSQL."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "PostgresExecutor":
        """Context manager entry - opens connection."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        self.close()

    def _ensure_connected(self) -> psycopg2.extensions.connection:
        """Ensure connection is open and return it."""
        if self._conn is None or self._conn.closed:
            self.connect()
        assert self._conn is not None
        return self._conn

    def execute(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Args:
            sql: SQL query to execute.
            params: Query parameters (optional).

        Returns:
            List of dictionaries, one per row.
        """
        conn = self._ensure_connected()

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)

            if cur.description:
                rows = cur.fetchall()
                return [dict(row) for row in rows]
            return []

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script.

        Splits script by semicolons and executes each statement.
        Useful for schema creation and data seeding.

        Args:
            sql_script: SQL script with multiple statements.
        """
        conn = self._ensure_connected()

        with conn.cursor() as cur:
            # PostgreSQL can handle multi-statement scripts
            cur.execute(sql_script)
        conn.commit()

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan as JSON dict.

        Uses PostgreSQL's EXPLAIN (FORMAT JSON) to get plan information.
        If analyze=True, also runs EXPLAIN ANALYZE to get actual timing.

        Args:
            sql: SQL query to explain.
            analyze: If True, run EXPLAIN ANALYZE for actual timing.

        Returns:
            Execution plan as dictionary with plan tree and timing.
        """
        conn = self._ensure_connected()
        result: dict[str, Any] = {}

        explain_options = "FORMAT JSON, COSTS"
        if analyze:
            explain_options = "ANALYZE, FORMAT JSON, COSTS, TIMING"

        with conn.cursor() as cur:
            try:
                cur.execute(f"EXPLAIN ({explain_options}) {sql}")
                plan_result = cur.fetchall()

                if plan_result:
                    # PostgreSQL returns JSON as first element
                    plan_json = plan_result[0][0]
                    if isinstance(plan_json, list) and len(plan_json) > 0:
                        plan_data = plan_json[0]
                        result["Plan"] = plan_data.get("Plan", {})
                        result["Planning Time"] = plan_data.get("Planning Time", 0)
                        result["Execution Time"] = plan_data.get("Execution Time", 0)

                        # Extract latency in seconds
                        if "Execution Time" in plan_data:
                            result["latency"] = plan_data["Execution Time"] / 1000.0

                        # Extract rows from plan
                        plan = plan_data.get("Plan", {})
                        result["rows_returned"] = plan.get("Actual Rows", plan.get("Plan Rows", 0))
                        result["children"] = [plan]

            except psycopg2.Error as e:
                # Rollback on error
                conn.rollback()
                return {
                    "error": str(e),
                    "type": "error_plan",
                }

        # Commit any changes from ANALYZE
        conn.commit()
        return result

    def get_schema_info(self, include_row_counts: bool = True) -> dict[str, Any]:
        """Get schema information (tables, columns) from the database.

        Returns:
            Dictionary with a list of table definitions.
        """
        conn = self._ensure_connected()

        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = %s
                  AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """, (self.schema,))
            tables_result = cur.fetchall()

            tables_list = []

            for (table_name,) in tables_result:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s
                      AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.schema, table_name))
                columns_result = cur.fetchall()
                row_count = None
                if include_row_counts:
                    try:
                        cur.execute(
                            """
                            SELECT reltuples::bigint
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE c.relname = %s AND n.nspname = %s
                            """,
                            (table_name, self.schema),
                        )
                        est = cur.fetchone()
                        row_count = est[0] if est else None
                    except Exception:
                        row_count = None

                indexes = []
                try:
                    cur.execute(
                        """
                        SELECT indexname
                        FROM pg_indexes
                        WHERE schemaname = %s AND tablename = %s
                        """,
                        (self.schema, table_name),
                    )
                    indexes = [r[0] for r in cur.fetchall()]
                except Exception:
                    pass

                primary_key = []
                try:
                    cur.execute(
                        """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type = 'PRIMARY KEY'
                          AND tc.table_schema = %s
                          AND tc.table_name = %s
                        """,
                        (self.schema, table_name),
                    )
                    primary_key = [r[0] for r in cur.fetchall()]
                except Exception:
                    pass

                tables_list.append({
                    "name": table_name,
                    "table_name": table_name,
                    "row_count": row_count,
                    "primary_key": primary_key,
                    "indexes": indexes,
                    "columns": [
                        {
                            "name": col[0],
                            "type": col[1],
                            "nullable": col[2] == "YES",
                        }
                        for col in columns_result
                    ],
                })

        return {"tables": tables_list}

    def create_schema(self, schema_name: str) -> None:
        """Create a new schema.

        Args:
            schema_name: Name of the schema to create.
        """
        conn = self._ensure_connected()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.commit()
        self.schema = schema_name

    def drop_schema(self, schema_name: str, cascade: bool = True) -> None:
        """Drop a schema.

        Args:
            schema_name: Name of the schema to drop.
            cascade: If True, drop all objects in the schema.
        """
        conn = self._ensure_connected()
        cascade_clause = "CASCADE" if cascade else ""
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema_name} {cascade_clause}")
        conn.commit()

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the current schema.

        Args:
            table_name: Name of the table to check.

        Returns:
            True if the table exists.
        """
        conn = self._ensure_connected()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = %s
                      AND table_name = %s
                )
            """, (self.schema, table_name))
            result = cur.fetchone()
            return result[0] if result else False

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._conn:
            self._conn.rollback()

    def commit(self) -> None:
        """Commit the current transaction."""
        if self._conn:
            self._conn.commit()

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Fetch row counts and index info for SOTA context.

        Args:
            table_name: Name of the table to get stats for.

        Returns:
            Dictionary with row_count, indexes list, and primary_key columns.
        """
        stats: dict[str, Any] = {"row_count": 0, "indexes": [], "primary_key": []}

        try:
            # 1. Row count
            result = self.execute(f'SELECT COUNT(*) as cnt FROM "{table_name}"')
            stats["row_count"] = result[0]["cnt"] if result else 0

            # 2. Indexes from pg_indexes
            idx_result = self.execute(
                """
                SELECT indexname FROM pg_indexes
                WHERE tablename = %s AND schemaname = %s
                """,
                (table_name, self.schema),
            )
            stats["indexes"] = [r["indexname"] for r in idx_result]

            # 3. Primary key columns from pg_constraint
            pk_result = self.execute(
                """
                SELECT a.attname
                FROM pg_constraint c
                JOIN pg_attribute a ON a.attnum = ANY(c.conkey) AND a.attrelid = c.conrelid
                WHERE c.contype = 'p'
                  AND c.conrelid = %s::regclass
                """,
                (f"{self.schema}.{table_name}",),
            )
            stats["primary_key"] = [r["attname"] for r in pk_result]

        except Exception:
            # Stats are optional, don't fail
            pass

        return stats

    def get_column_stats(self, table_name: str, column_name: str) -> dict[str, Any]:
        """Fetch column-level statistics for SOTA context.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.

        Returns:
            Dictionary with distinct_count, null_ratio, min_value, max_value.
        """
        stats: dict[str, Any] = {
            "distinct_count": None,
            "null_ratio": None,
            "min_value": None,
            "max_value": None,
        }

        try:
            # Get stats in one query
            result = self.execute(
                f'''
                SELECT
                    COUNT(DISTINCT "{column_name}") as distinct_count,
                    COUNT(*) as total_count,
                    COUNT(*) - COUNT("{column_name}") as null_count,
                    MIN("{column_name}")::text as min_val,
                    MAX("{column_name}")::text as max_val
                FROM "{table_name}"
                '''
            )

            if result:
                r = result[0]
                stats["distinct_count"] = r["distinct_count"]
                total = r["total_count"] or 0
                stats["null_ratio"] = (
                    round(r["null_count"] / total, 4) if total > 0 else 0.0
                )
                stats["min_value"] = r["min_val"]
                stats["max_value"] = r["max_val"]

        except Exception:
            # Stats are optional, don't fail
            pass

        return stats

    def get_full_table_stats(
        self, table_name: str, include_column_stats: bool = True
    ) -> dict[str, Any]:
        """Get comprehensive table statistics including column-level stats.

        Args:
            table_name: Name of the table.
            include_column_stats: If True, include per-column statistics.

        Returns:
            Dictionary with table stats and optionally column stats.
        """
        stats = self.get_table_stats(table_name)

        if include_column_stats:
            schema_info = self.get_schema_info()
            tables = schema_info.get("tables", {})
            if isinstance(tables, list):
                table_cols = []
                for tbl in tables:
                    name = tbl.get("name") or tbl.get("table_name")
                    if name == table_name:
                        table_cols = tbl.get("columns", [])
                        break
            elif isinstance(tables, dict):
                table_cols = tables.get(table_name, [])
            else:
                table_cols = []

            if table_cols:
                stats["columns"] = {}
                for col in table_cols:
                    col_name = col["name"]
                    stats["columns"][col_name] = self.get_column_stats(
                        table_name, col_name
                    )

        return stats
