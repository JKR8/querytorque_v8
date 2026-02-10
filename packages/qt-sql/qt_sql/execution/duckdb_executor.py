"""DuckDB database executor for SQL analysis."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

try:
    import duckdb
except ImportError as e:
    raise ImportError(
        "DuckDB is not installed. Install with: pip install query-torque[duckdb]"
    ) from e

# Parameter placeholder patterns
PARAM_PATTERN_POSITIONAL = re.compile(r"\$(\d+)")
PARAM_PATTERN_NAMED = re.compile(r":(\w+)")


def _validate_params(sql: str, params: tuple | dict) -> None:
    """Validate parameters match placeholders in SQL.

    Args:
        sql: SQL query with parameter placeholders ($1, $2 or :name)
        params: Parameters as tuple (positional) or dict (named)

    Raises:
        ValueError: If mixing positional and named parameters
        TypeError: If params type doesn't match placeholder style
        ValueError: If required parameters are missing
    """
    positional = set(int(m) for m in PARAM_PATTERN_POSITIONAL.findall(sql))
    named = set(PARAM_PATTERN_NAMED.findall(sql))

    if positional and named:
        raise ValueError("Cannot mix positional ($1) and named (:name) parameters")

    if positional:
        if not isinstance(params, (tuple, list)):
            raise TypeError("Positional parameters ($1, $2, ...) require tuple or list")
        max_idx = max(positional) if positional else 0
        if len(params) < max_idx:
            raise ValueError(
                f"SQL expects {max_idx} positional parameters, got {len(params)}"
            )

    if named:
        if not isinstance(params, Mapping):
            raise TypeError("Named parameters (:name) require dict")
        missing = named - set(params.keys())
        if missing:
            raise ValueError(f"Missing named parameters: {missing}")


class DuckDBExecutor:
    """DuckDB database executor for SQL analysis and execution plan generation.

    Provides connection management, query execution, and EXPLAIN plan generation
    using DuckDB's JSON profiling output.

    Usage:
        with DuckDBExecutor(":memory:") as db:
            db.execute_script("CREATE TABLE t (x INT); INSERT INTO t VALUES (1);")
            plan = db.explain("SELECT * FROM t")

    Args:
        database: Path to database file or ":memory:" for in-memory database.
        read_only: If True, open database in read-only mode.
    """

    def __init__(self, database: str = ":memory:", read_only: bool = False):
        self.database = database
        self.read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        """Open connection to DuckDB."""
        if self._conn is not None:
            return  # Already connected

        # Respect caller-provided temp directory; otherwise only set the historical
        # WSL location when it actually exists.
        import os
        if "DUCKDB_TEMP_DIRECTORY" not in os.environ and os.path.isdir("/mnt/d/duckdb_temp"):
            os.environ["DUCKDB_TEMP_DIRECTORY"] = "/mnt/d/duckdb_temp"

        self._conn = duckdb.connect(
            database=self.database,
            read_only=self.read_only,
        )

    def close(self) -> None:
        """Close connection to DuckDB."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "DuckDBExecutor":
        """Context manager entry - opens connection."""
        self.connect()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit - closes connection."""
        self.close()

    def _ensure_connected(self) -> duckdb.DuckDBPyConnection:
        """Ensure connection is open and return it."""
        if self._conn is None:
            self.connect()
        assert self._conn is not None
        return self._conn

    def execute(
        self,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any] = (),
        validate_params: bool = True,
    ) -> list[dict[str, Any]]:
        """Execute SQL and return results as list of dicts.

        Args:
            sql: SQL query to execute. Supports positional ($1, $2) or named (:name) params.
            params: Query parameters as tuple (positional) or dict (named).
            validate_params: If True, validate params match SQL placeholders.

        Returns:
            List of dictionaries, one per row.

        Example:
            # Positional parameters
            db.execute("SELECT * FROM users WHERE id = $1", (42,))

            # Named parameters
            db.execute("SELECT * FROM users WHERE name = :name", {"name": "Alice"})
        """
        conn = self._ensure_connected()

        if validate_params and params:
            _validate_params(sql, params)

        if params:
            result = conn.execute(sql, params)
        else:
            result = conn.execute(sql)

        columns = [desc[0] for desc in result.description] if result.description else []
        rows = result.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def execute_safe(
        self,
        sql: str,
        params: tuple[Any, ...] | dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Execute query with required parameters (safer than execute).

        This method enforces parameter usage to prevent SQL injection.
        Parameters are required (empty not allowed).

        Args:
            sql: SQL query with parameter placeholders (must use $1/:name, not string formatting)
            params: Required parameters (cannot be empty)

        Returns:
            List of dictionaries, one per row.

        Raises:
            ValueError: If params is empty or sql uses string formatting patterns.

        Example:
            # Safe: parameters are validated and required
            db.execute_safe("SELECT * FROM users WHERE id = $1", (user_id,))

            # Unsafe: would raise ValueError
            db.execute_safe(f"SELECT * FROM users WHERE id = {user_id}", ())
        """
        if not params:
            raise ValueError("execute_safe requires parameters - use execute() for parameterless queries")
        if "%s" in sql or "{" in sql:
            raise ValueError(
                "Detected string formatting in SQL. Use $1/:name parameters instead of %s or {}"
            )
        return self.execute(sql, params, validate_params=True)

    def execute_script(self, sql_script: str) -> None:
        """Execute multi-statement SQL script.

        Splits script by semicolons and executes each statement.
        Useful for schema creation and data seeding.

        Args:
            sql_script: SQL script with multiple statements.
        """
        conn = self._ensure_connected()

        # DuckDB can execute multiple statements directly
        conn.execute(sql_script)

    def explain(self, sql: str, analyze: bool = True) -> dict[str, Any]:
        """Get execution plan as JSON dict with operator timing.

        Uses DuckDB's EXPLAIN (ANALYZE, FORMAT JSON) for actual operator-level timing.

        Args:
            sql: SQL query to explain.
            analyze: If True, run EXPLAIN ANALYZE for actual timing.

        Returns:
            Execution plan as dictionary with plan tree and timing.
        """
        conn = self._ensure_connected()
        result: dict[str, Any] = {}

        try:
            if analyze:
                # Use EXPLAIN (ANALYZE, FORMAT JSON) for timing data
                plan_result = conn.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}").fetchall()
            else:
                # Just structure without timing
                plan_result = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()

            # Parse the JSON result
            # DuckDB returns different plan types:
            # - analyzed_plan: EXPLAIN ANALYZE with timing (has root-level metrics)
            # - physical_plan: EXPLAIN without timing
            for plan_type, plan_json in plan_result:
                parsed = json.loads(plan_json)

                if plan_type == "analyzed_plan" and isinstance(parsed, dict):
                    # analyzed_plan has timing at root level plus children
                    # Root keys: latency, cpu_time, rows_returned, children, etc.
                    result = {
                        "type": "analyzed_plan",
                        "latency": parsed.get("latency", 0),
                        "cpu_time": parsed.get("cpu_time", 0),
                        "rows_returned": parsed.get("rows_returned", 0),
                        "cumulative_cardinality": parsed.get("cumulative_cardinality", 0),
                        "cumulative_rows_scanned": parsed.get("cumulative_rows_scanned", 0),
                        "children": parsed.get("children", []),
                    }
                    break
                elif plan_type == "physical_plan" and isinstance(parsed, dict):
                    # physical_plan is just the plan tree without timing
                    result = {
                        "type": "physical_plan",
                        "children": [parsed] if "children" not in parsed else parsed.get("children", []),
                    }
                    break
            else:
                # Fallback to logical_opt
                for plan_type, plan_json in plan_result:
                    if plan_type == "logical_opt":
                        parsed = json.loads(plan_json)
                        result = {
                            "type": "logical_opt",
                            "children": [parsed] if isinstance(parsed, dict) else parsed,
                        }
                        break

        except Exception as e:
            # Fallback to text plan
            try:
                explain_result = conn.execute(f"EXPLAIN {sql}").fetchall()
                return {
                    "type": "text_plan",
                    "plan_text": "\n".join(f"{t}: {p}" for t, p in explain_result),
                    "error": str(e),
                }
            except Exception:
                return {"type": "error", "error": str(e)}

        return result

    def _extract_total_timing(self, children: list[dict[str, Any]]) -> float:
        """Extract total timing from plan nodes (in seconds)."""
        total = 0.0
        for child in children:
            # DuckDB uses 'timing' for operator timing in seconds
            timing = child.get("timing", child.get("operator_timing", 0))
            if isinstance(timing, (int, float)):
                total += timing
            # Recurse into children
            if "children" in child:
                total += self._extract_total_timing(child["children"])
        return total

    def _extract_cardinality(self, children: list[dict[str, Any]]) -> int:
        """Extract estimated cardinality from plan nodes."""
        for child in children:
            extra_info = child.get("extra_info", {})
            if isinstance(extra_info, dict):
                cardinality = extra_info.get("Estimated Cardinality", "0")
                try:
                    return int(cardinality)
                except (ValueError, TypeError):
                    pass
        return 0

    def get_schema_info(self, include_row_counts: bool = True) -> dict[str, Any]:
        """Get schema information (tables, columns) from the database.

        Returns:
            Dictionary with a list of table definitions.
        """
        conn = self._ensure_connected()

        tables_result = conn.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
        """).fetchall()

        tables_list = []

        for (table_name,) in tables_result:
            columns_result = conn.execute(f"""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """).fetchall()

            row_count = None
            if include_row_counts:
                # Prefer metadata/estimated counts over full scans.
                try:
                    res = conn.execute(
                        "SELECT estimated_row_count FROM duckdb_tables() WHERE table_name = ?",
                        [table_name],
                    ).fetchone()
                    if res and res[0] is not None:
                        row_count = int(res[0])
                except Exception:
                    pass
                if row_count is None:
                    try:
                        res = conn.execute(
                            """
                            SELECT table_rows
                            FROM information_schema.tables
                            WHERE table_schema = 'main' AND table_name = ?
                            """,
                            [table_name],
                        ).fetchone()
                        if res and res[0] is not None:
                            row_count = int(res[0])
                    except Exception:
                        pass
                if row_count is None:
                    try:
                        res = conn.execute(
                            f"SELECT count(*) FROM {table_name}"
                        ).fetchone()
                        row_count = res[0] if res else 0
                    except Exception:
                        row_count = None

            indexes = []
            try:
                idx_res = conn.execute(
                    "SELECT index_name FROM duckdb_indexes() WHERE table_name = ?",
                    [table_name],
                ).fetchall()
                indexes = [r[0] for r in idx_res]
            except Exception:
                pass

            primary_key = []
            try:
                pk_res = conn.execute(
                    """
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_name = ?
                    """,
                    [table_name],
                ).fetchall()
                primary_key = [r[0] for r in pk_res]
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
                        "data_type": col[1],
                        "nullable": col[2] == "YES",
                    }
                    for col in columns_result
                ]
            })

        return {"tables": tables_list}

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Fetch row counts and index info for SOTA context.

        Args:
            table_name: Name of the table to get stats for.

        Returns:
            Dictionary with row_count and indexes list.
        """
        stats: dict[str, Any] = {"row_count": 0, "indexes": [], "primary_key": []}
        conn = self._ensure_connected()

        try:
            # 1. Row Count
            res = conn.execute(f"SELECT count(*) FROM {table_name}").fetchone()
            stats["row_count"] = res[0] if res else 0

            # 2. Indexes (DuckDB specific)
            try:
                idx_res = conn.execute(
                    f"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table_name}'"
                ).fetchall()
                stats["indexes"] = [r[0] for r in idx_res]
            except Exception:
                # duckdb_indexes may not exist in older versions
                pass

            # 3. Primary Key columns
            try:
                pk_res = conn.execute(f"""
                    SELECT column_name
                    FROM information_schema.key_column_usage
                    WHERE table_name = '{table_name}'
                """).fetchall()
                stats["primary_key"] = [r[0] for r in pk_res]
            except Exception:
                pass

        except Exception:
            # Log but don't crash - stats are optional
            pass

        return stats

    def get_column_stats(
        self, table_name: str, column_name: str, use_sampling: bool = True
    ) -> dict[str, Any]:
        """Fetch column-level statistics for SOTA context.

        Uses APPROX_COUNT_DISTINCT (HyperLogLog) for production safety.
        This avoids expensive full table scans on large tables.

        Args:
            table_name: Name of the table.
            column_name: Name of the column.
            use_sampling: If True, use approximate counts (default, safe for prod).
                         If False, use exact COUNT(DISTINCT) (only for small tables).

        Returns:
            Dictionary with distinct_count, null_ratio, min_value, max_value.
            distinct_count is approximate when use_sampling=True.
        """
        stats: dict[str, Any] = {
            "distinct_count": None,
            "null_ratio": None,
            "min_value": None,
            "max_value": None,
            "is_approximate": use_sampling,
        }
        conn = self._ensure_connected()

        try:
            # Get row count for ratio calculation
            row_count_result = conn.execute(
                f'SELECT COUNT(*) FROM "{table_name}"'
            ).fetchone()
            row_count = row_count_result[0] if row_count_result else 0

            if row_count == 0:
                return stats

            # Use APPROX_COUNT_DISTINCT for production safety (HyperLogLog)
            # This avoids full table scans on large tables
            if use_sampling:
                result = conn.execute(f'''
                    SELECT
                        APPROX_COUNT_DISTINCT("{column_name}") as distinct_count,
                        COUNT(*) - COUNT("{column_name}") as null_count,
                        MIN("{column_name}") as min_val,
                        MAX("{column_name}") as max_val
                    FROM "{table_name}"
                ''').fetchone()
            else:
                # Exact count - only use for small tables or when precision is critical
                result = conn.execute(f'''
                    SELECT
                        COUNT(DISTINCT "{column_name}") as distinct_count,
                        COUNT(*) - COUNT("{column_name}") as null_count,
                        MIN("{column_name}") as min_val,
                        MAX("{column_name}") as max_val
                    FROM "{table_name}"
                ''').fetchone()

            if result:
                stats["distinct_count"] = result[0]
                stats["null_ratio"] = round(result[1] / row_count, 4) if row_count > 0 else 0.0
                stats["min_value"] = result[2]
                stats["max_value"] = result[3]

        except Exception:
            # Stats are optional, don't fail
            pass

        return stats

    def get_full_table_stats(self, table_name: str, include_column_stats: bool = True) -> dict[str, Any]:
        """Get comprehensive table statistics including column-level stats.

        Args:
            table_name: Name of the table.
            include_column_stats: Whether to include per-column statistics.

        Returns:
            Dictionary with row_count, indexes, primary_key, and column_stats.
        """
        stats = self.get_table_stats(table_name)

        if include_column_stats:
            stats["column_stats"] = {}
            schema = self.get_schema_info()
            columns = schema.get("tables", {}).get(table_name, [])

            # Limit to first 20 columns for performance
            for col in columns[:20]:
                col_name = col.get("name")
                if col_name:
                    stats["column_stats"][col_name] = self.get_column_stats(table_name, col_name)

        return stats

    def explain_sampled(
        self, sql: str, sample_pct: float = 2.0, analyze: bool = True
    ) -> dict[str, Any]:
        """Run EXPLAIN ANALYZE on a sampled version of the query.

        Rewrites table references to use TABLESAMPLE BERNOULLI(pct) via
        sqlglot AST manipulation. Produces approximate timing and cardinality
        at a fraction of the cost (~50x speedup on large datasets).

        Use this for plan analysis where relative operator costs matter
        more than exact timing. Full explain() is for equivalence checks
        and exact benchmarking.

        Args:
            sql: SQL query to explain.
            sample_pct: Sample percentage (default 2.0 = 2%).
            analyze: If True, run EXPLAIN ANALYZE for actual timing.

        Returns:
            Execution plan as dictionary (same format as explain()).
        """
        try:
            import sqlglot
            from sqlglot import exp

            parsed = sqlglot.parse_one(sql, error_level=sqlglot.ErrorLevel.IGNORE)

            # Rewrite all base table references with TABLESAMPLE
            for table in parsed.find_all(exp.Table):
                # Skip CTEs, subqueries — only real tables
                if table.name and not table.find_ancestor(exp.CTE):
                    # Build a valid TABLESAMPLE node by parsing a helper statement
                    helper = sqlglot.parse_one(
                        f"SELECT 1 FROM {table.name} TABLESAMPLE BERNOULLI({sample_pct} PERCENT)",
                        error_level=sqlglot.ErrorLevel.IGNORE,
                    )
                    sample_nodes = list(helper.find_all(exp.TableSample))
                    if sample_nodes:
                        table.set("sample", sample_nodes[0])

            sampled_sql = parsed.sql(dialect="duckdb")
            return self.explain(sampled_sql, analyze=analyze)

        except ImportError:
            # sqlglot not available — fall back to full explain
            return self.explain(sql, analyze=analyze)
        except Exception:
            # Any rewrite failure — fall back to full explain
            return self.explain(sql, analyze=analyze)

    def get_cost_estimate(self, sql: str) -> float:
        """Return estimated cardinality as proxy for query cost.

        Uses EXPLAIN (FORMAT JSON) to extract the root cardinality estimate.

        Args:
            sql: SQL query to estimate cost for.

        Returns:
            Estimated cardinality (rows), or infinity if estimation fails.
        """
        conn = self._ensure_connected()

        try:
            plan_result = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()

            for plan_type, plan_json in plan_result:
                parsed = json.loads(plan_json)

                if isinstance(parsed, dict):
                    # Check for cumulative_cardinality at root level
                    if "cumulative_cardinality" in parsed:
                        return float(parsed["cumulative_cardinality"])

                    # Check children for estimated_cardinality
                    children = parsed.get("children", [])
                    if children and isinstance(children[0], dict):
                        extra_info = children[0].get("extra_info", {})
                        if isinstance(extra_info, dict):
                            cardinality = extra_info.get("Estimated Cardinality", "0")
                            try:
                                return float(cardinality)
                            except (ValueError, TypeError):
                                pass

            return float("inf")

        except Exception:
            return float("inf")

    def compare_cost(self, original_sql: str, optimized_sql: str) -> dict[str, Any]:
        """Compare estimated costs between original and optimized queries.

        Args:
            original_sql: Original SQL query.
            optimized_sql: Optimized SQL query.

        Returns:
            Dictionary with original_cost, optimized_cost, reduction_ratio.
        """
        original_cost = self.get_cost_estimate(original_sql)
        optimized_cost = self.get_cost_estimate(optimized_sql)

        # Calculate reduction ratio (positive = improvement)
        if original_cost > 0 and original_cost != float("inf"):
            reduction_ratio = (original_cost - optimized_cost) / original_cost
        else:
            reduction_ratio = 0.0

        return {
            "original_cost": original_cost,
            "optimized_cost": optimized_cost,
            "reduction_ratio": round(reduction_ratio, 4),
            "improved": optimized_cost < original_cost,
        }
