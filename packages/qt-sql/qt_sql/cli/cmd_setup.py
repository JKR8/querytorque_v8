"""qt setup — Initialize a new benchmark environment from a database connection.

Creates a benchmark directory with config.json from a DSN, discovers tables
and row counts, and optionally discovers slow queries from pg_stat_statements.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path

import click


@click.command()
@click.argument("name")
@click.option("--dsn", prompt="Database DSN", help="Database connection string (e.g., postgres://user:pass@host/db).")
@click.option(
    "--engine",
    type=click.Choice(["postgresql", "duckdb", "snowflake"]),
    default=None,
    help="Database engine (auto-detected from DSN if not provided).",
)
@click.option("--output-dir", default=None, help="Parent directory for the benchmark (default: current dir).")
@click.option("--discover-queries", is_flag=True, help="Discover slow queries from pg_stat_statements.")
@click.option("--limit", default=50, show_default=True, help="Max queries to discover.")
@click.pass_context
def setup(
    ctx: click.Context,
    name: str,
    dsn: str,
    engine: str | None,
    output_dir: str | None,
    discover_queries: bool,
    limit: int,
) -> None:
    """Initialize a new benchmark from a database connection.

    Creates a benchmark directory with config.json, discovers the database
    schema, and optionally imports slow queries from pg_stat_statements.

    Example:
        qt setup my_bench --dsn postgres://user:pass@localhost/mydb
        qt setup my_bench --dsn postgres://user:pass@localhost/mydb --discover-queries
    """
    from ._common import print_header, print_success, print_error

    print_header(f"Setting up benchmark: {name}")

    # Auto-detect engine from DSN
    if engine is None:
        engine = _detect_engine(dsn)
        click.echo(f"  Auto-detected engine: {engine}")

    # Create benchmark directory
    parent = Path(output_dir) if output_dir else Path.cwd()
    bench_dir = parent / name
    queries_dir = bench_dir / "queries"

    if bench_dir.exists():
        print_error(f"Directory already exists: {bench_dir}")
        raise SystemExit(1)

    bench_dir.mkdir(parents=True)
    queries_dir.mkdir()

    # Test connectivity
    click.echo("  Testing database connection...")
    try:
        from qt_sql.execution.factory import create_executor_from_dsn

        executor = create_executor_from_dsn(dsn)
        executor.connect()
        schema_info = executor.get_schema_info(include_row_counts=True)
        tables = schema_info.get("tables", [])
        click.echo(f"  Connected: {len(tables)} tables found")
    except Exception as e:
        print_error(f"Connection failed: {e}")
        # Clean up entire tree (queries/ may already exist)
        shutil.rmtree(bench_dir, ignore_errors=True)
        raise SystemExit(1)

    # Build config.json
    config = {
        "benchmark": name,
        "engine": engine,
        "db_path_or_dsn": dsn,
        "benchmark_dsn": dsn,
        "queries_dir": "queries",
        "mode": "swarm",
        "max_iterations": 3,
        "target_speedup": 1.10,
        "tables": [
            {"name": t["name"], "row_count": t.get("row_count", 0)}
            for t in tables
        ],
    }

    config_path = bench_dir / "config.json"
    config_path.write_text(json.dumps(config, indent=2))
    click.echo(f"  Created: {config_path}")

    # Discover slow queries from pg_stat_statements
    if discover_queries and engine == "postgresql":
        click.echo("  Discovering slow queries from pg_stat_statements...")
        try:
            discovered = _discover_slow_queries(executor, limit=limit)
            for i, q in enumerate(discovered):
                query_file = queries_dir / f"query{i+1:03d}.sql"
                query_file.write_text(q["sql"])
            click.echo(f"  Discovered {len(discovered)} queries")
        except Exception as e:
            click.echo(f"  Warning: query discovery failed: {e}")
    elif discover_queries:
        click.echo(f"  Warning: --discover-queries only supported for postgresql (got {engine})")

    try:
        executor.close()
    except Exception:
        pass

    print_success(f"Benchmark '{name}' ready at {bench_dir}")
    click.echo(f"\n  Next steps:")
    click.echo(f"    qt status {name}")
    click.echo(f"    qt prepare {name}")
    click.echo(f"    qt run {name}")


def _detect_engine(dsn: str) -> str:
    """Auto-detect database engine from DSN string."""
    lower = dsn.lower()
    if lower.startswith(("postgres://", "postgresql://")):
        return "postgresql"
    elif lower.startswith("snowflake://") or "snowflake" in lower:
        return "snowflake"
    elif lower.endswith(".db") or lower.endswith(".duckdb") or lower == ":memory:":
        return "duckdb"
    else:
        return "postgresql"


def _discover_slow_queries(executor, limit: int = 50) -> list:
    """Discover slow queries from pg_stat_statements.

    Returns list of dicts with sql, calls, mean_time_ms, total_time_ms.
    """
    discover_sql = """
        SELECT
            query,
            calls,
            mean_exec_time AS mean_time_ms,
            total_exec_time AS total_time_ms
        FROM pg_stat_statements
        WHERE mean_exec_time > 100
          AND query NOT LIKE '%pg_stat%'
          AND query NOT LIKE 'SET %'
          AND query NOT LIKE 'SHOW %'
          AND calls > 1
        ORDER BY mean_exec_time DESC
        LIMIT %s
    """ % limit

    rows = executor.execute(discover_sql)
    return [
        {
            "sql": row["query"],
            "calls": row["calls"],
            "mean_time_ms": row["mean_time_ms"],
            "total_time_ms": row["total_time_ms"],
        }
        for row in rows
    ]
