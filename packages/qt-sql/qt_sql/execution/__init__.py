"""SQL execution engines for QueryTorque SQL.

Executor imports are lazy — only loaded when accessed. This allows running
against a single engine (e.g. Snowflake) without installing drivers for
all engines (psycopg2, duckdb).
"""

from .factory import (
    # Protocol
    DatabaseExecutor,
    # Config classes
    DatabaseConfig,
    DuckDBConfig,
    PostgresConfig,
    SnowflakeConfig,
    # Factory functions
    create_executor,
    create_executor_from_dsn,
    create_executor_from_cli_args,
    create_executor_from_args,  # alias
    # Registry functions
    register_config,
    get_config_class,
)
from .database_utils import (
    extract_table_names,
    fetch_schema_with_stats,
    run_explain_analyze,
    run_explain_text,
    get_duckdb_engine_info,
)
from .plan_parser import (
    DuckDBPlanParser,
    PlanAnalyzer,
    PlanIssue,
    build_plan_summary,
    analyze_plan,
)


def __getattr__(name: str):
    """Lazy import for concrete executor classes."""
    if name == "DuckDBExecutor":
        from .duckdb_executor import DuckDBExecutor
        return DuckDBExecutor
    if name == "PostgresExecutor":
        from .postgres_executor import PostgresExecutor
        return PostgresExecutor
    if name == "SnowflakeExecutor":
        from .snowflake_executor import SnowflakeExecutor
        return SnowflakeExecutor
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Executors (lazy)
    "DuckDBExecutor",
    "PostgresExecutor",
    "SnowflakeExecutor",
    # Protocol
    "DatabaseExecutor",
    # Config classes
    "DatabaseConfig",
    "DuckDBConfig",
    "PostgresConfig",
    "SnowflakeConfig",
    # Factory functions
    "create_executor",
    "create_executor_from_dsn",
    "create_executor_from_cli_args",
    "create_executor_from_args",
    # Registry functions
    "register_config",
    "get_config_class",
    # Database utils
    "extract_table_names",
    "fetch_schema_with_stats",
    "run_explain_analyze",
    "run_explain_text",
    "get_duckdb_engine_info",
    # Plan parsing
    "DuckDBPlanParser",
    "PlanAnalyzer",
    "PlanIssue",
    "build_plan_summary",
    "analyze_plan",
]
