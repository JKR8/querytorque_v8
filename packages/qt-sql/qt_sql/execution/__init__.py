"""SQL execution engines for QueryTorque SQL."""

from .duckdb_executor import DuckDBExecutor
from .postgres_executor import PostgresExecutor
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

__all__ = [
    # Executors
    "DuckDBExecutor",
    "PostgresExecutor",
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
