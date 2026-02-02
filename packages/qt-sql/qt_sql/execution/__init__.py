"""SQL execution engines for QueryTorque SQL."""

from .duckdb_executor import DuckDBExecutor
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
    "DuckDBExecutor",
    "extract_table_names",
    "fetch_schema_with_stats",
    "run_explain_analyze",
    "run_explain_text",
    "get_duckdb_engine_info",
    "DuckDBPlanParser",
    "PlanAnalyzer",
    "PlanIssue",
    "build_plan_summary",
    "analyze_plan",
]
