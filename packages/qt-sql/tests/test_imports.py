"""Smoke Tests - Import Validation for qt-sql.

Validates that all qt-sql modules can be imported correctly,
including cross-package imports from qt-shared.
"""

import pytest


class TestQtSqlCoreImports:
    """Test that all core qt-sql modules import without errors."""

    def test_import_qt_sql_root(self):
        """Test root package import."""
        import qt_sql
        assert hasattr(qt_sql, "Pipeline")
        assert hasattr(qt_sql, "OptimizationMode")

    def test_import_pipeline(self):
        """Test pipeline import."""
        from qt_sql.pipeline import Pipeline
        assert Pipeline is not None

    def test_import_schemas(self):
        """Test schemas import."""
        from qt_sql.schemas import (
            OptimizationMode,
            SessionResult,
            WorkerResult,
            ValidationStatus,
        )
        assert OptimizationMode is not None
        assert WorkerResult is not None

    def test_import_runner(self):
        """Test runner import."""
        from qt_sql.runner import ADORunner, ADOConfig
        assert ADORunner is not None
        assert ADOConfig is not None

    def test_import_dag(self):
        """Test DAG module import."""
        from qt_sql.dag import DagBuilder, CostAnalyzer
        assert DagBuilder is not None
        assert CostAnalyzer is not None

    def test_import_generate(self):
        """Test generate module import."""
        from qt_sql.generate import CandidateGenerator
        assert CandidateGenerator is not None

    def test_import_sql_rewriter(self):
        """Test SQL rewriter import."""
        from qt_sql.sql_rewriter import SQLRewriter
        assert SQLRewriter is not None


class TestQtSqlExecutionImports:
    """Test execution module imports."""

    def test_import_execution_base(self):
        """Test execution base classes import."""
        from qt_sql.execution.base import (
            DBExecutor,
            PlanNode,
            ExecutionPlanAnalysis,
        )
        assert DBExecutor is not None
        assert PlanNode is not None

    def test_import_duckdb_executor(self):
        """Test DuckDB executor import."""
        from qt_sql.execution.duckdb_executor import DuckDBExecutor
        assert DuckDBExecutor is not None

    def test_import_postgres_executor(self):
        """Test PostgreSQL executor import."""
        from qt_sql.execution.postgres_executor import PostgresExecutor
        assert PostgresExecutor is not None

    def test_import_factory(self):
        """Test executor factory import."""
        from qt_sql.execution.factory import create_executor_from_dsn
        assert callable(create_executor_from_dsn)

    def test_import_plan_parser(self):
        """Test plan parser import."""
        from qt_sql.execution.plan_parser import (
            DuckDBPlanParser,
            analyze_plan,
            build_plan_summary,
        )
        assert DuckDBPlanParser is not None
        assert callable(analyze_plan)
        assert callable(build_plan_summary)


class TestQtSqlPromptImports:
    """Test prompt module imports."""

    def test_import_prompts_init(self):
        """Test prompts package import."""
        from qt_sql.prompts import (
            build_snipe_analyst_prompt,
            build_sniper_prompt,
            SnipeAnalysis,
            parse_snipe_response,
        )
        assert callable(build_snipe_analyst_prompt)
        assert callable(build_sniper_prompt)

    def test_import_worker_prompt(self):
        """Test worker prompt import."""
        from qt_sql.prompts.worker import build_worker_prompt
        assert callable(build_worker_prompt)

    def test_import_analyst_briefing(self):
        """Test analyst briefing import."""
        from qt_sql.prompts.analyst_briefing import build_analyst_briefing_prompt
        assert callable(build_analyst_briefing_prompt)

    def test_import_swarm_parsers(self):
        """Test swarm parsers import."""
        from qt_sql.prompts.swarm_parsers import (
            BriefingShared,
            BriefingWorker,
            ParsedBriefing,
            parse_briefing_response,
        )
        assert BriefingShared is not None
        assert callable(parse_briefing_response)


class TestQtSqlSessionImports:
    """Test session module imports."""

    def test_import_sessions(self):
        """Test sessions package import."""
        from qt_sql.sessions import (
            SwarmSession,
            OneshotSession,
            ExpertSession,
        )
        assert SwarmSession is not None
        assert OneshotSession is not None
        assert ExpertSession is not None


class TestQtSqlValidationImports:
    """Test validation module imports."""

    def test_import_sql_validator(self):
        """Test SQL validator import."""
        from qt_sql.validation.sql_validator import SQLValidator
        assert SQLValidator is not None


class TestQtSqlCrossPackageImports:
    """Test cross-package imports from qt-shared work correctly."""

    def test_import_qt_shared_from_qt_sql(self):
        """Verify qt-sql can import qt-shared components."""
        from qt_shared.config import get_settings
        from qt_shared.auth import UserContext
        from qt_shared.llm import create_llm_client
        assert callable(get_settings)
        assert UserContext is not None
