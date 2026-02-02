"""Phase 1: Smoke Tests - Import Validation for qt-sql.

Validates that all qt-sql modules can be imported correctly,
including cross-package imports from qt-shared.
"""

import pytest


class TestQtSqlImports:
    """Test that all qt-sql modules import without errors."""

    def test_import_qt_sql_root(self):
        """Test root package import."""
        import qt_sql
        assert hasattr(qt_sql, "__version__")
        assert hasattr(qt_sql, "detect_antipatterns")
        assert hasattr(qt_sql, "CalciteClient")

    def test_import_ast_detector_base(self):
        """Test AST detector base classes import."""
        from qt_sql.analyzers.ast_detector.base import (
            ASTContext,
            ASTRule,
            ASTDetector,
            RuleMatch,
        )
        assert ASTContext is not None
        assert ASTRule is not None
        assert ASTDetector is not None

    def test_import_ast_detector_registry(self):
        """Test AST detector registry imports."""
        from qt_sql.analyzers.ast_detector.registry import (
            get_all_rules,
            get_rule_by_id,
            get_rules_by_category,
            get_categories,
            get_rule_count,
        )
        assert callable(get_all_rules)
        assert callable(get_rule_by_id)
        assert callable(get_rule_count)

    def test_import_sql_antipattern_detector(self):
        """Test main detector imports."""
        from qt_sql.analyzers.sql_antipattern_detector import (
            SQLIssue,
            SQLAnalysisResult,
            SQLAntiPatternDetector,
            analyze_sql,
        )
        assert SQLIssue is not None
        assert SQLAntiPatternDetector is not None
        assert callable(analyze_sql)

    def test_import_sql_remediation_payload(self):
        """Test remediation payload imports."""
        from qt_sql.analyzers.sql_remediation_payload import (
            SQLRemediationPayload,
            SQLRemediationPayloadGenerator,
            generate_sql_remediation_payload,
        )
        assert SQLRemediationPayload is not None
        assert callable(generate_sql_remediation_payload)

    def test_import_calcite_client(self):
        """Test Calcite client imports."""
        from qt_sql.calcite_client import (
            CalciteClient,
            CalciteResult,
            get_calcite_client,
        )
        assert CalciteClient is not None
        assert CalciteResult is not None
        assert callable(get_calcite_client)

    def test_import_sql_parser(self):
        """Test SQL parser imports."""
        from qt_sql.sql_parser import (
            SQLParser,
            QueryGraph,
            TableNode,
            JoinEdge,
            ColumnLineage,
            parse_sql,
        )
        assert SQLParser is not None
        assert QueryGraph is not None
        assert callable(parse_sql)

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


class TestQtSqlRuleImports:
    """Test that all rule modules import without errors."""

    def test_import_select_rules(self):
        """Test SELECT rules import."""
        from qt_sql.analyzers.ast_detector.rules.select_rules import (
            SelectStarRule,
            ScalarSubqueryInSelectRule,
            MultipleScalarSubqueriesRule,
            CorrelatedSubqueryInSelectRule,
            DistinctCrutchRule,
            ScalarUDFInSelectRule,
        )
        assert SelectStarRule is not None
        assert ScalarSubqueryInSelectRule is not None

    def test_import_where_rules(self):
        """Test WHERE rules import."""
        from qt_sql.analyzers.ast_detector.rules.where_rules import (
            FunctionOnColumnRule,
            LeadingWildcardRule,
            NotInSubqueryRule,
            ImplicitTypeConversionRule,
        )
        assert FunctionOnColumnRule is not None

    def test_import_join_rules(self):
        """Test JOIN rules import."""
        from qt_sql.analyzers.ast_detector.rules.join_rules import (
            CartesianJoinRule,
            ImplicitJoinRule,
            FunctionInJoinRule,
            TooManyJoinsRule,
        )
        assert CartesianJoinRule is not None

    def test_import_cte_rules(self):
        """Test CTE rules import."""
        from qt_sql.analyzers.ast_detector.rules.cte_rules import (
            SelectStarInCTERule,
            MultiRefCTERule,
            RecursiveCTERule,
            DeeplyNestedCTERule,
        )
        assert MultiRefCTERule is not None

    def test_import_union_rules(self):
        """Test UNION rules import."""
        from qt_sql.analyzers.ast_detector.rules.union_rules import (
            UnionWithoutAllRule,
            LargeUnionChainRule,
        )
        assert UnionWithoutAllRule is not None

    def test_import_order_rules(self):
        """Test ORDER BY rules import."""
        from qt_sql.analyzers.ast_detector.rules.order_rules import (
            OrderByInSubqueryRule,
            OrderByWithoutLimitRule,
            OrderByOrdinalRule,
        )
        assert OrderByInSubqueryRule is not None

    def test_import_window_rules(self):
        """Test window function rules import."""
        from qt_sql.analyzers.ast_detector.rules.window_rules import (
            RowNumberWithoutOrderRule,
            MultipleWindowPartitionsRule,
            WindowWithoutPartitionRule,
        )
        assert RowNumberWithoutOrderRule is not None

    def test_import_aggregation_rules(self):
        """Test aggregation rules import."""
        from qt_sql.analyzers.ast_detector.rules.aggregation_rules import (
            GroupByOrdinalRule,
            HavingWithoutAggregateRule,
            DistinctInsideAggregateRule,
        )
        assert GroupByOrdinalRule is not None

    def test_import_subquery_rules(self):
        """Test subquery rules import."""
        from qt_sql.analyzers.ast_detector.rules.subquery_rules import (
            CorrelatedSubqueryInWhereRule,
            SubqueryInsteadOfJoinRule,
            DeeplyNestedSubqueryRule,
        )
        assert CorrelatedSubqueryInWhereRule is not None

    def test_import_cursor_rules(self):
        """Test cursor rules import."""
        from qt_sql.analyzers.ast_detector.rules.cursor_rules import (
            CursorUsageRule,
            WhileLoopRule,
            DynamicSQLRule,
        )
        assert CursorUsageRule is not None

    def test_import_type_rules(self):
        """Test type conversion rules import."""
        from qt_sql.analyzers.ast_detector.rules.type_rules import (
            StringNumericComparisonRule,
            DateAsStringRule,
        )
        assert StringNumericComparisonRule is not None

    def test_import_snowflake_rules(self):
        """Test Snowflake-specific rules import."""
        from qt_sql.analyzers.ast_detector.rules.snowflake_rules import (
            CopyIntoWithoutFileFormatRule,
            SelectWithoutLimitOrSampleRule,
        )
        assert CopyIntoWithoutFileFormatRule is not None

    def test_import_postgres_rules(self):
        """Test PostgreSQL-specific rules import."""
        from qt_sql.analyzers.ast_detector.rules.postgres_rules import (
            CountStarInsteadOfExistsRule,
            LargeInListRule,
        )
        assert CountStarInsteadOfExistsRule is not None

    def test_import_duckdb_rules(self):
        """Test DuckDB-specific rules import."""
        from qt_sql.analyzers.ast_detector.rules.duckdb_rules import (
            NotUsingQualifyRule,
            NotUsingGroupByAllRule,
        )
        assert NotUsingQualifyRule is not None


class TestQtSqlCrossPackageImports:
    """Test cross-package imports from qt-shared work correctly."""

    def test_import_qt_shared_from_qt_sql(self):
        """Verify qt-sql can import qt-shared components."""
        from qt_shared.config import get_settings
        from qt_shared.auth import UserContext
        from qt_shared.llm import create_llm_client
        assert callable(get_settings)
        assert UserContext is not None

    def test_api_imports_shared_auth(self):
        """API module should import shared auth."""
        from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector
        # Verify detector works independently
        detector = SQLAntiPatternDetector(dialect="generic")
        assert detector is not None


class TestQtSqlCliImports:
    """Test CLI module imports."""

    def test_import_cli_main(self):
        """Test CLI main module import."""
        from cli.main import cli, audit, optimize, validate
        import click
        assert isinstance(cli, click.Group)
        assert callable(audit)
        assert callable(optimize)
        assert callable(validate)
