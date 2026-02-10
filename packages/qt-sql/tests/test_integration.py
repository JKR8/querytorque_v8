"""Integration tests for qt_sql pipeline components.

Tests cross-module interactions: DAG parsing, knowledge retrieval,
SQL rewriting, validation, and execution.
"""

import pytest
from unittest.mock import MagicMock, patch


class TestDAGPipeline:
    """Tests for the logical tree (DAG) parsing pipeline."""

    def test_simple_query_parsing(self):
        """Test DAG builds from a simple query."""
        from qt_sql.dag import LogicalTreeBuilder, CostAnalyzer

        sql = "SELECT id, name FROM users WHERE active = true"
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        costs = CostAnalyzer(dag).analyze()

        assert dag is not None
        assert len(dag.nodes) >= 1
        assert costs is not None

    def test_cte_structure_extraction(self):
        """Test CTE structure is correctly extracted."""
        from qt_sql.dag import LogicalTreeBuilder

        sql = """
        WITH active_users AS (
            SELECT id, name FROM users WHERE active = true
        ),
        user_orders AS (
            SELECT u.id, COUNT(*) as order_count
            FROM active_users u
            JOIN orders o ON u.id = o.user_id
            GROUP BY u.id
        )
        SELECT * FROM user_orders WHERE order_count > 5
        """
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()

        assert len(dag.nodes) >= 2  # At least 2 CTEs + main
        node_names = list(dag.nodes.keys())
        assert any("active_users" in name for name in node_names)

    def test_join_structure_extraction(self):
        """Test JOIN structure is correctly extracted."""
        from qt_sql.dag import LogicalTreeBuilder

        sql = """
        SELECT u.id, o.order_id, p.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN products p ON o.product_id = p.id
        """
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        assert dag is not None
        assert len(dag.nodes) >= 1

    def test_subquery_structure_extraction(self):
        """Test subquery structure is correctly extracted."""
        from qt_sql.dag import LogicalTreeBuilder

        sql = """
        SELECT * FROM users
        WHERE id IN (
            SELECT user_id FROM orders
            WHERE amount > (SELECT AVG(amount) FROM orders)
        )
        """
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        assert dag is not None


class TestDuckDBIntegration:
    """Tests for DuckDB execution integration."""

    @pytest.mark.duckdb
    def test_duckdb_executor_execute(self):
        """Test DuckDB execution."""
        from qt_sql.execution import DuckDBExecutor

        executor = DuckDBExecutor(":memory:")
        executor.connect()
        try:
            result = executor.execute("SELECT 1 as value")
            assert result is not None
        finally:
            executor.close()

    @pytest.mark.duckdb
    def test_duckdb_executor_with_tables(self, duckdb_connection):
        """Test DuckDB execution with sample tables."""
        results = duckdb_connection.execute("SELECT * FROM users").fetchall()
        assert len(results) > 0

    @pytest.mark.duckdb
    def test_duckdb_explain(self):
        """Test DuckDB EXPLAIN."""
        from qt_sql.execution import DuckDBExecutor

        executor = DuckDBExecutor(":memory:")
        executor.connect()
        try:
            result = executor.explain("SELECT 1")
            assert result is not None
        finally:
            executor.close()

    @pytest.mark.duckdb
    def test_duckdb_validate_sql(self, duckdb_connection):
        """Test SQL validation via DuckDB."""
        duckdb_connection.execute("SELECT id FROM users WHERE id = 1")

        with pytest.raises(Exception):
            duckdb_connection.execute("SELEKT * FORM users")


class TestCrossPackageIntegration:
    """Tests for cross-package integration."""

    def test_shared_config_available(self):
        """Test that shared config is importable and works."""
        from qt_shared.config import get_settings

        settings = get_settings()
        assert settings is not None

    @patch("qt_shared.llm.factory.create_llm_client")
    def test_optimization_with_llm_mock(self, mock_create_client):
        """Test LLM client integration pattern."""
        mock_client = MagicMock()
        mock_client.analyze.return_value = "SELECT id, name FROM users WHERE active = true"
        mock_create_client.return_value = mock_client

        from qt_shared.llm import create_llm_client
        client = create_llm_client()

        if client:
            result = client.analyze("Optimize: SELECT * FROM users")
            assert "SELECT" in result


class TestSQLRewriterIntegration:
    """Tests for SQL rewriter integration."""

    def test_transform_inference_from_diff(self):
        """Test AST-based transform inference."""
        from qt_sql.sql_rewriter import infer_transforms_from_sql_diff

        original = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE active = true)"
        optimized = """
        WITH active_customers AS (
            SELECT id FROM customers WHERE active = true
        )
        SELECT o.* FROM orders o
        JOIN active_customers ac ON o.customer_id = ac.id
        """

        transforms = infer_transforms_from_sql_diff(original, optimized, dialect="duckdb")
        assert isinstance(transforms, list)

    def test_set_local_split(self):
        """Test SET LOCAL extraction from SQL."""
        from qt_sql.sql_rewriter import SQLRewriter

        rewriter = SQLRewriter("SELECT 1", dialect="duckdb")
        sql_with_config = """SET LOCAL work_mem = '256MB';
SET LOCAL jit = off;
SELECT id FROM users WHERE active = true"""

        sql, commands = rewriter._split_set_local(sql_with_config)
        assert "SELECT" in sql
        assert len(commands) >= 1


class TestKnowledgeIntegration:
    """Tests for knowledge retrieval integration."""

    def test_tag_recommender_loads(self):
        """Test TagRecommender can be instantiated."""
        from qt_sql.knowledge import TagRecommender

        recommender = TagRecommender()
        assert recommender is not None

    def test_tag_recommender_finds_examples(self):
        """Test TagRecommender returns examples for TPC-DS-like SQL."""
        from qt_sql.knowledge import TagRecommender

        recommender = TagRecommender()
        sql = """
        SELECT d_year, SUM(ss_sales_price) as total
        FROM store_sales
        JOIN date_dim ON ss_sold_date_sk = d_date_sk
        WHERE d_year = 2002
        GROUP BY d_year
        """
        examples = recommender.find_similar_examples(sql, dialect="duckdb", k=3)
        assert isinstance(examples, list)


class TestPrompterIntegration:
    """Tests for prompter integration."""

    def test_prompter_builds_prompt(self):
        """Test Prompter builds a complete prompt."""
        from qt_sql.prompter import Prompter
        from qt_sql.dag import LogicalTreeBuilder, CostAnalyzer

        sql = "SELECT id, name FROM users WHERE active = true"
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        costs = CostAnalyzer(dag).analyze()

        prompter = Prompter()
        prompt = prompter.build_prompt(
            query_id="test_q",
            full_sql=sql,
            dag=dag,
            costs=costs,
            history=None,
            dialect="duckdb",
        )
        assert isinstance(prompt, str)
        assert len(prompt) > 100  # Should be a substantial prompt

    def test_engine_profile_loads(self):
        """Test engine profiles load correctly."""
        from qt_sql.prompter import _load_engine_profile

        for engine in ["duckdb", "postgres"]:
            profile = _load_engine_profile(engine)
            if profile:
                assert "gaps" in profile or "strengths" in profile


class TestDialectIntegration:
    """Tests for dialect-specific handling."""

    def test_duckdb_dag_parsing(self):
        """Test DuckDB-specific SQL parsing."""
        from qt_sql.dag import LogicalTreeBuilder

        sql = """
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price) as rn
            FROM products
        ) WHERE rn = 1
        """
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        assert dag is not None

    def test_postgres_dag_parsing(self):
        """Test PostgreSQL-specific SQL parsing."""
        from qt_sql.dag import LogicalTreeBuilder

        sql = "SELECT * FROM users ORDER BY RANDOM() LIMIT 10"
        dag = LogicalTreeBuilder(sql, dialect="postgres").build()
        assert dag is not None


class TestErrorHandlingIntegration:
    """Tests for error handling across the pipeline."""

    def test_dag_handles_invalid_sql(self):
        """Test DAG handles invalid SQL gracefully."""
        from qt_sql.dag import LogicalTreeBuilder

        # Should not crash on invalid SQL
        try:
            dag = LogicalTreeBuilder("SELEKT * FORM users", dialect="duckdb").build()
            # If it builds, it should still be valid
            assert dag is not None
        except Exception:
            # Acceptable to raise on invalid SQL
            pass

    def test_dag_handles_empty_input(self):
        """Test DAG handles empty input."""
        from qt_sql.dag import LogicalTreeBuilder

        try:
            dag = LogicalTreeBuilder("", dialect="duckdb").build()
            assert dag is not None
        except Exception:
            pass  # Acceptable

    def test_dag_handles_complex_sql(self):
        """Test DAG handles complex SQL with many CTEs."""
        from qt_sql.dag import LogicalTreeBuilder

        ctes = ", ".join([
            f"cte{i} AS (SELECT {i} AS val)"
            for i in range(10)
        ])
        sql = f"WITH {ctes} SELECT * FROM cte0"

        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        assert dag is not None
        assert len(dag.nodes) >= 10


class TestPerformanceIntegration:
    """Tests for performance characteristics."""

    @pytest.mark.slow
    def test_dag_parsing_performance(self):
        """Test that DAG parsing completes in reasonable time."""
        import time
        from qt_sql.dag import LogicalTreeBuilder, CostAnalyzer

        sql = """
        WITH base AS (
            SELECT * FROM very_large_table WHERE date > '2024-01-01'
        ),
        aggregated AS (
            SELECT category, SUM(amount) as total
            FROM base
            GROUP BY category
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (ORDER BY total DESC) as rank
            FROM aggregated
        )
        SELECT * FROM ranked WHERE rank <= 10
        """

        start = time.time()
        dag = LogicalTreeBuilder(sql, dialect="duckdb").build()
        costs = CostAnalyzer(dag).analyze()
        elapsed = time.time() - start

        assert elapsed < 2.0
        assert dag is not None

    @pytest.mark.slow
    def test_batch_dag_parsing_performance(self):
        """Test parsing multiple queries."""
        import time
        from qt_sql.dag import LogicalTreeBuilder

        queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM orders WHERE status = 'pending'",
            "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        ]

        start = time.time()
        results = [LogicalTreeBuilder(q, dialect="duckdb").build() for q in queries]
        elapsed = time.time() - start

        assert elapsed < 3.0
        assert all(r is not None for r in results)
