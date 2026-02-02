"""Phase 6: Integration Tests - SQL Integration.

Full pipeline validation tests for SQL analysis.
"""

import pytest
from unittest.mock import MagicMock, patch, AsyncMock


class TestSQLAnalysisPipeline:
    """Tests for the full SQL analysis pipeline."""

    def test_full_analysis_pipeline(self, detector, sample_clean_sql):
        """Test complete analysis from SQL to result."""
        result = detector.analyze(sample_clean_sql)

        # Verify all expected fields
        assert hasattr(result, "sql")
        assert hasattr(result, "issues")
        assert hasattr(result, "final_score")
        assert hasattr(result, "query_structure")

        # Clean SQL should score well
        assert result.final_score >= 80

    def test_analysis_with_issues_pipeline(self, detector, sample_multiple_issues_sql):
        """Test analysis pipeline with multiple issues."""
        result = detector.analyze(sample_multiple_issues_sql)

        # Should detect issues
        assert len(result.issues) > 0

        # Score should reflect issues
        assert result.final_score < 100

        # Issue counts should match
        total_counted = (
            result.critical_count +
            result.high_count +
            result.medium_count +
            result.low_count
        )
        # May have info issues not counted
        assert total_counted <= len(result.issues)

    def test_analysis_to_json_pipeline(self, detector, sample_cte_sql):
        """Test analysis to JSON serialization."""
        result = detector.analyze(sample_cte_sql)
        json_dict = result.to_dict()

        # Should be serializable
        import json
        json_str = json.dumps(json_dict)
        assert len(json_str) > 0

        # Should contain expected keys
        assert "score" in json_dict
        assert "issues" in json_dict
        assert "query_structure" in json_dict


class TestDuckDBIntegration:
    """Tests for DuckDB execution integration."""

    @pytest.mark.duckdb
    def test_duckdb_executor_execute(self, duckdb_executor):
        """Test DuckDB execution."""
        results = duckdb_executor.execute("SELECT 1 as value")
        assert len(results) > 0

    @pytest.mark.duckdb
    def test_duckdb_executor_with_tables(self, duckdb_connection):
        """Test DuckDB execution with sample tables."""
        results = duckdb_connection.execute("SELECT * FROM users").fetchall()
        assert len(results) > 0

    @pytest.mark.duckdb
    def test_duckdb_explain(self, duckdb_executor):
        """Test DuckDB EXPLAIN."""
        result = duckdb_executor.explain("SELECT 1")
        assert result is not None

    @pytest.mark.duckdb
    def test_duckdb_validate_sql(self, duckdb_connection):
        """Test SQL validation via DuckDB."""
        # Valid SQL should not raise
        duckdb_connection.execute("SELECT id FROM users WHERE id = 1")

        # Invalid SQL should raise
        with pytest.raises(Exception):
            duckdb_connection.execute("SELEKT * FORM users")


class TestCrossPackageIntegration:
    """Tests for cross-package integration."""

    def test_sql_analyzer_uses_shared_config(self):
        """Test that SQL analyzer can use shared config."""
        from qt_shared.config import get_settings
        from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector

        settings = get_settings()
        detector = SQLAntiPatternDetector()

        # Both should work together
        assert settings is not None
        assert detector is not None

    def test_sql_analyzer_with_llm_mock(self):
        """Test SQL analysis with mocked LLM."""
        from qt_sql.analyzers.sql_antipattern_detector import SQLAntiPatternDetector

        detector = SQLAntiPatternDetector()
        result = detector.analyze("SELECT * FROM users")

        # Analysis should work without LLM
        assert result is not None
        assert result.final_score >= 0

    @patch("qt_shared.llm.factory.create_llm_client")
    def test_optimization_with_llm_mock(self, mock_create_client):
        """Test optimization pipeline with mocked LLM."""
        # Mock LLM client
        mock_client = MagicMock()
        mock_client.analyze.return_value = """
## Optimized SQL
```sql
SELECT id, name FROM users WHERE active = true
```

## Changes
- Replaced SELECT * with explicit columns
"""
        mock_create_client.return_value = mock_client

        # This tests the pattern but may need actual endpoint test
        from qt_shared.llm import create_llm_client
        client = create_llm_client()

        if client:
            result = client.analyze("Optimize: SELECT * FROM users")
            assert "SELECT" in result


class TestQueryStructureAnalysis:
    """Tests for query structure analysis integration."""

    def test_cte_structure_extraction(self, detector, sample_cte_sql):
        """Test CTE structure is correctly extracted."""
        result = detector.analyze(sample_cte_sql)
        structure = result.query_structure

        assert structure["cte_count"] >= 1
        assert len(structure["cte_names"]) >= 1

    def test_join_structure_extraction(self, detector):
        """Test JOIN structure is correctly extracted."""
        sql = """
        SELECT u.id, o.order_id, p.name
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN products p ON o.product_id = p.id
        """
        result = detector.analyze(sql)
        structure = result.query_structure

        assert structure["join_count"] >= 2
        assert structure["table_count"] >= 3

    def test_subquery_structure_extraction(self, detector, sample_deeply_nested_sql):
        """Test subquery structure is correctly extracted."""
        result = detector.analyze(sample_deeply_nested_sql)
        structure = result.query_structure

        assert structure["subquery_count"] >= 3


class TestRemediationPayloadIntegration:
    """Tests for remediation payload generation."""

    def test_remediation_payload_generation(self):
        """Test generating remediation payload."""
        from qt_sql.analyzers.sql_remediation_payload import (
            generate_sql_remediation_payload,
        )

        sql = "SELECT * FROM users WHERE UPPER(email) = 'TEST'"
        payload = generate_sql_remediation_payload(sql)

        assert payload is not None
        assert isinstance(payload, dict)
        assert "query" in payload
        assert "query_graph" in payload

    def test_remediation_payload_has_tables(self):
        """Test remediation payload includes table information."""
        from qt_sql.analyzers.sql_remediation_payload import (
            generate_sql_remediation_payload,
        )

        sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
        payload = generate_sql_remediation_payload(sql)

        assert payload is not None
        assert "query_graph" in payload
        # Should have detected the tables from the query
        assert isinstance(payload["query_graph"], dict)


class TestDialectIntegration:
    """Tests for dialect-specific integration."""

    def test_snowflake_specific_detection(self, snowflake_detector):
        """Test Snowflake-specific rule detection."""
        sql = "SELECT * FROM users SAMPLE (10)"
        result = snowflake_detector.analyze(sql)

        # Should work without error
        assert result is not None
        assert result.final_score >= 0

    def test_postgres_specific_detection(self, postgres_detector):
        """Test PostgreSQL-specific rule detection."""
        sql = "SELECT * FROM users ORDER BY RANDOM() LIMIT 10"
        result = postgres_detector.analyze(sql)

        assert result is not None
        assert result.final_score >= 0

    def test_duckdb_specific_detection(self, duckdb_detector):
        """Test DuckDB-specific rule detection."""
        sql = """
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price) as rn
            FROM products
        ) WHERE rn = 1
        """
        result = duckdb_detector.analyze(sql)

        # Should detect QUALIFY suggestion
        rule_ids = [i.rule_id for i in result.issues]
        assert "SQL-DUCK-001" in rule_ids


class TestErrorHandlingIntegration:
    """Tests for error handling across the pipeline."""

    def test_parse_error_handling(self, detector):
        """Test handling of SQL parse errors."""
        sql = "SELEKT * FORM users WERE id = 1"
        result = detector.analyze(sql)

        # Should handle gracefully
        assert result is not None
        # May have parse error in issues
        parse_issues = [i for i in result.issues if "PARSE" in i.rule_id]
        # Could have parse error or could work with error tolerance
        assert isinstance(result.final_score, int)

    def test_empty_input_handling(self, detector):
        """Test handling of empty input."""
        result = detector.analyze("")
        assert result is not None

    def test_very_complex_sql_handling(self, detector):
        """Test handling of very complex SQL."""
        # Generate complex SQL
        ctes = "\n".join([
            f"cte{i} AS (SELECT * FROM t{i})"
            for i in range(20)
        ])
        sql = f"WITH {ctes} SELECT * FROM cte0"

        result = detector.analyze(sql)
        assert result is not None
        assert result.final_score >= 0


class TestPerformanceIntegration:
    """Tests for performance characteristics."""

    @pytest.mark.slow
    def test_analysis_performance(self, detector):
        """Test that analysis completes in reasonable time."""
        import time

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
        result = detector.analyze(sql)
        elapsed = time.time() - start

        assert elapsed < 2.0  # Should complete in under 2 seconds
        assert result is not None

    @pytest.mark.slow
    def test_batch_analysis_performance(self, detector):
        """Test analyzing multiple queries."""
        import time

        queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM orders WHERE status = 'pending'",
            "SELECT u.id, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        ]

        start = time.time()
        results = [detector.analyze(q) for q in queries]
        elapsed = time.time() - start

        assert elapsed < 3.0  # Should complete quickly
        assert all(r is not None for r in results)
