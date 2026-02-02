"""Phase 2: SQL Analyzer Tests - SQLAntiPatternDetector Integration.

Tests for the main SQLAntiPatternDetector class and its integration
with the AST detector, query structure parsing, and result generation.
"""

import pytest
from qt_sql.analyzers.sql_antipattern_detector import (
    SQLAntiPatternDetector,
    SQLAnalysisResult,
    SQLIssue,
    analyze_sql,
)


class TestSQLAntiPatternDetector:
    """Tests for SQLAntiPatternDetector class."""

    def test_detector_initialization_default(self):
        """Test detector initializes with default dialect."""
        detector = SQLAntiPatternDetector()
        assert detector.dialect == "generic"

    def test_detector_initialization_with_dialect(self):
        """Test detector initializes with specified dialect."""
        detector = SQLAntiPatternDetector(dialect="snowflake")
        assert detector.dialect == "snowflake"

    def test_detector_supported_dialects(self):
        """Test detector accepts known dialects."""
        dialects = ["generic", "snowflake", "postgres", "duckdb", "tsql", "bigquery"]
        for dialect in dialects:
            detector = SQLAntiPatternDetector(dialect=dialect)
            assert detector.dialect == dialect

    def test_analyze_returns_result(self, detector):
        """Test analyze returns SQLAnalysisResult."""
        result = detector.analyze("SELECT 1")
        assert isinstance(result, SQLAnalysisResult)

    def test_analyze_result_has_sql(self, detector):
        """Test result contains original SQL."""
        sql = "SELECT * FROM users"
        result = detector.analyze(sql)
        assert result.sql == sql

    def test_analyze_result_has_issues_list(self, detector):
        """Test result has issues list."""
        result = detector.analyze("SELECT 1")
        assert isinstance(result.issues, list)

    def test_analyze_result_has_scores(self, detector):
        """Test result has scoring fields."""
        result = detector.analyze("SELECT 1")
        assert hasattr(result, "base_score")
        assert hasattr(result, "total_penalty")
        assert hasattr(result, "final_score")
        assert result.base_score == 100
        assert result.final_score >= 0
        assert result.final_score <= 100


class TestSQLIssueDataclass:
    """Tests for SQLIssue dataclass."""

    def test_sql_issue_creation(self):
        """Test SQLIssue can be created with required fields."""
        issue = SQLIssue(
            rule_id="TEST-001",
            name="Test Issue",
            severity="medium",
            category="test",
            penalty=5,
            description="A test issue",
        )
        assert issue.rule_id == "TEST-001"
        assert issue.penalty == 5

    def test_sql_issue_optional_fields(self):
        """Test SQLIssue optional fields default correctly."""
        issue = SQLIssue(
            rule_id="TEST-001",
            name="Test Issue",
            severity="medium",
            category="test",
            penalty=5,
            description="A test issue",
        )
        assert issue.location is None
        assert issue.match is None
        assert issue.explanation == ""
        assert issue.suggestion == ""


class TestSQLAnalysisResult:
    """Tests for SQLAnalysisResult dataclass."""

    def test_result_to_dict(self, detector):
        """Test to_dict returns serializable dict."""
        result = detector.analyze("SELECT * FROM users")
        result_dict = result.to_dict()

        assert isinstance(result_dict, dict)
        assert "score" in result_dict
        assert "total_penalty" in result_dict
        assert "severity_counts" in result_dict
        assert "issues" in result_dict

    def test_result_to_dict_issues_are_dicts(self, detector):
        """Test issues in to_dict are dictionaries."""
        result = detector.analyze("SELECT * FROM users")
        result_dict = result.to_dict()

        for issue in result_dict["issues"]:
            assert isinstance(issue, dict)
            assert "rule_id" in issue
            assert "severity" in issue

    def test_result_severity_counts_dict(self, detector):
        """Test severity_counts is a proper dict."""
        result = detector.analyze("SELECT 1")
        result_dict = result.to_dict()

        counts = result_dict["severity_counts"]
        assert "critical" in counts
        assert "high" in counts
        assert "medium" in counts
        assert "low" in counts


class TestAnalyzeSQLFunction:
    """Tests for the analyze_sql convenience function."""

    def test_analyze_sql_returns_dict(self):
        """Test analyze_sql returns dictionary."""
        result = analyze_sql("SELECT 1")
        assert isinstance(result, dict)

    def test_analyze_sql_with_dialect(self):
        """Test analyze_sql accepts dialect parameter."""
        result = analyze_sql("SELECT 1", dialect="postgres")
        assert isinstance(result, dict)
        assert "score" in result


class TestQueryStructure:
    """Tests for query structure parsing."""

    def test_structure_included_by_default(self, detector):
        """Test query structure is included by default."""
        result = detector.analyze("SELECT id FROM users")
        assert result.query_structure is not None

    def test_structure_can_be_excluded(self, detector):
        """Test query structure can be excluded."""
        result = detector.analyze("SELECT id FROM users", include_structure=False)
        assert result.query_structure is None

    def test_structure_has_table_count(self, detector):
        """Test structure includes table count."""
        result = detector.analyze("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
        assert result.query_structure["table_count"] >= 2

    def test_structure_has_join_count(self, detector):
        """Test structure includes join count."""
        result = detector.analyze("SELECT id FROM users JOIN orders ON users.id = orders.user_id")
        assert result.query_structure["join_count"] >= 1

    def test_structure_has_cte_count(self, detector, sample_cte_sql):
        """Test structure includes CTE count."""
        result = detector.analyze(sample_cte_sql)
        assert result.query_structure["cte_count"] >= 1

    def test_structure_has_cte_names(self, detector, sample_cte_sql):
        """Test structure includes CTE names."""
        result = detector.analyze(sample_cte_sql)
        assert len(result.query_structure["cte_names"]) >= 1

    def test_structure_has_subquery_count(self, detector):
        """Test structure includes subquery count."""
        sql = "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders)"
        result = detector.analyze(sql)
        assert result.query_structure["subquery_count"] >= 1


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_sql(self, detector):
        """Test handling of empty SQL."""
        result = detector.analyze("")
        assert isinstance(result, SQLAnalysisResult)
        assert result.final_score >= 0

    def test_whitespace_only_sql(self, detector):
        """Test handling of whitespace-only SQL."""
        result = detector.analyze("   \n\t  ")
        assert isinstance(result, SQLAnalysisResult)

    def test_comment_only_sql(self, detector):
        """Test handling of comment-only SQL."""
        result = detector.analyze("-- This is just a comment")
        assert isinstance(result, SQLAnalysisResult)

    def test_invalid_sql_syntax(self, detector):
        """Test handling of invalid SQL syntax."""
        result = detector.analyze("SELEKT * FORM users")
        # Should return a parse error issue
        assert isinstance(result, SQLAnalysisResult)
        # May have parse error in issues
        parse_errors = [i for i in result.issues if "PARSE" in i.rule_id]
        # Could have parse error or could parse with error tolerance
        assert isinstance(result.final_score, int)

    def test_very_long_sql(self, detector):
        """Test handling of very long SQL."""
        # Generate a long SQL with many columns
        columns = ", ".join([f"col{i}" for i in range(100)])
        sql = f"SELECT {columns} FROM users WHERE id = 1"
        result = detector.analyze(sql)
        assert isinstance(result, SQLAnalysisResult)

    def test_unicode_in_sql(self, detector):
        """Test handling of Unicode characters."""
        sql = "SELECT * FROM users WHERE name = '日本語'"
        result = detector.analyze(sql)
        assert isinstance(result, SQLAnalysisResult)

    def test_multiline_sql(self, detector, sample_cte_sql):
        """Test handling of multiline SQL."""
        result = detector.analyze(sample_cte_sql)
        assert isinstance(result, SQLAnalysisResult)
        assert result.query_structure["line_count"] > 1


class TestDialectBehavior:
    """Tests for dialect-specific behavior."""

    def test_generic_dialect_broad_rules(self, detector):
        """Generic dialect should include broad rules."""
        from qt_sql.analyzers.ast_detector.registry import get_all_rules

        # Get rules for generic
        all_rules = get_all_rules()
        generic_rules = [r for r in all_rules if r.applies_to_dialect("generic")]

        # Should have substantial rules
        assert len(generic_rules) > 50

    def test_snowflake_has_extra_rules(self, snowflake_detector):
        """Snowflake dialect should include Snowflake-specific rules."""
        from qt_sql.analyzers.ast_detector.registry import get_all_rules

        all_rules = get_all_rules()
        snowflake_rules = [r for r in all_rules if "snowflake" in str(r.dialects).lower() or not r.dialects]

        # Should have Snowflake-specific rules
        snowflake_only = [r for r in all_rules if r.dialects and "snowflake" in str(r.dialects).lower()]
        assert len(snowflake_only) > 0

    def test_different_dialects_different_results(self):
        """Different dialects may produce different results."""
        sql = "SELECT * FROM users WHERE id = 1"

        generic = SQLAntiPatternDetector(dialect="generic")
        snowflake = SQLAntiPatternDetector(dialect="snowflake")

        generic_result = generic.analyze(sql)
        snowflake_result = snowflake.analyze(sql)

        # Both should work
        assert isinstance(generic_result, SQLAnalysisResult)
        assert isinstance(snowflake_result, SQLAnalysisResult)


class TestPerformance:
    """Tests for performance characteristics."""

    @pytest.mark.slow
    def test_analysis_completes_quickly(self, detector):
        """Test that analysis completes in reasonable time."""
        import time

        sql = """
        WITH cte1 AS (SELECT * FROM t1),
             cte2 AS (SELECT * FROM t2)
        SELECT a.*, b.*,
               (SELECT MAX(x) FROM t3 WHERE t3.id = a.id)
        FROM cte1 a
        JOIN cte2 b ON a.id = b.id
        WHERE a.status = 'active'
        ORDER BY a.created_at DESC
        LIMIT 100
        """

        start = time.time()
        result = detector.analyze(sql)
        elapsed = time.time() - start

        # Should complete in under 1 second
        assert elapsed < 1.0
        assert isinstance(result, SQLAnalysisResult)

    @pytest.mark.slow
    def test_repeated_analysis_consistent(self, detector):
        """Test that repeated analysis produces consistent results."""
        sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"

        results = [detector.analyze(sql) for _ in range(5)]

        # All results should have same score and issue count
        scores = [r.final_score for r in results]
        issue_counts = [len(r.issues) for r in results]

        assert len(set(scores)) == 1
        assert len(set(issue_counts)) == 1
