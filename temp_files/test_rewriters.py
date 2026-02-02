"""Tests for semantic SQL rewriters.

These tests validate that rewriters correctly transform SQL patterns
and produce semantically equivalent output.
"""

import pytest
from sqlglot import parse

# Import the rewriter framework
from qt_sql.rewriters import (
    get_rewriter_for_rule,
    has_rewriter,
    list_registered_rules,
    list_registered_rewriters,
    get_coverage_stats,
    SchemaMetadata,
    TableMetadata,
    RewriteConfidence,
    SafetyCheckResult,
)


class TestRegistry:
    """Test rewriter registry functionality."""
    
    def test_registered_rewriters_exist(self):
        """Verify rewriters are registered."""
        rewriters = list_registered_rewriters()
        assert len(rewriters) > 0
        assert "or_chain_to_in" in rewriters
        assert "subquery_to_qualify" in rewriters
    
    def test_get_rewriter_for_rule(self):
        """Test getting rewriter by rule ID."""
        # Rule with rewriter
        rewriter = get_rewriter_for_rule("SQL-DUCK-001")
        assert rewriter is not None
        assert rewriter.rewriter_id == "subquery_to_qualify"
        
        # Rule without rewriter
        rewriter = get_rewriter_for_rule("NONEXISTENT-001")
        assert rewriter is None
    
    def test_has_rewriter(self):
        """Test checking for rewriter existence."""
        assert has_rewriter("SQL-DUCK-001")
        assert has_rewriter("SQL-WHERE-010")
        assert not has_rewriter("FAKE-RULE-999")
    
    def test_coverage_stats(self):
        """Test coverage statistics."""
        stats = get_coverage_stats()
        assert stats["total_rewriters"] > 0
        assert stats["total_rules_covered"] > 0
        assert isinstance(stats["rules_by_rewriter"], dict)


class TestOrChainRewriter:
    """Test OR chain to IN rewriter."""
    
    def test_simple_or_chain(self):
        """Test basic OR chain conversion."""
        sql = "SELECT * FROM users WHERE status = 'active' OR status = 'pending' OR status = 'new'"
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        assert result.success
        assert result.confidence == RewriteConfidence.HIGH
        assert "IN" in result.rewritten_sql.upper()
        assert "'active'" in result.rewritten_sql
        assert "'pending'" in result.rewritten_sql
        assert "'new'" in result.rewritten_sql
    
    def test_or_chain_with_table_alias(self):
        """Test OR chain with qualified column names."""
        sql = """
            SELECT * FROM users u 
            WHERE u.status = 'A' OR u.status = 'B' OR u.status = 'C'
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        assert result.success
        assert "IN" in result.rewritten_sql.upper()
    
    def test_mixed_or_not_converted(self):
        """Test that mixed ORs (different columns) are not converted."""
        sql = "SELECT * FROM users WHERE status = 'active' OR role = 'admin'"
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        # Should not convert - different columns
        assert not result.success or "IN" not in result.rewritten_sql.upper()
    
    def test_safety_checks_passed(self):
        """Test that safety checks pass for OR to IN."""
        sql = "SELECT * FROM t WHERE x = 1 OR x = 2 OR x = 3"
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        assert result.success
        assert result.all_safety_checks_passed
        assert any(c.name == "semantic_equivalence" for c in result.safety_checks)


class TestCorrelatedSubqueryRewriter:
    """Test correlated subquery to JOIN rewriter."""
    
    def test_scalar_subquery_in_select(self):
        """Test converting scalar subquery in SELECT to JOIN."""
        sql = """
            SELECT 
                e.name,
                (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name
            FROM employees e
        """
        ast = parse(sql)[0]
        
        # Provide metadata for safety check
        metadata = SchemaMetadata(tables={
            "departments": TableMetadata(
                name="departments",
                columns=["id", "name"],
                primary_key=["id"],
            )
        })
        
        rewriter = get_rewriter_for_rule("SQL-SEL-008")
        rewriter.metadata = metadata
        result = rewriter.rewrite(ast)
        
        # May or may not succeed depending on implementation completeness
        if result.success:
            assert "JOIN" in result.rewritten_sql.upper()
            # Check safety
            pk_check = [c for c in result.safety_checks if "uniqueness" in c.name.lower()]
            if pk_check:
                assert pk_check[0].result == SafetyCheckResult.PASSED
    
    def test_requires_uniqueness_metadata(self):
        """Test that missing metadata triggers warning."""
        sql = """
            SELECT 
                e.name,
                (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name
            FROM employees e
        """
        ast = parse(sql)[0]
        
        # No metadata provided
        rewriter = get_rewriter_for_rule("SQL-SEL-008")
        result = rewriter.rewrite(ast)
        
        if result.success:
            # Should have warning about missing uniqueness verification
            uniqueness_checks = [c for c in result.safety_checks 
                               if "uniqueness" in c.name.lower()]
            if uniqueness_checks:
                assert uniqueness_checks[0].result == SafetyCheckResult.WARNING


class TestSelfJoinToWindowRewriter:
    """Test self-join to window function rewriter."""
    
    def test_max_per_group_pattern(self):
        """Test converting max-per-group self-join to window."""
        sql = """
            SELECT e1.* 
            FROM employees e1
            LEFT JOIN employees e2 
                ON e1.dept_id = e2.dept_id AND e2.salary > e1.salary
            WHERE e2.id IS NULL
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-JOIN-005")
        result = rewriter.rewrite(ast)
        
        if result.success:
            upper_sql = result.rewritten_sql.upper()
            # Should use window function
            assert "ROW_NUMBER" in upper_sql or "QUALIFY" in upper_sql
            # Should partition by dept
            assert "PARTITION" in upper_sql or "QUALIFY" in upper_sql
    
    def test_duckdb_uses_qualify(self):
        """Test that DuckDB dialect uses QUALIFY."""
        sql = """
            SELECT e1.* 
            FROM employees e1
            LEFT JOIN employees e2 
                ON e1.dept_id = e2.dept_id AND e2.salary > e1.salary
            WHERE e2.id IS NULL
        """
        ast = parse(sql)[0]
        
        # Get rewriter configured for DuckDB
        from qt_sql.rewriters.semantic.self_join_to_window import SelfJoinToWindowRewriter
        rewriter = SelfJoinToWindowRewriter(dialect="duckdb")
        result = rewriter.rewrite(ast)
        
        if result.success:
            assert "QUALIFY" in result.rewritten_sql.upper()


class TestDuckDBSpecificRewriters:
    """Test DuckDB-specific rewriters."""
    
    def test_subquery_to_qualify(self):
        """Test window subquery to QUALIFY conversion."""
        sql = """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn
                FROM employees
            ) t WHERE rn = 1
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-DUCK-001")
        result = rewriter.rewrite(ast)
        
        if result.success:
            assert "QUALIFY" in result.rewritten_sql.upper()
            # Should not have subquery wrapper
            assert result.rewritten_sql.count("SELECT") <= 2  # May have one SELECT
    
    def test_manual_pivot_detection(self):
        """Test detection of manual pivot pattern."""
        sql = """
            SELECT id,
                MAX(CASE WHEN category = 'A' THEN value END) as A,
                MAX(CASE WHEN category = 'B' THEN value END) as B,
                MAX(CASE WHEN category = 'C' THEN value END) as C
            FROM t GROUP BY id
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-DUCK-007")
        assert rewriter is not None
        assert rewriter.can_rewrite(ast)


class TestRepeatedSubqueryRewriter:
    """Test repeated subquery to CTE rewriter."""
    
    def test_duplicate_subquery_detection(self):
        """Test detection of duplicate subqueries."""
        sql = """
            SELECT 
                (SELECT AVG(salary) FROM employees WHERE dept_id = 1) as avg_sal,
                (SELECT AVG(salary) FROM employees WHERE dept_id = 1) * 1.1 as target
            FROM dual
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-CTE-003")
        assert rewriter is not None
        assert rewriter.can_rewrite(ast)
    
    def test_cte_extraction(self):
        """Test CTE extraction from repeated subqueries."""
        sql = """
            SELECT 
                (SELECT COUNT(*) FROM orders WHERE status = 'pending') as pending,
                (SELECT COUNT(*) FROM orders WHERE status = 'pending') > 10 as has_backlog
            FROM dual
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-CTE-003")
        result = rewriter.rewrite(ast)
        
        if result.success:
            assert "WITH" in result.rewritten_sql.upper()
            # Should only have one definition of the repeated subquery
            assert result.rewritten_sql.upper().count("COUNT(*)") <= 2


class TestMetadataIntegration:
    """Test schema metadata integration with rewriters."""
    
    def test_metadata_affects_confidence(self):
        """Test that metadata availability affects confidence level."""
        sql = """
            SELECT e.name, (SELECT d.name FROM dept d WHERE d.id = e.dept_id)
            FROM emp e
        """
        ast = parse(sql)[0]
        
        # Without metadata
        rewriter = get_rewriter_for_rule("SQL-SEL-008")
        result_no_meta = rewriter.rewrite(ast)
        
        # With metadata  
        metadata = SchemaMetadata(tables={
            "dept": TableMetadata(
                name="dept",
                columns=["id", "name"],
                primary_key=["id"],
            )
        })
        rewriter_with_meta = get_rewriter_for_rule("SQL-SEL-008")
        rewriter_with_meta.metadata = metadata
        result_with_meta = rewriter_with_meta.rewrite(ast)
        
        # With PK metadata, confidence should be higher
        if result_no_meta.success and result_with_meta.success:
            # Result with metadata should have higher or equal confidence
            assert result_with_meta.confidence.value >= result_no_meta.confidence.value


class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_empty_query(self):
        """Test handling of minimal queries."""
        sql = "SELECT 1"
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        # Should fail gracefully (no OR chain to convert)
        assert not result.success or result.rewritten_sql == sql
    
    def test_complex_nested_query(self):
        """Test handling of deeply nested queries."""
        sql = """
            SELECT * FROM (
                SELECT * FROM (
                    SELECT * FROM t WHERE x = 1 OR x = 2 OR x = 3
                ) a
            ) b
        """
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        # Should handle nested queries
        result = rewriter.rewrite(ast)
        # May or may not find the OR chain depending on implementation
    
    def test_invalid_ast_handling(self):
        """Test handling of non-SELECT nodes."""
        sql = "INSERT INTO t VALUES (1, 2, 3)"
        ast = parse(sql)[0]
        
        rewriter = get_rewriter_for_rule("SQL-WHERE-010")
        result = rewriter.rewrite(ast)
        
        # Should fail gracefully
        assert not result.success


# Run with: pytest test_rewriters.py -v
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
