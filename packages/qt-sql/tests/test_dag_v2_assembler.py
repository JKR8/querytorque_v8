"""Tests for DAG v2 RewriteAssembler.

Tests that the assembler correctly handles:
1. New CTEs added by LLM rewrites
2. CTE dependency ordering (topological sort)
3. main_query with WITH clause (use directly)
4. Empty rewrite sets (return original)
5. Partial rewrites (some nodes rewritten, others kept)
"""

import pytest
from qt_sql.optimization.dag_v2 import (
    DagBuilder,
    RewriteAssembler,
    RewriteSet,
    DagV2Pipeline,
)


class TestRewriteAssemblerNewCTEs:
    """Test that assembler handles new CTEs added by LLM."""

    def test_new_ctes_added(self):
        """Rewrite adds new CTEs not in original DAG."""
        # Original SQL with one CTE
        original_sql = """
        WITH customer_total_return AS (
            SELECT sr_customer_sk AS ctr_customer_sk,
                   sr_store_sk AS ctr_store_sk,
                   SUM(sr_fee) AS ctr_total_return
            FROM store_returns
            GROUP BY sr_customer_sk, sr_store_sk
        )
        SELECT c_customer_id
        FROM customer_total_return ctr1, customer
        WHERE ctr1.ctr_customer_sk = c_customer_sk
        """

        # Build DAG
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # Rewrite adds TWO new CTEs: filtered_returns, store_avg_return
        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={
                "filtered_returns": "SELECT sr_customer_sk, sr_store_sk, sr_fee FROM store_returns WHERE sr_store_sk IN (SELECT s_store_sk FROM store WHERE s_state = 'SD')",
                "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, sr_store_sk AS ctr_store_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk, sr_store_sk",
                "store_avg_return": "SELECT ctr_store_sk, AVG(ctr_total_return) * 1.2 AS avg_return FROM customer_total_return GROUP BY ctr_store_sk",
                "main_query": "SELECT c.c_customer_id FROM customer_total_return ctr1 JOIN store_avg_return sar ON ctr1.ctr_store_sk = sar.ctr_store_sk JOIN customer c ON ctr1.ctr_customer_sk = c.c_customer_sk WHERE ctr1.ctr_total_return > sar.avg_return",
            },
            invariants_kept=["same result"],
            transform_type="decorrelate",
            expected_speedup="2.9x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # Verify all CTEs are present
        assert "filtered_returns AS (" in result
        assert "customer_total_return AS (" in result
        assert "store_avg_return AS (" in result
        assert result.strip().startswith("WITH")

    def test_cte_ordering_by_dependency(self):
        """CTEs should be ordered by dependency (dependencies come first)."""
        original_sql = """
        WITH base AS (SELECT 1 as x)
        SELECT * FROM base
        """

        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # Create chain: level1 -> level2 -> level3
        # level3 depends on level2, level2 depends on level1
        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={
                "level3": "SELECT * FROM level2 WHERE y > 10",
                "level1": "SELECT 1 AS x",
                "level2": "SELECT x, x * 2 AS y FROM level1",
                "main_query": "SELECT * FROM level3",
            },
            invariants_kept=[],
            transform_type="test",
            expected_speedup="1x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # Verify order: level1 should come before level2, level2 before level3
        level1_pos = result.find("level1 AS (")
        level2_pos = result.find("level2 AS (")
        level3_pos = result.find("level3 AS (")

        assert level1_pos < level2_pos, "level1 should come before level2"
        assert level2_pos < level3_pos, "level2 should come before level3"


class TestRewriteAssemblerEdgeCases:
    """Test edge cases in the assembler."""

    def test_main_query_with_with_clause(self):
        """main_query already has WITH clause -> use directly."""
        original_sql = """
        WITH base AS (SELECT 1 as x)
        SELECT * FROM base
        """

        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # LLM returns complete SQL with WITH in main_query
        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={
                "main_query": "WITH optimized AS (SELECT 1 AS x) SELECT * FROM optimized WHERE x > 0",
            },
            invariants_kept=[],
            transform_type="test",
            expected_speedup="1x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # Should use main_query directly (it has WITH)
        assert result.strip().startswith("WITH optimized AS")
        # Should NOT have nested WITH clauses
        assert result.count("WITH") == 1

    def test_empty_rewrite_returns_original(self):
        """Empty rewrite set should return original SQL."""
        original_sql = """
        WITH base AS (SELECT 1 as x)
        SELECT * FROM base
        """

        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={},
            invariants_kept=[],
            transform_type="none",
            expected_speedup="1x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # Should return original
        assert result == original_sql

    def test_partial_rewrite_keeps_original_nodes(self):
        """Partial rewrite - only some nodes rewritten, others kept."""
        original_sql = """
        WITH cte1 AS (SELECT 1 AS a),
             cte2 AS (SELECT 2 AS b)
        SELECT * FROM cte1, cte2
        """

        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # Only rewrite cte1, keep cte2
        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={
                "cte1": "SELECT 100 AS a",
                "main_query": "SELECT * FROM cte1, cte2",
            },
            invariants_kept=[],
            transform_type="test",
            expected_speedup="1x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # cte1 should have new value
        assert "SELECT 100 AS a" in result
        # cte2 should still be present (from original)
        assert "cte2 AS (" in result

    def test_cte_with_with_prefix_stripped(self):
        """CTE that starts with WITH should have prefix stripped."""
        original_sql = """
        WITH base AS (SELECT 1 as x)
        SELECT * FROM base
        """

        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # LLM erroneously includes WITH in CTE body
        rewrite_set = RewriteSet(
            id="rs_01",
            nodes={
                "base": "WITH base AS (SELECT 2 AS x)",  # Malformed
                "main_query": "SELECT * FROM base",
            },
            invariants_kept=[],
            transform_type="test",
            expected_speedup="1x",
        )

        result = assembler.apply_rewrite_set(rewrite_set)

        # Result should be valid SQL without nested WITH
        assert result.count("WITH") == 1


class TestDagV2PipelineIntegration:
    """Integration tests for the full pipeline."""

    def test_pipeline_apply_response_with_new_ctes(self):
        """Test full pipeline with LLM response adding new CTEs."""
        original_sql = """
        WITH customer_total_return AS (
            SELECT sr_customer_sk AS ctr_customer_sk,
                   SUM(sr_fee) AS ctr_total_return
            FROM store_returns
            GROUP BY sr_customer_sk
        )
        SELECT * FROM customer_total_return
        """

        pipeline = DagV2Pipeline(original_sql)

        # Simulate LLM response with new CTEs
        llm_response = '''
        Here is the optimized query:
        ```json
        {
            "rewrite_sets": [{
                "id": "rs_01",
                "transform": "early_filter",
                "nodes": {
                    "filtered_returns": "SELECT sr_customer_sk, sr_fee FROM store_returns WHERE sr_store_sk = 10",
                    "customer_total_return": "SELECT sr_customer_sk AS ctr_customer_sk, SUM(sr_fee) AS ctr_total_return FROM filtered_returns GROUP BY sr_customer_sk",
                    "main_query": "SELECT * FROM customer_total_return"
                },
                "invariants_kept": ["same output"],
                "expected_speedup": "2x",
                "risk": "low"
            }],
            "explanation": "Added early filter CTE"
        }
        ```
        '''

        result = pipeline.apply_response(llm_response)

        # Both CTEs should be present
        assert "filtered_returns AS (" in result
        assert "customer_total_return AS (" in result
        # filtered_returns should come first (customer_total_return depends on it)
        assert result.find("filtered_returns AS (") < result.find("customer_total_return AS (")

    def test_pipeline_handles_no_rewrite_sets(self):
        """Pipeline returns original when no rewrite_sets in response."""
        original_sql = "SELECT * FROM foo"

        pipeline = DagV2Pipeline(original_sql)

        llm_response = '''
        ```json
        {
            "rewrite_sets": [],
            "explanation": "No optimization needed"
        }
        ```
        '''

        result = pipeline.apply_response(llm_response)

        # Should return original
        assert result == original_sql

    def test_pipeline_handles_invalid_json(self):
        """Pipeline returns original when response has invalid JSON."""
        original_sql = "SELECT * FROM foo"

        pipeline = DagV2Pipeline(original_sql)

        llm_response = "This is not valid JSON at all"

        result = pipeline.apply_response(llm_response)

        # Should return original
        assert result == original_sql


class TestDependencyGraph:
    """Test the dependency graph building."""

    def test_build_dependency_graph_simple(self):
        """Test simple dependency detection."""
        original_sql = "SELECT 1"
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        cte_nodes = {
            "a": "SELECT 1",
            "b": "SELECT * FROM a",
            "c": "SELECT * FROM b",
        }

        deps = assembler._build_dependency_graph(cte_nodes)

        assert deps["a"] == []
        assert deps["b"] == ["a"]
        assert deps["c"] == ["b"]

    def test_build_dependency_graph_multiple_deps(self):
        """Test CTE with multiple dependencies."""
        original_sql = "SELECT 1"
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        cte_nodes = {
            "a": "SELECT 1",
            "b": "SELECT 2",
            "c": "SELECT * FROM a JOIN b",
        }

        deps = assembler._build_dependency_graph(cte_nodes)

        assert deps["a"] == []
        assert deps["b"] == []
        assert set(deps["c"]) == {"a", "b"}

    def test_topological_sort_simple(self):
        """Test topological sort produces correct order."""
        original_sql = "SELECT 1"
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        deps = {
            "a": [],
            "b": ["a"],
            "c": ["b"],
        }

        result = assembler._topological_sort(deps)

        # a should come before b, b before c
        assert result.index("a") < result.index("b")
        assert result.index("b") < result.index("c")

    def test_topological_sort_handles_cycle(self):
        """Topological sort should handle cycles gracefully (not infinite loop)."""
        original_sql = "SELECT 1"
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        # Cycle: a -> b -> c -> a
        deps = {
            "a": ["c"],
            "b": ["a"],
            "c": ["b"],
        }

        # Should not hang - just return some ordering
        result = assembler._topological_sort(deps)

        # All nodes should be in result
        assert set(result) == {"a", "b", "c"}

    def test_word_boundary_matching(self):
        """Test that dependency detection uses word boundaries.

        'store' should not match 'store_returns'.
        """
        original_sql = "SELECT 1"
        dag = DagBuilder(original_sql).build()
        assembler = RewriteAssembler(dag)

        cte_nodes = {
            "store": "SELECT 1",
            "store_returns": "SELECT * FROM fact_store_returns",
        }

        deps = assembler._build_dependency_graph(cte_nodes)

        # store_returns should NOT depend on store (word boundary)
        assert "store" not in deps["store_returns"]
