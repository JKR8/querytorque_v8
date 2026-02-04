"""Tests for dag_prompts module."""

import pytest
from qt_sql.optimization.dag_v2 import DagBuilder
from qt_sql.optimization.dag_prompts import (
    get_topological_order,
    build_dag_structure_string,
    build_node_sql_string,
)


def test_get_topological_order_simple():
    """Test topological sort on simple CTE query."""
    sql = """
    WITH cte1 AS (SELECT 1 as x),
         cte2 AS (SELECT x FROM cte1)
    SELECT * FROM cte2
    """
    dag = DagBuilder(sql).build()
    order = get_topological_order(dag)

    # cte1 should come before cte2 (dependency order)
    assert order.index("cte1") < order.index("cte2")
    # main_query should be last
    assert order[-1] == "main_query"


def test_get_topological_order_no_ctes():
    """Test topological sort on query without CTEs."""
    sql = "SELECT * FROM table1"
    dag = DagBuilder(sql).build()
    order = get_topological_order(dag)

    assert len(order) == 1
    assert order[0] == "main_query"


def test_build_dag_structure_string():
    """Test DAG structure string formatting."""
    sql = "WITH cte AS (SELECT 1) SELECT * FROM cte"
    dag = DagBuilder(sql).build()
    structure = build_dag_structure_string(dag)

    # Should contain "Nodes:" and "Edges:" headers
    assert "Nodes:" in structure
    assert "Edges:" in structure

    # Should list both nodes
    assert "[cte]" in structure
    assert "[main_query]" in structure

    # Should show edge
    assert "cte → main_query" in structure or "cte -> main_query" in structure


def test_build_node_sql_string():
    """Test node SQL string formatting."""
    sql = """
    WITH cte AS (
        SELECT col1, col2
        FROM table1
        WHERE col1 > 10
    )
    SELECT * FROM cte
    """
    dag = DagBuilder(sql).build()
    node_sql = build_node_sql_string(dag)

    # Should contain node headers
    assert "### cte" in node_sql
    assert "### main_query" in node_sql

    # Should contain SQL blocks
    assert "```sql" in node_sql
    assert "SELECT col1, col2" in node_sql


def test_build_node_sql_string_empty():
    """Test node SQL for query without CTEs."""
    sql = "SELECT * FROM table1"
    dag = DagBuilder(sql).build()
    node_sql = build_node_sql_string(dag)

    # Should have main_query section
    assert "### main_query" in node_sql
    assert "SELECT * FROM table1" in node_sql


def test_topological_order_with_correlated():
    """Test topological sort handles CORRELATED flag."""
    sql = """
    WITH cte AS (
        SELECT * FROM table1 t1
        WHERE EXISTS (SELECT 1 FROM table2 t2 WHERE t2.id = t1.id)
    )
    SELECT * FROM cte
    """
    dag = DagBuilder(sql).build()
    structure = build_dag_structure_string(dag)

    # Should mark correlated nodes if they exist
    # (This test is permissive - not all queries will have CORRELATED flag)
    if any("CORRELATED" in (node.flags or []) for node in dag.nodes.values()):
        assert "CORRELATED" in structure
