"""Tests for KB-backed MCTS transforms (AST-only)."""

import re

from qt_sql.optimization.mcts.transforms import apply_transform


def test_or_to_union():
    sql = "SELECT * FROM orders WHERE customer_id = 1 OR product_id = 2"
    rewritten = apply_transform(sql, "or_to_union")
    assert rewritten is not None
    assert "UNION" in rewritten.upper()


def test_correlated_to_cte():
    sql = (
        "SELECT a.id, "
        "(SELECT AVG(b.val) FROM b WHERE b.a_id = a.id) AS avg_val "
        "FROM a"
    )
    rewritten = apply_transform(sql, "correlated_to_cte")
    assert rewritten is not None
    assert "JOIN" in rewritten.upper()


def test_date_cte_isolate():
    sql = (
        "SELECT * FROM web_sales "
        "JOIN date_dim ON d_date_sk = ws_sold_date_sk "
        "WHERE d_year = 2001"
    )
    rewritten = apply_transform(sql, "date_cte_isolate")
    assert rewritten is not None
    assert "WITH" in rewritten.upper()
    assert "DATE_FILTER" in rewritten.upper()


def test_push_predicate():
    sql = "SELECT * FROM (SELECT * FROM sales) s WHERE s.price > 100"
    rewritten = apply_transform(sql, "push_pred")
    assert rewritten is not None
    assert "FROM sales" in rewritten
    assert "price > 100" in rewritten


def test_materialize_cte():
    sql = (
        "SELECT * FROM (SELECT * FROM sales WHERE price > 100) a "
        "JOIN (SELECT * FROM sales WHERE price > 100) b ON a.id = b.id"
    )
    rewritten = apply_transform(sql, "materialize_cte")
    assert rewritten is not None
    assert "WITH" in rewritten.upper()
    assert "_CTE" in rewritten.upper()


def test_flatten_subquery():
    sql = "SELECT * FROM orders WHERE customer_id IN (SELECT customer_id FROM vip)"
    rewritten = apply_transform(sql, "flatten_subq")
    assert rewritten is not None
    assert re.search(r"\bIN\s*\(\s*SELECT\b", rewritten.upper()) is None
    assert "JOIN" in rewritten.upper()


def test_reorder_join_noop_on_single_join():
    sql = "SELECT * FROM a JOIN b ON a.id = b.id"
    rewritten = apply_transform(sql, "reorder_join")
    assert rewritten is None or "JOIN" in rewritten.upper()


def test_inline_cte():
    sql = "WITH cte AS (SELECT * FROM sales) SELECT * FROM cte"
    rewritten = apply_transform(sql, "inline_cte")
    assert rewritten is not None
    assert "WITH" not in rewritten.upper()


def test_remove_redundant():
    sql = "SELECT DISTINCT col FROM t GROUP BY col"
    rewritten = apply_transform(sql, "remove_redundant")
    assert rewritten is not None
    assert "DISTINCT" not in rewritten.upper()


def test_consolidate_scans():
    sql = (
        "WITH a AS ("
        "SELECT k, SUM(x) AS x1 FROM t WHERE flag = 1 GROUP BY k"
        "), "
        "b AS ("
        "SELECT k, SUM(x) AS x2 FROM t WHERE flag = 2 GROUP BY k"
        ") "
        "SELECT a.k, a.x1, b.x2 FROM a JOIN b ON a.k = b.k"
    )
    rewritten = apply_transform(sql, "consolidate_scans")
    assert rewritten is not None
    assert "SCAN_CONSOLIDATED" in rewritten.upper()
    assert "CASE WHEN" in rewritten.upper()
