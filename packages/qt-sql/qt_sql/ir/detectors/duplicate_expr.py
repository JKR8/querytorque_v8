"""Detector: Duplicate expensive expressions.

Finds repeated expression subtrees (e.g., haversine distance computed twice)
and emulated cross joins (JOIN ... ON 1=1).
"""
from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from sqlglot import exp

from ..schema import DuplicateGroup, ScriptIR, UseSite, canonical_hash

log = logging.getLogger(__name__)


# ── Generic duplicate finder ───────────────────────────────────────────


def detect_duplicate_expressions(
    script_ir: ScriptIR,
    min_complexity: int = 80,
) -> List[DuplicateGroup]:
    """Find duplicate expensive expression subtrees across the script.

    Only considers expressions >= *min_complexity* characters (skip trivial).
    Returns groups sorted by expression length (longest = most expensive).
    """
    dialect = script_ir.dialect.value
    expr_map: Dict[str, List[Tuple[str, str]]] = defaultdict(list)

    for stmt in script_ir.statements:
        if not stmt.ast:
            continue
        _walk_expensive(stmt.ast, stmt.id, dialect, min_complexity, expr_map)

    groups: List[DuplicateGroup] = []
    for h, entries in expr_map.items():
        if len(entries) >= 2:
            sites = [
                UseSite(statement_id=sid, path=f"{sid}.expr", snippet_hash=h)
                for sid, _ in entries
            ]
            groups.append(
                DuplicateGroup(
                    canonical_hash=h, canonical_sql=entries[0][1], sites=sites
                )
            )

    groups.sort(key=lambda g: len(g.canonical_sql), reverse=True)
    return groups


# ── Haversine-specific detector ────────────────────────────────────────


def detect_haversine_duplicates(script_ir: ScriptIR) -> List[UseSite]:
    """Detect duplicated haversine distance expressions.

    Labels: ``geo.distance_haversine``

    Haversine signature::

        ROUND(6371 * 2 * ASIN(SQRT(POW(SIN(RADIANS(...)), 2) + ...)), N)
    """
    dialect = script_ir.dialect.value
    sites: List[UseSite] = []

    for stmt in script_ir.statements:
        if not stmt.ast:
            continue

        for node in stmt.ast.walk():
            if _func_name(node).lower() != "asin":
                continue

            top = _find_haversine_root(node)
            sql_text = top.sql(dialect=dialect)
            label = "geo.distance_haversine"

            site = UseSite(
                statement_id=stmt.id,
                path=f"{stmt.id}.expr.haversine",
                snippet_hash=canonical_hash(sql_text),
                labels=[label],
            )
            sites.append(site)

            if label not in stmt.labels:
                stmt.labels.append(label)

    return sites


# ── Cross-join-on-true detector ────────────────────────────────────────


def detect_cross_join_on_true(script_ir: ScriptIR) -> List[UseSite]:
    """Detect cross joins emulated as ``JOIN ... ON 1=1``.

    Labels: ``join.cross_join_{table_name}``
    """
    sites: List[UseSite] = []

    for stmt in script_ir.statements:
        if not stmt.ast:
            continue

        for join_node in stmt.ast.find_all(exp.Join):
            on_clause = join_node.args.get("on")
            if not on_clause:
                continue

            on_sql = on_clause.sql().strip()
            if on_sql not in ("1 = 1", "1=1", "TRUE", "true"):
                continue

            table = join_node.this
            tname = (
                table.name.lower() if isinstance(table, exp.Table) else "unknown"
            )
            label = f"join.cross_join_{tname}"

            site = UseSite(
                statement_id=stmt.id,
                path=f"{stmt.id}.join.cross_{tname}",
                snippet_hash=canonical_hash(on_sql),
                labels=[label],
            )
            sites.append(site)

            if label not in stmt.labels:
                stmt.labels.append(label)

    return sites


# ── Helpers ────────────────────────────────────────────────────────────


def _walk_expensive(
    ast: Any,
    stmt_id: str,
    dialect: str,
    min_len: int,
    result: Dict[str, List[Tuple[str, str]]],
):
    for node in ast.walk():
        if not isinstance(node, (exp.Func, exp.Case)):
            continue
        sql = node.sql(dialect=dialect)
        if len(sql) < min_len:
            continue
        h = canonical_hash(sql)
        result[h].append((stmt_id, sql))


def _find_haversine_root(node: Any) -> Any:
    """Walk up from ASIN to find the enclosing ROUND(...)."""
    current = node
    for _ in range(10):
        parent = current.parent
        if parent is None:
            return current
        if isinstance(parent, exp.Func) and _func_name(parent).lower() == "round":
            return parent
        if isinstance(parent, (exp.Select, exp.Where, exp.Alias)):
            return current
        current = parent
    return current


def _func_name(node: Any) -> str:
    if isinstance(node, exp.Anonymous):
        return node.name
    if isinstance(node, exp.Func):
        return type(node).__name__
    return ""
