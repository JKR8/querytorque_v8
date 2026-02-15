#!/usr/bin/env python3
"""Compute anchor hashes for gold example original_sql.

Builds IR from each example's original_sql, renders the node map
(with anchor hashes for WHERE clauses), and prints all hashes so
they can be used in patch_plan targeting.

Usage:
    PYTHONPATH=packages/qt-shared:packages/qt-sql:. python3 scripts/compute_patch_hashes.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from qt_sql.ir import build_script_ir, render_ir_node_map
from qt_sql.ir.schema import Dialect, canonical_hash


EXAMPLE_DIRS = [
    "packages/qt-sql/qt_sql/examples/snowflake",
    "packages/qt-sql/qt_sql/examples/duckdb",
    "packages/qt-sql/qt_sql/examples/postgres",
]


def process_example(path: Path) -> None:
    """Build IR from an example file and print the node map with hashes."""
    data = json.loads(path.read_text())
    original_sql = data.get("original_sql", "")
    if not original_sql:
        print(f"  [SKIP] No original_sql in {path.name}")
        return

    # Detect dialect from path
    dialect_str = path.parent.name
    dialect_map = {
        "snowflake": Dialect.SNOWFLAKE,
        "duckdb": Dialect.DUCKDB,
        "postgres": Dialect.POSTGRES,
    }
    dialect = dialect_map.get(dialect_str, Dialect.DUCKDB)

    print(f"\n{'='*70}")
    print(f"  {path.name}  (dialect={dialect.value})")
    print(f"{'='*70}")

    try:
        script_ir = build_script_ir(original_sql, dialect)
    except Exception as e:
        print(f"  [ERROR] Failed to build IR: {e}")
        return

    # Print node map (includes WHERE hashes)
    node_map = render_ir_node_map(script_ir)
    print(node_map)

    # Print all expression hashes for reference
    print(f"\n--- All anchor hashes ---")
    for stmt in script_ir.statements:
        print(f"\nStatement {stmt.id}:")
        print(f"  full_stmt hash: {canonical_hash(stmt.sql_text)}")

        if stmt.query is None:
            continue

        q = stmt.query
        if q.from_clause:
            _print_from_hashes(q.from_clause, indent=2)

        if q.where:
            print(f"  WHERE hash: {q.where.snippet_hash}  text: {q.where.sql_text[:80]}...")

        for cte in q.with_ctes:
            print(f"  CTE '{cte.name}' query_id={cte.query.id}")
            cq = cte.query
            if cq.from_clause:
                _print_from_hashes(cq.from_clause, indent=4)
            if cq.where:
                print(f"    WHERE hash: {cq.where.snippet_hash}  text: {cq.where.sql_text[:80]}...")

    # Also hash the full original SQL for statement-level targeting
    print(f"\n  Statement-level hash (full SQL): {canonical_hash(original_sql)}")


def _print_from_hashes(from_ir, indent: int = 2) -> None:
    from qt_sql.ir.schema import FromKind
    pad = " " * indent
    if from_ir.kind == FromKind.TABLE and from_ir.table:
        name = from_ir.table.name
        alias = from_ir.table.alias or ""
        print(f"{pad}FROM table: {name} {alias}")
    elif from_ir.kind == FromKind.JOIN and from_ir.join:
        _print_from_hashes(from_ir.join.left, indent)
        _print_from_hashes(from_ir.join.right, indent)
        if from_ir.join.condition:
            cond = from_ir.join.condition
            print(f"{pad}JOIN cond hash: {cond.snippet_hash}  text: {cond.sql_text[:80]}")


def main():
    # If specific files given as args, process those only
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            process_example(Path(arg))
        return

    # Otherwise process all example dirs
    root = Path(".")
    for dir_path in EXAMPLE_DIRS:
        d = root / dir_path
        if not d.exists():
            continue
        for f in sorted(d.glob("*.json")):
            process_example(f)


if __name__ == "__main__":
    main()
