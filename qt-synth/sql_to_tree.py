#!/usr/bin/env python3
"""Convert SQL to qt-synth logical tree text instantly (AST-only).

Usage:
  python3 qt-synth/sql_to_tree.py path/to/query.sql
  python3 qt-synth/sql_to_tree.py path/to/query.sql --dialect postgres
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "packages" / "qt-sql"))

from qt_sql.dag import CostAnalyzer, LogicalTreeBuilder  # noqa: E402
from qt_sql.logic_tree import build_logic_tree  # noqa: E402


def _read_sql(path: str) -> str:
    if path == "-":
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AST-only SQL -> tree converter (no query execution)."
    )
    parser.add_argument("sql_file", help="Path to SQL file, or '-' to read stdin")
    parser.add_argument(
        "--dialect",
        default="postgres",
        help="SQL dialect for parsing (e.g. postgres, duckdb, snowflake)",
    )
    parser.add_argument(
        "--output",
        "-o",
        help="Optional output file path for tree text",
    )
    args = parser.parse_args()

    sql = _read_sql(args.sql_file)
    dag = LogicalTreeBuilder(sql, dialect=args.dialect).build()
    costs = CostAnalyzer(dag, plan_context=None).analyze()
    tree = build_logic_tree(sql=sql, dag=dag, costs=costs, dialect=args.dialect)

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(tree + "\n", encoding="utf-8")
    else:
        print(tree)


if __name__ == "__main__":
    main()
