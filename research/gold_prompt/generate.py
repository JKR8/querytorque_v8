"""Generate gold-standard optimization prompts.

This is the winning format that achieved 2.18x speedup on Q23 with Gemini.
"""

import os
import sys
import json

# Add package paths
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-sql'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../packages/qt-shared'))

from qt_sql.optimization import build_full_prompt


def get_plan_summary(sql: str, db_path: str) -> dict | None:
    """Run query and extract execution plan summary from DuckDB profiler."""
    try:
        import duckdb
    except ImportError:
        return None

    try:
        conn = duckdb.connect(db_path, read_only=True)
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute("PRAGMA profiling_output='/tmp/plan.json'")
        conn.execute(sql).fetchall()
        conn.close()

        with open('/tmp/plan.json') as f:
            plan = json.load(f)

        operators = []
        scans = []
        total_time = 0.0

        def walk(node):
            nonlocal total_time
            name = node.get("operator_name", node.get("name", "")).strip()
            timing = node.get("operator_timing", 0)
            rows = node.get("operator_cardinality", 0)
            extra = node.get("extra_info", {})

            if name and name != "EXPLAIN_ANALYZE":
                total_time += timing

                table = ""
                if isinstance(extra, dict):
                    table = extra.get("Table", "")
                elif isinstance(extra, str) and "Table:" in extra:
                    table = extra.split("Table:")[-1].strip().split()[0]

                operators.append({
                    "op": name,
                    "table": table,
                    "time": timing,
                    "rows_out": rows,
                })

                if "SCAN" in name.upper() and table:
                    has_filter = False
                    filter_expr = ""
                    if isinstance(extra, dict):
                        filters = extra.get("Filters", "")
                        if filters:
                            has_filter = True
                            filter_expr = filters[:50]

                    scans.append({
                        "table": table,
                        "rows": rows,
                        "has_filter": has_filter,
                        "filter_expr": filter_expr,
                    })

            for child in node.get("children", []):
                walk(child)

        for child in plan.get("children", []):
            walk(child)

        for op in operators:
            op["cost_pct"] = round(op["time"] / total_time * 100, 1) if total_time > 0 else 0

        operators.sort(key=lambda x: x["cost_pct"], reverse=True)

        return {
            "top_operators": operators[:5],
            "scans": scans,
            "total_time_ms": round(total_time * 1000, 1),
        }
    except Exception as e:
        print(f"Warning: Could not get plan: {e}")
        return None


def generate_prompt(sql: str, db_path: str = None) -> str:
    """Generate gold-standard optimization prompt.

    Args:
        sql: The SQL query to optimize
        db_path: Optional DuckDB path for execution plan. If None, generates without plan.

    Returns:
        Complete prompt string ready for LLM
    """
    plan_summary = None
    if db_path:
        plan_summary = get_plan_summary(sql, db_path)

    return build_full_prompt(sql, plan_summary)


# Example usage
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate optimization prompt")
    parser.add_argument("sql_file", help="Path to SQL file")
    parser.add_argument("--db", help="DuckDB path for execution plan (optional)")
    parser.add_argument("-o", "--output", help="Output file (default: stdout)")
    args = parser.parse_args()

    with open(args.sql_file) as f:
        sql = f.read()

    prompt = generate_prompt(sql, args.db)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(prompt)
        print(f"Wrote {len(prompt):,} chars to {args.output}")
    else:
        print(prompt)
