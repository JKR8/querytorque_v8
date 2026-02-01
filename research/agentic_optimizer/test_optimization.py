"""Test an optimization for speedup and correctness."""

import time
import duckdb


def test_optimization(
    original_sql: str,
    optimized_sql: str,
    db_path: str,
    runs: int = 3,
) -> dict:
    """Test an optimization on a database.

    Args:
        original_sql: The original SQL query
        optimized_sql: The optimized SQL query
        db_path: Path to DuckDB database
        runs: Number of runs (first is warmup, rest are averaged)

    Returns:
        dict with keys:
            - original_time: Average time for original query
            - optimized_time: Average time for optimized query
            - speedup: Ratio (original_time / optimized_time)
            - correct: True if results match exactly
            - error: Error message if optimized query failed
    """
    conn = duckdb.connect(db_path, read_only=True)

    try:
        # Run original
        orig_times = []
        orig_result = None
        for i in range(runs):
            t1 = time.time()
            result = conn.execute(original_sql).fetchall()
            elapsed = time.time() - t1
            orig_times.append(elapsed)
            if i == 0:
                orig_result = result

        # Run optimized
        opt_times = []
        opt_result = None
        opt_error = None
        for i in range(runs):
            t1 = time.time()
            try:
                result = conn.execute(optimized_sql).fetchall()
                elapsed = time.time() - t1
                opt_times.append(elapsed)
                if i == 0:
                    opt_result = result
            except Exception as e:
                opt_error = str(e)
                break

        # Calculate averages (discard first run - warmup)
        orig_avg = sum(orig_times[1:]) / max(len(orig_times) - 1, 1)
        opt_avg = sum(opt_times[1:]) / max(len(opt_times) - 1, 1) if opt_times else 0

        # Calculate speedup
        speedup = orig_avg / opt_avg if opt_avg > 0 else 0

        # Check correctness
        correct = orig_result == opt_result

        return {
            "original_time": orig_avg,
            "optimized_time": opt_avg,
            "speedup": speedup,
            "correct": correct,
            "error": opt_error,
            "original_rows": len(orig_result) if orig_result else 0,
            "optimized_rows": len(opt_result) if opt_result else 0,
        }

    finally:
        conn.close()


def get_execution_plan(sql: str, db_path: str) -> dict:
    """Get execution plan with timing info.

    Args:
        sql: The SQL query
        db_path: Path to DuckDB database

    Returns:
        dict with plan JSON
    """
    import json

    conn = duckdb.connect(db_path, read_only=True)
    try:
        conn.execute("PRAGMA enable_profiling='json'")
        conn.execute("PRAGMA profiling_output='/tmp/profile.json'")
        conn.execute(sql).fetchall()
        conn.close()

        with open('/tmp/profile.json') as f:
            return json.load(f)
    finally:
        conn.close()


def summarize_plan(plan: dict) -> str:
    """Summarize an execution plan."""
    lines = []

    def walk(node, depth=0):
        name = node.get("operator_name", node.get("name", "?")).strip()
        timing = node.get("operator_timing", 0)
        rows = node.get("operator_cardinality", 0)
        extra = node.get("extra_info", {})
        table = extra.get("Table", "") if isinstance(extra, dict) else ""
        filters = extra.get("Filters", "") if isinstance(extra, dict) else ""

        if name and name != "EXPLAIN_ANALYZE":
            indent = "  " * depth
            info = f"{name}"
            if table:
                info += f" ({table})"
            if rows:
                info += f" - {rows:,} rows"
            if timing:
                info += f" - {timing*1000:.1f}ms"
            if filters:
                info += f" [filter: {filters}]"
            lines.append(f"{indent}{info}")

        for child in node.get("children", []):
            walk(child, depth + 1)

    for child in plan.get("children", []):
        walk(child)

    return "\n".join(lines)


# Example usage
if __name__ == "__main__":
    # Example: Test a simple optimization
    original = "SELECT * FROM store LIMIT 10"
    optimized = "SELECT s_store_sk, s_store_name FROM store LIMIT 10"

    result = test_optimization(
        original,
        optimized,
        "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
    )

    print(f"Speedup: {result['speedup']:.2f}x")
    print(f"Correct: {result['correct']}")
    if result['error']:
        print(f"Error: {result['error']}")
