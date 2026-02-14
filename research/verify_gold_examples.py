"""Verify ALL gold examples (DuckDB + PG) for literal/column preservation using AST.

Also re-benchmarks DuckDB examples on SF10 with 3x3 validation.
"""
import os
import sys
import json
import glob
import time
import duckdb
import sqlglot
from sqlglot import exp

os.chdir("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
DUCKDB_EXAMPLES = "packages/qt-sql/qt_sql/examples/duckdb"
PG_EXAMPLES = "packages/qt-sql/qt_sql/examples/postgres"
N_RUNS = 3


def extract_literals(sql, dialect="duckdb"):
    strings, numbers = set(), set()
    try:
        trees = sqlglot.parse(sql, dialect=dialect)
    except Exception:
        return None, None
    for tree in trees:
        if tree is None:
            continue
        for node in tree.walk():
            if isinstance(node, exp.Literal):
                if node.is_string:
                    val = node.this
                    if val and len(val) > 1:
                        strings.add(val)
                elif node.is_number:
                    try:
                        num = float(node.this)
                        if num not in (0, 1, -1):
                            numbers.add(num)
                    except (ValueError, TypeError):
                        pass
    return strings, numbers


def extract_column_refs(sql, dialect="duckdb"):
    cols = set()
    try:
        trees = sqlglot.parse(sql, dialect=dialect)
    except Exception:
        return None
    for tree in trees:
        if tree is None:
            continue
        for node in tree.walk():
            if isinstance(node, exp.Column):
                if node.name:
                    cols.add(node.name.lower())
    return cols


def check_example(example_data, dialect="duckdb"):
    """Check a gold example for literal/column preservation."""
    orig_sql = example_data.get("original_sql", "")
    opt_sql = example_data.get("optimized_sql", "")

    if not orig_sql or not opt_sql:
        return {"status": "MISSING_SQL"}

    orig_strings, orig_numbers = extract_literals(orig_sql, dialect)
    opt_strings, opt_numbers = extract_literals(opt_sql, dialect)

    if orig_strings is None or opt_strings is None:
        return {"status": "PARSE_ERROR"}

    missing_strings = orig_strings - opt_strings
    missing_numbers = orig_numbers - opt_numbers

    orig_cols = extract_column_refs(orig_sql, dialect)
    opt_cols = extract_column_refs(opt_sql, dialect)

    if orig_cols is None or opt_cols is None:
        return {"status": "PARSE_ERROR"}

    missing_cols = orig_cols - opt_cols

    issues = []
    if missing_strings:
        issues.append(f"missing_strings: {sorted(missing_strings)[:5]}")
    if missing_numbers:
        issues.append(f"missing_numbers: {sorted(missing_numbers)[:5]}")
    if missing_cols:
        issues.append(f"missing_cols: {sorted(missing_cols)[:5]}")

    if issues:
        return {"status": "MISMATCH", "details": "; ".join(issues)}
    return {"status": "OK"}


def benchmark_example(example_data):
    """3x3 benchmark a gold example on DuckDB SF10."""
    orig_sql = example_data.get("original_sql", "")
    opt_sql = example_data.get("optimized_sql", "")

    if not orig_sql or not opt_sql:
        return None

    def run_timed(con, sql):
        try:
            start = time.perf_counter()
            con.execute(sql)
            _ = con.fetchall()
            return (time.perf_counter() - start) * 1000
        except Exception as e:
            return None

    # Original
    con = duckdb.connect(DB_PATH, read_only=True)
    orig_times = []
    for i in range(N_RUNS):
        t = run_timed(con, orig_sql)
        if t is None:
            con.close()
            return {"status": "ORIG_ERROR"}
        orig_times.append(t)
    con.close()

    # Optimized
    con = duckdb.connect(DB_PATH, read_only=True)
    opt_times = []
    for i in range(N_RUNS):
        t = run_timed(con, opt_sql)
        if t is None:
            con.close()
            return {"status": "OPT_ERROR", "orig_ms": sum(orig_times[1:]) / (N_RUNS - 1)}
        opt_times.append(t)
    con.close()

    orig_avg = sum(orig_times[1:]) / (N_RUNS - 1)
    opt_avg = sum(opt_times[1:]) / (N_RUNS - 1)

    return {
        "status": "OK",
        "orig_ms": round(orig_avg, 1),
        "opt_ms": round(opt_avg, 1),
        "speedup": round(orig_avg / opt_avg, 2) if opt_avg > 0 else 0,
        "orig_times": [round(t, 1) for t in orig_times],
        "opt_times": [round(t, 1) for t in opt_times],
    }


def main():
    print("=" * 80)
    print("GOLD EXAMPLE VERIFICATION")
    print("=" * 80)

    # DuckDB examples
    print(f"\n--- DuckDB Examples ({DUCKDB_EXAMPLES}) ---\n")
    duckdb_files = sorted(glob.glob(os.path.join(DUCKDB_EXAMPLES, "*.json")))

    duckdb_ok = 0
    duckdb_bad = 0
    duckdb_results = []

    for fpath in duckdb_files:
        name = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)

        # AST check
        result = check_example(data, dialect="duckdb")
        claimed_speedup = data.get("speedup", "?")
        query_id = data.get("query_id", "?")

        # Benchmark on SF10
        bench = benchmark_example(data)

        if result["status"] == "OK":
            duckdb_ok += 1
            marker = "OK"
        elif result["status"] == "MISMATCH":
            duckdb_bad += 1
            marker = "MISMATCH"
        else:
            duckdb_bad += 1
            marker = result["status"]

        bench_str = ""
        if bench and bench["status"] == "OK":
            bench_str = f" | measured={bench['speedup']:.2f}x (orig={bench['orig_ms']:.0f}ms opt={bench['opt_ms']:.0f}ms)"
        elif bench:
            bench_str = f" | bench={bench['status']}"

        print(f"  [{marker:>10}] {name:<35} query={query_id:<8} claimed={claimed_speedup}x{bench_str}")
        if result["status"] == "MISMATCH":
            print(f"             {result['details']}")

        duckdb_results.append({
            "name": name,
            "query_id": query_id,
            "claimed_speedup": claimed_speedup,
            "ast_status": result["status"],
            "ast_details": result.get("details", ""),
            "bench": bench,
        })

    print(f"\n  DuckDB: {duckdb_ok} OK, {duckdb_bad} BAD out of {len(duckdb_files)}")

    # PG examples
    print(f"\n--- PostgreSQL Examples ({PG_EXAMPLES}) ---\n")
    pg_files = sorted(glob.glob(os.path.join(PG_EXAMPLES, "*.json")))

    pg_ok = 0
    pg_bad = 0

    for fpath in pg_files:
        name = os.path.basename(fpath).replace(".json", "")
        with open(fpath) as f:
            data = json.load(f)

        result = check_example(data, dialect="postgres")
        claimed_speedup = data.get("speedup", "?")
        query_id = data.get("query_id", "?")

        if result["status"] == "OK":
            pg_ok += 1
            marker = "OK"
        elif result["status"] == "MISMATCH":
            pg_bad += 1
            marker = "MISMATCH"
        else:
            pg_bad += 1
            marker = result["status"]

        print(f"  [{marker:>10}] {name:<40} query={query_id:<8} claimed={claimed_speedup}x")
        if result["status"] == "MISMATCH":
            print(f"             {result['details']}")

    print(f"\n  PostgreSQL: {pg_ok} OK, {pg_bad} BAD out of {len(pg_files)}")

    # Save DuckDB benchmark results
    output = "research/gold_example_verification.json"
    with open(output, "w") as f:
        json.dump(duckdb_results, f, indent=2)
    print(f"\nDuckDB benchmark results saved to {output}")


if __name__ == "__main__":
    main()
