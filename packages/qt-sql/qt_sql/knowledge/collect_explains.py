"""
Collect EXPLAIN ANALYZE plans for all gold examples (original + optimized).

Produces: knowledge/explain_plans.json
"""
import json
import subprocess
import time
from pathlib import Path

KNOWLEDGE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = KNOWLEDGE_DIR.parents[3]
EXAMPLES_DIR = KNOWLEDGE_DIR.parent / "examples"

DUCKDB_PATH = "/mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
PG_DSN = {"host": "127.0.0.1", "port": "5434", "dbname": "dsb_sf10", "user": "jakc9"}


def _run_duckdb_explain(sql: str, timeout: int = 120) -> str:
    """Run EXPLAIN ANALYZE via DuckDB CLI using stdin."""
    sql_clean = sql.rstrip().rstrip(";")
    explain_sql = f"EXPLAIN ANALYZE {sql_clean};\n"
    try:
        result = subprocess.run(
            ["duckdb", DUCKDB_PATH],
            input=explain_sql,
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr.strip()}"
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        return f"TIMEOUT: query exceeded {timeout}s"


def _run_pg_explain(sql: str, timeout: int = 120) -> str:
    """Run EXPLAIN (ANALYZE, BUFFERS) via psql."""
    # Strip trailing semicolons, wrap in EXPLAIN
    sql_clean = sql.rstrip().rstrip(";")
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql_clean};"
    try:
        result = subprocess.run(
            ["psql", "-h", PG_DSN["host"], "-p", PG_DSN["port"],
             "-d", PG_DSN["dbname"], "-U", PG_DSN["user"],
             "-c", explain_sql],
            capture_output=True, text=True, timeout=timeout
        )
        if result.returncode != 0:
            return f"ERROR: {result.stderr.strip()}"
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        return f"TIMEOUT: query exceeded {timeout}s"


def collect_all():
    results = []

    # DuckDB golds
    duckdb_dir = EXAMPLES_DIR / "duckdb"
    duckdb_files = sorted(duckdb_dir.glob("*.json"))
    print(f"Found {len(duckdb_files)} DuckDB gold examples")

    for f in duckdb_files:
        example = json.loads(f.read_text())
        eid = example["id"]
        print(f"  [{eid}] collecting original plan...", end=" ", flush=True)

        t0 = time.time()
        orig_plan = _run_duckdb_explain(example["original_sql"])
        orig_time = time.time() - t0
        print(f"{orig_time:.1f}s", end=" | ", flush=True)

        print("optimized plan...", end=" ", flush=True)
        t0 = time.time()
        opt_plan = _run_duckdb_explain(example["optimized_sql"])
        opt_time = time.time() - t0
        print(f"{opt_time:.1f}s")

        results.append({
            "id": eid,
            "engine": "duckdb",
            "benchmark": example.get("benchmark_queries", []),
            "verified_speedup": example.get("verified_speedup", ""),
            "sf10_speedup": example.get("sf10_speedup"),
            "original_plan": orig_plan,
            "optimized_plan": opt_plan,
            "original_time_s": round(orig_time, 2),
            "optimized_time_s": round(opt_time, 2),
        })

    # PostgreSQL golds
    pg_dir = EXAMPLES_DIR / "postgres"
    pg_files = sorted(pg_dir.glob("*.json"))
    print(f"\nFound {len(pg_files)} PostgreSQL gold examples")

    for f in pg_files:
        example = json.loads(f.read_text())
        eid = example["id"]
        print(f"  [{eid}] collecting original plan...", end=" ", flush=True)

        t0 = time.time()
        orig_plan = _run_pg_explain(example["original_sql"])
        orig_time = time.time() - t0
        print(f"{orig_time:.1f}s", end=" | ", flush=True)

        print("optimized plan...", end=" ", flush=True)
        t0 = time.time()
        opt_plan = _run_pg_explain(example["optimized_sql"])
        opt_time = time.time() - t0
        print(f"{opt_time:.1f}s")

        results.append({
            "id": eid,
            "engine": "postgresql",
            "benchmark": example.get("benchmark_queries", []),
            "verified_speedup": example.get("verified_speedup", ""),
            "sf10_speedup": example.get("sf10_speedup"),
            "original_plan": orig_plan,
            "optimized_plan": opt_plan,
            "original_time_s": round(orig_time, 2),
            "optimized_time_s": round(opt_time, 2),
        })

    # Save
    out_path = KNOWLEDGE_DIR / "explain_plans.json"
    out_path.write_text(json.dumps(results, indent=2))
    print(f"\nSaved {len(results)} explain plan pairs to {out_path}")

    # Summary
    print("\n=== Summary ===")
    for r in results:
        status = "OK" if not r["original_plan"].startswith(("ERROR", "TIMEOUT")) else "FAIL"
        opt_status = "OK" if not r["optimized_plan"].startswith(("ERROR", "TIMEOUT")) else "FAIL"
        print(f"  {r['id']:40s} orig={status:4s} opt={opt_status:4s} "
              f"({r['original_time_s']:.1f}s / {r['optimized_time_s']:.1f}s)")


if __name__ == "__main__":
    collect_all()
