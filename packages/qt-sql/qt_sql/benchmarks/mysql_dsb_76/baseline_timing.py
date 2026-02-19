"""Run all 76 DSB queries on MySQL and record baseline timings.

Runs each query twice (warmup + measured), records the measured run.
Outputs a CSV with query_id, status, elapsed_ms, row_count.
"""
import json
import subprocess
import sys
import time
from pathlib import Path

QUERIES_DIR = Path(__file__).parent / "queries"
CONTAINER = "mysql-dsb"
DB = "dsb_sf10"
USER = "root"
PASSWORD = "dsb2026"
TIMEOUT_S = 300


def run_query(sql: str, timeout_s: int = TIMEOUT_S):
    """Execute SQL in the MySQL container, return (elapsed_ms, row_count, error)."""
    cmd = [
        "docker", "exec", "-i", CONTAINER,
        "mysql", f"-u{USER}", f"-p{PASSWORD}", DB,
        "--batch", "--skip-column-names",
    ]
    start = time.time()
    try:
        result = subprocess.run(
            cmd,
            input=sql,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
        elapsed_ms = (time.time() - start) * 1000

        if result.returncode != 0:
            stderr = result.stderr.replace("mysql: [Warning] Using a password on the command line interface can be insecure.\n", "")
            return elapsed_ms, 0, stderr.strip()[:200]

        rows = [line for line in result.stdout.strip().split("\n") if line]
        return elapsed_ms, len(rows), None

    except subprocess.TimeoutExpired:
        elapsed_ms = (time.time() - start) * 1000
        return elapsed_ms, 0, f"TIMEOUT ({timeout_s}s)"


def main():
    query_files = sorted(QUERIES_DIR.glob("*.sql"))
    print(f"Running {len(query_files)} queries on MySQL ({CONTAINER})...")
    print(f"{'#':<4} {'QUERY':<30} {'STATUS':<12} {'MS':>10} {'ROWS':>8}")
    print("-" * 70)

    results = []
    for i, qf in enumerate(query_files, 1):
        qid = qf.stem
        sql = qf.read_text().strip()

        # Warmup run (discard)
        run_query(sql, timeout_s=TIMEOUT_S)

        # Measured run
        elapsed_ms, row_count, error = run_query(sql, timeout_s=TIMEOUT_S)

        if error:
            status = "ERROR" if "TIMEOUT" not in error else "TIMEOUT"
            print(f"{i:<4} {qid:<30} {status:<12} {elapsed_ms:>10.1f} {0:>8}  {error[:60]}")
        else:
            status = "OK"
            print(f"{i:<4} {qid:<30} {status:<12} {elapsed_ms:>10.1f} {row_count:>8}")
        sys.stdout.flush()

        results.append({
            "query_id": qid,
            "status": status,
            "elapsed_ms": round(elapsed_ms, 1),
            "row_count": row_count,
            "error": error,
        })

    # Summary
    ok = sum(1 for r in results if r["status"] == "OK")
    err = sum(1 for r in results if r["status"] == "ERROR")
    timeout = sum(1 for r in results if r["status"] == "TIMEOUT")
    ok_times = [r["elapsed_ms"] for r in results if r["status"] == "OK"]
    avg_ms = sum(ok_times) / len(ok_times) if ok_times else 0

    print(f"\nOK: {ok}  |  ERROR: {err}  |  TIMEOUT: {timeout}  |  AVG(OK): {avg_ms:.0f}ms")

    out_path = Path(__file__).parent / "baseline_timing.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"Saved to {out_path}")


if __name__ == "__main__":
    main()
