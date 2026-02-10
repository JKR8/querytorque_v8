#!/usr/bin/env python3
"""Benchmark top winners on full DB."""

import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))

FULL_DB = Path("/mnt/d/TPC-DS/tpcds_sf100.duckdb")

# Remaining improvements to test on full DB
WINNERS = [4, 5, 8, 10, 12, 13, 17, 18, 19, 20, 23, 27, 31, 33, 37, 40, 41, 45, 46, 49, 52, 54, 57, 58, 60, 63, 64, 66, 69, 73, 76, 77, 78, 79, 80, 84, 96]

OPT_DIRS = [
    REPO_ROOT / "research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready",
    REPO_ROOT / "research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready",
]

def get_sql(query_num: int) -> tuple[str, str]:
    """Get original and optimized SQL for a query."""
    for opt_dir in OPT_DIRS:
        orig = opt_dir / f"q{query_num}_original.sql"
        opt = opt_dir / f"q{query_num}_optimized.sql"
        if orig.exists() and opt.exists():
            return orig.read_text(), opt.read_text()
    raise FileNotFoundError(f"Q{query_num} not found")

def run_validation(original_sql: str, optimized_sql: str, query_num: int) -> dict:
    """Run validation on full DB."""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        orig_file = Path(tmpdir) / "original.sql"
        opt_file = Path(tmpdir) / "optimized.sql"
        orig_file.write_text(original_sql)
        opt_file.write_text(optimized_sql)

        cmd = [
            sys.executable, "-m", "cli.main", "validate",
            str(orig_file), str(opt_file),
            "--database", str(FULL_DB),
            "--mode", "full",
            "--json",
        ]

        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT / "packages" / "qt-sql"),
            capture_output=True,
            text=True,
            timeout=600,
        )

        if result.stdout.strip():
            return json.loads(result.stdout.strip())
        return {"status": "error", "error": result.stderr}

def main():
    print(f"\n{'='*60}")
    print(f"Validating Top Winners on Full DB (SF100)")
    print(f"{'='*60}\n")

    results = []
    for q_num in WINNERS:
        print(f"Q{q_num}: ", end="", flush=True)
        try:
            orig, opt = get_sql(q_num)
            validation = run_validation(orig, opt, q_num)

            status = validation.get("status", "unknown")
            timing = validation.get("timing", {})
            speedup = timing.get("speedup", 0)
            orig_ms = timing.get("original_ms", 0)
            opt_ms = timing.get("optimized_ms", 0)

            if status == "pass":
                print(f"✓ {speedup:.2f}x ({orig_ms:.0f}ms → {opt_ms:.0f}ms)")
            else:
                err = str(validation.get("error", validation.get("errors", "unknown")))[:40]
                print(f"✗ {status}: {err}")

            results.append({
                "query": q_num,
                "status": status,
                "speedup": speedup,
                "original_ms": orig_ms,
                "optimized_ms": opt_ms,
            })

        except Exception as e:
            print(f"✗ ERROR: {e}")
            results.append({"query": q_num, "status": "error", "error": str(e)})

    # Summary
    passed = [r for r in results if r.get("status") == "pass"]
    print(f"\n{'='*60}")
    print(f"Results: {len(passed)}/{len(WINNERS)} passed on full DB")
    if passed:
        avg = sum(r["speedup"] for r in passed) / len(passed)
        print(f"Average speedup: {avg:.2f}x")

    # Save results
    output_dir = REPO_ROOT / "research/experiments/benchmarks/kimi_benchmark_20260202_221828"
    output_file = output_dir / "full_db_validation.json"

    # Load existing or create new
    if output_file.exists():
        existing = json.loads(output_file.read_text())
    else:
        existing = {"results": []}

    # Merge results
    existing_queries = {r["query"] for r in existing["results"]}
    for r in results:
        if r["query"] not in existing_queries:
            existing["results"].append(r)

    existing["results"].sort(key=lambda x: x["query"])
    output_file.write_text(json.dumps(existing, indent=2))
    print(f"\nResults saved to: {output_file}")

if __name__ == "__main__":
    main()
