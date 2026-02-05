#!/usr/bin/env python3
"""
Benchmark collected optimizations against sample DB.

Runs validation sequentially on all collected optimized SQL.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))

# Sample DB for validation
SAMPLE_DB = Path("/mnt/d/TPC-DS/tpcds_sf100.duckdb")

# Collected optimizations
OPT_DIRS = [
    REPO_ROOT / "research/experiments/optimizations/kimi_q1-q30_20260202_213955/benchmark_ready",
    REPO_ROOT / "research/experiments/optimizations/kimi_q31-q99_20260202_215203/benchmark_ready",
]

OUTPUT_DIR = REPO_ROOT / "research/benchmarks/qt-sql/runs"


def run_validation(original_sql: str, optimized_sql: str, query_num: int, output_dir: Path) -> dict:
    """Run qt-sql CLI validation."""
    query_dir = output_dir / f"q{query_num}"
    query_dir.mkdir(parents=True, exist_ok=True)

    # Write SQL files
    orig_file = query_dir / "original.sql"
    opt_file = query_dir / "optimized.sql"
    orig_file.write_text(original_sql)
    opt_file.write_text(optimized_sql)

    # Run CLI validation
    cmd = [
        sys.executable, "-m", "cli.main", "validate",
        str(orig_file),
        str(opt_file),
        "--database", str(SAMPLE_DB),
        "--mode", "sample",
        "--json",
    ]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT / "packages" / "qt-sql"),
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.stdout.strip():
            validation = json.loads(result.stdout.strip())
            (query_dir / "validation.json").write_text(json.dumps(validation, indent=2))
            return validation
        else:
            error = result.stderr or "No output"
            (query_dir / "error.txt").write_text(error)
            return {"status": "error", "error": error}

    except subprocess.TimeoutExpired:
        return {"status": "error", "error": "Timeout (300s)"}
    except json.JSONDecodeError as e:
        (query_dir / "error.txt").write_text(f"JSON error: {e}\nOutput: {result.stdout}")
        return {"status": "error", "error": f"Invalid JSON: {e}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def main():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = OUTPUT_DIR / f"kimi_benchmark_{timestamp}"
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{'='*60}")
    print(f"Benchmarking Collected Optimizations")
    print(f"Database: {SAMPLE_DB}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}\n")

    # Collect all query files
    queries = {}
    for opt_dir in OPT_DIRS:
        if not opt_dir.exists():
            print(f"Warning: {opt_dir} not found")
            continue
        for f in opt_dir.glob("q*_original.sql"):
            q_num = int(f.stem.replace("q", "").replace("_original", ""))
            queries[q_num] = {
                "original": f.read_text(),
                "optimized": (opt_dir / f"q{q_num}_optimized.sql").read_text(),
            }

    print(f"Found {len(queries)} queries to benchmark\n")

    results = []
    for q_num in sorted(queries.keys()):
        q = queries[q_num]
        print(f"Q{q_num}: ", end="", flush=True)

        validation = run_validation(q["original"], q["optimized"], q_num, output_dir)

        # Extract key metrics
        status = validation.get("status", "unknown")
        timing = validation.get("timing", {})
        speedup = timing.get("speedup", 0)
        orig_ms = timing.get("original_ms", 0)
        opt_ms = timing.get("optimized_ms", 0)

        result = {
            "query": q_num,
            "status": status,
            "original_ms": orig_ms,
            "optimized_ms": opt_ms,
            "speedup": speedup,
            "error": validation.get("error") or validation.get("errors"),
        }
        results.append(result)

        if status == "pass":
            print(f"✓ {speedup:.2f}x ({orig_ms:.0f}ms → {opt_ms:.0f}ms)")
        elif status == "error":
            err = str(result["error"])[:40] if result["error"] else "unknown"
            print(f"✗ ERROR: {err}")
        else:
            print(f"✗ FAIL: {status}")

    # Summary
    passed = [r for r in results if r["status"] == "pass"]
    failed = [r for r in results if r["status"] != "pass"]

    print(f"\n{'='*60}")
    print(f"Results: {len(passed)}/{len(results)} passed")

    if passed:
        speedups = [r["speedup"] for r in passed]
        avg_speedup = sum(speedups) / len(speedups)
        improvements = [r for r in passed if r["speedup"] > 1.0]
        regressions = [r for r in passed if r["speedup"] < 1.0]

        print(f"Average speedup: {avg_speedup:.2f}x")
        print(f"Improvements (>1x): {len(improvements)}")
        print(f"Regressions (<1x): {len(regressions)}")

        # Top 10 speedups
        top = sorted(passed, key=lambda r: r["speedup"], reverse=True)[:10]
        print(f"\nTop 10 speedups:")
        for r in top:
            print(f"  Q{r['query']}: {r['speedup']:.2f}x")

    if failed:
        print(f"\nFailed queries:")
        for r in failed[:10]:
            err = str(r["error"])[:50] if r["error"] else "unknown"
            print(f"  Q{r['query']}: {err}")

    # Save results
    summary = {
        "timestamp": timestamp,
        "total": len(results),
        "passed": len(passed),
        "failed": len(failed),
        "avg_speedup": sum(r["speedup"] for r in passed) / len(passed) if passed else 0,
        "results": results,
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))

    print(f"\nResults saved to: {output_dir}")


if __name__ == "__main__":
    main()
