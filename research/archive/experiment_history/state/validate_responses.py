#!/usr/bin/env python3
"""
Validate all 99 collected responses on SF10 with 3-run method.
3-run: discard 1st (warmup), average last 2.

Usage:
    python3 research/state/validate_responses.py
"""

import sys
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT / "packages" / "qt-shared"))

from qt_sql.optimization.dag_v2 import DagV2Pipeline

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
RESPONSES_DIR = PROJECT / "research" / "state" / "responses"
QUERIES_DIR = PROJECT / "research" / "state" / "queries"
VALIDATION_DIR = PROJECT / "research" / "state" / "validation"
LEADERBOARD_FILE = PROJECT / "research" / "state" / "leaderboard.json"


def clean_sql(sql: str) -> str:
    lines = [l for l in sql.split('\n') if not l.strip().startswith('--')]
    clean = '\n'.join(lines).strip()
    while clean.endswith(';'):
        clean = clean[:-1].strip()
    return clean


def extract_optimized_sql(original_sql: str, response_text: str) -> Optional[str]:
    try:
        pipeline = DagV2Pipeline(original_sql)
        optimized = pipeline.apply_response(response_text)
        if optimized and optimized.strip() != original_sql.strip():
            return optimized
        return None
    except Exception as e:
        return None


def validate_one(q: int, con) -> Dict[str, Any]:
    """Validate one query. Returns result dict."""
    response_path = RESPONSES_DIR / f"q{q}_response.txt"
    query_path = QUERIES_DIR / f"q{q}_current.sql"

    if not response_path.exists():
        return {"query": f"q{q}", "status": "skip", "error": "no response"}
    if not query_path.exists():
        query_path = PROJECT / "research" / "pipeline" / "state_0" / "queries" / f"q{q}.sql"
    if not query_path.exists():
        return {"query": f"q{q}", "status": "skip", "error": "no SQL"}

    original_sql = clean_sql(query_path.read_text())
    response_text = response_path.read_text()

    # Multi-statement: use first
    statements = [s.strip() for s in original_sql.split(';') if s.strip()]
    sql = statements[0] if statements else original_sql

    # Parse response
    optimized_sql = extract_optimized_sql(sql, response_text)
    if optimized_sql is None:
        return {"query": f"q{q}", "status": "parse_error", "speedup": 0}

    # Save optimized SQL
    opt_path = RESPONSES_DIR / f"q{q}_optimized.sql"
    opt_path.write_text(optimized_sql)

    def run_timed(sql_text):
        try:
            start = time.time()
            con.execute(sql_text).fetchall()
            return (time.time() - start) * 1000
        except Exception as e:
            return None

    # 3 runs original
    orig_times = []
    for _ in range(3):
        t = run_timed(sql)
        if t is None:
            return {"query": f"q{q}", "status": "error", "error": "original failed", "speedup": 1.0}
        orig_times.append(t)

    # 3 runs optimized
    opt_times = []
    for _ in range(3):
        t = run_timed(optimized_sql)
        if t is None:
            return {"query": f"q{q}", "status": "error", "error": "optimized failed", "speedup": 0}
        opt_times.append(t)

    # Discard 1st, average last 2
    orig_avg = sum(orig_times[1:]) / 2
    opt_avg = sum(opt_times[1:]) / 2
    speedup = orig_avg / opt_avg if opt_avg > 0 else 0

    # Row count check
    try:
        orig_rows = len(con.execute(sql).fetchall())
        opt_rows = len(con.execute(optimized_sql).fetchall())
        rows_match = orig_rows == opt_rows
    except:
        rows_match = False

    if not rows_match:
        status = "wrong_results"
    elif speedup >= 1.1:
        status = "WIN"
    elif speedup >= 1.05:
        status = "IMPROVED"
    elif speedup >= 0.95:
        status = "NEUTRAL"
    else:
        status = "REGRESSION"

    return {
        "query": f"q{q}",
        "status": status,
        "speedup": round(speedup, 4),
        "original_ms": round(orig_avg, 2),
        "optimized_ms": round(opt_avg, 2),
        "original_times": [round(t, 2) for t in orig_times],
        "optimized_times": [round(t, 2) for t in opt_times],
        "rows_match": rows_match,
    }


def main():
    import duckdb
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {DB_PATH}...", file=sys.stderr)
    con = duckdb.connect(DB_PATH, read_only=True)

    results = {}
    for q in range(1, 100):
        print(f"[{q}/99] Q{q}: ", file=sys.stderr, end="", flush=True)
        r = validate_one(q, con)
        results[f"q{q}"] = r

        status = r["status"]
        speedup = r.get("speedup", 0)
        if status in ("WIN", "IMPROVED", "NEUTRAL", "REGRESSION"):
            print(f"{speedup:.2f}x [{status}]", file=sys.stderr)
        else:
            print(f"{status} - {r.get('error', '')}", file=sys.stderr)

        # Save per-query validation
        val_path = VALIDATION_DIR / f"q{q}_validation.json"
        val_path.write_text(json.dumps(r, indent=2))

    con.close()

    # Build leaderboard
    queries = sorted(results.values(), key=lambda r: r.get("speedup", 0), reverse=True)
    wins = sum(1 for r in results.values() if r["status"] == "WIN")
    improved = sum(1 for r in results.values() if r["status"] == "IMPROVED")
    neutral = sum(1 for r in results.values() if r["status"] == "NEUTRAL")
    regression = sum(1 for r in results.values() if r["status"] == "REGRESSION")
    errors = sum(1 for r in results.values() if r["status"] in ("error", "parse_error", "wrong_results"))
    skips = sum(1 for r in results.values() if r["status"] == "skip")

    valid = [r["speedup"] for r in results.values() if r["status"] in ("WIN", "IMPROVED", "NEUTRAL", "REGRESSION")]
    avg = sum(valid) / len(valid) if valid else 1.0

    leaderboard = {
        "state": "state_0",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "provider": "deepseek",
        "model": "deepseek-reasoner",
        "validation": "3-run (discard warmup, average last 2)",
        "database": DB_PATH,
        "summary": {
            "total": 99,
            "wins": wins,
            "improved": improved,
            "neutral": neutral,
            "regression": regression,
            "errors": errors,
            "skips": skips,
            "avg_speedup": round(avg, 4),
        },
        "queries": queries,
    }

    LEADERBOARD_FILE.write_text(json.dumps(leaderboard, indent=2))

    print(f"\n{'='*50}", file=sys.stderr)
    print(f"WIN: {wins}, IMPROVED: {improved}, NEUTRAL: {neutral}", file=sys.stderr)
    print(f"REGRESSION: {regression}, ERRORS: {errors}, SKIPS: {skips}", file=sys.stderr)
    print(f"Avg speedup: {avg:.2f}x", file=sys.stderr)
    print(f"Leaderboard: {LEADERBOARD_FILE}", file=sys.stderr)


if __name__ == "__main__":
    main()
