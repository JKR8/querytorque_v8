#!/usr/bin/env python3
"""
Pipeline Step 2+3: Call LLM API with all 99 prompts, then validate on SF10 (3-run).

Usage:
    # Test on single query first:
    python3 research/state/run_api_and_validate.py --test-one 1

    # Run all 99:
    python3 research/state/run_api_and_validate.py

    # Resume from checkpoint:
    python3 research/state/run_api_and_validate.py --resume
"""

import sys
import json
import time
import argparse
import traceback
from pathlib import Path
from typing import Optional, Dict, Any

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT / "packages" / "qt-shared"))

from qt_shared.llm import create_llm_client
from qt_sql.optimization.dag_v2 import DagV2Pipeline

# ============================================================================
# PATHS
# ============================================================================

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
PROMPTS_DIR = PROJECT / "research" / "state" / "prompts"
QUERIES_DIR = PROJECT / "research" / "state" / "queries"
RESPONSES_DIR = PROJECT / "research" / "state" / "responses"
VALIDATION_DIR = PROJECT / "research" / "state" / "validation"
CHECKPOINT_FILE = VALIDATION_DIR / "checkpoint.json"
LEADERBOARD_FILE = PROJECT / "research" / "state" / "leaderboard.json"

# Validation: 3-run (discard 1st warmup, average last 2)
VALIDATION_RUNS = 3


def load_checkpoint() -> Dict[str, Any]:
    """Load checkpoint to resume from."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"completed": [], "results": {}}


def save_checkpoint(checkpoint: Dict[str, Any]):
    """Save checkpoint after each query."""
    CHECKPOINT_FILE.write_text(json.dumps(checkpoint, indent=2))


def call_api(prompt: str, provider: str = None, model: str = None) -> str:
    """Send prompt to LLM and return raw response text."""
    client = create_llm_client(provider=provider, model=model)
    if client is None:
        raise RuntimeError("No LLM client configured. Check .env for QT_LLM_PROVIDER")
    return client.analyze(prompt)


def extract_optimized_sql(original_sql: str, llm_response: str) -> Optional[str]:
    """Parse LLM JSON response and assemble optimized SQL."""
    try:
        pipeline = DagV2Pipeline(original_sql)
        optimized = pipeline.apply_response(llm_response)
        if optimized and optimized.strip() != original_sql.strip():
            return optimized
        return None
    except Exception as e:
        print(f"    apply_response error: {e}", file=sys.stderr)
        return None


def validate_query(db_path: str, original_sql: str, optimized_sql: str) -> Dict[str, Any]:
    """Validate optimized SQL on SF10 with 3-run method.

    3-run: discard 1st (warmup), average last 2.
    """
    import duckdb

    def run_timed(con, sql: str, timeout: int = 300) -> Optional[float]:
        """Run query and return execution time in ms."""
        try:
            start = time.time()
            con.execute(sql).fetchall()
            elapsed = (time.time() - start) * 1000
            return elapsed
        except Exception as e:
            print(f"    Execution error: {e}", file=sys.stderr)
            return None

    con = duckdb.connect(db_path, read_only=True)

    # Time original (3 runs)
    original_times = []
    for i in range(VALIDATION_RUNS):
        t = run_timed(con, original_sql)
        if t is None:
            con.close()
            return {"status": "error", "error": "original query failed", "speedup": 1.0}
        original_times.append(t)

    # Time optimized (3 runs)
    optimized_times = []
    for i in range(VALIDATION_RUNS):
        t = run_timed(con, optimized_sql)
        if t is None:
            con.close()
            return {"status": "error", "error": "optimized query failed", "speedup": 0.0}
        optimized_times.append(t)

    con.close()

    # 3-run method: discard 1st, average last 2
    orig_avg = sum(original_times[1:]) / (VALIDATION_RUNS - 1)
    opt_avg = sum(optimized_times[1:]) / (VALIDATION_RUNS - 1)

    speedup = orig_avg / opt_avg if opt_avg > 0 else 0.0

    # Correctness check: compare row counts
    # (full result comparison would be ideal but row count is a quick sanity check)
    try:
        con2 = duckdb.connect(db_path, read_only=True)
        orig_rows = len(con2.execute(original_sql).fetchall())
        opt_rows = len(con2.execute(optimized_sql).fetchall())
        con2.close()
        rows_match = orig_rows == opt_rows
    except Exception:
        rows_match = False

    # Classify
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
        "status": status,
        "speedup": round(speedup, 4),
        "original_ms": round(orig_avg, 2),
        "optimized_ms": round(opt_avg, 2),
        "original_times": [round(t, 2) for t in original_times],
        "optimized_times": [round(t, 2) for t in optimized_times],
        "rows_match": rows_match,
    }


def clean_sql(sql: str) -> str:
    """Strip comments and trailing semicolons."""
    lines = []
    for line in sql.split('\n'):
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        lines.append(line)
    clean = '\n'.join(lines).strip()
    while clean.endswith(';'):
        clean = clean[:-1].strip()
    return clean


def process_query(q: int, provider: str = None, model: str = None) -> Dict[str, Any]:
    """Process a single query: API call → parse → validate."""

    prompt_path = PROMPTS_DIR / f"q{q}_prompt.txt"
    query_path = QUERIES_DIR / f"q{q}_current.sql"

    if not prompt_path.exists():
        return {"query": f"q{q}", "status": "skip", "error": "no prompt file"}
    if not query_path.exists():
        # Fallback to pipeline baseline
        query_path = PROJECT / "research" / "pipeline" / "state_0" / "queries" / f"q{q}.sql"
    if not query_path.exists():
        return {"query": f"q{q}", "status": "skip", "error": "no SQL file"}

    prompt = prompt_path.read_text()
    original_sql = clean_sql(query_path.read_text())

    if not original_sql.strip():
        return {"query": f"q{q}", "status": "skip", "error": "empty SQL"}

    # For multi-statement queries, use first statement
    statements = [s.strip() for s in original_sql.split(';') if s.strip()]
    sql_for_validation = statements[0] if statements else original_sql

    result = {"query": f"q{q}"}

    # Step 1: Call API
    print(f"  Q{q}: Calling API...", file=sys.stderr, end="", flush=True)
    start = time.time()
    try:
        response_text = call_api(prompt, provider=provider, model=model)
        api_duration = time.time() - start
        print(f" {api_duration:.1f}s", file=sys.stderr, end="", flush=True)
    except Exception as e:
        print(f" ERROR: {e}", file=sys.stderr)
        result["status"] = "api_error"
        result["error"] = str(e)
        return result

    # Save response
    RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
    response_path = RESPONSES_DIR / f"q{q}_response.txt"
    response_path.write_text(response_text)

    # Step 2: Parse response → optimized SQL
    print(" → Parse...", file=sys.stderr, end="", flush=True)
    optimized_sql = extract_optimized_sql(sql_for_validation, response_text)

    if optimized_sql is None:
        print(" PARSE_FAIL", file=sys.stderr)
        result["status"] = "parse_error"
        result["error"] = "Could not extract optimized SQL from response"
        result["api_duration_s"] = round(api_duration, 1)
        return result

    # Save optimized SQL
    optimized_path = RESPONSES_DIR / f"q{q}_optimized.sql"
    optimized_path.write_text(optimized_sql)

    # Step 3: Validate on SF10
    print(" → Validate...", file=sys.stderr, end="", flush=True)
    validation = validate_query(DB_PATH, sql_for_validation, optimized_sql)
    print(f" {validation['speedup']:.2f}x [{validation['status']}]", file=sys.stderr)

    # Save validation
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)
    val_path = VALIDATION_DIR / f"q{q}_validation.json"
    val_path.write_text(json.dumps({
        "query": f"q{q}",
        "api_duration_s": round(api_duration, 1),
        **validation,
        "optimized_sql_path": str(optimized_path),
        "response_path": str(response_path),
    }, indent=2))

    result.update(validation)
    result["api_duration_s"] = round(api_duration, 1)
    return result


def build_leaderboard(results: Dict[str, Dict]) -> Dict[str, Any]:
    """Build leaderboard from all results."""
    queries = []
    wins = improved = neutral = regression = errors = skips = 0

    for qid, r in sorted(results.items(), key=lambda x: x[1].get("speedup", 0), reverse=True):
        status = r.get("status", "skip")
        if status == "WIN":
            wins += 1
        elif status == "IMPROVED":
            improved += 1
        elif status == "NEUTRAL":
            neutral += 1
        elif status == "REGRESSION":
            regression += 1
        elif status in ("error", "api_error", "parse_error", "wrong_results"):
            errors += 1
        else:
            skips += 1

        queries.append({
            "query": r.get("query", qid),
            "speedup": r.get("speedup", 0),
            "status": status,
            "original_ms": r.get("original_ms", 0),
            "optimized_ms": r.get("optimized_ms", 0),
        })

    valid_speedups = [r.get("speedup", 1.0) for r in results.values()
                      if r.get("status") in ("WIN", "IMPROVED", "NEUTRAL", "REGRESSION")]
    avg_speedup = sum(valid_speedups) / len(valid_speedups) if valid_speedups else 1.0

    return {
        "state": "state_0_deepseek",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "provider": "deepseek",
        "model": "deepseek-reasoner",
        "validation_method": "3-run (discard warmup, average last 2)",
        "database": DB_PATH,
        "summary": {
            "total": len(results),
            "wins": wins,
            "improved": improved,
            "neutral": neutral,
            "regression": regression,
            "errors": errors,
            "skips": skips,
            "avg_speedup": round(avg_speedup, 4),
        },
        "queries": queries,
    }


def main():
    parser = argparse.ArgumentParser(description="Run API + Validate for TPC-DS queries")
    parser.add_argument("--test-one", type=int, help="Test on a single query number")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    parser.add_argument("--provider", type=str, help="Override LLM provider")
    parser.add_argument("--model", type=str, help="Override LLM model")
    parser.add_argument("--queries", type=str, help="Comma-separated query numbers (e.g. 1,2,3)")
    args = parser.parse_args()

    RESPONSES_DIR.mkdir(parents=True, exist_ok=True)
    VALIDATION_DIR.mkdir(parents=True, exist_ok=True)

    provider = args.provider
    model = args.model

    # Single query test mode
    if args.test_one:
        print(f"\n=== TEST MODE: Q{args.test_one} ===\n", file=sys.stderr)
        result = process_query(args.test_one, provider=provider, model=model)
        print(json.dumps(result, indent=2))
        return

    # Load checkpoint
    checkpoint = load_checkpoint() if args.resume else {"completed": [], "results": {}}

    # Determine which queries to run
    if args.queries:
        query_nums = [int(q.strip()) for q in args.queries.split(",")]
    else:
        query_nums = list(range(1, 100))

    total = len(query_nums)
    completed = checkpoint["completed"]
    remaining = [q for q in query_nums if f"q{q}" not in completed]

    print(f"\n=== Pipeline: API + Validate (SF10 3-run) ===", file=sys.stderr)
    print(f"Total: {total}, Already done: {len(completed)}, Remaining: {len(remaining)}", file=sys.stderr)
    print(f"Provider: {provider or 'from .env'}, Model: {model or 'from .env'}", file=sys.stderr)
    print(f"Output: {RESPONSES_DIR}", file=sys.stderr)
    print(f"", file=sys.stderr)

    for i, q in enumerate(remaining):
        print(f"[{i+1}/{len(remaining)}]", file=sys.stderr, end="", flush=True)

        try:
            result = process_query(q, provider=provider, model=model)
        except Exception as e:
            print(f"  Q{q}: FATAL ERROR: {e}", file=sys.stderr)
            traceback.print_exc(file=sys.stderr)
            result = {"query": f"q{q}", "status": "error", "error": str(e)}

        checkpoint["results"][f"q{q}"] = result
        checkpoint["completed"].append(f"q{q}")
        save_checkpoint(checkpoint)

    # Build leaderboard
    print(f"\n=== Building Leaderboard ===", file=sys.stderr)
    leaderboard = build_leaderboard(checkpoint["results"])
    LEADERBOARD_FILE.write_text(json.dumps(leaderboard, indent=2))

    # Print summary
    s = leaderboard["summary"]
    print(f"\nRESULTS:", file=sys.stderr)
    print(f"  WIN: {s['wins']}, IMPROVED: {s['improved']}, NEUTRAL: {s['neutral']}", file=sys.stderr)
    print(f"  REGRESSION: {s['regression']}, ERRORS: {s['errors']}", file=sys.stderr)
    print(f"  Avg speedup: {s['avg_speedup']:.2f}x", file=sys.stderr)
    print(f"\nLeaderboard: {LEADERBOARD_FILE}", file=sys.stderr)

    # Also print JSON to stdout
    print(json.dumps(leaderboard, indent=2))


if __name__ == "__main__":
    main()
