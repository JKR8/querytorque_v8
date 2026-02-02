#!/usr/bin/env python3
"""
DAG v1 vs DAG v2 vs Full SQL Benchmark Comparison

Compares 3 optimization modes using Kimi K2.5 via OpenRouter:
1. v1 DAG (DagOptimizationPipeline)
2. v2 DAG (DagV2Pipeline)
3. Full SQL (ValidatedOptimizationPipeline)

Uses qt-sql CLI validation module with 1-1-2-2 timing pattern.
Saves all API outputs for reproducibility.

Usage:
    python benchmark_dag_comparison.py                    # Run Q1 only
    python benchmark_dag_comparison.py --queries q1,q15   # Specific queries
    python benchmark_dag_comparison.py --queries all      # All 99 queries
    python benchmark_dag_comparison.py --verbose          # Verbose output
"""

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

# Add packages to path
REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-sql"))
sys.path.insert(0, str(REPO_ROOT / "packages" / "qt-shared"))

import dspy
from openai import OpenAI

# Configuration
OPENROUTER_API_BASE = "https://openrouter.ai/api/v1"
MODEL_NAME = "moonshotai/kimi-k2.5"
PROVIDER_NAME = "kimi-k2.5"

# Paths
FULL_DB = Path("/mnt/d/TPC-DS/tpcds_sf100.duckdb")
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
PROMPTS_DIR = REPO_ROOT / "research" / "prompts" / "batch"
OUTPUT_BASE = REPO_ROOT / "research" / "experiments" / "dspy_runs"


@dataclass
class APICall:
    """Record of an API call."""
    timestamp: str
    mode: str
    query: str
    model: str
    prompt: str
    response: str
    tokens_in: int = 0
    tokens_out: int = 0
    latency_ms: float = 0


@dataclass
class QueryResult:
    """Result for a single query in one mode."""
    query: str
    mode: str
    status: str  # success, validation_failed, error
    original_time_ms: float = 0
    optimized_time_ms: float = 0
    speedup: float = 0
    row_count: int = 0
    checksum_match: bool = False
    attempts: int = 1
    error: Optional[str] = None
    optimized_sql: Optional[str] = None
    api_calls: List[APICall] = field(default_factory=list)


@dataclass
class BenchmarkRun:
    """Complete benchmark run."""
    timestamp: str
    model: str
    queries: List[str]
    modes: List[str]
    results: List[QueryResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "model": self.model,
            "queries": self.queries,
            "modes": self.modes,
            "results": [asdict(r) for r in self.results],
        }


def get_api_key() -> str:
    """Get OpenRouter API key from env or file."""
    key = os.getenv("OPENROUTER_API_KEY")
    if not key:
        key_file = REPO_ROOT / "openrouter.txt"
        if key_file.exists():
            key = key_file.read_text().strip()
    if not key:
        print("ERROR: Set OPENROUTER_API_KEY or create openrouter.txt")
        sys.exit(1)
    return key


def setup_dspy(api_key: str) -> None:
    """Configure DSPy with Kimi via OpenRouter."""
    lm = dspy.LM(
        f"openai/{MODEL_NAME}",
        api_key=api_key,
        api_base=OPENROUTER_API_BASE,
        extra_headers={
            "HTTP-Referer": "https://querytorque.com",
            "X-Title": "QueryTorque DAG Benchmark"
        }
    )
    dspy.configure(lm=lm)


def load_query(query_name: str) -> tuple[str, str]:
    """Load query SQL and prompt."""
    # Extract query number (q1 -> 1, q15 -> 15)
    query_num = query_name.replace("q", "")

    # Load SQL - files are named query_1.sql, query_2.sql, etc.
    sql_file = QUERIES_DIR / f"query_{query_num}.sql"
    if not sql_file.exists():
        raise FileNotFoundError(f"Query file not found: {sql_file}")
    sql = sql_file.read_text()

    # Load prompt if exists
    prompt_file = PROMPTS_DIR / f"{query_name}_prompt.txt"
    prompt = ""
    if prompt_file.exists():
        prompt = prompt_file.read_text()

    return sql, prompt


def run_cli_validation(
    original_sql: str,
    optimized_sql: str,
    output_dir: Path,
    verbose: bool = False,
) -> dict:
    """Run qt-sql CLI validation and return results."""
    # Write SQL files
    orig_file = output_dir / "original.sql"
    opt_file = output_dir / "optimized.sql"
    orig_file.write_text(original_sql)
    opt_file.write_text(optimized_sql)

    # Run CLI validation
    cmd = [
        sys.executable, "-m", "cli.main", "validate",
        str(orig_file),
        str(opt_file),
        "--database", str(FULL_DB),
        "--mode", "full",
        "--json",  # Output as JSON
    ]

    if verbose:
        print(f"  Running: {' '.join(cmd[:6])}...")

    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT / "packages" / "qt-sql"),
            capture_output=True,
            text=True,
            timeout=600,  # 10 min timeout
        )

        # Parse JSON output
        if result.stdout.strip():
            return json.loads(result.stdout.strip())
        else:
            return {
                "status": "error",
                "error": result.stderr or "No output from validation",
            }
    except subprocess.TimeoutExpired:
        return {"status": "error", "error": "Validation timeout (600s)"}
    except json.JSONDecodeError as e:
        return {"status": "error", "error": f"Invalid JSON: {e}"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


def optimize_full_sql(
    sql: str,
    prompt: str,
    api_key: str,
    output_dir: Path,
    max_retries: int = 2,
    verbose: bool = False,
) -> tuple[Optional[str], List[APICall]]:
    """Optimize using Full SQL mode via DSPy."""
    from qt_sql.optimization.dspy_optimizer import ValidatedOptimizationPipeline

    api_calls = []

    # Save INPUT before calling API
    input_data = {
        "mode": "full_sql",
        "model": MODEL_NAME,
        "sql": sql,
        "plan": prompt,
        "max_retries": max_retries,
    }
    (output_dir / "input.json").write_text(json.dumps(input_data, indent=2))
    (output_dir / "input_sql.sql").write_text(sql)
    (output_dir / "input_plan.txt").write_text(prompt)

    # Create a simple validator that always passes (we validate separately)
    def dummy_validator(orig, opt):
        return True, "Deferred validation"

    try:
        pipeline = ValidatedOptimizationPipeline(
            validator_fn=dummy_validator,
            max_retries=max_retries,
            model_name=PROVIDER_NAME,
            db_name="duckdb",
        )

        # Capture the call
        start = time.perf_counter()
        result = pipeline(query=sql, plan=prompt, rows="")
        latency = (time.perf_counter() - start) * 1000

        # Record API call
        api_calls.append(APICall(
            timestamp=datetime.now().isoformat(),
            mode="full_sql",
            query="optimization",
            model=MODEL_NAME,
            prompt=f"SQL:\n{sql}\n\nPlan:\n{prompt}",
            response=result.optimized_sql or "",
            latency_ms=latency,
        ))

        # Save ALL outputs
        (output_dir / "output_optimized.sql").write_text(result.optimized_sql or "")
        (output_dir / "output_rationale.txt").write_text(result.rationale or "")
        (output_dir / "output.json").write_text(json.dumps({
            "optimized_sql": result.optimized_sql,
            "rationale": result.rationale,
            "correct": result.correct,
            "attempts": result.attempts,
            "latency_ms": latency,
        }, indent=2))

        return result.optimized_sql, api_calls

    except Exception as e:
        if verbose:
            print(f"  Full SQL error: {e}")
        (output_dir / "error.txt").write_text(str(e))
        return None, api_calls


def optimize_dag_v1(
    sql: str,
    prompt: str,
    api_key: str,
    output_dir: Path,
    max_retries: int = 2,
    verbose: bool = False,
) -> tuple[Optional[str], List[APICall]]:
    """Optimize using DAG v1 mode via DSPy."""
    from qt_sql.optimization.dspy_optimizer import DagOptimizationPipeline

    api_calls = []

    # Save INPUT before calling API
    input_data = {
        "mode": "dag_v1",
        "model": MODEL_NAME,
        "sql": sql,
        "plan": prompt,
        "max_retries": max_retries,
    }
    (output_dir / "input.json").write_text(json.dumps(input_data, indent=2))
    (output_dir / "input_sql.sql").write_text(sql)
    (output_dir / "input_plan.txt").write_text(prompt)

    # Dummy validator
    def dummy_validator(orig, opt):
        return True, "Deferred validation"

    try:
        pipeline = DagOptimizationPipeline(
            validator_fn=dummy_validator,
            max_retries=max_retries,
            model_name=PROVIDER_NAME,
            db_name="duckdb",
        )

        start = time.perf_counter()
        result = pipeline(sql=sql, plan=prompt)
        latency = (time.perf_counter() - start) * 1000

        # Record API call
        api_calls.append(APICall(
            timestamp=datetime.now().isoformat(),
            mode="dag_v1",
            query="optimization",
            model=MODEL_NAME,
            prompt=f"SQL:\n{sql}\n\nPlan:\n{prompt}",
            response=result.optimized_sql or "",
            latency_ms=latency,
        ))

        # Save ALL outputs
        (output_dir / "output_optimized.sql").write_text(result.optimized_sql or "")
        (output_dir / "output_explanation.txt").write_text(result.explanation or "")
        if result.rewrites:
            (output_dir / "output_rewrites.json").write_text(
                json.dumps(result.rewrites, indent=2)
            )
        (output_dir / "output.json").write_text(json.dumps({
            "optimized_sql": result.optimized_sql,
            "explanation": result.explanation,
            "rewrites": result.rewrites,
            "correct": result.correct,
            "attempts": result.attempts,
            "latency_ms": latency,
        }, indent=2))

        return result.optimized_sql, api_calls

    except Exception as e:
        if verbose:
            print(f"  DAG v1 error: {e}")
        (output_dir / "error.txt").write_text(str(e))
        return None, api_calls


def optimize_dag_v2(
    sql: str,
    api_key: str,
    output_dir: Path,
    verbose: bool = False,
) -> tuple[Optional[str], List[APICall]]:
    """Optimize using DAG v2 mode via raw API."""
    from qt_sql.optimization.dag_v2 import DagV2Pipeline, get_dag_v2_examples

    api_calls = []

    try:
        # Build pipeline and prompt
        pipeline = DagV2Pipeline(sql)

        # Build few-shot examples
        examples = get_dag_v2_examples()
        few_shot_parts = []
        for ex in examples[:2]:  # Use 2 examples
            few_shot_parts.append(f"### Example: {ex['opportunity']}")
            few_shot_parts.append(f"Input:\n{ex['input_slice']}")
            few_shot_parts.append(f"Output:\n```json\n{json.dumps(ex['output'], indent=2)}\n```")
            if 'key_insight' in ex:
                few_shot_parts.append(f"Key insight: {ex['key_insight']}")
            few_shot_parts.append("")

        few_shot = "\n".join(few_shot_parts)
        base_prompt = pipeline.get_prompt()
        full_prompt = f"## Examples\n\n{few_shot}\n\n---\n\n## Your Task\n\n{base_prompt}"

        # Save ALL inputs
        dag_summary = pipeline.get_dag_summary()
        input_data = {
            "mode": "dag_v2",
            "model": MODEL_NAME,
            "sql": sql,
            "dag_summary": dag_summary,
            "num_examples": 2,
        }
        (output_dir / "input.json").write_text(json.dumps(input_data, indent=2))
        (output_dir / "input_sql.sql").write_text(sql)
        (output_dir / "input_dag_summary.txt").write_text(dag_summary)
        (output_dir / "input_prompt.txt").write_text(full_prompt)

        # Call API directly
        client = OpenAI(
            api_key=api_key,
            base_url=OPENROUTER_API_BASE,
            default_headers={
                "HTTP-Referer": "https://querytorque.com",
                "X-Title": "QueryTorque DAG v2 Benchmark"
            }
        )

        start = time.perf_counter()
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": full_prompt}],
            temperature=0.1,
            max_tokens=8192,
            timeout=600,
        )
        latency = (time.perf_counter() - start) * 1000

        content = response.choices[0].message.content
        tokens_in = response.usage.prompt_tokens if response.usage else 0
        tokens_out = response.usage.completion_tokens if response.usage else 0

        # Record API call
        api_calls.append(APICall(
            timestamp=datetime.now().isoformat(),
            mode="dag_v2",
            query="optimization",
            model=MODEL_NAME,
            prompt=full_prompt,
            response=content,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            latency_ms=latency,
        ))

        # Apply response
        optimized_sql = pipeline.apply_response(content)

        # Save ALL outputs
        (output_dir / "output_raw_response.txt").write_text(content)
        (output_dir / "output_optimized.sql").write_text(optimized_sql)
        (output_dir / "output.json").write_text(json.dumps({
            "raw_response": content,
            "optimized_sql": optimized_sql,
            "tokens_in": tokens_in,
            "tokens_out": tokens_out,
            "latency_ms": latency,
        }, indent=2))

        return optimized_sql, api_calls

    except Exception as e:
        if verbose:
            print(f"  DAG v2 error: {e}")
            import traceback
            traceback.print_exc()
        (output_dir / "error.txt").write_text(str(e))
        return None, api_calls


def run_benchmark(
    queries: List[str],
    modes: List[str],
    api_key: str,
    output_dir: Path,
    verbose: bool = False,
) -> BenchmarkRun:
    """Run full benchmark comparison."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = output_dir / f"comparison_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    run = BenchmarkRun(
        timestamp=timestamp,
        model=MODEL_NAME,
        queries=queries,
        modes=modes,
    )

    print(f"\n{'='*60}")
    print(f"DAG Comparison Benchmark")
    print(f"{'='*60}")
    print(f"Timestamp: {timestamp}")
    print(f"Model: {MODEL_NAME}")
    print(f"Queries: {', '.join(queries)}")
    print(f"Modes: {', '.join(modes)}")
    print(f"Database: {FULL_DB}")
    print(f"Output: {run_dir}")
    print(f"{'='*60}\n")

    # Setup DSPy for v1 and full_sql modes
    setup_dspy(api_key)

    for query_name in queries:
        print(f"\n[{query_name}]")
        query_dir = run_dir / query_name
        query_dir.mkdir(exist_ok=True)

        try:
            sql, prompt = load_query(query_name)
            (query_dir / "original.sql").write_text(sql)
        except FileNotFoundError as e:
            print(f"  SKIP: {e}")
            continue

        for mode in modes:
            mode_dir = query_dir / mode
            mode_dir.mkdir(exist_ok=True)

            print(f"  {mode}:", end=" ", flush=True)

            # Run optimization
            optimized_sql = None
            api_calls = []

            if mode == "full_sql":
                optimized_sql, api_calls = optimize_full_sql(
                    sql, prompt, api_key, mode_dir, verbose=verbose
                )
            elif mode == "dag_v1":
                optimized_sql, api_calls = optimize_dag_v1(
                    sql, prompt, api_key, mode_dir, verbose=verbose
                )
            elif mode == "dag_v2":
                optimized_sql, api_calls = optimize_dag_v2(
                    sql, api_key, mode_dir, verbose=verbose
                )

            # Save API calls
            if api_calls:
                (mode_dir / "api_calls.json").write_text(
                    json.dumps([asdict(c) for c in api_calls], indent=2)
                )

            if not optimized_sql:
                result = QueryResult(
                    query=query_name,
                    mode=mode,
                    status="error",
                    error="Optimization failed",
                    api_calls=api_calls,
                )
                run.results.append(result)
                print("ERROR (optimization failed)")
                continue

            # Run validation with CLI
            validation = run_cli_validation(
                sql, optimized_sql, mode_dir, verbose=verbose
            )

            # Save validation result
            (mode_dir / "validation.json").write_text(json.dumps(validation, indent=2))

            # Build result - parse nested validation JSON structure
            timing = validation.get("timing", {})
            row_counts = validation.get("row_counts", {})
            values = validation.get("values", {})

            if validation.get("status") == "pass":
                result = QueryResult(
                    query=query_name,
                    mode=mode,
                    status="success",
                    original_time_ms=timing.get("original_ms", 0),
                    optimized_time_ms=timing.get("optimized_ms", 0),
                    speedup=timing.get("speedup", 0),
                    row_count=row_counts.get("original", 0),
                    checksum_match=values.get("checksum_match", False),
                    optimized_sql=optimized_sql,
                    api_calls=api_calls,
                )
                print(f"{result.speedup:.2f}x")
            elif validation.get("status") == "error":
                result = QueryResult(
                    query=query_name,
                    mode=mode,
                    status="error",
                    error=validation.get("error") or str(validation.get("errors", [])),
                    optimized_sql=optimized_sql,
                    api_calls=api_calls,
                )
                print(f"ERROR ({str(validation.get('error', 'unknown'))[:30]})")
            else:
                result = QueryResult(
                    query=query_name,
                    mode=mode,
                    status="validation_failed",
                    original_time_ms=timing.get("original_ms", 0),
                    optimized_time_ms=timing.get("optimized_ms", 0),
                    speedup=timing.get("speedup", 0),
                    error=f"values_match={values.get('match')}, checksum={values.get('checksum_match')}",
                    optimized_sql=optimized_sql,
                    api_calls=api_calls,
                )
                print(f"FAIL (values mismatch)")

            run.results.append(result)

    # Save results
    (run_dir / "results.json").write_text(json.dumps(run.to_dict(), indent=2))

    # Generate summary
    summary = generate_summary(run)
    (run_dir / "summary.txt").write_text(summary)
    print(f"\n{summary}")

    return run


def generate_summary(run: BenchmarkRun) -> str:
    """Generate human-readable summary."""
    lines = [
        "DAG Comparison Benchmark",
        f"Date: {run.timestamp}",
        f"Model: {run.model}",
        f"Queries: {', '.join(run.queries)}",
        f"Modes: {', '.join(run.modes)}",
        "",
    ]

    # Group by mode
    by_mode: Dict[str, List[QueryResult]] = {}
    for r in run.results:
        by_mode.setdefault(r.mode, []).append(r)

    for mode in run.modes:
        results = by_mode.get(mode, [])
        success = [r for r in results if r.status == "success"]
        failed = [r for r in results if r.status == "validation_failed"]
        errors = [r for r in results if r.status == "error"]

        lines.append(f"## {mode}")
        lines.append(f"Success: {len(success)}, Failed: {len(failed)}, Errors: {len(errors)}")

        if success:
            speedups = [r.speedup for r in success]
            avg_speedup = sum(speedups) / len(speedups)
            max_speedup = max(speedups)
            lines.append(f"Avg speedup: {avg_speedup:.2f}x, Max: {max_speedup:.2f}x")

            # Top speedups
            top = sorted(success, key=lambda r: r.speedup, reverse=True)[:5]
            for r in top:
                lines.append(f"  {r.query}: {r.speedup:.2f}x")

        lines.append("")

    # Comparison table
    lines.append("## Comparison Table")
    lines.append(f"{'Query':<8} | " + " | ".join(f"{m:<10}" for m in run.modes))
    lines.append("-" * (10 + 13 * len(run.modes)))

    by_query: Dict[str, Dict[str, QueryResult]] = {}
    for r in run.results:
        by_query.setdefault(r.query, {})[r.mode] = r

    for query in run.queries:
        row = [f"{query:<8}"]
        for mode in run.modes:
            r = by_query.get(query, {}).get(mode)
            if r and r.status == "success":
                row.append(f"{r.speedup:.2f}x")
            elif r:
                row.append(r.status[:10])
            else:
                row.append("-")
        lines.append(" | ".join(f"{c:<10}" for c in row))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="DAG Comparison Benchmark")
    parser.add_argument(
        "--queries", "-q",
        default="q1",
        help="Comma-separated query names or 'all' for all 99 queries",
    )
    parser.add_argument(
        "--modes", "-m",
        default="dag_v1,dag_v2,full_sql",
        help="Comma-separated modes to test (dag_v1, dag_v2, full_sql)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output",
    )
    args = parser.parse_args()

    # Parse queries
    if args.queries == "all":
        queries = [f"q{i}" for i in range(1, 100)]
    else:
        queries = [q.strip() for q in args.queries.split(",")]

    # Parse modes
    modes = [m.strip() for m in args.modes.split(",")]

    # Validate database exists
    if not FULL_DB.exists():
        print(f"ERROR: Database not found: {FULL_DB}")
        sys.exit(1)

    # Get API key
    api_key = get_api_key()

    # Run benchmark
    run_benchmark(
        queries=queries,
        modes=modes,
        api_key=api_key,
        output_dir=OUTPUT_BASE,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
