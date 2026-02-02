#!/usr/bin/env python3
"""
DSPy Full Benchmark - All 99 TPC-DS Queries
With validation + retries, correct benchmark methodology

Usage:
    # Default (backward compatible)
    python research/scripts/test_dspy_all.py

    # DAG mode
    python research/scripts/test_dspy_all.py --dag

    # MCTS fallback on failures
    python research/scripts/test_dspy_all.py --mcts-on-failure

    # Combined
    python research/scripts/test_dspy_all.py --dag --mcts-on-failure

    # Specific queries
    python research/scripts/test_dspy_all.py --dag --queries q1,q16,q23

    # Verbose logging
    python research/scripts/test_dspy_all.py --dag --mcts-on-failure --verbose
"""

import argparse
import logging
import os
import sys
import re
import json
import time
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


def parse_args():
    parser = argparse.ArgumentParser(description="DSPy Full Benchmark")
    parser.add_argument("--dag", action="store_true",
                        help="Use DAG-based node-level rewrites")
    parser.add_argument("--mcts-on-failure", action="store_true",
                        help="Escalate failed optimizations to MCTS")
    parser.add_argument("--mcts-iterations", type=int, default=30,
                        help="Max MCTS iterations (default: 30)")
    parser.add_argument("--mcts-parallel", type=int, default=4,
                        help="MCTS parallel workers (default: 4)")
    parser.add_argument("--queries", type=str, default=None,
                        help="Comma-separated query IDs (e.g., q1,q16)")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose logging")
    return parser.parse_args()


args = parse_args()


# ============================================================
# Logging & Progress Tracking
# ============================================================
@dataclass
class APICall:
    """Record of an API call."""
    timestamp: str
    provider: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    duration_ms: int = 0
    error: Optional[str] = None


@dataclass
class TransformAttempt:
    """Record of a transformation attempt."""
    transform_id: str
    input_sql_hash: str
    output_sql: Optional[str] = None
    valid: bool = False
    speedup: float = 1.0
    error: Optional[str] = None
    duration_ms: int = 0


@dataclass
class QueryLog:
    """Detailed log for a single query optimization."""
    query_id: str
    start_time: str = ""
    end_time: str = ""
    phase: str = ""  # "dspy", "mcts"
    api_calls: List[APICall] = field(default_factory=list)
    transform_attempts: List[TransformAttempt] = field(default_factory=list)
    db_validations: int = 0
    db_benchmarks: int = 0
    final_status: str = ""
    final_speedup: float = 1.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_id": self.query_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "phase": self.phase,
            "api_calls": [vars(c) for c in self.api_calls],
            "transform_attempts": [vars(t) for t in self.transform_attempts],
            "db_validations": self.db_validations,
            "db_benchmarks": self.db_benchmarks,
            "final_status": self.final_status,
            "final_speedup": self.final_speedup,
        }


class ProgressTracker:
    """Track and display progress with detailed logging."""

    def __init__(self, output_dir: Path, verbose: bool = False):
        self.output_dir = output_dir
        self.verbose = verbose
        self.query_logs: Dict[str, QueryLog] = {}
        self.current_query: Optional[str] = None
        self.total_api_calls = 0
        self.total_db_calls = 0

        # Setup logging
        self.log_file = output_dir / "detailed.log"
        self._setup_logging()

    def _setup_logging(self):
        """Configure file and console logging."""
        self.logger = logging.getLogger("dspy_benchmark")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()

        # File handler - everything
        fh = logging.FileHandler(self.log_file, mode='w')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s | %(levelname)-5s | %(message)s',
            datefmt='%H:%M:%S'
        ))
        self.logger.addHandler(fh)

        # Console handler - info and above (or debug if verbose)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.DEBUG if self.verbose else logging.INFO)
        ch.setFormatter(logging.Formatter('%(message)s'))
        self.logger.addHandler(ch)

    def start_query(self, query_id: str, total: int, current: int):
        """Start tracking a new query."""
        self.current_query = query_id
        self.query_logs[query_id] = QueryLog(
            query_id=query_id,
            start_time=datetime.now().isoformat()
        )
        progress = f"[{current}/{total}]"
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"{progress} Starting {query_id.upper()}")
        self.logger.info(f"{'='*60}")

    def log_phase(self, phase: str):
        """Log current optimization phase."""
        if self.current_query:
            self.query_logs[self.current_query].phase = phase
        self.logger.info(f"  Phase: {phase}")

    def log_api_call(self, provider: str, duration_ms: int,
                     prompt_tokens: int = 0, completion_tokens: int = 0,
                     error: Optional[str] = None):
        """Log an API call."""
        self.total_api_calls += 1
        call = APICall(
            timestamp=datetime.now().isoformat(),
            provider=provider,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            duration_ms=duration_ms,
            error=error
        )
        if self.current_query:
            self.query_logs[self.current_query].api_calls.append(call)

        status = "✓" if not error else f"✗ {error[:30]}"
        self.logger.debug(
            f"    API [{provider}] {duration_ms}ms "
            f"tokens={prompt_tokens}+{completion_tokens} {status}"
        )

    def log_transform(self, transform_id: str, valid: bool,
                      speedup: float = 1.0, error: Optional[str] = None,
                      duration_ms: int = 0):
        """Log a transformation attempt."""
        attempt = TransformAttempt(
            transform_id=transform_id,
            input_sql_hash="",  # Could add hash if needed
            valid=valid,
            speedup=speedup,
            error=error,
            duration_ms=duration_ms
        )
        if self.current_query:
            self.query_logs[self.current_query].transform_attempts.append(attempt)

        status = f"✓ {speedup:.2f}x" if valid else f"✗ {error[:40] if error else 'invalid'}"
        self.logger.debug(f"    Transform [{transform_id}] {duration_ms}ms → {status}")

    def log_db_validation(self, original_rows: int, optimized_rows: int,
                          match: bool, duration_ms: int):
        """Log a database validation."""
        self.total_db_calls += 1
        if self.current_query:
            self.query_logs[self.current_query].db_validations += 1

        status = "✓ match" if match else f"✗ {original_rows} vs {optimized_rows}"
        self.logger.debug(f"    DB Validate: {duration_ms}ms → {status}")

    def log_db_benchmark(self, sql_type: str, time_ms: float, rows: int):
        """Log a database benchmark run."""
        self.total_db_calls += 1
        if self.current_query:
            self.query_logs[self.current_query].db_benchmarks += 1
        self.logger.debug(f"    DB Benchmark [{sql_type}]: {time_ms:.1f}ms, {rows} rows")

    def log_mcts_iteration(self, iteration: int, total: int,
                           best_speedup: float, nodes_expanded: int):
        """Log MCTS progress."""
        bar_width = 20
        filled = int(bar_width * iteration / total)
        bar = "█" * filled + "░" * (bar_width - filled)
        self.logger.info(
            f"    MCTS [{bar}] {iteration}/{total} "
            f"best={best_speedup:.2f}x nodes={nodes_expanded}"
        )

    def end_query(self, status: str, speedup: float = 1.0):
        """End tracking for current query."""
        if self.current_query:
            log = self.query_logs[self.current_query]
            log.end_time = datetime.now().isoformat()
            log.final_status = status
            log.final_speedup = speedup

            # Summary
            n_api = len(log.api_calls)
            n_transforms = len(log.transform_attempts)
            self.logger.info(
                f"  Result: {status} | speedup={speedup:.2f}x | "
                f"api_calls={n_api} transforms={n_transforms}"
            )
        self.current_query = None

    def save_logs(self):
        """Save all query logs to JSON."""
        logs_file = self.output_dir / "query_logs.json"
        with open(logs_file, 'w') as f:
            json.dump(
                {qid: log.to_dict() for qid, log in self.query_logs.items()},
                f, indent=2
            )
        self.logger.info(f"\nDetailed logs saved to: {logs_file}")
        self.logger.info(f"Total API calls: {self.total_api_calls}")
        self.logger.info(f"Total DB calls: {self.total_db_calls}")

if not os.getenv("DEEPSEEK_API_KEY"):
    print("ERROR: Set DEEPSEEK_API_KEY")
    sys.exit(1)

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

import dspy
import duckdb

# ============================================================
# Config
# ============================================================
SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
PROMPTS_DIR = Path("research/prompts/batch")
MAX_RETRIES = 2

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
mode_suffix = "_dag" if args.dag else ""
mode_suffix += "_mcts" if args.mcts_on_failure else ""
output_dir = Path(f"research/experiments/dspy_runs/all_{timestamp}{mode_suffix}")
output_dir.mkdir(parents=True, exist_ok=True)

# Initialize progress tracker
tracker = ProgressTracker(output_dir, verbose=args.verbose)

# Query selection
if args.queries:
    QUERIES = [q.strip() for q in args.queries.split(",")]
else:
    QUERIES = [f"q{i}" for i in range(1, 100)]

mode_desc = "DAG" if args.dag else "Full-SQL"
if args.mcts_on_failure:
    mode_desc += " + MCTS fallback"

print(f"DSPy Full Benchmark - DeepSeek Baseline")
print(f"=" * 60)
print(f"Mode: {mode_desc}")
print(f"Output: {output_dir}")
print(f"Queries: {len(QUERIES)} queries")
print(f"Max retries: {MAX_RETRIES}")
print(f"Benchmark: 3 runs, discard 1st, avg 2-3")
print(f"Verbose: {args.verbose}")
if args.mcts_on_failure:
    print(f"MCTS config: {args.mcts_iterations} iterations, {args.mcts_parallel} parallel")

# ============================================================
# Setup
# ============================================================
MODEL_NAME = "deepseek"
DB_NAME = "duckdb"

print(f"\nConfiguring DeepSeek with model-specific constraints...")
lm = dspy.LM(
    "openai/deepseek-chat",
    api_key=os.getenv("DEEPSEEK_API_KEY"),
    api_base="https://api.deepseek.com"
)
dspy.configure(lm=lm)

from qt_sql.optimization.dspy_optimizer import (
    ValidatedOptimizationPipeline,
    DagOptimizationPipeline,
    create_duckdb_validator,
    load_model_config,
)

# Show loaded constraints
model_config = load_model_config(MODEL_NAME)
if model_config.get("constraints"):
    print(f"Loaded {len(model_config['constraints'])} constraints for {MODEL_NAME}:")
    for c in model_config['constraints'][:3]:
        print(f"  - {c['id']}")

base_validator = create_duckdb_validator(SAMPLE_DB)


def logging_validator(original_sql: str, optimized_sql: str):
    """Validator wrapper that logs DB calls."""
    start = time.perf_counter()
    correct, error = base_validator(original_sql, optimized_sql)
    duration_ms = int((time.perf_counter() - start) * 1000)

    # Log the validation
    tracker.log_db_validation(
        original_rows=0,  # Would need to extract from error msg
        optimized_rows=0,
        match=correct,
        duration_ms=duration_ms
    )
    return correct, error


def create_pipeline(use_dag: bool):
    """Create the appropriate optimization pipeline."""
    if use_dag:
        return DagOptimizationPipeline(
            validator_fn=logging_validator,
            max_retries=MAX_RETRIES,
            model_name=MODEL_NAME,
            db_name=DB_NAME
        ), "dag"
    else:
        return ValidatedOptimizationPipeline(
            validator_fn=logging_validator,
            max_retries=MAX_RETRIES,
            model_name=MODEL_NAME,
            db_name=DB_NAME
        ), "full-sql"


def escalate_to_mcts(sql: str, db_path: str, iterations: int, parallel: int):
    """Escalate a failed DSPy optimization to MCTS.

    Returns:
        tuple: (optimized_sql, method, speedup, valid, mcts_log) or failure tuple
    """
    tracker.log_phase("MCTS Escalation")
    mcts_log = {
        "iterations": iterations,
        "parallel": parallel,
        "transforms_tried": [],
        "best_speedup": 1.0,
        "elapsed_ms": 0,
        "tree_stats": {},
    }

    try:
        from qt_sql.optimization.mcts import MCTSSQLOptimizer
        import logging

        # Enable MCTS logging to see progress
        mcts_logger = logging.getLogger("qt_sql.optimization.mcts")
        mcts_logger.setLevel(logging.INFO)
        # Add handler that forwards to our tracker
        class TrackerHandler(logging.Handler):
            def emit(self, record):
                tracker.logger.debug(f"    MCTS: {record.getMessage()}")
        mcts_logger.addHandler(TrackerHandler())

        tracker.logger.info(f"    Starting MCTS: {iterations} iterations, {parallel} parallel")

        mcts_start = time.perf_counter()

        with MCTSSQLOptimizer(database=db_path, provider="deepseek") as optimizer:
            if parallel > 1:
                result = optimizer.optimize_parallel(
                    query=sql,
                    max_iterations=iterations,
                    num_parallel=parallel,
                )
            else:
                result = optimizer.optimize(
                    query=sql,
                    max_iterations=iterations,
                )

        elapsed_ms = int((time.perf_counter() - mcts_start) * 1000)
        mcts_log["elapsed_ms"] = elapsed_ms
        mcts_log["best_speedup"] = result.speedup
        mcts_log["transforms_tried"] = result.transforms_applied
        mcts_log["tree_stats"] = result.tree_stats

        # Log results
        tracker.logger.info(
            f"    MCTS completed in {elapsed_ms}ms: "
            f"speedup={result.speedup:.2f}x valid={result.valid} "
            f"iterations={result.iterations}"
        )

        if result.transforms_applied:
            for t in result.transforms_applied:
                tracker.log_transform(
                    transform_id=t,
                    valid=result.valid,
                    speedup=result.speedup,
                    duration_ms=elapsed_ms // max(1, len(result.transforms_applied))
                )

        tracker.log_mcts_iteration(
            iteration=result.iterations,
            total=iterations,
            best_speedup=result.speedup,
            nodes_expanded=result.tree_stats.get("total_nodes", 0)
        )

        # Add detailed log and summary to mcts_log
        if result.detailed_log:
            mcts_log["detailed_log"] = result.detailed_log
        if result.attempt_summary:
            mcts_log["attempt_summary"] = result.attempt_summary
            # Log summary to console
            tracker.logger.info("    MCTS Attempt Summary:")
            for tid, stats in result.attempt_summary.items():
                tracker.logger.info(
                    f"      {tid}: {stats['total']} attempts, "
                    f"{stats['validation_pass']} passed, "
                    f"max speedup={stats['max_speedup']:.2f}x"
                )

        if result.valid and result.speedup > 1.0:
            return result.optimized_sql, result.method, result.speedup, True, mcts_log
        return None, None, None, False, mcts_log

    except Exception as e:
        tracker.logger.error(f"    MCTS error: {e}")
        import traceback
        tracker.logger.debug(traceback.format_exc())
        return None, None, None, False, mcts_log


pipeline, pipeline_mode = create_pipeline(args.dag)

print(f"Connecting to database...")
conn = duckdb.connect(SAMPLE_DB, read_only=True)

# ============================================================
# Helpers
# ============================================================
def extract_from_prompt(prompt_text):
    sql_match = re.search(r'```sql\n(.*?)```', prompt_text, re.DOTALL)
    sql = sql_match.group(1).strip() if sql_match else ""
    plan_match = re.search(r'\*\*Operators by cost:\*\*(.*?)(?=\*\*Table scans|\n---)', prompt_text, re.DOTALL)
    plan = plan_match.group(1).strip() if plan_match else ""
    scans_match = re.search(r'\*\*Table scans:\*\*(.*?)(?=\n---)', prompt_text, re.DOTALL)
    scans = scans_match.group(1).strip() if scans_match else ""
    return sql, plan, scans

def benchmark(sql, runs=3, sql_type="query"):
    """3 runs, discard first, average 2-3."""
    times = []
    rows = 0
    for i in range(runs):
        start = time.perf_counter()
        result = conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        rows = len(result)
        tracker.log_db_benchmark(f"{sql_type}_run{i+1}", elapsed * 1000, rows)
    return sum(times[1:]) / 2, rows

# ============================================================
# Run all queries
# ============================================================
results = []
stats = {"success": 0, "failed": 0, "error": 0, "skipped": 0}
method_counts = defaultdict(int)
total_queries = len(QUERIES)

print(f"\n{'='*60}")
print(f"Running {total_queries} queries...")
print(f"{'='*60}\n")

for idx, qname in enumerate(QUERIES, 1):
    prompt_file = PROMPTS_DIR / f"{qname}_prompt.txt"

    # Skip if no prompt
    if not prompt_file.exists():
        tracker.logger.info(f"[{idx}/{total_queries}] {qname.upper()} SKIP - no prompt")
        stats["skipped"] += 1
        continue

    sql, plan, scans = extract_from_prompt(prompt_file.read_text())
    if not sql:
        tracker.logger.info(f"[{idx}/{total_queries}] {qname.upper()} SKIP - no SQL in prompt")
        stats["skipped"] += 1
        continue

    # Start tracking this query
    tracker.start_query(qname, total_queries, idx)

    # Benchmark original
    try:
        tracker.logger.debug("  Benchmarking original query...")
        orig_time, orig_rows = benchmark(sql, sql_type="original")
        tracker.logger.info(f"  Original: {orig_time*1000:.1f}ms, {orig_rows} rows")
    except Exception as e:
        tracker.logger.error(f"  ERROR - original query failed: {e}")
        tracker.end_query("orig_error")
        stats["error"] += 1
        results.append({"query": qname, "status": "orig_error", "error": str(e)})
        continue

    # Run DSPy pipeline (full-sql or dag)
    try:
        tracker.log_phase(f"DSPy ({pipeline_mode})")
        dspy_start = time.perf_counter()

        if args.dag:
            result = pipeline(sql=sql, plan=plan)
        else:
            result = pipeline(query=sql, plan=plan, rows=scans)

        dspy_duration = int((time.perf_counter() - dspy_start) * 1000)
        tracker.log_api_call("deepseek", dspy_duration)
        tracker.logger.info(f"  DSPy completed in {dspy_duration}ms, attempts={result.attempts}")

    except Exception as e:
        tracker.logger.error(f"  ERROR - DSPy failed: {e}")
        tracker.end_query("dspy_error")
        stats["error"] += 1
        results.append({"query": qname, "status": "dspy_error", "error": str(e)})
        continue

    # Check result
    method = pipeline_mode
    optimized_sql = result.optimized_sql
    dspy_correct = result.correct
    mcts_log = None

    # If DSPy failed and MCTS fallback is enabled, try MCTS
    if not dspy_correct and args.mcts_on_failure:
        tracker.logger.info(f"  DSPy validation failed, escalating to MCTS...")
        mcts_sql, mcts_method, mcts_speedup, mcts_valid, mcts_log = escalate_to_mcts(
            sql, SAMPLE_DB, args.mcts_iterations, args.mcts_parallel
        )
        if mcts_valid:
            optimized_sql = mcts_sql
            method = mcts_method
            dspy_correct = True
            tracker.logger.info(f"  MCTS succeeded: {mcts_speedup:.2f}x via {mcts_method}")

    if dspy_correct:
        try:
            tracker.logger.debug("  Benchmarking optimized query...")
            opt_time, opt_rows = benchmark(optimized_sql, sql_type="optimized")
            speedup = orig_time / opt_time if opt_time > 0 else 1.0

            # Classify result
            if speedup >= 1.5:
                status_icon = "✓✓"
                stats["success"] += 1
            elif speedup >= 1.1:
                status_icon = "✓"
                stats["success"] += 1
            else:
                status_icon = "~"
                stats["success"] += 1

            # Track method
            method_key = method.split(":")[0] if ":" in method else method
            method_counts[method_key] += 1

            tracker.end_query("success", speedup)
            tracker.logger.info(
                f"  ▶ {status_icon} {orig_time*1000:.1f}ms → {opt_time*1000:.1f}ms "
                f"({speedup:.2f}x) method={method}"
            )

            result_entry = {
                "query": qname,
                "status": "success",
                "method": method,
                "original_time": round(orig_time, 4),
                "optimized_time": round(opt_time, 4),
                "speedup": round(speedup, 2),
                "attempts": result.attempts,
                "rows": orig_rows
            }
            if mcts_log:
                result_entry["mcts_log"] = mcts_log
            results.append(result_entry)

            # Save query
            qdir = output_dir / qname
            qdir.mkdir(exist_ok=True)
            (qdir / "original.sql").write_text(sql)
            (qdir / "optimized.sql").write_text(optimized_sql)
            # Use explanation for DAG, rationale for full-sql
            rationale = getattr(result, 'explanation', None) or getattr(result, 'rationale', '')
            (qdir / "rationale.txt").write_text(rationale)
            (qdir / "method.txt").write_text(method)
            if mcts_log:
                (qdir / "mcts_log.json").write_text(json.dumps(mcts_log, indent=2))

        except Exception as e:
            tracker.logger.error(f"  ERROR - benchmark failed: {e}")
            tracker.end_query("bench_error")
            stats["error"] += 1
            results.append({"query": qname, "status": "bench_error", "error": str(e)})
    else:
        error_preview = (result.error or "Unknown")[:50]
        tracker.logger.warning(f"  ✗ FAILED after {result.attempts} attempts - {error_preview}...")
        tracker.end_query("validation_failed")
        stats["failed"] += 1
        result_entry = {
            "query": qname,
            "status": "validation_failed",
            "method": method,
            "attempts": result.attempts,
            "error": result.error
        }
        if mcts_log:
            result_entry["mcts_log"] = mcts_log
        results.append(result_entry)

        # Save failed attempt
        qdir = output_dir / qname
        qdir.mkdir(exist_ok=True)
        (qdir / "original.sql").write_text(sql)
        (qdir / "failed.sql").write_text(result.optimized_sql)
        (qdir / "error.txt").write_text(result.error or "Unknown")
        if mcts_log:
            (qdir / "mcts_log.json").write_text(json.dumps(mcts_log, indent=2))

conn.close()

# Save detailed logs
tracker.save_logs()

# ============================================================
# Summary
# ============================================================
print(f"\n{'='*60}")
print(f"SUMMARY")
print(f"{'='*60}")

print(f"\nStats:")
print(f"  Success: {stats['success']}")
print(f"  Failed validation: {stats['failed']}")
print(f"  Errors: {stats['error']}")
print(f"  Skipped: {stats['skipped']}")

# Method breakdown
if method_counts:
    print(f"\nBy method:")
    for method, count in sorted(method_counts.items()):
        print(f"  {method}: {count}")

# Top speedups
successful = [r for r in results if r.get("status") == "success" and r.get("speedup", 0) >= 1.1]
successful.sort(key=lambda x: x["speedup"], reverse=True)

print(f"\nTop speedups (≥1.1x):")
print(f"{'Query':<8} {'Original':<10} {'Optimized':<10} {'Speedup':<10} {'Method':<15} {'Attempts'}")
print("-" * 70)
for r in successful[:20]:
    method_short = r.get('method', 'unknown')[:15]
    print(f"{r['query']:<8} {r['original_time']:<10.4f} {r['optimized_time']:<10.4f} {r['speedup']:<10.2f}x {method_short:<15} {r['attempts']}")

# Failed queries
failed = [r for r in results if r.get("status") == "validation_failed"]
if failed:
    print(f"\nFailed queries ({len(failed)}):")
    for r in failed:
        print(f"  {r['query']}: {r.get('error', 'Unknown')[:60]}...")

# Save results
with open(output_dir / "results.json", "w") as f:
    json.dump(results, f, indent=2)

with open(output_dir / "summary.txt", "w") as f:
    f.write(f"DSPy Full Benchmark\n")
    f.write(f"Date: {timestamp}\n")
    f.write(f"Mode: {mode_desc}\n")
    f.write(f"Model: {MODEL_NAME} (with tuned constraints)\n")
    f.write(f"Database: {DB_NAME}\n")
    f.write(f"Max retries: {MAX_RETRIES}\n")
    if args.mcts_on_failure:
        f.write(f"MCTS iterations: {args.mcts_iterations}\n")
        f.write(f"MCTS parallel: {args.mcts_parallel}\n")
    f.write(f"\n")
    f.write(f"Success: {stats['success']}\n")
    f.write(f"Failed: {stats['failed']}\n")
    f.write(f"Errors: {stats['error']}\n")
    f.write(f"Skipped: {stats['skipped']}\n\n")
    if method_counts:
        f.write(f"By method:\n")
        for method, count in sorted(method_counts.items()):
            f.write(f"  {method}: {count}\n")
        f.write(f"\n")
    f.write(f"Top speedups:\n")
    for r in successful[:20]:
        f.write(f"  {r['query']}: {r['speedup']:.2f}x ({r.get('method', 'unknown')})\n")

print(f"\nResults saved to: {output_dir}")
