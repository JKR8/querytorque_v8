#!/usr/bin/env python3
"""
Analyze Q-Error for entire TPC-DS SF10 benchmark (88 queries)
Identifies which queries have high optimizer wrongness
"""
import json
import sys
import csv
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

sys.path.insert(0, "packages/qt-shared")
sys.path.insert(0, "packages/qt-sql")

from qt_sql.execution.factory import create_executor_from_dsn
from analyze_qerror import extract_q_errors_from_node, QErrorNode

@dataclass
class BenchmarkResult:
    """Q-Error analysis for a TPC-DS query"""
    query_id: str
    query_num: int
    max_qerror: float
    severity: str
    worst_node_type: str
    estimated: int
    actual: int
    n_qerror_nodes: int
    execution_time_ms: float

def load_tpcds_queries() -> Dict[str, str]:
    """Load TPC-DS queries from benchmark directory"""
    benchmark_dir = Path("packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds")
    queries = {}

    # Try prepared directory first
    prepared_dirs = sorted(benchmark_dir.glob("prepared/*/queries"))
    if prepared_dirs:
        query_dir = prepared_dirs[-1]  # Most recent
        print(f"Loading queries from: {query_dir}", file=sys.stderr)

        for query_file in sorted(query_dir.glob("*.sql")):
            query_id = query_file.stem
            with open(query_file) as f:
                queries[query_id] = f.read()

    # Fallback: check if there's a queries directory
    if not queries:
        query_dir = benchmark_dir / "queries"
        if query_dir.exists():
            print(f"Loading queries from: {query_dir}", file=sys.stderr)
            for query_file in sorted(query_dir.glob("*.sql")):
                query_id = query_file.stem
                with open(query_file) as f:
                    queries[query_id] = f.read()

    return queries

def get_explain_analyze(executor, sql: str) -> Optional[Dict]:
    """Run EXPLAIN ANALYZE and return plan"""
    try:
        result = executor.explain(sql, analyze=True)
        return result if result else None
    except Exception as e:
        return None

def extract_query_number(query_id: str) -> int:
    """Extract numeric query ID from various formats (q1, query_1, etc.)"""
    try:
        if query_id.startswith("query_"):
            return int(query_id.replace("query_", ""))
        elif query_id.startswith("q"):
            return int(query_id.replace("q", ""))
        else:
            return 0
    except ValueError:
        return 0

def analyze_query(executor, query_id: str, sql: str) -> Optional[BenchmarkResult]:
    """Analyze Q-Error for a single TPC-DS query"""
    # Get EXPLAIN
    plan = get_explain_analyze(executor, sql)
    if not plan:
        return None

    # Extract execution time
    exec_time_ms = plan.get("latency", 0) * 1000  # Convert to ms

    # Extract Q-Error nodes
    plan_tree = {"children": plan.get("children", [])}
    errors = extract_q_errors_from_node(plan_tree)

    query_num = extract_query_number(query_id)

    if not errors:
        # No significant Q-Error found
        return BenchmarkResult(
            query_id=query_id,
            query_num=query_num,
            max_qerror=1.0,
            severity="ACCURATE",
            worst_node_type="N/A",
            estimated=0,
            actual=0,
            n_qerror_nodes=0,
            execution_time_ms=exec_time_ms
        )

    # Find max Q-Error
    max_error = max(errors, key=lambda x: x.q_error)

    return BenchmarkResult(
        query_id=query_id,
        query_num=query_num,
        max_qerror=max_error.q_error,
        severity=max_error.severity(),
        worst_node_type=max_error.node_type,
        estimated=max_error.estimated,
        actual=max_error.actual,
        n_qerror_nodes=len(errors),
        execution_time_ms=exec_time_ms
    )

def main():
    # Connect to DuckDB
    dsn = "duckdb:///mnt/d/TPC-DS/tpcds_sf10_1.duckdb"
    print(f"Connecting to: {dsn}", file=sys.stderr)

    try:
        executor = create_executor_from_dsn(dsn)
        print(f"✅ Connected: {type(executor).__name__}", file=sys.stderr)
    except Exception as e:
        print(f"❌ Connection failed: {e}", file=sys.stderr)
        return

    # Load TPC-DS queries
    queries = load_tpcds_queries()
    if not queries:
        print(f"❌ No queries found!", file=sys.stderr)
        return

    print(f"\nLoaded {len(queries)} TPC-DS queries", file=sys.stderr)

    # Analyze all queries
    print(f"\nAnalyzing all {len(queries)} queries...\n", file=sys.stderr)

    results = []
    for i, (query_id, sql) in enumerate(sorted(queries.items()), 1):
        print(f"[{i}/{len(queries)}] {query_id}...", file=sys.stderr, end=" ")
        try:
            result = analyze_query(executor, query_id, sql)
            if result:
                results.append(result)
                print(f"Q={result.max_qerror:.1f} ({result.severity})", file=sys.stderr)
            else:
                print(f"SKIP (no plan)", file=sys.stderr)
        except Exception as e:
            print(f"ERROR: {e}", file=sys.stderr)

    if not results:
        print("\n❌ No results collected!", file=sys.stderr)
        return

    print(f"\n✅ Analyzed {len(results)}/{len(queries)} queries\n", file=sys.stderr)

    # Sort by Q-Error descending
    results_sorted = sorted(results, key=lambda x: x.max_qerror, reverse=True)

    # Write results as CSV
    output_csv = Path("Q-Error/results_tpcds_benchmark.csv")
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'query_id', 'query_num', 'max_qerror', 'severity',
            'worst_node_type', 'estimated', 'actual',
            'n_qerror_nodes', 'execution_time_ms'
        ])
        writer.writeheader()
        for r in results_sorted:
            writer.writerow(asdict(r))

    print(f"📊 Results saved to: {output_csv}", file=sys.stderr)

    # Print markdown table to stdout
    print("\n# Q-Error Analysis: TPC-DS SF10 Benchmark (88 Queries)\n")
    print("| Query | Max Q-Error | Severity | Node Type | Estimated → Actual | Exec Time | Q-Error Nodes |")
    print("|-------|-------------|----------|-----------|--------------------|-----------|---------------|")

    for r in results_sorted:
        # Format Q-Error
        if r.max_qerror > 1000000:
            qerr_str = f"{r.max_qerror/1e6:.1f}M"
        elif r.max_qerror > 1000:
            qerr_str = f"{r.max_qerror/1e3:.1f}K"
        else:
            qerr_str = f"{r.max_qerror:.1f}"

        # Severity emoji
        sev_emoji = {
            "CATASTROPHIC_BLINDNESS": "🚨",
            "MAJOR_HALLUCINATION": "🟠",
            "MODERATE_GUESS": "🟡",
            "MINOR_DRIFT": "🔵",
            "ACCURATE": "✅"
        }.get(r.severity, "")

        # Format estimates
        if r.estimated > 1000000:
            est_str = f"{r.estimated/1e6:.1f}M"
        elif r.estimated > 1000:
            est_str = f"{r.estimated/1e3:.1f}K"
        else:
            est_str = f"{r.estimated}"

        if r.actual > 1000000:
            act_str = f"{r.actual/1e6:.1f}M"
        elif r.actual > 1000:
            act_str = f"{r.actual/1e3:.1f}K"
        else:
            act_str = f"{r.actual}"

        est_act = f"{est_str} → {act_str}" if r.estimated > 0 else "—"

        exec_time_str = f"{r.execution_time_ms:.0f}ms" if r.execution_time_ms > 0 else "—"

        print(f"| {r.query_id:<7} | {qerr_str:>11} | {sev_emoji} {r.severity[:15]:<15} | {r.worst_node_type[:11]:<11} | {est_act:^18} | {exec_time_str:>9} | {r.n_qerror_nodes:>13} |")

    # Summary statistics
    print("\n## Summary Statistics\n")

    catastrophic = len([r for r in results if r.severity == "CATASTROPHIC_BLINDNESS"])
    major = len([r for r in results if r.severity == "MAJOR_HALLUCINATION"])
    moderate = len([r for r in results if r.severity == "MODERATE_GUESS"])
    minor = len([r for r in results if r.severity == "MINOR_DRIFT"])
    accurate = len([r for r in results if r.severity == "ACCURATE"])

    print(f"- **Total Queries Analyzed**: {len(results)}")
    print(f"- 🚨 **Catastrophic Blindness (>1M)**: {catastrophic}/{len(results)} ({100*catastrophic/len(results):.0f}%)")
    print(f"- 🟠 **Major Hallucination (100-1M)**: {major}/{len(results)} ({100*major/len(results):.0f}%)")
    print(f"- 🟡 **Moderate Guess (10-100)**: {moderate}/{len(results)} ({100*moderate/len(results):.0f}%)")
    print(f"- 🔵 **Minor Drift (2-10)**: {minor}/{len(results)} ({100*minor/len(results):.0f}%)")
    print(f"- ✅ **Accurate (<2)**: {accurate}/{len(results)} ({100*accurate/len(results):.0f}%)")

    # High-value targets
    print("\n## High-Value Optimization Targets (Q-Error > 100)\n")
    high_value = [r for r in results_sorted if r.max_qerror > 100]
    print(f"**{len(high_value)} queries** have Q-Error > 100 and are prime optimization candidates:\n")

    for r in high_value[:20]:  # Top 20
        if r.max_qerror > 1000000:
            qerr_str = f"{r.max_qerror/1e6:.1f}M"
        elif r.max_qerror > 1000:
            qerr_str = f"{r.max_qerror/1e3:.1f}K"
        else:
            qerr_str = f"{r.max_qerror:.1f}"

        print(f"- **{r.query_id}**: Q-Error = {qerr_str} ({r.severity})")

    if len(high_value) > 20:
        print(f"\n*...and {len(high_value) - 20} more queries with Q-Error > 100*")

    print(f"\n---\n*Analysis performed on TPC-DS SF10 using DuckDB EXPLAIN ANALYZE*")

if __name__ == "__main__":
    main()
