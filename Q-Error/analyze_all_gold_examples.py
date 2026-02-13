#!/usr/bin/env python3
"""
Analyze Q-Error for ALL DuckDB gold examples
Outputs clean table with correlation analysis
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
class GoldExample:
    """Gold example with original and optimized SQL"""
    id: str
    name: str
    queries: List[str]
    speedup: float
    original_sql: str
    optimized_sql: str

@dataclass
class QErrorResult:
    """Q-Error analysis result for a gold example"""
    example_id: str
    name: str
    queries: str
    speedup: float
    orig_max_qerror: float
    orig_severity: str
    orig_node_type: str
    orig_estimated: int
    orig_actual: int
    opt_max_qerror: float
    opt_severity: str
    qerror_improvement: float
    n_orig_errors: int
    n_opt_errors: int

def load_gold_examples() -> List[GoldExample]:
    """Load DuckDB gold examples"""
    gold_dir = Path("packages/qt-sql/qt_sql/examples/duckdb")
    examples = []

    for json_file in sorted(gold_dir.glob("*.json")):
        try:
            with open(json_file) as f:
                data = json.load(f)

            speedup = data.get("sf10_speedup", 0)
            if speedup <= 1.0:
                continue

            examples.append(GoldExample(
                id=data["id"],
                name=data["name"],
                queries=data.get("benchmark_queries", []),
                speedup=speedup,
                original_sql=data["original_sql"],
                optimized_sql=data["optimized_sql"]
            ))

        except Exception as e:
            print(f"Error loading {json_file}: {e}", file=sys.stderr)

    return examples

def get_explain_analyze(executor, sql: str) -> Optional[Dict]:
    """Run EXPLAIN ANALYZE and return plan"""
    try:
        result = executor.explain(sql, analyze=True)
        return result if result else None
    except Exception as e:
        print(f"  EXPLAIN error: {e}", file=sys.stderr)
        return None

def analyze_query_pair(executor, example: GoldExample, verbose: bool = False) -> Optional[QErrorResult]:
    """Analyze Q-Error for original vs optimized query"""
    if verbose:
        print(f"\nAnalyzing: {example.name}", file=sys.stderr)

    # Get EXPLAIN for original
    orig_plan = get_explain_analyze(executor, example.original_sql)
    if not orig_plan:
        return None

    # Get EXPLAIN for optimized
    opt_plan = get_explain_analyze(executor, example.optimized_sql)
    if not opt_plan:
        return None

    # Extract Q-Error from both
    orig_plan_tree = {"children": orig_plan.get("children", [])}
    opt_plan_tree = {"children": opt_plan.get("children", [])}

    orig_errors = extract_q_errors_from_node(orig_plan_tree)
    opt_errors = extract_q_errors_from_node(opt_plan_tree)

    if not orig_errors:
        if verbose:
            print(f"  ⚠️  No Q-Error nodes found", file=sys.stderr)
        return None

    # Find max Q-Error
    orig_max = max(orig_errors, key=lambda x: x.q_error)
    opt_max = max(opt_errors, key=lambda x: x.q_error) if opt_errors else None

    qerror_improvement = orig_max.q_error / opt_max.q_error if opt_max else 0

    return QErrorResult(
        example_id=example.id,
        name=example.name,
        queries=", ".join(example.queries),
        speedup=example.speedup,
        orig_max_qerror=orig_max.q_error,
        orig_severity=orig_max.severity(),
        orig_node_type=orig_max.node_type,
        orig_estimated=orig_max.estimated,
        orig_actual=orig_max.actual,
        opt_max_qerror=opt_max.q_error if opt_max else 0,
        opt_severity=opt_max.severity() if opt_max else "N/A",
        qerror_improvement=qerror_improvement,
        n_orig_errors=len(orig_errors),
        n_opt_errors=len(opt_errors)
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

    # Load gold examples
    examples = load_gold_examples()
    print(f"\nLoaded {len(examples)} gold examples with verified speedups", file=sys.stderr)

    # Sort by speedup descending
    examples_sorted = sorted(examples, key=lambda x: x.speedup, reverse=True)

    # Analyze all examples
    print(f"\nAnalyzing all {len(examples_sorted)} examples...\n", file=sys.stderr)

    results = []
    for i, example in enumerate(examples_sorted, 1):
        print(f"[{i}/{len(examples_sorted)}] {example.id}...", file=sys.stderr)
        try:
            result = analyze_query_pair(executor, example, verbose=False)
            if result:
                results.append(result)
        except Exception as e:
            print(f"  Error: {e}", file=sys.stderr)

    if not results:
        print("\n❌ No results collected!", file=sys.stderr)
        return

    print(f"\n✅ Analyzed {len(results)}/{len(examples_sorted)} examples\n", file=sys.stderr)

    # Write results as CSV
    output_csv = Path("Q-Error/results_all_gold_examples.csv")
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'example_id', 'name', 'queries', 'speedup',
            'orig_max_qerror', 'orig_severity', 'orig_node_type',
            'orig_estimated', 'orig_actual',
            'opt_max_qerror', 'opt_severity', 'qerror_improvement',
            'n_orig_errors', 'n_opt_errors'
        ])
        writer.writeheader()
        for r in results:
            writer.writerow(asdict(r))

    print(f"📊 Results saved to: {output_csv}", file=sys.stderr)

    # Print markdown table to stdout
    print("\n# Q-Error Analysis: All 16 DuckDB Gold Examples\n")
    print("| Example | Queries | Speedup | Max Q-Error | Severity | Node Type | Est → Act | Q-Err Improve |")
    print("|---------|---------|---------|-------------|----------|-----------|-----------|---------------|")

    for r in results:
        # Format Q-Error with appropriate precision
        if r.orig_max_qerror > 1000000:
            qerr_str = f"{r.orig_max_qerror/1e6:.1f}M"
        elif r.orig_max_qerror > 1000:
            qerr_str = f"{r.orig_max_qerror/1e3:.1f}K"
        else:
            qerr_str = f"{r.orig_max_qerror:.1f}"

        # Severity emoji
        sev_emoji = {
            "CATASTROPHIC_BLINDNESS": "🚨",
            "MAJOR_HALLUCINATION": "🟠",
            "MODERATE_GUESS": "🟡",
            "MINOR_DRIFT": "🔵",
            "ACCURATE": "✅"
        }.get(r.orig_severity, "")

        # Format estimates
        if r.orig_estimated > 1000000:
            est_str = f"{r.orig_estimated/1e6:.1f}M"
        elif r.orig_estimated > 1000:
            est_str = f"{r.orig_estimated/1e3:.1f}K"
        else:
            est_str = f"{r.orig_estimated}"

        if r.orig_actual > 1000000:
            act_str = f"{r.orig_actual/1e6:.1f}M"
        elif r.orig_actual > 1000:
            act_str = f"{r.orig_actual/1e3:.1f}K"
        else:
            act_str = f"{r.orig_actual}"

        est_act = f"{est_str} → {act_str}"

        qerr_improve = f"{r.qerror_improvement:.1f}x" if r.qerror_improvement > 0 else "—"

        print(f"| {r.example_id[:20]:<20} | {r.queries[:8]:<8} | {r.speedup:>6.2f}x | {qerr_str:>11} | {sev_emoji} {r.orig_severity[:15]:<15} | {r.orig_node_type[:11]:<11} | {est_act:<13} | {qerr_improve:>13} |")

    # Summary statistics
    print("\n## Summary Statistics\n")

    high_qerror = len([r for r in results if r.orig_max_qerror > 100])
    high_speedup = len([r for r in results if r.speedup > 1.5])
    overlap = len([r for r in results if r.orig_max_qerror > 100 and r.speedup > 1.5])

    print(f"- **Total Examples Analyzed**: {len(results)}")
    print(f"- **High Q-Error (>100)**: {high_qerror}/{len(results)} ({100*high_qerror/len(results):.0f}%)")
    print(f"- **High Speedup (>1.5x)**: {high_speedup}/{len(results)} ({100*high_speedup/len(results):.0f}%)")
    print(f"- **Overlap (both conditions)**: {overlap}/{len(results)} ({100*overlap/len(results):.0f}%)")

    if len(results) > 0:
        overlap_pct = (overlap / len(results)) * 100
        print(f"\n### Correlation Strength: ", end="")
        if overlap_pct > 60:
            print(f"✅ **STRONG** ({overlap_pct:.0f}%)")
        elif overlap_pct > 40:
            print(f"⚠️  **MODERATE** ({overlap_pct:.0f}%)")
        else:
            print(f"❌ **WEAK** ({overlap_pct:.0f}%)")

    # Severity breakdown
    print("\n## Q-Error Severity Distribution\n")
    severity_counts = {}
    for r in results:
        severity_counts[r.orig_severity] = severity_counts.get(r.orig_severity, 0) + 1

    for sev in ["CATASTROPHIC_BLINDNESS", "MAJOR_HALLUCINATION", "MODERATE_GUESS", "MINOR_DRIFT", "ACCURATE"]:
        count = severity_counts.get(sev, 0)
        if count > 0:
            pct = 100 * count / len(results)
            emoji = {
                "CATASTROPHIC_BLINDNESS": "🚨",
                "MAJOR_HALLUCINATION": "🟠",
                "MODERATE_GUESS": "🟡",
                "MINOR_DRIFT": "🔵",
                "ACCURATE": "✅"
            }.get(sev, "")
            print(f"- {emoji} **{sev}**: {count}/{len(results)} ({pct:.0f}%)")

    print(f"\n---\n*Generated from {len(results)} gold examples with verified speedups*")

if __name__ == "__main__":
    main()
