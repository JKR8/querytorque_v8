#!/usr/bin/env python3
"""
Quick validation summary - analyzes existing validation data from collections
Does NOT run queries, just summarizes what we have
"""

import json
import csv
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
from statistics import mean

def load_validation_json(filepath: str) -> Dict:
    """Load validation JSON file"""
    with open(filepath) as f:
        return json.load(f)

def load_retry_results(directory: str) -> Dict[str, Dict]:
    """Load retry results from directory"""
    results = {}

    # Try to load validation CSV
    val_csv = Path(directory) / "validation_20260206_010443.csv"
    if val_csv.exists():
        with open(val_csv) as f:
            reader = csv.DictReader(f)
            for row in reader:
                query_id = row['query_id']
                results[query_id] = {
                    'original_speedup': float(row.get('original_speedup', 1.0)),
                    'best_speedup': float(row.get('best_speedup', 1.0)),
                    'best_worker': row.get('best_worker', ''),
                    'best_status': row.get('best_status', 'unknown'),
                    'improvement': float(row.get('improvement', 0.0)),
                }

    return results

def analyze_collection(directory: str, name: str) -> None:
    """Analyze a retry collection"""
    results = load_retry_results(directory)

    if not results:
        print(f"\n{name}: No validation data found")
        return

    print(f"\n{'='*70}")
    print(f"{name} - VALIDATION SUMMARY")
    print(f"{'='*70}")

    # Statistics
    total = len(results)
    passed = sum(1 for r in results.values() if r['best_status'] == 'pass')
    failed = sum(1 for r in results.values() if r['best_status'] == 'fail')

    speedups = [r['best_speedup'] for r in results.values()]
    improvements = [r['improvement'] for r in results.values()]

    wins = sum(1 for s in speedups if s >= 1.5)
    regressions = sum(1 for s in speedups if s < 1.0)

    print(f"\nTotal Queries: {total}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Pass Rate: {100*passed/total:.1f}%")

    print(f"\nSpeedup Statistics:")
    print(f"  Average Speedup: {mean(speedups):.2f}x")
    print(f"  Min Speedup: {min(speedups):.2f}x")
    print(f"  Max Speedup: {max(speedups):.2f}x")
    print(f"  Wins (≥1.5x): {wins}")
    print(f"  Regressions (<1.0x): {regressions}")

    print(f"\nImprovement Statistics:")
    if improvements:
        avg_improvement = mean(improvements)
        improved = sum(1 for i in improvements if i > 0)
        print(f"  Queries Improved: {improved}/{total}")
        print(f"  Average Improvement: +{avg_improvement:.2f}x" if avg_improvement > 0 else f"  Average Improvement: {avg_improvement:.2f}x")

    # Worker analysis
    worker_counts = defaultdict(int)
    worker_wins = defaultdict(int)
    for r in results.values():
        worker = r['best_worker']
        if worker:
            worker_counts[worker] += 1
            if r['best_speedup'] >= 1.5:
                worker_wins[worker] += 1

    if worker_counts:
        print(f"\nWorker Performance:")
        for worker in sorted(worker_counts.keys()):
            count = worker_counts[worker]
            wins = worker_wins[worker]
            print(f"  {worker}: {wins}/{count} wins ({100*wins/count:.0f}%)")

    # Top performers
    print(f"\nTop Performers (≥2.0x):")
    top = sorted(results.items(), key=lambda x: x[1]['best_speedup'], reverse=True)
    for query_id, result in top[:15]:
        if result['best_speedup'] >= 2.0:
            status = "✓" if result['best_status'] == 'pass' else "✗"
            print(f"  {query_id:5s}: {result['best_speedup']:6.2f}x ({result['best_worker']}) {status}")

    # Failed queries
    failed_queries = [q for q, r in results.items() if r['best_status'] == 'fail']
    if failed_queries:
        print(f"\nFailed Queries ({len(failed_queries)}):")
        for query_id in sorted(failed_queries):
            speedup = results[query_id]['best_speedup']
            print(f"  {query_id}: {speedup:.2f}x")

    print(f"{'='*70}\n")


def generate_combined_report() -> None:
    """Generate combined validation report"""
    print("\n" + "="*70)
    print("DUCKDB TPC-DS VALIDATION SUMMARY (Feb 6, 2026)")
    print("="*70)

    # Load retry_neutrals
    analyze_collection("retry_neutrals", "NEUTRAL QUERIES (4-Worker Retry)")

    # Load retry_collect
    analyze_collection("retry_collect", "REGRESSION BATCH (3-Worker Retry)")

    # Overall statistics
    retry_neutrals = load_retry_results("retry_neutrals")
    retry_collect = load_retry_results("retry_collect")

    total_all = len(retry_neutrals) + len(retry_collect)
    if total_all > 0:
        all_results = {**retry_neutrals, **retry_collect}
        passed_all = sum(1 for r in all_results.values() if r['best_status'] == 'pass')
        speedups_all = [r['best_speedup'] for r in all_results.values()]
        wins_all = sum(1 for s in speedups_all if s >= 1.5)

        print(f"\nCOMBINED RESULTS:")
        print(f"  Total Queries: {total_all}")
        print(f"  Passed: {passed_all}/{total_all}")
        print(f"  Win Rate (≥1.5x): {100*wins_all/total_all:.1f}%")
        print(f"  Average Speedup: {mean(speedups_all):.2f}x")
        print(f"  Biggest Win: {max(speedups_all):.2f}x")
        print("="*70 + "\n")


def generate_validation_checklist() -> None:
    """Generate checklist for manual validation"""
    print("\n" + "="*70)
    print("VALIDATION CHECKLIST")
    print("="*70)

    dirs = [
        ("retry_neutrals", "43 neutral queries"),
        ("retry_collect", "7 regression queries")
    ]

    print("\nTo run full DuckDB validation:")
    print("\n1. Ensure DuckDB and TPC-DS are available:")
    print("   pip install duckdb")
    print("   python research/validate_duckdb_tpcds.py --collection both")

    print("\n2. Options for validation:")
    print("   --scale 0.1    # Default: 100MB SF0.1 (fast)")
    print("   --scale 1.0    # 10GB SF1 (accurate, slow)")
    print("   --tolerance 0.15  # 15% deviation allowed (default)")

    print("\n3. Collections to validate:")
    for dir_name, desc in dirs:
        csv_file = Path(dir_name) / "retry_4worker_20260206_004710.csv"
        sql_count = len(list(Path(dir_name).glob("q*")))
        if csv_file.exists():
            with open(csv_file) as f:
                csv_lines = sum(1 for _ in f) - 1
            print(f"   {dir_name}: {csv_lines} queries ({desc})")
        else:
            print(f"   {dir_name}: {sql_count} queries ({desc})")

    print("\n4. Validation method (CRITICAL):")
    print("   - 5 runs per query (original + optimized)")
    print("   - Discard min/max from 5 runs")
    print("   - Average remaining 3 runs (trimmed mean)")
    print("   - Calculate speedup = baseline_mean / optimized_mean")
    print("   - Check if speedup within tolerance of expected")

    print("\n5. Expected results:")
    print("   - retry_neutrals: 30/43 improved to WIN")
    print("   - retry_collect: 14/25 improved from baseline")
    print("   - Q88: 5.25x (biggest win)")
    print("   - Average improvement: +0.4-0.5x across collection")

    print("="*70 + "\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--checklist":
        generate_validation_checklist()
    else:
        generate_combined_report()
        generate_validation_checklist()
