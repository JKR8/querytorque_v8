#!/usr/bin/env python3
"""Test Gemini-optimized queries on sample DB using qt_sql pipeline."""

import json
import os
import sys
from pathlib import Path

# Add package paths
sys.path.insert(0, str(Path(__file__).parent.parent / 'packages' / 'qt-sql'))
sys.path.insert(0, str(Path(__file__).parent.parent / 'packages' / 'qt-shared'))

from qt_sql.optimization import apply_operations, test_optimization, parse_response

SAMPLE_DB = "/mnt/d/TPC-DS/tpcds_sf100_sampled_1pct.duckdb"
QUERIES_DIR = Path("/mnt/d/TPC-DS/queries_duckdb_converted")
RESPONSES_DIR = Path(__file__).parent / "gemini_responses"


def load_query(qnum: int) -> str:
    """Load original query."""
    qfile = QUERIES_DIR / f"query_{qnum}.sql"
    return qfile.read_text()


def main():
    results = []

    for qnum in range(1, 10):  # Q1-Q9
        response_file = RESPONSES_DIR / f"q{qnum}_response.txt"

        if not response_file.exists():
            print(f"Q{qnum}: No response file")
            continue

        content = response_file.read_text().strip()
        if not content:
            print(f"Q{qnum}: Empty response")
            continue

        # Parse operations
        parsed = parse_response(content)
        operations = parsed.get('operations', [])
        explanation = parsed.get('explanation', '')

        if not operations:
            print(f"Q{qnum}: No operations - {parsed.get('error', 'unknown')}")
            continue

        # Load original
        original_sql = load_query(qnum)

        # Apply operations
        try:
            optimized_sql = apply_operations(original_sql, operations)
        except Exception as e:
            print(f"Q{qnum}: Apply error - {e}")
            continue

        # Test
        try:
            result = test_optimization(original_sql, optimized_sql, SAMPLE_DB)
        except Exception as e:
            print(f"Q{qnum}: Test error - {e}")
            continue

        status = "✓" if result.semantically_correct else "✗"
        print(f"Q{qnum}: {result.speedup:.2f}x {status} (orig={result.original_time:.3f}s, opt={result.optimized_time:.3f}s)")

        if result.error:
            print(f"      Error: {result.error}")

        results.append({
            'query': qnum,
            'speedup': result.speedup,
            'correct': result.semantically_correct,
            'orig_time': result.original_time,
            'opt_time': result.optimized_time,
            'error': result.error,
            'explanation': explanation
        })

    # Summary
    print("\n=== SUMMARY ===")
    wins = [r for r in results if r['correct'] and r['speedup'] >= 1.2]
    print(f"Wins (≥1.2x correct): {len(wins)}/{len(results)}")
    for r in sorted(wins, key=lambda x: -x['speedup']):
        print(f"  Q{r['query']}: {r['speedup']:.2f}x - {r['explanation'][:60]}...")

    incorrect = [r for r in results if not r['correct']]
    if incorrect:
        print(f"\nIncorrect: {len(incorrect)}")
        for r in incorrect:
            print(f"  Q{r['query']}: {r['speedup']:.2f}x - {r.get('error', 'semantics changed')}")

    # Save results
    with open(RESPONSES_DIR / 'results.json', 'w') as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {RESPONSES_DIR / 'results.json'}")


if __name__ == '__main__':
    main()
