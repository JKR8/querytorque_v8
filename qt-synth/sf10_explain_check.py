#!/usr/bin/env python3
"""
SF10 EXPLAIN Cost Prediction Check

Hypothesis: DuckDB's estimated cardinality in EXPLAIN does NOT predict
optimization wins even at full SF10 scale. If true, the 0/7 prediction
failure on synthetic data is a cost-model problem, not a data-size problem.

For each known-winning query:
  - Runs EXPLAIN (FORMAT JSON) on original and optimized SQL against SF10
  - Extracts total estimated cardinality (sum across all plan operators)
  - Compares: does the optimizer think the optimized version is cheaper?
  - NO query execution — pure plan analysis
"""

import json
import os
import sys

import duckdb

SF10_DB = '/mnt/d/TPC-DS/tpcds_sf10_2.duckdb'

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
QUERY_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries')
OPT_DIR = os.path.join(PROJECT_ROOT,
    'research/ALL_OPTIMIZATIONS/duckdb_tpcds')

QUERIES = {
    1:  {'speedup': 1.62, 'sub': 'swarm_final'},
    3:  {'speedup': 1.04, 'sub': 'swarm_final'},
    7:  {'speedup': 1.15, 'sub': 'swarm_final'},
    12: {'speedup': 1.08, 'sub': 'swarm_final'},
    15: {'speedup': 3.17, 'sub': 'v1_standard'},
    19: {'speedup': 1.08, 'sub': 'swarm_final'},
    42: {'speedup': 1.03, 'sub': 'swarm_final'},
}

# Operators that don't contribute meaningful cardinality estimates
SKIP_OPS = {'PROJECTION', 'RESULT_COLLECTOR', 'EXPLAIN_ANALYZE', 'COLUMN_DATA_SCAN'}


def get_explain_plan(conn, sql):
    """Run EXPLAIN (FORMAT JSON) and return the physical plan as a dict."""
    try:
        result = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()
        for plan_type, plan_json_str in result:
            if plan_type == 'physical_plan':
                parsed = json.loads(plan_json_str)
                # physical_plan returns a JSON list, wrap it
                if isinstance(parsed, list):
                    return {'children': parsed}
                elif isinstance(parsed, dict):
                    return parsed
        # Fallback: use first result
        for plan_type, plan_json_str in result:
            parsed = json.loads(plan_json_str)
            if isinstance(parsed, list):
                return {'children': parsed}
            return parsed
    except Exception as e:
        return {'error': str(e)}
    return {'error': 'no plan returned'}


def collect_est_card(node):
    """Sum estimated cardinality across all plan operators."""
    total = 0
    if not node:
        return total
    extra = node.get('extra_info', {})
    if isinstance(extra, dict):
        est = extra.get('Estimated Cardinality', '0')
        try:
            total += int(str(est).lstrip('~'))
        except (ValueError, TypeError):
            pass
    for child in node.get('children', []):
        total += collect_est_card(child)
    return total


def collect_scan_card(node):
    """Sum estimated cardinality for SCAN operators only."""
    total = 0
    if not node:
        return total
    op_name = node.get('name', '').strip()
    extra = node.get('extra_info', {})
    if isinstance(extra, dict) and 'SCAN' in op_name:
        est = extra.get('Estimated Cardinality', '0')
        try:
            total += int(str(est).lstrip('~'))
        except (ValueError, TypeError):
            pass
    for child in node.get('children', []):
        total += collect_scan_card(child)
    return total


def collect_operators(node, depth=0):
    """Collect all operators with their estimated cardinality."""
    ops = []
    if not node:
        return ops
    op_name = node.get('name', '').strip()
    extra = node.get('extra_info', {})
    est_card = 0
    table_info = ''
    if isinstance(extra, dict):
        est = extra.get('Estimated Cardinality', '0')
        try:
            est_card = int(str(est).lstrip('~'))
        except (ValueError, TypeError):
            pass
        table_info = extra.get('Table', '')
    if op_name:
        label = f"{op_name}"
        if table_info:
            label += f"({table_info})"
        ops.append((depth, label, est_card))
    for child in node.get('children', []):
        ops.extend(collect_operators(child, depth + 1))
    return ops


def fmt(n):
    """Format large numbers with K/M suffix."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def main():
    print("=" * 100)
    print("SF10 EXPLAIN COST PREDICTION CHECK")
    print("  Hypothesis: DuckDB cost model fails to predict optimization wins")
    print("  Method: EXPLAIN (FORMAT JSON) on SF10 data (2.9 GB, full scale)")
    print("  Metric: Total estimated cardinality (sum across all plan operators)")
    print("=" * 100)
    print()

    conn = duckdb.connect(SF10_DB, read_only=True)
    print(f"Connected to SF10 database: {SF10_DB}")
    # Quick sanity check
    row_count = conn.execute(
        "SELECT COUNT(*) FROM store_sales").fetchone()[0]
    print(f"  store_sales rows: {row_count:,}")
    print()

    results = []
    correct_predictions = 0
    total_queries = 0

    for q, info in sorted(QUERIES.items()):
        query_file = os.path.join(QUERY_DIR, f'query_{q}.sql')
        opt_file = os.path.join(OPT_DIR, f'q{q}', info['sub'], 'optimized.sql')

        if not os.path.exists(query_file):
            print(f"  Q{q:2d}: SKIP (original missing)")
            continue
        if not os.path.exists(opt_file):
            print(f"  Q{q:2d}: SKIP (optimized missing)")
            continue

        with open(query_file) as f:
            original_sql = f.read().strip()
        with open(opt_file) as f:
            optimized_sql = f.read().strip()

        print(f"  Q{q:2d}: Running EXPLAIN...", end=" ", flush=True)

        orig_plan = get_explain_plan(conn, original_sql)
        if 'error' in orig_plan:
            print(f"ERROR (original): {orig_plan['error'][:80]}")
            continue

        opt_plan = get_explain_plan(conn, optimized_sql)
        if 'error' in opt_plan:
            print(f"ERROR (optimized): {opt_plan['error'][:80]}")
            continue

        orig_total = collect_est_card(orig_plan)
        opt_total = collect_est_card(opt_plan)
        orig_scan = collect_scan_card(orig_plan)
        opt_scan = collect_scan_card(opt_plan)
        orig_ops = collect_operators(orig_plan)
        opt_ops = collect_operators(opt_plan)

        # Ratio: orig/opt  (>1 means optimizer thinks opt is cheaper = correct)
        total_ratio = orig_total / opt_total if opt_total > 0 else float('inf')
        scan_ratio = orig_scan / opt_scan if opt_scan > 0 else float('inf')

        # Does the cost model predict opt is cheaper?
        predicted_cheaper = total_ratio > 1.0
        actual_faster = info['speedup'] > 1.0
        prediction_correct = predicted_cheaper == actual_faster

        total_queries += 1
        if prediction_correct:
            correct_predictions += 1

        verdict = "CORRECT" if prediction_correct else "WRONG"
        direction = "lower" if predicted_cheaper else "HIGHER"

        r = {
            'query': q,
            'speedup': info['speedup'],
            'orig_total': orig_total,
            'opt_total': opt_total,
            'total_ratio': total_ratio,
            'orig_scan': orig_scan,
            'opt_scan': opt_scan,
            'scan_ratio': scan_ratio,
            'predicted_cheaper': predicted_cheaper,
            'prediction_correct': prediction_correct,
            'orig_ops': orig_ops,
            'opt_ops': opt_ops,
        }
        results.append(r)

        print(f"actual={info['speedup']:.2f}x  "
              f"est_ratio={total_ratio:.3f}x  "
              f"opt est is {direction}  "
              f"[{verdict}]")

    conn.close()

    if not results:
        print("\nNo results to report.")
        return

    # ========== SUMMARY TABLE ==========
    print()
    print("=" * 100)
    print("SUMMARY: SF10 EXPLAIN COST vs ACTUAL SPEEDUP")
    print("=" * 100)
    print(f"{'Q':>3}  {'Actual':>8}  {'Orig Est':>14}  {'Opt Est':>14}  "
          f"{'Est Ratio':>10}  {'Scan Ratio':>10}  {'Prediction':>12}")
    print("-" * 100)

    for r in results:
        q = r['query']
        actual = f"{r['speedup']:.2f}x"
        orig_e = fmt(r['orig_total'])
        opt_e = fmt(r['opt_total'])
        t_ratio = f"{r['total_ratio']:.3f}x"
        s_ratio = f"{r['scan_ratio']:.3f}x"
        verdict = "CORRECT" if r['prediction_correct'] else "WRONG"

        print(f"{q:>3}  {actual:>8}  {orig_e:>14}  {opt_e:>14}  "
              f"{t_ratio:>10}  {s_ratio:>10}  {verdict:>12}")

    print("-" * 100)
    print(f"Direction accuracy: {correct_predictions}/{total_queries} "
          f"({correct_predictions/total_queries*100:.0f}%)")

    # Interpretation
    print()
    print("=" * 100)
    print("INTERPRETATION")
    print("=" * 100)
    if correct_predictions <= total_queries // 2:
        print("  CONFIRMED: DuckDB cost model FAILS to predict optimization wins at SF10.")
        print("  The 0/7 prediction failure on synthetic data is NOT a data-size problem.")
        print("  It is a fundamental cost-model limitation:")
        print("    - Estimated cardinality does not reflect actual execution cost")
        print("    - Rewrites that reduce wall-clock time often show HIGHER estimated cost")
        print("    - This means EXPLAIN-based pre-screening would reject winning rewrites")
    else:
        print("  SURPRISE: DuckDB cost model DOES predict optimization wins at SF10.")
        print("  The synthetic-data failure WAS a data-size problem.")
        print("  EXPLAIN-based pre-screening may be viable on full-scale data.")

    # ========== OPERATOR DETAIL ==========
    print()
    print("=" * 100)
    print("OPERATOR-LEVEL DETAIL (per query)")
    print("=" * 100)

    for r in results:
        q = r['query']
        print(f"\n--- Q{q} (actual {r['speedup']:.2f}x, est ratio {r['total_ratio']:.3f}x) ---")

        print(f"  ORIGINAL (total est: {r['orig_total']:,}, scan est: {r['orig_scan']:,}):")
        for depth, op, est in r['orig_ops']:
            indent = "    " + "  " * depth
            print(f"{indent}{op:<40} est={est:>12,}")

        print(f"  OPTIMIZED (total est: {r['opt_total']:,}, scan est: {r['opt_scan']:,}):")
        for depth, op, est in r['opt_ops']:
            indent = "    " + "  " * depth
            print(f"{indent}{op:<40} est={est:>12,}")

    # ========== CORRELATION ANALYSIS ==========
    print()
    print("=" * 100)
    print("CORRELATION: Est Ratio vs Actual Speedup")
    print("=" * 100)
    print(f"{'Q':>3}  {'Actual':>8}  {'Est Ratio':>10}  {'Difference':>10}  {'Direction Match':>15}")
    print("-" * 60)
    for r in results:
        diff = r['total_ratio'] - r['speedup']
        match = "YES" if r['prediction_correct'] else "NO"
        print(f"{r['query']:>3}  {r['speedup']:>7.2f}x  {r['total_ratio']:>9.3f}x  "
              f"{diff:>+9.3f}   {match:>15}")

    # Spearman-like rank correlation (manual, no scipy dependency)
    n = len(results)
    if n >= 3:
        actual_vals = [r['speedup'] for r in results]
        est_vals = [r['total_ratio'] for r in results]

        def rank(vals):
            sorted_idx = sorted(range(len(vals)), key=lambda i: vals[i])
            ranks = [0] * len(vals)
            for rank_pos, idx in enumerate(sorted_idx):
                ranks[idx] = rank_pos + 1
            return ranks

        actual_ranks = rank(actual_vals)
        est_ranks = rank(est_vals)

        # Spearman rho
        d_sq = sum((a - e) ** 2 for a, e in zip(actual_ranks, est_ranks))
        rho = 1 - (6 * d_sq) / (n * (n**2 - 1))

        print(f"\nSpearman rank correlation (rho): {rho:.3f}")
        if abs(rho) < 0.3:
            print("  Interpretation: NEGLIGIBLE correlation — cost model ranking is near-random")
        elif abs(rho) < 0.5:
            print("  Interpretation: WEAK correlation — cost model has slight signal")
        elif abs(rho) < 0.7:
            print("  Interpretation: MODERATE correlation — cost model partially useful")
        else:
            print("  Interpretation: STRONG correlation — cost model is informative")


if __name__ == '__main__':
    main()
