#!/usr/bin/env python3
"""
Synthetic EXPLAIN Cost Validation: Does synthetic data predict optimization direction?

For queries with known SF10 speedups, compares EXPLAIN estimated cardinality
between original and optimized SQL on synthetic data. If synthetic EXPLAIN
correctly predicts "optimized is cheaper", the synthetic validator can serve
as a pre-screening filter before expensive SF10 benchmarking.
"""

import sys
import json
import os
import io

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-sql'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-shared'))

import duckdb
import sqlglot
from sqlglot import exp
from validator import SyntheticValidator

QUERY_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries')
OPT_DIR = os.path.join(PROJECT_ROOT,
    'research/ALL_OPTIMIZATIONS/duckdb_tpcds')
SF10_DB = '/mnt/d/TPC-DS/tpcds_sf10_2.duckdb'

SKIP_OPS = {'PROJECTION', 'RESULT_COLLECTOR', 'EXPLAIN_ANALYZE', 'COLUMN_DATA_SCAN'}

# Queries with known SF10 speedups and their optimization directories
QUERIES = {
    1:  {'speedup': 1.62, 'sub': 'swarm_final'},
    3:  {'speedup': 1.04, 'sub': 'swarm_final'},
    7:  {'speedup': 1.15, 'sub': 'swarm_final'},
    12: {'speedup': 1.08, 'sub': 'swarm_final'},
    15: {'speedup': 3.17, 'sub': 'v1_standard'},
    19: {'speedup': 1.08, 'sub': 'swarm_final'},
    42: {'speedup': 1.03, 'sub': 'swarm_final'},
}


def get_explain_plan(conn, sql):
    """Run EXPLAIN (FORMAT JSON) and return parsed plan."""
    try:
        result = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()
        for plan_type, plan_json_str in result:
            parsed = json.loads(plan_json_str)
            if plan_type == 'physical_plan':
                if isinstance(parsed, list):
                    return {'children': parsed}
                elif isinstance(parsed, dict):
                    return {'children': [parsed]}
        for plan_type, plan_json_str in result:
            parsed = json.loads(plan_json_str)
            if isinstance(parsed, list):
                return {'children': parsed}
            if isinstance(parsed, dict):
                return {'children': [parsed]} if 'children' not in parsed else parsed
    except Exception as e:
        return {'error': str(e)}
    return {'error': 'no plan'}


def collect_plan_stats(node, depth=0):
    """Recursively collect operator stats from plan JSON."""
    stats = {
        'total_estimated_rows': 0,
        'operators': [],
        'scans': [],
        'joins': [],
    }
    if not node:
        return stats

    op_name = node.get('name', node.get('operator_name', node.get('operator_type', '')))
    if isinstance(op_name, str):
        op_name = op_name.strip()

    extra = node.get('extra_info', {})
    if isinstance(extra, str):
        extra = {}

    # Get estimated cardinality
    est_card = 0
    if isinstance(extra, dict):
        est_str = extra.get('Estimated Cardinality', '0')
        try:
            est_card = int(str(est_str).lstrip('~'))
        except (ValueError, TypeError):
            est_card = 0

    if op_name and op_name not in SKIP_OPS:
        stats['operators'].append({
            'op': op_name,
            'depth': depth,
            'est_rows': est_card,
            'table': extra.get('Table', '') if isinstance(extra, dict) else '',
        })
        stats['total_estimated_rows'] += est_card

        if 'SCAN' in op_name and extra.get('Table'):
            stats['scans'].append({
                'table': extra['Table'],
                'est_rows': est_card,
                'has_filter': bool(extra.get('Filters')),
            })

        if 'JOIN' in op_name:
            stats['joins'].append({
                'type': op_name,
                'est_rows': est_card,
            })

    for child in node.get('children', []):
        child_stats = collect_plan_stats(child, depth + 1)
        stats['total_estimated_rows'] += child_stats['total_estimated_rows']
        stats['operators'].extend(child_stats['operators'])
        stats['scans'].extend(child_stats['scans'])
        stats['joins'].extend(child_stats['joins'])

    return stats


def run_timed(conn, sql, runs=5):
    """Run query multiple times and return trimmed mean (ms).

    5x runs, drop min/max, average remaining 3.
    """
    import time
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        try:
            conn.execute(sql).fetchall()
        except Exception:
            return None
        elapsed = (time.perf_counter() - start) * 1000
        times.append(elapsed)
    times.sort()
    # Drop min and max, average middle 3
    trimmed = times[1:-1]
    return sum(trimmed) / len(trimmed) if trimmed else None


def extract_tables_from_sql(sql):
    """Extract table names from SQL using sqlglot."""
    tables = set()
    try:
        parsed = sqlglot.parse(sql, read='duckdb')
        for stmt in parsed:
            if stmt is None:
                continue
            for table in stmt.find_all(exp.Table):
                name = table.name.lower()
                if name:
                    tables.add(name)
    except Exception:
        pass
    return tables


def create_tablesample_conn(original_sql, optimized_sql):
    """Create 2% TABLESAMPLE of SF10 for both queries."""
    # Get all tables referenced by either query
    tables = extract_tables_from_sql(original_sql) | extract_tables_from_sql(optimized_sql)

    conn = duckdb.connect(':memory:')
    try:
        conn.execute(f"ATTACH '{SF10_DB}' AS sf10 (READ_ONLY)")
        for table in tables:
            try:
                conn.execute(
                    f"CREATE TABLE {table} AS "
                    f"SELECT * FROM sf10.main.{table} USING SAMPLE 2 PERCENT"
                )
            except Exception:
                # CTE names get extracted as tables — skip
                continue
        return conn
    except Exception:
        conn.close()
        return None


def main():
    print("=" * 85)
    print("SYNTHETIC COST VALIDATION: Does synthetic EXPLAIN predict optimization direction?")
    print("=" * 85)
    print()

    results = []

    for q, info in sorted(QUERIES.items()):
        query_file = os.path.join(QUERY_DIR, f'query_{q}.sql')
        opt_file = os.path.join(OPT_DIR, f'q{q}', info['sub'], 'optimized.sql')

        if not os.path.exists(query_file) or not os.path.exists(opt_file):
            print(f"Q{q:2d}  SKIP (files missing)")
            continue

        with open(query_file) as f:
            original_sql = f.read().strip()
        with open(opt_file) as f:
            optimized_sql = f.read().strip()

        print(f"Q{q:2d}...", end=" ", flush=True)

        # Create synthetic data using original query
        validator = SyntheticValidator()
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            result = validator.validate(query_file, target_rows=100)
        finally:
            sys.stdout = old_stdout

        if not result['success']:
            print(f"SYNTH_FAIL: {result.get('error', 'unknown')[:60]}")
            continue

        conn = validator.conn

        # Get EXPLAIN for original on synthetic data
        orig_plan = get_explain_plan(conn, original_sql)
        if 'error' in orig_plan:
            print(f"ORIG_EXPLAIN_ERR: {orig_plan['error'][:60]}")
            continue

        # Get EXPLAIN for optimized on synthetic data
        opt_plan = get_explain_plan(conn, optimized_sql)
        if 'error' in opt_plan:
            print(f"OPT_EXPLAIN_ERR: {opt_plan['error'][:60]}")
            continue

        # Collect plan stats
        orig_stats = collect_plan_stats(orig_plan)
        opt_stats = collect_plan_stats(opt_plan)

        # Time on synthetic data (5x trimmed mean)
        orig_time = run_timed(conn, original_sql)
        opt_time = run_timed(conn, optimized_sql)
        synth_speedup = orig_time / opt_time if opt_time and opt_time > 0 else None

        # === 2% TABLESAMPLE comparison ===
        sample_conn = create_tablesample_conn(original_sql, optimized_sql)
        sample_speedup = None
        sample_orig_time = None
        sample_opt_time = None
        if sample_conn:
            try:
                sample_orig_time = run_timed(sample_conn, original_sql)
                sample_opt_time = run_timed(sample_conn, optimized_sql)
                if sample_orig_time and sample_opt_time and sample_opt_time > 0:
                    sample_speedup = sample_orig_time / sample_opt_time
            except Exception:
                pass
            finally:
                sample_conn.close()

        # Direction prediction
        sf10_speedup = info['speedup']

        synth_match = synth_speedup is not None and synth_speedup > 1.0
        sample_match = sample_speedup is not None and sample_speedup > 1.0

        r = {
            'query': q,
            'sf10_speedup': sf10_speedup,
            'synth_speedup': synth_speedup,
            'synth_orig_ms': orig_time,
            'synth_opt_ms': opt_time,
            'sample_speedup': sample_speedup,
            'sample_orig_ms': sample_orig_time,
            'sample_opt_ms': sample_opt_time,
            'synth_match': synth_match,
            'sample_match': sample_match,
            'orig_est_rows': orig_stats['total_estimated_rows'],
            'opt_est_rows': opt_stats['total_estimated_rows'],
            'orig_ops': len(orig_stats['operators']),
            'opt_ops': len(opt_stats['operators']),
        }
        results.append(r)

        # Print summary line
        s_str = f"{synth_speedup:.2f}x" if synth_speedup else "N/A"
        t_str = f"{sample_speedup:.2f}x" if sample_speedup else "N/A"
        s_sym = "OK" if synth_match else "MISS"
        t_sym = "OK" if sample_match else "MISS"
        print(f"SF10={sf10_speedup:.2f}x  "
              f"synth={s_str}[{s_sym}]  "
              f"2%sam={t_str}[{t_sym}]")

    if not results:
        print("\nNo results.")
        return

    # === SUMMARY TABLE ===
    print()
    print("=" * 90)
    print("DETAILED RESULTS")
    print("=" * 90)
    print(f"{'Q':>3} {'SF10':>7} {'Synth':>8} {'S_Orig':>8} {'S_Opt':>8} "
          f"{'2%Sam':>8} {'T_Orig':>8} {'T_Opt':>8} {'S_Dir':>6} {'T_Dir':>6}")
    print("-" * 90)

    for r in results:
        q = r['query']
        sf10 = f"{r['sf10_speedup']:.2f}x"
        synth = f"{r['synth_speedup']:.2f}x" if r['synth_speedup'] else "N/A"
        s_orig = f"{r['synth_orig_ms']:.1f}" if r['synth_orig_ms'] else "N/A"
        s_opt = f"{r['synth_opt_ms']:.1f}" if r['synth_opt_ms'] else "N/A"
        samp = f"{r['sample_speedup']:.2f}x" if r['sample_speedup'] else "N/A"
        t_orig = f"{r['sample_orig_ms']:.1f}" if r['sample_orig_ms'] else "N/A"
        t_opt = f"{r['sample_opt_ms']:.1f}" if r['sample_opt_ms'] else "N/A"
        s_dir = "OK" if r['synth_match'] else "MISS"
        t_dir = "OK" if r['sample_match'] else "MISS"

        print(f"{q:>3} {sf10:>7} {synth:>8} {s_orig:>8} {s_opt:>8} "
              f"{samp:>8} {t_orig:>8} {t_opt:>8} {s_dir:>6} {t_dir:>6}")

    # === ACCURACY METRICS ===
    print()
    print("=" * 90)
    print("ACCURACY METRICS")
    print("=" * 90)

    n = len(results)
    synth_ok = sum(1 for r in results if r['synth_match'])
    sample_ok = sum(1 for r in results if r['sample_match'])

    print(f"  Direction correct (synthetic):    {synth_ok}/{n} ({synth_ok/n*100:.0f}%)")
    print(f"  Direction correct (2% sample):    {sample_ok}/{n} ({sample_ok/n*100:.0f}%)")

    # Pearson correlation
    def pearson(xs, ys):
        n_c = len(xs)
        if n_c < 3:
            return None
        mx = sum(xs) / n_c
        my = sum(ys) / n_c
        cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / n_c
        sx = (sum((x - mx)**2 for x in xs) / n_c) ** 0.5
        sy = (sum((y - my)**2 for y in ys) / n_c) ** 0.5
        return cov / (sx * sy) if sx * sy > 0 else 0

    synth_pairs = [(r['sf10_speedup'], r['synth_speedup']) for r in results
                   if r['synth_speedup'] is not None]
    sample_pairs = [(r['sf10_speedup'], r['sample_speedup']) for r in results
                    if r['sample_speedup'] is not None]

    r_synth = pearson([p[0] for p in synth_pairs], [p[1] for p in synth_pairs])
    r_sample = pearson([p[0] for p in sample_pairs], [p[1] for p in sample_pairs])

    if r_synth is not None:
        print(f"  Pearson r (synth vs SF10):         {r_synth:+.3f}")
    if r_sample is not None:
        print(f"  Pearson r (2% sample vs SF10):     {r_sample:+.3f}")

    winner = "Synthetic" if synth_ok > sample_ok else ("2% Sample" if sample_ok > synth_ok else "TIE")
    print(f"\n  Cost prediction winner: {winner}")


if __name__ == '__main__':
    main()
