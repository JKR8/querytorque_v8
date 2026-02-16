#!/usr/bin/env python3
"""
EXPLAIN-Based Cost Prediction: Do synthetic EXPLAIN plans predict optimization wins?

Compares EXPLAIN estimated cardinality between original and optimized SQL
on synthetic data vs 2% TABLESAMPLE. Purely plan-based — no query execution timing.

Metrics compared:
1. Total estimated rows processed (sum across all operators)
2. Max scan cardinality (largest table scan estimate)
3. Root output cardinality
4. Total scan rows (sum of all SCAN estimates)
"""

import sys
import json
import os
import io

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..', '..')
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-sql'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-shared'))

import duckdb
import sqlglot
from sqlglot import exp
from validator import SyntheticValidator

QUERY_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries')
EXPLAIN_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/explains')
OPT_DIR = os.path.join(PROJECT_ROOT,
    'research/ALL_OPTIMIZATIONS/duckdb_tpcds')
SF10_DB = '/mnt/d/TPC-DS/tpcds_sf10_2.duckdb'

SKIP_OPS = {'PROJECTION', 'RESULT_COLLECTOR', 'EXPLAIN_ANALYZE', 'COLUMN_DATA_SCAN'}

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


def extract_plan_costs(node, depth=0):
    """Extract cost metrics from a plan node tree.

    Returns dict with:
      total_est: sum of all operator estimated cardinalities
      scan_est: sum of scan operator estimates only
      max_scan: largest single scan estimate
      root_est: estimated cardinality of the root operator
      n_ops: total operator count (excluding noise)
      n_scans: number of scan operators
      n_joins: number of join operators
      operators: list of (op_name, est_rows) tuples
    """
    result = {
        'total_est': 0, 'scan_est': 0, 'max_scan': 0,
        'root_est': 0, 'n_ops': 0, 'n_scans': 0, 'n_joins': 0,
        'operators': [],
    }
    _collect_costs(node, result, depth, is_root=(depth == 0))
    return result


def _collect_costs(node, result, depth, is_root=False):
    if not node:
        return

    op_name = node.get('name', node.get('operator_name', node.get('operator_type', '')))
    if isinstance(op_name, str):
        op_name = op_name.strip()

    extra = node.get('extra_info', {})
    if isinstance(extra, str):
        extra = {}

    est_card = 0
    if isinstance(extra, dict):
        est_str = extra.get('Estimated Cardinality', '0')
        try:
            est_card = int(str(est_str).lstrip('~'))
        except (ValueError, TypeError):
            est_card = 0

    if op_name and op_name not in SKIP_OPS:
        result['n_ops'] += 1
        result['total_est'] += est_card
        result['operators'].append((op_name, est_card))

        if is_root or (depth == 0 and result['root_est'] == 0):
            result['root_est'] = est_card

        if 'SCAN' in op_name:
            result['n_scans'] += 1
            result['scan_est'] += est_card
            result['max_scan'] = max(result['max_scan'], est_card)

        if 'JOIN' in op_name:
            result['n_joins'] += 1

    children = node.get('children', [])
    for child in children:
        _collect_costs(child, result, depth + 1)


def extract_tables_from_sql(sql):
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
                continue
        return conn
    except Exception:
        conn.close()
        return None


def load_sf10_costs(query_num, sql_type='original'):
    """Load SF10 EXPLAIN plan costs from stored JSON."""
    explain_file = os.path.join(EXPLAIN_DIR, f'query_{query_num}.json')
    if not os.path.exists(explain_file):
        return None
    with open(explain_file) as f:
        data = json.load(f)
    plan = data.get('plan_json', {})
    return extract_plan_costs(plan)


def fmt(n):
    """Format number with K/M suffixes."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n/1_000:.1f}K"
    return str(n)


def main():
    print("=" * 95)
    print("EXPLAIN PLAN COST PREDICTION: Synthetic vs 2% TABLESAMPLE")
    print("  Question: Does EXPLAIN estimated cardinality predict optimization wins?")
    print("=" * 95)
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

        # === SYNTHETIC ===
        validator = SyntheticValidator()
        old_stdout = sys.stdout
        sys.stdout = io.StringIO()
        try:
            vresult = validator.validate(query_file, target_rows=100)
        finally:
            sys.stdout = old_stdout

        if not vresult['success']:
            print(f"SYNTH_FAIL: {vresult.get('error', '')[:60]}")
            continue

        synth_orig_plan = get_explain_plan(validator.conn, original_sql)
        synth_opt_plan = get_explain_plan(validator.conn, optimized_sql)

        if 'error' in synth_orig_plan or 'error' in synth_opt_plan:
            err = synth_orig_plan.get('error', '') or synth_opt_plan.get('error', '')
            print(f"EXPLAIN_ERR: {err[:60]}")
            continue

        synth_orig = extract_plan_costs(synth_orig_plan)
        synth_opt = extract_plan_costs(synth_opt_plan)

        # === 2% TABLESAMPLE ===
        sample_conn = create_tablesample_conn(original_sql, optimized_sql)
        samp_orig = samp_opt = None
        if sample_conn:
            try:
                samp_orig_plan = get_explain_plan(sample_conn, original_sql)
                samp_opt_plan = get_explain_plan(sample_conn, optimized_sql)
                if 'error' not in samp_orig_plan and 'error' not in samp_opt_plan:
                    samp_orig = extract_plan_costs(samp_orig_plan)
                    samp_opt = extract_plan_costs(samp_opt_plan)
            except Exception:
                pass
            finally:
                sample_conn.close()

        # === SF10 (ground truth for original only) ===
        sf10_orig = load_sf10_costs(q)

        # === Compute ratios ===
        # For each metric: ratio = original / optimized (>1 means opt is cheaper = correct prediction)
        def ratio(orig, opt, key):
            if orig and opt and opt[key] > 0:
                return orig[key] / opt[key]
            return None

        r = {
            'query': q,
            'sf10_speedup': info['speedup'],
            # Synthetic ratios
            'synth_total_ratio': ratio(synth_orig, synth_opt, 'total_est'),
            'synth_scan_ratio': ratio(synth_orig, synth_opt, 'scan_est'),
            'synth_max_scan_ratio': ratio(synth_orig, synth_opt, 'max_scan'),
            'synth_orig_total': synth_orig['total_est'],
            'synth_opt_total': synth_opt['total_est'],
            'synth_orig_scan': synth_orig['scan_est'],
            'synth_opt_scan': synth_opt['scan_est'],
            'synth_orig_ops': synth_orig['n_ops'],
            'synth_opt_ops': synth_opt['n_ops'],
            # 2% sample ratios
            'samp_total_ratio': ratio(samp_orig, samp_opt, 'total_est') if samp_orig else None,
            'samp_scan_ratio': ratio(samp_orig, samp_opt, 'scan_est') if samp_orig else None,
            'samp_max_scan_ratio': ratio(samp_orig, samp_opt, 'max_scan') if samp_orig else None,
            'samp_orig_total': samp_orig['total_est'] if samp_orig else None,
            'samp_opt_total': samp_opt['total_est'] if samp_opt else None,
            # SF10 original for reference
            'sf10_orig_total': sf10_orig['total_est'] if sf10_orig else None,
            'sf10_orig_scans': sf10_orig['n_scans'] if sf10_orig else None,
            # Operator detail
            'synth_orig_operators': synth_orig['operators'],
            'synth_opt_operators': synth_opt['operators'],
        }
        results.append(r)

        # One-line summary
        st = r['synth_total_ratio']
        tt = r['samp_total_ratio']
        ss = r['synth_scan_ratio']
        ts = r['samp_scan_ratio']
        st_dir = "OK" if st and st > 1.0 else "MISS"
        tt_dir = "OK" if tt and tt > 1.0 else ("N/A" if tt is None else "MISS")
        print(f"SF10={info['speedup']:.2f}x  "
              f"synth_est={st:.2f}x[{st_dir}]  "
              f"samp_est={'%.2f' % tt if tt else 'N/A'}x[{tt_dir}]  "
              f"synth_scan={ss:.2f}x  "
              f"samp_scan={'%.2f' % ts if ts else 'N/A'}x")

    if not results:
        print("\nNo results.")
        return

    # === DETAILED TABLE ===
    print()
    print("=" * 95)
    print("ESTIMATED CARDINALITY COMPARISON (orig/opt ratio — >1.0 means opt is cheaper)")
    print("=" * 95)
    print(f"{'Q':>3} {'SF10':>6}  {'--- Synthetic ---':^25}  {'--- 2% Sample ---':^25}")
    print(f"{'':>3} {'spdup':>6}  {'TotalRatio':>10} {'ScanRatio':>10} {'Ops':>5}  "
          f"{'TotalRatio':>10} {'ScanRatio':>10}")
    print("-" * 95)

    for r in results:
        q = r['query']
        sf10 = f"{r['sf10_speedup']:.2f}x"

        st = f"{r['synth_total_ratio']:.2f}x" if r['synth_total_ratio'] else "N/A"
        ss = f"{r['synth_scan_ratio']:.2f}x" if r['synth_scan_ratio'] else "N/A"
        s_ops = f"{r['synth_orig_ops']}/{r['synth_opt_ops']}"

        tt = f"{r['samp_total_ratio']:.2f}x" if r['samp_total_ratio'] else "N/A"
        ts = f"{r['samp_scan_ratio']:.2f}x" if r['samp_scan_ratio'] else "N/A"

        print(f"{q:>3} {sf10:>6}  {st:>10} {ss:>10} {s_ops:>5}  {tt:>10} {ts:>10}")

    # === DIRECTION ACCURACY ===
    print()
    print("=" * 95)
    print("DIRECTION ACCURACY (does est ratio >1.0 match SF10 speedup >1.0?)")
    print("=" * 95)

    metrics = [
        ('Total est ratio', 'synth_total_ratio', 'samp_total_ratio'),
        ('Scan est ratio', 'synth_scan_ratio', 'samp_scan_ratio'),
        ('Max scan ratio', 'synth_max_scan_ratio', 'samp_max_scan_ratio'),
    ]

    for label, synth_key, samp_key in metrics:
        synth_ok = sum(1 for r in results if r[synth_key] and r[synth_key] > 1.0)
        samp_ok = sum(1 for r in results if r[samp_key] and r[samp_key] > 1.0)
        synth_n = sum(1 for r in results if r[synth_key] is not None)
        samp_n = sum(1 for r in results if r[samp_key] is not None)

        sp = f"{synth_ok}/{synth_n} ({synth_ok/synth_n*100:.0f}%)" if synth_n else "N/A"
        tp = f"{samp_ok}/{samp_n} ({samp_ok/samp_n*100:.0f}%)" if samp_n else "N/A"
        winner = "Synth" if synth_ok > samp_ok else ("2%Samp" if samp_ok > synth_ok else "TIE")
        print(f"  {label:<20} Synth: {sp:<15} 2%Samp: {tp:<15} [{winner}]")

    # === RAW ESTIMATES ===
    print()
    print("=" * 95)
    print("RAW ESTIMATED CARDINALITY (total across all operators)")
    print("=" * 95)
    print(f"{'Q':>3} {'SF10':>6}  {'Synth Orig':>12} {'Synth Opt':>12} {'Ratio':>7}  "
          f"{'Samp Orig':>12} {'Samp Opt':>12} {'Ratio':>7}")
    print("-" * 95)
    for r in results:
        so = fmt(r['synth_orig_total'])
        sop = fmt(r['synth_opt_total'])
        sr = f"{r['synth_total_ratio']:.2f}" if r['synth_total_ratio'] else "N/A"
        to = fmt(r['samp_orig_total']) if r['samp_orig_total'] else "N/A"
        tp = fmt(r['samp_opt_total']) if r['samp_opt_total'] else "N/A"
        tr = f"{r['samp_total_ratio']:.2f}" if r['samp_total_ratio'] else "N/A"
        print(f"{r['query']:>3} {r['sf10_speedup']:.2f}x  {so:>12} {sop:>12} {sr:>7}  "
              f"{to:>12} {tp:>12} {tr:>7}")

    # === OPERATOR DETAIL ===
    print()
    print("=" * 95)
    print("OPERATOR DETAIL (synthetic)")
    print("=" * 95)
    for r in results:
        q = r['query']
        print(f"\nQ{q} (SF10 speedup: {r['sf10_speedup']:.2f}x):")
        print(f"  Original ({r['synth_orig_ops']} ops, total est {r['synth_orig_total']:,}):")
        for op, est in r['synth_orig_operators']:
            print(f"    {op:<25} est={est:,}")
        print(f"  Optimized ({r['synth_opt_ops']} ops, total est {r['synth_opt_total']:,}):")
        for op, est in r['synth_opt_operators']:
            print(f"    {op:<25} est={est:,}")


if __name__ == '__main__':
    main()
