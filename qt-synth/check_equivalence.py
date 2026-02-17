#!/usr/bin/env python3
"""
Semantic Equivalence Checker: QT DSB-76 Optimizations

Two-layer validation:
  1. SYNTHETIC: Generate synthetic data, run original + optimized, compare results
  2. SF100:     Run both on real DSB SF100 DuckDB, compare results (ground truth)

The SF100 layer is the oracle — it tests both:
  - Whether the QT rewrite is semantically equivalent
  - Whether the synthetic validator correctly predicts equivalence
"""

import sys
import os
import io
import json
import hashlib
import time

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-sql'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-shared'))

import duckdb
import sqlglot

from validator import SyntheticValidator

# Paths
BASELINE_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/postgres_dsb_76/baseline_queries')
OPT_DIR = os.path.join(PROJECT_ROOT, 'research/ALL_OPTIMIZATIONS/postgres_dsb')
SF100_DB = '/mnt/d/DSB/dsb_sf100.duckdb'
PG_DSN = 'postgres://jakc9:jakc9@127.0.0.1:5434/dsb_sf10'


def transpile_pg_to_duckdb(sql: str) -> str:
    """Transpile PostgreSQL SQL to DuckDB dialect."""
    parts = sqlglot.transpile(sql, read='postgres', write='duckdb')
    return '\n'.join(parts)


def result_hash(rows) -> str:
    """MD5 hash of sorted result set for fast comparison."""
    if not rows:
        return 'EMPTY'
    # Sort rows and hash — order-independent equivalence
    sorted_rows = sorted(str(r) for r in rows)
    content = '\n'.join(sorted_rows)
    return hashlib.md5(content.encode()).hexdigest()


def compare_results(rows_orig, rows_opt, cols_orig=None, cols_opt=None):
    """Compare two result sets, return comparison dict."""
    if rows_orig is None or rows_opt is None:
        return {
            'match': False,
            'reason': 'one or both queries failed',
            'orig_rows': 0 if rows_orig is None else len(rows_orig),
            'opt_rows': 0 if rows_opt is None else len(rows_opt),
        }

    n_orig = len(rows_orig)
    n_opt = len(rows_opt)

    if n_orig != n_opt:
        return {
            'match': False,
            'reason': f'row count mismatch: {n_orig} vs {n_opt}',
            'orig_rows': n_orig,
            'opt_rows': n_opt,
        }

    # Column count check
    if cols_orig and cols_opt and len(cols_orig) != len(cols_opt):
        return {
            'match': False,
            'reason': f'column count mismatch: {len(cols_orig)} vs {len(cols_opt)}',
            'orig_rows': n_orig,
            'opt_rows': n_opt,
        }

    # Hash comparison (order-independent)
    h_orig = result_hash(rows_orig)
    h_opt = result_hash(rows_opt)

    if h_orig == h_opt:
        return {
            'match': True,
            'reason': 'hash match',
            'orig_rows': n_orig,
            'opt_rows': n_opt,
            'hash': h_orig,
        }

    # Hashes differ — find first differing row for diagnosis
    sorted_orig = sorted(str(r) for r in rows_orig)
    sorted_opt = sorted(str(r) for r in rows_opt)
    first_diff = None
    for i, (a, b) in enumerate(zip(sorted_orig, sorted_opt)):
        if a != b:
            first_diff = f'row {i}: {a[:120]} vs {b[:120]}'
            break

    return {
        'match': False,
        'reason': f'value mismatch: {first_diff or "unknown"}',
        'orig_rows': n_orig,
        'opt_rows': n_opt,
    }


def run_synthetic_check(orig_file, opt_file, reference_db=None):
    """Run synthetic validation: generate data, execute both, compare."""
    validator = SyntheticValidator(reference_db=reference_db, dialect='postgres')

    # Suppress validator stdout
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        result = validator.validate(orig_file, target_rows=100)
    finally:
        sys.stdout = old_stdout

    if not result['success']:
        return {
            'synth_status': 'SETUP_FAIL',
            'synth_error': result.get('error', 'unknown')[:120],
            'synth_match': None,
        }

    conn = validator.conn

    # Read and transpile the original SQL
    with open(orig_file) as f:
        orig_sql_raw = f.read().strip()
    # Handle multi-statement (take first)
    stmts = [s.strip() for s in orig_sql_raw.split(';') if s.strip()]
    stmts = [s for s in stmts if not all(
        line.strip().startswith('--') or not line.strip()
        for line in s.split('\n')
    )]
    orig_sql_pg = stmts[0] if stmts else orig_sql_raw

    try:
        orig_sql = transpile_pg_to_duckdb(orig_sql_pg)
    except Exception as e:
        return {
            'synth_status': 'TRANSPILE_FAIL',
            'synth_error': f'orig transpile: {e}',
            'synth_match': None,
        }

    # Read and transpile the optimized SQL
    with open(opt_file) as f:
        opt_sql_raw = f.read().strip()
    stmts = [s.strip() for s in opt_sql_raw.split(';') if s.strip()]
    stmts = [s for s in stmts if not all(
        line.strip().startswith('--') or not line.strip()
        for line in s.split('\n')
    )]
    opt_sql_pg = stmts[0] if stmts else opt_sql_raw

    try:
        opt_sql = transpile_pg_to_duckdb(opt_sql_pg)
    except Exception as e:
        return {
            'synth_status': 'TRANSPILE_FAIL',
            'synth_error': f'opt transpile: {e}',
            'synth_match': None,
        }

    # Execute original on synthetic data
    try:
        rows_orig = conn.execute(orig_sql).fetchall()
        cols_orig = [d[0] for d in conn.description] if conn.description else []
    except Exception as e:
        return {
            'synth_status': 'ORIG_EXEC_FAIL',
            'synth_error': str(e)[:120],
            'synth_match': None,
        }

    # Execute optimized on synthetic data
    try:
        rows_opt = conn.execute(opt_sql).fetchall()
        cols_opt = [d[0] for d in conn.description] if conn.description else []
    except Exception as e:
        return {
            'synth_status': 'OPT_EXEC_FAIL',
            'synth_error': str(e)[:120],
            'synth_match': None,
        }

    comp = compare_results(rows_orig, rows_opt, cols_orig, cols_opt)
    return {
        'synth_status': 'OK',
        'synth_match': comp['match'],
        'synth_orig_rows': comp['orig_rows'],
        'synth_opt_rows': comp['opt_rows'],
        'synth_reason': comp['reason'],
    }


def run_sf100_check(orig_file, opt_file):
    """Run SF100 ground-truth check: execute both on real data, compare."""
    # Read and transpile both
    with open(orig_file) as f:
        orig_sql_raw = f.read().strip()
    stmts = [s.strip() for s in orig_sql_raw.split(';') if s.strip()]
    stmts = [s for s in stmts if not all(
        line.strip().startswith('--') or not line.strip()
        for line in s.split('\n')
    )]
    orig_sql_pg = stmts[0] if stmts else orig_sql_raw

    with open(opt_file) as f:
        opt_sql_raw = f.read().strip()
    stmts = [s.strip() for s in opt_sql_raw.split(';') if s.strip()]
    stmts = [s for s in stmts if not all(
        line.strip().startswith('--') or not line.strip()
        for line in s.split('\n')
    )]
    opt_sql_pg = stmts[0] if stmts else opt_sql_raw

    try:
        orig_sql = transpile_pg_to_duckdb(orig_sql_pg)
    except Exception as e:
        return {
            'sf100_status': 'TRANSPILE_FAIL',
            'sf100_error': f'orig: {e}',
            'sf100_match': None,
        }

    try:
        opt_sql = transpile_pg_to_duckdb(opt_sql_pg)
    except Exception as e:
        return {
            'sf100_status': 'TRANSPILE_FAIL',
            'sf100_error': f'opt: {e}',
            'sf100_match': None,
        }

    conn = duckdb.connect(SF100_DB, read_only=True)
    try:
        # Run original
        t0 = time.perf_counter()
        try:
            rows_orig = conn.execute(orig_sql).fetchall()
            cols_orig = [d[0] for d in conn.description] if conn.description else []
        except Exception as e:
            return {
                'sf100_status': 'ORIG_EXEC_FAIL',
                'sf100_error': str(e)[:120],
                'sf100_match': None,
            }
        t_orig = (time.perf_counter() - t0) * 1000

        # Run optimized
        t0 = time.perf_counter()
        try:
            rows_opt = conn.execute(opt_sql).fetchall()
            cols_opt = [d[0] for d in conn.description] if conn.description else []
        except Exception as e:
            return {
                'sf100_status': 'OPT_EXEC_FAIL',
                'sf100_error': str(e)[:120],
                'sf100_match': None,
            }
        t_opt = (time.perf_counter() - t0) * 1000

        comp = compare_results(rows_orig, rows_opt, cols_orig, cols_opt)
        return {
            'sf100_status': 'OK',
            'sf100_match': comp['match'],
            'sf100_orig_rows': comp['orig_rows'],
            'sf100_opt_rows': comp['opt_rows'],
            'sf100_reason': comp['reason'],
            'sf100_orig_ms': round(t_orig, 1),
            'sf100_opt_ms': round(t_opt, 1),
        }
    finally:
        conn.close()


def find_queries():
    """Find all QT optimizations with swarm2_final/optimized.sql."""
    queries = []
    for entry in sorted(os.listdir(OPT_DIR)):
        if not entry.startswith('query'):
            continue
        opt_file = os.path.join(OPT_DIR, entry, 'swarm2_final', 'optimized.sql')
        orig_file = os.path.join(BASELINE_DIR, f'{entry}.sql')
        if os.path.exists(opt_file) and os.path.exists(orig_file):
            queries.append((entry, orig_file, opt_file))
    return queries


def main():
    print("=" * 90)
    print("SEMANTIC EQUIVALENCE CHECKER: QT DSB-76 Optimizations")
    print("  Layer 1: Synthetic data (~100 rows)")
    print("  Layer 2: DSB SF100 DuckDB (ground truth)")
    print("=" * 90)
    print()

    queries = find_queries()
    print(f"Found {len(queries)} QT optimizations with final SQL\n")

    # Use PG DSN as reference DB for synthetic validator
    reference_db = PG_DSN

    results = []
    for name, orig_file, opt_file in queries:
        print(f"{name:25s} ", end="", flush=True)

        # Layer 1: Synthetic
        synth = run_synthetic_check(orig_file, opt_file, reference_db=reference_db)

        # Layer 2: SF100
        sf100 = run_sf100_check(orig_file, opt_file)

        r = {'query': name, **synth, **sf100}
        results.append(r)

        # One-line summary
        s_sym = {True: 'EQ', False: 'NEQ', None: 'ERR'}.get(synth.get('synth_match'))
        f_sym = {True: 'EQ', False: 'NEQ', None: 'ERR'}.get(sf100.get('sf100_match'))

        s_detail = ''
        if synth.get('synth_match') is not None:
            s_detail = f" ({synth.get('synth_orig_rows', '?')}r)"
        elif synth.get('synth_error'):
            s_detail = f" ({synth['synth_status']})"

        f_detail = ''
        if sf100.get('sf100_match') is not None:
            f_detail = f" ({sf100.get('sf100_orig_rows', '?')}r"
            if sf100.get('sf100_orig_ms'):
                f_detail += f" {sf100['sf100_orig_ms']:.0f}ms→{sf100['sf100_opt_ms']:.0f}ms"
            f_detail += ")"
        elif sf100.get('sf100_error'):
            f_detail = f" ({sf100['sf100_status']})"

        # Agreement check
        agree = ''
        if synth.get('synth_match') is not None and sf100.get('sf100_match') is not None:
            if synth['synth_match'] == sf100['sf100_match']:
                agree = ' ✓AGREE'
            else:
                agree = ' ✗DISAGREE'

        print(f"synth={s_sym}{s_detail:20s}  sf100={f_sym}{f_detail}{agree}")

    if not results:
        print("\nNo results.")
        return

    # === SUMMARY ===
    print()
    print("=" * 90)
    print("SUMMARY")
    print("=" * 90)

    n = len(results)

    # Synthetic stats
    synth_ok = sum(1 for r in results if r.get('synth_status') == 'OK')
    synth_eq = sum(1 for r in results if r.get('synth_match') is True)
    synth_neq = sum(1 for r in results if r.get('synth_match') is False)
    synth_err = sum(1 for r in results if r.get('synth_match') is None)

    # SF100 stats
    sf100_ok = sum(1 for r in results if r.get('sf100_status') == 'OK')
    sf100_eq = sum(1 for r in results if r.get('sf100_match') is True)
    sf100_neq = sum(1 for r in results if r.get('sf100_match') is False)
    sf100_err = sum(1 for r in results if r.get('sf100_match') is None)

    print(f"\n  {'Layer':<20} {'Total':>6} {'EQ':>6} {'NEQ':>6} {'ERR':>6}")
    print(f"  {'-'*50}")
    print(f"  {'Synthetic':<20} {synth_ok:>6} {synth_eq:>6} {synth_neq:>6} {synth_err:>6}")
    print(f"  {'SF100 (truth)':<20} {sf100_ok:>6} {sf100_eq:>6} {sf100_neq:>6} {sf100_err:>6}")

    # Agreement matrix: how well does synthetic predict SF100?
    both_ok = [(r.get('synth_match'), r.get('sf100_match'))
               for r in results
               if r.get('synth_match') is not None and r.get('sf100_match') is not None]

    if both_ok:
        agree = sum(1 for s, f in both_ok if s == f)
        disagree = len(both_ok) - agree
        tp = sum(1 for s, f in both_ok if s and f)  # both say EQ
        tn = sum(1 for s, f in both_ok if not s and not f)  # both say NEQ
        fp = sum(1 for s, f in both_ok if s and not f)  # synth says EQ, sf100 says NEQ (dangerous!)
        fn = sum(1 for s, f in both_ok if not s and f)  # synth says NEQ, sf100 says EQ (conservative)

        print(f"\n  SYNTHETIC vs SF100 AGREEMENT ({len(both_ok)} comparable)")
        print(f"  {'Agree':>12}: {agree}/{len(both_ok)} ({agree/len(both_ok)*100:.0f}%)")
        print(f"  {'Disagree':>12}: {disagree}/{len(both_ok)}")
        print(f"\n  Confusion matrix (synth prediction vs SF100 truth):")
        print(f"    True Positive  (both EQ):     {tp}")
        print(f"    True Negative  (both NEQ):    {tn}")
        print(f"    False Positive (synth EQ, sf100 NEQ — DANGEROUS): {fp}")
        print(f"    False Negative (synth NEQ, sf100 EQ — conservative): {fn}")

        if tp + fp > 0:
            precision = tp / (tp + fp)
            print(f"\n  Precision (of synth EQ calls): {precision:.0%}")
        if tp + fn > 0:
            recall = tp / (tp + fn)
            print(f"  Recall (of sf100 EQ queries):  {recall:.0%}")

    # List disagreements
    disagree_list = [r for r in results
                     if r.get('synth_match') is not None
                     and r.get('sf100_match') is not None
                     and r['synth_match'] != r['sf100_match']]
    if disagree_list:
        print(f"\n  DISAGREEMENTS ({len(disagree_list)}):")
        for r in disagree_list:
            s = 'EQ' if r['synth_match'] else 'NEQ'
            f = 'EQ' if r['sf100_match'] else 'NEQ'
            reason = r.get('sf100_reason', r.get('synth_reason', ''))
            print(f"    {r['query']:25s} synth={s} sf100={f}  ({reason})")

    # List SF100 NEQ (real semantic errors in QT rewrites)
    sf100_neq_list = [r for r in results if r.get('sf100_match') is False]
    if sf100_neq_list:
        print(f"\n  SF100 SEMANTIC ERRORS ({len(sf100_neq_list)} QT rewrites NOT equivalent):")
        for r in sf100_neq_list:
            print(f"    {r['query']:25s} {r.get('sf100_reason', '')}")

    # List errors
    err_list = [r for r in results
                if r.get('synth_match') is None or r.get('sf100_match') is None]
    if err_list:
        print(f"\n  ERRORS ({len(err_list)}):")
        for r in err_list:
            s_err = r.get('synth_error', r.get('synth_status', ''))
            f_err = r.get('sf100_error', r.get('sf100_status', ''))
            if r.get('synth_match') is None:
                print(f"    {r['query']:25s} synth: {s_err}")
            if r.get('sf100_match') is None:
                print(f"    {r['query']:25s} sf100: {f_err}")

    # Save full results to JSON
    out_file = os.path.join(os.path.dirname(__file__), 'equivalence_results.json')
    with open(out_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n  Full results saved to: {out_file}")


if __name__ == '__main__':
    main()
