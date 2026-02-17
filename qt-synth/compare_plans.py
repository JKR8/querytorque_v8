#!/usr/bin/env python3
"""
3-Way EXPLAIN Plan Comparison: Synthetic vs 2% TABLESAMPLE vs SF10

Compares operator trees from three data regimes to measure how well
synthetic data and 2% sampling predict the SF10 physical plan.

Uses DuckDBPlanParser from qt_sql for proper JSON plan parsing.
"""

import sys
import json
import os
import io

# Add project paths for imports
PROJECT_ROOT = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, os.path.dirname(__file__))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-sql'))
sys.path.insert(0, os.path.join(PROJECT_ROOT, 'packages', 'qt-shared'))

import duckdb
import sqlglot
from sqlglot import exp
from validator import SyntheticValidator
from qt_sql.execution.plan_parser import DuckDBPlanParser

QUERY_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries')
EXPLAIN_DIR = os.path.join(PROJECT_ROOT,
    'packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/explains')
SF10_DB = '/mnt/d/TPC-DS/tpcds_sf10_2.duckdb'

# Queries that pass synthetic validation
QUERIES = [1, 3, 7, 12, 15, 19, 42, 2, 5, 10]

# Skip these noise operators for comparison
SKIP_OPS = {'PROJECTION', 'RESULT_COLLECTOR', 'EXPLAIN_ANALYZE', 'COLUMN_DATA_SCAN'}


def collect_operators(node, depth=0):
    """Recursively collect operators from plan_json, skipping noise."""
    ops = []
    if not node:
        return ops

    op_name = node.get('operator_name', node.get('name', node.get('operator_type', '')))
    if isinstance(op_name, str):
        op_name = op_name.strip()

    if op_name and op_name not in SKIP_OPS:
        extra = node.get('extra_info', {})
        if isinstance(extra, str):
            extra = {}
        ops.append({
            'op': op_name,
            'depth': depth,
            'rows': node.get('operator_cardinality', 0),
            'table': extra.get('Table', '') if isinstance(extra, dict) else '',
        })

    for child in node.get('children', []):
        ops.extend(collect_operators(child, depth + 1))

    return ops


def get_op_sequence(ops):
    """Get operator type sequence (for LCS comparison)."""
    return [o['op'] for o in ops]


def get_join_types(ops):
    """Extract join operator types."""
    return [o['op'] for o in ops if 'JOIN' in o['op']]


def get_scan_tables(ops):
    """Extract scanned table names."""
    return sorted([o['table'] for o in ops if 'SCAN' in o['op'] and o['table']])


def lcs_similarity(seq1, seq2):
    """LCS-based similarity (0.0 = no overlap, 1.0 = identical)."""
    if not seq1 or not seq2:
        return 0.0
    m, n = len(seq1), len(seq2)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if seq1[i-1] == seq2[j-1]:
                dp[i][j] = dp[i-1][j-1] + 1
            else:
                dp[i][j] = max(dp[i-1][j], dp[i][j-1])
    return dp[m][n] / max(m, n)


def jaccard_similarity(set1, set2):
    """Jaccard similarity between two sets."""
    s1, s2 = set(set1), set(set2)
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    return len(s1 & s2) / len(s1 | s2)


def extract_tables_from_sql(sql):
    """Extract table names referenced in SQL using sqlglot."""
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


def get_explain_json(conn, sql):
    """Run EXPLAIN (FORMAT JSON) on a DuckDB connection and return plan dict.

    DuckDB EXPLAIN (FORMAT JSON) returns physical_plan as a list of nodes
    (field: 'name'), while EXPLAIN ANALYZE returns analyzed_plan as a dict
    (field: 'operator_name'). We normalize both to {'children': [...]}.
    """
    try:
        result = conn.execute(f"EXPLAIN (FORMAT JSON) {sql}").fetchall()
        for plan_type, plan_json_str in result:
            parsed = json.loads(plan_json_str)
            if plan_type == 'physical_plan':
                if isinstance(parsed, list):
                    return {'children': parsed}
                elif isinstance(parsed, dict):
                    return {'children': [parsed]}
        # Fallback: try any available plan type
        for plan_type, plan_json_str in result:
            parsed = json.loads(plan_json_str)
            if isinstance(parsed, list):
                return {'children': parsed}
            elif isinstance(parsed, dict):
                if 'children' in parsed:
                    return parsed
                return {'children': [parsed]}
    except Exception as e:
        return {'error': str(e)}
    return {'error': 'no plan returned'}


def load_sf10_plan(query_num):
    """Load pre-computed SF10 EXPLAIN plan from JSON file."""
    explain_file = os.path.join(EXPLAIN_DIR, f'query_{query_num}.json')
    if not os.path.exists(explain_file):
        return None
    with open(explain_file) as f:
        data = json.load(f)
    return data.get('plan_json', {})


def get_synthetic_plan(query_file, sql):
    """Run synthetic validator and get EXPLAIN (FORMAT JSON) from synthetic DB."""
    validator = SyntheticValidator()

    # Suppress validator output
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        result = validator.validate(query_file, target_rows=100)
    finally:
        sys.stdout = old_stdout

    if not result['success']:
        return None, f"validation failed: {result.get('error', 'unknown')}"

    # Get EXPLAIN from the synthetic connection
    plan = get_explain_json(validator.conn, sql)
    if 'error' in plan:
        return None, plan['error']

    return plan, None


def get_tablesample_plan(sql, tables):
    """Create 2% TABLESAMPLE of SF10 and get EXPLAIN (FORMAT JSON)."""
    conn = duckdb.connect(':memory:')
    try:
        conn.execute(f"ATTACH '{SF10_DB}' AS sf10 (READ_ONLY)")

        # Create 2% sampled tables in memory
        for table in tables:
            try:
                conn.execute(
                    f"CREATE TABLE {table} AS "
                    f"SELECT * FROM sf10.main.{table} USING SAMPLE 2 PERCENT"
                )
            except Exception as e:
                # Table might not exist in SF10 or name mismatch
                return None, f"sample failed for {table}: {e}"

        plan = get_explain_json(conn, sql)
        if 'error' in plan:
            return None, plan['error']
        return plan, None
    except Exception as e:
        return None, str(e)
    finally:
        conn.close()


def compare_plans(sf10_plan, synth_plan, sample_plan):
    """Compare three plans and return similarity metrics."""
    sf10_ops = collect_operators(sf10_plan)
    synth_ops = collect_operators(synth_plan) if synth_plan else []
    sample_ops = collect_operators(sample_plan) if sample_plan else []

    sf10_seq = get_op_sequence(sf10_ops)
    synth_seq = get_op_sequence(synth_ops)
    sample_seq = get_op_sequence(sample_ops)

    sf10_joins = get_join_types(sf10_ops)
    synth_joins = get_join_types(synth_ops)
    sample_joins = get_join_types(sample_ops)

    sf10_scans = get_scan_tables(sf10_ops)
    synth_scans = get_scan_tables(synth_ops)
    sample_scans = get_scan_tables(sample_ops)

    return {
        # Operator sequence similarity (LCS)
        'synth_op_sim': lcs_similarity(sf10_seq, synth_seq),
        'sample_op_sim': lcs_similarity(sf10_seq, sample_seq),
        # Join type similarity (LCS)
        'synth_join_sim': lcs_similarity(sf10_joins, synth_joins),
        'sample_join_sim': lcs_similarity(sf10_joins, sample_joins),
        # Table scan match (Jaccard)
        'synth_scan_sim': jaccard_similarity(sf10_scans, synth_scans),
        'sample_scan_sim': jaccard_similarity(sf10_scans, sample_scans),
        # Raw counts for display
        'sf10_ops': sf10_seq,
        'synth_ops': synth_seq,
        'sample_ops': sample_seq,
        'sf10_joins': sf10_joins,
        'synth_joins': synth_joins,
        'sample_joins': sample_joins,
        'sf10_scans': sf10_scans,
        'synth_scans': synth_scans,
        'sample_scans': sample_scans,
    }


def main():
    print("=" * 78)
    print("3-WAY EXPLAIN PLAN CORRELATION")
    print("  Synthetic (~100 rows) vs 2% TABLESAMPLE vs SF10 (ground truth)")
    print("=" * 78)
    print()

    results = []

    for q in QUERIES:
        query_file = os.path.join(QUERY_DIR, f'query_{q}.sql')
        if not os.path.exists(query_file):
            print(f"Q{q:2d}  SKIPPED (query file missing)")
            continue

        with open(query_file) as f:
            sql = f.read().strip()

        print(f"Q{q:2d}...", end=" ", flush=True)

        # 1. SF10 plan (from stored explains)
        sf10_plan = load_sf10_plan(q)
        if not sf10_plan:
            print("SKIP (no SF10 explain)")
            continue

        # 2. Synthetic plan
        synth_plan, synth_err = get_synthetic_plan(query_file, sql)
        if synth_err:
            print(f"SYNTH_ERR: {synth_err}")
            continue

        # 3. 2% TABLESAMPLE plan
        tables = extract_tables_from_sql(sql)
        sample_plan, sample_err = get_tablesample_plan(sql, tables)
        if sample_err:
            print(f"SAMPLE_ERR: {sample_err}")
            # Still compare synthetic vs SF10
            sample_plan = None

        # Compare
        metrics = compare_plans(sf10_plan, synth_plan, sample_plan)
        metrics['query'] = q
        results.append(metrics)

        # Print one-line summary
        s_op = metrics['synth_op_sim']
        t_op = metrics['sample_op_sim']
        s_join = metrics['synth_join_sim']
        t_join = metrics['sample_join_sim']
        s_scan = metrics['synth_scan_sim']
        t_scan = metrics['sample_scan_sim']
        winner = "SYNTH" if s_op > t_op else ("2%SAM" if t_op > s_op else "TIE")
        print(f"ops={s_op:.0%}/{t_op:.0%}  "
              f"joins={s_join:.0%}/{t_join:.0%}  "
              f"scans={s_scan:.0%}/{t_scan:.0%}  "
              f"[{winner}]")

    if not results:
        print("\nNo results to summarize.")
        return

    # === SUMMARY ===
    print()
    print("=" * 78)
    print("SUMMARY")
    print("=" * 78)
    print(f"{'Metric':<30} {'Synthetic':>10} {'2% Sample':>10} {'Winner':>8}")
    print("-" * 62)

    valid = results
    n = len(valid)

    avg_synth_op = sum(r['synth_op_sim'] for r in valid) / n
    avg_sample_op = sum(r['sample_op_sim'] for r in valid) / n
    w_op = "Synth" if avg_synth_op > avg_sample_op else "2%Samp"
    print(f"{'Avg operator seq similarity':<30} {avg_synth_op:>9.1%} {avg_sample_op:>9.1%} {w_op:>8}")

    avg_synth_join = sum(r['synth_join_sim'] for r in valid) / n
    avg_sample_join = sum(r['sample_join_sim'] for r in valid) / n
    w_join = "Synth" if avg_synth_join > avg_sample_join else "2%Samp"
    print(f"{'Avg join type similarity':<30} {avg_synth_join:>9.1%} {avg_sample_join:>9.1%} {w_join:>8}")

    avg_synth_scan = sum(r['synth_scan_sim'] for r in valid) / n
    avg_sample_scan = sum(r['sample_scan_sim'] for r in valid) / n
    w_scan = "Synth" if avg_synth_scan > avg_sample_scan else "2%Samp"
    print(f"{'Avg scan table match':<30} {avg_synth_scan:>9.1%} {avg_sample_scan:>9.1%} {w_scan:>8}")

    # Composite score (weighted average)
    synth_composite = 0.4 * avg_synth_op + 0.4 * avg_synth_join + 0.2 * avg_synth_scan
    sample_composite = 0.4 * avg_sample_op + 0.4 * avg_sample_join + 0.2 * avg_sample_scan
    w_comp = "Synth" if synth_composite > sample_composite else "2%Samp"
    print("-" * 62)
    print(f"{'COMPOSITE (40/40/20)':<30} {synth_composite:>9.1%} {sample_composite:>9.1%} {w_comp:>8}")

    # === DETAILED COMPARISON ===
    print()
    print("=" * 78)
    print("DETAILED OPERATOR SEQUENCES")
    print("=" * 78)
    for r in valid:
        q = r['query']
        print(f"\nQ{q}:")
        # Show operator sequences (compact)
        sf10_compact = ' > '.join(r['sf10_ops'])
        synth_compact = ' > '.join(r['synth_ops'])
        sample_compact = ' > '.join(r['sample_ops']) if r['sample_ops'] else '(error)'
        print(f"  SF10:   {sf10_compact}")
        print(f"  Synth:  {synth_compact}")
        print(f"  2%Sam:  {sample_compact}")
        # Join comparison
        if r['sf10_joins'] or r['synth_joins'] or r['sample_joins']:
            print(f"  Joins SF10:  {r['sf10_joins']}")
            print(f"  Joins Synth: {r['synth_joins']}")
            print(f"  Joins 2%Sam: {r['sample_joins']}")


if __name__ == '__main__':
    main()
