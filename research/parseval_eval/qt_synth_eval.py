import argparse
import json
import os
import shutil
import tempfile
from collections import Counter
from pathlib import Path

import duckdb

import sys
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-shared')
sys.path.insert(0, '/mnt/c/Users/jakc9/Documents/QueryTorque_V8/packages/qt-sql')

from qt_sql.validation.synthetic_validator import SyntheticValidator


def schema_to_ddl(schema_dict, constraints=None):
    constraints = constraints or []
    primary = {}
    for c in constraints:
        if not isinstance(c, dict):
            continue
        if 'primary' in c:
            for item in c['primary'] or []:
                v = item.get('value') if isinstance(item, dict) else None
                if isinstance(v, str) and '__' in v:
                    t, col = v.split('__', 1)
                    primary.setdefault(t, set()).add(col)

    stmts = []
    for t, cols in schema_dict.items():
        defs = []
        for c, ty in cols.items():
            tpe = ty or 'INT'
            defs.append(f'"{c}" {tpe}')
        pks = sorted(primary.get(t, set()))
        if pks:
            defs.append('PRIMARY KEY (' + ', '.join(f'"{c}"' for c in pks) + ')')
        stmts.append(f'CREATE TABLE "{t}" (' + ', '.join(defs) + ')')
    return stmts


def gt_from_states(states):
    s = set(states or [])
    if 'NEQ' in s:
        return 'NEQ'
    if 'EQU' in s:
        return 'EQ'
    return 'UNK'


def eval_file(out_file, limit=None, seed=42, target_rows=100):
    import random
    random.seed(seed)

    rows = []
    with open(out_file) as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            rows.append(json.loads(ln))

    if limit is not None and limit < len(rows):
        rows = rows[:limit]

    tmp_root = Path(tempfile.mkdtemp(prefix='qt_synth_eval_'))
    results = []

    try:
        for i, row in enumerate(rows):
            gt = gt_from_states(row.get('states'))
            q1, q2 = row['pair'][0], row['pair'][1]

            db_path = tmp_root / f"schema_{i}.duckdb"
            conn = duckdb.connect(str(db_path))
            for ddl in schema_to_ddl(row['schema'], row.get('constraint')):
                conn.execute(ddl)
            conn.close()

            validator = SyntheticValidator(reference_db=str(db_path), dialect='mysql')
            try:
                r = validator.validate_sql_pair(q1, q2, target_rows=target_rows)
                if r.get('orig_success') and r.get('opt_success'):
                    pred = 'EQ' if r.get('match') else 'NEQ'
                else:
                    pred = 'ERR'
                reason = r.get('reason', '')
            except Exception as e:
                pred = 'ERR'
                reason = f'{type(e).__name__}: {e}'

            results.append({
                'index': row.get('index', i),
                'gt': gt,
                'pred': pred,
                'reason': reason,
            })

    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    gt_counter = Counter(r['gt'] for r in results)
    pred_counter = Counter(r['pred'] for r in results)

    labelled = [r for r in results if r['gt'] in ('EQ', 'NEQ')]
    strict_correct = sum(1 for r in labelled if r['pred'] == r['gt'])
    strict_acc = (strict_correct / len(labelled)) if labelled else 0.0

    resolved = [r for r in labelled if r['pred'] in ('EQ', 'NEQ')]
    resolved_correct = sum(1 for r in resolved if r['pred'] == r['gt'])
    resolved_acc = (resolved_correct / len(resolved)) if resolved else 0.0

    neqs = [r for r in labelled if r['gt'] == 'NEQ']
    neq_recall = (sum(1 for r in neqs if r['pred'] == 'NEQ') / len(neqs)) if neqs else 0.0

    eqs = [r for r in labelled if r['gt'] == 'EQ']
    eq_fp = (sum(1 for r in eqs if r['pred'] == 'NEQ') / len(eqs)) if eqs else 0.0

    return {
        'file': out_file,
        'rows': len(results),
        'gt_counts': dict(gt_counter),
        'pred_counts': dict(pred_counter),
        'strict_accuracy': strict_acc,
        'resolved_accuracy': resolved_acc,
        'resolved_coverage': (len(resolved) / len(labelled)) if labelled else 0.0,
        'neq_recall': neq_recall,
        'eq_false_positive_rate': eq_fp,
        'sample_errors': [r for r in results if r['pred'] == 'ERR'][:10],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out-file', required=True)
    ap.add_argument('--limit', type=int, default=None)
    ap.add_argument('--target-rows', type=int, default=100)
    args = ap.parse_args()

    summary = eval_file(args.out_file, limit=args.limit, target_rows=args.target_rows)
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
