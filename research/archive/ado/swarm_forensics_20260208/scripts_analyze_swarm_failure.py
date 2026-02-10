#!/usr/bin/env python3
import csv
import json
import re
import shutil
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median

ROOT = Path('/mnt/c/Users/jakc9/Documents/QueryTorque_V8')
BENCH = ROOT / 'packages/qt-sql/ado/benchmarks/duckdb_tpcds'
SWARM_BATCH = BENCH / 'swarm_batch_20260208_102033'
OUT = ROOT / 'packages/qt-sql/ado/swarm_forensics_20260208'

DATA_DIR = OUT / 'data'
SQL_DIR = OUT / 'sql'
SQL_PREV = SQL_DIR / 'prev_winners'
SQL_SWARM = SQL_DIR / 'swarm_best'
SQL_ORIG = SQL_DIR / 'original'

for p in [DATA_DIR, SQL_DIR, SQL_PREV, SQL_SWARM, SQL_ORIG]:
    p.mkdir(parents=True, exist_ok=True)


PRINCIPLE_MAP = {
    'or_to_union': 'OR predicate decomposition into UNION ALL branches',
    'decorrelate': 'Decorrelate subqueries into join/group pipelines',
    'date_cte_isolate': 'Isolate date filters into selective CTE before fact joins',
    'materialize_cte': 'Materialize reusable intermediate result sets',
    'early_filter': 'Push selective filters before heavy joins/aggregations',
    'pushdown': 'Predicate pushdown into fact scans',
    'single_pass_aggregation': 'Replace repeated scans with one CASE-based aggregate pass',
    'prefetch_fact_join': 'Pre-join/prefetch filtered fact rows ahead of final shape',
    'multi_dimension_prefetch': 'Prefetch multiple dimension filters before aggregation',
    'multi_date_range_cte': 'Split multiple date ranges into isolated CTEs',
    'dimension_cte_isolate': 'Isolate selective dimensions in dedicated CTEs',
    'shared_dimension_multi_channel': 'Share filtered dimensions across sales channels',
    'composite_decorrelate_union': 'Hybrid decorrelation + union decomposition',
    'intersect_to_exists': 'Replace INTERSECT with EXISTS or semijoin structure',
    'history_steered': 'History-guided structural rewrite pattern',
    'semantic_rewrite': 'Semantic rewrite without explicit structural label',
    'final_worker': 'Final synthesis worker rewrite',
    'snipe_worker': 'Sniping worker rewrite',
}

PRINCIPLE_ALIASES = {
    'or_to_union': {'or_to_union'},
    'decorrelate': {'decorrelate', 'composite_decorrelate_union'},
    'date_cte_isolate': {'date_cte_isolate', 'multi_date_range_cte'},
    'materialize_cte': {'materialize_cte'},
    'early_filter': {'early_filter', 'pushdown'},
    'single_pass_aggregation': {'single_pass_aggregation'},
    'prefetch_fact_join': {'prefetch_fact_join', 'multi_dimension_prefetch'},
    'dimension_cte_isolate': {'dimension_cte_isolate', 'shared_dimension_multi_channel'},
    'intersect_to_exists': {'intersect_to_exists'},
    'history_steered': {'history_steered'},
    'semantic_rewrite': {'semantic_rewrite'},
    'final_worker': {'final_worker'},
    'snipe_worker': {'snipe_worker'},
}


def read_json(path: Path):
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding='utf-8'))


def read_text(path: Path):
    if not path.exists():
        return None
    return path.read_text(encoding='utf-8', errors='ignore')


def qid_to_query_file(qid: str) -> str:
    return f"query_{qid[1:]}.sql"


def qid_to_query_dir(qid: str) -> str:
    return f"query_{qid[1:]}"


def worker_file_for_id(worker_id: int) -> str:
    if worker_id in {1, 2, 3, 4}:
        return f'worker_{worker_id}_sql.sql'
    if worker_id == 5:
        return 'snipe_worker_sql.sql'
    if worker_id == 6:
        return 'final_worker_sql.sql'
    return f'worker_{worker_id}_sql.sql'


def parse_worker_id(sw: str):
    if not sw:
        return None
    m = re.match(r'W(\d+)$', sw.strip())
    return int(m.group(1)) if m else None


def extract_numbers(sql: str):
    if not sql:
        return set()
    nums = set(re.findall(r'\b\d+(?:\.\d+)?\b', sql))
    return {n for n in nums if n not in {'0', '1', '2', '100'}}


def extract_measure_cols(sql: str):
    if not sql:
        return set()
    pattern = re.compile(r'\b(?:ss|ws|cs|sr|wr|cr)_[a-z0-9_]+\b', re.I)
    cols = {c.lower() for c in pattern.findall(sql)}
    cols = {
        c for c in cols
        if any(tok in c for tok in ['sales', 'profit', 'discount', 'paid', 'quantity', 'list_price', 'ext_', 'net_'])
    }
    return cols


def has_date_literal_changes(orig: str, rew: str) -> bool:
    if not orig or not rew:
        return False
    pats = [
        r'd_year\s*=\s*(\d+)',
        r'd_moy\s*=\s*(\d+)',
        r'd_month_seq\s+in\s*\(([^\)]*)\)',
    ]
    for p in pats:
        o = re.findall(p, orig, flags=re.I)
        r = re.findall(p, rew, flags=re.I)
        if o and r and set(o) != set(r):
            return True
    return False


def infer_principle_key(prev_transforms, orig_sql: str, prev_sql: str):
    if prev_transforms:
        for t in prev_transforms:
            if t in PRINCIPLE_MAP:
                return t
        return prev_transforms[0]

    o = (orig_sql or '').lower()
    p = (prev_sql or '').lower()

    if 'sum(case when' in p and o.count('select') >= 6:
        return 'single_pass_aggregation'
    if ' union all ' in p and ' or ' in o:
        return 'or_to_union'
    if (' exists (' in o or ' in (select' in o) and (' exists (' not in p and ' in (select' not in p):
        return 'decorrelate'
    if 'date_dim' in p and ('with ' in p or 'filtered_dates' in p or 'd_year' in p or 'd_month_seq' in p):
        return 'date_cte_isolate'
    if 'with ' in p and ('filtered_' in p or 'prefilter' in p):
        return 'early_filter'
    return 'unlabeled_structural_rewrite'


def principle_label(key: str):
    return PRINCIPLE_MAP.get(key, key.replace('_', ' '))


def principle_aliases(key: str):
    return PRINCIPLE_ALIASES.get(key, {key})


def sql_matches_principle(key: str, sql: str):
    if not sql:
        return False
    s = sql.lower()
    if key == 'or_to_union':
        return ' union all ' in s
    if key == 'decorrelate':
        return ' exists (' not in s and ' in (select' not in s and ' join ' in s
    if key == 'date_cte_isolate':
        return 'date_dim' in s and ('with ' in s or 'filtered_date' in s or 'd_year' in s or 'd_month_seq' in s)
    if key == 'materialize_cte':
        return 'with ' in s and s.count('),') >= 1
    if key == 'early_filter':
        return 'where' in s and ('filtered_' in s or 'prefilter' in s or 'date_dim' in s)
    if key == 'single_pass_aggregation':
        return s.count('sum(case when') >= 2 or s.count('avg(case when') >= 2
    if key == 'prefetch_fact_join':
        return 'with ' in s and ('fact' in s or 'prefetch' in s or 'filtered_sales' in s)
    if key == 'dimension_cte_isolate':
        return 'with ' in s and ('dimension' in s or 'filtered_items' in s or 'filtered_customer' in s)
    if key == 'intersect_to_exists':
        return ' exists ' in s and ' intersect ' not in s
    if key == 'history_steered':
        return 'with ' in s
    if key == 'semantic_rewrite':
        return True
    return False


@dataclass
class QueryForensic:
    query_id: str
    prev_source: str
    prev_speedup_stored: float
    prev_speedup_recomputed: float
    prev_speedup_gap: float
    swarm_speedup: float
    delta_stored: str
    delta_same_baseline: float
    prev_ms: float
    swarm_ms: float
    baseline_ms: float
    prev_transforms: list
    swarm_transforms: list
    prev_principle_key: str
    prev_principle_label: str
    explored_assignment: bool
    explored_reanalyze: bool
    explored_sql: bool
    explored_where: str
    best_principle_worker_speedup: float
    reason_category: str
    reason_detail: str
    baseline_mismatch_flag: bool
    semantic_drift_flag: bool
    semantic_drift_notes: str
    provenance_risk_flag: bool
    prev_sql_path: str
    swarm_sql_path: str
    original_sql_path: str


comparison = read_json(BENCH / 'swarm_comparison.json')
if not comparison:
    raise SystemExit('Missing swarm_comparison.json')

queries = comparison['queries']
prev_cases = [q for q in queries if q.get('winner') == 'prev']

records = []

for q in prev_cases:
    qid = q['query_id']
    qdir = SWARM_BATCH / qid_to_query_dir(qid)

    baseline_ms = q.get('baseline_ms')
    prev_speedup = q.get('prev_speedup')
    prev_ms = q.get('prev_ms')
    swarm_speedup = q.get('swarm_speedup')
    swarm_ms = q.get('swarm_ms')

    prev_recomputed = None
    prev_gap = None
    delta_same = None
    if baseline_ms and prev_ms:
        prev_recomputed = baseline_ms / prev_ms
    if prev_recomputed is not None and prev_speedup is not None:
        prev_gap = prev_speedup - prev_recomputed
    if swarm_speedup is not None and prev_recomputed is not None:
        delta_same = swarm_speedup - prev_recomputed

    prev_meta = read_json(BENCH / 'best' / f'{qid}.json') or {}
    prev_sql_source_path = prev_meta.get('sql_source_path')
    prev_sql_fallback = BENCH / 'best' / f'{qid}.sql'

    prev_sql_path = Path(prev_sql_source_path) if prev_sql_source_path else prev_sql_fallback
    if not prev_sql_path.exists() and prev_sql_fallback.exists():
        prev_sql_path = prev_sql_fallback

    orig_sql_path = BENCH / 'queries' / qid_to_query_file(qid)

    worker_id = parse_worker_id(q.get('swarm_worker'))
    swarm_sql_path = None
    if worker_id is not None:
        c = qdir / worker_file_for_id(worker_id)
        if c.exists():
            swarm_sql_path = c

    orig_sql = read_text(orig_sql_path)
    prev_sql = read_text(prev_sql_path)
    swarm_sql = read_text(swarm_sql_path) if swarm_sql_path else None

    # Copy SQL evidence.
    if orig_sql_path.exists():
        shutil.copy2(orig_sql_path, SQL_ORIG / f'{qid}.sql')
    if prev_sql_path and prev_sql_path.exists():
        shutil.copy2(prev_sql_path, SQL_PREV / f'{qid}.sql')
    if swarm_sql_path and swarm_sql_path.exists():
        shutil.copy2(swarm_sql_path, SQL_SWARM / f'{qid}.sql')

    prev_transforms = q.get('prev_transforms') or []
    swarm_transforms = q.get('swarm_transforms') or []

    prev_principle_key = infer_principle_key(prev_transforms, orig_sql, prev_sql)
    prev_principle_label = principle_label(prev_principle_key)

    # Explore whether swarm explored principle.
    aliases = {a.lower() for a in principle_aliases(prev_principle_key)}

    assignments = read_json(qdir / 'assignments.json') or []
    assigned_workers = set()
    for a in assignments:
        ex = {x.lower() for x in a.get('examples', [])}
        if ex.intersection(aliases):
            assigned_workers.add(a.get('worker_id'))

    rean = read_json(qdir / 'reanalyze_parsed.json') or {}
    rean_examples = {x.lower() for x in (rean.get('examples') or [])}
    explored_reanalyze = bool(rean_examples.intersection(aliases))

    sql_match_workers = set()
    for wid in [1, 2, 3, 4, 5, 6]:
        sp = qdir / worker_file_for_id(wid)
        if sql_matches_principle(prev_principle_key, read_text(sp)):
            sql_match_workers.add(wid)

    explored_assignment = len(assigned_workers) > 0
    explored_sql = len(sql_match_workers) > 0

    explored_where_parts = []
    if explored_assignment:
        explored_where_parts.append('assignment')
    if explored_reanalyze:
        explored_where_parts.append('reanalyze')
    if explored_sql:
        explored_where_parts.append('worker_sql')
    explored_where = ','.join(explored_where_parts) if explored_where_parts else 'none'

    # Best worker speedup tied to principle-related workers.
    iter_files = sorted(qdir.glob('benchmark_iter*.json')) if qdir.exists() else []
    worker_best = {}
    for it in iter_files:
        bj = read_json(it) or {}
        for w in bj.get('workers', []):
            wid = w.get('worker_id')
            sp = w.get('speedup')
            if wid is None or sp is None:
                continue
            if wid not in worker_best or sp > worker_best[wid]:
                worker_best[wid] = sp

    principle_workers = set(assigned_workers) | set(sql_match_workers)
    principle_best = None
    if principle_workers:
        vals = [worker_best[wid] for wid in principle_workers if wid in worker_best]
        if vals:
            principle_best = max(vals)

    # Quality/risk checks.
    orig_nums = extract_numbers(orig_sql)
    prev_nums = extract_numbers(prev_sql)
    num_new = sorted(prev_nums - orig_nums)
    num_missing = sorted(orig_nums - prev_nums)

    orig_measures = extract_measure_cols(orig_sql)
    prev_measures = extract_measure_cols(prev_sql)

    measure_jacc = 1.0
    if orig_measures or prev_measures:
        inter = len(orig_measures & prev_measures)
        union = len(orig_measures | prev_measures)
        measure_jacc = inter / union if union else 1.0

    date_lit_change = has_date_literal_changes(orig_sql or '', prev_sql or '')

    semantic_drift_flag = (
        date_lit_change
        or (len(num_new) + len(num_missing) >= 10)
        or (measure_jacc < 0.5 and len(orig_measures) >= 2 and len(prev_measures) >= 2)
    )

    semantic_notes = []
    if date_lit_change:
        semantic_notes.append('date literals changed')
    if len(num_new) + len(num_missing) >= 10:
        semantic_notes.append(f'numeric literals diverged ({len(num_new)} new, {len(num_missing)} missing)')
    if measure_jacc < 0.5 and len(orig_measures) >= 2 and len(prev_measures) >= 2:
        semantic_notes.append(f'measure columns changed (jaccard={measure_jacc:.2f})')

    baseline_mismatch_flag = abs(prev_gap) >= 0.25 if prev_gap is not None else False

    provenance_risk_flag = (q.get('prev_source') or '').lower() in {'unvalidated', 'state_0', 'analyst_mode'}

    # Reason classification.
    reason_category = ''
    reason_detail = ''

    if swarm_speedup is None:
        reason_category = 'missing_swarm_result'
        reason_detail = 'No valid swarm speedup recorded; previous best stood uncontested in this batch.'
    elif prev_principle_key == 'unlabeled_structural_rewrite':
        if semantic_drift_flag:
            reason_category = 'unlabeled_prev_plus_semantic_risk'
            reason_detail = 'Previous winning SQL is unlabeled and shows high semantic drift risk; swarm searched safer patterns.'
        else:
            reason_category = 'unlabeled_prev_principle'
            reason_detail = 'Previous winning SQL has no explicit transform label, so swarm had weak guidance on the exact winning tactic.'
    elif not (explored_assignment or explored_reanalyze or explored_sql):
        reason_category = 'principle_not_explored'
        reason_detail = f'Swarm did not target principle `{prev_principle_key}` in assignments, reanalysis, or SQL outputs.'
    else:
        if principle_best is None:
            reason_category = 'principle_explored_no_measured_output'
            reason_detail = f'Principle `{prev_principle_key}` appeared in planning/SQL but has no benchmarked worker result.'
        elif principle_best < 1.0:
            reason_category = 'principle_attempt_regressed'
            reason_detail = f'Workers using principle `{prev_principle_key}` regressed (best {principle_best:.3f}x).'
        else:
            target_prev = prev_recomputed if prev_recomputed is not None else prev_speedup
            if target_prev is not None and principle_best < target_prev:
                reason_category = 'principle_attempt_underperformed'
                reason_detail = (
                    f'Principle `{prev_principle_key}` was explored but best swarm attempt ({principle_best:.3f}x) '
                    f'was below previous result ({target_prev:.3f}x on same-baseline when available).'
                )
            else:
                reason_category = 'selection_or_variance_gap'
                reason_detail = 'Principle was explored near/above prior level, but final swarm pick still under previous best.'

    records.append(QueryForensic(
        query_id=qid,
        prev_source=q.get('prev_source'),
        prev_speedup_stored=prev_speedup,
        prev_speedup_recomputed=prev_recomputed,
        prev_speedup_gap=prev_gap,
        swarm_speedup=swarm_speedup,
        delta_stored=q.get('delta'),
        delta_same_baseline=delta_same,
        prev_ms=prev_ms,
        swarm_ms=swarm_ms,
        baseline_ms=baseline_ms,
        prev_transforms=prev_transforms,
        swarm_transforms=swarm_transforms,
        prev_principle_key=prev_principle_key,
        prev_principle_label=prev_principle_label,
        explored_assignment=explored_assignment,
        explored_reanalyze=explored_reanalyze,
        explored_sql=explored_sql,
        explored_where=explored_where,
        best_principle_worker_speedup=principle_best,
        reason_category=reason_category,
        reason_detail=reason_detail,
        baseline_mismatch_flag=baseline_mismatch_flag,
        semantic_drift_flag=semantic_drift_flag,
        semantic_drift_notes='; '.join(semantic_notes),
        provenance_risk_flag=provenance_risk_flag,
        prev_sql_path=str(prev_sql_path) if prev_sql_path else '',
        swarm_sql_path=str(swarm_sql_path) if swarm_sql_path else '',
        original_sql_path=str(orig_sql_path),
    ))

# Write machine-readable outputs.
rows = []
for r in records:
    rows.append({
        'query_id': r.query_id,
        'prev_source': r.prev_source,
        'prev_speedup_stored': r.prev_speedup_stored,
        'prev_speedup_recomputed': r.prev_speedup_recomputed,
        'prev_speedup_gap': r.prev_speedup_gap,
        'swarm_speedup': r.swarm_speedup,
        'delta_stored': r.delta_stored,
        'delta_same_baseline': r.delta_same_baseline,
        'prev_ms': r.prev_ms,
        'swarm_ms': r.swarm_ms,
        'baseline_ms': r.baseline_ms,
        'prev_transforms': '|'.join(r.prev_transforms),
        'swarm_transforms': '|'.join(r.swarm_transforms),
        'prev_principle_key': r.prev_principle_key,
        'prev_principle_label': r.prev_principle_label,
        'explored_assignment': r.explored_assignment,
        'explored_reanalyze': r.explored_reanalyze,
        'explored_sql': r.explored_sql,
        'explored_where': r.explored_where,
        'best_principle_worker_speedup': r.best_principle_worker_speedup,
        'reason_category': r.reason_category,
        'reason_detail': r.reason_detail,
        'baseline_mismatch_flag': r.baseline_mismatch_flag,
        'semantic_drift_flag': r.semantic_drift_flag,
        'semantic_drift_notes': r.semantic_drift_notes,
        'provenance_risk_flag': r.provenance_risk_flag,
        'prev_sql_path': r.prev_sql_path,
        'swarm_sql_path': r.swarm_sql_path,
        'original_sql_path': r.original_sql_path,
    })

(DATA_DIR / 'prev_winner_forensics.json').write_text(json.dumps(rows, indent=2), encoding='utf-8')

with (DATA_DIR / 'prev_winner_forensics.csv').open('w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
    if rows:
        w.writeheader()
        w.writerows(rows)

# Aggregate stats.
n = len(records)
mismatch_count = sum(1 for r in records if r.baseline_mismatch_flag)
semantic_count = sum(1 for r in records if r.semantic_drift_flag)
provenance_count = sum(1 for r in records if r.provenance_risk_flag)
missing_swarm = sum(1 for r in records if r.swarm_speedup is None)

flip_count = sum(1 for r in records if r.delta_same_baseline is not None and r.delta_same_baseline > 0)

reason_counts = Counter(r.reason_category for r in records)
principle_counts = Counter(r.prev_principle_key for r in records)

principle_breakdown = {}
for key in sorted(principle_counts):
    group = [r for r in records if r.prev_principle_key == key]
    principle_breakdown[key] = {
        'count': len(group),
        'avg_prev_speedup_stored': round(mean([r.prev_speedup_stored for r in group if r.prev_speedup_stored is not None]), 4) if any(r.prev_speedup_stored is not None for r in group) else None,
        'avg_prev_speedup_recomputed': round(mean([r.prev_speedup_recomputed for r in group if r.prev_speedup_recomputed is not None]), 4) if any(r.prev_speedup_recomputed is not None for r in group) else None,
        'avg_swarm_speedup': round(mean([r.swarm_speedup for r in group if r.swarm_speedup is not None]), 4) if any(r.swarm_speedup is not None for r in group) else None,
        'explored_assignment': sum(1 for r in group if r.explored_assignment),
        'explored_reanalyze': sum(1 for r in group if r.explored_reanalyze),
        'explored_sql': sum(1 for r in group if r.explored_sql),
        'baseline_mismatch': sum(1 for r in group if r.baseline_mismatch_flag),
        'semantic_drift_flagged': sum(1 for r in group if r.semantic_drift_flag),
    }

summary = {
    'analysis_timestamp_utc': __import__('datetime').datetime.utcnow().isoformat() + 'Z',
    'comparison_updated_at': comparison.get('updated_at'),
    'total_prev_winner_queries': n,
    'baseline_mismatch_flagged': mismatch_count,
    'semantic_drift_flagged': semantic_count,
    'provenance_risk_flagged': provenance_count,
    'missing_swarm_results': missing_swarm,
    'same_baseline_flip_count': flip_count,
    'reason_counts': dict(reason_counts),
    'principle_counts': dict(principle_counts),
    'principle_breakdown': principle_breakdown,
}

(DATA_DIR / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')

# Top regression tables.
def stored_delta_value(d):
    if d is None or d == 'N/A':
        return None
    try:
        return float(str(d).replace('x', '').replace('+', ''))
    except ValueError:
        return None

stored_rank = sorted(
    [r for r in records if stored_delta_value(r.delta_stored) is not None],
    key=lambda r: stored_delta_value(r.delta_stored)
)

with (DATA_DIR / 'top_stored_regressions.csv').open('w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['query_id', 'delta_stored', 'prev_speedup_stored', 'swarm_speedup', 'reason_category'])
    for r in stored_rank[:25]:
        w.writerow([r.query_id, r.delta_stored, r.prev_speedup_stored, r.swarm_speedup, r.reason_category])

same_rank = sorted(
    [r for r in records if r.delta_same_baseline is not None],
    key=lambda r: r.delta_same_baseline
)

with (DATA_DIR / 'top_same_baseline_regressions.csv').open('w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['query_id', 'delta_same_baseline', 'prev_speedup_recomputed', 'swarm_speedup', 'reason_category'])
    for r in same_rank[:25]:
        w.writerow([r.query_id, round(r.delta_same_baseline, 6), round(r.prev_speedup_recomputed, 6), round(r.swarm_speedup, 6), r.reason_category])

# Build markdown report.
report = []
report.append('# Swarm Forensics Report: Why New Swarm Did Not Beat Previous Efforts')
report.append('')
report.append(f"- Comparison source: `packages/qt-sql/ado/benchmarks/duckdb_tpcds/swarm_comparison.json` ({comparison.get('updated_at')})")
report.append('- Scope: queries where comparison winner is `prev` (previous effort outperformed swarm).')
report.append('- Artifact set generated in this folder: copied SQL + per-query forensic tables + root-cause summary.')
report.append('')
report.append('## Executive Findings')
report.append('')
report.append(f'- Previous efforts won **{n}** queries in this batch comparison.')
report.append(f'- **{mismatch_count}/{n}** prev-winning queries have significant speedup comparability mismatch (`|prev_speedup - baseline/prev_ms| >= 0.25`).')
report.append(f'- **{flip_count}/{n}** prev-winning queries would flip to swarm wins if previous speedup is recomputed on the same baseline used for swarm.')
report.append(f'- **{semantic_count}/{n}** prev-winning queries show semantic-drift risk in previous SQL (literal/measure-column drift heuristics).')
report.append(f'- **{provenance_count}/{n}** prev-winning queries come from weaker provenance labels (`unvalidated`, `state_0`, `analyst_mode`).')
report.append(f'- **{missing_swarm}/{n}** prev-winning queries had no valid swarm candidate, so previous best stood by default.')
report.append('')

if records:
    gaps = [abs(r.prev_speedup_gap) for r in records if r.prev_speedup_gap is not None]
    report.append(f"- Prev speedup mismatch magnitude: mean={mean(gaps):.3f}x, median={median(gaps):.3f}x, max={max(gaps):.3f}x.")

report.append('')
report.append('## Why Swarm Lost (Root Causes)')
report.append('')
for k, c in reason_counts.most_common():
    report.append(f'- `{k}`: {c} queries')
report.append('')
report.append('Primary interpretation:')
report.append('- A large portion of the gap comes from **non-comparable historical speedups** and **legacy SQL quality/provenance issues** in previous winners, not purely from swarm search failure.')
report.append('- Where comparison is fair, swarm still misses or under-implements certain prior principles on some queries (especially `decorrelate`, `or_to_union`, and unlabeled structural rewrites).')
report.append('')

report.append('## Principle Coverage (Prev-Winner Side)')
report.append('')
report.append('| Principle Key | Count | Avg Prev Stored | Avg Prev Recomputed | Avg Swarm | Explored in Assignments | Explored in Reanalyze | Explored in Worker SQL | Baseline Mismatch | Semantic Drift Flagged |')
report.append('|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|')
for key, vals in principle_breakdown.items():
    report.append(
        f"| `{key}` | {vals['count']} | {vals['avg_prev_speedup_stored']} | {vals['avg_prev_speedup_recomputed']} | {vals['avg_swarm_speedup']} | {vals['explored_assignment']} | {vals['explored_reanalyze']} | {vals['explored_sql']} | {vals['baseline_mismatch']} | {vals['semantic_drift_flagged']} |"
    )
report.append('')

report.append('## Top 20 Losses by Stored Delta (as shown in swarm_comparison)')
report.append('')
report.append('| Query | Stored Delta | Prev Stored | Prev Recomputed | Swarm | Reason |')
report.append('|---|---:|---:|---:|---:|---|')
for r in stored_rank[:20]:
    report.append(
        f"| `{r.query_id}` | {r.delta_stored} | {r.prev_speedup_stored:.4f}x | {(r.prev_speedup_recomputed if r.prev_speedup_recomputed is not None else float('nan')):.4f}x | {(r.swarm_speedup if r.swarm_speedup is not None else float('nan')):.4f}x | `{r.reason_category}` |"
    )
report.append('')

report.append('## Top 20 Losses on Same-Baseline Delta (Swarm - Recomputed Prev)')
report.append('')
report.append('| Query | Same-Baseline Delta | Prev Recomputed | Swarm | Reason |')
report.append('|---|---:|---:|---:|---|')
for r in same_rank[:20]:
    report.append(
        f"| `{r.query_id}` | {r.delta_same_baseline:.4f}x | {r.prev_speedup_recomputed:.4f}x | {r.swarm_speedup:.4f}x | `{r.reason_category}` |"
    )
report.append('')

report.append('## Query-by-Query Forensic Ledger (Prev Winners)')
report.append('')
report.append('| Query | Prev Principle | Explored Where | Why Not | Stored Prev | Recomp Prev | Swarm | Baseline Mismatch | Semantic Drift | Provenance Risk | SQL Evidence |')
report.append('|---|---|---|---|---:|---:|---:|---|---|---|---|')
for r in sorted(records, key=lambda x: x.query_id):
    prev_sql_ref = f"`packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/{r.query_id}.sql`"
    swarm_sql_ref = f"`packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/{r.query_id}.sql`" if r.swarm_sql_path else '`(none)`'
    report.append(
        f"| `{r.query_id}` | `{r.prev_principle_key}` | `{r.explored_where}` | {r.reason_detail} | "
        f"{r.prev_speedup_stored:.4f}x | "
        f"{(r.prev_speedup_recomputed if r.prev_speedup_recomputed is not None else float('nan')):.4f}x | "
        f"{(r.swarm_speedup if r.swarm_speedup is not None else float('nan')):.4f}x | "
        f"{'yes' if r.baseline_mismatch_flag else 'no'} | "
        f"{'yes' if r.semantic_drift_flag else 'no'} | "
        f"{'yes' if r.provenance_risk_flag else 'no'} | "
        f"prev: {prev_sql_ref}<br>swarm: {swarm_sql_ref} |"
    )
report.append('')

report.append('## Generated Artifacts')
report.append('')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/data/summary.json`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/data/prev_winner_forensics.csv`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/data/prev_winner_forensics.json`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/data/top_stored_regressions.csv`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/data/top_same_baseline_regressions.csv`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/sql/prev_winners/*.sql`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/sql/swarm_best/*.sql`')
report.append('- `packages/qt-sql/ado/swarm_forensics_20260208/sql/original/*.sql`')

(OUT / 'report.md').write_text('\n'.join(report) + '\n', encoding='utf-8')

print(f'Wrote {OUT / "report.md"}')
print(f'Wrote {DATA_DIR / "summary.json"}')
print(f'Copied prev SQL files: {len(list(SQL_PREV.glob("*.sql")))}')
print(f'Copied swarm SQL files: {len(list(SQL_SWARM.glob("*.sql")))}')
print(f'Copied original SQL files: {len(list(SQL_ORIG.glob("*.sql")))}')
