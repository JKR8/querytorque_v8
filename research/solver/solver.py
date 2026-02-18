"""
General TPC-DS Boundary Solver

Automatically extracts joins + predicates from any TPC-DS query via sqlglot,
builds a Z3 model, solves for MIN/MAX boundary witness rows, and verifies
in DuckDB.

Usage:
    python solver.py              # run all Q1-Q99
    python solver.py 3 7 12       # run specific queries
    python solver.py 3-20         # run a range
"""

import re
import sys
import time
from collections import defaultdict

import duckdb
import z3
import sqlglot
from sqlglot import exp

MAX_SK = 2**31 - 1


# ==========================================================
# 1. TPC-DS environment (schema + queries from DuckDB)
# ==========================================================

def init_tpcds():
    """Load schema, queries, and sample defaults from DuckDB tpcds extension."""
    con = duckdb.connect(':memory:')
    con.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01)")

    # Schema: {table_name: {col_name: data_type}}
    schema = {}
    for tbl, col, dtype in con.execute(
        "SELECT table_name, column_name, data_type "
        "FROM information_schema.columns"
    ).fetchall():
        schema.setdefault(tbl, {})[col] = dtype

    # Sample row per table for column defaults
    defaults = {}
    for tbl in schema:
        try:
            row = con.execute(f"SELECT * FROM {tbl} LIMIT 1").fetchone()
            if row:
                for i, col in enumerate(schema[tbl]):
                    defaults[(tbl, col)] = row[i]
        except Exception:
            pass

    # Queries
    queries = {}
    for nr, sql in con.execute("SELECT query_nr, query FROM tpcds_queries()").fetchall():
        queries[int(nr)] = sql

    con.close()
    return schema, queries, defaults


# ==========================================================
# 2. SQL parsing — extract tables, joins, predicates
# ==========================================================

def parse_sql(sql, schema):
    """
    Parse a TPC-DS query and extract:
      - tables: {uid: (table_name, set_of_columns_used)}
      - joins: [(uid.col, uid.col)]
      - num_preds: [(uid.col, op, value)]  — numeric predicates
      - str_preds: {uid.col: string_value}  — string equality predicates
    """
    ast = sqlglot.parse_one(sql, dialect='duckdb')

    # --- Physical tables (skip CTE names) ---
    cte_names = {c.alias.lower() for c in ast.find_all(exp.CTE) if c.alias}

    tables = {}       # uid -> table_name
    table_cols = {}   # uid -> set(col_names)
    uid_counter = defaultdict(int)

    for t in ast.find_all(exp.Table):
        name = t.name.lower()
        if name not in schema or name in cte_names:
            continue
        alias = t.alias.lower() if t.alias else name
        # Deduplicate: if alias already used, number it
        if alias in tables:
            uid_counter[alias] += 1
            alias = f"{alias}_{uid_counter[alias]}"
        tables[alias] = name
        table_cols[alias] = set()

    # --- CTE column mappings (cte_alias.output_col -> uid.physical_col) ---
    cte_map = _build_cte_map(ast, tables, schema)

    # --- Walk ALL conditions in the entire AST ---
    joins = []
    num_preds = []
    str_preds = {}

    for cond in _collect_conditions(ast):
        _process_condition(cond, tables, table_cols, schema, cte_map,
                           joins, num_preds, str_preds)

    # --- Also pick up columns from SELECT / GROUP BY / ORDER BY ---
    for col_node in ast.find_all(exp.Column):
        ref = _resolve(col_node, tables, schema, cte_map)
        if ref:
            uid, col = ref.split('.', 1)
            table_cols[uid].add(col)

    return tables, table_cols, joins, num_preds, str_preds


def _build_cte_map(ast, tables, schema):
    """Map CTE output columns back to physical table columns."""
    cte_map = {}
    for cte_node in ast.find_all(exp.CTE):
        cte_name = cte_node.alias.lower() if cte_node.alias else ''
        select = cte_node.this
        if not isinstance(select, exp.Select):
            continue
        for expr in select.expressions:
            out_name = (expr.alias or '').lower() if hasattr(expr, 'alias') else ''
            source = expr
            if isinstance(expr, exp.Alias):
                out_name = expr.alias.lower()
                source = expr.this
            elif isinstance(expr, exp.Column):
                out_name = expr.name.lower()
            if not out_name or not isinstance(source, exp.Column):
                continue
            src_col = source.name.lower()
            src_tbl = (source.table or '').lower()
            for uid, tbl_name in tables.items():
                if src_tbl and not uid.startswith(src_tbl):
                    continue
                if src_col in schema.get(tbl_name, {}):
                    cte_map[f"{cte_name}.{out_name}"] = f"{uid}.{src_col}"
                    break
    return cte_map


def _collect_conditions(ast):
    """Collect all comparison conditions from WHERE, ON, HAVING."""
    conds = []
    for node in ast.find_all((exp.Where, exp.Having)):
        conds.extend(_flatten(node.this))
    for join_node in ast.find_all(exp.Join):
        on = join_node.args.get('on')
        if on:
            conds.extend(_flatten(on))
    return conds


def _flatten(e):
    if isinstance(e, exp.And):
        return _flatten(e.left) + _flatten(e.right)
    if isinstance(e, exp.Paren):
        return _flatten(e.this)
    return [e]


def _resolve(col_node, tables, schema, cte_map):
    """Resolve a Column node to 'uid.column' or None."""
    col = col_node.name.lower()
    tbl = (col_node.table or '').lower()

    # Direct match
    if tbl in tables and col in schema.get(tables[tbl], {}):
        return f"{tbl}.{col}"

    # CTE mapping
    cte_key = f"{tbl}.{col}"
    if cte_key in cte_map:
        return cte_map[cte_key]

    # Unqualified — scan all tables
    if not tbl:
        for uid, tbl_name in tables.items():
            if col in schema.get(tbl_name, {}):
                return f"{uid}.{col}"
    return None


def _process_condition(cond, tables, table_cols, schema, cte_map,
                       joins, num_preds, str_preds):
    """Extract a single condition into joins/num_preds/str_preds."""

    # --- EQ: col=col (join) or col=literal (predicate) ---
    if isinstance(cond, exp.EQ):
        left, right = cond.left, cond.right

        # col = col → join
        if isinstance(left, exp.Column) and isinstance(right, exp.Column):
            l = _resolve(left, tables, schema, cte_map)
            r = _resolve(right, tables, schema, cte_map)
            if l and r and l != r:
                joins.append((l, r))
                _track(l, table_cols)
                _track(r, table_cols)
            return

        # col = literal
        col_node, lit_node = None, None
        if isinstance(left, exp.Column):
            col_node, lit_node = left, right
        elif isinstance(right, exp.Column):
            col_node, lit_node = right, left

        if col_node:
            ref = _resolve(col_node, tables, schema, cte_map)
            if ref:
                val = _eval_expr(lit_node)
                if val is not None:
                    if isinstance(val, str):
                        str_preds[ref] = val
                    else:
                        num_preds.append((ref, 'eq', val))
                    _track(ref, table_cols)
        return

    # --- GT, GTE, LT, LTE ---
    op_map = {exp.GT: 'gt', exp.GTE: 'gte', exp.LT: 'lt', exp.LTE: 'lte'}
    for cls, op in op_map.items():
        if isinstance(cond, cls):
            if isinstance(cond.left, exp.Column):
                ref = _resolve(cond.left, tables, schema, cte_map)
                val = _eval_expr(cond.right)
                if ref and isinstance(val, (int, float)):
                    num_preds.append((ref, op, val))
                    _track(ref, table_cols)
            return

    # --- BETWEEN ---
    if isinstance(cond, exp.Between):
        col = cond.this
        if isinstance(col, exp.Column):
            ref = _resolve(col, tables, schema, cte_map)
            lo = _eval_expr(cond.args.get('low'))
            hi = _eval_expr(cond.args.get('high'))
            if ref:
                if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
                    num_preds.append((ref, 'gte', lo))
                    num_preds.append((ref, 'lte', hi))
                    _track(ref, table_cols)
                elif isinstance(lo, str) and isinstance(hi, str):
                    # DATE BETWEEN — store lower bound as string pred
                    str_preds[ref] = lo
                    _track(ref, table_cols)
        return

    # --- IN (literal list) ---
    if isinstance(cond, exp.In):
        col = cond.this
        if isinstance(col, exp.Column):
            ref = _resolve(col, tables, schema, cte_map)
            if ref:
                vals = [_eval_expr(e) for e in cond.expressions if isinstance(e, exp.Literal)]
                int_vals = [v for v in vals if isinstance(v, (int, float))]
                str_vals = [v for v in vals if isinstance(v, str)]
                if int_vals:
                    num_preds.append((ref, 'in', int_vals))
                    _track(ref, table_cols)
                elif str_vals:
                    str_preds[ref] = str_vals[0]  # pick first for boundary
                    _track(ref, table_cols)


def _eval_expr(node):
    """Try to evaluate an expression to a Python literal."""
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        if node.is_int:
            return int(node.this)
        if node.is_number:
            return float(node.this)
        return str(node.this)
    if isinstance(node, exp.Cast):
        # CAST('1999-02-22' AS date) → '1999-02-22'
        return _eval_expr(node.this)
    if isinstance(node, exp.Neg):
        inner = _eval_expr(node.this)
        if isinstance(inner, (int, float)):
            return -inner
    if isinstance(node, exp.Add):
        l, r = _eval_expr(node.left), _eval_expr(node.right)
        if isinstance(l, (int, float)) and isinstance(r, (int, float)):
            return l + r
    if isinstance(node, exp.Sub):
        l, r = _eval_expr(node.left), _eval_expr(node.right)
        if isinstance(l, (int, float)) and isinstance(r, (int, float)):
            return l - r
    if isinstance(node, exp.Mul):
        l, r = _eval_expr(node.left), _eval_expr(node.right)
        if isinstance(l, (int, float)) and isinstance(r, (int, float)):
            return l * r
    return None


def _track(ref, table_cols):
    parts = ref.split('.', 1)
    if len(parts) == 2 and parts[0] in table_cols:
        table_cols[parts[0]].add(parts[1])


# ==========================================================
# 3. Z3 model builder + solver
# ==========================================================

def solve_boundary(tables, table_cols, joins, num_preds, schema, direction):
    """
    Build Z3 model and solve for min or max boundary.
    Returns {uid.col: value} or None.
    """
    opt = z3.Optimize()
    z3v = {}  # 'uid.col' -> z3 var

    # Create variables
    for uid, tbl_name in tables.items():
        for col in table_cols.get(uid, set()):
            key = f"{uid}.{col}"
            dtype = schema.get(tbl_name, {}).get(col, '') or ''
            upper = dtype.upper()

            if any(t in upper for t in ('INT', 'BIGINT')):
                z3v[key] = z3.Int(key)
                opt.add(z3v[key] >= 1)
                if direction == 'max':
                    opt.add(z3v[key] <= MAX_SK)
            elif any(t in upper for t in ('DECIMAL', 'NUMERIC', 'DOUBLE', 'FLOAT')):
                z3v[key] = z3.Real(key)
                opt.add(z3v[key] >= -99999)
                opt.add(z3v[key] <= 99999)
            # Skip VARCHAR/DATE/etc — handled as string preds

    if not z3v:
        return None

    # Add join constraints
    for left, right in joins:
        if left in z3v and right in z3v:
            opt.add(z3v[left] == z3v[right])

    # Add numeric predicates
    for ref, op, val in num_preds:
        if ref not in z3v:
            continue
        v = z3v[ref]
        if op == 'eq':
            opt.add(v == val)
        elif op == 'gt':
            opt.add(v > val)
        elif op == 'gte':
            opt.add(v >= val)
        elif op == 'lt':
            opt.add(v < val)
        elif op == 'lte':
            opt.add(v <= val)
        elif op == 'in':
            opt.add(z3.Or([v == x for x in val]))

    # Combined objective
    terms = []
    for key, var in z3v.items():
        if z3.is_int(var):
            terms.append(z3.ToReal(var))
        else:
            terms.append(var)

    if direction == 'min':
        opt.minimize(z3.Sum(terms))
    else:
        opt.maximize(z3.Sum(terms))

    if opt.check() != z3.sat:
        return None

    model = opt.model()
    result = {}
    for key, var in z3v.items():
        v = model[var]
        if v is None:
            result[key] = 1
        elif hasattr(v, 'numerator_as_long'):
            n, d = v.numerator_as_long(), v.denominator_as_long()
            result[key] = n / d if d != 1 else n
        else:
            result[key] = v.as_long()
    return result


def solve_negative_witnesses(tables, table_cols, joins, num_preds, str_preds, schema):
    """
    For each predicate, solve for a witness that violates ONLY that predicate
    while satisfying all joins and other predicates. These 'near-miss' rows
    detect when a rewrite drops or weakens a predicate.

    Returns list of (witness_dict, modified_str_preds).
    """
    negatives = []

    # Negate numeric predicates one at a time
    for i in range(len(num_preds)):
        ref, op, val = num_preds[i]

        if op == 'eq':
            step = 1 if isinstance(val, int) else 0.01
            neg = [(ref, 'eq', val + step)]
        elif op == 'gt':
            # col > val → col = val (at boundary, fails >)
            neg = [(ref, 'eq', val)]
        elif op == 'gte':
            step = 1 if isinstance(val, int) else 0.01
            neg = [(ref, 'eq', val - step)]
        elif op == 'lt':
            neg = [(ref, 'eq', val)]
        elif op == 'lte':
            step = 1 if isinstance(val, int) else 0.01
            neg = [(ref, 'eq', val + step)]
        elif op == 'in':
            neg = [(ref, 'eq', max(val) + 1)]
        else:
            continue

        modified = num_preds[:i] + neg + num_preds[i+1:]
        w = solve_boundary(tables, table_cols, joins, modified, schema, 'min')
        if w:
            negatives.append((w, dict(str_preds)))

    # Negate string predicates one at a time
    for ref in str_preds:
        neg_sp = dict(str_preds)
        val = str_preds[ref]
        # Use a value that won't match the original predicate
        if re.match(r'\d{4}-\d{2}-\d{2}', val):
            neg_sp[ref] = '1900-01-01'  # outside TPC-DS date range
        else:
            neg_sp[ref] = 'ZZ'  # short, won't match any TPC-DS string pred
        w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'min')
        if w:
            negatives.append((w, neg_sp))

    return negatives


# ==========================================================
# 4. DuckDB verification
# ==========================================================

def _create_witness_db(tables, witness, str_preds, schema, defaults):
    """Create a DuckDB in-memory database populated with witness data."""
    con = duckdb.connect(':memory:')
    created = set()
    for uid, tbl_name in tables.items():
        if tbl_name in created or tbl_name not in schema:
            continue
        cols = [f'"{c}" {t}' for c, t in schema[tbl_name].items()]
        try:
            con.execute(f'CREATE TABLE "{tbl_name}" ({", ".join(cols)})')
            created.add(tbl_name)
        except Exception:
            pass

    for uid, tbl_name in tables.items():
        if tbl_name not in schema:
            continue
        all_cols = list(schema[tbl_name].keys())
        values = []
        for col in all_cols:
            key = f"{uid}.{col}"
            if key in witness:
                values.append(witness[key])
            elif key in str_preds:
                values.append(str_preds[key])
            elif (tbl_name, col) in defaults:
                values.append(defaults[(tbl_name, col)])
            else:
                values.append(None)
        placeholders = ', '.join(['?' for _ in all_cols])
        col_list = ', '.join(f'"{c}"' for c in all_cols)
        try:
            con.execute(f'INSERT INTO "{tbl_name}" ({col_list}) VALUES ({placeholders})', values)
        except Exception:
            pass
    return con


def _run_on_db(con, sql):
    """Run query on witness DB, return result rows or None on error."""
    try:
        return con.execute(sql).fetchall()
    except Exception:
        return None


def _results_differ(r1, r2):
    """
    Compare two query result sets robustly.
    Returns True if they differ semantically.
    - Returns False if either is None (execution error = inconclusive, not a diff)
    - Sorts rows to ignore ORDER BY differences
    - Compares values by position (ignores column names)
    """
    if r1 is None or r2 is None:
        return False  # execution error is inconclusive, not evidence of NEQ
    if len(r1) != len(r2):
        return True
    if not r1:
        return False  # both empty
    # Sort rows for order-independent comparison
    try:
        s1 = sorted(r1, key=lambda r: tuple(str(x) for x in r))
        s2 = sorted(r2, key=lambda r: tuple(str(x) for x in r))
        return s1 != s2
    except TypeError:
        # Fallback: compare as-is (some types not sortable)
        return r1 != r2


def verify(sql, tables, table_cols, witness, str_preds, schema, defaults):
    """Insert witness data into DuckDB and run the query."""
    if not witness:
        return False
    con = _create_witness_db(tables, witness, str_preds, schema, defaults)
    result = _run_on_db(con, sql)
    con.close()
    return result is not None and len(result) > 0


# ==========================================================
# 5. Semantic mutation testing
# ==========================================================

def _extract_original_sql(session_dir):
    """Extract original SQL from analyst_prompt.txt in a swarm session."""
    import os
    prompt_file = os.path.join(session_dir, 'iteration_00_fan_out', 'analyst_prompt.txt')
    if not os.path.exists(prompt_file):
        return None
    with open(prompt_file) as f:
        text = f.read()
    m = re.search(r'```sql\s*\n(.*?)```', text, re.DOTALL)
    if not m:
        return None
    raw = m.group(1)
    lines = []
    for line in raw.split('\n'):
        m2 = re.match(r'\s*\d+\s*\|\s?(.*)', line)
        lines.append(m2.group(1) if m2 else line)
    return '\n'.join(lines).strip()


def load_benchmark_rewrites(benchmark_dir):
    """
    Load all real LLM-generated rewrites from swarm session benchmark.
    Returns {query_num: (original_sql, [(worker_id, status, speedup, rewrite_sql), ...])}
    """
    import json
    import os

    sessions = {}  # qnum -> (orig_sql, [(wid, status, speedup, rw_sql)])
    if not os.path.isdir(benchmark_dir):
        return sessions

    for qdir in os.listdir(benchmark_dir):
        qpath = os.path.join(benchmark_dir, qdir)
        if not os.path.isdir(qpath):
            continue
        qnum_str = qdir.replace('query_', '').replace('q', '')
        try:
            qnum = int(qnum_str)
        except ValueError:
            continue

        orig_sql = _extract_original_sql(qpath)
        if not orig_sql:
            continue

        rewrites = []
        for root, _dirs, files in os.walk(qpath):
            if 'result.json' not in files or 'optimized.sql' not in files:
                continue
            try:
                with open(os.path.join(root, 'result.json')) as f:
                    result = json.load(f)
                with open(os.path.join(root, 'optimized.sql')) as f:
                    rewrite_sql = f.read().strip()
                if not rewrite_sql:
                    continue
                rewrites.append((
                    result.get('worker_id', 0),
                    result.get('status', 'UNKNOWN'),
                    result.get('speedup', 0),
                    rewrite_sql,
                ))
            except Exception:
                continue

        if rewrites:
            sessions[qnum] = (orig_sql, rewrites)

    return sessions


def test_real_rewrites(query_nums, schema, queries, defaults, benchmark_dir, verbose=False):
    """
    Test boundary witnesses against real benchmark rewrites.

    For each query where boundaries pass:
    - Run original query on witness data → orig_result
    - Run each real rewrite on witness data → rewrite_result
    - Compare: if results differ, boundary data detected the rewrite is non-equivalent

    Also validates ground truth on SF0.01 real data.

    Ground truth:
    - WIN/NEUTRAL/IMPROVED/REGRESSION = semantically correct (should match)
    - FAIL/ERROR = semantically wrong (should differ)
    """
    sessions = load_benchmark_rewrites(benchmark_dir)
    total_rewrites = sum(len(rw) for _, rw in sessions.values())
    print(f"Loaded {total_rewrites} rewrites across {len(sessions)} queries\n")

    # Accumulators — ground truth from SF0.01 comparison
    #   "truly_correct" = exact match on SF0.01 real data
    #   "truly_wrong"   = different results on SF0.01 real data
    tp = 0   # truly wrong AND boundary caught it
    tn = 0   # truly correct AND boundary says same
    fp = 0   # truly correct BUT boundary says different
    fn = 0   # truly wrong BUT boundary missed it

    queries_tested = 0
    queries_skipped = 0

    # SF0.01 real data for ground truth
    con_real = duckdb.connect(':memory:')
    con_real.execute("INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0.01)")

    for qn in sorted(query_nums):
        if qn not in sessions:
            continue
        orig_sql, q_rewrites = sessions[qn]

        # Parse original for Z3
        try:
            tables, table_cols, joins, num_preds, str_preds = parse_sql(orig_sql, schema)
        except Exception:
            continue
        if not tables:
            continue

        min_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'min')
        max_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'max')

        min_ok = verify(orig_sql, tables, table_cols, min_w, str_preds, schema, defaults) if min_w else False
        max_ok = verify(orig_sql, tables, table_cols, max_w, str_preds, schema, defaults) if max_w else False

        # Negative witnesses: one per predicate, violates ONLY that predicate
        neg_witnesses = solve_negative_witnesses(
            tables, table_cols, joins, num_preds, str_preds, schema
        )

        if not min_ok and not max_ok and not neg_witnesses:
            queries_skipped += 1
            continue

        queries_tested += 1

        # Get original results on positive witness DBs
        orig_min = orig_max = None
        if min_ok:
            con = _create_witness_db(tables, min_w, str_preds, schema, defaults)
            orig_min = _run_on_db(con, orig_sql)
            con.close()
        if max_ok:
            con = _create_witness_db(tables, max_w, str_preds, schema, defaults)
            orig_max = _run_on_db(con, orig_sql)
            con.close()

        # Pre-compute original results on each negative witness DB
        neg_orig_results = []
        for neg_w, neg_sp in neg_witnesses:
            con = _create_witness_db(tables, neg_w, neg_sp, schema, defaults)
            neg_orig_results.append(_run_on_db(con, orig_sql))
            con.close()

        # Ground truth: original on real SF0.01
        try:
            orig_real = con_real.execute(orig_sql).fetchall()
        except Exception:
            continue

        q_tp = q_tn = q_fp = q_fn = 0

        for wid, status, speedup, rewrite_sql in q_rewrites:
            bench_status = status
            if bench_status not in ('WIN', 'NEUTRAL', 'IMPROVED', 'REGRESSION',
                                     'FAIL', 'ERROR'):
                continue

            # Ground truth: compare on real data
            try:
                rw_real = con_real.execute(rewrite_sql).fetchall()
                truly_correct = (orig_real == rw_real)
            except Exception:
                truly_correct = False

            # Positive boundary test: compare on MIN/MAX witness data
            diff_min = False
            if min_ok and orig_min is not None:
                con = _create_witness_db(tables, min_w, str_preds, schema, defaults)
                rw_rows = _run_on_db(con, rewrite_sql)
                con.close()
                diff_min = (rw_rows is None) or (orig_min != rw_rows)

            diff_max = False
            if max_ok and orig_max is not None:
                con = _create_witness_db(tables, max_w, str_preds, schema, defaults)
                rw_rows = _run_on_db(con, rewrite_sql)
                con.close()
                diff_max = (rw_rows is None) or (orig_max != rw_rows)

            # Negative witness test: detect dropped/weakened predicates
            diff_neg = False
            if not (diff_min or diff_max):
                for j, (neg_w, neg_sp) in enumerate(neg_witnesses):
                    con = _create_witness_db(tables, neg_w, neg_sp, schema, defaults)
                    rw_neg = _run_on_db(con, rewrite_sql)
                    con.close()
                    if neg_orig_results[j] != rw_neg:
                        diff_neg = True
                        break

            boundary_detected = diff_min or diff_max or diff_neg

            if truly_correct and not boundary_detected:
                tn += 1; q_tn += 1
            elif truly_correct and boundary_detected:
                fp += 1; q_fp += 1
            elif not truly_correct and boundary_detected:
                tp += 1; q_tp += 1
            elif not truly_correct and not boundary_detected:
                fn += 1; q_fn += 1

            if verbose:
                gt = 'CORRECT' if truly_correct else 'WRONG'
                bd = 'CAUGHT' if boundary_detected else 'SAME'
                ok = '✓' if (truly_correct == (not boundary_detected)) else '✗'
                sp_str = f"{speedup:.2f}x" if speedup else "---"
                print(f"    {ok} W{wid} {bench_status:10s} {sp_str:>8s}  "
                      f"real:{gt:7s}  boundary:{bd}")

        total_q = q_tp + q_tn + q_fp + q_fn
        neg_str = f" neg:{len(neg_witnesses)}" if neg_witnesses else ""
        print(f"  Q{qn:02d}: {total_q} rewrites | "
              f"TP:{q_tp} TN:{q_tn} FP:{q_fp} FN:{q_fn}{neg_str}")

    con_real.close()

    # Summary — confusion matrix
    total = tp + tn + fp + fn
    print(f"\n{'='*60}")
    print(f"BOUNDARY WITNESS vs REAL BENCHMARK REWRITES")
    print(f"{'='*60}")
    print(f"Queries tested:  {queries_tested}  (skipped {queries_skipped} — no boundary)")
    print(f"Total rewrites:  {total}")
    print(f"")
    print(f"--- CONFUSION MATRIX (ground truth = SF0.01 exact match) ---")
    print(f"                    Boundary:SAME    Boundary:DIFF")
    print(f"  Really correct:   TN={tn:<5d}         FP={fp}")
    print(f"  Really wrong:     FN={fn:<5d}         TP={tp}")
    print(f"")
    if total:
        accuracy = (tp + tn) / total * 100
        print(f"  ACCURACY:     {tp+tn}/{total} ({accuracy:.1f}%)")
    if tp + fn:
        recall = tp / (tp + fn) * 100
        print(f"  RECALL:       {tp}/{tp+fn} ({recall:.1f}%)  — of wrong rewrites, how many caught?")
    if tp + fp:
        precision = tp / (tp + fp) * 100
        print(f"  PRECISION:    {tp}/{tp+fp} ({precision:.1f}%)  — of flagged rewrites, how many actually wrong?")
    if tn + fp:
        specificity = tn / (tn + fp) * 100
        print(f"  SPECIFICITY:  {tn}/{tn+fp} ({specificity:.1f}%)  — of correct rewrites, how many pass?")
    print(f"{'='*60}")


# ==========================================================
# 6. VeriEQL benchmark testing
# ==========================================================

def _defaults_from_schema(schema):
    """Generate unique defaults per column so column reordering is detectable."""
    defaults = {}
    int_counter = 1
    str_counter = 0
    real_counter = 1.0
    for tbl, cols in schema.items():
        for col, dtype in cols.items():
            if not dtype:
                defaults[(tbl, col)] = int_counter
                int_counter += 1
                continue
            dt = dtype.upper()
            if any(t in dt for t in ('INT', 'BIGINT', 'SMALLINT', 'TINYINT')):
                defaults[(tbl, col)] = int_counter
                int_counter += 1
            elif any(t in dt for t in ('DECIMAL', 'NUMERIC', 'DOUBLE', 'FLOAT', 'REAL')):
                defaults[(tbl, col)] = real_counter
                real_counter += 1.0
            elif 'BOOL' in dt:
                defaults[(tbl, col)] = True
            elif 'DATE' in dt or 'TIME' in dt:
                defaults[(tbl, col)] = '2000-01-01'
            else:
                # Unique string per column
                defaults[(tbl, col)] = chr(65 + (str_counter % 26))
                str_counter += 1
    return defaults


def _test_pair_on_witnesses(q1, q2, tables, table_cols, joins, num_preds,
                            str_preds, schema, defaults):
    """
    Solve boundaries for q1, then run both q1 and q2 on witness data.
    Returns True if a difference is detected (non-equivalent), False otherwise,
    or None if no usable witnesses.
    """
    min_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'min')
    max_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'max')

    min_ok = verify(q1, tables, table_cols, min_w, str_preds, schema, defaults) if min_w else False
    max_ok = verify(q1, tables, table_cols, max_w, str_preds, schema, defaults) if max_w else False

    neg_witnesses = solve_negative_witnesses(
        tables, table_cols, joins, num_preds, str_preds, schema
    )

    if not min_ok and not max_ok and not neg_witnesses:
        return None

    # Test positive witnesses
    if min_ok:
        con = _create_witness_db(tables, min_w, str_preds, schema, defaults)
        r1 = _run_on_db(con, q1)
        r2 = _run_on_db(con, q2)
        con.close()
        if _results_differ(r1, r2):
            return True

    if max_ok:
        con = _create_witness_db(tables, max_w, str_preds, schema, defaults)
        r1 = _run_on_db(con, q1)
        r2 = _run_on_db(con, q2)
        con.close()
        if _results_differ(r1, r2):
            return True

    # Test negative witnesses
    for neg_w, neg_sp in neg_witnesses:
        con = _create_witness_db(tables, neg_w, neg_sp, schema, defaults)
        r1 = _run_on_db(con, q1)
        r2 = _run_on_db(con, q2)
        con.close()
        if _results_differ(r1, r2):
            return True

    return False


def test_verieql_benchmark(benchmark_path, results_path, limit=0, verbose=False):
    """
    Test our boundary witness solver against VeriEQL benchmark pairs.

    Ground truth:
      - VeriEQL NEQ → pair is NOT equivalent
      - VeriEQL has_EQU (no NEQ) → pair is equivalent
      - VeriEQL NSE/TMO only → skip (unknown)
    """
    import json, os

    # Load benchmark pairs AND VeriEQL results side-by-side (matched by line number)
    # The 'index' field is NOT unique — must use line position for mapping.
    pairs = []
    verdicts = []  # line_number -> ('NEQ' | 'EQU' | 'SKIP')
    with open(benchmark_path) as bf, open(results_path) as rf:
        for bf_line, rf_line in zip(bf, rf):
            pairs.append(json.loads(bf_line))
            r = json.loads(rf_line)
            states = r.get('states', [])
            if 'NEQ' in states:
                verdicts.append('NEQ')
            elif any(s == 'EQU' for s in states):
                verdicts.append('EQU')
            else:
                verdicts.append('SKIP')

    neq_total = verdicts.count('NEQ')
    equ_total = verdicts.count('EQU')
    skip_total = verdicts.count('SKIP')
    print(f"Loaded {len(pairs)} pairs — VeriEQL verdicts: "
          f"NEQ={neq_total} EQU={equ_total} SKIP={skip_total}")

    if limit:
        pairs = pairs[:limit]
        print(f"Testing first {limit} pairs")
    print()

    tp = tn = fp = fn = 0
    unsupported = 0
    skipped_verdict = 0

    t_start = time.time()

    for i, entry in enumerate(pairs):
        verdict = verdicts[i]

        if verdict == 'SKIP':
            skipped_verdict += 1
            continue

        ground_truth_neq = (verdict == 'NEQ')

        # Normalize schema to lowercase
        raw_schema = entry.get('schema', {})
        schema = {}
        for tbl, cols in raw_schema.items():
            schema[tbl.lower()] = {c.lower(): t for c, t in cols.items()}

        defaults = _defaults_from_schema(schema)
        q1, q2 = entry['pair']

        # Parse Q1
        try:
            tables, table_cols, joins, num_preds, str_preds = parse_sql(q1, schema)
        except Exception:
            unsupported += 1
            continue

        if not tables:
            unsupported += 1
            continue

        # Test Q1→Q2 direction
        detected = _test_pair_on_witnesses(
            q1, q2, tables, table_cols, joins, num_preds,
            str_preds, schema, defaults
        )

        if detected is None:
            # No usable witnesses — try parsing Q2 as well
            try:
                t2, tc2, j2, np2, sp2 = parse_sql(q2, schema)
                if t2:
                    detected = _test_pair_on_witnesses(
                        q2, q1, t2, tc2, j2, np2, sp2, schema, defaults
                    )
            except Exception:
                pass

        if detected is None:
            unsupported += 1
            continue

        # Confusion matrix
        if ground_truth_neq and detected:
            tp += 1
        elif ground_truth_neq and not detected:
            fn += 1
        elif not ground_truth_neq and detected:
            fp += 1
        else:
            tn += 1

        if verbose and (ground_truth_neq != detected):
            label = 'FN' if ground_truth_neq else 'FP'
            print(f"  {label} #{idx}: {q1[:60]}...")

        # Progress
        done = tp + tn + fp + fn + unsupported + skipped_verdict
        if done % 500 == 0 and done > 0:
            elapsed = time.time() - t_start
            rate = done / elapsed
            remaining = (len(pairs) - done) / rate if rate else 0
            print(f"  ... {done}/{len(pairs)} ({elapsed:.0f}s, "
                  f"~{remaining:.0f}s remaining) "
                  f"TP:{tp} TN:{tn} FP:{fp} FN:{fn}")

    elapsed = time.time() - t_start

    # Summary
    total = tp + tn + fp + fn
    print(f"\n{'='*60}")
    print(f"BOUNDARY WITNESS vs VERIEQL BENCHMARK")
    print(f"{'='*60}")
    print(f"Pairs tested:     {total}  (unsupported: {unsupported}, "
          f"skipped: {skipped_verdict})")
    print(f"Time:             {elapsed:.1f}s  ({total/elapsed:.0f} pairs/sec)" if elapsed else "")
    print()
    print(f"--- CONFUSION MATRIX (ground truth = VeriEQL verdict) ---")
    print(f"                    Ours:SAME    Ours:DIFF")
    print(f"  VeriEQL EQU:      TN={tn:<6d}    FP={fp}")
    print(f"  VeriEQL NEQ:      FN={fn:<6d}    TP={tp}")
    print()
    if total:
        print(f"  ACCURACY:     {tp+tn}/{total} ({(tp+tn)/total*100:.1f}%)")
    if tp + fn:
        print(f"  RECALL:       {tp}/{tp+fn} ({tp/(tp+fn)*100:.1f}%)"
              f"  — of NEQ pairs, how many we caught?")
    if tp + fp:
        print(f"  PRECISION:    {tp}/{tp+fp} ({tp/(tp+fp)*100:.1f}%)"
              f"  — of pairs we flagged, how many actually NEQ?")
    if tn + fp:
        print(f"  SPECIFICITY:  {tn}/{tn+fp} ({tn/(tn+fp)*100:.1f}%)"
              f"  — of EQU pairs, how many we pass?")
    print(f"{'='*60}")


# ==========================================================
# 7. Main
# ==========================================================

def run_query(qn, sql, schema, defaults, verbose=False):
    """Solve one query. Returns (min_ok, max_ok, info_str)."""
    try:
        tables, table_cols, joins, num_preds, str_preds = parse_sql(sql, schema)
    except Exception as e:
        return False, False, f"parse error: {e}"

    if not tables:
        return False, False, "no physical tables"

    info = f"{len(tables)}T {len(joins)}J {len(num_preds)}P {len(str_preds)}S"

    t0 = time.time()
    min_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'min')
    max_w = solve_boundary(tables, table_cols, joins, num_preds, schema, 'max')
    solve_ms = (time.time() - t0) * 1000

    if min_w is None and max_w is None:
        return False, False, f"{info} | UNSAT"

    min_ok = verify(sql, tables, table_cols, min_w, str_preds, schema, defaults) if min_w else False
    max_ok = verify(sql, tables, table_cols, max_w, str_preds, schema, defaults) if max_w else False

    info += f" | {solve_ms:.0f}ms"
    if not min_ok and min_w:
        info += " | MIN verify fail"
    if not max_ok and max_w:
        info += " | MAX verify fail"
    if min_w is None:
        info += " | MIN UNSAT"
    if max_w is None:
        info += " | MAX UNSAT"

    if verbose and min_ok and min_w:
        print(f"  MIN witness: { {k: v for k, v in sorted(min_w.items())} }")
    if verbose and max_ok and max_w:
        print(f"  MAX witness: { {k: v for k, v in sorted(max_w.items())} }")

    return min_ok, max_ok, info


def main():
    args = sys.argv[1:]
    verbose = '-v' in args
    semantic = '--semantic' in args
    benchmark = '--benchmark' in args
    args = [a for a in args if a not in ('-v', '--semantic', '--benchmark')]

    # --benchmark mode: test against VeriEQL benchmark
    if benchmark:
        import os
        # Default to leetcode; accept 'calcite' or 'literature' as arg
        bench_name = args[0] if args else 'leetcode'
        limit = 0
        for a in args[1:]:
            if a.isdigit():
                limit = int(a)

        verieql_dir = '/mnt/d/VeriEQL'
        if bench_name == 'calcite':
            bp = os.path.join(verieql_dir, 'benchmarks/calcite/calcite2.jsonlines')
            rp = os.path.join(verieql_dir, 'experiments/2025_10_31/calcite.out')
        elif bench_name == 'literature':
            bp = os.path.join(verieql_dir, 'benchmarks/literature/literature.jsonlines')
            rp = os.path.join(verieql_dir, 'experiments/2025_10_31/literature.out')
        else:
            bp = os.path.join(verieql_dir, 'benchmarks/leetcode/leetcode.jsonlines')
            rp = os.path.join(verieql_dir, 'experiments/2025_10_31/leetcode.out')

        print(f"=== BOUNDARY WITNESS vs VeriEQL [{bench_name}] ===\n")
        test_verieql_benchmark(bp, rp, limit=limit, verbose=verbose)
        return

    # Parse query numbers
    query_nums = []
    if not args or args == ['all']:
        query_nums = list(range(1, 100))
    else:
        for a in args:
            if '-' in a:
                lo, hi = a.split('-', 1)
                query_nums.extend(range(int(lo), int(hi) + 1))
            else:
                query_nums.append(int(a))

    # --semantic mode needs TPC-DS
    if semantic:
        print("Loading TPC-DS schema and queries...")
        t0 = time.time()
        schema, queries, defaults = init_tpcds()
        print(f"  {len(schema)} tables, {len(queries)} queries loaded in {time.time()-t0:.1f}s\n")

        import os
        bench_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'packages', 'qt-sql', 'qt_sql', 'benchmarks',
            'duckdb_tpcds', 'swarm_sessions'
        )
        if not os.path.isdir(bench_dir):
            bench_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), '..', '..',
                'packages', 'qt-sql', 'qt_sql', 'benchmarks',
                'duckdb_tpcds', 'swarm_sessions'
            )
        print(f"=== BOUNDARY WITNESS vs REAL BENCHMARK REWRITES ===")
        print(f"Benchmark: {bench_dir}\n")
        test_real_rewrites(query_nums, schema, queries, defaults, bench_dir, verbose)
        return

    # Default: solve TPC-DS boundaries
    print("Loading TPC-DS schema and queries...")
    t0 = time.time()
    schema, queries, defaults = init_tpcds()
    print(f"  {len(schema)} tables, {len(queries)} queries loaded in {time.time()-t0:.1f}s\n")

    results = {}
    for qn in sorted(query_nums):
        if qn not in queries:
            continue
        min_ok, max_ok, info = run_query(qn, queries[qn], schema, defaults, verbose)
        results[qn] = (min_ok, max_ok, info)
        m = '✓' if min_ok else '✗'
        x = '✓' if max_ok else '✗'
        print(f"  Q{qn:02d}: MIN {m}  MAX {x}  [{info}]")

    # Summary
    total = len(results)
    both = sum(1 for m, x, _ in results.values() if m and x)
    either = sum(1 for m, x, _ in results.values() if m or x)
    print(f"\n{'='*60}")
    print(f"RESULTS: {both}/{total} both pass, {either}/{total} either pass")
    print(f"{'='*60}")


if __name__ == '__main__':
    main()
