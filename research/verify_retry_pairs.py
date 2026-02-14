"""Verify that retry original/optimized pairs are internally consistent.

For each retry batch, checks that the optimized SQL preserves all literals
and column references from its OWN original (not Stream 0).

This answers: "Are the retry rewrites valid optimizations of the queries
they were actually given?"
"""
import os
import sys
import glob
import sqlglot
from sqlglot import exp
from collections import defaultdict

RETRY_BASE = "research/archive/benchmark_results/retry_runs"
STREAM0_BASE = "packages/qt-sql/qt_sql/benchmarks/duckdb_tpcds/queries"
FIXTURES_BASE = "packages/qt-sql/tests/fixtures/tpcds"

def extract_literals(sql: str, dialect: str = "duckdb"):
    """Extract string and numeric literals from SQL using sqlglot AST."""
    strings = set()
    numbers = set()
    try:
        trees = sqlglot.parse(sql, dialect=dialect)
    except Exception:
        return None, None  # parse failure

    for tree in trees:
        if tree is None:
            continue
        for node in tree.walk():
            if isinstance(node, exp.Literal):
                if node.is_string:
                    val = node.this
                    if val and len(val) > 1:  # skip single chars
                        strings.add(val)
                elif node.is_number:
                    try:
                        num = float(node.this)
                        if num not in (0, 1, -1):  # skip trivial
                            numbers.add(num)
                    except (ValueError, TypeError):
                        pass
    return strings, numbers

def extract_column_refs(sql: str, dialect: str = "duckdb"):
    """Extract column references from SQL using sqlglot AST."""
    cols = set()
    try:
        trees = sqlglot.parse(sql, dialect=dialect)
    except Exception:
        return None

    for tree in trees:
        if tree is None:
            continue
        for node in tree.walk():
            if isinstance(node, exp.Column):
                col_name = node.name
                if col_name:
                    cols.add(col_name.lower())
    return cols

def check_pair(orig_sql: str, opt_sql: str):
    """Check if optimized SQL preserves all literals and columns from original."""
    orig_strings, orig_numbers = extract_literals(orig_sql)
    opt_strings, opt_numbers = extract_literals(opt_sql)

    if orig_strings is None or opt_strings is None:
        return {"status": "PARSE_ERROR", "details": "Could not parse SQL"}

    missing_strings = orig_strings - opt_strings
    missing_numbers = orig_numbers - opt_numbers

    orig_cols = extract_column_refs(orig_sql)
    opt_cols = extract_column_refs(opt_sql)

    if orig_cols is None or opt_cols is None:
        return {"status": "PARSE_ERROR", "details": "Could not parse columns"}

    missing_cols = orig_cols - opt_cols

    issues = []
    if missing_strings:
        issues.append(f"missing_strings: {sorted(missing_strings)}")
    if missing_numbers:
        issues.append(f"missing_numbers: {sorted(missing_numbers)}")
    if missing_cols:
        issues.append(f"missing_cols: {sorted(missing_cols)}")

    if issues:
        return {"status": "MISMATCH", "details": "; ".join(issues)}
    return {"status": "OK", "details": ""}

def check_stream_match(retry_orig_sql: str, stream0_sql: str):
    """Check if the retry original matches Stream 0 or is from a different stream."""
    r_strings, r_numbers = extract_literals(retry_orig_sql)
    s_strings, s_numbers = extract_literals(stream0_sql)

    if r_strings is None or s_strings is None:
        return "PARSE_ERROR"

    diff_strings = r_strings.symmetric_difference(s_strings)
    diff_numbers = r_numbers.symmetric_difference(s_numbers)

    if not diff_strings and not diff_numbers:
        return "SAME_STREAM"
    return f"DIFF_STREAM (strings: {len(diff_strings)}, numbers: {len(diff_numbers)})"

def main():
    os.chdir("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")

    batches = sorted(glob.glob(os.path.join(RETRY_BASE, "*")))

    results = defaultdict(list)
    stream_check = {}

    total = 0
    ok = 0
    mismatch = 0
    parse_err = 0

    for batch_dir in batches:
        batch_name = os.path.basename(batch_dir)
        query_dirs = sorted(glob.glob(os.path.join(batch_dir, "q*")))

        for qdir in query_dirs:
            qid = os.path.basename(qdir)
            qnum = qid.replace("q", "")

            orig_path = os.path.join(qdir, "original.sql")
            if not os.path.exists(orig_path):
                continue

            with open(orig_path) as f:
                orig_sql = f.read()

            # Check if this original matches Stream 0
            if qid not in stream_check:
                s0_path = os.path.join(STREAM0_BASE, f"query_{int(qnum):02d}.sql")
                if not os.path.exists(s0_path):
                    s0_path = os.path.join(STREAM0_BASE, f"query_{qnum}.sql")
                if os.path.exists(s0_path):
                    with open(s0_path) as f:
                        s0_sql = f.read()
                    stream_check[qid] = check_stream_match(orig_sql, s0_sql)
                else:
                    stream_check[qid] = "NO_STREAM0_FILE"

            # Check each worker's optimized SQL against THIS original
            opt_files = sorted(glob.glob(os.path.join(qdir, "w*_optimized.sql")))
            for opt_path in opt_files:
                worker = os.path.basename(opt_path).split("_")[0]  # w1, w2, etc.

                with open(opt_path) as f:
                    opt_sql = f.read()

                if not opt_sql.strip():
                    continue

                result = check_pair(orig_sql, opt_sql)
                total += 1

                if result["status"] == "OK":
                    ok += 1
                elif result["status"] == "MISMATCH":
                    mismatch += 1
                else:
                    parse_err += 1

                results[(batch_name, qid, worker)] = result

    # Print summary
    print("=" * 80)
    print("RETRY PAIR INTERNAL CONSISTENCY CHECK")
    print("=" * 80)
    print(f"\nTotal pairs checked: {total}")
    print(f"  OK (internally consistent): {ok}")
    print(f"  MISMATCH (LLM changed literals): {mismatch}")
    print(f"  PARSE_ERROR: {parse_err}")

    # Stream check
    print(f"\n{'=' * 80}")
    print("STREAM ANALYSIS (retry original vs Stream 0)")
    print("=" * 80)
    same = sum(1 for v in stream_check.values() if v == "SAME_STREAM")
    diff = sum(1 for v in stream_check.values() if v.startswith("DIFF"))
    print(f"  Same stream as Stream 0: {same}")
    print(f"  Different stream: {diff}")
    print(f"  No Stream 0 file: {sum(1 for v in stream_check.values() if v == 'NO_STREAM0_FILE')}")

    if diff > 0:
        print(f"\n  Queries from DIFFERENT stream:")
        for qid, status in sorted(stream_check.items(), key=lambda x: int(x[0].replace('q',''))):
            if status.startswith("DIFF"):
                print(f"    {qid}: {status}")

    # Print mismatches
    if mismatch > 0:
        print(f"\n{'=' * 80}")
        print("MISMATCHES (LLM actually changed literals in its rewrite)")
        print("=" * 80)
        for (batch, qid, worker), result in sorted(results.items()):
            if result["status"] == "MISMATCH":
                print(f"  {batch}/{qid}/{worker}: {result['details']}")

    # Print parse errors
    if parse_err > 0:
        print(f"\n{'=' * 80}")
        print("PARSE ERRORS")
        print("=" * 80)
        for (batch, qid, worker), result in sorted(results.items()):
            if result["status"] == "PARSE_ERROR":
                print(f"  {batch}/{qid}/{worker}")

    # Summary of recoverable pairs
    print(f"\n{'=' * 80}")
    print("RECOVERY ASSESSMENT")
    print("=" * 80)

    # For each DIFF_STREAM query, count OK pairs
    recoverable = defaultdict(list)
    for (batch, qid, worker), result in sorted(results.items()):
        if stream_check.get(qid, "").startswith("DIFF") and result["status"] == "OK":
            recoverable[qid].append(f"{batch}/{worker}")

    print(f"  Queries from different stream with internally consistent rewrites: {len(recoverable)}")
    for qid in sorted(recoverable.keys(), key=lambda x: int(x.replace('q',''))):
        print(f"    {qid}: {len(recoverable[qid])} valid rewrites ({', '.join(recoverable[qid][:4])})")

if __name__ == "__main__":
    main()
