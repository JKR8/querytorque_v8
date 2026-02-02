"""Diagnose rewrite failures on TPC-DS queries."""

import sys
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

import sqlglot
from qt_sql.analyzers.ast_detector import ASTDetector
from qt_sql.rewriters import get_rewriter_for_rule, get_coverage_stats

TPCDS_PATH = Path("/mnt/d/TPC-DS/queries_sf100")


def diagnose():
    """Diagnose rewrite failures."""
    detector = ASTDetector(dialect="duckdb")

    # Track failure reasons
    failure_reasons = defaultdict(list)
    success_by_rewriter = defaultdict(int)
    failure_by_rewriter = defaultdict(int)
    no_rewriter_rules = defaultdict(int)

    queries = sorted(TPCDS_PATH.glob("query_*.sql"))

    for query_path in queries:
        sql = query_path.read_text(encoding="utf-8")
        query_num = int(query_path.stem.split("_")[1])

        try:
            issues = detector.detect(sql)
        except Exception as e:
            continue

        for issue in issues:
            if issue.rule_id.startswith("SQL-PARSE"):
                continue

            rewriter = get_rewriter_for_rule(issue.rule_id)
            if not rewriter:
                no_rewriter_rules[issue.rule_id] += 1
                continue

            try:
                parsed = sqlglot.parse_one(sql, dialect="duckdb")
                result = rewriter.rewrite(parsed)

                if result.success:
                    success_by_rewriter[rewriter.rewriter_id] += 1
                else:
                    failure_by_rewriter[rewriter.rewriter_id] += 1
                    # Capture failure reason
                    reason = result.explanation or "No explanation"
                    failure_reasons[rewriter.rewriter_id].append({
                        "query": query_num,
                        "rule": issue.rule_id,
                        "reason": reason[:200],
                    })
            except Exception as e:
                failure_by_rewriter[rewriter.rewriter_id] += 1
                failure_reasons[rewriter.rewriter_id].append({
                    "query": query_num,
                    "rule": issue.rule_id,
                    "reason": f"Exception: {str(e)[:150]}",
                })

    # Print results
    print("=" * 70)
    print("REWRITE FAILURE DIAGNOSIS")
    print("=" * 70)

    print("\n## Rules Without Rewriters (detected but no auto-fix)")
    sorted_no_rewriter = sorted(no_rewriter_rules.items(), key=lambda x: -x[1])
    for rule_id, count in sorted_no_rewriter[:15]:
        print(f"  {rule_id:25s}: {count:4d} detections")

    print("\n## Rewriter Success/Failure Rates")
    all_rewriters = set(success_by_rewriter.keys()) | set(failure_by_rewriter.keys())
    rewriter_stats = []
    for rw_id in all_rewriters:
        success = success_by_rewriter.get(rw_id, 0)
        failure = failure_by_rewriter.get(rw_id, 0)
        total = success + failure
        rate = 100 * success / total if total else 0
        rewriter_stats.append((rw_id, success, failure, rate))

    # Sort by failure count descending
    rewriter_stats.sort(key=lambda x: -x[2])

    print(f"  {'Rewriter':<40s} {'Success':>8s} {'Fail':>8s} {'Rate':>8s}")
    print(f"  {'-'*40} {'-'*8} {'-'*8} {'-'*8}")
    for rw_id, success, failure, rate in rewriter_stats:
        print(f"  {rw_id:<40s} {success:>8d} {failure:>8d} {rate:>7.1f}%")

    print("\n## Failure Reasons by Rewriter")
    for rw_id, failures in sorted(failure_reasons.items(), key=lambda x: -len(x[1])):
        if len(failures) == 0:
            continue
        print(f"\n### {rw_id} ({len(failures)} failures)")

        # Group by reason
        reason_counts = defaultdict(int)
        for f in failures:
            reason_counts[f["reason"]] += 1

        for reason, count in sorted(reason_counts.items(), key=lambda x: -x[1])[:5]:
            print(f"  [{count:3d}x] {reason[:80]}")


if __name__ == "__main__":
    diagnose()
