#!/usr/bin/env python3
"""Find which TPC-DS queries match unverified gold example patterns."""

from pathlib import Path
import re

queries_dir = Path("/mnt/d/TPC-DS/queries_duckdb_converted")

print("Finding TPC-DS queries matching unverified patterns...\n")

# Pattern 1: flatten_subquery - IN subqueries
print("=== flatten_subquery (IN/EXISTS subqueries) ===")
in_queries = []
for f in sorted(queries_dir.glob("query_*.sql")):
    sql = f.read_text().lower()
    if " in (" in sql or " in(" in sql or "exists (" in sql:
        in_queries.append(f.stem.replace("query_", "Q"))
print(f"Found {len(in_queries)} queries: {', '.join(in_queries[:20])}")
if len(in_queries) > 20:
    print(f"... and {len(in_queries) - 20} more")
print()

# Pattern 2: inline_cte - Single-use CTEs
print("=== inline_cte (queries with CTEs) ===")
cte_queries = []
for f in sorted(queries_dir.glob("query_*.sql")):
    sql = f.read_text()
    if re.search(r'^\s*with\s+', sql, re.IGNORECASE | re.MULTILINE):
        cte_queries.append(f.stem.replace("query_", "Q"))
print(f"Found {len(cte_queries)} queries: {', '.join(cte_queries[:20])}")
if len(cte_queries) > 20:
    print(f"... and {len(cte_queries) - 20} more")
print()

# Pattern 3: remove_redundant - DISTINCT after GROUP BY
print("=== remove_redundant (DISTINCT + GROUP BY) ===")
redundant_queries = []
for f in sorted(queries_dir.glob("query_*.sql")):
    sql = f.read_text().lower()
    if "select distinct" in sql and "group by" in sql:
        redundant_queries.append(f.stem.replace("query_", "Q"))
print(f"Found {len(redundant_queries)} queries: {', '.join(redundant_queries)}")
print()

# Pattern 4: multi_push_predicate - Multiple CTEs (3+)
print("=== multi_push_predicate (nested CTEs: 3+ CTEs) ===")
nested_queries = []
for f in sorted(queries_dir.glob("query_*.sql")):
    sql = f.read_text()
    # Count CTE definitions (word AS ()
    cte_count = len(re.findall(r'\b\w+\s+as\s*\(', sql, re.IGNORECASE))
    if cte_count >= 3:
        nested_queries.append(f"{f.stem.replace('query_', 'Q')} ({cte_count} CTEs)")
print(f"Found {len(nested_queries)} queries: {', '.join(nested_queries[:15])}")
if len(nested_queries) > 15:
    print(f"... and {len(nested_queries) - 15} more")
print()

# Pattern 5: reorder_join - Multiple table joins
print("=== reorder_join (multi-table joins: 4+ tables) ===")
join_queries = []
for f in sorted(queries_dir.glob("query_*.sql")):
    sql = f.read_text().lower()
    # Count FROM and JOIN occurrences
    table_count = sql.count(" join ") + sql.count("\njoin ")
    if table_count >= 3:
        join_queries.append(f"{f.stem.replace('query_', 'Q')} ({table_count+1} joins)")
print(f"Found {len(join_queries)} queries: {', '.join(join_queries[:15])}")
if len(join_queries) > 15:
    print(f"... and {len(join_queries) - 15} more")
print()

# Summary
print("=" * 80)
print("SUMMARY: Test Candidates")
print("=" * 80)
print(f"flatten_subquery: Test on Q8 (simple IN), Q16, Q38, Q40, Q62 (complex)")
print(f"inline_cte: Test on Q{cte_queries[0][1:]}, Q{cte_queries[1][1:]}, Q{cte_queries[2][1:]} (has CTEs)")
print(f"remove_redundant: {'None found - pattern may not exist in TPC-DS' if not redundant_queries else 'Test on ' + ', '.join(redundant_queries[:3])}")
print(f"multi_push_predicate: Test on {', '.join(nested_queries[:3])}")
print(f"reorder_join: Test on {', '.join(join_queries[:3])}")
