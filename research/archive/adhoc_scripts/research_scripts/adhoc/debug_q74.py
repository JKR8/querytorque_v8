#!/usr/bin/env python3
"""Debug why QT-OPT-004 doesn't detect Q74."""

from pathlib import Path
from qt_sql.analyzers.ast_detector.detector import detect_patterns

q74_sql = Path("research/experiments/benchmarks/kimi_benchmark_20260202_221828/q74/original.sql").read_text()

print("Q74 Query Structure:")
print("=" * 60)

# Check what's in main FROM clause
import sqlglot
from sqlglot import exp

parsed = sqlglot.parse_one(q74_sql, dialect="duckdb")

# Find the main SELECT (after WITH)
main_select = None
for node in parsed.walk():
    if isinstance(node, exp.Select) and node.parent and isinstance(node.parent, exp.With):
        # This is inside a CTE, skip
        continue
    if isinstance(node, exp.Select):
        main_select = node
        break

if main_select:
    print("\nMain FROM clause tables:")
    from_clause = main_select.args.get('from')
    if from_clause:
        for table in from_clause.find_all(exp.Table):
            if table.this:
                print(f"  - {table.this}")

    # Check JOINs
    joins = list(main_select.find_all(exp.Join))
    if joins:
        print(f"\nJOINs: {len(joins)}")

    # Check main WHERE
    where = main_select.find(exp.Where)
    if where:
        print("\nMain WHERE filters:")
        for eq in where.find_all(exp.EQ):
            print(f"  - {eq.sql()[:80]}")

# Now run detection
print("\n" + "=" * 60)
print("AST Detection Results:")
print("=" * 60)

results = detect_patterns(q74_sql, dialect="duckdb")
for match in results:
    if "QT-OPT-004" in match.rule_id or "pushdown" in match.message.lower():
        print(f"\n{match.rule_id}: {match.message}")
        print(f"  Line {match.line_number}: {match.matched_text[:100]}")

if not any("QT-OPT-004" in m.rule_id for m in results):
    print("\n❌ QT-OPT-004 NOT DETECTED")

print("\n" + "=" * 60)
print("Why QT-OPT-004 fails:")
print("=" * 60)
print("""
QT-OPT-004 looks for:
1. WITH clause (CTEs) ✓
2. CTE with fact table and aggregation ✓
3. Dimension table in MAIN query FROM ✗

Q74 problem:
- Main FROM only references CTE 'year_total' (4 aliases)
- NO dimension tables in main FROM
- date_dim is INSIDE the CTE (already filtered by d_year)
- Main WHERE only filters on CTE columns (customer_id, sale_type, year)

This is NOT the "pushdown" pattern QT-OPT-004 detects.
QT-OPT-004 expects: main query joins CTE to dimension and filters dimension.
Q74 has: CTE already includes dimension filter, main only filters CTE results.

This is a DIFFERENT pattern - maybe should be detected by different rule?
""")
