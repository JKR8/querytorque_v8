#!/usr/bin/env python3
"""Simple debug of Q74 structure."""

import sqlglot
from sqlglot import exp
from pathlib import Path

q74_sql = Path("research/experiments/benchmarks/kimi_benchmark_20260202_221828/q74/original.sql").read_text()

parsed = sqlglot.parse_one(q74_sql, dialect="duckdb")

# Find main SELECT (the one with WITH as parent)
with_node = parsed.find(exp.With)
main_select = None
if with_node and with_node.parent:
    main_select = with_node.parent

if not main_select or not isinstance(main_select, exp.Select):
    print("ERROR: Can't find main SELECT")
    exit(1)

print("Q74 Main Query Analysis:")
print("=" * 60)

# Get CTE names
cte_names = set()
for cte in with_node.find_all(exp.CTE):
    if cte.alias:
        cte_names.add(str(cte.alias).lower())
        print(f"CTE: {cte.alias}")

print(f"\nCTE names: {cte_names}")

# Get main FROM tables (excluding CTEs)
print("\nMain FROM tables (excluding CTE references):")
from_clause = main_select.args.get('from')
real_tables = []
if from_clause:
    for table in from_clause.find_all(exp.Table):
        if table.this:
            name = str(table.this)
            if name.lower() not in cte_names:
                real_tables.append(name)
                print(f"  - {name} (real table)")
            else:
                print(f"  - {name} (CTE reference)")

# Check JOINs
print("\nJOINs in main query:")
for join in main_select.find_all(exp.Join):
    for table in join.find_all(exp.Table):
        if table.this:
            name = str(table.this)
            if name.lower() not in cte_names:
                real_tables.append(name)
                print(f"  - {name} (real table)")
            else:
                print(f"  - {name} (CTE reference)")

print(f"\nReal dimension tables in main query: {real_tables}")

# Main WHERE
where = main_select.find(exp.Where)
if where:
    print("\nMain WHERE clauses:")
    for eq in list(where.find_all(exp.EQ))[:5]:
        print(f"  - {eq.sql()[:100]}")

print("\n" + "=" * 60)
print("Conclusion:")
print("=" * 60)
print(f"Real tables in main FROM: {len(real_tables)}")
print(f"  -> QT-OPT-004 requires dimension tables in main FROM")
print(f"  -> Q74 has ZERO real tables (only CTE references)")
print(f"  -> This is why QT-OPT-004 doesn't detect Q74")
print()
print("Q74 pattern is DIFFERENT:")
print("  - Filters (d_year IN (1999, 2000)) are INSIDE the CTE")
print("  - Main query just filters on CTE columns (sale_type, year)")
print("  - This is not the 'pushdown' pattern QT-OPT-004 looks for")
