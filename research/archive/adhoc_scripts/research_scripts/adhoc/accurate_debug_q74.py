#!/usr/bin/env python3
"""Accurate debug of Q74 - don't descend into CTEs."""

import sqlglot
from sqlglot import exp
from pathlib import Path

q74_sql = Path("research/experiments/benchmarks/kimi_benchmark_20260202_221828/q74/original.sql").read_text()

parsed = sqlglot.parse_one(q74_sql, dialect="duckdb")

# Find WITH and main SELECT
with_node = parsed.find(exp.With)
if not with_node:
    print("No WITH clause found")
    exit(1)

# The parent of WITH should be the main SELECT
main_select = with_node.parent
if not isinstance(main_select, exp.Select):
    print(f"WITH parent is {type(main_select)}, not Select")
    exit(1)

print("Q74 Analysis (NOT descending into CTEs):")
print("=" * 60)

# Get CTE names
cte_names = set()
for cte in with_node.find_all(exp.CTE):
    if cte.alias:
        cte_names.add(str(cte.alias).lower())

print(f"CTEs defined: {cte_names}")

# Get tables from main query FROM only (shallow search)
print("\nMain query FROM clause:")
from_clause = main_select.args.get('from')
if from_clause:
    # Only look at direct children, don't recurse into subqueries/CTEs
    for node in from_clause.walk():
        if isinstance(node, exp.Table) and node.this:
            name = str(node.this)
            is_cte = name.lower() in cte_names
            print(f"  - {name} {'(CTE)' if is_cte else '(real table)'}")
            # Stop descending into subqueries
            if isinstance(node.parent, (exp.Subquery, exp.CTE)):
                break
else:
    print("  (no FROM clause)")

# Get tables from main query JOINs only
print("\nMain query JOINs:")
join_tables = []
for join in main_select.find_all(exp.Join):
    # Check if this JOIN is in the main query, not inside a CTE
    # Walk up the tree to see if we hit a CTE before hitting main_select
    parent = join.parent
    in_cte = False
    while parent:
        if isinstance(parent, exp.CTE):
            in_cte = True
            break
        if parent == main_select:
            break
        parent = parent.parent

    if not in_cte:
        for table in join.find_all(exp.Table):
            if table.this:
                name = str(table.this)
                is_cte = name.lower() in cte_names
                print(f"  - {name} {'(CTE)' if is_cte else '(real table)'}")
                if not is_cte:
                    join_tables.append(name)

print(f"\n{'='*60}")
print(f"Real tables in main query: {join_tables if join_tables else '[]'}")

# Check dimension tables
DIMENSION_TABLES = {
    'store', 'customer', 'item', 'customer_address', 'date_dim', 'reason'
}

dim_in_main = [t for t in join_tables if t.lower() in DIMENSION_TABLES or 'dim' in t.lower()]
print(f"Dimension tables in main query: {dim_in_main if dim_in_main else '[]'}")

print(f"\n{'='*60}")
print("Why QT-OPT-004 doesn't fire on Q74:")
print("=" * 60)
if not dim_in_main:
    print("✗ No dimension tables in main query FROM/JOINs")
    print("✗ All tables in main query are CTE references")
    print()
    print("Q74 has date_dim INSIDE the CTE, not in main query.")
    print("This means the filter is already pushed down into the CTE.")
    print("QT-OPT-004 looks for the OPPOSITE pattern:")
    print("  - Dimension in main query WITH filter")
    print("  - Opportunity to push that filter INTO the CTE")
else:
    print(f"✓ Found dimension tables: {dim_in_main}")
    print("  -> Should detect, need to investigate further")
