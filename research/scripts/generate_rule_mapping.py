#!/usr/bin/env python3
"""Generate standardized rule naming scheme: GLD-XXX (verified) vs SQL-XXX (detection)."""

import json
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent.parent
AST_ANALYSIS = BASE / "docs" / "tpcds_ast_analysis.json"
BENCHMARK = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828" / "summary.json"
OUTPUT_MAP = BASE / "docs" / "rule_naming_migration.md"


# Verified winning transforms (from benchmark analysis)
GOLD_RULES = {
    "QT-OPT-002": {
        "new_id": "GLD-001",
        "name": "Decorrelate Subquery to CTE",
        "transform": "decorrelate",
        "proven_speedup": "2.81x (Q1)",
        "evidence": "Strong predictor, avg 1.32x speedup"
    },
    "QT-OPT-001": {
        "new_id": "GLD-002",
        "name": "OR to UNION ALL",
        "transform": "or_to_union",
        "proven_speedup": "2.67x (Q15)",
        "evidence": "Avg 1.18x speedup"
    },
    # These need to be CREATED
    "MISSING-EARLY-FILTER": {
        "new_id": "GLD-003",
        "name": "Early Filter Pushdown (Dimension Before Fact)",
        "transform": "early_filter",
        "proven_speedup": "2.71x (Q93), 1.84x (Q90)",
        "evidence": "4 wins, highest-value missing transform",
        "status": "⚠️ NEEDS CREATION"
    },
    "MISSING-PROJECTION-PRUNE": {
        "new_id": "GLD-004",
        "name": "Projection Pruning",
        "transform": "projection_prune",
        "proven_speedup": "1.21x (Q78)",
        "evidence": "Win with no detection",
        "status": "⚠️ NEEDS CREATION"
    },
    "SQL-SUB-001": {
        "new_id": "GLD-005",
        "name": "Correlated Subquery in WHERE",
        "transform": "correlated_subquery",
        "proven_speedup": "1.80x avg",
        "evidence": "67% win rate, strongest SQL-* predictor"
    },
}


def load_data():
    with open(AST_ANALYSIS) as f:
        ast_data = json.load(f)
    return ast_data


def categorize_rules():
    """Categorize all rules into GLD (verified) vs SQL (detection)."""

    ast_data = load_data()
    all_rules = ast_data["rule_frequency"]

    # Rules already classified as gold
    gold_existing = {old_id: info for old_id, info in GOLD_RULES.items()
                     if not old_id.startswith("MISSING")}
    gold_missing = {old_id: info for old_id, info in GOLD_RULES.items()
                    if old_id.startswith("MISSING")}

    # All other rules become SQL-XXX
    sql_rules = {}
    sql_counter = 1

    for rule_id in sorted(all_rules.keys()):
        if rule_id in gold_existing:
            continue  # Already in gold

        sql_rules[rule_id] = {
            "new_id": f"SQL-{sql_counter:03d}",
            "old_id": rule_id,
            "frequency": all_rules[rule_id]
        }
        sql_counter += 1

    return gold_existing, gold_missing, sql_rules


def generate_migration_doc():
    """Generate complete migration mapping document."""

    gold_existing, gold_missing, sql_rules = categorize_rules()

    md = []
    md.append("# Rule Naming Migration: GLD-XXX vs SQL-XXX")
    md.append("")
    md.append("**Standard naming scheme:**")
    md.append("- `GLD-XXX` = Gold standard rules with proven benchmark speedups")
    md.append("- `SQL-XXX` = Pattern detection rules (not yet verified or negative correlation)")
    md.append("")

    # Gold rules (existing)
    md.append("## Gold Rules (GLD-XXX) - Verified Transforms")
    md.append("")
    md.append("These rules detect patterns that have produced proven speedups in benchmarks:")
    md.append("")
    md.append("| New ID | Old ID | Name | Transform | Proven Speedup | Evidence |")
    md.append("|--------|--------|------|-----------|----------------|----------|")

    for old_id in sorted(gold_existing.keys(), key=lambda x: GOLD_RULES[x]["new_id"]):
        info = GOLD_RULES[old_id]
        md.append(f"| **{info['new_id']}** | {old_id} | {info['name']} | "
                 f"{info['transform']} | {info['proven_speedup']} | {info['evidence']} |")

    md.append("")

    # Gold rules (missing - need creation)
    md.append("## ⚠️ Missing Gold Rules (Need Creation)")
    md.append("")
    md.append("These high-value transforms are NOT detected by current AST rules:")
    md.append("")
    md.append("| New ID | Name | Transform | Proven Speedup | Priority |")
    md.append("|--------|------|-----------|----------------|----------|")

    for old_id in sorted(gold_missing.keys(), key=lambda x: GOLD_RULES[x]["new_id"]):
        info = GOLD_RULES[old_id]
        md.append(f"| **{info['new_id']}** | {info['name']} | {info['transform']} | "
                 f"{info['proven_speedup']} | 🔴 CRITICAL |")

    md.append("")

    # SQL detection rules
    md.append("## SQL Detection Rules (SQL-XXX)")
    md.append("")
    md.append("Pattern detection rules (may have negative or neutral correlation with speedups):")
    md.append("")
    md.append("<details>")
    md.append("<summary>Click to expand complete mapping</summary>")
    md.append("")
    md.append("| New ID | Old ID | Frequency | Notes |")
    md.append("|--------|--------|-----------|-------|")

    for old_id in sorted(sql_rules.keys()):
        info = sql_rules[old_id]
        note = ""
        if old_id.startswith("QT-OPT"):
            note = "Over-optimized pattern"
        elif old_id.startswith("QT-AGG"):
            note = "Aggregation pattern"
        elif old_id.startswith("SQL-DUCK"):
            note = "DuckDB-specific"

        md.append(f"| {info['new_id']} | {old_id} | {info['frequency']} | {note} |")

    md.append("")
    md.append("</details>")
    md.append("")

    # Migration instructions
    md.append("## Migration Plan")
    md.append("")
    md.append("### Phase 1: Create Missing Gold Rules (Priority)")
    md.append("")
    md.append("1. **GLD-003 (Early Filter)**: Create AST detector for dimension filter before fact join")
    md.append("   - Pattern: `WITH filtered_dim AS (SELECT ... WHERE selective_filter) SELECT ... FROM fact JOIN filtered_dim`")
    md.append("   - Detection: Dimension table with selective filter joined to fact table")
    md.append("")
    md.append("2. **GLD-004 (Projection Pruning)**: Create detector for unused columns in CTEs")
    md.append("   - Pattern: CTE selects columns that aren't used in main query")
    md.append("   - Detection: Column in CTE SELECT list not referenced later")
    md.append("")

    md.append("### Phase 2: Rename Existing Rules")
    md.append("")
    md.append("**Code changes required:**")
    md.append("")
    md.append("1. Update rule_id in all rule classes:")
    md.append("   - `packages/qt-sql/qt_sql/analyzers/ast_detector/rules/*.py`")
    md.append("")
    md.append("2. Update references in:")
    md.append("   - Knowledge base mappings")
    md.append("   - Documentation")
    md.append("   - Test cases")
    md.append("")

    md.append("### Phase 3: Update Rule Categories")
    md.append("")
    md.append("**New severity/category scheme:**")
    md.append("")
    md.append("- `GLD-XXX` rules:")
    md.append("  - `severity: 'gold'`")
    md.append("  - `category: 'verified_optimization'`")
    md.append("  - Higher priority in scoring")
    md.append("")
    md.append("- `SQL-XXX` rules:")
    md.append("  - `severity: 'low' | 'medium' | 'info'`")
    md.append("  - `category: 'pattern_detection'`")
    md.append("  - Lower priority, informational")
    md.append("")

    # Summary
    md.append("## Summary")
    md.append("")
    md.append(f"- **Gold rules (existing):** {len(gold_existing)}")
    md.append(f"- **Gold rules (missing):** {len(gold_missing)} ⚠️")
    md.append(f"- **SQL detection rules:** {len(sql_rules)}")
    md.append("")
    md.append("**Action items:**")
    md.append("1. Create GLD-003 and GLD-004 AST detectors (highest priority)")
    md.append("2. Rename QT-OPT-002 → GLD-001")
    md.append("3. Rename QT-OPT-001 → GLD-002")
    md.append("4. Rename SQL-SUB-001 → GLD-005")
    md.append(f"5. Rename {len(sql_rules)} rules to SQL-XXX scheme")
    md.append("")

    md.append("---")
    md.append("")
    md.append("*Generated from TPC-DS benchmark analysis*")

    return "\n".join(md)


def main():
    print("Categorizing rules...")
    gold_existing, gold_missing, sql_rules = categorize_rules()

    print(f"  Gold (existing): {len(gold_existing)}")
    print(f"  Gold (missing): {len(gold_missing)}")
    print(f"  SQL detection: {len(sql_rules)}")

    print("\nGenerating migration document...")
    doc = generate_migration_doc()

    OUTPUT_MAP.write_text(doc)

    print(f"\n✓ Migration plan saved: {OUTPUT_MAP}")
    print("\nNext steps:")
    print("  1. Review docs/rule_naming_migration.md")
    print("  2. Create GLD-003 (early_filter) detector - HIGHEST PRIORITY")
    print("  3. Create GLD-004 (projection_prune) detector")
    print("  4. Run migration script to rename existing rules")


if __name__ == "__main__":
    main()
