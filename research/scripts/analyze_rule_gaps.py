#!/usr/bin/env python3
"""Analyze the gap between successful transforms and AST rule coverage."""

import json
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).parent.parent
AST_ANALYSIS = BASE / "docs" / "tpcds_ast_analysis.json"
BENCHMARK = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828" / "summary.json"
OUTPUT = BASE / "docs" / "rule_effectiveness_analysis.md"


# Known winning transforms from REPORT.md
WINNING_TRANSFORMS = {
    1: {"transform": "decorrelate", "rule": "QT-OPT-002", "speedup": 2.81},
    15: {"transform": "or_to_union", "rule": "QT-OPT-001", "speedup": 2.67},
    93: {"transform": "early_filter", "rule": None, "speedup": 2.71},
    90: {"transform": "early_filter", "rule": None, "speedup": 1.84},
    74: {"transform": "pushdown", "rule": "QT-OPT-004", "speedup": 1.42},
    80: {"transform": "early_filter", "rule": None, "speedup": 1.24},
    73: {"transform": "pushdown", "rule": "QT-OPT-004", "speedup": 1.24},
    27: {"transform": "early_filter", "rule": None, "speedup": 1.23},
    78: {"transform": "projection_prune", "rule": None, "speedup": 1.21},
}


def load_data():
    with open(AST_ANALYSIS) as f:
        ast_data = json.load(f)
    with open(BENCHMARK) as f:
        bench_data = json.load(f)
    return ast_data, bench_data


def check_detection_coverage(ast_data):
    """Check if winning transforms are detected by AST rules."""

    coverage = {}

    for qnum, info in WINNING_TRANSFORMS.items():
        query_key = f"query_{qnum}"
        rules = ast_data["queries"].get(query_key, [])
        rule_ids = {r["rule_id"] for r in rules}

        expected_rule = info["rule"]
        detected = expected_rule in rule_ids if expected_rule else False

        coverage[qnum] = {
            "transform": info["transform"],
            "expected_rule": expected_rule or "MISSING",
            "detected": detected,
            "all_rules": list(rule_ids),
            "speedup": info["speedup"]
        }

    return coverage


def analyze_rule_independence(ast_data, bench_data):
    """Analyze which rules tend to co-occur (not independent)."""

    # Build speedup map
    speedup_map = {}
    for result in bench_data["results"]:
        qnum = result["query"]
        if result["status"] == "pass":
            speedup_map[qnum] = result["speedup"]

    # Track co-occurrences
    rule_cooccur = defaultdict(lambda: defaultdict(int))

    for query_key, findings in ast_data["queries"].items():
        qnum = int(query_key.replace("query_", ""))
        if qnum not in speedup_map:
            continue

        rules = sorted({f["rule_id"] for f in findings})

        # Count pairwise co-occurrence
        for i in range(len(rules)):
            for j in range(i + 1, len(rules)):
                rule_cooccur[rules[i]][rules[j]] += 1
                rule_cooccur[rules[j]][rules[i]] += 1

    return rule_cooccur


def identify_overoptimized_patterns(ast_data, bench_data):
    """Find rules that appear in queries where optimizer makes them WORSE."""

    speedup_map = {}
    for result in bench_data["results"]:
        qnum = result["query"]
        if result["status"] == "pass":
            speedup_map[qnum] = result["speedup"]

    rule_stats = defaultdict(lambda: {"improvements": 0, "regressions": 0, "queries": []})

    for query_key, findings in ast_data["queries"].items():
        qnum = int(query_key.replace("query_", ""))
        if qnum not in speedup_map:
            continue

        speedup = speedup_map[qnum]
        rules = {f["rule_id"] for f in findings}

        for rule_id in rules:
            rule_stats[rule_id]["queries"].append((qnum, speedup))
            if speedup < 1.0:
                rule_stats[rule_id]["regressions"] += 1
            elif speedup >= 1.2:
                rule_stats[rule_id]["improvements"] += 1

    # Compute "over-optimization" score: high frequency but more regressions than wins
    overoptimized = []
    for rule_id, stats in rule_stats.items():
        total = len(stats["queries"])
        if total < 10:
            continue  # Need significant data

        reg_rate = stats["regressions"] / total
        imp_rate = stats["improvements"] / total

        if reg_rate > imp_rate:
            overoptimized.append({
                "rule_id": rule_id,
                "total": total,
                "regressions": stats["regressions"],
                "improvements": stats["improvements"],
                "overopt_score": reg_rate - imp_rate
            })

    return sorted(overoptimized, key=lambda x: x["overopt_score"], reverse=True)


def generate_gap_analysis():
    """Generate comprehensive gap analysis report."""

    print("Loading data...")
    ast_data, bench_data = load_data()

    print("Checking detection coverage...")
    coverage = check_detection_coverage(ast_data)

    print("Analyzing rule independence...")
    rule_cooccur = analyze_rule_independence(ast_data, bench_data)

    print("Identifying over-optimized patterns...")
    overoptimized = identify_overoptimized_patterns(ast_data, bench_data)

    # Generate report
    md = []
    md.append("# Rule Effectiveness Analysis (Revised)")
    md.append("")
    md.append("**Critical insight:** Many high-value transforms are NOT detected by AST rules.")
    md.append("")

    # Coverage gaps
    md.append("## Detection Coverage for Winning Transforms")
    md.append("")
    md.append("| Query | Speedup | Transform | Expected Rule | Detected? | Rules Found |")
    md.append("|-------|---------|-----------|---------------|-----------|-------------|")

    detected_count = 0
    missing_count = 0

    for qnum in sorted(coverage.keys(), key=lambda x: coverage[x]["speedup"], reverse=True):
        c = coverage[qnum]
        status = "✓" if c["detected"] else "✗"
        if c["detected"]:
            detected_count += 1
        else:
            missing_count += 1

        rules_found = ", ".join(c["all_rules"][:3])
        if len(c["all_rules"]) > 3:
            rules_found += f" +{len(c['all_rules']) - 3} more"

        md.append(f"| Q{qnum} | **{c['speedup']:.2f}x** | {c['transform']} | "
                 f"{c['expected_rule']} | {status} | {rules_found} |")

    md.append("")
    md.append(f"**Coverage: {detected_count}/{detected_count + missing_count} "
             f"({detected_count/(detected_count + missing_count)*100:.0f}%)**")
    md.append("")

    # Missing transforms
    missing = [c for c in coverage.values() if not c["detected"]]
    if missing:
        md.append("### ⚠️ HIGH-VALUE TRANSFORMS NOT DETECTED")
        md.append("")
        md.append("These transforms produced big wins but have no AST detection:")
        md.append("")
        for c in sorted(missing, key=lambda x: x["speedup"], reverse=True):
            md.append(f"- **{c['transform']}** ({c['speedup']:.2f}x speedup) - {c['expected_rule']}")
        md.append("")

    # Over-optimized patterns
    md.append("## Over-Optimized Rules")
    md.append("")
    md.append("Rules that appear frequently but optimizer makes them WORSE:")
    md.append("")
    md.append("| Rule ID | Total | Improvements | Regressions | Over-Opt Score |")
    md.append("|---------|-------|--------------|-------------|----------------|")

    for rule in overoptimized[:10]:
        md.append(f"| **{rule['rule_id']}** | {rule['total']} | "
                 f"{rule['improvements']} | {rule['regressions']} | "
                 f"{rule['overopt_score']:.2f} |")

    md.append("")
    md.append("**Interpretation:** These rules detect patterns where DuckDB's optimizer already does well. "
             "Our LLM applying additional transforms makes queries WORSE.")
    md.append("")

    # Rule co-occurrence
    md.append("## Rule Co-Occurrence (Top 15 Pairs)")
    md.append("")
    md.append("Rules that frequently appear together (not independent):")
    md.append("")

    # Flatten and sort co-occurrences
    cooccur_list = []
    seen = set()
    for r1, targets in rule_cooccur.items():
        for r2, count in targets.items():
            if r1 < r2:  # Avoid duplicates
                pair = (r1, r2)
                if pair not in seen:
                    cooccur_list.append((r1, r2, count))
                    seen.add(pair)

    cooccur_list.sort(key=lambda x: x[2], reverse=True)

    md.append("| Rule 1 | Rule 2 | Co-occurrences |")
    md.append("|--------|--------|----------------|")
    for r1, r2, count in cooccur_list[:15]:
        md.append(f"| {r1} | {r2} | {count} |")

    md.append("")

    # Recommendations
    md.append("## Recommendations")
    md.append("")
    md.append("### 1. Add Missing Detection Rules")
    md.append("")
    md.append("Priority transforms to add AST detection for:")
    md.append("")
    md.append("- **early_filter**: 2.71x, 1.84x, 1.24x, 1.23x wins (4 big wins, NO detection!)")
    md.append("- **projection_prune**: 1.21x win (no detection)")
    md.append("")

    md.append("### 2. Reduce False Positives")
    md.append("")
    md.append("Rules with high over-optimization scores need refinement:")
    md.append("")
    for rule in overoptimized[:5]:
        md.append(f"- **{rule['rule_id']}**: {rule['overopt_score']:.0%} over-optimization rate")
    md.append("")

    md.append("### 3. Treat Co-Occurring Rules as Patterns")
    md.append("")
    md.append("Rules that co-occur >15 times should be evaluated together, not independently.")
    md.append("")

    md.append("---")
    md.append("")
    md.append("*Analysis based on TPC-DS SF100 benchmark (Kimi K2.5, 87 validated queries)*")

    report = "\n".join(md)
    OUTPUT.write_text(report)

    print(f"\n✓ Gap analysis complete: {OUTPUT}")
    print(f"  - Detection coverage: {detected_count}/{detected_count + missing_count}")
    print(f"  - Missing high-value transforms: {missing_count}")
    print(f"  - Over-optimized rules identified: {len(overoptimized)}")


if __name__ == "__main__":
    generate_gap_analysis()
