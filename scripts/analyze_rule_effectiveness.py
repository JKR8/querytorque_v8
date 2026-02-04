#!/usr/bin/env python3
"""Analyze correlation between AST detector rules and query optimization success.

Reads:
- docs/tpcds_ast_analysis.json (rule detections per query)
- research/experiments/benchmarks/kimi_benchmark_20260202_221828/summary.json (speedups)

Outputs:
- docs/rule_effectiveness_analysis.md (single comprehensive report)
"""

import json
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Paths
BASE = Path(__file__).parent.parent
AST_ANALYSIS = BASE / "docs" / "tpcds_ast_analysis.json"
BENCHMARK = BASE / "research" / "experiments" / "benchmarks" / "kimi_benchmark_20260202_221828" / "summary.json"
OUTPUT = BASE / "docs" / "rule_effectiveness_analysis.md"


def load_data():
    """Load AST analysis and benchmark results."""
    with open(AST_ANALYSIS) as f:
        ast_data = json.load(f)

    with open(BENCHMARK) as f:
        bench_data = json.load(f)

    return ast_data, bench_data


def parse_query_number(query_key: str) -> int:
    """Extract query number from 'query_1' or 'q1' format."""
    return int(query_key.replace("query_", "").replace("q", ""))


def build_rule_speedup_map(ast_data: dict, bench_data: dict) -> Dict[str, List[float]]:
    """Map each rule to list of speedups for queries where it appeared."""
    rule_speedups = defaultdict(list)

    # Build query number -> speedup map
    speedup_map = {}
    for result in bench_data["results"]:
        qnum = result["query"]
        if result["status"] == "pass":  # Only validated wins
            speedup_map[qnum] = result["speedup"]

    # For each query with findings
    for query_key, findings in ast_data["queries"].items():
        qnum = parse_query_number(query_key)

        if qnum not in speedup_map:
            continue  # Skip queries without valid benchmark

        speedup = speedup_map[qnum]

        # Get unique rules that triggered
        rules_in_query = {f["rule_id"] for f in findings}

        for rule_id in rules_in_query:
            rule_speedups[rule_id].append(speedup)

    return dict(rule_speedups)


def score_rules(rule_speedups: Dict[str, List[float]]) -> List[Tuple]:
    """Score each rule by its predictive power."""
    scores = []

    for rule_id, speedups in rule_speedups.items():
        if not speedups:
            continue

        total = len(speedups)
        wins = sum(1 for s in speedups if s >= 1.2)
        big_wins = sum(1 for s in speedups if s >= 1.5)
        regressions = sum(1 for s in speedups if s < 1.0)
        neutral = total - wins - regressions

        avg_speedup = sum(speedups) / total
        max_speedup = max(speedups)

        # Predictive score: (wins - regressions) / total
        predictive_score = (wins - regressions) / total if total > 0 else 0

        # High-value score: weighted by big wins
        value_score = (big_wins * 2 + wins) / total if total > 0 else 0

        scores.append({
            "rule_id": rule_id,
            "appearances": total,
            "wins": wins,
            "big_wins": big_wins,
            "neutral": neutral,
            "regressions": regressions,
            "avg_speedup": avg_speedup,
            "max_speedup": max_speedup,
            "predictive_score": predictive_score,
            "value_score": value_score,
        })

    return scores


def find_rule_combinations(ast_data: dict, bench_data: dict, min_cooccurrence: int = 5) -> List[Tuple]:
    """Find rule combinations (pairs) that predict successful optimization."""
    combo_speedups = defaultdict(list)

    # Build speedup map
    speedup_map = {}
    for result in bench_data["results"]:
        qnum = result["query"]
        if result["status"] == "pass":
            speedup_map[qnum] = result["speedup"]

    # For each query
    for query_key, findings in ast_data["queries"].items():
        qnum = parse_query_number(query_key)

        if qnum not in speedup_map:
            continue

        speedup = speedup_map[qnum]
        rules = sorted({f["rule_id"] for f in findings})

        # Generate all pairs
        for i in range(len(rules)):
            for j in range(i + 1, len(rules)):
                combo = (rules[i], rules[j])
                combo_speedups[combo].append(speedup)

    # Score combinations
    combo_scores = []
    for combo, speedups in combo_speedups.items():
        if len(speedups) < min_cooccurrence:
            continue

        total = len(speedups)
        wins = sum(1 for s in speedups if s >= 1.2)
        big_wins = sum(1 for s in speedups if s >= 1.5)
        avg_speedup = sum(speedups) / total

        predictive_score = wins / total

        combo_scores.append({
            "rules": combo,
            "appearances": total,
            "wins": wins,
            "big_wins": big_wins,
            "avg_speedup": avg_speedup,
            "predictive_score": predictive_score,
        })

    return combo_scores


def generate_report(rule_scores: List[dict], combo_scores: List[dict]):
    """Generate markdown report."""

    # Sort rules by predictive score
    by_predictive = sorted(rule_scores, key=lambda x: x["predictive_score"], reverse=True)
    by_value = sorted(rule_scores, key=lambda x: x["value_score"], reverse=True)
    by_avg_speedup = sorted(rule_scores, key=lambda x: x["avg_speedup"], reverse=True)

    # Sort combos
    top_combos = sorted(combo_scores, key=lambda x: x["predictive_score"], reverse=True)[:20]

    md = []
    md.append("# Rule Effectiveness Analysis")
    md.append("")
    md.append("**Analysis of AST detector rules vs. actual TPC-DS SF100 speedups**")
    md.append("")
    md.append("## Methodology")
    md.append("")
    md.append("- **Data**: 99 TPC-DS queries, 87 validated optimizations")
    md.append("- **Win**: Speedup ≥ 1.2x")
    md.append("- **Big Win**: Speedup ≥ 1.5x")
    md.append("- **Regression**: Speedup < 1.0x")
    md.append("- **Predictive Score**: (wins - regressions) / total appearances")
    md.append("- **Value Score**: (big_wins × 2 + wins) / total")
    md.append("")

    # Top predictive rules
    md.append("## Top 15 Rules by Predictive Score")
    md.append("")
    md.append("Rules most correlated with successful optimizations:")
    md.append("")
    md.append("| Rank | Rule ID | Predictive Score | Appearances | Wins | Big Wins | Avg Speedup | Max Speedup |")
    md.append("|------|---------|------------------|-------------|------|----------|-------------|-------------|")

    for i, rule in enumerate(by_predictive[:15], 1):
        md.append(f"| {i} | **{rule['rule_id']}** | {rule['predictive_score']:.2f} | "
                 f"{rule['appearances']} | {rule['wins']} | {rule['big_wins']} | "
                 f"{rule['avg_speedup']:.2f}x | {rule['max_speedup']:.2f}x |")

    md.append("")

    # Top value rules
    md.append("## Top 15 Rules by Value Score")
    md.append("")
    md.append("Rules most associated with big wins (≥1.5x):")
    md.append("")
    md.append("| Rank | Rule ID | Value Score | Appearances | Big Wins | Wins | Avg Speedup |")
    md.append("|------|---------|-------------|-------------|----------|------|-------------|")

    for i, rule in enumerate(by_value[:15], 1):
        md.append(f"| {i} | **{rule['rule_id']}** | {rule['value_score']:.2f} | "
                 f"{rule['appearances']} | {rule['big_wins']} | {rule['wins']} | "
                 f"{rule['avg_speedup']:.2f}x |")

    md.append("")

    # Highest avg speedup
    md.append("## Top 15 Rules by Average Speedup")
    md.append("")
    md.append("Rules appearing in queries with highest average speedup:")
    md.append("")
    md.append("| Rank | Rule ID | Avg Speedup | Appearances | Wins | Regressions |")
    md.append("|------|---------|-------------|-------------|------|-------------|")

    for i, rule in enumerate(by_avg_speedup[:15], 1):
        md.append(f"| {i} | **{rule['rule_id']}** | {rule['avg_speedup']:.2f}x | "
                 f"{rule['appearances']} | {rule['wins']} | {rule['regressions']} |")

    md.append("")

    # Full rule table
    md.append("## Complete Rule Scores")
    md.append("")
    md.append("<details>")
    md.append("<summary>Click to expand full table</summary>")
    md.append("")
    md.append("| Rule ID | Appearances | Wins | Big Wins | Neutral | Regressions | Avg Speedup | Predictive Score | Value Score |")
    md.append("|---------|-------------|------|----------|---------|-------------|-------------|------------------|-------------|")

    for rule in sorted(by_predictive, key=lambda x: x["rule_id"]):
        md.append(f"| {rule['rule_id']} | {rule['appearances']} | {rule['wins']} | "
                 f"{rule['big_wins']} | {rule['neutral']} | {rule['regressions']} | "
                 f"{rule['avg_speedup']:.2f}x | {rule['predictive_score']:.2f} | "
                 f"{rule['value_score']:.2f} |")

    md.append("")
    md.append("</details>")
    md.append("")

    # Rule combinations
    md.append("## Top 20 Rule Combinations")
    md.append("")
    md.append("Rule pairs that frequently co-occur in successful optimizations:")
    md.append("")
    md.append("| Rank | Rule 1 | Rule 2 | Predictive Score | Appearances | Wins | Big Wins | Avg Speedup |")
    md.append("|------|--------|--------|------------------|-------------|------|----------|-------------|")

    for i, combo in enumerate(top_combos, 1):
        r1, r2 = combo["rules"]
        md.append(f"| {i} | {r1} | {r2} | {combo['predictive_score']:.2f} | "
                 f"{combo['appearances']} | {combo['wins']} | {combo['big_wins']} | "
                 f"{combo['avg_speedup']:.2f}x |")

    md.append("")

    # Key insights
    md.append("## Key Insights")
    md.append("")

    # Find QT-OPT rules
    qt_opt_rules = [r for r in by_predictive if r["rule_id"].startswith("QT-OPT")]
    if qt_opt_rules:
        md.append("### High-Value Optimization Opportunities (QT-OPT-*)")
        md.append("")
        md.append("Confirmed high-value transforms from knowledge base:")
        md.append("")
        for rule in qt_opt_rules[:10]:
            md.append(f"- **{rule['rule_id']}**: {rule['avg_speedup']:.2f}x avg speedup, "
                     f"{rule['wins']}/{rule['appearances']} wins ({rule['predictive_score']:.0%})")
        md.append("")

    # Find SQL-* rules with good scores
    sql_good = [r for r in by_predictive if r["rule_id"].startswith("SQL-") and r["predictive_score"] > 0.3]
    if sql_good:
        md.append("### Valuable SQL Pattern Rules")
        md.append("")
        md.append("SQL anti-patterns that correlate with optimization opportunities:")
        md.append("")
        for rule in sql_good[:10]:
            md.append(f"- **{rule['rule_id']}**: {rule['predictive_score']:.0%} win rate, "
                     f"{rule['avg_speedup']:.2f}x avg speedup")
        md.append("")

    # Find rules with poor predictive power
    poor = [r for r in by_predictive if r["predictive_score"] < 0 and r["appearances"] >= 10]
    if poor:
        md.append("### Low-Value Rules")
        md.append("")
        md.append("Rules that appear frequently but don't correlate with speedups:")
        md.append("")
        for rule in poor[-5:]:
            md.append(f"- **{rule['rule_id']}**: {rule['predictive_score']:.2f} score, "
                     f"{rule['regressions']} regressions vs {rule['wins']} wins")
        md.append("")

    md.append("## Recommendations")
    md.append("")
    md.append("### Priority 1: Focus on These Rules")
    md.append("")
    top5 = by_value[:5]
    for rule in top5:
        md.append(f"- **{rule['rule_id']}**: Strong predictor with {rule['value_score']:.2f} value score")
    md.append("")

    md.append("### Priority 2: Rule Combinations to Watch")
    md.append("")
    for combo in top_combos[:5]:
        r1, r2 = combo["rules"]
        md.append(f"- **{r1} + {r2}**: {combo['predictive_score']:.0%} win rate over {combo['appearances']} queries")
    md.append("")

    md.append("---")
    md.append("")
    md.append("*Generated from TPC-DS SF100 benchmark on DuckDB*")
    md.append("")

    return "\n".join(md)


def main():
    print("Loading data...")
    ast_data, bench_data = load_data()

    print("Building rule-speedup correlations...")
    rule_speedups = build_rule_speedup_map(ast_data, bench_data)

    print("Scoring individual rules...")
    rule_scores = score_rules(rule_speedups)

    print("Finding rule combinations...")
    combo_scores = find_rule_combinations(ast_data, bench_data)

    print("Generating report...")
    report = generate_report(rule_scores, combo_scores)

    print(f"Writing to {OUTPUT}...")
    OUTPUT.write_text(report)

    print(f"\n✓ Analysis complete: {OUTPUT}")
    print(f"  - Analyzed {len(rule_scores)} rules")
    print(f"  - Found {len(combo_scores)} rule combinations")
    print(f"  - Top rule: {rule_scores[0]['rule_id']} ({rule_scores[0]['predictive_score']:.2f} score)")


if __name__ == "__main__":
    main()
