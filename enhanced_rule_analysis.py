#!/usr/bin/env python3
"""
Enhanced rule effectiveness analysis with correlation and detailed insights.
"""

import json
import csv
from pathlib import Path
from collections import defaultdict
import statistics

# File paths
AST_ANALYSIS_PATH = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/docs/tpcds_ast_analysis.json")
BENCHMARK_PATH = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/benchmarks/kimi_benchmark_20260202_221828/summary.json")
OUTPUT_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/docs")

# Output path
ENHANCED_OUTPUT = OUTPUT_DIR / "rule_effectiveness_detailed.md"

def load_data():
    """Load all necessary data."""
    with open(AST_ANALYSIS_PATH, 'r') as f:
        ast_data = json.load(f)
    with open(BENCHMARK_PATH, 'r') as f:
        benchmark_data = json.load(f)
    with open(OUTPUT_DIR / "rule_effectiveness_analysis.csv", 'r') as f:
        reader = csv.DictReader(f)
        rule_scores = list(reader)
    with open(OUTPUT_DIR / "rule_combinations.json", 'r') as f:
        combinations = json.load(f)

    return ast_data, benchmark_data, rule_scores, combinations

def extract_query_details(ast_data, benchmark_data):
    """Extract detailed query information."""
    query_details = {}

    benchmark_results = {}
    if 'results' in benchmark_data:
        for result in benchmark_data['results']:
            query_num = result.get('query', 0)
            benchmark_results[query_num] = result

    ast_queries = ast_data.get('queries', {})

    for query_key, findings in ast_queries.items():
        if query_key.startswith('query_'):
            query_num = int(query_key.split('_')[1])
        else:
            continue

        if query_num in benchmark_results:
            rules = {}
            for finding in findings:
                rule_id = finding.get('rule_id', '')
                if rule_id:
                    rules[rule_id] = {
                        'name': finding.get('name', ''),
                        'severity': finding.get('severity', ''),
                        'category': finding.get('category', ''),
                        'description': finding.get('description', '')
                    }

            bench = benchmark_results[query_num]
            query_details[query_num] = {
                'rules': list(rules.keys()),
                'rule_details': rules,
                'speedup': bench.get('speedup', 0.0),
                'status': bench.get('status', 'unknown'),
                'original_ms': bench.get('original_ms', 0),
                'optimized_ms': bench.get('optimized_ms', 0)
            }

    return query_details

def analyze_by_speedup_category(query_details):
    """Analyze rules by speedup category."""
    categories = {
        'big_wins': {'threshold': '>2.0x', 'queries': []},
        'good_wins': {'threshold': '1.2-2.0x', 'queries': []},
        'neutral': {'threshold': '1.0-1.2x', 'queries': []},
        'regressions': {'threshold': '<1.0x', 'queries': []}
    }

    for query_num, data in query_details.items():
        speedup = data['speedup']
        if speedup > 2.0:
            categories['big_wins']['queries'].append((query_num, data))
        elif speedup > 1.2:
            categories['good_wins']['queries'].append((query_num, data))
        elif speedup >= 1.0:
            categories['neutral']['queries'].append((query_num, data))
        else:
            categories['regressions']['queries'].append((query_num, data))

    # Count rule appearances in each category
    for cat_name, cat_data in categories.items():
        rule_counts = defaultdict(int)
        for query_num, data in cat_data['queries']:
            for rule in data['rules']:
                rule_counts[rule] += 1
        cat_data['rule_counts'] = dict(sorted(rule_counts.items(), key=lambda x: x[1], reverse=True))

    return categories

def find_exclusive_patterns(query_details):
    """Find rules that appear almost exclusively in wins or losses."""
    rule_in_wins = defaultdict(int)
    rule_in_losses = defaultdict(int)
    rule_total = defaultdict(int)

    for query_num, data in query_details.items():
        speedup = data['speedup']
        is_win = speedup > 1.2
        is_loss = speedup < 1.0

        for rule in data['rules']:
            rule_total[rule] += 1
            if is_win:
                rule_in_wins[rule] += 1
            if is_loss:
                rule_in_losses[rule] += 1

    # Find rules that are strongly biased
    exclusive_wins = []
    exclusive_losses = []

    for rule in rule_total:
        total = rule_total[rule]
        if total >= 5:  # Only consider rules with 5+ appearances
            win_ratio = rule_in_wins[rule] / total
            loss_ratio = rule_in_losses[rule] / total

            if win_ratio >= 0.6:
                exclusive_wins.append((rule, win_ratio, rule_in_wins[rule], total))
            if loss_ratio >= 0.6:
                exclusive_losses.append((rule, loss_ratio, rule_in_losses[rule], total))

    exclusive_wins.sort(key=lambda x: x[1], reverse=True)
    exclusive_losses.sort(key=lambda x: x[1], reverse=True)

    return exclusive_wins, exclusive_losses

def write_enhanced_report(query_details, categories, rule_scores, combinations, exclusive_wins, exclusive_losses):
    """Write enhanced markdown report."""
    print(f"Writing enhanced report to {ENHANCED_OUTPUT}...")

    with open(ENHANCED_OUTPUT, 'w') as f:
        f.write("# Enhanced Rule Effectiveness Analysis\n\n")
        f.write("Detailed correlation analysis between AST detector rules and query optimization success.\n\n")

        # Executive Summary
        f.write("## Executive Summary\n\n")
        total = len(query_details)
        big_wins = len(categories['big_wins']['queries'])
        good_wins = len(categories['good_wins']['queries'])
        neutral = len(categories['neutral']['queries'])
        regressions = len(categories['regressions']['queries'])

        f.write(f"**Total Queries:** {total}\n\n")
        f.write("### Speedup Distribution\n\n")
        f.write(f"- **Big Wins (>2.0x):** {big_wins} queries ({big_wins/total*100:.1f}%)\n")
        f.write(f"- **Good Wins (1.2-2.0x):** {good_wins} queries ({good_wins/total*100:.1f}%)\n")
        f.write(f"- **Neutral (1.0-1.2x):** {neutral} queries ({neutral/total*100:.1f}%)\n")
        f.write(f"- **Regressions (<1.0x):** {regressions} queries ({regressions/total*100:.1f}%)\n\n")

        # Key Findings
        f.write("## Key Findings\n\n")

        f.write("### Most Predictive Rules\n\n")
        f.write("Rules with highest correlation to successful optimization:\n\n")
        for i, rule in enumerate(rule_scores[:5], 1):
            f.write(f"{i}. **`{rule['rule']}`**\n")
            f.write(f"   - Win Rate: {float(rule['win_rate'])*100:.1f}%\n")
            f.write(f"   - Average Speedup: {float(rule['avg_speedup']):.2f}x\n")
            f.write(f"   - Appearances: {rule['appearances']}\n\n")

        f.write("### Most Risky Rules\n\n")
        f.write("Rules associated with regressions:\n\n")
        risky = sorted(rule_scores, key=lambda x: float(x['loss_rate']), reverse=True)[:5]
        for i, rule in enumerate(risky, 1):
            if float(rule['loss_rate']) > 0:
                f.write(f"{i}. **`{rule['rule']}`**\n")
                f.write(f"   - Loss Rate: {float(rule['loss_rate'])*100:.1f}%\n")
                f.write(f"   - Win Rate: {float(rule['win_rate'])*100:.1f}%\n")
                f.write(f"   - Average Speedup: {float(rule['avg_speedup']):.2f}x\n")
                f.write(f"   - Appearances: {rule['appearances']}\n\n")

        # Category Analysis
        f.write("## Analysis by Speedup Category\n\n")

        for cat_name, cat_data in categories.items():
            queries = cat_data['queries']
            if not queries:
                continue

            f.write(f"### {cat_name.replace('_', ' ').title()} ({cat_data['threshold']})\n\n")
            f.write(f"**{len(queries)} queries**\n\n")

            if cat_data['rule_counts']:
                f.write("Top rules in this category:\n\n")
                f.write("| Rule | Appearances | % of Category |\n")
                f.write("|------|-------------|---------------|\n")
                for rule, count in list(cat_data['rule_counts'].items())[:10]:
                    pct = count / len(queries) * 100
                    f.write(f"| `{rule}` | {count} | {pct:.1f}% |\n")
                f.write("\n")

            # Show example queries
            f.write("Example queries:\n")
            for query_num, data in queries[:3]:
                f.write(f"- **Query {query_num}**: {data['speedup']:.2f}x speedup ")
                f.write(f"({data['original_ms']:.0f}ms → {data['optimized_ms']:.0f}ms)\n")
                f.write(f"  Rules: `{', '.join(data['rules'][:5])}`")
                if len(data['rules']) > 5:
                    f.write(f" +{len(data['rules'])-5} more")
                f.write("\n")
            f.write("\n")

        # Exclusive patterns
        f.write("## Strongly Correlated Rules\n\n")

        if exclusive_wins:
            f.write("### Rules Strongly Associated with Wins\n\n")
            f.write("Rules that appear predominantly in successful optimizations:\n\n")
            f.write("| Rule | Win Appearances | Total | Win % |\n")
            f.write("|------|-----------------|-------|-------|\n")
            for rule, ratio, wins, total in exclusive_wins[:10]:
                f.write(f"| `{rule}` | {wins} | {total} | {ratio*100:.1f}% |\n")
            f.write("\n")

        if exclusive_losses:
            f.write("### Rules Strongly Associated with Regressions\n\n")
            f.write("Rules that appear predominantly in queries with regressions:\n\n")
            f.write("| Rule | Loss Appearances | Total | Loss % |\n")
            f.write("|------|------------------|-------|--------|\n")
            for rule, ratio, losses, total in exclusive_losses[:10]:
                f.write(f"| `{rule}` | {losses} | {total} | {ratio*100:.1f}% |\n")
            f.write("\n")

        # Rule combinations deep dive
        f.write("## Rule Combination Analysis\n\n")
        f.write("### Top Performing Combinations\n\n")

        top_combos = combinations['rule_combinations'][:10]
        for i, combo in enumerate(top_combos, 1):
            f.write(f"{i}. **`{combo['combination']}`**\n")
            f.write(f"   - Appearances: {combo['appearances']}\n")
            f.write(f"   - Win Rate: {combo['win_rate']*100:.1f}%\n")
            f.write(f"   - Average Speedup: {combo['avg_speedup']:.2f}x\n")
            f.write(f"   - Net Score: {combo['net_score']:+d}\n\n")

        # Actionable insights
        f.write("## Actionable Insights\n\n")

        f.write("### Priority Rules for LLM Focus\n\n")
        f.write("When these rules are detected, the LLM should prioritize optimization efforts:\n\n")

        priority_rules = [r for r in rule_scores if float(r['win_rate']) > 0.5 or
                         (float(r['avg_speedup']) > 1.3 and int(r['appearances']) >= 3)][:10]

        for i, rule in enumerate(priority_rules, 1):
            f.write(f"{i}. **`{rule['rule']}`** - {float(rule['win_rate'])*100:.0f}% win rate, "
                   f"{float(rule['avg_speedup']):.2f}x avg speedup\n")
        f.write("\n")

        f.write("### Rules Requiring Caution\n\n")
        f.write("When these rules are detected, carefully validate optimizations:\n\n")

        caution_rules = [r for r in rule_scores if float(r['loss_rate']) > 0.5 and int(r['appearances']) >= 3]

        for i, rule in enumerate(caution_rules, 1):
            f.write(f"{i}. **`{rule['rule']}`** - {float(rule['loss_rate'])*100:.0f}% loss rate, "
                   f"{float(rule['avg_speedup']):.2f}x avg speedup\n")
        f.write("\n")

        f.write("### Optimization Strategy Recommendations\n\n")
        f.write("1. **High Priority Patterns**: Focus on queries with correlated subqueries (SQL-SUB-001) "
               "and aggregate-after-join patterns (QT-AGG-002 + QT-OPT-002)\n\n")
        f.write("2. **Pre-computation Strategy**: Queries with QT-OPT-002 (Correlated Subquery to CTE) "
               "show 1.57x average speedup - prioritize CTE extraction\n\n")
        f.write("3. **Join Reordering**: QT-OPT-009 appears in 39 queries but only 20.5% win rate - "
               "be selective about when to reorder joins\n\n")
        f.write("4. **Risk Mitigation**: Queries with SQL-DUCK-001, SQL-ORD-001, or QT-PLAN-001 "
               "have >60% loss rate - use conservative optimizations\n\n")

        # Statistical summary
        f.write("## Statistical Summary\n\n")
        f.write(f"- **Total Rules Analyzed:** {len(rule_scores)}\n")
        f.write(f"- **Rules with >50% Win Rate:** {len([r for r in rule_scores if float(r['win_rate']) > 0.5])}\n")
        f.write(f"- **Rules with >50% Loss Rate:** {len([r for r in rule_scores if float(r['loss_rate']) > 0.5])}\n")
        f.write(f"- **Rule Combinations Analyzed:** {len(combinations['rule_combinations'])}\n")
        f.write(f"- **Combinations with >70% Win Rate:** "
               f"{len([c for c in combinations['rule_combinations'] if c['win_rate'] > 0.7])}\n\n")

        # Correlation matrix would go here if we had more complex stats

        f.write("---\n\n")
        f.write("*Generated by enhanced_rule_analysis.py*\n")

def main():
    """Main analysis pipeline."""
    print("=" * 60)
    print("Enhanced Rule Effectiveness Analysis")
    print("=" * 60)

    # Load data
    ast_data, benchmark_data, rule_scores, combinations = load_data()

    # Extract query details
    query_details = extract_query_details(ast_data, benchmark_data)
    print(f"Extracted details for {len(query_details)} queries")

    # Analyze by category
    categories = analyze_by_speedup_category(query_details)
    print(f"Categorized queries into {len(categories)} speedup categories")

    # Find exclusive patterns
    exclusive_wins, exclusive_losses = find_exclusive_patterns(query_details)
    print(f"Found {len(exclusive_wins)} win-biased rules, {len(exclusive_losses)} loss-biased rules")

    # Write enhanced report
    write_enhanced_report(query_details, categories, rule_scores, combinations, exclusive_wins, exclusive_losses)

    print("=" * 60)
    print("Enhanced Analysis Complete!")
    print("=" * 60)
    print(f"\nOutput: {ENHANCED_OUTPUT}")
    print()

if __name__ == "__main__":
    main()
