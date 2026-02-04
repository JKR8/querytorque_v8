#!/usr/bin/env python3
"""
Analyze correlation between AST detector rules and query optimization speedups.
"""

import json
import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple
import statistics

# File paths
AST_ANALYSIS_PATH = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/docs/tpcds_ast_analysis.json")
BENCHMARK_PATH = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/research/experiments/benchmarks/kimi_benchmark_20260202_221828/summary.json")
OUTPUT_DIR = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8/docs")

# Output paths
CSV_OUTPUT = OUTPUT_DIR / "rule_effectiveness_analysis.csv"
MD_OUTPUT = OUTPUT_DIR / "rule_effectiveness_report.md"
JSON_OUTPUT = OUTPUT_DIR / "rule_combinations.json"

def load_ast_analysis() -> Dict:
    """Load AST analysis results."""
    print("Loading AST analysis...")
    with open(AST_ANALYSIS_PATH, 'r') as f:
        return json.load(f)

def load_benchmark_results() -> Dict:
    """Load benchmark results."""
    print("Loading benchmark results...")
    with open(BENCHMARK_PATH, 'r') as f:
        return json.load(f)

def extract_query_data(ast_data: Dict, benchmark_data: Dict) -> Dict:
    """
    Extract and correlate query data.
    Returns: {query_id: {rules: [], speedup: float, status: str}}
    """
    print("Extracting and correlating query data...")

    query_data = {}

    # Get benchmark results - query is an integer
    benchmark_results = {}
    if 'results' in benchmark_data:
        for result in benchmark_data['results']:
            query_num = result.get('query', 0)
            speedup = result.get('speedup', 0.0)
            status = result.get('status', 'unknown')
            benchmark_results[query_num] = {
                'speedup': speedup,
                'status': status,
                'original_time': result.get('original_ms', 0),
                'optimized_time': result.get('optimized_ms', 0)
            }

    # Match with AST analysis - AST uses "query_N" format
    ast_queries = ast_data.get('queries', {})

    for query_key, findings in ast_queries.items():
        # Extract query number from "query_1" -> 1
        if query_key.startswith('query_'):
            query_num = int(query_key.split('_')[1])
        else:
            continue

        if query_num in benchmark_results:
            rules = []

            # Extract triggered rules from findings
            for finding in findings:
                rule_id = finding.get('rule_id', '')
                if rule_id and rule_id not in rules:
                    rules.append(rule_id)

            query_data[f"q{query_num}"] = {
                'query_num': query_num,
                'rules': rules,
                'speedup': benchmark_results[query_num]['speedup'],
                'status': benchmark_results[query_num]['status'],
                'original_time': benchmark_results[query_num]['original_time'],
                'optimized_time': benchmark_results[query_num]['optimized_time']
            }

    print(f"Matched {len(query_data)} queries with both AST and benchmark data")
    return query_data

def calculate_rule_scores(query_data: Dict) -> List[Dict]:
    """
    Calculate effectiveness scores for each rule.
    Returns list of dicts with rule statistics.
    """
    print("Calculating rule scores...")

    rule_stats = defaultdict(lambda: {
        'appearances': 0,
        'wins': 0,  # >1.2x speedup
        'losses': 0,  # <1.0x speedup
        'neutrals': 0,  # 1.0-1.2x
        'speedups': [],
        'query_count': 0
    })

    # Collect statistics
    for query_id, data in query_data.items():
        speedup = data['speedup']
        rules = data['rules']

        for rule in rules:
            stats = rule_stats[rule]
            stats['appearances'] += 1
            stats['speedups'].append(speedup)
            stats['query_count'] += 1

            if speedup > 1.2:
                stats['wins'] += 1
            elif speedup < 1.0:
                stats['losses'] += 1
            else:
                stats['neutrals'] += 1

    # Calculate derived metrics
    results = []
    for rule, stats in rule_stats.items():
        total = stats['appearances']
        if total == 0:
            continue

        avg_speedup = statistics.mean(stats['speedups']) if stats['speedups'] else 0
        median_speedup = statistics.median(stats['speedups']) if stats['speedups'] else 0
        stdev_speedup = statistics.stdev(stats['speedups']) if len(stats['speedups']) > 1 else 0

        # Predictive score: wins / total appearances
        predictive_score = stats['wins'] / total if total > 0 else 0

        # Win rate
        win_rate = stats['wins'] / total if total > 0 else 0
        loss_rate = stats['losses'] / total if total > 0 else 0

        results.append({
            'rule': rule,
            'appearances': total,
            'wins': stats['wins'],
            'losses': stats['losses'],
            'neutrals': stats['neutrals'],
            'win_rate': win_rate,
            'loss_rate': loss_rate,
            'avg_speedup': avg_speedup,
            'median_speedup': median_speedup,
            'stdev_speedup': stdev_speedup,
            'predictive_score': predictive_score,
            'net_score': stats['wins'] - stats['losses']
        })

    # Sort by predictive score
    results.sort(key=lambda x: x['predictive_score'], reverse=True)

    print(f"Analyzed {len(results)} unique rules")
    return results

def find_rule_combinations(query_data: Dict, min_appearances: int = 3) -> List[Dict]:
    """
    Find rule combinations (2-3 rules) that predict successful optimization.
    """
    print("Finding rule combinations...")

    combo_stats = defaultdict(lambda: {
        'appearances': 0,
        'wins': 0,
        'losses': 0,
        'speedups': []
    })

    for query_id, data in query_data.items():
        speedup = data['speedup']
        rules = sorted(data['rules'])  # Sort for consistent combinations

        # 2-rule combinations
        for i in range(len(rules)):
            for j in range(i + 1, len(rules)):
                combo = f"{rules[i]} + {rules[j]}"
                stats = combo_stats[combo]
                stats['appearances'] += 1
                stats['speedups'].append(speedup)

                if speedup > 1.2:
                    stats['wins'] += 1
                elif speedup < 1.0:
                    stats['losses'] += 1

        # 3-rule combinations
        for i in range(len(rules)):
            for j in range(i + 1, len(rules)):
                for k in range(j + 1, len(rules)):
                    combo = f"{rules[i]} + {rules[j]} + {rules[k]}"
                    stats = combo_stats[combo]
                    stats['appearances'] += 1
                    stats['speedups'].append(speedup)

                    if speedup > 1.2:
                        stats['wins'] += 1
                    elif speedup < 1.0:
                        stats['losses'] += 1

    # Filter and calculate metrics
    results = []
    for combo, stats in combo_stats.items():
        if stats['appearances'] < min_appearances:
            continue

        total = stats['appearances']
        avg_speedup = statistics.mean(stats['speedups']) if stats['speedups'] else 0
        predictive_score = stats['wins'] / total if total > 0 else 0
        win_rate = stats['wins'] / total if total > 0 else 0

        results.append({
            'combination': combo,
            'appearances': total,
            'wins': stats['wins'],
            'losses': stats['losses'],
            'win_rate': win_rate,
            'avg_speedup': avg_speedup,
            'predictive_score': predictive_score,
            'net_score': stats['wins'] - stats['losses']
        })

    # Sort by predictive score, then by appearances
    results.sort(key=lambda x: (x['predictive_score'], x['appearances']), reverse=True)

    print(f"Found {len(results)} rule combinations with {min_appearances}+ appearances")
    return results[:20]  # Top 20

def write_csv_report(rule_scores: List[Dict]):
    """Write CSV report with rule scores."""
    print(f"Writing CSV report to {CSV_OUTPUT}...")

    with open(CSV_OUTPUT, 'w', newline='') as f:
        fieldnames = [
            'rule', 'appearances', 'wins', 'losses', 'neutrals',
            'win_rate', 'loss_rate', 'avg_speedup', 'median_speedup',
            'stdev_speedup', 'predictive_score', 'net_score'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rule_scores:
            writer.writerow(row)

def write_markdown_report(rule_scores: List[Dict], combinations: List[Dict], query_data: Dict):
    """Write markdown report with analysis."""
    print(f"Writing markdown report to {MD_OUTPUT}...")

    # Calculate summary statistics
    total_queries = len(query_data)
    wins = sum(1 for q in query_data.values() if q['speedup'] > 1.2)
    losses = sum(1 for q in query_data.values() if q['speedup'] < 1.0)
    neutrals = total_queries - wins - losses
    avg_speedup = statistics.mean([q['speedup'] for q in query_data.values()])

    with open(MD_OUTPUT, 'w') as f:
        f.write("# Rule Effectiveness Analysis Report\n\n")
        f.write(f"**Generated:** {Path(__file__).name}\n\n")
        f.write("## Summary\n\n")
        f.write(f"- **Total Queries Analyzed:** {total_queries}\n")
        if total_queries > 0:
            f.write(f"- **Wins (>1.2x speedup):** {wins} ({wins/total_queries*100:.1f}%)\n")
            f.write(f"- **Losses (<1.0x speedup):** {losses} ({losses/total_queries*100:.1f}%)\n")
            f.write(f"- **Neutral (1.0-1.2x):** {neutrals} ({neutrals/total_queries*100:.1f}%)\n")
            f.write(f"- **Average Speedup:** {avg_speedup:.2f}x\n")
        f.write(f"- **Unique Rules Detected:** {len(rule_scores)}\n\n")

        # Top performing rules
        f.write("## Top 20 Rules by Predictive Score\n\n")
        f.write("Rules most likely to indicate successful optimization (>1.2x speedup).\n\n")
        f.write("| Rank | Rule | Appearances | Win Rate | Avg Speedup | Predictive Score | Net Score |\n")
        f.write("|------|------|-------------|----------|-------------|------------------|----------|\n")

        for i, rule in enumerate(rule_scores[:20], 1):
            f.write(f"| {i} | `{rule['rule']}` | {rule['appearances']} | "
                   f"{rule['win_rate']*100:.1f}% | {rule['avg_speedup']:.2f}x | "
                   f"{rule['predictive_score']:.3f} | {rule['net_score']:+d} |\n")

        # Bottom 20 rules (highest loss rate)
        f.write("\n## Bottom 20 Rules by Win Rate\n\n")
        f.write("Rules most associated with regressions or neutral results.\n\n")
        f.write("| Rank | Rule | Appearances | Win Rate | Loss Rate | Avg Speedup | Net Score |\n")
        f.write("|------|------|-------------|----------|-----------|-------------|----------|\n")

        sorted_by_win_rate = sorted(rule_scores, key=lambda x: x['win_rate'])
        for i, rule in enumerate(sorted_by_win_rate[:20], 1):
            f.write(f"| {i} | `{rule['rule']}` | {rule['appearances']} | "
                   f"{rule['win_rate']*100:.1f}% | {rule['loss_rate']*100:.1f}% | "
                   f"{rule['avg_speedup']:.2f}x | {rule['net_score']:+d} |\n")

        # Rule combinations
        f.write("\n## Top 20 Rule Combinations\n\n")
        f.write("Rule combinations most predictive of successful optimization.\n\n")
        f.write("| Rank | Combination | Appearances | Win Rate | Avg Speedup | Net Score |\n")
        f.write("|------|-------------|-------------|----------|-------------|----------|\n")

        for i, combo in enumerate(combinations, 1):
            f.write(f"| {i} | `{combo['combination']}` | {combo['appearances']} | "
                   f"{combo['win_rate']*100:.1f}% | {combo['avg_speedup']:.2f}x | "
                   f"{combo['net_score']:+d} |\n")

        # Statistical insights
        f.write("\n## Statistical Insights\n\n")

        # High-value rules (>70% win rate, 5+ appearances)
        high_value = [r for r in rule_scores if r['win_rate'] > 0.7 and r['appearances'] >= 5]
        f.write(f"### High-Value Rules ({len(high_value)} rules)\n\n")
        f.write("Rules with >70% win rate and 5+ appearances:\n\n")
        for rule in high_value:
            f.write(f"- **`{rule['rule']}`**: {rule['wins']}/{rule['appearances']} wins "
                   f"({rule['win_rate']*100:.1f}%), avg {rule['avg_speedup']:.2f}x speedup\n")

        # Risky rules (>50% loss rate, 5+ appearances)
        risky = [r for r in rule_scores if r['loss_rate'] > 0.5 and r['appearances'] >= 5]
        f.write(f"\n### Risky Rules ({len(risky)} rules)\n\n")
        f.write("Rules with >50% loss rate and 5+ appearances:\n\n")
        for rule in risky:
            f.write(f"- **`{rule['rule']}`**: {rule['losses']}/{rule['appearances']} losses "
                   f"({rule['loss_rate']*100:.1f}%), avg {rule['avg_speedup']:.2f}x speedup\n")

        # Recommendations
        f.write("\n## Recommendations\n\n")
        f.write("### Prioritize These Rules for Optimization\n\n")
        f.write("Focus LLM attention on queries where these high-value rules are detected:\n\n")
        for i, rule in enumerate(rule_scores[:10], 1):
            if rule['appearances'] >= 3:
                f.write(f"{i}. **`{rule['rule']}`** - {rule['win_rate']*100:.0f}% win rate, "
                       f"{rule['avg_speedup']:.2f}x avg speedup\n")

        f.write("\n### Use Caution with These Rules\n\n")
        f.write("These rules may indicate queries that are harder to optimize or have edge cases:\n\n")
        caution_rules = [r for r in sorted_by_win_rate[:10] if r['appearances'] >= 3]
        for i, rule in enumerate(caution_rules, 1):
            f.write(f"{i}. **`{rule['rule']}`** - {rule['win_rate']*100:.0f}% win rate, "
                   f"{rule['loss_rate']*100:.0f}% loss rate\n")

        f.write("\n### Rule Combination Insights\n\n")
        f.write("When these rule combinations appear together, optimization success is highly likely:\n\n")
        for i, combo in enumerate(combinations[:5], 1):
            if combo['win_rate'] > 0.7:
                f.write(f"{i}. **`{combo['combination']}`** - {combo['win_rate']*100:.0f}% win rate, "
                       f"{combo['avg_speedup']:.2f}x avg speedup\n")

def write_json_report(combinations: List[Dict]):
    """Write JSON report with rule combinations."""
    print(f"Writing JSON report to {JSON_OUTPUT}...")

    with open(JSON_OUTPUT, 'w') as f:
        json.dump({
            'rule_combinations': combinations,
            'metadata': {
                'min_appearances': 3,
                'top_n': 20,
                'win_threshold': 1.2,
                'loss_threshold': 1.0
            }
        }, f, indent=2)

def main():
    """Main analysis pipeline."""
    print("=" * 60)
    print("Rule Effectiveness Analysis")
    print("=" * 60)

    # Load data
    ast_data = load_ast_analysis()
    benchmark_data = load_benchmark_results()

    # Extract and correlate
    query_data = extract_query_data(ast_data, benchmark_data)

    # Calculate rule scores
    rule_scores = calculate_rule_scores(query_data)

    # Find rule combinations
    combinations = find_rule_combinations(query_data)

    # Write reports
    write_csv_report(rule_scores)
    write_markdown_report(rule_scores, combinations, query_data)
    write_json_report(combinations)

    print("=" * 60)
    print("Analysis Complete!")
    print("=" * 60)
    print(f"\nOutputs:")
    print(f"  - CSV: {CSV_OUTPUT}")
    print(f"  - Markdown: {MD_OUTPUT}")
    print(f"  - JSON: {JSON_OUTPUT}")
    print()
    print(f"Summary:")
    print(f"  - {len(query_data)} queries analyzed")
    print(f"  - {len(rule_scores)} unique rules")
    print(f"  - {len(combinations)} top rule combinations")
    print()

if __name__ == "__main__":
    main()
