#!/usr/bin/env python3
"""
Generate optimization prompts for all 99 TPC-DS queries.

For each query, creates a prompt combining:
1. The current SQL (optimized or baseline)
2. EXPLAIN ANALYZE plan from SF10
3. Attempt history (what worked, what failed, what had no effect)
4. Structurally-matched gold pattern example(s)
5. Specific recommendation based on state analysis

Output: research/state/prompts/qN_prompt.txt
"""

import json
import csv
import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from collections import defaultdict
import sys
import textwrap

# ============================================================================
# PATHS
# ============================================================================

PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
STATE_DIR = PROJECT / "research" / "state_histories_all_99"
CSV_PATH = PROJECT / "research" / "CONSOLIDATED_BENCHMARKS" / "DuckDB_TPC-DS_Master_v2_20260206.csv"
EXAMPLES_DIR = PROJECT / "packages" / "qt-sql" / "qt_sql" / "optimization" / "examples"
EXPLAIN_DIR = PROJECT / "research" / "state" / "explain_plans"
QUERIES_DIR = PROJECT / "research" / "state" / "queries"
OUTPUT_DIR = PROJECT / "research" / "state" / "prompts"

# ============================================================================
# QUERY FEATURES (from generate_state_analysis.py)
# ============================================================================

TPCDS_QUERY_FEATURES = {
    1:  ["correlated_subquery", "date_filter", "dim_fact_chain"],
    2:  ["date_filter", "complex_multi_join"],
    3:  ["date_filter", "dim_fact_chain"],
    4:  ["multi_date_alias", "complex_multi_join", "correlated_subquery"],
    5:  ["date_filter", "union_year", "multi_dim_filter"],
    6:  ["correlated_subquery", "date_filter"],
    7:  ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    8:  ["correlated_subquery", "date_filter"],
    9:  ["repeated_scan"],
    10: ["date_filter", "multi_dim_filter", "exists_repeat"],
    11: ["correlated_subquery", "date_filter", "multi_date_alias"],
    12: ["date_filter", "dim_fact_chain"],
    13: ["multi_dim_filter", "dim_fact_chain"],
    14: ["intersect", "date_filter", "complex_multi_join"],
    15: ["or_condition", "date_filter"],
    16: ["date_filter", "exists_repeat", "complex_multi_join"],
    17: ["multi_date_alias", "dim_fact_chain"],
    18: ["date_filter", "multi_dim_filter"],
    19: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    20: ["date_filter", "dim_fact_chain"],
    21: ["date_filter", "dim_fact_chain"],
    22: ["date_filter", "window_fn"],
    23: ["correlated_subquery", "date_filter", "complex_multi_join"],
    24: ["correlated_subquery", "complex_multi_join"],
    25: ["multi_date_alias", "dim_fact_chain"],
    26: ["multi_dim_filter", "dim_fact_chain"],
    27: ["multi_dim_filter", "dim_fact_chain"],
    28: ["repeated_scan"],
    29: ["multi_date_alias", "dim_fact_chain"],
    30: ["correlated_subquery", "date_filter"],
    31: ["date_filter", "complex_multi_join"],
    32: ["correlated_subquery", "date_filter"],
    33: ["date_filter", "multi_dim_filter", "union_year"],
    34: ["multi_dim_filter", "dim_fact_chain"],
    35: ["date_filter", "exists_repeat", "multi_dim_filter"],
    36: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    37: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    38: ["date_filter", "intersect", "complex_multi_join"],
    39: ["date_filter", "dim_fact_chain"],
    40: ["date_filter", "dim_fact_chain"],
    41: ["correlated_subquery"],
    42: ["date_filter", "dim_fact_chain"],
    43: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    44: ["repeated_scan", "window_fn"],
    45: ["or_condition", "date_filter", "correlated_subquery"],
    46: ["multi_dim_filter", "dim_fact_chain"],
    47: ["date_filter", "dim_fact_chain", "window_fn"],
    48: ["multi_dim_filter", "or_condition"],
    49: ["date_filter", "union_year", "window_fn"],
    50: ["date_filter", "dim_fact_chain"],
    51: ["date_filter", "window_fn"],
    52: ["date_filter", "dim_fact_chain"],
    53: ["date_filter", "dim_fact_chain"],
    54: ["date_filter", "complex_multi_join"],
    55: ["date_filter", "dim_fact_chain"],
    56: ["multi_dim_filter", "union_year"],
    57: ["date_filter", "dim_fact_chain", "window_fn"],
    58: ["date_filter", "multi_date_alias"],
    59: ["date_filter", "complex_multi_join"],
    60: ["date_filter", "multi_dim_filter", "union_year"],
    61: ["date_filter", "multi_dim_filter"],
    62: ["date_filter", "dim_fact_chain"],
    63: ["date_filter", "dim_fact_chain"],
    64: ["complex_multi_join", "multi_date_alias"],
    65: ["date_filter", "dim_fact_chain"],
    66: ["date_filter", "multi_dim_filter", "union_year"],
    67: ["date_filter", "dim_fact_chain", "window_fn"],
    68: ["multi_dim_filter", "dim_fact_chain"],
    69: ["multi_dim_filter", "dim_fact_chain"],
    70: ["date_filter", "window_fn", "correlated_subquery"],
    71: ["date_filter", "multi_dim_filter", "dim_fact_chain"],
    72: ["multi_date_alias", "multi_dim_filter", "complex_multi_join"],
    73: ["multi_dim_filter", "dim_fact_chain"],
    74: ["union_year", "date_filter"],
    75: ["date_filter", "union_year"],
    76: ["date_filter", "or_condition", "union_year"],
    77: ["date_filter", "union_year"],
    78: ["date_filter", "complex_multi_join"],
    79: ["multi_dim_filter", "dim_fact_chain"],
    80: ["date_filter", "multi_date_alias", "union_year"],
    81: ["correlated_subquery", "date_filter"],
    82: ["multi_dim_filter", "dim_fact_chain"],
    83: ["multi_dim_filter", "dim_fact_chain"],
    84: ["multi_dim_filter", "dim_fact_chain"],
    85: ["multi_dim_filter", "dim_fact_chain"],
    86: ["multi_dim_filter", "dim_fact_chain"],
    87: ["date_filter", "multi_dim_filter", "complex_multi_join"],
    88: ["repeated_scan", "or_condition"],
    89: ["date_filter", "dim_fact_chain"],
    90: ["date_filter", "dim_fact_chain"],
    91: ["date_filter", "dim_fact_chain"],
    92: ["date_filter", "dim_fact_chain"],
    93: ["dim_fact_chain"],
    94: ["date_filter", "exists_repeat", "complex_multi_join"],
    95: ["exists_repeat", "date_filter"],
    96: ["multi_dim_filter", "dim_fact_chain"],
    97: ["date_filter", "union_year"],
    98: ["date_filter", "dim_fact_chain"],
    99: ["date_filter", "multi_dim_filter"],
}

FEATURE_TO_PATTERN = {
    "correlated_subquery": ["decorrelate", "date_cte_isolate"],
    "date_filter":         ["date_cte_isolate", "prefetch_fact_join"],
    "multi_date_alias":    ["multi_date_range_cte"],
    "or_condition":        ["or_to_union"],
    "multi_dim_filter":    ["dimension_cte_isolate", "multi_dimension_prefetch", "early_filter"],
    "dim_fact_chain":      ["prefetch_fact_join", "early_filter", "multi_dimension_prefetch"],
    "repeated_scan":       ["single_pass_aggregation", "pushdown"],
    "intersect":           ["intersect_to_exists"],
    "union_year":          ["union_cte_split"],
    "exists_repeat":       ["materialize_cte"],
    "complex_multi_join":  [],
    "window_fn":           [],
}


# ============================================================================
# DATA LOADING
# ============================================================================

def load_state_histories() -> Dict[int, dict]:
    """Load all 99 query state histories from YAML"""
    states = {}
    for yaml_file in sorted(STATE_DIR.glob("q*_state_history.yaml")):
        try:
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
            q = data['query_num']
            states[q] = data
        except Exception as e:
            print(f"Warning: {yaml_file}: {e}", file=sys.stderr)
    return states


def load_csv_data() -> Dict[int, dict]:
    """Load master leaderboard CSV"""
    csv_data = {}
    if CSV_PATH.exists():
        with open(CSV_PATH) as f:
            reader = csv.DictReader(f)
            for row in reader:
                q = int(row['Query_Num'])
                csv_data[q] = row
    return csv_data


def load_gold_patterns() -> Dict[str, dict]:
    """Load all gold pattern examples"""
    patterns = {}
    for json_file in EXAMPLES_DIR.glob("*.json"):
        try:
            with open(json_file) as f:
                data = json.load(f)
            patterns[data['id']] = data
        except Exception as e:
            print(f"Warning: {json_file}: {e}", file=sys.stderr)
    return patterns


def get_matching_patterns(query_num: int) -> List[str]:
    """Get gold patterns that match query's structural features"""
    features = TPCDS_QUERY_FEATURES.get(query_num, [])
    matched = []
    seen = set()
    for feature in features:
        for pattern in FEATURE_TO_PATTERN.get(feature, []):
            if pattern not in seen:
                matched.append(pattern)
                seen.add(pattern)
    return matched


def get_explain_reasons(query_num: int) -> List[str]:
    """Return EXPLAIN ANALYZE investigation reasons"""
    features = TPCDS_QUERY_FEATURES.get(query_num, [])
    reasons = []
    if "complex_multi_join" in features:
        reasons.append("Complex multi-join: Check join ordering and cardinality estimates")
    if "window_fn" in features:
        reasons.append("Window functions: Check for full sort on grouped TopN")
    return reasons


# ============================================================================
# PROMPT GENERATION
# ============================================================================

def build_attempt_history(state_data: dict) -> str:
    """Build the attempt history section for a query"""
    lines = []
    best_speedup = state_data.get('best_speedup', 1.0)

    transforms_tried = set()
    transforms_succeeded = set()
    transforms_failed = set()
    transforms_neutral = set()

    state_order = ['baseline', 'kimi', 'v2_standard', 'retry3w_1', 'retry3w_2',
                   'retry3w_3', 'retry3w_4', 'W1', 'W2', 'W3', 'W4']
    shown = set()

    for state in state_data.get('states', []):
        state_id = state.get('state_id', 'unknown')
        speedup = state.get('speedup', 1.0)
        transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
        status = state.get('status', 'unknown')
        error = state.get('error', '')

        for t in transforms:
            transforms_tried.add(t)
            if status == 'success' and speedup > 1.1:
                transforms_succeeded.add(t)
            elif status in ('error', 'regression') or speedup < 0.95:
                transforms_failed.add(t)
            else:
                transforms_neutral.add(t)

    # Format history
    for state_id in state_order:
        for state in state_data.get('states', []):
            if state.get('state_id') == state_id and state_id not in shown:
                speedup = state.get('speedup', 1.0)
                transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
                status = state.get('status', 'unknown')
                t_str = ", ".join(transforms) if transforms else "none"
                lines.append(f"  - {state_id}: {speedup:.2f}x [{t_str}] {status}")
                shown.add(state_id)

    # Any remaining states not in standard order
    for state in state_data.get('states', []):
        state_id = state.get('state_id', 'unknown')
        if state_id not in shown:
            speedup = state.get('speedup', 1.0)
            transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
            status = state.get('status', 'unknown')
            t_str = ", ".join(transforms) if transforms else "none"
            lines.append(f"  - {state_id}: {speedup:.2f}x [{t_str}] {status}")

    # Learning record
    if transforms_tried:
        lines.append("")
        lines.append("  LEARNING RECORD:")
        if transforms_succeeded:
            lines.append(f"    Worked: {', '.join(sorted(transforms_succeeded))}")
        if transforms_neutral - transforms_succeeded - transforms_failed:
            neutral = transforms_neutral - transforms_succeeded - transforms_failed
            lines.append(f"    No effect: {', '.join(sorted(neutral))}")
        if transforms_failed:
            lines.append(f"    Failed/Regression: {', '.join(sorted(transforms_failed))}")

    return '\n'.join(lines), transforms_tried, transforms_succeeded, transforms_failed


def build_gold_example_section(pattern_ids: List[str], patterns: Dict[str, dict]) -> str:
    """Build the gold example section with full example details"""
    if not pattern_ids:
        return ""

    lines = []
    for pid in pattern_ids:
        if pid not in patterns:
            continue
        p = patterns[pid]
        example = p.get('example', {})
        output = example.get('output', {})
        rewrite_sets = output.get('rewrite_sets', [{}])
        rs = rewrite_sets[0] if rewrite_sets else {}

        lines.append(f"### Gold Pattern: {p['name']} ({p['id']})")
        lines.append(f"Verified speedup: {p.get('verified_speedup', 'N/A')}")
        lines.append(f"Description: {p.get('description', '')}")
        lines.append(f"Key insight: {example.get('key_insight', '')}")
        lines.append("")

        # Show the input/output structure
        input_slice = example.get('input_slice', '')
        if input_slice:
            lines.append("**Before (pattern):**")
            lines.append("```sql")
            lines.append(input_slice)
            lines.append("```")
            lines.append("")

        nodes = rs.get('nodes', {})
        if nodes:
            lines.append("**After (rewritten pattern):**")
            lines.append("```sql")
            for node_name, node_sql in nodes.items():
                lines.append(f"-- CTE: {node_name}")
                lines.append(node_sql)
                lines.append("")
            lines.append("```")
            lines.append("")

    return '\n'.join(lines)


def build_recommendation_section(
    query_num: int,
    transforms_tried: set,
    transforms_succeeded: set,
    transforms_failed: set,
    explain_available: bool,
) -> str:
    """Build specific recommendations based on structural analysis"""
    matched = get_matching_patterns(query_num)
    explain_reasons = get_explain_reasons(query_num)

    lines = []

    # Untried structural matches (highest priority)
    untried = [p for p in matched if p not in transforms_tried]
    retry_candidates = [p for p in matched if p in transforms_tried and p not in transforms_failed and p not in transforms_succeeded]

    if untried:
        lines.append("PRIMARY RECOMMENDATIONS (untried patterns matching query structure):")
        for i, p in enumerate(untried, 1):
            features = TPCDS_QUERY_FEATURES.get(query_num, [])
            matching_features = [f for f in features if p in FEATURE_TO_PATTERN.get(f, [])]
            lines.append(f"  {i}. {p} - Query has {', '.join(matching_features)} structure")

    if retry_candidates:
        lines.append("")
        lines.append("SECONDARY (tried with no effect, may work with different approach):")
        for p in retry_candidates:
            lines.append(f"  - {p} (previously neutral - consider combining with other transforms)")

    if explain_reasons:
        lines.append("")
        lines.append("EXPLAIN ANALYZE INVESTIGATION AREAS:")
        for reason in explain_reasons:
            lines.append(f"  - {reason}")

    if not untried and not retry_candidates and not explain_reasons:
        lines.append("All matched patterns tried. Focus on EXPLAIN ANALYZE to find novel approaches.")
        lines.append("Look for: cardinality misestimates, unnecessary sorts, redundant scans, join reordering opportunities.")

    return '\n'.join(lines)


def generate_prompt(
    query_num: int,
    state_data: dict,
    csv_row: dict,
    patterns: Dict[str, dict],
) -> str:
    """Generate the full optimization prompt for one query"""

    best_speedup = state_data.get('best_speedup', 1.0)
    original_ms = 0.0
    try:
        original_ms = float(csv_row.get('Kimi_Original_ms', 0) or 0)
    except:
        pass

    # Determine classification
    if best_speedup >= 1.5:
        category = "WIN"
    elif best_speedup >= 1.1:
        category = "IMPROVED"
    elif best_speedup >= 0.95:
        category = "NEUTRAL"
    else:
        category = "REGRESSION"

    # Current state description
    if best_speedup >= 1.1:
        state_desc = f"OPTIMIZED at {best_speedup:.2f}x - looking for further improvement"
        starting_point = "current optimized version"
    else:
        state_desc = f"UNOPTIMIZED (baseline) - looking for initial optimization"
        starting_point = "original baseline SQL"

    # Load EXPLAIN plan
    explain_path = EXPLAIN_DIR / f"q{query_num}_explain.txt"
    explain_text = ""
    if explain_path.exists():
        explain_text = explain_path.read_text()

    # Load current SQL
    query_path = QUERIES_DIR / f"q{query_num}_current.sql"
    current_sql = ""
    if query_path.exists():
        current_sql = query_path.read_text()

    # Build attempt history
    history_text, transforms_tried, transforms_succeeded, transforms_failed = build_attempt_history(state_data)

    # Get structural matches
    matched_patterns = get_matching_patterns(query_num)
    untried_patterns = [p for p in matched_patterns if p not in transforms_tried]

    # Decide which gold examples to include (top 2 most relevant untried)
    example_patterns = untried_patterns[:2]
    if not example_patterns and matched_patterns:
        # All tried - show the most successful one as reference
        for p in matched_patterns:
            if p in transforms_succeeded:
                example_patterns = [p]
                break
    if not example_patterns and matched_patterns:
        example_patterns = matched_patterns[:1]

    gold_section = build_gold_example_section(example_patterns, patterns)
    recommendation_section = build_recommendation_section(
        query_num, transforms_tried, transforms_succeeded, transforms_failed,
        explain_available=bool(explain_text)
    )

    # ====================================================================
    # BUILD THE PROMPT
    # ====================================================================

    prompt = f"""# TPC-DS Q{query_num} Optimization Task

## Current State
- **Status**: {category} ({state_desc})
- **Starting point**: {starting_point}
- **Baseline runtime**: {original_ms:.0f}ms (SF10, DuckDB)
- **Best achieved**: {best_speedup:.2f}x speedup
- **Target**: Achieve measurable speedup (>1.1x validated via 3-run mean)

## Objective
Rewrite the SQL below to run faster on DuckDB (TPC-DS SF10).
The rewrite MUST return identical results (same rows, same values, same ordering).
Do NOT add LIMIT, remove ORDER BY, change aggregation logic, or alter semantics.

## Previous Attempts (CRITICAL - learn from history)
{history_text}

## Structural Analysis
Features detected in Q{query_num}: {', '.join(TPCDS_QUERY_FEATURES.get(query_num, ['unknown']))}
Matching gold patterns: {', '.join(matched_patterns) if matched_patterns else 'None - needs novel approach'}
Untried patterns: {', '.join(untried_patterns) if untried_patterns else 'All structural matches tried'}

## Recommendations
{recommendation_section}

"""

    if gold_section:
        prompt += f"""## Gold Pattern Examples (verified working on similar queries)
{gold_section}
"""

    prompt += f"""## Current SQL (starting point for optimization)
```sql
{current_sql}
```

## EXPLAIN ANALYZE (SF10 execution plan)
```
{explain_text}
```

## Instructions
1. Study the EXPLAIN ANALYZE plan above for bottlenecks:
   - Look for high-cardinality scans that could be pre-filtered
   - Look for nested loop joins that should be hash joins
   - Look for redundant scans of the same table
   - Look for sorts that could be avoided
   - Look for cardinality misestimates (actual vs estimated rows)

2. Apply the recommended gold pattern(s) if applicable, or devise a new approach based on the EXPLAIN plan

3. Output a single complete SQL query (not fragments) that:
   - Returns identical results to the original
   - Uses CTEs to restructure the computation
   - Targets the specific bottleneck identified in the EXPLAIN plan

4. CONSTRAINTS:
   - Do NOT use patterns that already FAILED on this query: {', '.join(sorted(transforms_failed)) if transforms_failed else 'none'}
   - If using or_to_union, limit to ≤3 UNION ALL branches (>3 causes severe regressions)
   - Output must be valid DuckDB SQL (not PostgreSQL-specific syntax)
   - Do NOT add extra columns, change column names, or modify ORDER BY

5. Output format: Return ONLY the optimized SQL query, no explanation needed.
"""

    return prompt


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading data sources...", file=sys.stderr)
    state_histories = load_state_histories()
    csv_data = load_csv_data()
    patterns = load_gold_patterns()

    print(f"Loaded: {len(state_histories)} states, {len(csv_data)} CSV rows, {len(patterns)} gold patterns", file=sys.stderr)

    generated = 0
    for q in range(1, 100):
        state_data = state_histories.get(q)
        csv_row = csv_data.get(q, {})

        if not state_data:
            print(f"Q{q}: SKIP - no state history", file=sys.stderr)
            continue

        prompt = generate_prompt(q, state_data, csv_row, patterns)

        output_path = OUTPUT_DIR / f"q{q}_prompt.txt"
        with open(output_path, 'w') as f:
            f.write(prompt)

        best = state_data.get('best_speedup', 1.0)
        matched = get_matching_patterns(q)
        untried = [p for p in matched if p not in set()]
        print(f"Q{q}: {best:.2f}x - {len(matched)} patterns matched - prompt written", file=sys.stderr)
        generated += 1

    print(f"\nDone: {generated} prompts generated in {OUTPUT_DIR}", file=sys.stderr)


if __name__ == "__main__":
    main()
