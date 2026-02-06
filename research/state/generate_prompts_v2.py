#!/usr/bin/env python3
"""
Generate optimization prompts for all 99 TPC-DS queries using the real DAG V3 system.

Uses:
- DagV2Pipeline for base prompt (DAG decomposition + JSON output format)
- run_explain_analyze → analyze_plan_for_optimization → _format_plan_summary (parsed EXPLAIN)
- State analysis recommendations to select and order gold examples (not FAISS)
- Attempt history from state YAMLs
- build_prompt_with_examples to assemble (constraints + examples + DAG + EXPLAIN + history)

Output: research/state/prompts/qN_prompt.txt (overwrites previous garbage prompts)
"""

import sys
import yaml
import json
from pathlib import Path
from typing import List, Dict, Optional, Set

# Add packages to path
PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT / "packages" / "qt-shared"))

from qt_sql.optimization.dag_v3 import (
    load_example,
    load_all_examples,
    GoldExample,
)
from ado.prompt_builder import build_prompt_with_examples
from qt_sql.optimization.dag_v2 import DagV2Pipeline
from qt_sql.optimization.plan_analyzer import analyze_plan_for_optimization
from qt_sql.execution.database_utils import run_explain_analyze
from qt_sql.optimization.adaptive_rewriter_v5 import _format_plan_summary

# ============================================================================
# PATHS
# ============================================================================

DB_PATH = "/mnt/d/TPC-DS/tpcds_sf10.duckdb"
STATE_DIR = PROJECT / "research" / "state_histories_all_99"
QUERIES_DIR = PROJECT / "research" / "state" / "queries"
OUTPUT_DIR = PROJECT / "research" / "state" / "prompts"

# ============================================================================
# RECOMMENDATION DATA (from state analysis)
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


def get_recommended_examples(query_num: int, transforms_tried: Set[str], transforms_failed: Set[str]) -> List[GoldExample]:
    """Get gold examples selected by our state analysis, in recommended order.

    Priority:
    1. Untried patterns that match query structure (highest value)
    2. Patterns that worked before on this query (reinforcement)
    3. Skip patterns that failed on this query
    """
    features = TPCDS_QUERY_FEATURES.get(query_num, [])

    # Get structurally matched pattern IDs in priority order
    matched_ids = []
    seen = set()
    for feature in features:
        for pattern_id in FEATURE_TO_PATTERN.get(feature, []):
            if pattern_id not in seen:
                matched_ids.append(pattern_id)
                seen.add(pattern_id)

    # Partition into untried, succeeded, and failed
    untried = [p for p in matched_ids if p not in transforms_tried]
    succeeded = [p for p in matched_ids if p in transforms_tried and p not in transforms_failed]
    failed = [p for p in matched_ids if p in transforms_failed]

    # Order: untried first, then succeeded (reinforcement), skip failed
    ordered_ids = untried + succeeded

    # Load actual GoldExample objects
    examples = []
    for pid in ordered_ids:
        ex = load_example(pid)
        if ex:
            examples.append(ex)

    # Cap at 3 examples per prompt
    return examples[:3]


def load_attempt_history(query_num: int) -> tuple:
    """Load attempt history from state YAML. Returns (history_text, transforms_tried, transforms_failed)."""
    yaml_path = STATE_DIR / f"q{query_num}_state_history.yaml"
    if not yaml_path.exists():
        return "", set(), set()

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    transforms_tried = set()
    transforms_succeeded = set()
    transforms_failed = set()

    lines = []
    lines.append("## Previous Attempts\n")

    for state in data.get('states', []):
        state_id = state.get('state_id', 'unknown')
        speedup = state.get('speedup', 1.0)
        transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
        yaml_status = state.get('status', 'unknown')

        # Derive status from speedup, not YAML (YAML often says "success" for 1.0x)
        if yaml_status == 'error':
            status = 'ERROR'
        elif speedup >= 1.1:
            status = 'WIN'
        elif speedup >= 1.05:
            status = 'IMPROVED'
        elif speedup >= 0.95:
            status = 'NEUTRAL'
        else:
            status = 'REGRESSION'

        for t in transforms:
            transforms_tried.add(t)
            if speedup >= 1.1:
                transforms_succeeded.add(t)
            elif yaml_status == 'error' or speedup < 0.95:
                transforms_failed.add(t)

        t_str = ", ".join(transforms) if transforms else "none"
        lines.append(f"- {state_id}: {speedup:.2f}x [{t_str}] {status}")

    if transforms_tried:
        lines.append("")
        succeeded = sorted(transforms_succeeded)
        failed = sorted(transforms_failed)
        neutral = sorted(transforms_tried - transforms_succeeded - transforms_failed)
        if succeeded:
            lines.append(f"**Worked**: {', '.join(succeeded)}")
        if neutral:
            lines.append(f"**No effect**: {', '.join(neutral)}")
        if failed:
            lines.append(f"**Failed/Regression**: {', '.join(failed)} — DO NOT use these patterns")

    return "\n".join(lines), transforms_tried, transforms_failed


def clean_sql(sql_text: str) -> str:
    """Strip comments and trailing semicolons."""
    lines = []
    for line in sql_text.split('\n'):
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        lines.append(line)
    clean = '\n'.join(lines).strip()
    while clean.endswith(';'):
        clean = clean[:-1].strip()
    return clean


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {DB_PATH}...", file=sys.stderr)

    generated = 0
    errors = 0

    for q in range(1, 100):
        # Read current SQL (from state queries - optimized or baseline)
        query_path = QUERIES_DIR / f"q{q}_current.sql"
        if not query_path.exists():
            # Fall back to pipeline state_0
            query_path = PROJECT / "research" / "pipeline" / "state_0" / "queries" / f"q{q}.sql"
        if not query_path.exists():
            print(f"Q{q}: SKIP - no SQL file", file=sys.stderr)
            continue

        sql_raw = query_path.read_text()
        sql_clean = clean_sql(sql_raw)

        if not sql_clean.strip():
            print(f"Q{q}: SKIP - empty SQL", file=sys.stderr)
            continue

        # For multi-statement queries (e.g. Q23 has two WITH blocks),
        # use only the first statement for EXPLAIN
        statements = [s.strip() for s in sql_clean.split(';') if s.strip()]
        sql_for_explain = statements[0] if statements else sql_clean

        print(f"Q{q}: ", file=sys.stderr, end="", flush=True)

        # 1. Load attempt history
        history_text, transforms_tried, transforms_failed = load_attempt_history(q)

        # 2. Get recommended examples (analysis-driven, not FAISS)
        examples = get_recommended_examples(q, transforms_tried, transforms_failed)
        ex_ids = [e.id for e in examples]

        # 3. Run EXPLAIN ANALYZE (JSON parsed)
        plan_summary = ""
        plan_context = None
        try:
            result = run_explain_analyze(DB_PATH, sql_for_explain)
            if result and result.get('plan_json'):
                plan_context = analyze_plan_for_optimization(result['plan_json'], sql_for_explain)
                plan_summary = _format_plan_summary(plan_context)
                print("EXPLAIN:OK ", file=sys.stderr, end="", flush=True)
            else:
                print("EXPLAIN:NO_JSON ", file=sys.stderr, end="", flush=True)
        except Exception as e:
            print(f"EXPLAIN:ERR({e}) ", file=sys.stderr, end="", flush=True)

        # 4. Build DAG base prompt (with plan context for node-level optimization hints)
        try:
            pipeline = DagV2Pipeline(sql_for_explain, plan_context=plan_context)
            base_prompt = pipeline.get_prompt()
        except Exception as e:
            # Fallback: raw SQL without DAG decomposition
            print(f"DAG:ERR({e}) ", file=sys.stderr, end="", flush=True)
            base_prompt = f"Optimize this SQL query:\n```sql\n{sql_clean}\n```"

        # 5. Assemble with build_prompt_with_examples (constraints + examples + DAG + EXPLAIN + history)
        prompt = build_prompt_with_examples(
            base_prompt=base_prompt,
            examples=examples,
            execution_plan=plan_summary,
            history_section=history_text,
            include_constraints=True,
        )

        # 6. Save
        output_path = OUTPUT_DIR / f"q{q}_prompt.txt"
        with open(output_path, 'w') as f:
            f.write(prompt)

        print(f"examples={ex_ids} len={len(prompt)}", file=sys.stderr)
        generated += 1

    print(f"\nDone: {generated} prompts, {errors} errors", file=sys.stderr)
    print(f"Output: {OUTPUT_DIR}", file=sys.stderr)


if __name__ == "__main__":
    main()
