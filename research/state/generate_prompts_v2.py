#!/usr/bin/env python3
"""
Generate optimization prompts for all 99 TPC-DS queries.

Prompt structure v2 (attention-optimized):
  1. ROLE + TASK          (primacy: model knows what it is)
  2. THE QUERY            (DAG structure, SQL, contracts)
  3. PERFORMANCE PROFILE  (execution plan, costs, opportunities)
  4. HISTORY + HINT       (what was tried + pattern preview)
  5. EXAMPLES             (contrastive: DO + DON'T paired)
  6. CONSTRAINTS          (sandwich: CRITICAL top/bottom)
  7. OUTPUT FORMAT        (recency: last thing before generation)
"""

import sys
import yaml
import json
import sqlglot
from pathlib import Path
from typing import List, Dict, Optional, Set

# Add packages to path
PROJECT = Path("/mnt/c/Users/jakc9/Documents/QueryTorque_V8")
sys.path.insert(0, str(PROJECT / "packages" / "qt-sql"))
sys.path.insert(0, str(PROJECT / "packages" / "qt-shared"))

from qt_sql.optimization.dag_v3 import (
    load_example,
    load_all_examples,
    format_example_for_prompt,
    GoldExample,
)
from ado.prompt_builder import (
    load_all_constraints,
)
from qt_sql.optimization.dag_v2 import DagV2Pipeline, DagBuilder
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
DISCOVERY_TEMPLATE = PROJECT / "research" / "discovery_prompts" / "PROMPT_DISCOVER_NEW_PATTERNS.txt"

# ============================================================================
# ALLOWED TRANSFORMS (from DagBuilder)
# ============================================================================

ALLOWED_TRANSFORMS = ", ".join(DagBuilder.ALLOWED_TRANSFORMS)

# ============================================================================
# PROMPT SECTIONS
# ============================================================================

ROLE_SECTION = f"""You are an autonomous Query Rewrite Engine. Your goal is to maximize execution speed while strictly preserving semantic invariants.

RULES:
- Maximize execution speed while preserving semantic invariants (output columns, grain, total result rows).
- Group dependent changes into a single rewrite_set.
- Use descriptive CTE names (e.g., `filtered_returns` not `cte1`).
- If a standard SQL optimization applies that is not in the allowed list, label it "semantic_rewrite".

ALLOWED TRANSFORMS: {ALLOWED_TRANSFORMS}"""

OUTPUT_FORMAT_SECTION = """## Output Format

Respond with a JSON object containing your rewrite_sets:

```json
{
  "rewrite_sets": [
    {
      "id": "rs_01",
      "transform": "transform_name",
      "nodes": {
        "node_id": "new SQL..."
      },
      "invariants_kept": ["same result rows", "same ordering"],
      "expected_speedup": "2x",
      "risk": "low"
    }
  ],
  "explanation": "what was changed and why"
}
```

Now output your rewrite_sets:"""

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
    "correlated_subquery": ["decorrelate", "composite_decorrelate_union", "date_cte_isolate"],
    "date_filter":         ["date_cte_isolate", "prefetch_fact_join"],
    "multi_date_alias":    ["multi_date_range_cte"],
    "or_condition":        ["or_to_union", "composite_decorrelate_union"],
    "multi_dim_filter":    ["dimension_cte_isolate", "multi_dimension_prefetch", "early_filter", "shared_dimension_multi_channel"],
    "dim_fact_chain":      ["prefetch_fact_join", "early_filter", "multi_dimension_prefetch", "shared_dimension_multi_channel"],
    "repeated_scan":       ["single_pass_aggregation", "pushdown"],
    "intersect":           ["intersect_to_exists"],
    "union_year":          ["union_cte_split"],
    "exists_repeat":       ["materialize_cte", "composite_decorrelate_union"],
    "complex_multi_join":  [],
    "window_fn":           ["deferred_window_aggregation"],
}


# ============================================================================
# EXAMPLE / HISTORY / CONSTRAINT HELPERS
# ============================================================================

def get_recommended_examples(query_num: int, transforms_tried: Set[str], transforms_failed: Set[str], transforms_succeeded: Set[str]) -> List[GoldExample]:
    """Get gold examples in recommended order: untried first, then succeeded."""
    features = TPCDS_QUERY_FEATURES.get(query_num, [])

    matched_ids = []
    seen = set()
    for feature in features:
        for pattern_id in FEATURE_TO_PATTERN.get(feature, []):
            if pattern_id not in seen:
                matched_ids.append(pattern_id)
                seen.add(pattern_id)

    untried = [p for p in matched_ids if p not in transforms_tried]
    succeeded = [p for p in matched_ids if p in transforms_succeeded]
    ordered_ids = untried + succeeded

    examples = []
    for pid in ordered_ids:
        ex = load_example(pid)
        if ex:
            examples.append(ex)

    return examples[:3]


def load_attempt_history(query_num: int) -> tuple:
    """Load attempt history. Returns (history_text, transforms_tried, transforms_failed, transforms_succeeded)."""
    yaml_path = STATE_DIR / f"q{query_num}_state_history.yaml"
    if not yaml_path.exists():
        return "", set(), set(), set()

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    transforms_tried = set()
    transforms_succeeded = set()
    transforms_failed = set()

    lines = []
    for state in data.get('states', []):
        state_id = state.get('state_id', 'unknown')
        speedup = state.get('speedup', 1.0)
        transforms = [t.strip() for t in state.get('transforms', []) if t and t.strip()]
        yaml_status = state.get('status', 'unknown')

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

    summary_parts = []
    if transforms_tried:
        succeeded = sorted(transforms_succeeded)
        failed = sorted(transforms_failed)
        neutral = sorted(transforms_tried - transforms_succeeded - transforms_failed)
        if succeeded:
            summary_parts.append(f"**Worked**: {', '.join(succeeded)}")
        if neutral:
            summary_parts.append(f"**No effect**: {', '.join(neutral)}")
        if failed:
            summary_parts.append(f"**Regression**: {', '.join(failed)}")

    history = "\n".join(lines)
    if summary_parts:
        history += "\n\n" + "\n".join(summary_parts)

    return history, transforms_tried, transforms_failed, transforms_succeeded


def format_pattern_hint(examples: List[GoldExample]) -> str:
    """Generate a 3-5 line primacy-boosted pattern preview."""
    if not examples:
        return ""
    lines = ["**Recommended patterns** (details in Examples section below):"]
    for ex in examples:
        lines.append(f"- **{ex.id}** ({ex.verified_speedup}) — {ex.description.split('.')[0]}.")
    return "\n".join(lines)


def format_constraints_sandwich(constraints) -> str:
    """Format constraints with sandwich ordering: CRITICAL top+bottom, HIGH in middle."""
    if not constraints:
        return ""

    critical = [c for c in constraints if c.severity == "CRITICAL"]
    high = [c for c in constraints if c.severity == "HIGH"]

    # Sandwich: split CRITICAL between top and bottom
    top_critical = critical[:2]    # CTE_COLUMN_COMPLETENESS, LITERAL_PRESERVATION
    bottom_critical = critical[2:] # NO_MATERIALIZE_EXISTS (if any)

    ordered = top_critical + high + bottom_critical

    lines = []
    for c in ordered:
        lines.append(f"**{c.id}** [{c.severity}]: {c.prompt_instruction}")
        lines.append("")

    return "\n".join(lines)


# ============================================================================
# DAG PARSING - extract sections from DagV2Pipeline output
# ============================================================================

def parse_dag_sections(dag_prompt: str) -> Dict[str, str]:
    """Parse the monolithic DAG prompt into named sections.

    Strips premature output instructions (e.g. 'Now output your rewrite_sets:')
    that leak from DagV2Pipeline's internal prompt structure.
    """
    sections = {}
    current_key = None
    current_lines = []

    for line in dag_prompt.split('\n'):
        if line.startswith('## '):
            # Save previous section
            if current_key:
                sections[current_key] = '\n'.join(current_lines).strip()
            current_key = line[3:].strip()
            current_lines = []
        elif current_key:
            # Strip premature output instructions from DAG prompt
            if line.strip().startswith('Now output'):
                continue
            current_lines.append(line)
        # Skip lines before first ## (system prompt)

    # Save last section
    if current_key:
        sections[current_key] = '\n'.join(current_lines).strip()

    # Extract system prompt (everything before first ##)
    first_section = dag_prompt.find('## ')
    if first_section > 0:
        sections['_system_prompt'] = dag_prompt[:first_section].strip()

    return sections


# ============================================================================
# DAG TOPOLOGY + SQL FORMATTING
# ============================================================================

def format_dag_topology(pipeline) -> str:
    """Render explicit DAG topology: nodes with types + dependency edges."""
    dag = pipeline.dag
    lines = ["### DAG Topology", "```"]

    # Nodes
    for node_id, node in dag.nodes.items():
        flags = " ".join(node.flags) if node.flags else ""
        tables = ", ".join(node.tables[:5]) if node.tables else ""
        lines.append(f"  [{node_id}] type={node.node_type} tables=[{tables}] {flags}")

    # Edges
    if dag.edges:
        lines.append("")
        lines.append("  Edges:")
        seen = set()
        for src, dst in dag.edges:
            key = f"{src} -> {dst}"
            if key not in seen:
                lines.append(f"    {key}")
                seen.add(key)

    lines.append("```")
    return "\n".join(lines)


def pretty_format_sql(sql: str) -> str:
    """Format SQL for human readability using sqlglot."""
    try:
        result = sqlglot.transpile(sql, read="duckdb", write="duckdb", pretty=True)
        return result[0] if result else sql
    except Exception:
        return sql


def format_dag_nodes_sql(pipeline) -> str:
    """Format each DAG node's SQL as a human-readable block."""
    lines = []
    for node_id, node in pipeline.dag.nodes.items():
        lines.append(f"[{node_id}] type={node.node_type}")
        lines.append("```sql")
        lines.append(pretty_format_sql(node.sql))
        lines.append("```")
        lines.append("")
    return "\n".join(lines)


# ============================================================================
# PROMPT ASSEMBLY v2
# ============================================================================

def build_prompt_v2(
    dag_prompt: str,
    plan_summary: str,
    history_text: str,
    examples: List[GoldExample],
    pattern_hint: str,
    pipeline=None,
) -> str:
    """Assemble prompt in v2 order: Role → Query → Performance → History → Examples → Constraints → Output."""
    parts = []

    # Parse DAG output into sections
    dag = parse_dag_sections(dag_prompt)

    # ── 1. ROLE + TASK ──
    parts.append(ROLE_SECTION)
    parts.append("")

    # ── 2. THE QUERY (DAG structure) ──
    parts.append("---")
    parts.append("## Query Structure")
    parts.append("")

    # DAG topology (explicit node graph)
    if pipeline:
        parts.append(format_dag_topology(pipeline))
        parts.append("")

    if 'Target Nodes' in dag:
        parts.append("### Target Nodes")
        parts.append(dag['Target Nodes'])
        parts.append("")

    # Node SQL — use pretty-formatted from pipeline if available, else fallback
    if pipeline:
        parts.append("### SQL")
        parts.append(format_dag_nodes_sql(pipeline))
    elif 'Subgraph Slice' in dag:
        parts.append("### SQL")
        parts.append(dag['Subgraph Slice'])
        parts.append("")

    if 'Node Contracts' in dag:
        parts.append("### Contracts")
        parts.append(dag['Node Contracts'])
    if 'Downstream Usage' in dag:
        parts.append("")
        parts.append("### Downstream Usage")
        parts.append(dag['Downstream Usage'])

    # ── 3. PERFORMANCE PROFILE ──
    parts.append("")
    parts.append("---")
    parts.append("## Performance Profile")
    parts.append("")
    if 'Cost Attribution' in dag:
        parts.append("### Cost Attribution")
        parts.append(dag['Cost Attribution'])
        parts.append("")
    if plan_summary and plan_summary.strip():
        parts.append("### Execution Plan")
        parts.append(f"```\n{plan_summary}\n```")
        parts.append("")
    # Merge all opportunity sections into one
    opps = []
    for key in ['Detected Opportunities', 'Knowledge Base Patterns (verified on TPC-DS)',
                 'Detected Optimization Opportunities', 'Node-Specific Opportunities']:
        if key in dag and dag[key].strip():
            opps.append(dag[key])
    if opps:
        parts.append("### Optimization Opportunities")
        parts.append("\n\n".join(opps))

    # ── 4. HISTORY + PATTERN HINT ──
    parts.append("")
    parts.append("---")
    parts.append("## Previous Attempts")
    parts.append("")
    if history_text:
        parts.append(history_text)
    else:
        parts.append("No previous attempts.")

    if pattern_hint:
        parts.append("")
        parts.append(pattern_hint)

    # ── 5. EXAMPLES (contrastive: key_insight + when_not_to_use paired) ──
    if examples:
        parts.append("")
        parts.append("---")
        parts.append("## Examples (Verified Patterns)")
        parts.append("")
        for i, ex in enumerate(examples):
            parts.append(format_example_for_prompt(ex))
            if i < len(examples) - 1:
                parts.append("")

    # ── 6. CONSTRAINTS (sandwich ordered) ──
    constraints = load_all_constraints()
    if constraints:
        parts.append("")
        parts.append("---")
        parts.append("## Constraints")
        parts.append("")
        parts.append(format_constraints_sandwich(constraints))

    # ── 7. OUTPUT FORMAT (recency: last thing before generation) ──
    parts.append("")
    parts.append("---")
    parts.append(OUTPUT_FORMAT_SECTION)

    return "\n".join(parts)


def build_discovery_prompt_v2(
    dag_prompt: str,
    plan_summary: str,
    history_text: str,
    pipeline=None,
) -> str:
    """Build discovery prompt for exhausted queries, using v2 structure."""
    parts = []

    # Load discovery template
    if DISCOVERY_TEMPLATE.exists():
        discovery_text = DISCOVERY_TEMPLATE.read_text()
        discovery_text = discovery_text.replace(
            "## The Query to Optimize\n\n[QUERY_WILL_BE_INSERTED_HERE]\n", ""
        )
        if "## Important Notes" in discovery_text:
            discovery_text = discovery_text[:discovery_text.index("## Important Notes")]
    else:
        discovery_text = "# DISCOVERY MODE: All known patterns exhausted.\nFind a NOVEL optimization technique.\n"

    # Parse DAG sections
    dag = parse_dag_sections(dag_prompt)

    # ── 1. DISCOVERY PREAMBLE (replaces ROLE for exhausted queries) ──
    parts.append(discovery_text.strip())

    # ── 2. THE QUERY ──
    parts.append("")
    parts.append("---")
    parts.append("## Query Structure")
    parts.append("")

    # DAG topology
    if pipeline:
        parts.append(format_dag_topology(pipeline))
        parts.append("")

    if 'Target Nodes' in dag:
        parts.append("### Target Nodes")
        parts.append(dag['Target Nodes'])
        parts.append("")

    # Node SQL — pretty-formatted
    if pipeline:
        parts.append("### SQL")
        parts.append(format_dag_nodes_sql(pipeline))
    elif 'Subgraph Slice' in dag:
        parts.append("### SQL")
        parts.append(dag['Subgraph Slice'])
        parts.append("")

    if 'Node Contracts' in dag:
        parts.append("### Contracts")
        parts.append(dag['Node Contracts'])
    if 'Downstream Usage' in dag:
        parts.append("")
        parts.append("### Downstream Usage")
        parts.append(dag['Downstream Usage'])

    # ── 3. PERFORMANCE PROFILE ──
    parts.append("")
    parts.append("---")
    parts.append("## Performance Profile")
    parts.append("")
    if 'Cost Attribution' in dag:
        parts.append("### Cost Attribution")
        parts.append(dag['Cost Attribution'])
        parts.append("")
    if plan_summary and plan_summary.strip():
        parts.append("### Execution Plan")
        parts.append(f"```\n{plan_summary}\n```")

    # ── 4. HISTORY ──
    parts.append("")
    parts.append("---")
    parts.append("## Previous Attempts")
    parts.append("")
    if history_text:
        parts.append(history_text)
    parts.append("")
    parts.append("**ALL known patterns have been tried. You MUST find a novel technique.**")

    # ── 5. No examples (exhausted) ──

    # ── 6. CONSTRAINTS ──
    constraints = load_all_constraints()
    if constraints:
        parts.append("")
        parts.append("---")
        parts.append("## Constraints")
        parts.append("")
        parts.append(format_constraints_sandwich(constraints))

    # ── 7. OUTPUT FORMAT ──
    parts.append("")
    parts.append("---")
    parts.append(OUTPUT_FORMAT_SECTION)

    return "\n".join(parts)


# ============================================================================
# UTILITIES
# ============================================================================

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


# ============================================================================
# MAIN
# ============================================================================

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to {DB_PATH}...", file=sys.stderr)

    generated = 0
    errors = 0

    for q in range(1, 100):
        # ALWAYS use original baseline SQL - compound optimization (opt->opt) failed
        BASELINE_DIR = PROJECT / "research" / "pipeline" / "state_0" / "queries"
        query_path = BASELINE_DIR / f"q{q}.sql"
        if not query_path.exists():
            query_path = QUERIES_DIR / f"q{q}_current.sql"
        if not query_path.exists():
            print(f"Q{q}: SKIP - no SQL file", file=sys.stderr)
            continue

        sql_raw = query_path.read_text()
        sql_clean = clean_sql(sql_raw)

        if not sql_clean.strip():
            print(f"Q{q}: SKIP - empty SQL", file=sys.stderr)
            continue

        statements = [s.strip() for s in sql_clean.split(';') if s.strip()]
        sql_for_explain = statements[0] if statements else sql_clean

        print(f"Q{q}: ", file=sys.stderr, end="", flush=True)

        # 1. Load attempt history
        history_text, transforms_tried, transforms_failed, transforms_succeeded = load_attempt_history(q)

        # 2. Get recommended examples
        examples = get_recommended_examples(q, transforms_tried, transforms_failed, transforms_succeeded)
        ex_ids = [e.id for e in examples]

        # 3. Run EXPLAIN ANALYZE
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

        # 4. Build DAG base prompt
        pipeline = None
        try:
            pipeline = DagV2Pipeline(sql_for_explain, plan_context=plan_context)
            dag_prompt = pipeline.get_prompt()
        except Exception as e:
            print(f"DAG:ERR({e}) ", file=sys.stderr, end="", flush=True)
            dag_prompt = f"## Subgraph Slice\n[main_query] type=main\n```sql\n{sql_clean}\n```"

        # 5. Assemble prompt v2
        if not examples:
            prompt = build_discovery_prompt_v2(
                dag_prompt=dag_prompt,
                plan_summary=plan_summary,
                history_text=history_text,
                pipeline=pipeline,
            )
            mode = "DISCOVERY"
        else:
            pattern_hint = format_pattern_hint(examples)
            prompt = build_prompt_v2(
                dag_prompt=dag_prompt,
                plan_summary=plan_summary,
                history_text=history_text,
                examples=examples,
                pattern_hint=pattern_hint,
                pipeline=pipeline,
            )
            mode = "STANDARD"

        # 6. Save
        output_path = OUTPUT_DIR / f"q{q}_prompt.txt"
        with open(output_path, 'w') as f:
            f.write(prompt)

        print(f"mode={mode} examples={ex_ids} len={len(prompt)}", file=sys.stderr)
        generated += 1

    print(f"\nDone: {generated} prompts, {errors} errors", file=sys.stderr)
    print(f"Output: {OUTPUT_DIR}", file=sys.stderr)


if __name__ == "__main__":
    main()
