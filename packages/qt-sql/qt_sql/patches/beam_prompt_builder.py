"""Build compact BEAM prompts with dialect-first knowledge guidance."""

from functools import lru_cache
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import json
import logging

from qt_sql.prompter import _load_engine_profile

logger = logging.getLogger(__name__)
PROMPT_TEMPLATES_DIR = (
    Path(__file__).resolve().parent.parent / "prompts" / "templates" / "V3"
)


# ── Prompt Formatting Helpers ────────────────────────────────────────────────

def _safe_md_cell(value: Any) -> str:
    """Render a markdown table cell safely (single-line, no pipe breaks)."""
    text = "" if value is None else str(value)
    text = text.replace("\n", " ").replace("\r", " ")
    text = text.replace("|", r"\|")
    return " ".join(text.split())


@lru_cache(maxsize=8)
def _load_prompt_template(filename: str) -> str:
    """Load a prompt template from prompts/templates/V3."""
    path = PROMPT_TEMPLATES_DIR / filename
    if not path.exists():
        logger.warning("Prompt template missing: %s", path)
        return ""
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception as e:
        logger.warning("Failed loading prompt template %s: %s", path, e)
        return ""


# ── Family Descriptions ────────────────────────────────────────────────────

FAMILY_DESCRIPTIONS = {
    "A": {
        "name": "Early Filtering",
        "pattern": "Predicate Pushback",
        "description": "Push small filters into CTEs early, reduce row count before expensive operations",
        "speedup_range": "1.3–4.0x",
        "win_rate": "~35% of all wins",
        "use_when": [
            "Late WHERE filters on dimension tables",
            "Cascading CTEs with filters applied downstream",
            "Expensive joins after filters could be pushed earlier"
        ],
        "stop_when": [
            "Filter ratio is weak and baseline runtime is already low",
            "Target CTE already contains the relevant selective predicate",
            "Three or more fact tables in a deep CTE chain (join-order lock risk)"
        ],
    },
    "B": {
        "name": "Decorrelation",
        "pattern": "Sets Over Loops",
        "description": "Convert correlated subqueries to standalone CTEs with GROUP BY, eliminate per-row re-execution",
        "speedup_range": "2.4–2.9x",
        "win_rate": "~15% of all wins",
        "use_when": [
            "Correlated subqueries in WHERE clause",
            "Scalar aggregates computed per outer row",
            "DELIM_SCAN in execution plan (indicates correlation)"
        ],
        "stop_when": [
            "EXPLAIN already shows a hash semi join on the same correlation key",
            "Simple EXISTS path already optimized by semi-join",
            "Outer side is already tiny after early filtering"
        ],
    },
    "C": {
        "name": "Aggregation Pushdown",
        "pattern": "Minimize Rows Touched",
        "description": "Aggregate before expensive joins when GROUP BY keys ⊇ join keys, reduce intermediate sizes",
        "speedup_range": "1.3–15.3x",
        "win_rate": "~5% of all wins (high variance)",
        "use_when": [
            "GROUP BY happens after large joins",
            "GROUP BY keys are subset of join keys",
            "Intermediate result size >> final result size"
        ],
        "stop_when": [
            "GROUP BY keys are not compatible with join keys (semantic risk)",
            "Aggregation includes grouping-sensitive metrics (e.g., STDDEV/VARIANCE)",
            "Rewrite introduces join duplication before AVG/STDDEV-style aggregates"
        ],
    },
    "D": {
        "name": "Set Operation Optimization",
        "pattern": "Sets Over Loops",
        "description": "Replace INTERSECT/UNION-based patterns with EXISTS/NOT EXISTS, avoid full materialization",
        "speedup_range": "1.7–2.7x",
        "win_rate": "~8% of all wins",
        "use_when": [
            "INTERSECT patterns between large sets",
            "UNION ALL with duplicate elimination",
            "Set operations materializing full intermediate results"
        ],
        "stop_when": [
            "Both set-operation sides are already small",
            "Result needs columns from both sides (semi-join rewrite invalid)"
        ],
    },
    "E": {
        "name": "Materialization / Prefetch",
        "pattern": "Don't Repeat Work",
        "description": "Extract repeated scans or pre-compute intermediate results for reuse across multiple consumers",
        "speedup_range": "1.3–6.2x",
        "win_rate": "~18% of all wins",
        "use_when": [
            "Repeated scans of same table with different filters",
            "Dimension filters applied independently multiple times",
            "CTE referenced multiple times with implicit re-evaluation"
        ],
        "stop_when": [
            "CTE is single-use and not expensive",
            "New CTE would be unfiltered (materialize-everything pattern)",
            "Original source scan would remain alongside replacement (orphan risk)"
        ],
    },
    "F": {
        "name": "Join Transform",
        "pattern": "Right Shape First",
        "description": "Restructure join topology: convert comma joins to explicit INNER JOIN, optimize join order, eliminate self-joins via single-pass aggregation",
        "speedup_range": "1.8–8.6x",
        "win_rate": "~19% of all wins",
        "use_when": [
            "Comma-separated joins (implicit cross joins) in FROM clause",
            "Self-joins scanning same table multiple times",
            "Dimension-fact join order suboptimal for predicate pushdown"
        ],
        "stop_when": [
            "Tiny join graph where optimizer is already accurate",
            "EXPLAIN shows good cardinality estimates and stable join shape"
        ],
    }
}


def _compact_text(value: Any, max_len: int = 220) -> str:
    """Single-line compact text for prompt payloads."""
    if value is None:
        return ""
    text = " ".join(str(value).strip().split())
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


# ── Build Individual Family Sections ───────────────────────────────────────

def format_family_section(
    family_id: str,
    gold_example: Dict[str, Any],
    include_dag_examples: bool = True,
    analyst_only: bool = False,
) -> str:
    """Format a single family section with description + gold example.

    Args:
        family_id: Family letter (A-F).
        gold_example: Gold example dict with original_sql, optimized_sql, ir_*, dag_example.
        include_dag_examples: If False, omit DAG-shape hints.
        analyst_only: If True, only emit family description + example ID.
            The analyst picks strategies; workers get the full examples.
    """

    desc = FAMILY_DESCRIPTIONS[family_id]

    lines = [
        f"### Family {family_id}: {desc['name']} ({desc['pattern']})",
        f"**Description**: {desc['description']}",
        f"**Speedup Range**: {desc['speedup_range']} ({desc['win_rate']})",
        f"**Use When**:",
    ]

    for i, condition in enumerate(desc['use_when'], 1):
        lines.append(f"  {i}. {condition}")

    stop_when = desc.get("stop_when") or []
    if stop_when:
        lines.append("**Decision Gates (STOP when):**")
        for i, condition in enumerate(stop_when, 1):
            lines.append(f"  {i}. {condition}")

    # Gold example ID + speedup (always shown)
    example_name = gold_example.get("id", "example")
    speedup = gold_example.get("verified_speedup", "?")

    lines.append("")
    lines.append(f"**Gold Example**: `{example_name}` ({speedup})")
    transforms = (
        gold_example.get("transforms")
        or gold_example.get("transforms_applied")
        or []
    )
    if isinstance(transforms, list) and transforms:
        lines.append(f"**Canonical transforms**: {', '.join(f'`{t}`' for t in transforms[:4])}")

    gap_ids = gold_example.get("gap_ids") or []
    if isinstance(gap_ids, list) and gap_ids:
        lines.append(f"**Targeted gaps**: {', '.join(f'`{g}`' for g in gap_ids[:3])}")

    insight = (
        gold_example.get("key_insight")
        or gold_example.get("principle")
        or gold_example.get("description")
        or ""
    )
    if insight:
        lines.append(f"**Pattern**: {_compact_text(insight, max_len=260)}")

    # Analyst and compiler use the same compact card; no embedded SQL blobs.
    if analyst_only:
        return "\n".join(lines)

    if include_dag_examples:
        dag_example = gold_example.get("dag_example") or gold_example.get("dag")
        dag_payload = None
        if isinstance(dag_example, dict):
            dag_payload = dag_example.get("dag") if isinstance(dag_example.get("dag"), dict) else dag_example
        if isinstance(dag_payload, dict):
            nodes = dag_payload.get("nodes") or []
            if isinstance(nodes, list):
                changed_nodes = [
                    str(n.get("node_id", "")).strip()
                    for n in nodes
                    if isinstance(n, dict)
                    and n.get("changed") is True
                    and str(n.get("node_id", "")).strip()
                ]
                if changed_nodes:
                    lines.append(
                        f"**DAG shape**: changed nodes {', '.join(f'`{n}`' for n in changed_nodes[:4])}"
                    )
                final_node = str(dag_payload.get("final_node_id", "")).strip()
                if final_node:
                    lines.append(f"**Final node**: `{final_node}`")

    return "\n".join(lines)


def format_family_description_only(family_id: str, dialect: str = "") -> str:
    """Format a family section when no gold example exists for this engine.

    Shows the full family description with a caveat that no gold example
    has been observed yet. The analyst can still try the strategy if the
    EXPLAIN plan warrants it.
    """
    desc = FAMILY_DESCRIPTIONS[family_id]
    engine_label = dialect.upper().replace('POSTGRES', 'PostgreSQL') if dialect else "this engine"

    lines = [
        f"### Family {family_id}: {desc['name']} ({desc['pattern']})",
        f"**Description**: {desc['description']}",
        f"**Speedup Range**: {desc['speedup_range']} ({desc['win_rate']})",
        f"**Use When**:",
    ]

    for i, condition in enumerate(desc['use_when'], 1):
        lines.append(f"  {i}. {condition}")

    stop_when = desc.get("stop_when") or []
    if stop_when:
        lines.append("**Decision Gates (STOP when):**")
        for i, condition in enumerate(stop_when, 1):
            lines.append(f"  {i}. {condition}")

    lines.append("")
    lines.append(f"**Gold Example**: None yet observed on {engine_label}. "
                 "Strategy is valid if EXPLAIN evidence supports it.")

    return "\n".join(lines)


# ── Engine Intelligence Loader ─────────────────────────────────────────────

def _load_engine_intelligence(dialect: str) -> Optional[str]:
    """Load structured dialect profile for compact prompt injection."""
    profile = _load_engine_profile(dialect)
    if not profile:
        return None

    lines = [f"## Dialect Profile ({dialect.upper()})"]
    briefing = profile.get("briefing_note")
    if briefing:
        lines.append("")
        lines.append(
            "**Combined Intelligence Baseline**: "
            + _compact_text(briefing, max_len=320)
        )

    strengths = profile.get("strengths") or []
    if strengths:
        lines.append("")
        lines.append("### Optimizer Strengths (don't fight these)")
        for s in strengths[:4]:
            sid = s.get("id", "?")
            implication = _compact_text(
                s.get("implication") or s.get("summary") or "",
                max_len=180,
            )
            if sid == "SEMI_JOIN_EXISTS":
                implication = (
                    implication
                    + " Note: NOT EXISTS anti-join decorrelation can still be valid when replacing large correlated anti patterns."
                )
            lines.append(f"- `{sid}`: {implication}")

    gaps = profile.get("gaps") or []
    if gaps:
        prio = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        lines.append("")
        lines.append("### Known Gaps (exploit these)")
        for g in sorted(gaps, key=lambda x: prio.get((x.get("priority") or "").upper(), 3))[:5]:
            gid = g.get("id", "?")
            priority = (g.get("priority") or "MEDIUM").upper()
            detect = _compact_text(g.get("detect", ""), max_len=140)
            opportunity = _compact_text(
                g.get("opportunity") or g.get("what") or "",
                max_len=160,
            )
            lines.append(
                f"- `{gid}` [{priority}] detect: {detect} | action: {opportunity}"
            )

    return "\n".join(lines)


def _build_explain_analysis_procedure_section() -> str:
    return """## EXPLAIN Analysis Procedure

1. **Identify cost spine**: isolate operator chain driving most runtime.
2. **Classify spine nodes**:
- SEQ_SCAN: row count + filter selectivity
- NESTED_LOOP/ANTI: inner re-execution risk
- AGGREGATE: input/output compression ratio
- MATERIALIZE: loops × rows amplification
3. **Trace row flow**: find where rows stay flat then collapse late.
4. **Count repeated scans**: same table scanned N times with similar joins/filters.
5. **State bottleneck hypothesis**: optimizer does X; transform Y should help because Z."""


def _build_pathology_routing_section() -> str:
    return """## Pathology Routing + Pruning

### Route by plan symptom
- Flat rows through CTE chain, late drop -> Family A
- Nested loop with correlated aggregate -> Family B
- Aggregate after large join with high compression -> Family C
- INTERSECT/EXCEPT materialization on large sets -> Family D
- Repeated scans of same fact subtree -> Family E/C
- Comma joins + cardinality mismatch -> Family F

### Pruning guide
- No nested loops -> skip Family B
- No repeated scans -> skip Family E consolidation paths
- No GROUP BY -> skip Family C
- No INTERSECT/EXCEPT -> skip Family D
- No comma joins -> skip Family F comma-join transforms
- Very low baseline runtime -> avoid CTE-heavy rewrites"""


def _build_regression_registry_section() -> str:
    return """## Regression Registry

Hard failures to gate against:
- Materialized simple EXISTS path -> severe regressions (semi-join lost)
- Same-column OR split to UNION ALL by default on PostgreSQL (only consider when EXPLAIN shows OR blocks index usage and UNION branches become index scans)
- Orphaned original CTE/table after replacement -> double materialization
- Unfiltered new CTE -> materialize-everything anti-pattern
- Over-deep fact-table CTE chains -> join-order lock / parallelism loss"""


def _build_aggregation_equivalence_rules_section() -> str:
    return """## Aggregation Equivalence Rules

- GROUP BY keys must remain compatible with join keys after rewrite.
- AVG/STDDEV/VARIANCE are duplication-sensitive.
- FILTER() semantics are group-membership sensitive.
- When pivoting with CASE/FILTER, preserve discriminator semantics exactly."""


def _build_target_rel_node_spec_section() -> str:
    return """## Target REL Node Specification

Convert bottlenecks into explicit target subtrees before drafting plans.

For each target capture:
- issue label from EXPLAIN (for example nested loop rescans)
- exact plan evidence (operator and key numbers)
- subtree locator via node id path and optional anchor hash
- intended new shape (set-based, keyset-first, pre-aggregate, or join-topology shift)
- invariants to preserve (join keys, grouping keys, order/limit, distinctness)
- expected EXPLAIN delta (ops, loops, rows)"""


def _build_compiler_combination_rules_section() -> str:
    return """## Combination Rules

- Non-overlapping targets compose cleanly.
- Overlapping WHERE rewrites must be merged, not applied sequentially.
- Overlapping FROM rewrites must unify joins without duplicate sources.
- If two new CTEs overlap strongly, keep the more selective one.
- Use best-speedup winner as foundation; layer one change at a time."""


# ── Shared Prompt Body — Cache-Friendly Order ─────────────────────────────
#
# Structure (for API prefix caching):
#   1st — System Instructions (CACHED): Role, task spec
#   2nd — Database Schema / Context (CACHED): Engine playbook, gold examples
#   3rd — "Query to Analyze" marker (CACHED)
#   4th — Unique query data (CACHE MISS): SQL, EXPLAIN, IR, intelligence
#
# Callers insert their task section into static, add the marker,
# then concatenate: "\n\n".join(static + dynamic).

def _build_prompt_body(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    role_text: str,
    include_dag_examples: bool = True,
    intelligence_brief: str = "",
    phase_a_items: Optional[List[str]] = None,
    phase_b_items: Optional[List[str]] = None,
    extra_static_sections: Optional[List[str]] = None,
    use_custom_prelude: bool = False,
) -> tuple[List[str], List[str], int]:
    """Build prompt sections in cache-friendly order.

    Returns:
        (static_sections, dynamic_sections, n_families).
        Callers append task section + marker to static, then join all.
    """
    static: List[str] = []
    dynamic: List[str] = []

    # ═══ STATIC PREFIX (identical for all queries on same dialect) ═════

    # ── Role ───────────────────────────────────────────────────────
    if use_custom_prelude:
        if role_text:
            static.append(role_text)
    else:
        phase_a_items = phase_a_items or [
            "Dialect Profile",
            "Optimization Families",
            "Task Contract",
        ]
        phase_b_items = phase_b_items or [
            "Query SQL",
            "Execution Plan",
            "IR Structure",
            "Detected Patterns (if available)",
        ]

        static.append(f"## Role\n\n{role_text}")
        static.append(
            "## Prompt Map\n\n"
            "### Phase A — Cached Context\n"
            + "\n".join(
                f"A{i}. {item}" for i, item in enumerate(phase_a_items, 1)
            )
            + "\n\n"
            "### Phase B — Query-Specific Input (after cache boundary)\n"
            + "\n".join(
                f"B{i}. {item}" for i, item in enumerate(phase_b_items, 1)
            )
        )

    # ── Engine Intelligence ────────────────────────────────────────
    engine_intel = _load_engine_intelligence(dialect)
    if engine_intel:
        static.append(engine_intel)

    # ── Families with examples ─────────────────────────────────────
    all_family_ids = ["A", "B", "C", "D", "E", "F"]
    families_with_plans = [
        f
        for f in all_family_ids
        if f in all_5_examples
        and (
            all_5_examples[f].get("dag_example")
            or all_5_examples[f].get("dag")
        )
    ]
    n_families = len(families_with_plans)

    analyst_only = not include_dag_examples
    family_intro = (
        f"{n_families}/6 families have validated gold examples on this dialect. "
        "Treat these as priors, not hard rules.\n\n"
        "Prioritize by: EXPLAIN bottleneck, transform precondition fit, and dialect gap match."
    )
    static.append(f"## Optimization Families\n\n{family_intro}\n")

    for family_id in all_family_ids:
        ex = all_5_examples.get(family_id)
        if ex and (ex.get("dag_example") or ex.get("dag")):
            static.append(format_family_section(
                family_id, ex, include_dag_examples, analyst_only=analyst_only,
            ))
            static.append("")
        else:
            static.append(format_family_description_only(family_id, dialect))
            static.append("")

    for section in extra_static_sections or []:
        if section:
            static.append(section)

    # ═══ DYNAMIC SUFFIX (unique per query — cache miss) ═══════════════

    # ── Query ──────────────────────────────────────────────────────
    dynamic.append(f"**Dialect**: {dialect.upper()}\n\n```sql\n{original_sql}\n```")

    # ── Execution Plan ─────────────────────────────────────────────
    dynamic.append(f"### Execution Plan\n\n```\n{explain_text}\n```")

    # ── IR Structure ───────────────────────────────────────────────
    dynamic.append(f"""### IR Structure (for patch targeting)

```
{ir_node_map}
```

**Note**: Use `by_node_id` (e.g., "S0") and `by_anchor_hash` (16-char hex) from map above to target patch operations.""")

    # ── Detected Patterns (per-query intelligence) ─────────────────
    if intelligence_brief:
        dynamic.append(f"""### Detected Patterns

{intelligence_brief}

**Instruction**: Prioritize detected patterns above. If a high-confidence
pathology is detected, your primary target SHOULD address it.""")

    return static, dynamic, n_families


def build_beam_compiler_prompt(
    query_id: str,
    original_sql: str,
    explain_text: str,
    ir_node_map: str,
    all_5_examples: Dict[str, Dict[str, Any]],
    dialect: str,
    strike_results: List[Dict[str, Any]],
    intelligence_brief: str = "",
    importance_stars: int = 2,
    schema_context: str = "",
    engine_knowledge: str = "",
    dispatch_hypothesis: str = "",
    dispatch_reasoning_trace: Optional[List[str]] = None,
    equivalence_tier: str = "",
) -> str:
    """Build compiler prompt from beam_compiler_v3 template + dynamic tail."""
    template = _load_prompt_template("beam_compiler_v3.txt")
    stars = max(1, min(3, int(importance_stars or 1)))
    star_label = "*" * stars

    dynamic: List[str] = [
        f"## Query ID\n{query_id}",
        (
            "## Runtime Dialect Contract\n"
            f"- target_dialect: {dialect}\n"
            "- runtime_dialect_is_source_of_truth: true\n"
            "- if static examples conflict, follow runtime dialect behavior"
        ),
        (
            "## Importance\n"
            f"- importance_stars: {stars}\n"
            f"- importance_label: {star_label}"
        ),
        f"## Original SQL\n```sql\n{original_sql}\n```",
        f"## Original Plan\n```\n{explain_text}\n```",
        f"## IR Structure + Anchor Hashes\n```\n{ir_node_map}\n```",
    ]
    if schema_context:
        dynamic.append(f"## Schema / Index / Stats Context\n{schema_context}")
    if engine_knowledge:
        dynamic.append(f"## Engine-Specific Knowledge\n{engine_knowledge}")
    if dispatch_hypothesis:
        dynamic.append(f"## Analyst Hypothesis\n{dispatch_hypothesis}")
    if dispatch_reasoning_trace:
        trace_lines = "\n".join(f"- {str(x)}" for x in dispatch_reasoning_trace[:5] if x)
        if trace_lines:
            dynamic.append(f"## Analyst Reasoning Trace\n{trace_lines}")
    if equivalence_tier:
        dynamic.append(f"## Equivalence Tier\n- {equivalence_tier}")
    if intelligence_brief:
        dynamic.append(f"## Additional Intelligence\n{intelligence_brief}")

    n_total = len(strike_results)
    n_pass = sum(
        1 for s in strike_results if s.get("status") in ("PASS", "WIN", "IMPROVED")
    )
    n_win = sum(1 for s in strike_results if s.get("status") == "WIN")
    dynamic.append(
        f"## Probe Summary\n{n_total} probes fired, {n_pass} passed validation, {n_win} showed speedup."
    )

    # BDA table for all probes (primary evidence surface).
    bda_lines = [
        "## BDA Table (all probes)\n",
        "| Probe | Transform | Family | Status | Failure Category | Speedup | Top EXPLAIN Nodes | Model Description | SQL Patch | Error/Notes |",
        "|-------|-----------|--------|--------|------------------|---------|-------------------|-------------------|-----------|-------------|",
    ]
    for s in strike_results:
        speedup = s.get("speedup")
        speedup_str = f"{speedup:.2f}x" if speedup else "-"
        error_str = _safe_md_cell(s.get("error") or "")
        failure_category = _safe_md_cell(s.get("failure_category") or "-")
        top_nodes = _safe_md_cell(
            _summarize_top_plan_nodes(s.get("explain_text") or "", max_nodes=2)
        )
        description = _safe_md_cell(s.get("description") or "")
        sql_ref = _safe_md_cell(s.get("probe_id")) if s.get("sql") else "-"
        bda_lines.append(
            f"| {_safe_md_cell(s.get('probe_id', '?'))} | "
                f"{_safe_md_cell(s.get('transform_id', '?'))} | "
                f"{_safe_md_cell(s.get('family', '?'))} | "
                f"{_safe_md_cell(s.get('status', '?'))} | "
                f"{failure_category} | {speedup_str} | {top_nodes} | {description} | {sql_ref} | {error_str} |"
        )
    dynamic.append("\n".join(bda_lines))

    # Full worker SQL patches (no truncation).
    sql_entries = [s for s in strike_results if s.get("sql")]
    if sql_entries:
        sql_sections = ["## Worker SQL Patches\n"]
        for s in sql_entries:
            speedup = s.get("speedup")
            speedup_str = f"{speedup:.2f}x" if speedup else "n/a"
            sql_sections.append(
                f"### {s.get('probe_id', '?')}: {s.get('transform_id', '?')} "
                f"({s.get('status', '?')}, {speedup_str})\n"
                f"```sql\n{s.get('sql', '').strip()}\n```\n"
            )
        dynamic.append("\n".join(sql_sections))

    if template:
        return f"{template}\n\n" + "\n\n".join(dynamic)

    return "\n\n".join(
        [
            "## Role",
            "You are the Beam Compiler. Output one DAG object or a JSON array with exactly two DAG objects.",
            "## Cache Boundary",
            "Everything below is query-specific input.",
        ]
        + dynamic
    )


def _summarize_top_plan_nodes(explain_text: str, max_nodes: int = 2) -> str:
    """Summarize top EXPLAIN operators by observed time."""
    if not explain_text:
        return "-"
    ops = _parse_explain_operators(explain_text)
    if not ops:
        return "-"
    top = ops[:max(1, max_nodes)]
    return "; ".join(f"{name} ({time_ms:.0f}ms)" for name, time_ms, _ in top)


# ── Helper: Load Gold Examples ─────────────────────────────────────────────

def load_gold_examples(dialect: str, base_path: str = "packages/qt-sql/qt_sql/examples") -> Dict[str, Dict[str, Any]]:
    """Load gold examples for all 5 families from disk.

    Args:
        dialect: "duckdb", "postgres", or "snowflake"
        base_path: Path to examples directory

    Returns:
        Dict mapping family ID (A-E) to example JSON
    """
    import json
    from pathlib import Path

    # Map families to example files (per dialect)
    # This is flexible - if Snowflake lacks a family, borrow from DuckDB/PG
    example_map = {
        "duckdb": {
            "A": "date_cte_isolate",
            "B": "decorrelate",
            "C": "aggregate_pushdown",
            "D": "intersect_to_exists",
            "E": "multi_dimension_prefetch",
            "F": "inner_join_conversion",
        },
        "postgres": {
            "A": "date_cte_explicit_join",
            "B": "shared_scan_decorrelate",
            "C": "materialized_dimension_fact_prefilter",
            "D": "intersect_to_exists",
            "E": "multi_dimension_prefetch",
            "F": "explicit_join_materialized",
        },
        "snowflake": {
            "A": "sk_pushdown_union_all",  # P4: date_sk pushdown (Family A = Early Filtering)
            "B": "inline_decorrelate",  # P3: decorrelation (Family B = Decorrelation)
            "C": "aggregate_pushdown",  # Borrow from DuckDB
            "D": "intersect_to_exists",  # Borrow from DuckDB
            "E": "multi_dimension_prefetch",  # Borrow from DuckDB
            "F": "inner_join_conversion",  # Borrow from DuckDB
        }
    }

    # Normalize dialect to lowercase for case-insensitive lookup
    dialect = dialect.lower().strip()

    examples = {}
    for family_id, example_name in example_map.get(dialect, {}).items():
        # Try native dialect first
        example_path = Path(base_path) / dialect / f"{example_name}.json"

        if not example_path.exists():
            # Fallback: try DuckDB (most complete)
            example_path = Path(base_path) / "duckdb" / f"{example_name}.json"

        if not example_path.exists():
            # Fallback: try PostgreSQL
            example_path = Path(base_path) / "postgres" / f"{example_name}.json"

        if example_path.exists():
            with open(example_path) as f:
                examples[family_id] = json.load(f)
        else:
            logger.warning(f"Example {example_name} for family {family_id} not found")

    return examples


# ── Snipe Prompt Builder ──────────────────────────────────────────────────

def _build_history_summary_table(
    all_iterations: List[List[Any]],
) -> str:
    """Build compact summary table of ALL prior iteration patches.

    One line per patch: iteration, patch_id, family, transform, speedup, status, bottleneck.
    This keeps prompt growth O(n_patches) — ~1 line per patch across all iterations.

    Args:
        all_iterations: List of lists of AppliedPatch objects, one list per iteration.

    Returns:
        Markdown table string.
    """
    lines = [
        "### History — All Prior Patches",
        "",
        "| Iter | Patch | Family | Transform | Speedup | Status | Orig ms | Patch ms | Error (summary) |",
        "|------|-------|--------|-----------|---------|--------|---------|----------|-----------------|",
    ]

    for iter_idx, patches in enumerate(all_iterations):
        for p in patches:
            speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "—"
            orig_str = f"{p.original_ms:.0f}" if p.original_ms is not None else "—"
            patch_str = f"{p.patch_ms:.0f}" if p.patch_ms is not None else "—"
            err_str = p.apply_error or ""
            lines.append(
                f"| {iter_idx} | {p.patch_id} | {p.family} | "
                f"{p.transform} | {speedup_str} | {p.status} | "
                f"{orig_str} | {patch_str} | {err_str} |"
            )

    return "\n".join(lines)


def _parse_explain_operators(
    explain_text: str,
) -> List[Tuple[str, float, str]]:
    """Parse compact EXPLAIN text into operator entries sorted by time desc.

    Returns list of (short_name, time_ms, bracket_info) tuples.
    """
    import re

    results: List[Tuple[str, float, str]] = []
    for line in explain_text.strip().split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("Total"):
            continue

        bracket_start = stripped.find("[")
        bracket_end = stripped.find("]")
        if bracket_start < 0 or bracket_end < 0:
            continue

        raw_op = stripped[:bracket_start].strip()
        bracket = stripped[bracket_start + 1 : bracket_end]

        # Extract time in ms
        time_match = re.search(r"(\d+\.?\d*)ms", bracket)
        if not time_match:
            continue
        time_ms = float(time_match.group(1))
        if time_ms < 1.0:
            continue  # skip sub-ms noise

        # Build short name: OPERATOR_TYPE [table_name | join_key]
        # e.g. "SEQ_SCAN  store_sales" -> "SEQ_SCAN store_sales"
        # e.g. "HASH_JOIN INNER on cd_demo_sk = c_current_cdemo_sk" -> "HASH_JOIN INNER (cd_demo_sk)"
        on_match = re.match(r"(.+?)\s+on\s+(\w+)", raw_op)
        if on_match:
            short_name = f"{on_match.group(1).strip()} ({on_match.group(2)})"
        else:
            # Collapse multiple spaces
            short_name = re.sub(r"\s+", " ", raw_op)

        # Extract row info for bracket display
        row_match = re.search(r"([\d.]+[KMB]?)\s+(?:of\s+[\d.]+[KMB]?\s+)?rows", bracket)
        row_str = row_match.group(1) if row_match else ""

        # Extract pct
        pct_match = re.search(r"(\d+)%", bracket)
        pct_str = f" ({pct_match.group(1)}%)" if pct_match else ""

        display = f"{short_name} [{row_str} rows]" if row_str else short_name
        results.append((display, time_ms, pct_str))

    results.sort(key=lambda x: x[1], reverse=True)
    return results


def _build_explain_comparison(
    original_explain: str,
    explains: Dict[str, str],
    patches: List[Any],
) -> str:
    """Build operator cost comparison table: original vs best winner, side by side.

    Shows top operators ranked by time, makes plan differences immediately visible.
    Also detects redundant plans (multiple winners with identical operator structure).
    """
    orig_ops = _parse_explain_operators(original_explain)
    if not orig_ops:
        return ""

    # Find winners with explains
    winners = [
        (p, explains.get(p.patch_id, ""))
        for p in patches
        if p.status == "WIN" and p.patch_id in explains and explains[p.patch_id]
    ]
    if not winners:
        return ""

    # Best winner by speedup
    best_patch, best_explain = max(winners, key=lambda x: x[0].speedup or 0)
    best_ops = _parse_explain_operators(best_explain)
    if not best_ops:
        return ""

    # Detect redundant plans: compare top-5 operator names.
    # Only compare when we parsed enough operators for a meaningful fingerprint.
    def _top_names(explain_text: str) -> tuple[List[str], int]:
        ops = _parse_explain_operators(explain_text)
        return [op[0] for op in ops[:5]], len(ops)

    best_names, best_count = _top_names(best_explain)
    redundant_ids = [best_patch.patch_id]
    complementary_ids = []
    for p, exp in winners:
        if p.patch_id == best_patch.patch_id:
            continue
        names, count = _top_names(exp)
        if best_count >= 3 and count >= 3 and names == best_names:
            redundant_ids.append(p.patch_id)
        else:
            complementary_ids.append(p.patch_id)

    lines = ["#### Operator Cost Comparison (original vs best winner)\n"]

    # Redundancy note
    if len(redundant_ids) > 1:
        lines.append(
            f"**Note**: Patches {', '.join(redundant_ids)} produce "
            f"**identical plans** (REDUNDANT — same structural change)."
        )
    if complementary_ids:
        lines.append(
            f"**Complementary**: Patches {', '.join(complementary_ids)} have "
            f"different plan structures."
        )
    lines.append("")

    # Build side-by-side table
    best_label = f"{best_patch.patch_id} ({best_patch.speedup:.2f}x)"
    lines.append(
        f"| # | Original Operator | Time | "
        f"{best_label} Operator | Time |"
    )
    lines.append("|---|------------------|------|" + "-" * (len(best_label) + 12) + "|------|")

    max_rows = min(max(len(orig_ops), len(best_ops)), 10)
    for i in range(max_rows):
        if i < len(orig_ops):
            o_name, o_time, o_pct = orig_ops[i]
            o_str = f"{o_name}"
            o_time_str = f"{o_time:.0f}ms{o_pct}"
        else:
            o_str, o_time_str = "—", "—"

        if i < len(best_ops):
            b_name, b_time, b_pct = best_ops[i]
            b_str = f"{b_name}"
            b_time_str = f"{b_time:.0f}ms{b_pct}"
        else:
            b_str, b_time_str = "—", "—"

        lines.append(f"| {i + 1} | {o_str} | {o_time_str} | {b_str} | {b_time_str} |")

    # Key changes summary
    lines.append("")
    lines.append("**Key changes**:")

    # Find operators in original that don't appear in winner (eliminated)
    orig_types = {op[0].split("[")[0].strip() for op in orig_ops if op[1] >= 10}
    best_types = {op[0].split("[")[0].strip() for op in best_ops if op[1] >= 10}
    eliminated = orig_types - best_types
    if eliminated:
        lines.append(f"- **ELIMINATED**: {', '.join(sorted(eliminated))}")

    # Biggest remaining operator
    if best_ops:
        top_op = best_ops[0]
        lines.append(
            f"- **Remaining bottleneck**: {top_op[0]} at {top_op[1]:.0f}ms{top_op[2]}"
        )

    lines.append("")
    return "\n".join(lines)


def _build_detailed_iteration_section(
    iteration: int,
    patches: List[Any],
    original_explain: str,
    explains: Dict[str, str],
    race_result: Optional[Any] = None,
) -> str:
    """Build full detail for the MOST RECENT iteration only.

    Includes race timings, EXPLAIN plans, patched SQL, and error details.

    Args:
        iteration: Iteration number (0-based).
        patches: AppliedPatch list from this iteration.
        original_explain: EXPLAIN text for original query.
        explains: Dict mapping patch_id → EXPLAIN text.
        race_result: Optional RaceResult from race validation.

    Returns:
        Detailed section string.
    """
    sections = []

    sections.append(f"### Latest Iteration {iteration} — Detailed Results\n")

    # Race results table
    sections.append("#### Race Results\n")
    sections.append("| Patch | Family | Transform | Speedup | Status | Orig ms | Patch ms | Semantic | Error |")
    sections.append("|-------|--------|-----------|---------|--------|---------|----------|----------|-------|")

    for p in patches:
        speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "—"
        orig_str = f"{p.original_ms:.0f}" if p.original_ms is not None else "—"
        patch_str = f"{p.patch_ms:.0f}" if p.patch_ms is not None else "—"
        sem_str = "PASS" if p.semantic_passed else "FAIL"
        error_str = p.apply_error or ""
        sections.append(
            f"| {p.patch_id} | {p.family} | {p.transform} | "
            f"{speedup_str} | {p.status} | "
            f"{orig_str} | {patch_str} | {sem_str} | {error_str} |"
        )

    # Race timings (if available)
    if race_result:
        sections.append("")
        orig_ms = race_result.original.elapsed_ms
        sections.append(f"**Original runtime**: {orig_ms:.0f}ms")

        for i, lane in enumerate(race_result.candidates):
            if i < len(patches):
                pid = patches[i].patch_id if patches[i].output_sql else "(failed)"
                status = "finished" if lane.finished else "DNF"
                sections.append(
                    f"  - {pid}: {lane.elapsed_ms:.0f}ms ({status})"
                )

    # ── Patched SQL for each patch (especially regressions/errors) ────
    sections.append("\n#### Patched SQL\n")
    for p in patches:
        if p.output_sql:
            label = f"{p.patch_id} (Family {p.family}, {p.transform})"
            if p.speedup is not None:
                label += f" — {p.speedup:.2f}x {p.status}"

            sections.append(f"**{label}:**")
            sections.append(f"```sql\n{p.output_sql.strip()}\n```\n")
        else:
            sections.append(
                f"**{p.patch_id}** (Family {p.family}): "
                f"FAILED to apply — {p.apply_error or 'unknown error'}\n"
            )

    # ── Execution Plans (compact parsed format, all patches) ─────
    sections.append("#### Execution Plans\n")

    sections.append("**Original EXPLAIN:**")
    sections.append(f"```\n{original_explain}\n```")

    # Show each patch's EXPLAIN; collapse identical plans only when
    # fingerprints are meaningful (>= 3 operators parsed).  When parsing
    # fails the fingerprint is empty/tiny for every patch, which would
    # incorrectly collapse unrelated plans.
    shown_fingerprints: Dict[tuple, str] = {}  # fingerprint → first patch_id
    for p in patches:
        if p.patch_id not in explains or not explains[p.patch_id]:
            continue
        explain_text = explains[p.patch_id]
        ops = _parse_explain_operators(explain_text)
        fp = tuple(op[0] for op in ops[:5])
        speedup_str = f"{p.speedup:.2f}x" if p.speedup is not None else "?"
        label = f"{p.patch_id} ({speedup_str} {p.status})"

        # Only deduplicate when we parsed enough operators for a reliable fingerprint
        if len(ops) >= 3 and fp in shown_fingerprints:
            sections.append(
                f"\n**{label} EXPLAIN:** same plan as {shown_fingerprints[fp]}"
            )
        else:
            if len(ops) >= 3:
                shown_fingerprints[fp] = p.patch_id
            sections.append(f"\n**{label} EXPLAIN:**")
            sections.append(f"```\n{explain_text}\n```")

    # ── Error Details ────────────────────────────────────────────────
    error_patches = [p for p in patches if p.apply_error]
    if error_patches:
        sections.append("\n#### Error Details\n")
        for p in error_patches:
            sections.append(f"- **{p.patch_id}** ({p.status}): {p.apply_error}")

    return "\n".join(sections)


def append_shot_results(
    base_prompt: str,
    patches: List[Any],
    explains: Dict[str, str],
) -> str:
    """Append shot 1 results to the base prompt for shot 2 (cache-hit pattern).

    The base prompt prefix is preserved exactly so the LLM's KV cache
    can be reused. Only results + new task are appended.

    Args:
        base_prompt: The shot 1 prompt (from build_beam_compiler_prompt).
        patches: List of AppliedPatch objects from shot 1 validation.
        explains: Dict mapping patch_id → EXPLAIN text for shot 1 patches.

    Returns:
        Shot 2 prompt = base_prompt + results + new task.
    """
    lines = [base_prompt, "", "## Shot 1 Results", ""]

    # Results table
    lines.append("| # | Family | Transform | Speedup | Status | Error |")
    lines.append("|---|--------|-----------|---------|--------|-------|")

    for p in patches:
        speedup_str = f"{p.speedup:.2f}x" if getattr(p, "speedup", None) is not None else "-"
        status = _safe_md_cell(getattr(p, "status", "?"))
        error = _safe_md_cell(getattr(p, "apply_error", "") or "")
        family = _safe_md_cell(getattr(p, "family", "?"))
        transform = _safe_md_cell(getattr(p, "transform", "?"))
        patch_id = _safe_md_cell(getattr(p, "patch_id", "?"))
        lines.append(
            f"| {patch_id} | {family} | {transform} | "
            f"{speedup_str} | {status} | {error} |"
        )

    lines.append("")

    # Show EXPLAIN plans for patches with speedup
    for p in patches:
        pid = getattr(p, "patch_id", "?")
        speedup = getattr(p, "speedup", None)
        status = getattr(p, "status", "?")

        if pid in explains and explains[pid]:
            speedup_str = f"{speedup:.2f}x" if speedup is not None else "?"
            lines.append(f"### {pid} EXPLAIN ({speedup_str} {status}):")
            lines.append(f"```\n{explains[pid].strip()}\n```")
            lines.append("")
        elif getattr(p, "apply_error", None):
            lines.append(f"### {pid} Error:")
            lines.append(p.apply_error)
            lines.append("")

    # Shot 2 task
    lines.extend(
        [
            "## Shot 2 — Design One or Two Compiler Plans",
            "",
            "Build on shot 1 results:",
            "1. Start from the strongest verified evidence in the BDA table.",
            "2. Refine the best winner or correct the most promising near-miss failure.",
            "3. Optionally produce a second materially different pathway for the remaining hotspot.",
            "",
            "If only one pathway is defensible, return a single plan.",
            "",
            "Output policy:",
            "- Output one DAG object, or a JSON array with exactly two DAG objects.",
            "- Follow the compiler contract in the base prompt.",
        ]
    )

    return "\n".join(lines)


# ── Utility: Print Prompt for Review ───────────────────────────────────────

def print_prompt_preview(prompt: str, max_lines: int = 100):
    """Print first N lines of prompt for review."""
    lines = prompt.split("\n")
    for i, line in enumerate(lines[:max_lines]):
        print(f"{i+1:3d} | {line}")
    if len(lines) > max_lines:
        print(f"\n... ({len(lines) - max_lines} more lines)")
