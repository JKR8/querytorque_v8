"""Snipe prompts for swarm mode — V2 architecture.

After fan-out workers attempt the target, the snipe flow runs:
  1. Analyst2: Diagnosis-then-synthesis — sees ALL worker SQL, errors, speedups,
     engine profile, EXPLAIN plan. Emits strategy for sniper + RETRY_WORTHINESS.
  2. Sniper: High-level reasoner with V2-quality context and full freedom.
  3. Sniper retry (optional): Gets compact failure digest, not raw SQL.

Old V1 functions (build_snipe_prompt, build_snipe_worker_context) preserved
for backward compatibility.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..schemas import WorkerResult
from .dag_helpers import append_dag_summary

if TYPE_CHECKING:
    from .swarm_parsers import SnipeAnalysisV2, WorkerAssignment

logger = logging.getLogger(__name__)


# =========================================================================
# V2 snipe architecture
# =========================================================================


def _normalize_speedup(speedup: Any) -> str:
    """Normalize a speedup value: strip trailing 'x', return clean string."""
    return str(speedup).rstrip("x")


def _build_worker_results_section(
    worker_results: List[WorkerResult],
    target_speedup: float,
    full_sql: bool = True,
) -> str:
    """Format ALL previous worker results for snipe prompts.

    Args:
        worker_results: All worker results across iterations.
        target_speedup: Target speedup ratio.
        full_sql: If True, include full SQL (analyst2). If False, compact table (sniper).

    Returns:
        Formatted string showing all previous attempts.
    """
    if not worker_results:
        return ""

    # Sort by speedup descending (best first)
    sorted_results = sorted(worker_results, key=lambda w: w.speedup, reverse=True)
    best_speedup = sorted_results[0].speedup if sorted_results else 0.0
    total = len(sorted_results)
    reaching = sum(1 for w in sorted_results if w.speedup >= target_speedup)

    lines: list[str] = []
    lines.append(f"## Previous Optimization Attempts")
    lines.append(f"Target: **>={_normalize_speedup(target_speedup)}x** | "
                 f"{total} workers tried | "
                 f"{'none' if reaching == 0 else reaching} reached target")
    lines.append("")

    if full_sql:
        # Full view for analyst2
        for wr in sorted_results:
            speedup = _normalize_speedup(wr.speedup)

            # Status label
            if wr.status == "ERROR" or wr.error_message:
                status_label = "ERROR"
            elif wr.speedup >= target_speedup:
                status_label = f"WIN ({speedup}x)"
            elif wr.speedup >= 1.0:
                status_label = f"PASS, below target ({speedup}x)"
            else:
                status_label = f"REGRESSION ({speedup}x)"

            best_marker = " ★ BEST" if wr.speedup == best_speedup and total > 1 else ""

            lines.append(f"### W{wr.worker_id}: {wr.strategy} → {speedup}x{best_marker} [{status_label}]")
            if wr.examples_used:
                lines.append(f"- **Examples**: {', '.join(wr.examples_used)}")
            if wr.transforms:
                lines.append(f"- **Transforms**: {', '.join(wr.transforms)}")
            if wr.hint:
                lines.append(f"- **Approach**: {wr.hint}")
            if wr.error_message:
                lines.append(f"- **Error**: {wr.error_message}")

            # Full SQL for analyst2
            sql = wr.optimized_sql.strip()
            if sql:
                lines.append("- **Optimized SQL:**")
                lines.append("```sql")
                lines.append(sql)
                lines.append("```")
            lines.append("")
    else:
        # Compact table for sniper
        lines.append("| Worker | Strategy | Speedup | Status | Error |")
        lines.append("|--------|----------|---------|--------|-------|")
        for wr in sorted_results:
            speedup = _normalize_speedup(wr.speedup)
            err = wr.error_message[:60] if wr.error_message else ""
            best_marker = " ★" if wr.speedup == best_speedup and total > 1 else ""
            lines.append(
                f"| W{wr.worker_id}{best_marker} | {wr.strategy} | "
                f"{speedup}x | {wr.status} | {err} |"
            )
        lines.append("")

    return "\n".join(lines)


def build_snipe_analyst_prompt(
    query_id: str,
    original_sql: str,
    worker_results: List[WorkerResult],
    target_speedup: float,
    dag: Any,
    costs: Dict[str, Any],
    explain_plan_text: Optional[str] = None,
    engine_profile: Optional[Dict[str, Any]] = None,
    constraints: Optional[List[Dict[str, Any]]] = None,
    matched_examples: Optional[List[Dict[str, Any]]] = None,
    all_available_examples: Optional[List[Dict[str, str]]] = None,
    semantic_intents: Optional[Dict[str, Any]] = None,
    regression_warnings: Optional[List[Dict[str, Any]]] = None,
    resource_envelope: Optional[str] = None,
    dialect: str = "duckdb",
    dialect_version: Optional[str] = None,
) -> str:
    """Build V2 snipe analyst prompt — diagnosis-then-synthesis.

    The analyst2 sees FULL consolidated info: all worker SQL, errors,
    speedups, engine profile, EXPLAIN plan. Its job is to diagnose WHY
    the best worker won and WHY the others didn't, then synthesize a
    strategy that couldn't have been designed without those empirical results.

    Args:
        query_id: Query identifier
        original_sql: The original SQL query
        worker_results: ALL results from previous iterations
        target_speedup: Target speedup ratio
        dag: Parsed DAG
        costs: Per-node cost analysis
        explain_plan_text: EXPLAIN ANALYZE plan text (may be None)
        engine_profile: Engine profile JSON with optimizer strengths/gaps
        constraints: Correctness constraints (4 gates)
        matched_examples: Tag-matched examples (full metadata)
        all_available_examples: Full catalog (compact)
        semantic_intents: Pre-computed per-query semantic intents
        regression_warnings: Regression examples
        resource_envelope: PG system resource envelope text
        dialect: SQL dialect
        dialect_version: Engine version string

    Returns:
        Complete analyst2 prompt string
    """
    from .analyst_briefing import (
        _add_line_numbers,
        _format_constraint_for_analyst,
        _format_example_compact,
        _format_example_full,
        _format_regression_for_analyst,
        _strip_template_comments,
        format_duckdb_explain_tree,
    )
    from ..analyst import _append_dag_analysis
    from ..node_prompter import _build_node_intent_map

    target = _normalize_speedup(target_speedup)
    lines: list[str] = []

    # ── 1. Role ──────────────────────────────────────────────────────────
    lines.append(
        f"You are the diagnostic analyst for query {query_id}. "
        f"You've seen {len(worker_results)} parallel attempts at >={target}x "
        f"speedup on this query. Your job: diagnose what worked, what didn't, "
        f"and WHY — then design a strategy the sniper couldn't have known "
        f"without these empirical results."
    )
    lines.append("")

    # ── 2. Target ────────────────────────────────────────────────────────
    lines.append(f"## Target: >={target}x speedup")
    lines.append(f"Anything below {target}x is a miss. The sniper you deploy "
                 "must be given a strategy with genuine headroom to reach this bar.")
    lines.append("")

    # ── 3. Previous Attempts (PRIMACY) ───────────────────────────────────
    lines.append(_build_worker_results_section(worker_results, target_speedup, full_sql=True))

    # ── 4. Original SQL ─────────────────────────────────────────────────
    dialect_str = dialect
    if dialect_version:
        dialect_str += f" v{dialect_version}"
    lines.append(f"## Original SQL ({query_id}, {dialect_str})")
    lines.append("")
    clean_sql = _strip_template_comments(original_sql)
    lines.append("```sql")
    lines.append(_add_line_numbers(clean_sql))
    lines.append("```")
    lines.append("")

    # ── 5. EXPLAIN plan ─────────────────────────────────────────────────
    if explain_plan_text:
        formatted_plan = format_duckdb_explain_tree(explain_plan_text)
        lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("")
        lines.append("```")
        plan_lines = formatted_plan.split("\n")
        if len(plan_lines) > 150:
            lines.extend(plan_lines[:150])
            lines.append(f"... ({len(plan_lines) - 150} more lines truncated)")
        else:
            lines.append(formatted_plan)
        lines.append("```")
        lines.append("")

    # ── 6. DAG structure with costs ─────────────────────────────────────
    lines.append("## Query Structure (DAG)")
    lines.append("")
    node_intents = _build_node_intent_map(semantic_intents)
    if semantic_intents:
        qi = semantic_intents.get("query_intent", "")
        if qi and "main_query" not in node_intents:
            node_intents["main_query"] = qi
    _append_dag_analysis(lines, dag, costs, dialect=dialect, node_intents=node_intents)
    lines.append("")

    # ── 7. Aggregation semantics check ──────────────────────────────────
    lines.append("## Aggregation Semantics Check")
    lines.append("")
    lines.append(
        "- **STDDEV_SAMP(x)** requires >=2 non-NULL values per group. "
        "Changing group membership changes the result.\n"
        "- **AVG and STDDEV are NOT duplicate-safe**: join-introduced "
        "row duplication changes the aggregate.\n"
        "- When splitting with GROUP BY + aggregate, each branch must preserve "
        "exact GROUP BY columns and filter to the same row set."
    )
    lines.append("")

    # ── 8. Engine profile ───────────────────────────────────────────────
    if engine_profile:
        briefing_note = engine_profile.get("briefing_note", "")
        lines.append("## Engine Profile")
        lines.append("")
        if briefing_note:
            lines.append(f"*{briefing_note}*")
            lines.append("")

        strengths = engine_profile.get("strengths", [])
        if strengths:
            lines.append("### Optimizer Strengths (DO NOT fight these)")
            for s in strengths:
                sid = s.get("id", "")
                summary = s.get("summary", "")
                lines.append(f"- **{sid}**: {summary}")
            lines.append("")

        gaps = engine_profile.get("gaps", [])
        if gaps:
            lines.append("### Optimizer Gaps (opportunities)")
            for g in gaps:
                gid = g.get("id", "")
                what = g.get("what", "")
                opportunity = g.get("opportunity", "")
                lines.append(f"- **{gid}**: {what}")
                if opportunity:
                    lines.append(f"  Opportunity: {opportunity}")
            lines.append("")

    # ── 9. Tag-matched examples ─────────────────────────────────────────
    if matched_examples:
        lines.append(f"## Tag-Matched Examples ({len(matched_examples)})")
        lines.append("")
        for ex in matched_examples:
            lines.append(_format_example_full(ex))
            lines.append("")

    # ── 10. Regression warnings ─────────────────────────────────────────
    if regression_warnings:
        lines.append("## Regression Warnings")
        lines.append("")
        for reg in regression_warnings:
            lines.append(_format_regression_for_analyst(reg))
            lines.append("")

    # ── 11. Correctness constraints (4 gates, no duplication) ───────────
    correctness_ids = {
        "LITERAL_PRESERVATION", "SEMANTIC_EQUIVALENCE",
        "COMPLETE_OUTPUT", "CTE_COLUMN_COMPLETENESS",
    }
    if constraints:
        cc = [c for c in constraints if c.get("id") in correctness_ids]
        if cc:
            lines.append(f"## Correctness Constraints ({len(cc)} — NEVER violate)")
            lines.append("")
            for c in cc:
                lines.append(_format_constraint_for_analyst(c))
                lines.append("")

    # ── 12. Task — 3-step chain ─────────────────────────────────────────
    lines.append("## Your Task")
    lines.append("")
    lines.append(
        "Work through these 3 steps in a `<reasoning>` block, then output "
        "the structured briefing below:"
    )
    lines.append("")
    lines.append(
        f"1. **DIAGNOSE**: Why did the best worker achieve {_normalize_speedup(worker_results[0].speedup if worker_results else 0)}x "
        f"instead of the {target}x target? Why did each other worker fail or regress? "
        f"Be specific about structural mechanisms."
    )
    lines.append(
        "2. **IDENTIFY**: What optimization angles couldn't have been designed "
        "BEFORE seeing these empirical results? What did the results reveal "
        "about the query's actual execution behavior?"
    )
    lines.append(
        "3. **SYNTHESIZE**: Design a strategy for the sniper that builds on "
        "the best foundation (if any) and exploits the newly-revealed angles. "
        "The sniper has full freedom — give it direction, not constraints."
    )
    lines.append("")

    # ── 13. Output format ───────────────────────────────────────────────
    lines.append("### Output Format (follow EXACTLY)")
    lines.append("")
    lines.append("```")
    lines.append("=== SNIPE BRIEFING ===")
    lines.append("")
    lines.append("FAILURE_SYNTHESIS:")
    lines.append("<WHY the best worker won, WHY each other failed — structural mechanisms>")
    lines.append("")
    lines.append("BEST_FOUNDATION:")
    lines.append("<What to build on from the best result, or 'None — start fresh' if all regressed>")
    lines.append("")
    lines.append("UNEXPLORED_ANGLES:")
    lines.append("<What optimization approaches couldn't have been designed pre-empirically>")
    lines.append("")
    lines.append("STRATEGY_GUIDANCE:")
    lines.append("<Synthesized approach for the sniper — ADVISORY, not mandatory>")
    lines.append("")
    lines.append("EXAMPLES: <ex1>, <ex2>, <ex3>")
    lines.append("")
    lines.append("EXAMPLE_ADAPTATION:")
    lines.append("<For each example: what to APPLY and what to IGNORE>")
    lines.append("")
    lines.append("HAZARD_FLAGS:")
    lines.append("<Risks based on observed failures — what NOT to do>")
    lines.append("")
    lines.append("RETRY_WORTHINESS: high|low — <reason>")
    lines.append(f"(Is there genuine headroom for a second sniper attempt if the first misses {target}x?)")
    lines.append("")
    lines.append("RETRY_DIGEST:")
    lines.append("<5-10 line compact failure guide for sniper2 IF retry is needed.")
    lines.append("What broke, why, what to change. The lesson, not the artifact.>")
    lines.append("```")

    return "\n".join(lines)


def build_sniper_prompt(
    snipe_analysis: "SnipeAnalysisV2",
    original_sql: str,
    worker_results: List[WorkerResult],
    best_worker_sql: Optional[str],
    examples: List[Dict[str, Any]],
    output_columns: List[str],
    dag: Any,
    costs: Dict[str, Any],
    engine_profile: Optional[Dict[str, Any]] = None,
    constraints: Optional[List[Dict[str, Any]]] = None,
    semantic_intents: Optional[Dict[str, Any]] = None,
    regression_warnings: Optional[List[Dict[str, Any]]] = None,
    dialect: str = "duckdb",
    engine_version: Optional[str] = None,
    resource_envelope: Optional[str] = None,
    target_speedup: float = 2.0,
    previous_sniper_result: Optional[WorkerResult] = None,
) -> str:
    """Build the sniper's prompt — high-level reasoner with V2-quality context.

    The sniper has FULL FREEDOM to design its own approach. It gets the
    analyst's diagnosis as advisory guidance, not mandatory instructions.

    Args:
        snipe_analysis: Parsed SnipeAnalysisV2 from analyst2
        original_sql: The original SQL query
        worker_results: ALL previous worker results
        best_worker_sql: Full optimized SQL of the best result (if any > 1.0x)
        examples: Loaded gold examples (full before/after SQL)
        output_columns: Expected output columns for completeness contract
        dag: Parsed DAG
        costs: Per-node cost analysis
        engine_profile: Engine profile JSON
        constraints: Correctness constraints
        semantic_intents: Pre-computed semantic intents
        regression_warnings: Regression examples
        dialect: SQL dialect
        engine_version: Engine version string
        resource_envelope: PG system resource envelope text
        target_speedup: Target speedup ratio
        previous_sniper_result: Previous sniper result (for retry)

    Returns:
        Complete sniper prompt string
    """
    from .worker_v2 import _section_examples, _section_output_format, _section_set_local_config
    from .analyst_briefing import _format_regression_for_analyst
    from .briefing_checks import build_worker_rewrite_checklist

    target = _normalize_speedup(target_speedup)
    sections: list[str] = []

    # ── RETRY prepend (only for sniper retry) ────────────────────────────
    if previous_sniper_result is not None:
        retry_lines = [
            "## PREVIOUS SNIPER ATTEMPT (iter 1) — Learn from this",
            "",
        ]
        prev_speedup = _normalize_speedup(previous_sniper_result.speedup)
        retry_lines.append(
            f"Your first attempt achieved **{prev_speedup}x** "
            f"against a target of **{target}x**."
        )
        if previous_sniper_result.error_message:
            retry_lines.append(f"**Error**: {previous_sniper_result.error_message}")
        retry_lines.append("")

        # Use analyst's compact retry digest (the lesson, not the artifact)
        if snipe_analysis.retry_digest:
            retry_lines.append("### What went wrong and what to change:")
            retry_lines.append(snipe_analysis.retry_digest)
        else:
            retry_lines.append(
                f"Previous strategy: {previous_sniper_result.strategy}. "
                f"Result: {previous_sniper_result.status} at {prev_speedup}x. "
                f"Try a fundamentally different approach."
            )
        retry_lines.append("")
        retry_lines.append("---")
        retry_lines.append("")
        sections.append("\n".join(retry_lines))

    # ── 1. Role ──────────────────────────────────────────────────────────
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
    }
    engine = engine_names.get(dialect, dialect)
    ver = f" v{engine_version}" if engine_version else ""

    sections.append(
        f"You are a senior SQL optimization architect for {engine}{ver}. "
        f"You have FULL FREEDOM to design your own approach — you are NOT "
        f"constrained to any specific DAG topology or CTE structure. "
        f"The analyst's strategy guidance below is ADVISORY, not mandatory.\n\n"
        f"Preserve defensive guards: if the original uses CASE WHEN x > 0 THEN "
        f"y/x END around a division, keep it — guards prevent silent breakage. "
        f"Strip benchmark comments (-- start query, -- end query) from output."
    )

    # ── 2. Target ────────────────────────────────────────────────────────
    sections.append(
        f"## Target: >={target}x speedup\n\n"
        f"Your target is >={target}x speedup on this query. This is the bar. "
        f"Anything below {target}x is a miss."
    )

    # ── 3. Previous attempts summary (compact) ──────────────────────────
    sections.append(
        _build_worker_results_section(worker_results, target_speedup, full_sql=False)
    )

    # ── 4. Best foundation SQL ──────────────────────────────────────────
    if best_worker_sql:
        sections.append(
            "## Best Foundation SQL\n\n"
            "The best previous result. You may build on this or start fresh.\n\n"
            "```sql\n"
            + best_worker_sql.strip() + "\n"
            "```"
        )

    # ── 5. Failure synthesis (from analyst) ─────────────────────────────
    if snipe_analysis.failure_synthesis:
        sections.append(
            "## Failure Synthesis (from diagnostic analyst)\n\n"
            + snipe_analysis.failure_synthesis
        )

    # ── 6. Unexplored angles ────────────────────────────────────────────
    if snipe_analysis.unexplored_angles:
        sections.append(
            "## Unexplored Angles\n\n"
            + snipe_analysis.unexplored_angles
        )

    # ── 7. Strategy guidance (ADVISORY) ─────────────────────────────────
    if snipe_analysis.strategy_guidance:
        sections.append(
            "## Strategy Guidance (ADVISORY — not mandatory)\n\n"
            + snipe_analysis.strategy_guidance
        )

    # ── 8. Example adaptation notes ─────────────────────────────────────
    if snipe_analysis.example_adaptation:
        sections.append(
            "## Example Adaptation Notes\n\n"
            + snipe_analysis.example_adaptation
        )

    # ── 9. Reference examples ───────────────────────────────────────────
    if examples:
        sections.append(_section_examples(examples))

    # ── 10. Hazard flags ────────────────────────────────────────────────
    if snipe_analysis.hazard_flags:
        sections.append(
            "## Hazard Flags\n\n"
            + snipe_analysis.hazard_flags
        )

    # ── 11. Engine profile ──────────────────────────────────────────────
    if engine_profile:
        ep_lines = ["## Engine Profile"]
        strengths = engine_profile.get("strengths", [])
        if strengths:
            ep_lines.append("")
            ep_lines.append("### Optimizer Strengths (DO NOT fight these)")
            for s in strengths:
                ep_lines.append(f"- **{s.get('id', '')}**: {s.get('summary', '')}")

        gaps = engine_profile.get("gaps", [])
        if gaps:
            ep_lines.append("")
            ep_lines.append("### Optimizer Gaps (opportunities)")
            for g in gaps:
                gid = g.get("id", "")
                what = g.get("what", "")
                opportunity = g.get("opportunity", "")
                ep_lines.append(f"- **{gid}**: {what}")
                if opportunity:
                    ep_lines.append(f"  Opportunity: {opportunity}")
                what_worked = g.get("what_worked", [])
                if what_worked:
                    for w in what_worked[:3]:
                        ep_lines.append(f"    + {w}")
        sections.append("\n".join(ep_lines))

    # ── 12. Correctness invariants (4 constraints — HARD STOPS) ─────────
    correctness_ids = {
        "LITERAL_PRESERVATION", "SEMANTIC_EQUIVALENCE",
        "COMPLETE_OUTPUT", "CTE_COLUMN_COMPLETENESS",
    }
    if constraints:
        cc = [c for c in constraints if c.get("id") in correctness_ids]
        if cc:
            ci_lines = [
                "## Correctness Invariants (HARD STOPS — non-negotiable)",
                "",
                "These 4 constraints are absolute. Even with full creative freedom, "
                "you may NEVER violate these:",
                "",
            ]
            for c in cc:
                cid = c.get("id", "?")
                instruction = c.get("prompt_instruction", c.get("description", ""))
                ci_lines.append(f"- **{cid}**: {instruction}")
            sections.append("\n".join(ci_lines))

    # ── 13. Aggregation semantics check (HARD STOP) ─────────────────────
    sections.append(
        "## Aggregation Semantics Check (HARD STOP)\n\n"
        "- STDDEV_SAMP/VARIANCE are grouping-sensitive — changing group "
        "membership changes the result.\n"
        "- AVG and STDDEV are NOT duplicate-safe.\n"
        "- FILTER over a combined group != separate per-group computation.\n"
        "- Verify aggregation equivalence for ANY proposed restructuring."
    )

    # ── 14. Regression warnings ─────────────────────────────────────────
    if regression_warnings:
        rw_lines = ["## Regression Warnings"]
        rw_lines.append("")
        for reg in regression_warnings:
            rw_lines.append(_format_regression_for_analyst(reg))
            rw_lines.append("")
        sections.append("\n".join(rw_lines))

    # ── 15. Original SQL (full, line-numbered) ──────────────────────────
    from .analyst_briefing import _add_line_numbers, _strip_template_comments
    clean_sql = _strip_template_comments(original_sql)
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + _add_line_numbers(clean_sql) + "\n"
        "```"
    )

    # ── 16. SET LOCAL config (PG only) ──────────────────────────────────
    if dialect in ("postgres", "postgresql") and resource_envelope:
        sections.append(_section_set_local_config(resource_envelope))

    # ── 17. Rewrite checklist ───────────────────────────────────────────
    sections.append(build_worker_rewrite_checklist())

    # ── 18. Column completeness contract + output format ────────────────
    sections.append(_section_output_format(output_columns))

    return "\n\n".join(sections)


# =========================================================================
# V1 snipe functions — preserved for backward compatibility
# =========================================================================


def build_snipe_worker_context(
    bench: Any,
    assignments: "List[WorkerAssignment]",
    worker_sqls: Dict[int, str],
    target_speedup: float,
) -> str:
    """Build rich context header for the snipe worker from all previous attempts.

    .. deprecated:: V2 architecture uses build_sniper_prompt() instead.
    """
    # Build lookup maps: worker_id -> assignment, worker_id -> bench result
    asn_map = {a.worker_id: a for a in assignments}

    # Sort workers by speedup descending (best first)
    sorted_workers = sorted(bench.workers, key=lambda w: w.speedup, reverse=True)
    best_speedup = sorted_workers[0].speedup if sorted_workers else 0.0

    total = len(sorted_workers)
    reaching = sum(1 for w in sorted_workers if w.speedup >= target_speedup)

    lines: list[str] = []
    lines.append(f"## Previous Optimization Attempts ({total} workers tried, "
                 f"{'none' if reaching == 0 else reaching} reached {target_speedup:.1f}x)")
    lines.append("")

    strategies_tried: list[str] = []
    examples_tried: set[str] = set()

    for w in sorted_workers:
        asn = asn_map.get(w.worker_id)
        strategy = asn.strategy if asn else "unknown"
        hint = asn.hint if asn else ""
        examples = asn.examples if asn else []

        strategies_tried.append(strategy)
        examples_tried.update(examples)

        # Status label
        if w.status == "error":
            status_label = "ERROR"
        elif w.speedup >= target_speedup:
            status_label = f"WIN ({w.speedup:.2f}x)"
        elif w.speedup >= 1.0:
            status_label = f"PASS, below target ({w.speedup:.2f}x)"
        else:
            status_label = f"REGRESSION ({w.speedup:.2f}x)"

        best_marker = " ★ BEST" if w.speedup == best_speedup and total > 1 else ""

        lines.append(f"### W{w.worker_id}: {strategy} → {w.speedup:.2f}x{best_marker} [{status_label}]")

        if examples:
            lines.append(f"- **Examples**: {', '.join(examples)}")
        if hint:
            lines.append(f"- **Approach**: {hint}")
        if w.error:
            lines.append(f"- **Error**: {w.error}")

        # Abbreviated SQL (first 20 lines)
        sql = worker_sqls.get(w.worker_id, "")
        if sql.strip():
            sql_lines = sql.strip().split("\n")
            if len(sql_lines) > 20:
                preview = "\n".join(sql_lines[:20]) + "\n-- ... (truncated)"
            else:
                preview = sql.strip()
            lines.append("- **SQL** (abbreviated):")
            lines.append("```sql")
            lines.append(preview)
            lines.append("```")

        lines.append("")

    # Summary of exhausted approaches
    lines.append("### What's been exhausted")
    lines.append(f"- **Strategies tried**: {', '.join(strategies_tried)}")
    if examples_tried:
        lines.append(f"- **Examples used across workers**: {', '.join(sorted(examples_tried))}")
    if best_speedup > 0:
        lines.append(f"- **Best result**: {best_speedup:.2f}x (target: {target_speedup:.1f}x)")
    lines.append("")

    lines.append("### Your task")
    lines.append(f"Find an UNEXPLORED optimization angle that reaches >={target_speedup:.1f}x. "
                 "Do NOT repeat the strategies above — they have already been tried and fell short.")
    lines.append("")
    lines.append("---")
    lines.append("")

    return "\n".join(lines)


def build_snipe_prompt(
    query_id: str,
    original_sql: str,
    worker_results: List[WorkerResult],
    target_speedup: float,
    dag: Any,
    costs: Dict[str, Any],
    all_available_examples: List[Dict[str, str]],
    dialect: str = "duckdb",
) -> str:
    """Build V1 prompt asking analyst to synthesize failures into refined approach.

    .. deprecated:: V2 architecture uses build_snipe_analyst_prompt() instead.
    """
    lines = []

    # Role
    lines.append(
        f"You are analyzing {len(worker_results)} failed optimization attempts "
        f"to design a refined approach that reaches {target_speedup:.1f}x speedup."
    )
    lines.append("")
    lines.append(
        "Your job: understand WHY each attempt fell short, identify unexplored "
        "optimization angles, and synthesize a NEW strategy that combines the "
        "best insights while avoiding repeated mistakes."
    )
    lines.append("")

    # Query
    lines.append(f"## Query: {query_id}")
    lines.append(f"## Target: {target_speedup:.1f}x speedup")
    lines.append(f"## Dialect: {dialect}")
    lines.append("")
    lines.append("```sql")
    lines.append(original_sql.strip())
    lines.append("```")
    lines.append("")

    # Failed attempts
    lines.append("## Previous Attempts")
    lines.append("")
    for wr in worker_results:
        lines.append(f"### Worker {wr.worker_id}: {wr.strategy}")
        lines.append(f"- **Status**: {wr.status} ({wr.speedup:.2f}x)")
        if wr.error_message:
            lines.append(f"- **Error**: {wr.error_message}")
        lines.append(f"- **Transforms**: {', '.join(wr.transforms) or 'none'}")
        lines.append(f"- **Examples used**: {', '.join(wr.examples_used)}")
        if wr.hint:
            lines.append(f"- **Strategy hint**: {wr.hint}")
        lines.append("")
        # Show abbreviated optimized SQL (first 30 lines)
        sql_lines = wr.optimized_sql.strip().split("\n")
        if len(sql_lines) > 30:
            sql_preview = "\n".join(sql_lines[:30]) + "\n-- ... (truncated)"
        else:
            sql_preview = wr.optimized_sql.strip()
        lines.append("```sql")
        lines.append(sql_preview)
        lines.append("```")
        lines.append("")

    # DAG summary
    lines.append("## DAG Structure & Bottlenecks")
    lines.append("")
    append_dag_summary(lines, dag, costs, include_operations=False)
    lines.append("")

    # Full catalog
    lines.append("## Available Examples (Full Catalog)")
    lines.append("")
    for ex in all_available_examples:
        ex_id = ex.get("id", "?")
        speedup = _normalize_speedup(ex.get("speedup", "?"))
        desc = ex.get("description", "")[:100]
        lines.append(f"- **{ex_id}** ({speedup}x) — {desc}")
    lines.append("")

    # Task
    lines.append("## Your Task")
    lines.append("")
    lines.append("Analyze the failed attempts and design a refined approach:")
    lines.append("")
    lines.append("1. **Failure Analysis**: Why did all attempts fall short? Be specific about mechanisms.")
    lines.append("2. **Common Patterns**: What did multiple workers try unsuccessfully?")
    lines.append("3. **Unexplored Space**: What optimization angles were missed entirely?")
    lines.append("4. **Refined Strategy**: Synthesize a NEW approach combining best insights.")
    lines.append("")

    # Output format
    lines.append("### Output Format (follow EXACTLY)")
    lines.append("")
    lines.append("```")
    lines.append("FAILURE_ANALYSIS:")
    lines.append("<Why all workers fell short — be specific about mechanisms>")
    lines.append("")
    lines.append("UNEXPLORED_OPPORTUNITIES:")
    lines.append("<What optimization approaches haven't been tried>")
    lines.append("")
    lines.append("REFINED_STRATEGY:")
    lines.append("<Concrete optimization approach for next attempt>")
    lines.append("")
    lines.append("EXAMPLES: <ex1>, <ex2>, <ex3>")
    lines.append("HINT: <specific guidance for the refined attempt>")
    lines.append("```")

    return "\n".join(lines)
