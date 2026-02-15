"""Snipe prompts for beam mode.

Current flow (canonical): Self-directed retry worker — single LLM call per retry
iteration. The worker diagnoses failures, identifies unexplored angles, and
produces optimized SQL in one pass.

Deprecated flow (V1): Two-call analyst + sniper. Functions kept for reference:
  - build_snipe_analyst_prompt() — DEPRECATED
  - build_sniper_prompt() — DEPRECATED
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from ..schemas import WorkerResult

if TYPE_CHECKING:
    from .parsers import SnipeAnalysis

logger = logging.getLogger(__name__)


def _normalize_speedup(speedup: Any) -> str:
    """Normalize a speedup value: strip trailing 'x', return clean string."""
    return str(speedup).rstrip("x")


def _build_worker_results_section(
    worker_results: List[WorkerResult],
    target_speedup: float,
    full_sql: bool = True,
    candidate_explains: Optional[Dict[int, str]] = None,
    race_timings: Optional[Dict[str, Any]] = None,
) -> str:
    """Format ALL previous worker results for snipe prompts.

    Args:
        worker_results: All worker results across iterations.
        target_speedup: Target speedup ratio.
        full_sql: If True, include full SQL + EXPLAIN (analyst2). If False, compact table (sniper).
        candidate_explains: Optional dict of worker_id → EXPLAIN ANALYZE text.

    Returns:
        Formatted string showing all previous attempts.
    """
    if not worker_results:
        return ""

    explains = candidate_explains or {}

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

    # Race timing summary (if available)
    if race_timings:
        orig_ms = race_timings.get("original_ms", 0)
        has_winner = race_timings.get("has_clear_winner", False)
        lines.append("### Race Results (all ran in parallel, identical conditions)")
        lines.append(f"- **Original**: {orig_ms:.0f}ms")
        candidate_times = race_timings.get("candidates", {})
        for wid, info in sorted(candidate_times.items()):
            ms = info.get("elapsed_ms", 0)
            finished = info.get("finished", True)
            if not finished:
                lines.append(f"- **W{wid}**: DID_NOT_FINISH (slower than original)")
            elif info.get("error"):
                lines.append(f"- **W{wid}**: ERROR — {info['error'][:80]}")
            else:
                delta = ((orig_ms / ms) - 1) * 100 if ms > 0 else 0
                sign = "+" if delta > 0 else ""
                lines.append(f"- **W{wid}**: {ms:.0f}ms ({sign}{delta:.1f}%)")
        if not has_winner:
            lines.append("")
            lines.append("**No clear winner** — all candidates within margin or slower. "
                         "Diagnose what prevented a breakaway.")
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

            # Execution plan or error context (co-located with SQL)
            explain_text = explains.get(wr.worker_id)
            if explain_text:
                # Detect whether this is an actual plan or error context
                if explain_text.startswith("[EXPLAIN failed"):
                    label = "Planner Diagnosis"
                elif explain_text.startswith("[EXPLAIN estimate"):
                    label = "Execution Plan (EXPLAIN estimate — query errored)"
                else:
                    label = "Execution Plan (EXPLAIN ANALYZE)"
                lines.append(f"- **{label}:**")
                lines.append("```")
                plan_lines = explain_text.split("\n")
                if len(plan_lines) > 80:
                    lines.extend(plan_lines[:80])
                    lines.append(f"... ({len(plan_lines) - 80} more lines truncated)")
                else:
                    lines.extend(plan_lines)
                lines.append("```")

            # Semantic validation diagnostics (if failed)
            if wr.semantic_validation and not wr.semantic_validation.passed:
                sem = wr.semantic_validation
                lines.append("- **Semantic Validation Failure:**")
                lines.append(f"  - Failed at Tier {sem.tier_passed}")
                lines.append(f"  - Errors: {', '.join(sem.errors[:2])}")

                # SQL diff
                if sem.sql_diff:
                    lines.append("  - **SQL Diff** (original vs rewrite):")
                    lines.append("    ```diff")
                    diff_lines = sem.sql_diff.split("\n")
                    if len(diff_lines) > 20:
                        lines.extend(diff_lines[:20])
                        lines.append(f"    ... ({len(diff_lines) - 20} more lines)")
                    else:
                        lines.extend(diff_lines)
                    lines.append("    ```")

                # Row count diff
                if sem.row_count_diff:
                    rcd = sem.row_count_diff
                    lines.append(f"  - **Row Count** (on {rcd.sample_pct}% sample):")
                    lines.append(f"    - Original: {rcd.original_count} rows")
                    lines.append(f"    - Rewrite: {rcd.rewrite_count} rows")
                    lines.append(f"    - Difference: {rcd.diff:+d} rows")

                # Value diffs
                if sem.value_diffs:
                    from ..validation.sql_differ import SQLDiffer
                    lines.append("  - **Value Differences** (first 10):")
                    diff_text = SQLDiffer.format_value_diffs(sem.value_diffs, max_per_column=2)
                    for diff_line in diff_text.split("\n"):
                        if diff_line.strip():
                            lines.append(f"    {diff_line}")

                # Column mismatch
                if sem.column_mismatch:
                    cm = sem.column_mismatch
                    lines.append(f"  - **Column Mismatch:**")
                    if cm.missing:
                        lines.append(f"    - Missing from rewrite: {', '.join(cm.missing[:5])}")
                    if cm.extra:
                        lines.append(f"    - Extra in rewrite: {', '.join(cm.extra[:5])}")

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


def build_retry_worker_prompt(
    original_sql: str,
    worker_results: List[WorkerResult],
    best_worker_sql: Optional[str],
    examples: List[Dict[str, Any]],
    output_columns: List[str],
    dag: Any,
    costs: Dict[str, Any],
    explain_plan_text: Optional[str],
    candidate_explains: Optional[Dict[int, str]],
    race_timings: Optional[Dict[str, Any]],
    engine_profile: Optional[Dict[str, Any]],
    constraints: Optional[List[Dict[str, Any]]],
    semantic_intents: Optional[Dict[str, Any]],
    regression_warnings: Optional[List[Dict[str, Any]]],
    shared_briefing: Optional[Any],
    dialect: str,
    engine_version: Optional[str],
    target_speedup: float,
    previous_retry_result: Optional[WorkerResult] = None,
    resource_envelope: Optional[str] = None,  # deprecated, ignored
) -> str:
    """Build self-directed retry worker prompt — diagnose + rewrite in one pass.

    Replaces the two-call analyst+sniper flow. The worker gets all raw evidence
    (previous results, EXPLAIN plans, race timings) plus standard worker context
    (engine profile, constraints, examples, semantic contract) and self-directs
    through diagnose → identify → rewrite.

    Args:
        original_sql: The original SQL query
        worker_results: ALL results from previous iterations
        best_worker_sql: Full optimized SQL of the best result (if any > 1.0x)
        examples: Loaded gold examples (full before/after SQL)
        output_columns: Expected output columns for completeness contract
        dag: Parsed logical tree
        costs: Per-node cost analysis
        explain_plan_text: EXPLAIN ANALYZE plan text for ORIGINAL query
        candidate_explains: EXPLAIN ANALYZE plans for each candidate (worker_id → text)
        race_timings: Raw race timing data
        engine_profile: Engine profile JSON with optimizer strengths/gaps
        constraints: Correctness constraints (4 gates)
        semantic_intents: Pre-computed per-query semantic intents
        regression_warnings: Regression examples
        shared_briefing: BriefingShared from fan-out analyst (semantic contract, bottleneck, etc.)
        dialect: SQL dialect
        engine_version: Engine version string
        target_speedup: Target speedup ratio
        previous_retry_result: Previous retry worker result (for iteration 3+)
        resource_envelope: Deprecated, ignored. Config tuning moved to config_boost phase.

    Returns:
        Complete self-directed retry worker prompt string
    """
    from .worker import _section_examples, _section_output_format
    from .analyst_briefing import (
        _format_regression_for_analyst,
        _strip_template_comments,
    )
    from .briefing_checks import build_sniper_rewrite_checklist

    target = _normalize_speedup(target_speedup)
    sections: list[str] = []

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
        f"constrained to any specific logical tree topology or CTE structure. "
        f"Your job: diagnose WHY previous workers failed to reach {target}x, "
        f"identify unexplored optimization angles, and produce an optimized SQL "
        f"rewrite that reaches the target.\n\n"
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

    # ── 3. Previous retry result (iteration 3+) ─────────────────────────
    if previous_retry_result is not None:
        retry_lines = [
            "## PREVIOUS RETRY ATTEMPT — Learn from this",
            "",
        ]
        prev_speedup = _normalize_speedup(previous_retry_result.speedup)
        retry_lines.append(
            f"Your previous retry achieved **{prev_speedup}x** "
            f"against a target of **{target}x**."
        )
        if previous_retry_result.error_message:
            retry_lines.append(f"**Error**: {previous_retry_result.error_message}")
        if previous_retry_result.strategy:
            retry_lines.append(f"**Strategy**: {previous_retry_result.strategy}")
        retry_lines.append("")
        retry_lines.append(
            "Diagnose why this approach fell short and try a fundamentally "
            "different angle."
        )
        sections.append("\n".join(retry_lines))

    # ── 4. Previous Attempts (PRIMACY) — full SQL + EXPLAIN + race ──────
    sections.append(_build_worker_results_section(
        worker_results, target_speedup, full_sql=True,
        candidate_explains=candidate_explains,
        race_timings=race_timings,
    ))

    # ── 5. Best foundation SQL ──────────────────────────────────────────
    if best_worker_sql:
        sections.append(
            "## Best Foundation SQL\n\n"
            "The best previous result. You may build on this or start fresh.\n\n"
            "```sql\n"
            + best_worker_sql.strip() + "\n"
            "```"
        )

    # ── 6. Original Execution Plan ─────────────────────────────────────
    # NOTE: Candidate EXPLAINs are already co-located with each worker's SQL
    # in section 4 via _build_worker_results_section(). Only the original
    # plan needs a dedicated section so the worker can compare.
    if explain_plan_text:
        explain_lines = ["## Original Execution Plan (EXPLAIN ANALYZE)", ""]
        explain_lines.append("Compare each candidate's plan (above) against this baseline.")
        explain_lines.append("")
        explain_lines.append("```")
        orig_lines = explain_plan_text.split("\n")
        if len(orig_lines) > 80:
            explain_lines.extend(orig_lines[:80])
            explain_lines.append(f"... ({len(orig_lines) - 80} more lines truncated)")
        else:
            explain_lines.extend(orig_lines)
        explain_lines.append("```")
        sections.append("\n".join(explain_lines))

    # ── 7. Semantic contract + bottleneck (from fan-out analyst) ─────────
    if shared_briefing:
        sc = getattr(shared_briefing, "semantic_contract", "")
        if sc:
            sections.append(
                "## Semantic Contract (MUST preserve)\n\n" + sc
            )
        bd = getattr(shared_briefing, "bottleneck_diagnosis", "")
        if bd:
            sections.append(
                "## Bottleneck Diagnosis\n\n" + bd
            )

    # ── 8. Engine profile ───────────────────────────────────────────────
    if engine_profile:
        ep_lines = ["## Engine Profile"]
        briefing_note = engine_profile.get("briefing_note", "")
        if briefing_note:
            ep_lines.append("")
            ep_lines.append(f"*{briefing_note}*")

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

    # ── 9. Reference examples ───────────────────────────────────────────
    if examples:
        sections.append(_section_examples(examples))

    # ── 10. Correctness invariants (4 constraints — HARD STOPS) ─────────
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

    # ── 11. Aggregation semantics check ─────────────────────────────────
    sections.append(
        "## Aggregation Semantics Check (HARD STOP)\n\n"
        "- STDDEV_SAMP/VARIANCE are grouping-sensitive — changing group "
        "membership changes the result.\n"
        "- AVG and STDDEV are NOT duplicate-safe.\n"
        "- FILTER over a combined group != separate per-group computation.\n"
        "- Verify aggregation equivalence for ANY proposed restructuring."
    )

    # ── 12. Regression warnings ─────────────────────────────────────────
    if regression_warnings:
        rw_lines = ["## Regression Warnings"]
        rw_lines.append("")
        for reg in regression_warnings:
            rw_lines.append(_format_regression_for_analyst(reg))
            rw_lines.append("")
        sections.append("\n".join(rw_lines))

    # ── 13. Original SQL ────────────────────────────────────────────────
    clean_sql = _strip_template_comments(original_sql)
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + clean_sql + "\n"
        "```"
    )

    # ── 14. Self-directed task ──────────────────────────────────────────
    best_speedup = max((w.speedup for w in worker_results), default=0.0)
    sections.append(
        "## Your Task — Self-Directed Retry\n\n"
        "Work through these 3 steps in a `<reasoning>` block, then output "
        "your optimized SQL:\n\n"
        f"1. **DIAGNOSE**: Why did the best worker achieve "
        f"{_normalize_speedup(best_speedup)}x instead of the {target}x target? "
        f"What do the EXPLAIN plans reveal about the actual execution bottleneck?\n"
        "2. **IDENTIFY**: What optimization angles are still unexplored? "
        "What did the empirical results reveal that couldn't have been known "
        "before seeing the execution plans?\n"
        "3. **REWRITE**: Produce optimized SQL that exploits the angles you "
        "identified. You may build on the best foundation or start fresh."
    )

    # ── 16. Rewrite checklist ───────────────────────────────────────────
    sections.append(build_sniper_rewrite_checklist())

    # ── 17. Column completeness contract + output format ────────────────
    sections.append(_section_output_format(output_columns, dialect=dialect))

    return "\n\n".join(sections)


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
    candidate_explains: Optional[Dict[int, str]] = None,
    race_timings: Optional[Dict[str, Any]] = None,
) -> str:
    """DEPRECATED: Use build_retry_worker_prompt() instead.

    Build snipe analyst prompt — diagnosis-then-synthesis.

    The analyst2 sees FULL consolidated info: all worker SQL, errors,
    speedups, engine profile, EXPLAIN plans (original + candidates),
    and race timings (if available). Its job is to diagnose WHY the best
    worker won and WHY the others didn't, then synthesize a strategy
    that couldn't have been designed without those empirical results.

    Args:
        query_id: Query identifier
        original_sql: The original SQL query
        worker_results: ALL results from previous iterations
        target_speedup: Target speedup ratio
        dag: Parsed logical tree
        costs: Per-node cost analysis
        explain_plan_text: EXPLAIN ANALYZE plan text for ORIGINAL query (may be None)
        engine_profile: Engine profile JSON with optimizer strengths/gaps
        constraints: Correctness constraints (4 gates)
        matched_examples: Tag-matched examples (full metadata)
        all_available_examples: Full catalog (compact)
        semantic_intents: Pre-computed per-query semantic intents
        regression_warnings: Regression examples
        resource_envelope: PG system resource envelope text
        dialect: SQL dialect
        dialect_version: Engine version string
        candidate_explains: EXPLAIN ANALYZE plans for each candidate (worker_id → text)
        race_timings: Raw race timing data (original_ms, candidate timings, has_clear_winner)

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
    from ..prompter import _build_node_intent_map

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

    # ── 3. Previous Attempts (PRIMACY) — includes race timings + EXPLAIN plans ─
    lines.append(_build_worker_results_section(
        worker_results, target_speedup, full_sql=True,
        candidate_explains=candidate_explains,
        race_timings=race_timings,
    ))

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
        # explain_plan_text is already pre-rendered text — just detect mode
        is_estimate = "est_rows=" in explain_plan_text or "EXPLAIN only" in explain_plan_text
        if is_estimate:
            lines.append("## EXPLAIN Plan (planner estimates)")
        else:
            lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("")
        lines.append("```")
        plan_lines = explain_plan_text.split("\n")
        if len(plan_lines) > 150:
            lines.extend(plan_lines[:150])
            lines.append(f"... ({len(plan_lines) - 150} more lines truncated)")
        else:
            lines.append(explain_plan_text)
        lines.append("```")
        lines.append("")

    # ── 6. Query Structure (Logic Tree + node details) ──────────────────
    from ..logic_tree import build_logic_tree
    node_intents = _build_node_intent_map(semantic_intents)
    if semantic_intents:
        qi = semantic_intents.get("query_intent", "")
        if qi and "main_query" not in node_intents:
            node_intents["main_query"] = qi

    lines.append("## Query Structure (Logic Tree)")
    lines.append("")
    tree = build_logic_tree(original_sql, dag, costs, dialect, node_intents)
    lines.append("```")
    lines.append(tree)
    lines.append("```")
    lines.append("")

    lines.append("## Node Details")
    lines.append("")
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
    snipe_analysis: "SnipeAnalysis",
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
    candidate_explains: Optional[Dict[int, str]] = None,
    original_explain_text: Optional[str] = None,
) -> str:
    """DEPRECATED: Use build_retry_worker_prompt() instead.

    Build the sniper's prompt — high-level reasoner with full context.

    The sniper has FULL FREEDOM to design its own approach. It gets the
    analyst's diagnosis as advisory guidance, not mandatory instructions,
    plus EXPLAIN ANALYZE plans for the original and best candidate.

    Args:
        snipe_analysis: Parsed SnipeAnalysis from analyst
        original_sql: The original SQL query
        worker_results: ALL previous worker results
        best_worker_sql: Full optimized SQL of the best result (if any > 1.0x)
        examples: Loaded gold examples (full before/after SQL)
        output_columns: Expected output columns for completeness contract
        dag: Parsed logical tree
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
        candidate_explains: EXPLAIN ANALYZE plans for candidates (worker_id → text)
        original_explain_text: EXPLAIN ANALYZE plan for original query

    Returns:
        Complete sniper prompt string
    """
    from .worker import _section_examples, _section_output_format
    from .analyst_briefing import _format_regression_for_analyst
    from .briefing_checks import build_sniper_rewrite_checklist

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
        f"constrained to any specific logical tree topology or CTE structure. "
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

    # ── 4b. Execution Plans (original vs best candidate) ─────────────
    explain_parts: list[str] = []
    if original_explain_text:
        explain_parts.append("### Original Query Execution Plan")
        explain_parts.append("")
        explain_parts.append("```")
        orig_lines = original_explain_text.split("\n")
        if len(orig_lines) > 80:
            explain_parts.extend(orig_lines[:80])
            explain_parts.append(f"... ({len(orig_lines) - 80} more lines truncated)")
        else:
            explain_parts.extend(orig_lines)
        explain_parts.append("```")
        explain_parts.append("")

    if candidate_explains and best_worker_sql:
        # Find the best worker's explain
        best_passing = [w for w in worker_results if w.speedup > 1.0]
        if best_passing:
            best_wid = max(best_passing, key=lambda w: w.speedup).worker_id
            best_explain = candidate_explains.get(best_wid)
            if best_explain:
                explain_parts.append(f"### Best Candidate Execution Plan (W{best_wid})")
                explain_parts.append("")
                explain_parts.append("Compare with original above — look for operator changes, "
                                     "new bottlenecks, or missed pushdown opportunities.")
                explain_parts.append("")
                explain_parts.append("```")
                cand_lines = best_explain.split("\n")
                if len(cand_lines) > 80:
                    explain_parts.extend(cand_lines[:80])
                    explain_parts.append(f"... ({len(cand_lines) - 80} more lines truncated)")
                else:
                    explain_parts.extend(cand_lines)
                explain_parts.append("```")
                explain_parts.append("")

    if explain_parts:
        sections.append("## Execution Plans (EXPLAIN ANALYZE)\n\n" + "\n".join(explain_parts))

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

    # ── 15. Original SQL (clean, no line numbers for rewriter) ──────────
    from .analyst_briefing import _strip_template_comments
    clean_sql = _strip_template_comments(original_sql)
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + clean_sql + "\n"
        "```"
    )

    # ── 16. Rewrite checklist (sniper-specific — no TARGET_LOGICAL_TREE ref) ────
    sections.append(build_sniper_rewrite_checklist())

    # ── 18. Column completeness contract + output format ────────────────
    sections.append(_section_output_format(output_columns, dialect=dialect))

    return "\n\n".join(sections)
