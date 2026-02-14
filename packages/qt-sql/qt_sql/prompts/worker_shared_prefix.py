"""Shared-prefix worker prompt builder for prompt caching.

All 4 workers share an identical prefix (analyst output + shared context),
diverging only at the assignment line. This enables automatic prompt caching
on DeepSeek (auto-detected repeated prefixes).

Usage:
    prefix = build_shared_worker_prefix(analyst_response, shared, workers, ...)
    for wid in [1, 2, 3, 4]:
        full_prompt = prefix + "\n\n" + build_worker_assignment(wid)
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def build_shared_worker_prefix(
    analyst_response: str,
    shared_briefing: Any,  # BriefingShared
    all_worker_briefings: List[Any],  # List[BriefingWorker]
    all_examples: Dict[int, List[Dict]],  # worker_id -> loaded examples
    original_sql: str,
    output_columns: List[str],
    dialect: str = "duckdb",
    engine_version: Optional[str] = None,
    original_logic_tree: Optional[str] = None,
    patch: bool = False,
    ir_node_map: Optional[str] = None,
) -> str:
    """Build the shared prefix that all 4 workers receive identically.

    Sections in order (attention-optimized):
    1. Role + dialect hints
    2. Semantic contract (from BriefingShared — rendered once)
    3. Active constraints (from BriefingShared — rendered once)
    4. Regression warnings (from BriefingShared — rendered once)
    5. Original SQL
    6. Analyst worker briefings (per-worker sections only, shared sections stripped)
    7. Reference examples (deduplicated union across all workers)
    8. Rewrite checklist
    9. Output format (DAP) + column completeness + original query structure
    10. Worker task summaries (compact index)

    Args:
        analyst_response: Full analyst output (contains shared analysis + all 4 task descriptions)
        shared_briefing: BriefingShared dataclass
        all_worker_briefings: All 4 BriefingWorker dataclasses
        all_examples: Dict of worker_id -> loaded examples for that worker
        original_sql: The original SQL query
        output_columns: Expected output columns
        dialect: SQL dialect
        engine_version: Engine version string
        original_logic_tree: Pre-built Logic Tree text

    Returns:
        Shared prefix string (identical for all 4 workers)
    """
    from .worker import _section_examples, _section_output_format, _section_output_format_patch
    from .briefing_checks import build_worker_rewrite_checklist

    sections: list[str] = []

    # ── [1] Role + dialect ──────────────────────────────────────────────
    engine_names = {
        "duckdb": "DuckDB",
        "postgres": "PostgreSQL",
        "postgresql": "PostgreSQL",
        "snowflake": "Snowflake",
    }
    engine = engine_names.get(dialect, dialect)
    ver = f" v{engine_version}" if engine_version else ""

    sections.append(
        f"You are a SQL rewrite engine for {engine}{ver}. "
        f"Follow the Target Logical Tree assigned to you. Write correct, "
        f"executable SQL for each node — do not decide whether to restructure. "
        f"Preserve exact semantic equivalence (same rows, columns, ordering). "
        f"Preserve defensive guards (e.g. CASE WHEN x > 0 THEN y/x END). "
        f"Strip benchmark comments (-- start/end query) from output."
    )

    if dialect == "duckdb":
        sections.append(
            "DuckDB: columnar storage (SELECT only needed columns). "
            "CTEs referenced once are inlined; multi-ref CTEs may materialize. "
            "FILTER clause is native. Predicate pushdown stops at "
            "UNION ALL boundaries and multi-level CTE refs."
        )

    # ── [2] Semantic contract (rendered once — NOT duplicated from analyst) ─
    if shared_briefing.semantic_contract:
        sections.append(
            "## Semantic Contract (MUST preserve)\n\n"
            + shared_briefing.semantic_contract
        )

    # ── [3] Active constraints ──────────────────────────────────────────
    if shared_briefing.active_constraints:
        sections.append(
            "## Constraints\n\n"
            + shared_briefing.active_constraints
        )

    # ── [4] Regression warnings ─────────────────────────────────────────
    if shared_briefing.regression_warnings:
        sections.append(
            "## Regression Warnings\n\n"
            + shared_briefing.regression_warnings
        )

    # ── [5] Original SQL ────────────────────────────────────────────────
    sections.append(
        "## Original SQL\n\n"
        "```sql\n"
        + original_sql + "\n"
        "```"
    )

    # ── [6] Analyst worker briefings (strip shared sections to avoid duplication)
    if analyst_response:
        worker_sections = _strip_shared_sections(analyst_response)
        if worker_sections.strip():
            sections.append(
                "## Worker Briefings\n\n"
                "The analyst has designed 4 strategies. You will be assigned ONE.\n\n"
                + worker_sections
            )

    # ── [7] Reference examples (deduplicated union across all workers) ──
    merged = _deduplicate_examples(all_examples)
    if merged:
        sections.append(_section_examples(merged))

    # ── [8] Rewrite checklist ───────────────────────────────────────────
    sections.append(build_worker_rewrite_checklist())

    # ── [9] Output format + column completeness + query structure ───────
    if patch and ir_node_map:
        sections.append(_section_output_format_patch(output_columns, ir_node_map, dialect=dialect))
    else:
        sections.append(_section_output_format(output_columns, original_logic_tree, dialect=dialect))

    # ── [10] Worker task summaries (compact index) ──────────────────────
    task_lines = ["## Worker Task Assignments", ""]
    for wb in sorted(all_worker_briefings, key=lambda w: w.worker_id):
        tree_summary = ""
        if wb.target_logical_tree:
            first_line = wb.target_logical_tree.strip().split("\n")[0]
            tree_summary = f" — {first_line}"
        task_lines.append(f"**TASK {wb.worker_id}**: [{wb.strategy}]{tree_summary}")
        if wb.hazard_flags:
            hazard_first = wb.hazard_flags.strip().split("\n")[0][:120]
            task_lines.append(f"  Hazard: {hazard_first}")
        task_lines.append("")
    sections.append("\n".join(task_lines))

    return "\n\n".join(sections)


def build_worker_assignment(worker_id: int) -> str:
    """Build the per-worker assignment suffix.

    This is the ONLY part that differs between workers. It must be short
    so the shared prefix dominates token count for cache efficiency.

    Args:
        worker_id: Worker ID (1-4)

    Returns:
        Assignment suffix string
    """
    return (
        f"YOU HAVE BEEN ASSIGNED TASK {worker_id}. "
        f"Execute ONLY your task (Task {worker_id}) from the Worker Briefings above. "
        f"Follow the Target Logical Tree and Node Contracts for Worker {worker_id}. "
        f"Output your rewrite now."
    )


def build_coach_worker_assignment(worker_id: int) -> str:
    """Build the assignment suffix for coach-refined workers.

    Unlike the fan-out assignment, this tells the worker to apply the
    Coach Refinement Directive which may override the original Target
    Logical Tree. The directive is authoritative for this round.

    Args:
        worker_id: Worker ID (1-4)

    Returns:
        Assignment suffix string
    """
    return (
        f"YOU ARE WORKER {worker_id} IN A REFINEMENT ROUND.\n\n"
        f"The Coach Refinement Directive for Worker {worker_id} above is your "
        f"PRIMARY instruction. It tells you what to KEEP, what to CHANGE, and "
        f"what to INCORPORATE from other workers.\n\n"
        f"Where the directive conflicts with the original Target Logical Tree "
        f"from the Worker Briefings, the directive wins — the coach has seen "
        f"the actual execution results and knows what needs to change.\n\n"
        f"Output your rewrite now."
    )


def _strip_shared_sections(analyst_response: str) -> str:
    """Strip the SHARED BRIEFING section from analyst response.

    The shared sections (SEMANTIC_CONTRACT, BOTTLENECK_DIAGNOSIS, ACTIVE_CONSTRAINTS,
    REGRESSION_WARNINGS) are already rendered as dedicated sections in the prefix.
    Including them again in the analyst response wastes ~300 tokens.

    Keeps only the per-worker sections (WORKER 1..4 BRIEFING).
    The analyst format uses === WORKER N BRIEFING === headers (not ## WORKER).
    """
    # Try all known formats for WORKER 1 header (most specific first)
    patterns = [
        # === WORKER 1 BRIEFING === (standard analyst format)
        r'^={3}\s*WORKER\s+1\s+BRIEFING.*={3}',
        # ### WORKER 1 BRIEFING (markdown triple-hash — some models)
        r'^#{2,3}\s*WORKER\s+1\s+BRIEFING',
        # ## WORKER 1 (any markdown heading)
        r'^#{2,3}\s*WORKER\s+1\s',
    ]
    for pat in patterns:
        worker_start = re.search(pat, analyst_response, re.MULTILINE | re.IGNORECASE)
        if worker_start:
            return analyst_response[worker_start.start():]

    # Fallback: match ANY worker number (not just 1) with the same formats
    fallback_patterns = [
        r'^={3}\s*WORKER\s+\d+\s+BRIEFING.*={3}',
        r'^#{2,3}\s*WORKER\s+\d+\s+BRIEFING',
        r'^#{2,3}\s*WORKER\s+\d',
    ]
    for pat in fallback_patterns:
        worker_start = re.search(pat, analyst_response, re.MULTILINE | re.IGNORECASE)
        if worker_start:
            return analyst_response[worker_start.start():]

    # Can't identify sections — return as-is (safe fallback)
    return analyst_response


def _deduplicate_examples(
    all_examples: Dict[int, List[Dict]],
) -> List[Dict]:
    """Merge and deduplicate examples across all workers.

    Preserves order: worker 1 examples first, then 2, 3, 4 — skipping duplicates.
    """
    seen_ids: set = set()
    merged: list = []
    for wid in sorted(all_examples.keys()):
        for ex in all_examples[wid]:
            ex_id = ex.get("id") or ex.get("name") or id(ex)
            if ex_id not in seen_ids:
                seen_ids.add(ex_id)
                merged.append(ex)
    return merged
