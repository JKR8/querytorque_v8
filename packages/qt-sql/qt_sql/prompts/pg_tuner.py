"""PG Tuner prompt builder — LLM-driven per-query SET LOCAL tuning.

Builds a prompt that gives the LLM:
  1. The SQL query
  2. The EXPLAIN ANALYZE plan (text format)
  3. Current PG settings
  4. The PG engine profile (strengths/gaps)
  5. The whitelist of tunable params with ranges
  6. Instructions to output JSON: {"params": {...}, "reasoning": "..."}
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from ..pg_tuning import PG_TUNABLE_PARAMS


def _format_param_catalog() -> str:
    """Format the tunable parameter catalog for the LLM."""
    lines = []
    for param, (ptype, pmin, pmax, desc) in sorted(PG_TUNABLE_PARAMS.items()):
        if ptype == "bool":
            range_str = "on | off"
        elif ptype == "bytes":
            range_str = f"{pmin}MB to {pmax}MB"
        elif ptype == "int":
            range_str = f"{pmin} to {pmax}"
        else:
            range_str = f"{pmin} to {pmax}"
        lines.append(f"- **{param}** ({ptype}, {range_str}): {desc}")
    return "\n".join(lines)


def _format_current_settings(settings: Dict[str, str]) -> str:
    """Format current PG settings for the prompt."""
    if not settings:
        return "*Current settings not available.*"
    lines = []
    for name, value in sorted(settings.items()):
        lines.append(f"  {name} = {value}")
    return "\n".join(lines)


def _format_engine_profile_compact(profile: Dict[str, Any]) -> str:
    """Format engine profile compactly for the tuner (not full analyst view)."""
    lines = []

    briefing = profile.get("briefing_note", "")
    if briefing:
        lines.append(f"*{briefing}*")
        lines.append("")

    strengths = profile.get("strengths", [])
    if strengths:
        lines.append("**Optimizer strengths (do not fight):**")
        for s in strengths:
            lines.append(f"- {s.get('id', '?')}: {s.get('summary', '')}")
        lines.append("")

    gaps = profile.get("gaps", [])
    if gaps:
        lines.append("**Optimizer gaps (configuration may help):**")
        for g in gaps:
            gid = g.get("id", "")
            what = g.get("what", "")
            lines.append(f"- {gid}: {what}")
        lines.append("")

    return "\n".join(lines)


def build_pg_tuner_prompt(
    query_sql: str,
    explain_plan: Optional[str] = None,
    current_settings: Optional[Dict[str, str]] = None,
    engine_profile: Optional[Dict[str, Any]] = None,
    baseline_ms: Optional[float] = None,
    plan_json: Optional[Any] = None,
) -> str:
    """Build the PG tuner prompt for LLM-driven SET LOCAL tuning.

    Args:
        query_sql: The SQL query being optimized.
        explain_plan: EXPLAIN ANALYZE output (text format). May be None.
        current_settings: Current PG settings from get_settings().
        engine_profile: PG engine profile JSON (optional).
        baseline_ms: Baseline execution time in ms (optional, for context).
        plan_json: PG EXPLAIN (FORMAT JSON) plan data. If provided and
            explain_plan is None, will be rendered to text automatically.

    Returns:
        Complete prompt string for the tuner LLM call.
    """
    # Render plan_json to text if no text plan provided
    if not explain_plan and plan_json:
        from .v2_analyst_briefing import format_pg_explain_tree
        explain_plan = format_pg_explain_tree(plan_json)

    if current_settings is None:
        current_settings = {}
    lines: list[str] = []

    # ── Role ──
    lines.append(
        "You are a PostgreSQL performance tuning expert. Your job is to "
        "recommend SET LOCAL configuration parameters that will improve "
        "the performance of a specific SQL query."
    )
    lines.append("")
    lines.append(
        "SET LOCAL changes settings only for the current transaction. "
        "Settings revert on COMMIT/ROLLBACK. This is production-safe — "
        "no other connections are affected."
    )
    lines.append("")

    # ── Query ──
    lines.append("## SQL Query")
    lines.append("")
    lines.append("```sql")
    lines.append(query_sql.strip())
    lines.append("```")
    lines.append("")

    if baseline_ms is not None:
        lines.append(f"**Current baseline:** {baseline_ms:.1f}ms")
        lines.append("")

    # ── EXPLAIN plan ──
    if explain_plan:
        lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("")
        lines.append("```")
        plan_lines = explain_plan.strip().split("\n")
        if len(plan_lines) > 200:
            lines.extend(plan_lines[:200])
            lines.append(f"... ({len(plan_lines) - 200} more lines truncated)")
        else:
            lines.append(explain_plan.strip())
        lines.append("```")
        lines.append("")
    else:
        lines.append("## EXPLAIN ANALYZE Plan")
        lines.append("*Not available. Recommend parameters based on query structure.*")
        lines.append("")

    # ── Current settings ──
    lines.append("## Current PostgreSQL Settings")
    lines.append("")
    lines.append(_format_current_settings(current_settings))
    lines.append("")

    # ── System constraints (derived from PG settings) ──
    max_workers = current_settings.get("max_parallel_workers")
    shared_buf = current_settings.get("shared_buffers")
    eff_cache = current_settings.get("effective_cache_size")
    max_conns = current_settings.get("max_connections")
    if max_workers or shared_buf:
        lines.append("## System Constraints")
        lines.append("")
        if max_workers:
            lines.append(
                f"- **max_parallel_workers = {max_workers}**: This is the server-wide "
                f"hard cap. Do NOT set max_parallel_workers_per_gather higher than this."
            )
        if shared_buf:
            lines.append(
                f"- **shared_buffers = {shared_buf}**: Server's dedicated RAM. "
                f"Size work_mem relative to this — total work_mem across all "
                f"concurrent operations should not exceed available RAM."
            )
        if eff_cache:
            lines.append(
                f"- **Current effective_cache_size = {eff_cache}**: "
                f"You may increase this if the system has more RAM available."
            )
        if max_conns:
            lines.append(
                f"- **max_connections = {max_conns}**: work_mem is per-operation, "
                f"and multiple connections run concurrently. Keep per-query "
                f"memory budgets conservative."
            )
        lines.append("")

    # ── Engine profile (compact) ──
    if engine_profile:
        lines.append("## Engine Profile")
        lines.append("")
        lines.append(_format_engine_profile_compact(engine_profile))

    # ── Parameter catalog ──
    lines.append("## Tunable Parameters (whitelist)")
    lines.append("")
    lines.append(
        "You may ONLY recommend parameters from this list. "
        "Any other parameters will be stripped."
    )
    lines.append("")
    lines.append(_format_param_catalog())
    lines.append("")

    # ── Analysis instructions ──
    lines.append("## Analysis Instructions")
    lines.append("")
    lines.append(
        "Analyze the EXPLAIN plan and query structure to identify bottlenecks "
        "that can be addressed via configuration changes:"
    )
    lines.append("")
    lines.append(
        "1. **Sort/Hash spills**: If you see 'Sort Method: external merge' "
        "or 'Batches: N' (N>1) on hash joins, increase work_mem. "
        "CRITICAL: work_mem is allocated PER-OPERATION, not per-query. "
        "Count the hash/sort nodes in the plan. A query with 12 hash joins "
        "at work_mem='1GB' uses 12GB total. "
        "Rule of thumb: (available_memory / num_hash_sort_ops) = max work_mem. "
        "2 ops → 1GB ok. 5+ ops → 256-512MB. 10+ ops → 128-256MB."
    )
    lines.append(
        "2. **Parallel workers not launching**: Look for 'Workers Planned: N' "
        "vs 'Workers Launched: M' where M < N (or M = 0). This means workers "
        "were planned but the planner's cost estimates prevented launch. "
        "Fix: reduce parallel_setup_cost (try 100) and parallel_tuple_cost "
        "(try 0.001) to lower the threshold for launching workers."
    )
    lines.append(
        "3. **No parallelism on large scans**: If you see sequential scans "
        "on large tables (>100K rows) with no Gather/Gather Merge above them, "
        "increase max_parallel_workers_per_gather (try 4). Check that the "
        "scan's estimated rows justify parallelism."
    )
    lines.append(
        "4. **random_page_cost — CAREFUL**: Default is 4.0 (HDD assumption). "
        "On SSD, 1.0-1.5 is appropriate. BUT: lowering this aggressively "
        "can cause severe regressions (0.5x-0.7x observed) on queries where "
        "the existing plan already uses optimal access paths. "
        "ONLY reduce random_page_cost if you see sequential scans where "
        "index scans would clearly be better (e.g., high selectivity "
        "predicates on indexed columns with Seq Scan). If the plan already "
        "uses index scans or bitmap scans effectively, do NOT touch this."
    )
    lines.append(
        "5. **JIT compilation overhead**: Look at the JIT section in the "
        "EXPLAIN output. If 'JIT:' shows Generation + Optimization + Emission "
        "time exceeding 5% of total execution time, set jit=off. "
        "Common on queries with many expressions (100+ functions compiled). "
        "Example: 820ms JIT on a 56s query = 1.5% → borderline, leave on. "
        "But 820ms JIT on a 5s query = 16% → turn off."
    )
    lines.append(
        "6. **Join strategy**: If nested-loop joins dominate on large tables "
        "and hash/merge would be better, the cost model may be wrong. "
        "Check if hashjoin/mergejoin are disabled. Do NOT disable join "
        "methods (enable_nestloop=off) unless the plan clearly shows "
        "a catastrophic nested loop (e.g., 30K+ loops on a large table)."
    )
    lines.append(
        "7. **effective_cache_size**: Advisory only — tells the planner "
        "how much OS cache to expect. Safe to set aggressively (75% of "
        "total RAM). Encourages index scan preference. Low-risk change."
    )
    lines.append(
        "8. **hash_mem_multiplier**: If hash joins spill to multiple "
        "batches but sort operations are fine, increasing this (try 4-8) "
        "gives hash operations more memory without inflating sort budgets."
    )
    lines.append("")
    lines.append(
        "## CRITICAL RULES"
    )
    lines.append("")
    lines.append(
        "- **Evidence-based only**: Every parameter you recommend MUST cite "
        "a specific line or node from the EXPLAIN plan that justifies it. "
        'Example: "Sort Method: external merge Disk: 39MB → work_mem=512MB".'
    )
    lines.append(
        "- **Empty is valid**: If the plan shows no clear bottleneck that "
        "configuration can fix (e.g., the query is CPU-bound on computation, "
        "or I/O-bound with optimal access paths), return empty params. "
        "Speculative tuning causes regressions."
    )
    lines.append(
        "- **Do NOT blindly reduce random_page_cost**: This is the #1 cause "
        "of regressions in our benchmarks. Only change it with clear evidence."
    )
    lines.append(
        "- **Count before sizing work_mem**: Always count hash/sort nodes "
        "in the plan before recommending a work_mem value."
    )
    lines.append("")

    # ── Output format ──
    lines.append("## Output Format")
    lines.append("")
    lines.append(
        "Respond with ONLY a JSON object. No markdown, no explanation outside "
        "the JSON. Use this exact format:"
    )
    lines.append("")
    lines.append("```json")
    lines.append("{")
    lines.append('  "params": {')
    lines.append('    "work_mem": "512MB",')
    lines.append('    "max_parallel_workers_per_gather": "4"')
    lines.append("  },")
    lines.append('  "reasoning": "The EXPLAIN shows 3 hash joins spilling to disk '
                  '(Sort Method: external merge). Increasing work_mem to 512MB '
                  'keeps them in-memory. Enabling 4 parallel workers for the '
                  'large sequential scan on store_sales (2.1M rows)."')
    lines.append("}")
    lines.append("```")
    lines.append("")
    lines.append(
        "If no configuration changes would help, return: "
        '{"params": {}, "reasoning": "No configuration bottlenecks identified."}'
    )

    return "\n".join(lines)
