"""Config Coach prompt builder — cache-stable prefix + iteration delta.

Sections 1-6 form the stable prefix (cached across iterations).
Section 7 grows per iteration with results and reflection.

The coach proposes 4-8 candidate bundles per iteration. Each candidate:
  {id, hypothesis, predicted_speedup, set_local: {}, hints: "", reasoning}
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..hint_plan import (
    PG_HINT_CATALOG,
    HintDirective,
    format_hint_catalog_for_prompt,
    parse_hint_string,
    validate_hint_directive,
)
from ..pg_tuning import PG_TUNABLE_PARAMS, validate_tuning_config

logger = logging.getLogger(__name__)


# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class CoachCandidate:
    """A single config+hint candidate proposed by the coach."""
    id: str                          # e.g. "C1", "C2"
    hypothesis: str                  # What the coach thinks will happen
    predicted_speedup: str           # e.g. "1.5-2x"
    set_local: Dict[str, str]        # param -> value (validated)
    hints: str                       # raw hint string (validated)
    reasoning: str                   # Why this combination

    @property
    def hint_directives(self) -> List[HintDirective]:
        return parse_hint_string(self.hints) if self.hints else []


@dataclass
class CandidateResult:
    """Benchmark result for a single candidate."""
    candidate: CoachCandidate
    elapsed_ms: float = 0.0          # Average of 2 measurement runs
    speedup: float = 0.0             # baseline_ms / elapsed_ms
    row_count: int = 0
    rows_match: bool = True
    explain_text: str = ""           # EXPLAIN ANALYZE (if collected)
    error: str = ""                  # Error message if execution failed


@dataclass
class IterationResult:
    """Results from one iteration of the coach loop."""
    iteration: int
    candidate_results: List[CandidateResult] = field(default_factory=list)
    best_candidate_id: str = ""
    best_speedup: float = 0.0


# ── Prompt builder ────────────────────────────────────────────────────────

def build_base_prefix(
    query_sql: str,
    explain_plan: str,
    engine_profile: Dict[str, Any],
    resource_envelope: str,
    current_settings: Dict[str, str],
    baseline_ms: float,
    hint_plan_available: bool = True,
) -> str:
    """Build the cache-stable prefix (sections 1-6).

    This prefix is identical across all iterations, enabling DeepSeek's
    auto-prefix caching to avoid re-processing ~80% of tokens.
    """
    parts: List[str] = []

    # §1 — Role
    parts.append(_section_role())

    # §2 — SQL + baseline timing
    parts.append(_section_sql(query_sql, baseline_ms))

    # §3 — EXPLAIN ANALYZE
    parts.append(_section_explain(explain_plan))

    # §4 — Engine profile (compact)
    parts.append(_section_engine_profile(engine_profile))

    # §5 — Resource envelope + tunable params catalog
    parts.append(_section_resource_envelope(resource_envelope, current_settings))

    # §6 — Hint catalog (if available)
    if hint_plan_available:
        parts.append(_section_hint_catalog())
    else:
        parts.append(
            "## [6] HINT CATALOG\n\n"
            "pg_hint_plan is NOT installed. Do NOT propose any hints.\n"
            "Use only SET LOCAL parameters.\n"
        )

    return "\n\n".join(parts)


def build_iteration_prompt(
    base_prefix: str,
    previous_iterations: List[IterationResult],
    iteration_num: int,
    max_candidates: int = 8,
    min_candidates: int = 4,
) -> str:
    """Build the full prompt for a specific iteration.

    Iteration 1: prefix + instructions (no results section).
    Iteration 2-3: prefix + previous results with reflection + instructions.
    """
    parts = [base_prefix]

    # §7 — Previous results + instructions
    if previous_iterations:
        parts.append(_section_previous_results(previous_iterations))

    parts.append(_section_instructions(
        iteration_num, max_candidates, min_candidates,
        has_previous=bool(previous_iterations),
    ))

    return "\n\n".join(parts)


def parse_coach_response(response_text: str) -> List[CoachCandidate]:
    """Parse coach LLM response into validated CoachCandidate list.

    Extracts JSON array from ```json blocks or raw JSON.
    Validates set_local params against PG_TUNABLE_PARAMS whitelist
    and hints against PG_HINT_CATALOG.
    """
    # Try to extract JSON block
    json_text = _extract_json_block(response_text)
    if not json_text:
        logger.warning("No JSON block found in coach response")
        return []

    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse coach JSON: {e}")
        return []

    if not isinstance(data, list):
        # Try wrapping single object
        if isinstance(data, dict):
            data = [data]
        else:
            logger.warning(f"Expected JSON array, got {type(data)}")
            return []

    candidates: List[CoachCandidate] = []
    seen_ids: set = set()
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            continue

        cid = str(item.get("id", f"C{i + 1}"))
        # Ensure unique IDs — append suffix on collision
        if cid in seen_ids:
            cid = f"{cid}_{i + 1}"
        seen_ids.add(cid)
        hypothesis = item.get("hypothesis", "")
        predicted = item.get("predicted_speedup", "?")
        reasoning = item.get("reasoning", "")

        # Validate SET LOCAL params
        raw_set_local = item.get("set_local", {})
        if not isinstance(raw_set_local, dict):
            raw_set_local = {}
        validated_config = validate_tuning_config(raw_set_local)

        # Validate hints
        raw_hints = item.get("hints", "")
        if not isinstance(raw_hints, str):
            raw_hints = ""
        hint_directives = parse_hint_string(raw_hints)
        validated_hints = " ".join(d.render() for d in hint_directives)

        # Skip if completely empty (no config and no hints)
        if not validated_config and not validated_hints:
            logger.debug(f"Skipping candidate {cid}: no valid config or hints")
            continue

        candidates.append(CoachCandidate(
            id=str(cid),
            hypothesis=str(hypothesis),
            predicted_speedup=str(predicted),
            set_local=validated_config,
            hints=validated_hints,
            reasoning=str(reasoning),
        ))

    return candidates


# ── Private section builders ──────────────────────────────────────────────

def _section_role() -> str:
    return (
        "## [1] ROLE\n\n"
        "You are a PostgreSQL performance tuning coach. Your job is to propose "
        "SET LOCAL configuration changes and pg_hint_plan directives that make "
        "the given query run faster.\n\n"
        "RULES:\n"
        "- You do NOT rewrite SQL. The query text is FIXED.\n"
        "- You propose config bundles: SET LOCAL parameters + pg_hint_plan hints.\n"
        "- Each candidate must have a clear HYPOTHESIS about why it will help.\n"
        "- Predict a speedup range. You will see actual results and reflect.\n"
        "- EXPLAIN ANALYZE cost gaps do NOT predict runtime gains. 6 false "
        "positives caught in our benchmarks (38-84% EXPLAIN gap -> 0% or "
        "regression). Reason about I/O patterns, cache behavior, and "
        "parallelism — not just plan cost.\n"
        "- Combo patterns often beat single changes. Combine hints + config."
    )


def _section_sql(query_sql: str, baseline_ms: float) -> str:
    # Truncate very long SQL
    sql_display = query_sql.strip()
    if len(sql_display) > 6000:
        sql_display = sql_display[:6000] + "\n-- [truncated]"

    return (
        f"## [2] QUERY + BASELINE\n\n"
        f"Baseline timing: {baseline_ms:.1f} ms (average of 2 measurement runs)\n\n"
        f"```sql\n{sql_display}\n```"
    )


def _section_explain(explain_plan: str) -> str:
    # Cap at 200 lines
    lines = explain_plan.strip().split("\n")
    if len(lines) > 200:
        lines = lines[:200]
        lines.append("-- [truncated at 200 lines]")

    return (
        "## [3] EXPLAIN ANALYZE\n\n"
        "```\n" + "\n".join(lines) + "\n```"
    )


def _load_config_playbook() -> Optional[str]:
    """Load the config tuning playbook from knowledge/postgres_config.md."""
    playbook_path = (
        Path(__file__).resolve().parent.parent / "knowledge" / "postgres_config.md"
    )
    if playbook_path.exists():
        text = playbook_path.read_text(errors="replace").strip()
        if text:
            return text
    return None


def _section_engine_profile(profile: Dict[str, Any]) -> str:
    # Prefer the structured markdown playbook over flat JSON
    playbook = _load_config_playbook()
    if playbook:
        return f"## [4] CONFIG TUNING PLAYBOOK\n\n{playbook}"

    # Fallback to flat JSON rendering from engine profile
    intel = profile.get("set_local_config_intel", {})
    if not intel:
        return "## [4] ENGINE PROFILE\n\nNo SET LOCAL intel available."

    lines = ["## [4] ENGINE PROFILE — SET LOCAL Intel\n"]

    note = intel.get("briefing_note", "")
    if note:
        lines.append(note)
        lines.append("")

    rules = intel.get("rules", [])
    for rule in rules:
        rid = rule.get("id", "?")
        trigger = rule.get("trigger", "")
        config = rule.get("config", "")
        evidence = rule.get("evidence", "")
        risk = rule.get("risk", "")
        lines.append(f"### {rid}")
        lines.append(f"- Trigger: {trigger}")
        lines.append(f"- Config: {config}")
        lines.append(f"- Evidence: {evidence}")
        lines.append(f"- Risk: {risk}")
        lines.append("")

    combos = intel.get("combo_patterns", [])
    if combos:
        lines.append("### Proven Combo Patterns")
        for c in combos:
            lines.append(f"- {c}")
        lines.append("")

    findings = intel.get("key_findings", [])
    if findings:
        lines.append("### Key Findings")
        for f in findings:
            lines.append(f"- {f}")

    return "\n".join(lines)


def _section_resource_envelope(
    envelope: str,
    current_settings: Dict[str, str],
) -> str:
    lines = ["## [5] RESOURCE ENVELOPE + TUNABLE PARAMS\n"]

    lines.append("### Current System State")
    lines.append(envelope)
    lines.append("")

    lines.append("### Current Settings")
    for name, value in sorted(current_settings.items()):
        lines.append(f"  {name} = {value}")
    lines.append("")

    lines.append("### Tunable Parameters (SET LOCAL whitelist)")
    for param, (ptype, pmin, pmax, desc) in sorted(PG_TUNABLE_PARAMS.items()):
        range_str = ""
        if pmin is not None and pmax is not None:
            range_str = f" [{pmin}-{pmax}]"
        lines.append(f"  {param} ({ptype}{range_str}): {desc}")

    return "\n".join(lines)


def _section_hint_catalog() -> str:
    return "## [6] HINT CATALOG\n\n" + format_hint_catalog_for_prompt()


def _section_previous_results(iterations: List[IterationResult]) -> str:
    lines = ["## [7a] PREVIOUS RESULTS\n"]

    for iteration in iterations:
        lines.append(f"### Iteration {iteration.iteration}")

        if not iteration.candidate_results:
            lines.append("No candidates tested.\n")
            continue

        # Summary table
        lines.append(
            "| ID | Speedup | Status | Hypothesis |"
        )
        lines.append(
            "|---|---|---|---|"
        )

        for cr in iteration.candidate_results:
            c = cr.candidate
            if cr.error:
                status = "ERROR"
                speedup_str = "N/A"
            elif not cr.rows_match:
                status = "ROW_MISMATCH"
                speedup_str = "N/A"
            elif cr.speedup >= 1.05:
                status = "WIN"
                speedup_str = f"{cr.speedup:.2f}x"
            elif cr.speedup >= 0.95:
                status = "NEUTRAL"
                speedup_str = f"{cr.speedup:.2f}x"
            else:
                status = "REGRESSION"
                speedup_str = f"{cr.speedup:.2f}x"

            lines.append(
                f"| {c.id} | {speedup_str} | {status} | {c.hypothesis} |"
            )

        lines.append("")

        # Per-candidate post-mortem
        for cr in iteration.candidate_results:
            lines.append(_format_candidate_postmortem(cr))

        lines.append("")

    return "\n".join(lines)


def _format_candidate_postmortem(cr: CandidateResult) -> str:
    """Format a single candidate result as a post-mortem reflection block."""
    c = cr.candidate
    lines = [f"### {c.id} Post-Mortem"]

    lines.append(f'Hypothesis: "{c.hypothesis}"')
    lines.append(f"Predicted: {c.predicted_speedup} | Actual: ")

    if cr.error:
        lines[-1] = f"Predicted: {c.predicted_speedup} | Actual: ERROR"
        lines.append(f"Error: {cr.error}")
        lines.append("VERDICT: ERROR — fix or abandon this approach.")
    elif not cr.rows_match:
        lines[-1] = f"Predicted: {c.predicted_speedup} | Actual: ROW_MISMATCH"
        lines.append("VERDICT: INVALID — row count changed. Config should not affect row counts.")
    elif cr.speedup >= 1.05:
        lines[-1] = f"Predicted: {c.predicted_speedup} | Actual: {cr.speedup:.2f}x WIN"
        lines.append("VERDICT: CORRECT — build on this approach.")
    elif cr.speedup >= 0.95:
        lines[-1] = f"Predicted: {c.predicted_speedup} | Actual: {cr.speedup:.2f}x NEUTRAL"
        lines.append("VERDICT: WRONG — this had no measurable effect. Do NOT retry as-is.")
    else:
        lines[-1] = f"Predicted: {c.predicted_speedup} | Actual: {cr.speedup:.2f}x REGRESSION"
        lines.append("VERDICT: WRONG — this made things worse. Do NOT retry this approach.")

    # Config summary
    config_parts = []
    if c.set_local:
        config_parts.append("SET LOCAL: " + ", ".join(
            f"{k}={v}" for k, v in sorted(c.set_local.items())
        ))
    if c.hints:
        config_parts.append(f"Hints: {c.hints}")
    if config_parts:
        lines.append("Config: " + " | ".join(config_parts))

    # EXPLAIN excerpt if available
    if cr.explain_text:
        explain_lines = cr.explain_text.strip().split("\n")
        if len(explain_lines) > 40:
            explain_lines = explain_lines[:40]
            explain_lines.append("-- [truncated]")
        lines.append("EXPLAIN excerpt:")
        lines.append("```")
        lines.extend(explain_lines)
        lines.append("```")

    return "\n".join(lines)


def _section_instructions(
    iteration_num: int,
    max_candidates: int,
    min_candidates: int,
    has_previous: bool,
) -> str:
    lines = [f"## [7b] INSTRUCTIONS — Iteration {iteration_num}\n"]

    if has_previous:
        lines.append(
            "Review the previous results above. Learn from what worked and "
            "what didn't. Do NOT repeat failed approaches.\n"
        )
        if iteration_num == 2:
            lines.append(
                "Strategy: Refine winning approaches (if any). Try different "
                "combinations. Address any regressions by understanding WHY "
                "they happened.\n"
            )
        elif iteration_num >= 3:
            lines.append(
                "Final iteration. Focus on your best remaining hypotheses. "
                "Try novel combinations you haven't tested yet. Be bold but "
                "principled.\n"
            )
    else:
        lines.append(
            "This is the first iteration. Analyze the EXPLAIN plan carefully. "
            "Identify bottleneck operators and propose targeted config changes.\n"
        )

    lines.append(
        f"Propose {min_candidates}-{max_candidates} candidates. Each must have:\n"
        "- A unique ID (C1, C2, ...)\n"
        "- A specific HYPOTHESIS about what will change in the plan\n"
        "- A predicted speedup range\n"
        "- SET LOCAL config and/or pg_hint_plan hints\n"
        "- Reasoning\n"
    )

    lines.append(
        "Output ONLY a JSON array. No other text before or after.\n\n"
        "```json\n"
        "[\n"
        "  {\n"
        '    "id": "C1",\n'
        '    "hypothesis": "Force hash join on ss-cd to avoid 30K NL loops",\n'
        '    "predicted_speedup": "1.5-2x",\n'
        '    "set_local": {"work_mem": "256MB", "enable_nestloop": "off"},\n'
        '    "hints": "HashJoin(ss cd)",\n'
        '    "reasoning": "EXPLAIN shows NL with 30K loops on cd. Hash join at 256MB work_mem will fit in memory."\n'
        "  }\n"
        "]\n"
        "```"
    )

    return "\n".join(lines)


# ── JSON extraction ───────────────────────────────────────────────────────

def _extract_json_block(text: str) -> Optional[str]:
    """Extract JSON array from LLM response text.

    Tries in order:
    1. ```json ... ``` block
    2. Raw [ ... ] array
    """
    # Try ```json block
    match = re.search(r'```json\s*\n?(.*?)```', text, re.DOTALL)
    if match:
        return match.group(1).strip()

    # Try ``` block (no language tag)
    match = re.search(r'```\s*\n?(.*?)```', text, re.DOTALL)
    if match:
        candidate = match.group(1).strip()
        if candidate.startswith("[") or candidate.startswith("{"):
            return candidate

    # Try raw JSON array
    match = re.search(r'\[\s*\{.*\}\s*\]', text, re.DOTALL)
    if match:
        return match.group(0)

    return None
