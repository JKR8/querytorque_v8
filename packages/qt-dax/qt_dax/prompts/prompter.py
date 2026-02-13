"""DAX prompt builder — attention-optimized sections.

Follows qt_sql/prompter.py structure: one class, section methods,
build_prompt() assembles them.  Primacy/recency ordering ensures
the LLM sees critical context at both ends of the context window.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Optional

from qt_dax.knowledge import load_playbook, match_examples


@dataclass
class PromptInputs:
    """All data needed to build a single-measure optimisation prompt."""

    measure_name: str
    measure_table: str
    measure_dax: str

    # Dependency closure — list of {name, table, expression}
    dependency_chain: list[dict]

    # Model schema — tables, columns, relationships (from TMDL or VPAX parse)
    model_schema: Optional[dict] = None

    # DAXAnalyzer issues — list of VPAXIssue-like dicts/objects
    detected_issues: list = None

    # Retry context
    previous_attempt: Optional[str] = None
    previous_error: Optional[str] = None

    def __post_init__(self) -> None:
        if self.detected_issues is None:
            self.detected_issues = []


class DAXPrompter:
    """Build attention-optimized prompts for DAX measure optimisation."""

    def build_prompt(self, inputs: PromptInputs) -> str:
        """Assemble all sections into a single prompt string."""
        sections = [
            self._section_role_task(),
            self._section_target_measure(inputs),
            self._section_dependency_closure(inputs),
            self._section_model_schema(inputs),
            self._section_detected_issues(inputs),
            self._section_playbook(),
            self._section_examples(inputs),
            self._section_retry_context(inputs),
            self._section_constraints(),
            self._section_output_format(),
        ]
        return "\n\n".join(s for s in sections if s)

    # ------------------------------------------------------------------
    # PRIMACY — first things the model reads
    # ------------------------------------------------------------------

    @staticmethod
    def _section_role_task() -> str:
        return (
            "# ROLE\n"
            "You are a DAX Storage-Engine / Formula-Engine performance engineer.\n"
            "Your goal: rewrite a single DAX measure so it executes faster while\n"
            "returning **exactly** the same numeric results in every filter context."
        )

    @staticmethod
    def _section_target_measure(inputs: PromptInputs) -> str:
        return (
            "# TARGET MEASURE\n"
            f"**Name:** {inputs.measure_table}[{inputs.measure_name}]\n\n"
            "```dax\n"
            f"{inputs.measure_dax}\n"
            "```"
        )

    @staticmethod
    def _section_dependency_closure(inputs: PromptInputs) -> str:
        if not inputs.dependency_chain:
            return ""
        lines = ["# DEPENDENCY CLOSURE", ""]
        lines.append(
            f"The target measure depends on {len(inputs.dependency_chain)} "
            "other measure(s).  Their definitions follow so you can inline, "
            "restructure, or collapse them."
        )
        lines.append("")
        for dep in inputs.dependency_chain:
            name = dep.get("name", "?")
            table = dep.get("table", "?")
            expr = dep.get("expression", "")
            lines.append(f"## {table}[{name}]")
            lines.append("```dax")
            lines.append(expr)
            lines.append("```")
            lines.append("")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # EARLY-MID — contextual backdrop
    # ------------------------------------------------------------------

    @staticmethod
    def _section_model_schema(inputs: PromptInputs) -> str:
        schema = inputs.model_schema
        if not schema:
            return ""

        def _attr(obj, name, default="?"):
            """Read from dict or dataclass transparently."""
            if isinstance(obj, dict):
                return obj.get(name, default)
            return getattr(obj, name, default)

        lines = ["# MODEL SCHEMA", ""]

        # Build table→columns index.  TMDL parser puts columns at top level
        # with a .table attribute; VPAX parser may embed them under tables.
        cols_by_table: dict[str, list[str]] = {}
        for c in schema.get("columns", []):
            tbl = _attr(c, "table", "")
            cname = _attr(c, "name", "?")
            if tbl:
                cols_by_table.setdefault(tbl, []).append(cname)

        tables = schema.get("tables", [])
        if tables:
            lines.append("## Tables")
            for t in tables:
                tname = _attr(t, "name", "?")
                # Try embedded columns first, then top-level index
                embedded = t.get("columns", []) if isinstance(t, dict) else []
                col_names = (
                    [_attr(c, "name", "?") for c in embedded]
                    if embedded
                    else cols_by_table.get(tname, [])
                )
                if col_names:
                    lines.append(f"- **{tname}**: {', '.join(col_names[:20])}")
                    if len(col_names) > 20:
                        lines.append(f"  ... and {len(col_names) - 20} more")
                else:
                    lines.append(f"- **{tname}**")
            lines.append("")

        # Relationships — may be dicts or TMDLRelationship dataclasses
        rels = schema.get("relationships", [])
        if rels:
            lines.append("## Relationships")
            for r in rels:
                from_t = _attr(r, "from_table")
                from_c = _attr(r, "from_column")
                to_t = _attr(r, "to_table")
                to_c = _attr(r, "to_column")
                lines.append(f"- {from_t}[{from_c}] -> {to_t}[{to_c}]")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # MIDDLE — diagnostic + intelligence
    # ------------------------------------------------------------------

    @staticmethod
    def _section_detected_issues(inputs: PromptInputs) -> str:
        if not inputs.detected_issues:
            return ""

        def _field(issue, name, default=""):
            if isinstance(issue, dict):
                return issue.get(name, default)
            return getattr(issue, name, default)

        lines = ["# DETECTED ISSUES", ""]
        for issue in inputs.detected_issues:
            rid = _field(issue, "rule_id")
            sev = _field(issue, "severity", "info").upper()
            desc = _field(issue, "description")
            rec = _field(issue, "recommendation")
            lines.append(f"- **[{rid}] {sev}**: {desc}")
            if rec:
                lines.append(f"  Suggestion: {rec}")
        return "\n".join(lines)

    @staticmethod
    def _section_playbook() -> str:
        playbook = load_playbook()
        return f"# REWRITE PLAYBOOK\n\n{playbook}"

    @staticmethod
    def _section_examples(inputs: PromptInputs) -> str:
        # Derive pathologies from detected issues
        pathologies = _infer_pathologies(inputs.detected_issues)

        examples = match_examples(pathologies, max_examples=2)
        if not examples:
            return ""

        lines = ["# GOLD EXAMPLES", ""]
        for ex in examples:
            eid = ex.get("id", "?")
            speedup = ex.get("verified_speedup", ex.get("timing", {}).get("speedup", "?"))
            key_insight = ex.get("example", {}).get("key_insight", "")
            when_not = ex.get("example", {}).get("when_not_to_use", "")
            transforms = ex.get("transforms_applied", [])
            addressed = ex.get("pathologies_addressed", [])

            lines.append(f"## Example: {eid} ({speedup}x speedup)")
            lines.append(f"Pathologies: {', '.join(addressed)}")
            lines.append(f"Transforms: {', '.join(transforms)}")
            lines.append(f"Insight: {key_insight}")
            if when_not:
                lines.append(f"When NOT to use: {when_not}")

            # Show optimized DAX snippet if available
            opt_dax = ex.get("optimized_dax", "")
            if opt_dax:
                # Truncate very long examples
                if len(opt_dax) > 1500:
                    opt_dax = opt_dax[:1500] + "\n... (truncated)"
                lines.append("```dax")
                lines.append(opt_dax)
                lines.append("```")

            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # LATE-MID — retry + guards
    # ------------------------------------------------------------------

    @staticmethod
    def _section_retry_context(inputs: PromptInputs) -> str:
        if not inputs.previous_attempt:
            return ""
        lines = [
            "# PREVIOUS ATTEMPT (FAILED)",
            "",
            "Your previous optimisation attempt did not pass validation.",
            "Fix the issue and try again.",
            "",
            "## Previous DAX",
            "```dax",
            inputs.previous_attempt,
            "```",
        ]
        if inputs.previous_error:
            lines.extend([
                "",
                "## Error",
                inputs.previous_error,
            ])
        return "\n".join(lines)

    @staticmethod
    def _section_constraints() -> str:
        return (
            "# CONSTRAINTS (MUST OBEY)\n"
            "1. **Semantic equivalence**: The optimised measure MUST return identical "
            "values in every filter context — no approximations.\n"
            "2. **No EVALUATE / DEFINE**: Return only the measure body expression. "
            "Do NOT wrap in EVALUATE or DEFINE MEASURE.\n"
            "3. **Power BI compatible**: No Analysis-Services-only functions.\n"
            "4. **Prefer VARs**: Use VAR/RETURN for readability and caching.\n"
            "5. **Preserve filter propagation**: Do not change CALCULATE filter "
            "semantics (KEEPFILTERS, ALL, ALLEXCEPT, REMOVEFILTERS).\n"
            "6. **No new calculated columns**: Do not assume columns exist "
            "unless they appear in the model schema above.\n"
            "7. **Do not split into multiple measures**: Return a single expression."
        )

    # ------------------------------------------------------------------
    # RECENCY — last thing the model reads before generating
    # ------------------------------------------------------------------

    @staticmethod
    def _section_output_format() -> str:
        return (
            "# OUTPUT FORMAT\n"
            "Return a single JSON object inside a ```json fenced block:\n\n"
            "```json\n"
            "{\n"
            '  "optimized_dax": "<the optimised DAX expression>",\n'
            '  "transforms_applied": ["<transform_name>", ...],\n'
            '  "rationale": "<1-3 sentence explanation of what changed and why>"\n'
            "}\n"
            "```\n\n"
            "If you determine the measure cannot be meaningfully improved, return:\n\n"
            "```json\n"
            "{\n"
            '  "optimized_dax": "",\n'
            '  "transforms_applied": [],\n'
            '  "rationale": "<why no optimisation is possible>"\n'
            "}\n"
            "```"
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _infer_pathologies(issues: list) -> list[str]:
    """Map DAXAnalyzer rule IDs to playbook pathology IDs.

    This is a heuristic bridge — the static analyser speaks in rule IDs
    (DAX001, DAX003, ...) while the playbook speaks in pathology IDs (P1-P5).
    """
    if not issues:
        return []

    def _field(issue, name, default=""):
        if isinstance(issue, dict):
            return issue.get(name, default)
        return getattr(issue, name, default)

    rule_ids = {_field(i, "rule_id", "") for i in issues}

    pathologies = set()

    # P1: Measure forest causing repeated table scans
    # DAX003 = deep CALCULATE nesting, DAX027 = measure chain depth > 5
    if rule_ids & {"DAX003", "DAX027"}:
        pathologies.add("P1")

    # P2: Uncached slicer state per iterator row
    # No exact rule — heuristic: FILTER-table-iterator (DAX001) often
    # co-occurs with SELECTEDVALUE inside iterators
    if rule_ids & {"DAX001"}:
        pathologies.add("P2")

    # P3: GROUPBY+SUMX in conditional branches
    # DAX002 = SUMX+FILTER pattern
    if rule_ids & {"DAX002"}:
        pathologies.add("P3")

    # P4: Grain-first materialisation for iterator cost
    # DAXC001 = row-by-row iteration with inline ownership+carbon calc
    # Also triggered by expensive iterator patterns (DAX001/DAX002)
    if rule_ids & {"DAX001", "DAX002", "DAXC001"}:
        pathologies.add("P4")

    # P5: Sum-of-ratios pattern
    # DAX028 = division inside SUMX/AVERAGEX (SUM_OF_RATIOS_PATTERN)
    if rule_ids & {"DAX028"}:
        pathologies.add("P5")

    # Fallback: if issues exist but no pathology mapped, include P1 + P4
    # (most common patterns)
    if not pathologies and issues:
        pathologies.update(["P1", "P4"])

    return sorted(pathologies)
