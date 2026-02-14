"""Briefing validation — checklist and structural validation for §VI output.

Validates:
  - SHARED: semantic_contract, optimal_path, current_plan_gap,
    active_constraints, regression_warnings, diversity_map
  - WORKER: strategy, approach, target_query_map, node_contracts,
    examples, example_adaptation, hazard_flags
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)


REQUIRED_CORRECTNESS_IDS = (
    "LITERAL_PRESERVATION",
    "SEMANTIC_EQUIVALENCE",
    "COMPLETE_OUTPUT",
    "CTE_COLUMN_COMPLETENESS",
)

VALID_GOALS = (
    "MINIMIZE ROWS TOUCHED",
    "SMALLEST SET FIRST",
    "DON'T REPEAT WORK",
    "SETS OVER LOOPS",
    "ARM THE OPTIMIZER",
    "MINIMIZE DATA MOVEMENT",
)

VALID_ROLES = (
    "proven_compound",
    "structural_alt",
    "aggressive_compound",
    "novel_orthogonal",
)

VALID_FAMILIES = ("A", "B", "C", "D", "E", "F")


def build_analyst_checklist(is_discovery_mode: bool = False) -> str:
    """Checklist the analyst must satisfy for swarm output."""
    lines = [
        "## Section Validation Checklist (MUST pass before final output)",
        "",
        "### SHARED BRIEFING",
        "- `SEMANTIC_CONTRACT`: 80-150 tokens covering business intent, JOIN semantics, aggregation traps, filter dependencies.",
        "- `OPTIMAL_PATH`: deduced ideal join order from Step 3 with running rowcount at each step.",
        "- `CURRENT_PLAN_GAP`: per divergence: which goal violated, which blind spot causes it, how many excess rows.",
        "- `ACTIVE_CONSTRAINTS`: all 4 correctness IDs + 0-3 engine gap IDs with EXPLAIN evidence.",
        "- `REGRESSION_WARNINGS`: `None applicable.` or entries with `CAUSE:` and `RULE:`.",
    ]
    if is_discovery_mode:
        lines.extend([
            "- `DIVERSITY_MAP`: table with 4 rows, columns: Worker, Role, Primary Family, Secondary, Key Structural Idea.",
            "- `FAMILY_COVERAGE`: lists which worker covers each family A-F.",
            "",
            "### WORKER N BRIEFING (N=1..4) — all exploration workers",
        ])
    else:
        lines.extend([
            "- `DIVERSITY_MAP`: table with 4 rows, columns: Worker, Role, Primary Family, Secondary, Key Structural Idea.",
            "- `FAMILY_COVERAGE`: lists which worker covers each family A-F.",
            "",
            "### WORKER N BRIEFING (N=1..4)",
        ])
    lines.extend([
        "- `STRATEGY`: non-empty, unique across workers.",
        "- `ROLE`: proven_compound | structural_alt | aggressive_compound | novel_orthogonal.",
        "- `PRIMARY_FAMILY`: A-F — which transform family this worker leads with.",
        "- `APPROACH`: 2-3 sentences: structural idea, which gap it closes, which goal it serves.",
        "- `TARGET_QUERY_MAP`: new query map showing restructured data flow with monotonically decreasing rowcounts.",
        "- `NODE_CONTRACTS`: every node has FROM, OUTPUT, CONSUMERS.",
        "- `EXAMPLES`: 1-3 IDs from §VII.B. `EXAMPLE_ADAPTATION`: what to adapt/ignore per example.",
        "- `HAZARD_FLAGS`: query-specific risks, not generic cautions.",
        "",
    ])
    if is_discovery_mode:
        lines.extend([
            "### EXPLORATION FIELDS (all workers in discovery mode)",
            "- All workers include `EXPLORATION_TYPE`, `HYPOTHESIS_TAG`.",
        ])
    else:
        lines.extend([
            "### WORKER 4 EXPLORATION FIELDS",
            "- Includes `EXPLORATION_TYPE`, `HYPOTHESIS_TAG`, `UNCOVERED_FAMILY`.",
        ])
    return "\n".join(lines)


def build_oneshot_checklist() -> str:
    """Checklist for oneshot mode."""
    return "\n".join([
        "## Section Validation Checklist (MUST pass before final output)",
        "",
        "### SHARED BRIEFING",
        "- `SEMANTIC_CONTRACT`: 80-150 tokens.",
        "- `OPTIMAL_PATH`: deduced ideal join order.",
        "- `CURRENT_PLAN_GAP`: at least one divergence.",
        "- `ACTIVE_CONSTRAINTS`: all 4 correctness IDs.",
        "- `REGRESSION_WARNINGS`: `None applicable.` or entries with CAUSE/RULE.",
        "",
        "### OPTIMIZED SQL",
        "- `STRATEGY` and `TRANSFORM` specified.",
        "- SQL enclosed in ```sql code block.",
        "- Semantically equivalent to original.",
    ])


def build_worker_rewrite_checklist() -> str:
    """Checklist the worker uses before returning SQL."""
    return "\n".join([
        "## Rewrite Checklist (must pass before final SQL)",
        "",
        "- Follow every node in `TARGET_QUERY_MAP` and produce each `NODE_CONTRACT` output column exactly.",
        "- Keep all semantic invariants from `Semantic Contract` and `Constraints`.",
        "- Preserve all literals and the exact final output schema/order.",
        "- Apply `Hazard Flags` and `Regression Warnings` as hard guards.",
        "- Verify your rewrite addresses the CURRENT_PLAN_GAP divergences.",
    ])


def validate_parsed_briefing(
    briefing: Any,
    expected_workers: int = 4,
    lenient: bool = False,
) -> List[str]:
    """Validate a parsed analyst briefing.

    Returns list of issues. Empty = passed.

    Args:
        lenient: If True (used after retry), tolerate 1 missing correctness ID
            in the ACTIVE_CONSTRAINTS briefing reminder section. Workers still
            receive the actual correctness gates from the shared prefix's
            CORRECTNESS GATES section (§V), which includes all required gates
            regardless of what's in the briefing reminder. One missing ID from
            the briefing reminder is non-fatal because the gates are still
            enforced at execution time.
    """
    if expected_workers < 1:
        raise ValueError("expected_workers must be >= 1")

    issues: List[str] = []
    shared = getattr(briefing, "shared", None)
    workers = list(getattr(briefing, "workers", []) or [])

    if shared is None:
        return ["Missing shared briefing section."]

    issues.extend(_validate_shared(shared, expected_workers, lenient=lenient))

    # Worker validation
    relevant_workers = []
    for w in workers:
        wid = int(getattr(w, "worker_id", 0) or 0)
        if 1 <= wid <= expected_workers:
            relevant_workers.append(w)

    seen_strategies: Dict[str, int] = {}
    worker_ids = {int(getattr(w, "worker_id", 0) or 0) for w in relevant_workers}
    for wid in range(1, expected_workers + 1):
        if wid not in worker_ids:
            issues.append(f"WORKER_{wid}: missing worker briefing block.")

    for worker in relevant_workers:
        wid = int(getattr(worker, "worker_id", 0) or 0)
        issues.extend(_validate_worker(worker, wid, seen_strategies))

    return issues


def _validate_shared(shared: Any, expected_workers: int, *, lenient: bool = False) -> List[str]:
    """Validate shared briefing fields."""
    issues: List[str] = []

    # SEMANTIC_CONTRACT
    sc = (getattr(shared, "semantic_contract", "") or "").strip()
    if not sc:
        issues.append("SHARED: SEMANTIC_CONTRACT missing.")
    else:
        token_count = len(sc.split())
        if token_count < 30 or token_count > 250:
            issues.append(
                f"SHARED: SEMANTIC_CONTRACT token count {token_count} (expected 30-250)."
            )

    # OPTIMAL_PATH (was BOTTLENECK_DIAGNOSIS)
    op = (getattr(shared, "optimal_path", "") or "").strip()
    # Backwards-compat: also check old alias
    if not op:
        op = (getattr(shared, "bottleneck_diagnosis", "") or "").strip()
    if not op:
        issues.append("SHARED: OPTIMAL_PATH missing.")

    # CURRENT_PLAN_GAP (was GOAL_VIOLATIONS)
    cpg = (getattr(shared, "current_plan_gap", "") or "").strip()
    # Backwards-compat: also check old alias
    if not cpg:
        cpg = (getattr(shared, "goal_violations", "") or "").strip()
    if not cpg:
        issues.append("SHARED: CURRENT_PLAN_GAP missing.")

    # ACTIVE_CONSTRAINTS
    ac = (getattr(shared, "active_constraints", "") or "").strip()
    if not ac:
        issues.append("SHARED: ACTIVE_CONSTRAINTS missing.")
    else:
        ids = re.findall(r"-\s*([A-Z][A-Z0-9_]+)\s*:", ac)
        id_set = set(ids)
        missing = [cid for cid in REQUIRED_CORRECTNESS_IDS if cid not in id_set]
        if missing:
            if lenient and len(missing) <= 1:
                # Post-retry leniency: 1 missing correctness ID is a warning,
                # not a hard error. Workers still receive all constraints from
                # §V CORRECTNESS GATES in the shared worker prefix.
                logger.warning(
                    "ACTIVE_CONSTRAINTS missing %s (lenient: continuing)",
                    ", ".join(missing),
                )
            else:
                for cid in missing:
                    issues.append(f"SHARED: ACTIVE_CONSTRAINTS missing {cid}.")
        gap_ids = [cid for cid in ids if cid not in REQUIRED_CORRECTNESS_IDS]
        if len(gap_ids) > 3:
            issues.append(
                f"SHARED: ACTIVE_CONSTRAINTS has too many gap IDs ({len(gap_ids)})."
            )

    # REGRESSION_WARNINGS
    rw = (getattr(shared, "regression_warnings", "") or "").strip()
    if not rw:
        issues.append("SHARED: REGRESSION_WARNINGS missing.")
    else:
        if rw.lower() != "none applicable.":
            if "CAUSE:" not in rw or "RULE:" not in rw:
                issues.append(
                    "SHARED: REGRESSION_WARNINGS entries must include CAUSE and RULE."
                )

    # DIVERSITY_MAP (swarm only)
    if expected_workers > 1:
        dm = (getattr(shared, "diversity_map", "") or "").strip()
        if not dm:
            issues.append("SHARED: DIVERSITY_MAP missing.")

    return issues


def _validate_worker(worker: Any, wid: int, seen_strategies: Dict[str, int]) -> List[str]:
    """Validate a single worker's briefing fields."""
    issues: List[str] = []

    # STRATEGY
    strategy = (getattr(worker, "strategy", "") or "").strip()
    if not strategy or strategy == f"strategy_{wid}":
        issues.append(f"WORKER_{wid}: STRATEGY missing or placeholder.")
    elif strategy in seen_strategies:
        issues.append(
            f"WORKER_{wid}: STRATEGY duplicates WORKER_{seen_strategies[strategy]}."
        )
    else:
        seen_strategies[strategy] = wid

    # ROLE (machine-readable family assignment)
    role = (getattr(worker, "role", "") or "").strip().lower()
    if not role:
        issues.append(f"WORKER_{wid}: ROLE missing.")
    elif role not in VALID_ROLES:
        issues.append(
            f"WORKER_{wid}: ROLE '{role}' must be one of: {', '.join(VALID_ROLES)}."
        )

    # PRIMARY_FAMILY (machine-readable optimization family A-F)
    primary_family = (getattr(worker, "primary_family", "") or "").strip().upper()
    if not primary_family:
        issues.append(f"WORKER_{wid}: PRIMARY_FAMILY missing.")
    elif primary_family not in VALID_FAMILIES:
        issues.append(
            f"WORKER_{wid}: PRIMARY_FAMILY '{primary_family}' must be one of: {', '.join(VALID_FAMILIES)}."
        )

    # APPROACH (new — structural idea, gap, goal)
    approach = (getattr(worker, "approach", "") or "").strip()
    if not approach:
        issues.append(f"WORKER_{wid}: APPROACH missing.")

    # TARGET_QUERY_MAP + NODE_CONTRACTS (replaces TARGET_LOGICAL_TREE)
    tqm = (getattr(worker, "target_query_map", "") or "").strip()
    nc = (getattr(worker, "node_contracts", "") or "").strip()
    # Also check backwards-compat alias
    tlt = (getattr(worker, "target_logical_tree", "") or "").strip()
    if not tqm and not tlt:
        issues.append(f"WORKER_{wid}: TARGET_QUERY_MAP missing.")
    if not nc and not tlt:
        issues.append(f"WORKER_{wid}: NODE_CONTRACTS missing.")

    # EXAMPLES
    examples = [e.strip() for e in (getattr(worker, "examples", []) or []) if e and e.strip()]
    if not examples:
        issues.append(f"WORKER_{wid}: EXAMPLES missing.")
    if len(examples) > 3:
        issues.append(f"WORKER_{wid}: EXAMPLES has more than 3 IDs.")

    # EXAMPLE_ADAPTATION
    if not (getattr(worker, "example_adaptation", "") or "").strip():
        issues.append(f"WORKER_{wid}: EXAMPLE_ADAPTATION missing.")

    # HAZARD_FLAGS
    if not (getattr(worker, "hazard_flags", "") or "").strip():
        issues.append(f"WORKER_{wid}: HAZARD_FLAGS missing.")

    return issues
