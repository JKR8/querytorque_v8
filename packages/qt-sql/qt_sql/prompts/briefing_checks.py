"""Checklist and validation helpers for V2 analyst briefings."""

from __future__ import annotations

import re
from typing import Any, Dict, List


REQUIRED_CORRECTNESS_IDS = (
    "LITERAL_PRESERVATION",
    "SEMANTIC_EQUIVALENCE",
    "COMPLETE_OUTPUT",
    "CTE_COLUMN_COMPLETENESS",
)


def build_analyst_section_checklist() -> str:
    """Checklist the analyst must satisfy for each output section."""
    return "\n".join([
        "## Section Validation Checklist (MUST pass before final output)",
        "",
        "Use this checklist to verify content quality, not just section presence:",
        "",
        "### SHARED BRIEFING",
        "- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.",
        "- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.",
        "- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.",
        "- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.",
        "",
        "### WORKER N BRIEFING (N=1..4)",
        "- `STRATEGY`: non-empty and unique across workers.",
        "- `TARGET_DAG`: explicit node chain (e.g., `a -> b -> c`).",
        "- `NODE_CONTRACTS`: every DAG node has a contract with `FROM`, `OUTPUT` (explicit columns), and `CONSUMERS`.",
        "- `EXAMPLES`: 1-3 IDs per worker. Sharing an example across workers is allowed if each worker's EXAMPLE_ADAPTATION explains a different aspect to apply.",
        "- `EXAMPLE_ADAPTATION`: for each example, states what to adapt and what to ignore for this worker's strategy.",
        "- `HAZARD_FLAGS`: query-specific risks, not generic cautions.",
        "",
        "### WORKER 4 EXPLORATION FIELDS",
        "- Includes `CONSTRAINT_OVERRIDE`, `OVERRIDE_REASONING`, and `EXPLORATION_TYPE`.",
    ])


def build_expert_section_checklist() -> str:
    """Checklist for single-worker expert mode."""
    return "\n".join([
        "## Section Validation Checklist (MUST pass before final output)",
        "",
        "Use this checklist to verify content quality, not just section presence:",
        "",
        "### SHARED BRIEFING",
        "- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.",
        "- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.",
        "- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.",
        "- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.",
        "",
        "### WORKER 1 BRIEFING",
        "- `STRATEGY`: non-empty, describes the best single strategy.",
        "- `TARGET_DAG`: explicit node chain (e.g., `a -> b -> c`).",
        "- `NODE_CONTRACTS`: every DAG node has a contract with `FROM`, `OUTPUT` (explicit columns), and `CONSUMERS`.",
        "- `EXAMPLES`: 1-3 IDs. Each has `EXAMPLE_ADAPTATION` explaining what to adapt and what to ignore.",
        "- `HAZARD_FLAGS`: query-specific risks, not generic cautions.",
    ])


def build_oneshot_section_checklist() -> str:
    """Checklist for one-shot mode (analysis + SQL output)."""
    return "\n".join([
        "## Section Validation Checklist (MUST pass before final output)",
        "",
        "Use this checklist to verify content quality, not just section presence:",
        "",
        "### SHARED BRIEFING",
        "- `SEMANTIC_CONTRACT`: 80-150 tokens and includes business intent, JOIN semantics, aggregation trap, and filter dependency.",
        "- `BOTTLENECK_DIAGNOSIS`: states dominant mechanism, bound type (`scan-bound`/`join-bound`/`aggregation-bound`), cardinality flow, and what optimizer already handles well.",
        "- `ACTIVE_CONSTRAINTS`: includes all 4 correctness IDs plus 1-3 active engine gaps with EXPLAIN evidence.",
        "- `REGRESSION_WARNINGS`: either `None applicable.` or numbered entries with both `CAUSE:` and `RULE:`.",
        "",
        "### OPTIMIZED SQL",
        "- `STRATEGY`: non-empty, describes the optimization approach.",
        "- `TRANSFORM`: lists the transform(s) applied.",
        "- SQL block is present, complete, and syntactically valid.",
        "- Output columns match original query exactly (same names, same order).",
        "- All literals preserved exactly (numbers, strings, date values).",
        "- Semantically equivalent to the original query.",
    ])


def build_worker_rewrite_checklist() -> str:
    """Checklist the worker uses before returning SQL."""
    return "\n".join([
        "## Rewrite Checklist (must pass before final SQL)",
        "",
        "- Follow every node in `TARGET_DAG` and produce each `NODE_CONTRACT` output column exactly.",
        "- Keep all semantic invariants from `Semantic Contract` and `Constraints` (including join/null behavior).",
        "- Preserve all literals and the exact final output schema/order.",
        "- Apply `Hazard Flags` and `Regression Warnings` as hard guards against known failure modes.",
    ])


def build_sniper_rewrite_checklist() -> str:
    """Checklist the sniper uses before returning SQL (no TARGET_DAG reference)."""
    return "\n".join([
        "## Rewrite Checklist (must pass before final SQL)",
        "",
        "- Verify output schema matches the Column Completeness Contract (same columns, same names, same order).",
        "- Keep all semantic invariants from `Correctness Invariants` (including join/null behavior).",
        "- Verify aggregation equivalence: same rows participate in each group, same aggregate semantics.",
        "- Preserve all literals exactly (numbers, strings, date values).",
        "- Apply `Hazard Flags` as hard guards against known failure modes.",
    ])


def validate_parsed_briefing(briefing: Any) -> List[str]:
    """Validate that a parsed analyst briefing is correctly populated.

    Returns a list of human-readable issues. Empty list means it passed checks.
    """
    issues: List[str] = []
    shared = getattr(briefing, "shared", None)
    workers = list(getattr(briefing, "workers", []) or [])

    if shared is None:
        return ["Missing shared briefing section."]

    issues.extend(_validate_shared(shared))

    seen_strategies: Dict[str, int] = {}
    seen_examples: Dict[str, int] = {}
    worker_ids = {int(getattr(w, "worker_id", 0) or 0) for w in workers}
    for wid in range(1, 5):
        if wid not in worker_ids:
            issues.append(f"WORKER_{wid}: missing worker briefing block.")

    for worker in workers:
        wid = int(getattr(worker, "worker_id", 0) or 0)
        strategy = (getattr(worker, "strategy", "") or "").strip()
        if not strategy or strategy == f"strategy_{wid}":
            issues.append(f"WORKER_{wid}: STRATEGY missing or placeholder.")
        elif strategy in seen_strategies:
            issues.append(
                f"WORKER_{wid}: STRATEGY duplicates WORKER_{seen_strategies[strategy]}."
            )
        else:
            seen_strategies[strategy] = wid

        examples = [e.strip() for e in (getattr(worker, "examples", []) or []) if e and e.strip()]
        if not examples:
            issues.append(f"WORKER_{wid}: EXAMPLES missing.")
        if len(examples) > 3:
            issues.append(f"WORKER_{wid}: EXAMPLES has more than 3 IDs.")
        for ex in examples:
            seen_examples.setdefault(ex, wid)

        if not (getattr(worker, "example_reasoning", "") or "").strip():
            issues.append(f"WORKER_{wid}: EXAMPLE_REASONING missing.")
        if not (getattr(worker, "hazard_flags", "") or "").strip():
            issues.append(f"WORKER_{wid}: HAZARD_FLAGS missing.")

        target_dag = (getattr(worker, "target_dag", "") or "").strip()
        if not target_dag:
            issues.append(f"WORKER_{wid}: TARGET_DAG/NODE_CONTRACTS missing.")
            continue
        issues.extend(_validate_worker_target_dag(wid, target_dag))

    return issues


def _validate_shared(shared: Any) -> List[str]:
    issues: List[str] = []

    semantic_contract = (getattr(shared, "semantic_contract", "") or "").strip()
    if not semantic_contract:
        issues.append("SHARED: SEMANTIC_CONTRACT missing.")
    else:
        token_count = len(semantic_contract.split())
        if token_count < 80 or token_count > 150:
            issues.append(
                f"SHARED: SEMANTIC_CONTRACT token count {token_count} (expected 80-150)."
            )

    bottleneck = (getattr(shared, "bottleneck_diagnosis", "") or "").strip()
    if not bottleneck:
        issues.append("SHARED: BOTTLENECK_DIAGNOSIS missing.")
    else:
        low = bottleneck.lower()
        if not any(t in low for t in ("scan-bound", "join-bound", "aggregation-bound")):
            issues.append(
                "SHARED: BOTTLENECK_DIAGNOSIS missing bound classification "
                "(`scan-bound`/`join-bound`/`aggregation-bound`)."
            )
        if "optimizer" not in low:
            issues.append("SHARED: BOTTLENECK_DIAGNOSIS should mention optimizer overlap.")

    active_constraints = (getattr(shared, "active_constraints", "") or "").strip()
    if not active_constraints:
        issues.append("SHARED: ACTIVE_CONSTRAINTS missing.")
    else:
        ids = re.findall(r"-\s*([A-Z][A-Z0-9_]+)\s*:", active_constraints)
        id_set = set(ids)
        for cid in REQUIRED_CORRECTNESS_IDS:
            if cid not in id_set:
                issues.append(f"SHARED: ACTIVE_CONSTRAINTS missing {cid}.")
        gap_ids = [cid for cid in ids if cid not in REQUIRED_CORRECTNESS_IDS]
        if not (1 <= len(gap_ids) <= 3):
            issues.append(
                "SHARED: ACTIVE_CONSTRAINTS should include 1-3 engine gap IDs in addition "
                "to the 4 correctness constraints."
            )

    regression = (getattr(shared, "regression_warnings", "") or "").strip()
    if not regression:
        issues.append("SHARED: REGRESSION_WARNINGS missing.")
    else:
        if regression.lower() != "none applicable.":
            if "CAUSE:" not in regression or "RULE:" not in regression:
                issues.append(
                    "SHARED: REGRESSION_WARNINGS entries must include both CAUSE and RULE."
                )

    return issues


def _validate_worker_target_dag(worker_id: int, target_dag: str) -> List[str]:
    issues: List[str] = []
    dag_nodes = _extract_dag_nodes(target_dag)
    if not dag_nodes:
        issues.append(f"WORKER_{worker_id}: TARGET_DAG node chain missing.")
        return issues

    contracts = _extract_node_contracts(target_dag)
    if not contracts:
        issues.append(f"WORKER_{worker_id}: NODE_CONTRACTS block missing or unparsable.")
        return issues

    for node in dag_nodes:
        if node not in contracts:
            issues.append(f"WORKER_{worker_id}: NODE_CONTRACTS missing node '{node}'.")

    required_fields = ("FROM", "OUTPUT", "CONSUMERS")
    for node, fields in contracts.items():
        for req in required_fields:
            if not (fields.get(req, "") or "").strip():
                issues.append(
                    f"WORKER_{worker_id}: node '{node}' missing required field `{req}`."
                )

        output = (fields.get("OUTPUT", "") or "").lower()
        if output and (
            "all " in output
            or " all_" in output
            or "columns" in output
            or "..." in output
        ):
            issues.append(
                f"WORKER_{worker_id}: node '{node}' OUTPUT uses placeholder text, "
                "not an explicit column list."
            )

    for idx, node in enumerate(dag_nodes[:-1]):
        next_node = dag_nodes[idx + 1]
        consumers = contracts.get(node, {}).get("CONSUMERS", "")
        consumer_nodes = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", consumers))
        if next_node not in consumer_nodes:
            issues.append(
                f"WORKER_{worker_id}: node '{node}' CONSUMERS does not include "
                f"next DAG node '{next_node}'."
            )

    return issues


def _extract_dag_nodes(text: str) -> List[str]:
    match = re.search(
        r"TARGET_DAG:\s*(.*?)(?:\n\s*NODE_CONTRACTS\s*:|$)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    block = match.group(1) if match else text
    nodes: List[str] = []
    for line in block.splitlines():
        if "->" not in line:
            continue
        parts = [p.strip() for p in line.split("->")]
        for raw in parts:
            node = raw.strip("`[]() ")
            if not node:
                continue
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", node) and node not in nodes:
                nodes.append(node)
    return nodes


def _extract_node_contracts(text: str) -> Dict[str, Dict[str, str]]:
    match = re.search(r"NODE_CONTRACTS:\s*\n(.*)$", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return {}
    block = match.group(1)

    contracts: Dict[str, Dict[str, str]] = {}
    current_node = ""
    last_field = ""
    field_re = re.compile(
        r"^\s{4,}(FROM|JOIN|WHERE|GROUP BY|AGGREGATE|OUTPUT|EXPECTED_ROWS|CONSUMERS)\s*:\s*(.*)$",
        re.IGNORECASE,
    )
    node_re = re.compile(r"^\s{2,}([A-Za-z_][A-Za-z0-9_]*)\s*:\s*$")

    for line in block.splitlines():
        node_match = node_re.match(line)
        if node_match:
            current_node = node_match.group(1)
            contracts.setdefault(current_node, {})
            last_field = ""
            continue

        if not current_node:
            continue

        field_match = field_re.match(line)
        if field_match:
            field = field_match.group(1).upper()
            value = field_match.group(2).strip()
            contracts[current_node][field] = value
            last_field = field
            continue

        # Continuation line for multi-line field values.
        if last_field and line.strip():
            prev = contracts[current_node].get(last_field, "")
            contracts[current_node][last_field] = f"{prev} {line.strip()}".strip()

    return contracts
