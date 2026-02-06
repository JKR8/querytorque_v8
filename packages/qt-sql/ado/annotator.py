"""Phase 2: DAG annotation via lightweight LLM call.

Sends DAG topology + cost attribution to an LLM and receives
back {node: pattern} assignments. Replaces the old hardcoded
FEATURE_TO_PATTERN mapping that Q47 proved was broken.

The annotator sees structure and costs only — NO SQL code —
keeping the call under ~1K tokens total.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from .schemas import AnnotationResult, NodeAnnotation, SkippedNode

logger = logging.getLogger(__name__)

# Cost threshold: only flag nodes > 10% of total cost
COST_THRESHOLD = 0.10


class Annotator:
    """Phase 2: Assign rewrite patterns to DAG nodes via lightweight LLM call.

    Input: DAG topology + per-node cost attribution (no SQL code).
    Output: {node: pattern} mapping as AnnotationResult.
    """

    def __init__(
        self,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        analyze_fn=None,
    ):
        self.provider = provider
        self.model = model
        self._analyze_fn = analyze_fn
        self._llm_client = None

    def _call_llm(self, prompt: str) -> str:
        """Send prompt to LLM and get response."""
        if self._analyze_fn is not None:
            return self._analyze_fn(prompt)

        if self._llm_client is None:
            try:
                from qt_shared.llm import create_llm_client
                self._llm_client = create_llm_client(
                    provider=self.provider,
                    model=self.model,
                )
            except ImportError:
                raise RuntimeError(
                    "No LLM client available. Provide analyze_fn or install qt_shared."
                )

        return self._llm_client.analyze(prompt)

    def annotate(
        self,
        dag,
        costs: Dict[str, Any],
        available_patterns: List[str],
        use_llm: bool = False,
    ) -> AnnotationResult:
        """Annotate DAG nodes with rewrite patterns.

        Args:
            dag: QueryDag from dag_v2.DagBuilder
            costs: Dict[node_id -> NodeCost] from CostAnalyzer
            available_patterns: List of pattern names from gold examples
            use_llm: If True, call LLM for annotation. If False (default),
                     use heuristic only (saves API cost).

        Returns:
            AnnotationResult with rewrites and skipped nodes
        """
        if not use_llm:
            return self._heuristic_fallback(dag, costs, available_patterns)

        prompt = self._build_prompt(dag, costs, available_patterns)

        try:
            response = self._call_llm(prompt)
            return self._parse_response(response, dag)
        except Exception as e:
            logger.warning(f"Annotation LLM call failed: {e}")
            return self._heuristic_fallback(dag, costs, available_patterns)

    def _build_prompt(
        self,
        dag,
        costs: Dict[str, Any],
        available_patterns: List[str],
    ) -> str:
        """Build the Phase 2 annotation prompt.

        Per design doc §Phase 2 Annotation:
        - Role: "You are a SQL performance analyst"
        - DAG topology: node names, types, depths, edges (NO SQL code)
        - Cost attribution: per-node % of total, bottleneck operators
        - Available patterns: from gold examples
        - Task: {node: pattern} for bottlenecks >10% cost
        - Output: JSON ~200 tokens
        """
        lines = [
            "You are a SQL performance analyst.",
            "",
            "## Query DAG Topology",
            "",
            "Nodes:",
        ]

        # Compute depths for each node
        depths = self._compute_depths(dag)

        for node_id, node in dag.nodes.items():
            depth = depths.get(node_id, 0)
            deps = ", ".join(node.refs) if node.refs else "source tables"
            flags = " ".join(node.flags) if node.flags else ""
            node_type = node.node_type

            row_est = ""
            if node_id in costs and hasattr(costs[node_id], 'row_estimate'):
                row_est = f", ~{costs[node_id].row_estimate} rows"

            lines.append(
                f"- {node_id} ({node_type}, depth {depth}{row_est}) "
                f"-> depends on: {deps} {flags}"
            )

        lines.append("")
        lines.append("## Execution Plan Cost Attribution")
        lines.append("")

        total_cost = sum(
            c.cost_pct for c in costs.values()
            if hasattr(c, 'cost_pct')
        )
        if total_cost == 0:
            total_cost = 100.0

        for node_id, cost in costs.items():
            pct = cost.cost_pct if hasattr(cost, 'cost_pct') else 0
            ops_str = ""
            if hasattr(cost, 'operators') and cost.operators:
                ops_str = ": " + ", ".join(cost.operators[:3])
            lines.append(f"- {node_id}: {pct:.1f}%{ops_str}")

        lines.append("")
        lines.append("## Available Patterns")
        lines.append("")
        for pattern in available_patterns:
            lines.append(f"- {pattern}")

        lines.append("")
        lines.append("## Task")
        lines.append("")
        lines.append("For each CTE/subquery node, determine:")
        lines.append("1. Is it a bottleneck worth rewriting? (>10% of total cost)")
        lines.append("2. If yes, which pattern best addresses its dominant cost operator?")
        lines.append("3. Brief rationale (one sentence)")
        lines.append("")
        lines.append("Return JSON:")
        lines.append("")
        lines.append('```json')
        lines.append('{')
        lines.append('  "rewrites": [')
        lines.append('    {')
        lines.append('      "node": "node_name",')
        lines.append('      "pattern": "pattern_name",')
        lines.append('      "rationale": "one sentence explaining why"')
        lines.append('    }')
        lines.append('  ],')
        lines.append('  "skip": [')
        lines.append('    {')
        lines.append('      "node": "node_name",')
        lines.append('      "reason": "below threshold or already efficient"')
        lines.append('    }')
        lines.append('  ]')
        lines.append('}')
        lines.append('```')
        lines.append("")
        lines.append("Now output your JSON:")

        return "\n".join(lines)

    def _parse_response(self, response: str, dag) -> AnnotationResult:
        """Parse LLM response into AnnotationResult."""
        # Extract JSON from response
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_match = re.search(r'\{.*"rewrites".*\}', response, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
            else:
                logger.warning("No JSON found in annotation response")
                return AnnotationResult(rewrites=[], skipped=[])

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse annotation JSON: {e}")
            return AnnotationResult(rewrites=[], skipped=[])

        rewrites = []
        for item in data.get("rewrites", []):
            node_id = item.get("node", "")
            if node_id in dag.nodes:
                rewrites.append(NodeAnnotation(
                    node_id=node_id,
                    pattern=item.get("pattern", "semantic_rewrite"),
                    rationale=item.get("rationale", ""),
                ))

        skipped = []
        for item in data.get("skip", []):
            skipped.append(SkippedNode(
                node_id=item.get("node", ""),
                reason=item.get("reason", ""),
            ))

        return AnnotationResult(rewrites=rewrites, skipped=skipped)

    def _heuristic_fallback(
        self,
        dag,
        costs: Dict[str, Any],
        available_patterns: List[str],
    ) -> AnnotationResult:
        """Heuristic fallback when LLM annotation fails.

        Uses DAG node flags to assign patterns deterministically.
        """
        rewrites = []
        skipped = []

        pattern_set = set(available_patterns)

        for node_id, node in dag.nodes.items():
            cost = costs.get(node_id)
            cost_pct = cost.cost_pct if cost and hasattr(cost, 'cost_pct') else 0

            # Skip low-cost nodes
            if cost_pct < COST_THRESHOLD * 100:
                skipped.append(SkippedNode(
                    node_id=node_id,
                    reason=f"Only {cost_pct:.1f}% of total cost",
                ))
                continue

            # Heuristic pattern assignment from node flags
            pattern = None

            if "CORRELATED" in node.flags:
                pattern = "decorrelate" if "decorrelate" in pattern_set else None
            elif "UNION_ALL" in node.flags:
                pattern = "union_cte_split" if "union_cte_split" in pattern_set else None
            elif "IN_SUBQUERY" in node.flags:
                pattern = "intersect_to_exists" if "intersect_to_exists" in pattern_set else None
            elif " OR " in node.sql.upper():
                pattern = "or_to_union" if "or_to_union" in pattern_set else None

            # Check for dimension join patterns
            if pattern is None and node.refs:
                dim_tables = {"date_dim", "customer", "store", "item", "warehouse",
                              "customer_address", "reason", "promotion"}
                node_tables = {t.lower() for t in node.tables}
                if node_tables & dim_tables:
                    if "date_dim" in node_tables and "date_cte_isolate" in pattern_set:
                        pattern = "date_cte_isolate"
                    elif "early_filter" in pattern_set:
                        pattern = "early_filter"

            # Default: pushdown for CTE nodes with high cost
            if pattern is None and node.node_type == "cte" and "pushdown" in pattern_set:
                pattern = "pushdown"

            if pattern:
                rewrites.append(NodeAnnotation(
                    node_id=node_id,
                    pattern=pattern,
                    rationale=f"Heuristic: {cost_pct:.1f}% cost, flags={node.flags}",
                ))
            else:
                skipped.append(SkippedNode(
                    node_id=node_id,
                    reason=f"No matching pattern for flags={node.flags}",
                ))

        return AnnotationResult(rewrites=rewrites, skipped=skipped)

    @staticmethod
    def _compute_depths(dag) -> Dict[str, int]:
        """Compute depth for each node in the DAG (topological depth)."""
        depths: Dict[str, int] = {}

        def _depth(node_id: str) -> int:
            if node_id in depths:
                return depths[node_id]
            node = dag.nodes.get(node_id)
            if not node or not node.refs:
                depths[node_id] = 0
                return 0
            max_parent = max(
                (_depth(ref) for ref in node.refs if ref in dag.nodes),
                default=-1,
            )
            depths[node_id] = max_parent + 1
            return depths[node_id]

        for node_id in dag.nodes:
            _depth(node_id)

        return depths

    @staticmethod
    def get_available_patterns() -> List[str]:
        """Get list of available pattern names from gold examples."""
        from .knowledge import _load_examples
        examples = _load_examples()
        return [ex.id for ex in examples]
