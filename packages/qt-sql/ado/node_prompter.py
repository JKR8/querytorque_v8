"""Phase 3: Query-level rewrite prompt builder.

Builds attention-optimized full-query rewrite prompts with DAG topology.
All rewrites are full-query scope — the LLM sees the complete SQL, the DAG
structure, and pattern hints for which nodes to target.

Section ordering (attention-optimized):
1. Role + Task          (PRIMACY - frames rewrite mindset)
2. Full Query SQL       (PRIMACY - pretty-formatted, complete query)
3. DAG Topology         (PRIMACY - nodes, edges, depths, flags, costs)
4. Performance Profile  (EARLY - per-node costs, bottleneck operators)
5. History              (EARLY-MID - previous attempts on this query)
6. Pattern Hints        (EARLY-MID - from Phase 2 annotation: which patterns where)
7. Full Example         (MIDDLE - 1 contrastive BEFORE/AFTER pair)
8. Constraints          (LATE-MID - sandwich: CRITICAL top/bottom, HIGH middle)
9. Output Format        (RECENCY - return complete rewritten SQL)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import AnnotationResult, EdgeContract, NodeAnnotation, PromotionAnalysis

logger = logging.getLogger(__name__)

# Directory paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"          # ado/examples/{duckdb,postgres}/
CONSTRAINTS_DIR = BASE_DIR / "constraints"


class Prompter:
    """Build attention-optimized full-query rewrite prompts with DAG context.

    The LLM sees:
    - Complete query SQL (not isolated nodes)
    - Full DAG topology (nodes, edges, depths, flags, costs)
    - Pattern hints from Phase 2 (which patterns to apply where)
    - 1 gold example (contrastive BEFORE/AFTER)
    - Constraints (sandwich ordered)

    This enables cross-node rewrites: creating new CTEs, restructuring
    joins, pushing filters across node boundaries.
    """

    def build_prompt(
        self,
        query_id: str,
        full_sql: str,
        dag: Any,
        costs: Dict[str, Any],
        annotation: AnnotationResult,
        history: Optional[Dict[str, Any]] = None,
        examples: Optional[List[Dict[str, Any]]] = None,
        expert_analysis: Optional[str] = None,
        dialect: str = "duckdb",
    ) -> str:
        """Build the full-query rewrite prompt.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            full_sql: Complete original SQL query
            dag: Parsed DAG from Phase 1 (DagBuilder output)
            costs: Per-node cost analysis from CostAnalyzer
            annotation: Phase 2 annotation result ({node: pattern} mapping)
            history: Previous attempts and promotion context for this query.
                     Dict with 'attempts' (list) and 'promotion' (PromotionAnalysis).
            examples: List of gold examples (FAISS-matched, up to 3)
            expert_analysis: Pre-computed LLM analyst output (analyst mode only).
                             When present, replaces pattern hints + examples with
                             concrete structural guidance.
            dialect: SQL dialect for pretty-printing
        """
        sections = []

        # Section 1: Role + Task (PRIMACY)
        sections.append(self._section_role_task())

        # Section 2: Full Query SQL (PRIMACY)
        sections.append(self._section_full_sql(query_id, full_sql, dialect))

        # Section 3: DAG Topology (PRIMACY)
        sections.append(self._section_dag_topology(dag, costs))

        # Section 4: Performance Profile (EARLY)
        sections.append(self._section_performance(dag, costs))

        # Section 5: History (EARLY-MID)
        if history:
            sections.append(self._section_history(history))

        if expert_analysis:
            # Analyst mode: inject the LLM analysis instead of pattern hints
            # This gives the rewrite LLM concrete structural guidance
            sections.append(expert_analysis)
        else:
            # Default mode: FAISS-matched gold examples + pattern hints
            # Section 6: Pattern Hints (EARLY-MID)
            sections.append(self._section_pattern_hints(annotation))

            # Section 7: Examples (MIDDLE) — up to 3 FAISS-matched
            if examples:
                sections.append(self._section_examples(examples))

        # Section 8: Constraints (LATE-MID, sandwich ordered)
        sections.append(self._section_constraints())

        # Section 9: Output Format (RECENCY)
        sections.append(self._section_output_format())

        return "\n\n".join(sections)

    # =========================================================================
    # Section builders
    # =========================================================================

    @staticmethod
    def _section_role_task() -> str:
        """Section 1: Role + Task."""
        return (
            "You are a SQL query rewrite engine.\n"
            "\n"
            "Your goal: rewrite the complete SQL query to maximize execution speed\n"
            "while preserving exact semantic equivalence (same rows, same columns,\n"
            "same ordering).\n"
            "\n"
            "You will receive the full query, its DAG structure showing how CTEs and\n"
            "subqueries connect, cost analysis per node, and suggested rewrite patterns.\n"
            "You may restructure the query freely: create new CTEs, merge existing ones,\n"
            "push filters across node boundaries, or decompose subqueries."
        )

    @staticmethod
    def _strip_comments(sql: str) -> str:
        """Strip block comments (/* ... */) and line comments (-- ...) from SQL."""
        import re
        # Remove block comments
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        # Remove line comments (but not inside strings)
        sql = re.sub(r'--[^\n]*', '', sql)
        # Collapse multiple blank lines
        sql = re.sub(r'\n{3,}', '\n\n', sql)
        return sql.strip()

    @staticmethod
    def _build_dag_comments(dag: Any, costs: Dict[str, Any]) -> str:
        """Build DAG topology as SQL comments to embed in the query."""
        from .annotator import Annotator
        depths = Annotator._compute_depths(dag)

        lines = ["-- DAG TOPOLOGY"]

        max_depth = max(depths.values()) if depths else 0
        for depth in range(max_depth + 1):
            nodes_at_depth = [
                nid for nid, d in depths.items() if d == depth
            ]
            if not nodes_at_depth:
                continue

            lines.append(f"-- Depth {depth}:")
            for nid in nodes_at_depth:
                node = dag.nodes[nid]
                flags = node.flags if hasattr(node, "flags") and node.flags else []
                refs = list(node.refs) if hasattr(node, "refs") else []

                cost = costs.get(nid)
                cost_pct = (
                    f"{cost.cost_pct:.0f}%"
                    if cost and hasattr(cost, "cost_pct")
                    else "?"
                )

                cols = []
                if node.contract and node.contract.output_columns:
                    cols = node.contract.output_columns[:8]
                cols_str = ", ".join(cols)
                if len(cols) < len(
                    node.contract.output_columns
                    if node.contract and node.contract.output_columns
                    else []
                ):
                    cols_str += ", ..."

                flag_str = f" [{', '.join(flags)}]" if flags else ""
                ref_str = f" ← reads [{', '.join(refs)}]" if refs else ""

                lines.append(
                    f"--   {nid} ({node.node_type}, {cost_pct} cost)"
                    f"{flag_str}{ref_str}"
                )
                if cols_str:
                    lines.append(f"--     outputs: [{cols_str}]")

        # Edges
        if dag.edges:
            lines.append("-- Edges:")
            for edge in dag.edges[:20]:
                src = edge.source if hasattr(edge, "source") else edge[0]
                tgt = edge.target if hasattr(edge, "target") else edge[1]
                lines.append(f"--   {src} → {tgt}")

        return "\n".join(lines)

    @staticmethod
    def _section_full_sql(query_id: str, sql: str, dialect: str) -> str:
        """Section 2: Full Query SQL with DAG topology stripped of comments."""
        # Strip existing comments
        clean_sql = Prompter._strip_comments(sql)

        # Pretty-print
        try:
            import sqlglot
            clean_sql = sqlglot.transpile(
                clean_sql, read=dialect, write=dialect, pretty=True
            )[0]
        except Exception:
            pass

        return (
            f"## Query: {query_id}\n"
            f"\n"
            f"```sql\n"
            f"{clean_sql}\n"
            f"```"
        )

    @staticmethod
    def _section_dag_topology(dag: Any, costs: Dict[str, Any]) -> str:
        """Section 3: DAG Topology as SQL comments."""
        dag_comments = Prompter._build_dag_comments(dag, costs)
        return (
            f"## DAG Topology\n"
            f"\n"
            f"```sql\n"
            f"{dag_comments}\n"
            f"```"
        )

    @staticmethod
    def _section_performance(dag: Any, costs: Dict[str, Any]) -> str:
        """Section 4: Performance Profile — per-node cost breakdown."""
        lines = ["## Performance Profile", ""]

        for nid, node in dag.nodes.items():
            cost = costs.get(nid)
            if not cost:
                continue

            cost_pct = cost.cost_pct if hasattr(cost, "cost_pct") else 0
            operators = cost.operators if hasattr(cost, "operators") else []
            row_est = cost.row_estimate if hasattr(cost, "row_estimate") else 0

            lines.append(f"**{nid}**: {cost_pct:.0f}% of total cost, ~{row_est:,} rows")
            if operators:
                ops_str = ", ".join(operators[:5])
                lines.append(f"  operators: {ops_str}")

        return "\n".join(lines)

    @staticmethod
    def _section_history(history: Dict[str, Any]) -> str:
        """Section 5: History of previous attempts with promotion context.

        If the query was promoted from a previous state, includes:
        - The original SQL before optimization
        - The optimized SQL that achieved the speedup
        - LLM analysis of what the transform did and why it worked
        - Suggestions for further optimization
        """
        lines = ["## Optimization History", ""]

        # Promotion context (most valuable — shows what already worked)
        promotion = history.get("promotion")
        if isinstance(promotion, PromotionAnalysis):
            lines.append(
                f"### Previous Winning Optimization "
                f"(State {promotion.state_promoted_from} → {promotion.speedup:.2f}x)"
            )
            lines.append("")
            lines.append(
                f"**Transforms applied:** {', '.join(promotion.transforms)}"
            )
            lines.append("")
            lines.append("**Original SQL (BEFORE optimization):**")
            lines.append(f"```sql\n{promotion.original_sql}\n```")
            lines.append("")
            lines.append(
                f"**Current SQL (AFTER optimization, {promotion.speedup:.2f}x faster):**"
            )
            lines.append(f"```sql\n{promotion.optimized_sql}\n```")
            lines.append("")
            lines.append("**Analysis — what the transform did and why it worked:**")
            lines.append(promotion.analysis)
            lines.append("")
            lines.append("**Suggestions — further optimization opportunities:**")
            lines.append(promotion.suggestions)
            lines.append("")
            lines.append(
                "Your task: build on this success. The current SQL above is your "
                "starting point. Apply the suggested optimizations or find new "
                "opportunities the previous round missed."
            )
            lines.append("")

        # Previous attempts summary (what was tried, what failed)
        attempts = history.get("attempts", [])
        if attempts:
            lines.append("### All Previous Attempts")
            lines.append("")
            for attempt in attempts[-5:]:
                status = attempt.get("status", "unknown")
                transforms = attempt.get("transforms", [])
                speedup = attempt.get("speedup", 0)
                error = attempt.get("error", "")

                t_str = ", ".join(transforms) if transforms else "unknown"
                if status in ("error", "ERROR"):
                    lines.append(f"- {t_str}: ERROR — {error}")
                elif speedup < 0.95:
                    lines.append(
                        f"- {t_str}: REGRESSION ({speedup:.2f}x), reverted"
                    )
                elif speedup >= 1.10:
                    lines.append(f"- {t_str}: **{speedup:.2f}x improvement** ✓")
                else:
                    lines.append(f"- {t_str}: {speedup:.2f}x (neutral)")

        return "\n".join(lines)

    @staticmethod
    def _section_pattern_hints(annotation: AnnotationResult) -> str:
        """Section 6: Pattern Hints from Phase 2 annotation."""
        lines = ["## Suggested Rewrite Strategy", ""]

        if not annotation.rewrites:
            lines.append("No specific patterns identified. Use your judgment.")
            return "\n".join(lines)

        lines.append(
            "Phase 2 analysis identified these optimization opportunities:"
        )
        lines.append("")

        for ann in annotation.rewrites:
            lines.append(f"- **{ann.node_id}** → apply **{ann.pattern}**")
            lines.append(f"  {ann.rationale}")

        if annotation.skipped:
            lines.append("")
            lines.append("Nodes not flagged (low cost or no opportunity):")
            for sk in annotation.skipped:
                lines.append(f"- {sk.node_id}: {sk.reason}")

        return "\n".join(lines)

    @staticmethod
    def _section_examples(examples: List[Dict[str, Any]]) -> str:
        """Section 7: Up to 3 contrastive BEFORE/AFTER examples (FAISS-matched)."""
        lines = ["## Reference Examples"]

        for i, example in enumerate(examples):
            pattern_name = (
                example.get("id")
                or example.get("name")
                or f"example_{i+1}"
            )
            speedup = example.get("verified_speedup", "")
            speedup_str = f" ({speedup})" if speedup else ""

            lines.append("")
            lines.append(f"### {i+1}. {pattern_name}{speedup_str}")

            ex = example.get("example", example)

            # BEFORE (slow)
            before_sql = ex.get("input_slice") or ex.get("before_sql", "")
            if not before_sql:
                inp = example.get("input", {})
                before_sql = inp.get("sql", "")
            if before_sql:
                lines.append("")
                lines.append("**BEFORE (slow):**")
                lines.append(f"```sql\n{before_sql}\n```")

            # Key insight
            insight = ex.get("key_insight") or example.get("key_insight", "")
            if insight:
                lines.append(f"\n**Key insight:** {insight}")

            # AFTER (fast)
            output = ex.get("output", example.get("output", {}))
            rewrite_sets = output.get("rewrite_sets", [])
            if rewrite_sets and rewrite_sets[0].get("nodes"):
                nodes = rewrite_sets[0]["nodes"]
                lines.append("")
                lines.append("**AFTER (fast):**")
                for nid, sql in nodes.items():
                    lines.append(f"[{nid}]:")
                    lines.append(f"```sql\n{sql}\n```")
            else:
                out = example.get("output", {})
                out_sql = out.get("sql", "")
                if out_sql:
                    lines.append("")
                    lines.append("**AFTER (fast):**")
                    lines.append(f"```sql\n{out_sql}\n```")

        return "\n".join(lines)

    @staticmethod
    def _section_constraints() -> str:
        """Section 8: Constraints (sandwich ordered)."""
        return (
            "## Constraints\n"
            "\n"
            "### CRITICAL — Correctness Guards (top of sandwich)\n"
            "\n"
            "**SEMANTIC_EQUIVALENCE**\n"
            "The rewritten query MUST return exactly the same rows, columns, and\n"
            "ordering as the original. This is the prime directive.\n"
            "\n"
            "**LITERAL_PRESERVATION**\n"
            "Keep all literal values (dates, strings, numbers) exactly as they appear in\n"
            "the original SQL. Do not round, truncate, or reformat them.\n"
            "\n"
            "### HIGH — Performance and Style Rules (middle of sandwich)\n"
            "\n"
            "**NO_UNFILTERED_DIM_CTE**\n"
            "When creating a new CTE that scans a dimension table, include at least one\n"
            "filter predicate. Never materialize an entire dimension without a WHERE clause.\n"
            "\n"
            "**OR_TO_UNION_LIMIT**\n"
            "When converting OR predicates to UNION ALL, limit to 4 branches maximum.\n"
            "Beyond 4, the UNION overhead exceeds the OR scan cost for most planners.\n"
            "\n"
            "**EXPLICIT_JOINS**\n"
            "Convert comma-separated implicit joins to explicit JOIN ... ON syntax.\n"
            "This gives the optimizer better join-order freedom.\n"
            "\n"
            "### CRITICAL — Correctness Guards (bottom of sandwich)\n"
            "\n"
            "**KEEP_EXISTS_AS_EXISTS**\n"
            "Preserve EXISTS/NOT EXISTS subqueries as-is. Do not convert them to\n"
            "IN/NOT IN or to JOINs — this risks NULL-handling semantic changes.\n"
            "\n"
            "**COMPLETE_OUTPUT**\n"
            "The rewritten query must output ALL columns from the original SELECT.\n"
            "Never drop, rename, or reorder output columns."
        )

    @staticmethod
    def _section_output_format() -> str:
        """Section 9: Output Format (RECENCY position)."""
        return (
            "## Output\n"
            "\n"
            "Return the complete rewritten SQL query. The query must be syntactically\n"
            "valid and ready to execute.\n"
            "\n"
            "```sql\n"
            "-- Your rewritten query here\n"
            "```\n"
            "\n"
            "After the SQL, briefly explain what you changed:\n"
            "\n"
            "```\n"
            "Changes: <1-2 sentence summary of the rewrite>\n"
            "Expected speedup: <estimate>\n"
            "```\n"
            "\n"
            "Now output your rewritten SQL:"
        )

    # =========================================================================
    # Utility: Load and match examples
    # =========================================================================

    @staticmethod
    def load_example_for_pattern(
        pattern_name: str,
        engine: str = "duckdb",
        seed_dirs: Optional[List[Path]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Load a gold example matching the given pattern name.

        Search order (same engine only — never mix DBs):
        1. ado/examples/<engine>/  (gold verified examples for this DB)
        2. seed_dirs               (state_0/seed/ generic catalog rules)

        Args:
            pattern_name: Pattern to match (e.g., "decorrelate")
            engine: Database engine ("duckdb" | "postgres")
            seed_dirs: Optional list of state_0/seed/ paths for catalog rules
        """
        engine_dir = "postgres" if engine in ("postgresql", "postgres") else engine

        search_dirs = []
        primary = EXAMPLES_DIR / engine_dir
        if primary.exists():
            search_dirs.append(primary)
        if seed_dirs:
            search_dirs.extend(seed_dirs)

        for search_dir in search_dirs:
            if not search_dir.exists():
                continue

            path = search_dir / f"{pattern_name}.json"
            if path.exists():
                try:
                    return json.loads(path.read_text())
                except Exception:
                    pass

            for p in sorted(search_dir.glob("*.json")):
                try:
                    data = json.loads(p.read_text())
                    if data.get("id", "").lower() == pattern_name.lower():
                        return data
                    if pattern_name.lower() in data.get("name", "").lower():
                        return data
                except Exception:
                    continue

        return None

    @staticmethod
    def build_edge_contract_from_node(node) -> EdgeContract:
        """Build an EdgeContract from a DAG node's contract."""
        if node.contract:
            return EdgeContract(
                columns=node.contract.output_columns or [],
                grain=(
                    ", ".join(node.contract.grain)
                    if node.contract.grain
                    else "not specified"
                ),
                filters=[
                    p for p in (node.contract.required_predicates or [])
                ],
                cardinality_estimate=(
                    node.cost.row_estimate
                    if node.cost
                    else None
                ),
            )
        return EdgeContract(
            columns=[], grain="not specified", filters=[]
        )
