"""Phase 3: Query-level rewrite prompt builder.

Builds attention-optimized full-query rewrite prompts with DAG topology.
All rewrites are full-query scope — the LLM sees the complete SQL, the DAG
structure, and FAISS-matched gold examples.

Section ordering (attention-optimized):
1. Role + Task          (PRIMACY - frames rewrite mindset)
2. Full Query SQL       (PRIMACY - pretty-formatted, complete query)
3. DAG Topology         (PRIMACY - nodes, edges, depths, flags, costs)
4. Performance Profile  (EARLY - per-node costs, bottleneck operators)
5. History              (EARLY-MID - previous attempts on this query)
5b. Global Learnings    (EARLY-MID - aggregate benchmark stats, optional)
6. Examples             (MIDDLE - FAISS-matched contrastive BEFORE/AFTER pairs)
6b. Regression Warnings (MIDDLE - FAISS-matched anti-patterns from past regressions)
7. Constraints          (LATE-MID - sandwich: CRITICAL top/bottom, HIGH middle)
8. Output Format        (RECENCY - return complete rewritten SQL)
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .schemas import EdgeContract, PromotionAnalysis

logger = logging.getLogger(__name__)

# Directory paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"          # ado/examples/{duckdb,postgres}/
CONSTRAINTS_DIR = BASE_DIR / "constraints"


def compute_depths(dag) -> Dict[str, int]:
    """Compute topological depth for each node in the DAG."""
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


def _load_constraint_files() -> List[Dict[str, Any]]:
    """Load all constraint JSON files from CONSTRAINTS_DIR.

    Returns list of constraint dicts sorted by severity (CRITICAL first,
    then HIGH, then MEDIUM).
    """
    if not CONSTRAINTS_DIR.exists():
        return []

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
    constraints = []

    for path in sorted(CONSTRAINTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            if "id" in data and "prompt_instruction" in data:
                constraints.append(data)
        except Exception as e:
            logger.warning(f"Failed to load constraint {path}: {e}")

    constraints.sort(key=lambda c: severity_order.get(c.get("severity", "MEDIUM"), 2))
    return constraints



def _build_node_intent_map(
    semantic_intents: Optional[Dict[str, Any]],
) -> Dict[str, str]:
    """Build {node_id: intent_string} from semantic_intents data."""
    if not semantic_intents:
        return {}
    result: Dict[str, str] = {}
    for node in semantic_intents.get("dag_nodes", []):
        nid = node.get("node_id", "")
        intent = node.get("intent", "")
        if nid and intent:
            result[nid] = intent
    return result


class Prompter:
    """Build attention-optimized full-query rewrite prompts with DAG context.

    The LLM sees:
    - Complete query SQL (not isolated nodes)
    - Full DAG topology (nodes, edges, depths, flags, costs)
    - FAISS-matched gold examples (contrastive BEFORE/AFTER pairs)
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
        history: Optional[Dict[str, Any]] = None,
        examples: Optional[List[Dict[str, Any]]] = None,
        expert_analysis: Optional[str] = None,
        global_learnings: Optional[Dict[str, Any]] = None,
        regression_warnings: Optional[List[Dict[str, Any]]] = None,
        dialect: str = "duckdb",
        semantic_intents: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Build the full-query rewrite prompt.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            full_sql: Complete original SQL query
            dag: Parsed DAG from Phase 1 (DagBuilder output)
            costs: Per-node cost analysis from CostAnalyzer
            history: Previous attempts and promotion context for this query.
                     Dict with 'attempts' (list) and 'promotion' (PromotionAnalysis).
            examples: List of gold examples (FAISS-matched, up to 3)
            expert_analysis: Pre-computed LLM analyst output (analyst mode only).
                             When present, replaces examples with concrete
                             structural guidance.
            global_learnings: Aggregate learnings from benchmark runs (from
                              Learner.build_learning_summary()). Shows transform
                              effectiveness, known anti-patterns, example success rates.
            regression_warnings: FAISS-matched regression examples showing
                                 structurally similar queries that regressed.
                                 Displayed as anti-patterns so the LLM avoids them.
            dialect: SQL dialect for pretty-printing
            semantic_intents: Pre-computed LLM-generated query and per-node intents.
                              Dict with 'query_intent' (str) and 'dag_nodes' (list).
        """
        sections = []

        # Section 1: Role + Task (PRIMACY)
        sections.append(self._section_role_task())

        # Section 2: Full Query SQL (PRIMACY)
        sections.append(self._section_full_sql(query_id, full_sql, dialect))

        # Section 3+4: Query Structure (DAG) — unified gold standard format
        sections.append(self._section_query_structure(
            dag, costs, dialect, semantic_intents=semantic_intents,
        ))

        # Section 5: History (EARLY-MID)
        if history:
            sections.append(self._section_history(history))

        # Section 5b: Global Learnings (EARLY-MID, after history)
        if global_learnings:
            gl_section = self._section_global_learnings(global_learnings)
            if gl_section:
                sections.append(gl_section)

        if expert_analysis:
            # Analyst mode: inject analysis AND examples
            # Analysis tells the rewriter WHAT to do, examples show HOW
            sections.append(expert_analysis)
        # Gold examples (FAISS-matched or analyst-overridden)
        if examples:
            sections.append(self._section_examples(examples))

        # Section 6b: Regression warnings (after examples, before constraints)
        if regression_warnings:
            sections.append(self._section_regression_warnings(regression_warnings))

        # Section 7: Constraints (LATE-MID, sandwich ordered)
        sections.append(self._section_constraints())

        # Section 8: Output Format (RECENCY) — includes column completeness contract
        output_columns = self._extract_output_columns(dag)
        sections.append(self._section_output_format(output_columns=output_columns))

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
            "subqueries connect, cost analysis per node, and reference examples of\n"
            "proven rewrites on structurally similar queries.\n"
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
        depths = compute_depths(dag)

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
    def _section_query_structure(
        dag: Any, costs: Dict[str, Any], dialect: str = "duckdb",
        semantic_intents: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Sections 3+4: Query Structure (DAG) — gold standard card format.

        Reuses the shared format from analyst.py so both prompts
        present the same structured view. When semantic_intents are
        available, annotates each DAG node with its LLM-generated intent.
        """
        from .analyst import _append_dag_analysis

        lines = ["## Query Structure (DAG)", ""]

        # Merge query-level intent into main_query's node intent
        node_intents = _build_node_intent_map(semantic_intents)
        if semantic_intents:
            query_intent = semantic_intents.get("query_intent", "")
            if query_intent and "main_query" not in node_intents:
                node_intents["main_query"] = query_intent

        _append_dag_analysis(
            lines, dag, costs, dialect=dialect,
            node_intents=node_intents,
        )
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
    def _section_global_learnings(learnings: Dict[str, Any]) -> str:
        """Section 5b: Global learnings from benchmark runs.

        Shows transform effectiveness, known anti-patterns, and example
        success rates to help the LLM make informed rewrite choices.
        """
        if not learnings or not learnings.get("total_attempts"):
            return ""

        lines = ["## Benchmark Learnings", ""]

        # Transform effectiveness (top transforms by success rate + avg speedup)
        transform_eff = learnings.get("transform_effectiveness", {})
        if transform_eff:
            # Sort by avg_speedup descending, filter to those with >= 2 attempts
            ranked = sorted(
                [
                    (name, stats)
                    for name, stats in transform_eff.items()
                    if stats.get("attempts", 0) >= 2
                ],
                key=lambda x: -x[1].get("avg_speedup", 0),
            )

            if ranked:
                lines.append("### Effective Transforms")
                for name, stats in ranked[:8]:
                    rate = stats.get("success_rate", 0)
                    avg = stats.get("avg_speedup", 0)
                    n = stats.get("attempts", 0)
                    lines.append(
                        f"- **{name}**: {rate:.0%} success rate, "
                        f"{avg:.2f}x avg speedup ({n} attempts)"
                    )
                lines.append("")

            # Known anti-patterns: transforms with low success rate or low speedup
            anti = [
                (name, stats)
                for name, stats in transform_eff.items()
                if stats.get("attempts", 0) >= 2
                and stats.get("success_rate", 1) < 0.3
            ]
            if anti:
                lines.append("### Known Anti-Patterns (avoid these)")
                for name, stats in anti:
                    rate = stats.get("success_rate", 0)
                    n = stats.get("attempts", 0)
                    lines.append(
                        f"- **{name}**: {rate:.0%} success rate "
                        f"({n} attempts) — usually causes regressions"
                    )
                lines.append("")

        # Example effectiveness
        example_eff = learnings.get("example_effectiveness", {})
        if example_eff:
            ranked_ex = sorted(
                [
                    (name, stats)
                    for name, stats in example_eff.items()
                    if stats.get("times_recommended", 0) >= 2
                ],
                key=lambda x: -x[1].get("effectiveness", 0),
            )
            if ranked_ex:
                lines.append("### Example Effectiveness")
                for name, stats in ranked_ex[:6]:
                    eff = stats.get("effectiveness", 0)
                    n = stats.get("times_recommended", 0)
                    lines.append(
                        f"- **{name}**: {eff:.0%} led to success "
                        f"({n} recommendations)"
                    )
                lines.append("")

        # Error patterns summary
        error_patterns = learnings.get("error_patterns", {})
        if error_patterns:
            lines.append("### Common Error Patterns")
            for category, info in error_patterns.items():
                count = info.get("count", 0)
                lines.append(f"- **{category}**: {count} occurrences")
            lines.append("")

        # Only return if we have content beyond the header
        if len(lines) <= 2:
            return ""

        return "\n".join(lines)

    @staticmethod
    def _section_examples(examples: List[Dict[str, Any]]) -> str:
        """Section 7: Up to 3 contrastive BEFORE/AFTER examples (FAISS-matched)."""
        lines = [
            "## Reference Examples",
            "",
            "The following examples are for **pattern reference only**. Do not copy "
            "their table names, column names, or literal values into your rewrite. "
            "Use only the schema and tables from the target query above.",
        ]

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

            # Principle — the abstract optimization reasoning
            principle = example.get("principle", "")
            if principle:
                lines.append(f"\n**Principle:** {principle}")

            # BEFORE (slow) — prefer complete original_sql over abbreviated input_slice
            before_sql = (
                example.get("original_sql")
                or ex.get("before_sql")
                or ex.get("input_slice")
                or ""
            )
            if not before_sql:
                inp = example.get("input", {})
                before_sql = inp.get("sql", "")
            if before_sql:
                lines.append("")
                lines.append("**BEFORE (slow):**")
                lines.append(f"```sql\n{before_sql}\n```")

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
    def _section_regression_warnings(
        regressions: List[Dict[str, Any]],
    ) -> str:
        """Section 6b: Regression warnings for structurally similar queries.

        Shows BEFORE/AFTER pairs from past regressions where a structurally
        similar query was rewritten and got SLOWER. The LLM sees:
        - The original query that was similar to this one
        - The rewrite that caused the regression
        - Why it regressed (regression_mechanism)

        This prevents the LLM from repeating the same mistakes on similar SQL.
        """
        if not regressions:
            return ""

        lines = [
            "## Regression Warnings",
            "",
            "The following rewrites were attempted on **structurally similar queries**",
            "and caused **performance regressions**. Do NOT repeat these patterns.",
            "",
        ]

        for i, reg in enumerate(regressions):
            reg_id = reg.get("id", f"regression_{i + 1}")
            speedup = reg.get("verified_speedup", "?")
            query_id = reg.get("query_id", "?")
            transform = reg.get("transform_attempted", "unknown")
            mechanism = reg.get("regression_mechanism", "")
            description = reg.get("description", "")

            lines.append(
                f"### Warning {i + 1}: {reg_id} ({speedup} — REGRESSION)"
            )
            lines.append(
                f"**Query:** {query_id} | **Transform attempted:** {transform}"
            )
            lines.append("")

            if description:
                lines.append(f"**Anti-pattern:** {description}")
                lines.append("")

            # Show the original SQL (the similar query)
            original_sql = reg.get("original_sql", "")
            if original_sql:
                # Truncate very long SQL to keep prompt manageable
                if len(original_sql) > 2000:
                    original_sql = original_sql[:2000] + "\n-- ... (truncated)"
                lines.append("**Original query (similar to yours):**")
                lines.append(f"```sql\n{original_sql}\n```")
                lines.append("")

            # Show the regressed rewrite (what NOT to do)
            example = reg.get("example", {})
            after_sql = example.get("after_sql", "")
            if after_sql:
                if len(after_sql) > 2000:
                    after_sql = after_sql[:2000] + "\n-- ... (truncated)"
                lines.append("**Regressed rewrite (DO NOT replicate this pattern):**")
                lines.append(f"```sql\n{after_sql}\n```")
                lines.append("")

            # Why it regressed
            if mechanism:
                lines.append(f"**Why it regressed:** {mechanism}")
                lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _section_constraints() -> str:
        """Section 8: Constraints (sandwich ordered).

        Loads constraints from ado/constraints/*.json and groups by severity:
        - CRITICAL at top and bottom (sandwich pattern for attention)
        - HIGH + MEDIUM in the middle
        """
        constraints = _load_constraint_files()

        critical = [c for c in constraints if c.get("severity") == "CRITICAL"]
        high = [c for c in constraints if c.get("severity") == "HIGH"]
        medium = [c for c in constraints if c.get("severity") == "MEDIUM"]

        lines = ["## Constraints", ""]

        # Top sandwich: CRITICAL
        if critical:
            lines.append("### CRITICAL — Correctness Guards (top of sandwich)")
            for c in critical:
                lines.append(f"\n**{c['id']}**")
                lines.append(c.get("prompt_instruction", c.get("description", "")))

        # Middle: HIGH + MEDIUM
        if high or medium:
            lines.append("\n### HIGH — Performance and Style Rules (middle of sandwich)")
            for c in high + medium:
                lines.append(f"\n**{c['id']}**")
                lines.append(c.get("prompt_instruction", c.get("description", "")))

        # Bottom sandwich: repeat CRITICAL IDs
        if critical:
            lines.append("\n### CRITICAL — Correctness Guards (bottom of sandwich)")
            for c in critical:
                lines.append(f"\n**{c['id']}**")
                lines.append(c.get("prompt_instruction", c.get("description", "")))

        return "\n".join(lines)

    @staticmethod
    def _extract_output_columns(dag: Any) -> List[str]:
        """Extract the final output columns from the main_query node's contract."""
        main = dag.nodes.get("main_query")
        if main and hasattr(main, "contract") and main.contract:
            cols = main.contract.output_columns
            if cols:
                return list(cols)
        # Fallback: try the last node in definition order
        for nid in reversed(list(dag.nodes)):
            node = dag.nodes[nid]
            if hasattr(node, "contract") and node.contract and node.contract.output_columns:
                return list(node.contract.output_columns)
        return []

    @staticmethod
    def _section_output_format(
        output_columns: Optional[List[str]] = None,
    ) -> str:
        """Section 9: Output Format (RECENCY position).

        Includes a column completeness contract when output_columns
        are available, so the LLM knows every column the rewritten
        query must produce.
        """
        lines = [
            "## Output",
            "",
            "Return the complete rewritten SQL query. The query must be syntactically",
            "valid and ready to execute.",
        ]

        # Column completeness contract (RECENCY — LLM sees this last)
        if output_columns:
            lines.append("")
            lines.append("### Column Completeness Contract")
            lines.append("")
            lines.append(
                "Your rewritten query MUST produce **exactly** these output columns "
                "(same names, same order):"
            )
            lines.append("")
            for i, col in enumerate(output_columns, 1):
                lines.append(f"  {i}. `{col}`")
            lines.append("")
            lines.append(
                "Do NOT add, remove, or rename any columns. "
                "The result set schema must be identical to the original query."
            )

        lines.extend([
            "",
            "```sql",
            "-- Your rewritten query here",
            "```",
            "",
            "After the SQL, briefly explain what you changed:",
            "",
            "```",
            "Changes: <1-2 sentence summary of the rewrite>",
            "Expected speedup: <estimate>",
            "```",
            "",
            "Now output your rewritten SQL:",
        ])

        return "\n".join(lines)

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
