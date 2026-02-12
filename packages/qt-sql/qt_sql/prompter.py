"""Phase 3: Query-level rewrite prompt builder.

Builds attention-optimized full-query rewrite prompts with logical-tree topology.
All rewrites are full-query scope — the LLM sees the complete SQL, the logical tree
structure, and tag-matched gold examples.

Section ordering (attention-optimized):
1. Role + Task          (PRIMACY - frames rewrite mindset)
2. Full Query SQL       (PRIMACY - pretty-formatted, complete query)
3. Logical Tree Topology (PRIMACY - nodes, edges, depths, flags, costs)
4. Performance Profile  (EARLY - per-node costs, bottleneck operators)
5. History              (EARLY-MID - previous attempts on this query)
5b. Global Learnings    (EARLY-MID - aggregate benchmark stats, optional)
6. Examples             (MIDDLE - tag-matched contrastive BEFORE/AFTER pairs)
6b. Regression Warnings (MIDDLE - tag-matched anti-patterns from past regressions)
7. Constraints          (LATE-MID - CRITICAL top/bottom, HIGH middle)
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
EXAMPLES_DIR = BASE_DIR / "examples"
CONSTRAINTS_DIR = BASE_DIR / "constraints"


def compute_depths(dag) -> Dict[str, int]:
    """Compute topological depth for each node in the logical tree."""
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


def load_exploit_algorithm(dialect: str = "duckdb") -> Optional[str]:
    """Load the master distilled intelligence for the target dialect.

    Each engine has a single master intelligence document in
    ``knowledge/{dialect}.md`` — the authoritative synthesis of all
    benchmarking, transform detection, and evidence-based distillation.

    Returns the raw markdown text or None if not available.
    """
    KNOWLEDGE_DIR = Path(__file__).resolve().parent / "knowledge"

    norm = dialect.lower()
    if norm in ("postgres", "pg"):
        norm = "postgresql"

    algo_path = KNOWLEDGE_DIR / f"{norm}.md"
    if algo_path.exists():
        try:
            return algo_path.read_text()
        except Exception as e:
            logger.warning(f"Failed to load master intelligence {algo_path}: {e}")
    return None


def _load_engine_profile(dialect: str = "duckdb") -> Optional[Dict[str, Any]]:
    """Load the engine profile JSON for the target dialect.

    Engine profiles describe what the optimizer handles well (don't fight)
    and where its gaps are (exploit these). This is the primary guidance
    for transform selection.
    """
    if not CONSTRAINTS_DIR.exists():
        return None

    norm = dialect.lower()
    if norm in ("postgres", "pg"):
        norm = "postgresql"

    profile_path = CONSTRAINTS_DIR / f"engine_profile_{norm}.json"
    if not profile_path.exists():
        logger.warning(
            "Engine profile missing for dialect '%s' at %s",
            norm,
            profile_path,
        )
        return None

    try:
        data = json.loads(profile_path.read_text())
        if data.get("profile_type") != "engine_profile":
            logger.warning(
                "Invalid engine profile type in %s: %r",
                profile_path,
                data.get("profile_type"),
            )
            return None

        profile_engine = str(data.get("engine", "")).lower()
        if profile_engine and profile_engine != norm:
            logger.warning(
                "Engine profile mismatch in %s: expected '%s', got '%s'",
                profile_path,
                norm,
                profile_engine,
            )
            return None

        return data
    except Exception as e:
        logger.warning(f"Failed to load engine profile {profile_path}: {e}")
        return None


def _load_constraint_files(dialect: str = "duckdb") -> List[Dict[str, Any]]:
    """Load constraint JSON files from CONSTRAINTS_DIR, filtered by engine.

    Each constraint may have an ``"engine"`` field (e.g. ``"postgresql"``).
    - If ``"engine"`` is absent, the constraint is universal (always loaded).
    - If ``"engine"`` is present, it's only loaded when ``dialect`` matches.
    - Engine profile files (profile_type=engine_profile) are excluded.

    Returns list of constraint dicts sorted by severity (CRITICAL first,
    then HIGH, then MEDIUM).
    """
    if not CONSTRAINTS_DIR.exists():
        return []

    # Normalize dialect for matching (e.g. "postgres" -> "postgresql")
    norm = dialect.lower()
    if norm in ("postgres", "pg"):
        norm = "postgresql"

    severity_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}
    constraints = []

    for path in sorted(CONSTRAINTS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text())
            # Skip engine profile files — they're loaded separately
            if data.get("profile_type") == "engine_profile":
                continue
            if "id" not in data or "prompt_instruction" not in data:
                continue
            engine = data.get("engine")
            if engine and engine.lower() != norm:
                continue  # skip constraints for other engines
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
    """Build attention-optimized full-query rewrite prompts with logical-tree context.

    The LLM sees:
    - Complete query SQL (not isolated nodes)
    - Full logical-tree topology (nodes, edges, depths, flags, costs)
    - tag-matched gold examples (contrastive BEFORE/AFTER pairs)
    - Constraints (CRITICAL top/bottom, HIGH middle)

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
        engine_version: Optional[str] = None,
    ) -> str:
        """Build the full-query rewrite prompt.

        Args:
            query_id: Query identifier (e.g., 'query_1')
            full_sql: Complete original SQL query
            dag: Parsed logical tree from Phase 1
            costs: Per-node cost analysis from CostAnalyzer
            history: Previous attempts and promotion context for this query.
                     Dict with 'attempts' (list) and 'promotion' (PromotionAnalysis).
            examples: List of gold examples (tag-matched, up to 3)
            expert_analysis: Pre-computed LLM analyst output (analyst mode only).
                             When present, replaces examples with concrete
                             structural guidance.
            global_learnings: Aggregate learnings from benchmark runs (from
                              Learner.build_learning_summary()). Shows transform
                              effectiveness, known anti-patterns, example success rates.
            regression_warnings: tag-matched regression examples showing
                                 structurally similar queries that regressed.
                                 Displayed as anti-patterns so the LLM avoids them.
            dialect: SQL dialect for pretty-printing
            semantic_intents: Pre-computed LLM-generated query and per-node intents.
                              Dict with 'query_intent' (str) and 'dag_nodes' (list).
            engine_version: Engine version string (e.g., '1.4.3' for DuckDB).
        """
        sections = []

        # Section 0: Retry preamble (PRIMACY — if this is a retry, say so FIRST)
        retry_preamble = self._section_retry_preamble(history)
        if retry_preamble:
            sections.append(retry_preamble)

        # Section 1: Role + Task (PRIMACY)
        sections.append(self._section_role_task(dialect, engine_version))

        # Section 2: Full Query SQL (PRIMACY)
        sections.append(self._section_full_sql(query_id, full_sql, dialect))

        # Section 3+4: Query Structure (Logical Tree) — unified gold standard format
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
        # Gold examples (tag-matched or analyst-overridden)
        if examples:
            sections.append(self._section_examples(examples))

        # Section 6b: Regression warnings (after examples, before constraints)
        if regression_warnings:
            sections.append(self._section_regression_warnings(regression_warnings))

        # Section 6c: Filter pushdown heuristic (engine-specific)
        pushdown = self._section_filter_pushdown_heuristic(dialect)
        if pushdown:
            sections.append(pushdown)

        # Section 7: Constraints (LATE-MID, sandwich ordered, engine-filtered)
        sections.append(self._section_constraints(dialect))

        # Section 8: Output Format (RECENCY) — includes column completeness contract
        output_columns = self._extract_output_columns(dag)
        sections.append(self._section_output_format(output_columns=output_columns))

        return "\n\n".join(sections)

    # =========================================================================
    # Section builders
    # =========================================================================

    @staticmethod
    def _section_retry_preamble(history: Optional[Dict[str, Any]]) -> str:
        """Section 0: Retry context — if previous attempts exist, lead with that.

        This goes FIRST in the prompt so the LLM knows it's a retry and
        immediately sees what failed and why.
        """
        if not history:
            return ""

        attempts = history.get("attempts", [])
        if not attempts:
            return ""

        # Only include analyst-iteration attempts (not batch state results)
        failed = [
            a for a in attempts
            if a.get("source", "").startswith("analyst_iter")
            and a.get("status") in ("ERROR", "FAIL", "error")
        ]
        if not failed:
            return ""

        n_attempts = len(attempts)
        lines = [
            f"## RETRY — This is attempt {n_attempts + 1}",
            "",
            f"The previous {len(failed)} attempt(s) on this query FAILED.",
            "You MUST use a different approach. Do NOT repeat the same structural mistake.",
            "",
        ]

        for a in failed[-3:]:  # Show last 3 failures max
            transforms = ", ".join(a.get("transforms", [])) or "unknown"
            status = a.get("status", "ERROR")
            error_msgs = a.get("error_messages", [])
            error_cat = a.get("error_category", "")
            fa = a.get("failure_analysis", "")

            lines.append(f"**Attempt ({transforms}): {status}**")

            # Raw error messages — the concrete validation failures
            if error_msgs:
                cat_label = f" [{error_cat}]" if error_cat else ""
                lines.append(f"Error{cat_label}:")
                for msg in error_msgs[:5]:
                    lines.append(f"  - {msg[:200]}")

            # Expert analysis — LLM commentary on root cause + next steps
            if fa:
                summary = fa[:400].strip()
                if len(fa) > 400:
                    summary += "..."
                lines.append(f"Expert analysis: {summary}")

            lines.append("")

        lines.append("---")
        return "\n".join(lines)

    @staticmethod
    def _section_role_task(
        dialect: str = "duckdb",
        engine_version: Optional[str] = None,
    ) -> str:
        """Section 1: Role + Task."""
        engine_names = {
            "duckdb": "DuckDB",
            "postgres": "PostgreSQL",
            "postgresql": "PostgreSQL",
            "snowflake": "Snowflake",
        }
        engine = engine_names.get(dialect, dialect)
        ver = f" v{engine_version}" if engine_version else ""
        base = (
            f"You are a SQL query rewrite engine for {engine}{ver}.\n"
            "\n"
            "Your goal: rewrite the complete SQL query to maximize execution speed\n"
            "while preserving exact semantic equivalence (same rows, same columns,\n"
            "same ordering).\n"
            "\n"
            "You will receive the full query, its logical tree structure showing how CTEs and\n"
            "subqueries connect, cost analysis per node, and reference examples of\n"
            "proven rewrites on structurally similar queries.\n"
            "You may restructure the query freely: create new CTEs, merge existing ones,\n"
            "push filters across node boundaries, or decompose subqueries."
        )

        # Engine-specific hints
        if dialect == "duckdb":
            base += (
                "\n\n"
                "### DuckDB Engine Characteristics\n"
                "\n"
                "- **Columnar storage**: SELECT only the columns you need. Removing "
                "unused columns from intermediate CTEs reduces memory and speeds scans.\n"
                "- **CTE inlining**: DuckDB inlines CTEs by default (no materialization "
                "fence). A CTE referenced once is zero-cost. A CTE referenced multiple "
                "times may be re-executed — consider whether re-execution or explicit "
                "materialization is cheaper.\n"
                "- **FILTER clause**: Use `COUNT(*) FILTER (WHERE cond)` instead of "
                "`SUM(CASE WHEN cond THEN 1 ELSE 0 END)`. FILTER is native syntax, "
                "skips branch evaluation, and enables better vectorized execution.\n"
                "- **Predicate pushdown limits**: The optimizer pushes simple predicates "
                "into base table scans but CANNOT push filters through multi-level CTEs, "
                "window functions, or UNION ALL. You must move filters inside manually.\n"
                "- **Hash joins**: DuckDB uses hash joins for most equi-joins. Build the "
                "smaller side first — put the filtered dimension CTE on the build side."
            )

        return base

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
    def _build_logical_tree_comments(dag: Any, costs: Dict[str, Any]) -> str:
        """Build logical-tree topology as SQL comments to embed in the query."""
        depths = compute_depths(dag)

        lines = ["-- LOGICAL TREE TOPOLOGY"]

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
        """Section 2: Full query SQL with topology comments stripped."""
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
        """Sections 3+4: Query Structure (Logical Tree) — gold standard card format.

        Reuses the shared format from analyst.py so both prompts
        present the same structured view. When semantic_intents are
        available, annotates each logical tree node with its LLM-generated intent.
        """
        from .analyst import _append_dag_analysis

        lines = ["## Query Structure (Logical Tree)", ""]

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
                failure_analysis = attempt.get("failure_analysis", "")

                error_msgs = attempt.get("error_messages", [])
                t_str = ", ".join(transforms) if transforms else "unknown"
                if status in ("error", "ERROR", "FAIL"):
                    lines.append(f"- {t_str}: {status} (0.00x)")
                    if error_msgs:
                        for msg in error_msgs[:3]:
                            lines.append(f"  - {msg[:200]}")
                    if failure_analysis:
                        lines.append(f"  **Expert analysis:** {failure_analysis[:500]}")
                    lines.append("")
                elif speedup < 0.95:
                    lines.append(
                        f"- {t_str}: REGRESSION ({speedup:.2f}x), reverted"
                    )
                    if failure_analysis:
                        lines.append("")
                        lines.append(f"  **Why it regressed:** {failure_analysis[:500]}")
                        lines.append("")
                elif speedup >= 1.10:
                    lines.append(f"- {t_str}: **{speedup:.2f}x improvement**")
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
        """Section 7: Up to 3 contrastive BEFORE/AFTER examples (tag-matched)."""
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
            speedup = str(example.get("verified_speedup", "")).rstrip("x")
            speedup_str = f" ({speedup}x)" if speedup else ""

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

        Shows concise anti-pattern descriptions from past regressions.
        No full SQL — just the transform, why it failed, and what to avoid.
        """
        if not regressions:
            return ""

        lines = [
            "## Regression Warnings",
            "",
            "The following transforms were attempted on structurally similar queries",
            "and caused performance regressions. Do NOT repeat these patterns.",
            "",
        ]

        for i, reg in enumerate(regressions):
            speedup = str(reg.get("verified_speedup", "?")).rstrip("x")
            query_id = reg.get("query_id", "?")
            transform = reg.get("transform_attempted", "unknown")
            mechanism = reg.get("regression_mechanism", "")
            description = reg.get("description", "")

            lines.append(
                f"### Warning {i + 1}: {transform} on {query_id} ({speedup}x)"
            )

            if description:
                lines.append(f"**Anti-pattern:** {description}")

            if mechanism:
                lines.append(f"**Why it regressed:** {mechanism}")

            lines.append("")

        return "\n".join(lines)

    @staticmethod
    def _section_filter_pushdown_heuristic(dialect: str = "duckdb") -> str:
        """Section 6c: Explicit filter pushdown rules.

        SQL engines cannot push predicates through multi-level CTEs,
        window functions, or UNION ALL. This section teaches the LLM
        to do it manually — a reliable, low-risk optimization.
        """
        if dialect not in ("duckdb", "postgres", "postgresql"):
            return ""

        return (
            "## Filter Pushdown Heuristic\n"
            "\n"
            "SQL optimizers CANNOT push filters through multi-level CTEs, "
            "window functions, DISTINCT, GROUP BY, or UNION ALL boundaries. "
            "You must move them manually. Apply these rules:\n"
            "\n"
            "1. **Static filter on CTE output → move inside CTE**\n"
            "   If the final SELECT (or an outer CTE) filters on a column produced "
            "by an inner CTE with a literal/constant value, move that WHERE clause "
            "inside the CTE definition. This reduces the intermediate result set.\n"
            "\n"
            "2. **Date/dimension filter → push to earliest scan**\n"
            "   If a date or dimension filter appears in the final query, push it "
            "into the CTE that first joins with the dimension table. Pre-filtering "
            "the dimension table into a small CTE and joining early is even better.\n"
            "\n"
            "3. **Repeated filter across CTEs → extract shared CTE**\n"
            "   If the same filter predicate (e.g., `d_year = 2001`) appears in "
            "multiple subqueries or CTEs, extract the filtered result into one "
            "shared CTE and reference it everywhere.\n"
            "\n"
            "4. **Filter after GROUP BY → push before GROUP BY**\n"
            "   If a HAVING clause or post-aggregation filter can be expressed as "
            "a pre-aggregation WHERE, move it before GROUP BY to reduce rows "
            "entering the aggregation.\n"
            "\n"
            "5. **Filter after window function → cannot push through**\n"
            "   Window functions require the full partition to compute. Do NOT push "
            "filters before window functions unless the filter is on partition columns "
            "only (which narrows partitions without changing results)."
        )

    @staticmethod
    def _section_constraints(dialect: str = "duckdb") -> str:
        """Section 8: Constraints (engine-filtered).

        Loads constraints from qt_sql/constraints/*.json and groups by severity:
        - CRITICAL at top and bottom (repeated for attention)
        - HIGH + MEDIUM in the middle

        Constraints with an ``"engine"`` field are only included when the
        dialect matches. Constraints without ``"engine"`` are universal.
        """
        constraints = _load_constraint_files(dialect)

        critical = [c for c in constraints if c.get("severity") == "CRITICAL"]
        high = [c for c in constraints if c.get("severity") == "HIGH"]
        medium = [c for c in constraints if c.get("severity") == "MEDIUM"]

        lines = ["## Constraints", ""]

        # Top sandwich: CRITICAL
        if critical:
            lines.append("### CRITICAL — Correctness Guards")
            for c in critical:
                lines.append(f"\n**{c['id']}**")
                lines.append(c.get("prompt_instruction", c.get("description", "")))

        # Middle: HIGH + MEDIUM
        if high or medium:
            lines.append("\n### HIGH — Performance and Style Rules")
            for c in high + medium:
                lines.append(f"\n**{c['id']}**")
                lines.append(c.get("prompt_instruction", c.get("description", "")))

        # Bottom sandwich: repeat CRITICAL IDs
        if critical:
            lines.append("\n### CRITICAL — Correctness Guards (repeated for emphasis)")
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
        1. qt_sql/examples/<engine>/  (gold verified examples for this DB)
        2. seed_dirs                  (state_0/seed/ generic catalog rules)

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
        """Build an EdgeContract from a logical-tree node's contract."""
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
