"""
Rulebook Registry - Maps rulebook rules to rewriter implementations.

Based on research in /research folder and qt-semantic-optimizer-rulebook-v1.
"""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional
import yaml


class FixApproach(Enum):
    AST_REWRITE = "ast_rewrite"
    LLM_REWRITE = "llm_rewrite"
    HYBRID = "hybrid"
    LLM_OR_HYBRID = "llm_or_hybrid"
    LLM_SUGGESTION_ONLY = "llm_suggestion_only"
    LLM_ASSISTED = "llm_assisted"


class ImplementationStatus(Enum):
    IMPLEMENTED = "implemented"
    PARTIAL = "partial"
    NOT_IMPLEMENTED = "not_implemented"
    PLANNED = "planned"


@dataclass
class RuleMapping:
    """Maps a rulebook rule to its implementation status."""
    rule_id: str
    family: str
    name: str
    core: bool
    fix_approach: FixApproach
    status: ImplementationStatus
    rewriter_id: Optional[str] = None
    notes: str = ""


# Current implementation mappings
RULE_MAPPINGS: list[RuleMapping] = [
    # === IMPLEMENTED - CORE ===
    RuleMapping(
        rule_id="QT-SUBQ-001",
        family="SUBQUERY_DECORRELATION",
        name="Correlated scalar aggregate subquery -> join + group",
        core=True,
        fix_approach=FixApproach.HYBRID,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="correlated_subquery_to_join",
        notes="Tested on TPC-DS q01, q06, q30, q81. q06 shows 1.21x speedup.",
    ),
    RuleMapping(
        rule_id="QT-SUBQ-002",
        family="SUBQUERY_DECORRELATION",
        name="Correlated EXISTS/IN -> SEMI-JOIN",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="exists_to_semi_join",
        notes="Converts EXISTS to SEMI JOIN syntax.",
    ),
    RuleMapping(
        rule_id="QT-TOPK-002",
        family="INTENT_TOPK_PER_GROUP",
        name="Greatest-per-group self-join -> window",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="self_join_to_window",
        notes="Uses RANK() for tie handling. No TPC-DS matches found.",
    ),
    RuleMapping(
        rule_id="QT-JOIN-002",
        family="JOIN_SEMI_ANTI",
        name="LEFT JOIN ... WHERE IS NULL -> ANTI-JOIN",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="left_join_null_to_not_exists",
        notes="Converts anti-join pattern to NOT EXISTS.",
    ),
    RuleMapping(
        rule_id="QT-FILT-001",
        family="FILTER_SARGABILITY",
        name="Non-sargable predicate -> range predicate",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="non_sargable_to_range",
        notes="DATE(col)=X -> col >= X AND col < X+1. Enables index usage.",
    ),
    RuleMapping(
        rule_id="QT-FILT-002",
        family="FILTER_SARGABILITY",
        name="OR-of-equalities on same column -> IN list",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="or_chain_to_in",
        notes="Requires 3+ OR conditions. No TPC-DS matches found.",
    ),
    RuleMapping(
        rule_id="QT-CTE-001",
        family="CTE_AND_COMMON_SUBEXPR",
        name="Nested/unused CTEs -> flatten/remove",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="unused_cte_remover",
        notes="Removes CTEs that are never referenced.",
    ),
    RuleMapping(
        rule_id="QT-CTE-002",
        family="CTE_AND_COMMON_SUBEXPR",
        name="CTE fence blocking pushdown -> inline or push filter",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="cte_inliner",
        notes="Inlines single-use CTEs to enable predicate pushdown.",
    ),
    RuleMapping(
        rule_id="QT-NULL-001",
        family="NULL_SEMANTICS_TRAPS",
        name="NOT IN (NULL trap) -> NOT EXISTS",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="not_in_to_not_exists",
        notes="Correctness fix - NOT IN with NULLs is a common trap.",
    ),

    # === IMPLEMENTED - EXTENDED ===
    RuleMapping(
        rule_id="QT-CTE-003",
        family="CTE_AND_COMMON_SUBEXPR",
        name="Repeated identical subquery blocks -> factor into single CTE",
        core=False,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="repeated_subquery_to_cte",
        notes="Hash-based duplicate detection.",
    ),
    RuleMapping(
        rule_id="QT-AGG-005",
        family="AGG_PREAGG_AND_REDUCE",
        name="HAVING filter that can be pushed to WHERE -> push down",
        core=False,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="having_to_where",
        notes="Pushes non-aggregate HAVING predicates to WHERE.",
    ),
    RuleMapping(
        rule_id="QT-AGG-006",
        family="AGG_PREAGG_AND_REDUCE",
        name="DISTINCT + GROUP BY redundancy -> remove redundant layer",
        core=False,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="distinct_group_by_redundancy",
        notes="Removes DISTINCT when GROUP BY covers all columns.",
    ),

    # === PARTIAL (DuckDB-specific) ===
    RuleMapping(
        rule_id="QT-TOPK-001",
        family="INTENT_TOPK_PER_GROUP",
        name="Latest-per-group via correlated MAX() -> window/qualify",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.PARTIAL,
        rewriter_id="subquery_to_qualify",
        notes="DuckDB QUALIFY only. Need generic window version for other engines.",
    ),
    RuleMapping(
        rule_id="QT-AGG-001",
        family="AGG_PREAGG_AND_REDUCE",
        name="Manual pivot via repeated subqueries/self-joins -> conditional aggregation",
        core=True,
        fix_approach=FixApproach.AST_REWRITE,
        status=ImplementationStatus.PARTIAL,
        rewriter_id="manual_pivot_to_pivot",
        notes="DuckDB PIVOT only. Actually slower (0.66x) on TPC-DS q43.",
    ),

    # === LLM-POWERED IMPLEMENTATIONS ===
    RuleMapping(
        rule_id="QT-INT-001",
        family="INTENT_PAGINATION",
        name="OFFSET pagination -> keyset pagination",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="offset_to_keyset",
        notes="LLM-powered. Requires cursor parameters from client.",
    ),
    RuleMapping(
        rule_id="QT-TOPK-003",
        family="INTENT_TOPK_PER_GROUP",
        name="Top-N per group -> window filter or LATERAL",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="topn_per_group",
        notes="LLM-powered. Uses window functions.",
    ),
    RuleMapping(
        rule_id="QT-JOIN-001",
        family="JOIN_SEMI_ANTI",
        name="LEFT JOIN + filter on right -> INNER or move to ON",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="left_join_filter",
        notes="LLM-powered. Detects intent from query structure.",
    ),
    RuleMapping(
        rule_id="QT-DIST-001",
        family="DISTINCT_DEDUP_CONSTRAINTS",
        name="Unnecessary DISTINCT -> remove or fix joins",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="unnecessary_distinct",
        notes="LLM-powered. Low confidence - needs metadata verification.",
    ),
    RuleMapping(
        rule_id="QT-AGG-002",
        family="AGG_PREAGG_AND_REDUCE",
        name="Pre-aggregate before join",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="pre_aggregate",
        notes="LLM-powered. Useful for large fact tables.",
    ),
    RuleMapping(
        rule_id="QT-AGG-003",
        family="AGG_PREAGG_AND_REDUCE",
        name="Drop redundant GROUP BY columns via FDs",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="group_by_fd",
        notes="LLM-powered. Low confidence - needs PK metadata.",
    ),
    RuleMapping(
        rule_id="QT-BOOL-001",
        family="SET_OPS_AND_BOOLEAN_LOGIC",
        name="OR across columns -> UNION ALL",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="or_to_union",
        notes="LLM-powered. Low confidence - may affect semantics.",
    ),
    RuleMapping(
        rule_id="QT-PLAN-001",
        family="WINDOW_PLANNER_BLINDSPOTS",
        name="Window blocks pushdown -> isolate filtered subset",
        core=True,
        fix_approach=FixApproach.LLM_REWRITE,
        status=ImplementationStatus.IMPLEMENTED,
        rewriter_id="window_pushdown",
        notes="LLM-powered. High confidence optimization.",
    ),
]


def load_rulebook() -> dict:
    """Load the rulebook YAML."""
    rulebook_path = Path(__file__).parent / "rulebook.yaml"
    with open(rulebook_path) as f:
        return yaml.safe_load(f)


def get_coverage_summary() -> dict:
    """Get implementation coverage statistics."""
    total_core = sum(1 for r in RULE_MAPPINGS if r.core)
    implemented_core = sum(
        1 for r in RULE_MAPPINGS
        if r.core and r.status in (ImplementationStatus.IMPLEMENTED, ImplementationStatus.PARTIAL)
    )

    total_extended = sum(1 for r in RULE_MAPPINGS if not r.core)
    implemented_extended = sum(
        1 for r in RULE_MAPPINGS
        if not r.core and r.status in (ImplementationStatus.IMPLEMENTED, ImplementationStatus.PARTIAL)
    )

    by_family = {}
    for r in RULE_MAPPINGS:
        if r.family not in by_family:
            by_family[r.family] = {"total": 0, "implemented": 0}
        by_family[r.family]["total"] += 1
        if r.status in (ImplementationStatus.IMPLEMENTED, ImplementationStatus.PARTIAL):
            by_family[r.family]["implemented"] += 1

    return {
        "core": {"total": total_core, "implemented": implemented_core},
        "extended": {"total": total_extended, "implemented": implemented_extended},
        "by_family": by_family,
    }


def print_coverage_report():
    """Print a formatted coverage report."""
    summary = get_coverage_summary()

    print("=" * 70)
    print("RULEBOOK COVERAGE REPORT")
    print("=" * 70)
    print()

    core = summary["core"]
    print(f"Core Rules:     {core['implemented']}/{core['total']} ({100*core['implemented']/core['total']:.0f}%)")

    ext = summary["extended"]
    if ext["total"] > 0:
        print(f"Extended Rules: {ext['implemented']}/{ext['total']} ({100*ext['implemented']/ext['total']:.0f}%)")

    print()
    print("By Family:")
    print("-" * 50)
    for family, stats in sorted(summary["by_family"].items()):
        pct = 100 * stats["implemented"] / stats["total"] if stats["total"] > 0 else 0
        bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
        print(f"  {family:<35} {bar} {stats['implemented']}/{stats['total']}")

    print()
    print("Implemented Rules:")
    print("-" * 70)
    for r in RULE_MAPPINGS:
        if r.status == ImplementationStatus.IMPLEMENTED:
            print(f"  ✓ {r.rule_id}: {r.name}")
            print(f"    Rewriter: {r.rewriter_id}")
            if r.notes:
                print(f"    Notes: {r.notes}")
            print()

    print("Partial Implementations:")
    print("-" * 70)
    for r in RULE_MAPPINGS:
        if r.status == ImplementationStatus.PARTIAL:
            print(f"  ◐ {r.rule_id}: {r.name}")
            print(f"    Rewriter: {r.rewriter_id}")
            if r.notes:
                print(f"    Notes: {r.notes}")
            print()

    print("Not Implemented (Core):")
    print("-" * 70)
    for r in RULE_MAPPINGS:
        if r.status == ImplementationStatus.NOT_IMPLEMENTED and r.core:
            approach = r.fix_approach.value.replace("_", " ")
            print(f"  ○ {r.rule_id}: {r.name}")
            print(f"    Approach: {approach}")
            if r.notes:
                print(f"    Notes: {r.notes}")
            print()


if __name__ == "__main__":
    print_coverage_report()
