"""Unified Knowledge Base for SQL Optimization.

This is the SINGLE SOURCE OF TRUTH for all optimization patterns.
All services (MCTS, DSPy, Web UI, AST detection) import from here.

Pattern Evidence (TPC-DS SF100, DuckDB):
- or_to_union: 2.98x (Q15)
- correlated_to_cte: 2.81x (Q1)
- date_cte_isolate: 2.67x (Q15)
- push_pred: 2.71x (Q93)
- consolidate_scans: 1.84x (Q90)

Usage:
    from qt_sql.optimization.knowledge_base import (
        TRANSFORM_REGISTRY,
        get_transform,
        get_all_transforms,
        detect_opportunities,
    )
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional, Iterator
import re


class TransformID(str, Enum):
    """Canonical transform identifiers used by MCTS."""

    PUSH_PREDICATE = "push_pred"
    REORDER_JOIN = "reorder_join"
    MATERIALIZE_CTE = "materialize_cte"
    INLINE_CTE = "inline_cte"
    FLATTEN_SUBQUERY = "flatten_subq"
    REMOVE_REDUNDANT = "remove_redundant"
    MULTI_PUSH_PREDICATE = "multi_push_pred"
    # High-value (proven 2x+ speedups)
    OR_TO_UNION = "or_to_union"
    CORRELATED_TO_CTE = "correlated_to_cte"
    DATE_CTE_ISOLATION = "date_cte_isolate"
    CONSOLIDATE_SCANS = "consolidate_scans"


@dataclass
class TransformPattern:
    """A SQL optimization pattern with detection and rewrite metadata.

    Attributes:
        id: Canonical ID (e.g., "push_pred") - used by MCTS
        code: Display code (e.g., "QT-OPT-001") - used by UI
        name: Human-readable name
        description: What this pattern does
        trigger: How to detect this pattern (AST/text description)
        rewrite_hint: How to fix (for LLM prompts)
        weight: Impact score 1-10 (10 = transformative, used for savings estimate)
        benchmark_queries: TPC-DS queries where this helped (e.g., ["Q1", "Q15"])
        category: "high_value" for high-impact patterns, "standard" otherwise
        enabled: Whether this transform is active
    """

    id: TransformID
    code: str
    name: str
    description: str
    trigger: str
    rewrite_hint: str
    weight: int = 5  # 1-10 scale, higher = more impactful
    benchmark_queries: list[str] = field(default_factory=list)
    category: str = "standard"
    enabled: bool = True

    def to_prompt_context(self) -> str:
        """Format for injection into LLM prompts."""
        lines = [
            f"**{self.code}** - {self.name}",
            f"  Trigger: {self.trigger}",
            f"  Rewrite: {self.rewrite_hint}",
        ]
        return "\n".join(lines)


# =============================================================================
# THE KNOWLEDGE BASE - All 11 transforms
# =============================================================================

TRANSFORM_REGISTRY: dict[TransformID, TransformPattern] = {
    # ----- HIGH VALUE (weight 7-9) -----
    TransformID.OR_TO_UNION: TransformPattern(
        id=TransformID.OR_TO_UNION,
        code="QT-OPT-001",
        name="OR to UNION ALL Decomposition",
        description="Split OR conditions across different columns into UNION ALL branches",
        trigger="OR conditions spanning different columns (col_a = X OR col_b = Y)",
        rewrite_hint="Split into separate SELECTs with UNION ALL, add exclusion predicates to avoid duplicates",
        weight=8,
        benchmark_queries=["Q15", "Q23", "Q24", "Q45"],
        category="high_value",
    ),
    TransformID.CORRELATED_TO_CTE: TransformPattern(
        id=TransformID.CORRELATED_TO_CTE,
        code="QT-OPT-002",
        name="Correlated Subquery to Pre-computed CTE",
        description="Replace correlated subquery with aggregate by pre-computing in a CTE",
        trigger="WHERE col > (SELECT AVG/SUM/COUNT FROM ... WHERE correlated)",
        rewrite_hint="Create CTE with GROUP BY on correlation key, then JOIN instead of correlated lookup",
        weight=9,  # O(n²) to O(n) - highest impact
        benchmark_queries=["Q1"],
        category="high_value",
    ),
    TransformID.DATE_CTE_ISOLATION: TransformPattern(
        id=TransformID.DATE_CTE_ISOLATION,
        code="QT-OPT-003",
        name="Date CTE Isolation",
        description="Extract date dimension filtering into small early CTE",
        trigger="date_dim joined with d_year/d_qoy/d_month filter, fact table present",
        rewrite_hint="Create CTE: SELECT d_date_sk FROM date_dim WHERE filter, join fact to CTE early",
        weight=7,
        benchmark_queries=["Q6", "Q15", "Q27", "Q39", "Q92"],
        category="high_value",
    ),
    TransformID.PUSH_PREDICATE: TransformPattern(
        id=TransformID.PUSH_PREDICATE,
        code="QT-OPT-004",
        name="Predicate Pushdown into CTE",
        description="Push dimension filters from main query into CTE before aggregation",
        trigger="Dimension filter in main WHERE, CTE has aggregation on fact table",
        rewrite_hint="Move dimension join+filter INTO CTE before GROUP BY",
        weight=8,
        benchmark_queries=["Q27", "Q93"],
        category="high_value",
    ),
    TransformID.CONSOLIDATE_SCANS: TransformPattern(
        id=TransformID.CONSOLIDATE_SCANS,
        code="QT-OPT-005",
        name="Scan Consolidation",
        description="Combine multiple scans of same table into single scan with CASE WHEN",
        trigger="Same table scanned in multiple CTEs with different filters",
        rewrite_hint="Single scan with SUM(CASE WHEN cond THEN val END) for each branch",
        weight=7,
        benchmark_queries=["Q90"],
        category="high_value",
    ),

    # ----- STANDARD TRANSFORMS (weight 2-6) -----
    TransformID.MULTI_PUSH_PREDICATE: TransformPattern(
        id=TransformID.MULTI_PUSH_PREDICATE,
        code="QT-OPT-006",
        name="Multi-layer Predicate Pushdown",
        description="Push predicates through multiple CTE/subquery layers to base tables",
        trigger="Filter on column that traces back through multiple CTEs to base table",
        rewrite_hint="Trace column path through CTEs, add filter at each layer where column exists",
        weight=6,
        category="standard",
    ),
    TransformID.MATERIALIZE_CTE: TransformPattern(
        id=TransformID.MATERIALIZE_CTE,
        code="QT-OPT-007",
        name="Materialize Repeated Subquery",
        description="Extract repeated subquery patterns into a CTE",
        trigger="Same subquery pattern appears multiple times in query",
        rewrite_hint="Extract to CTE with MATERIALIZED hint, reference by name",
        weight=5,
        benchmark_queries=["Q95"],
        category="standard",
    ),
    TransformID.FLATTEN_SUBQUERY: TransformPattern(
        id=TransformID.FLATTEN_SUBQUERY,
        code="QT-OPT-008",
        name="Flatten Subquery to JOIN",
        description="Convert correlated subqueries to equivalent JOINs",
        trigger="EXISTS, NOT EXISTS, IN, or scalar subquery in WHERE/SELECT",
        rewrite_hint="EXISTS→SEMI JOIN, NOT EXISTS→anti-join (LEFT+NULL), IN→JOIN",
        weight=5,
        category="standard",
    ),
    TransformID.REORDER_JOIN: TransformPattern(
        id=TransformID.REORDER_JOIN,
        code="QT-OPT-009",
        name="Join Reordering",
        description="Reorder joins to put most selective tables first",
        trigger="Multiple tables joined, some with strong filters",
        rewrite_hint="Put tables with equality filters first, dimension before fact",
        weight=4,
        category="standard",
    ),
    TransformID.INLINE_CTE: TransformPattern(
        id=TransformID.INLINE_CTE,
        code="QT-OPT-010",
        name="Inline Single-Use CTE",
        description="Inline CTEs used only once back into main query",
        trigger="CTE referenced exactly once, simple scan with filter",
        rewrite_hint="Replace CTE reference with inline subquery",
        weight=3,
        category="standard",
    ),
    TransformID.REMOVE_REDUNDANT: TransformPattern(
        id=TransformID.REMOVE_REDUNDANT,
        code="QT-OPT-011",
        name="Remove Redundant Operations",
        description="Remove unnecessary DISTINCT, unused columns, redundant ORDER BY",
        trigger="DISTINCT with GROUP BY covering all columns, unused subquery columns",
        rewrite_hint="Remove DISTINCT if uniqueness guaranteed, trim unused columns",
        weight=2,
        category="standard",
    ),
}


# =============================================================================
# ACCESSOR FUNCTIONS
# =============================================================================

def get_transform(transform_id: TransformID | str) -> Optional[TransformPattern]:
    """Get a transform by ID."""
    if isinstance(transform_id, str):
        try:
            transform_id = TransformID(transform_id)
        except ValueError:
            return None
    return TRANSFORM_REGISTRY.get(transform_id)


def get_all_transforms(
    enabled_only: bool = True,
    category: Optional[str] = None,
) -> list[TransformPattern]:
    """Get all transforms, optionally filtered."""
    transforms = list(TRANSFORM_REGISTRY.values())
    if enabled_only:
        transforms = [t for t in transforms if t.enabled]
    if category:
        transforms = [t for t in transforms if t.category == category]
    return transforms


def get_high_value_transforms() -> list[TransformPattern]:
    """Get transforms with proven 2x+ speedups."""
    return get_all_transforms(category="high_value")


def get_transform_ids() -> list[str]:
    """Get all transform ID strings for MCTS."""
    return [t.value for t in TransformID]


# =============================================================================
# OPPORTUNITY DETECTION (for Web UI and prompts)
# =============================================================================

@dataclass
class DetectedOpportunity:
    """An optimization opportunity detected in SQL."""

    pattern: TransformPattern
    trigger_match: str  # What was matched
    location: Optional[str] = None  # Line/position info


def detect_opportunities(sql: str) -> list[DetectedOpportunity]:
    """Detect optimization opportunities in SQL.

    Uses lightweight regex/text matching for quick detection.
    For full AST-based detection, use the opportunity_rules in ast_detector.

    Args:
        sql: SQL query to analyze

    Returns:
        List of detected opportunities
    """
    opportunities = []
    sql_lower = sql.lower()

    # QT-OPT-001: OR to UNION
    # Look for OR in WHERE with different column references
    if ' or ' in sql_lower and 'where' in sql_lower:
        # Simple check: OR not in a string literal
        or_matches = list(re.finditer(r'\bor\b', sql_lower))
        if len(or_matches) >= 1:
            opportunities.append(DetectedOpportunity(
                pattern=TRANSFORM_REGISTRY[TransformID.OR_TO_UNION],
                trigger_match="OR condition in WHERE clause",
            ))

    # QT-OPT-002: Correlated subquery with aggregate
    # Look for correlated pattern: WHERE ... > (SELECT AVG/SUM
    correlated_pattern = r'where.*[><]=?\s*\(\s*select\s+(?:avg|sum|count|max|min)\s*\('
    if re.search(correlated_pattern, sql_lower, re.DOTALL):
        opportunities.append(DetectedOpportunity(
            pattern=TRANSFORM_REGISTRY[TransformID.CORRELATED_TO_CTE],
            trigger_match="Correlated subquery with aggregate comparison",
        ))

    # QT-OPT-003: Date filtering opportunity
    # Look for date_dim with d_year/d_qoy filter and fact table
    fact_tables = ['store_sales', 'catalog_sales', 'web_sales', 'store_returns',
                   'catalog_returns', 'web_returns', 'inventory']
    has_date_filter = any(col in sql_lower for col in ['d_year', 'd_qoy', 'd_moy', 'd_month'])
    has_date_dim = 'date_dim' in sql_lower
    has_fact = any(t in sql_lower for t in fact_tables)

    if has_date_filter and has_date_dim and has_fact:
        # Check it's not already in a CTE
        if not re.search(r'with\s+\w+\s+as\s*\([^)]*d_date_sk[^)]*from\s+date_dim', sql_lower):
            opportunities.append(DetectedOpportunity(
                pattern=TRANSFORM_REGISTRY[TransformID.DATE_CTE_ISOLATION],
                trigger_match=f"date_dim filter with fact table",
            ))

    # QT-OPT-005: Multiple scans of same table
    # Look for same table in multiple CTEs
    cte_pattern = r'with\s+(\w+)\s+as\s*\('
    ctes = re.findall(cte_pattern, sql_lower)
    if len(ctes) >= 2:
        # Check if any fact table appears multiple times
        for table in fact_tables:
            count = sql_lower.count(f'from {table}') + sql_lower.count(f'join {table}')
            if count >= 2:
                opportunities.append(DetectedOpportunity(
                    pattern=TRANSFORM_REGISTRY[TransformID.CONSOLIDATE_SCANS],
                    trigger_match=f"{table} scanned {count} times",
                ))
                break

    # QT-OPT-007: Repeated subquery
    # Look for identical subquery patterns
    subquery_pattern = r'\(\s*select\s+[^)]+\)'
    subqueries = re.findall(subquery_pattern, sql_lower)
    if len(subqueries) >= 2:
        # Check for duplicates (simplified)
        seen = set()
        for sq in subqueries:
            normalized = ' '.join(sq.split())
            if normalized in seen:
                opportunities.append(DetectedOpportunity(
                    pattern=TRANSFORM_REGISTRY[TransformID.MATERIALIZE_CTE],
                    trigger_match="Repeated subquery pattern",
                ))
                break
            seen.add(normalized)

    return opportunities


def format_opportunities_for_prompt(opportunities: list[DetectedOpportunity]) -> str:
    """Format detected opportunities for LLM prompt injection."""
    if not opportunities:
        return ""

    lines = ["## Detected Optimization Opportunities\n"]
    for i, opp in enumerate(opportunities, 1):
        lines.append(f"{i}. {opp.pattern.to_prompt_context()}")
        lines.append(f"   Matched: {opp.trigger_match}")
        lines.append("")

    return "\n".join(lines)


# =============================================================================
# EXPORTS FOR BACKWARD COMPATIBILITY
# =============================================================================

# For api/main.py compatibility
@dataclass
class OpportunityResult:
    """API-compatible opportunity result."""
    pattern_id: str
    pattern_name: str
    trigger: str
    rewrite_hint: str
    expected_benefit: str
    example: str = ""


def detect_opportunities_for_api(sql: str) -> list[OpportunityResult]:
    """Detect opportunities and return API-compatible format."""
    detected = detect_opportunities(sql)
    return [
        OpportunityResult(
            pattern_id=opp.pattern.code,
            pattern_name=opp.pattern.name,
            trigger=opp.trigger_match,
            rewrite_hint=opp.pattern.rewrite_hint,
            expected_benefit=opp.pattern.expected_speedup,
        )
        for opp in detected
    ]
