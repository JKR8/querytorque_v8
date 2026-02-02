"""LLM-Powered SQL Rewriters.

Uses LLM for complex transformations that require:
- Intent detection
- Metadata inference
- Engine-specific knowledge
- Complex pattern matching

Each rule has a specific prompt with examples.
"""

import json
import re
from typing import Any, Optional
from dataclasses import dataclass

from sqlglot import exp
import sqlglot

from .base import (
    BaseRewriter,
    RewriteResult,
    RewriteConfidence,
    SafetyCheckResult,
)
from .registry import register_rewriter

# Try to import LLM client from qt-shared
try:
    from qt_shared.llm import create_llm_client
    LLM_AVAILABLE = True
except ImportError:
    LLM_AVAILABLE = False
    create_llm_client = None


@dataclass
class LLMRewritePrompt:
    """Prompt template for LLM rewriting."""
    rule_id: str
    rule_name: str
    description: str
    examples: list[tuple[str, str]]  # (before, after) pairs
    constraints: list[str]
    output_format: str = "sql_only"


# ============================================================================
# PROMPT DEFINITIONS FOR EACH RULE
# ============================================================================

PROMPTS = {
    "QT-INT-001": LLMRewritePrompt(
        rule_id="QT-INT-001",
        rule_name="OFFSET Pagination to Keyset Pagination",
        description="""Convert OFFSET-based pagination to keyset (cursor) pagination.
OFFSET pagination scans and discards rows, becoming slower as offset increases.
Keyset pagination uses WHERE conditions on indexed columns for O(1) performance.""",
        examples=[
            # Example 1: Simple single-column ordering
            (
                """SELECT id, name, created_at
FROM users
ORDER BY created_at DESC
LIMIT 20 OFFSET 1000""",
                """-- Keyset pagination (requires last_created_at from previous page)
SELECT id, name, created_at
FROM users
WHERE created_at < :last_created_at  -- cursor from previous page
ORDER BY created_at DESC
LIMIT 20"""
            ),
            # Example 2: Compound key for ties
            (
                """SELECT id, email, score
FROM players
ORDER BY score DESC, id ASC
LIMIT 50 OFFSET 500""",
                """-- Keyset with tie-breaker
SELECT id, email, score
FROM players
WHERE (score, id) < (:last_score, :last_id)  -- compound cursor
ORDER BY score DESC, id ASC
LIMIT 50"""
            ),
            # Example 3: With WHERE clause
            (
                """SELECT * FROM orders
WHERE status = 'shipped'
ORDER BY ship_date DESC
LIMIT 100 OFFSET 2000""",
                """SELECT * FROM orders
WHERE status = 'shipped'
  AND ship_date < :last_ship_date
ORDER BY ship_date DESC
LIMIT 100"""
            ),
        ],
        constraints=[
            "Preserve ORDER BY columns as cursor keys",
            "Add tie-breaker (usually id) if ORDER BY column has duplicates",
            "Keep all existing WHERE conditions",
            "Output must include comment explaining cursor parameters needed",
        ],
    ),

    "QT-TOPK-003": LLMRewritePrompt(
        rule_id="QT-TOPK-003",
        rule_name="Top-N Per Group Optimization",
        description="""Optimize top-N per group queries using window functions or LATERAL joins.
Choose strategy based on N size and engine capabilities.""",
        examples=[
            # Example 1: Latest 3 orders per customer using window
            (
                """SELECT * FROM orders o1
WHERE o1.order_date IN (
    SELECT order_date FROM orders o2
    WHERE o2.customer_id = o1.customer_id
    ORDER BY order_date DESC
    LIMIT 3
)""",
                """SELECT * FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC) as rn
    FROM orders
) ranked
WHERE rn <= 3"""
            ),
            # Example 2: Top 5 products per category by sales
            (
                """SELECT p.* FROM products p
WHERE p.product_id IN (
    SELECT product_id FROM sales s
    WHERE s.category_id = p.category_id
    GROUP BY product_id
    ORDER BY SUM(amount) DESC
    LIMIT 5
)""",
                """WITH ranked_products AS (
    SELECT
        p.*,
        SUM(s.amount) as total_sales,
        ROW_NUMBER() OVER (PARTITION BY p.category_id ORDER BY SUM(s.amount) DESC) as rn
    FROM products p
    JOIN sales s ON s.product_id = p.product_id
    GROUP BY p.product_id, p.category_id
)
SELECT * FROM ranked_products WHERE rn <= 5"""
            ),
        ],
        constraints=[
            "Use ROW_NUMBER() for exactly N rows per group",
            "Use RANK() if ties should all be included",
            "Preserve all columns from original query",
            "Handle NULL partition keys appropriately",
        ],
    ),

    "QT-JOIN-001": LLMRewritePrompt(
        rule_id="QT-JOIN-001",
        rule_name="LEFT JOIN Filter Optimization",
        description="""When a LEFT JOIN has a WHERE filter on the right table:
- If filter rejects NULLs: convert to INNER JOIN (more efficient)
- If filter should keep non-matches: move predicate to ON clause""",
        examples=[
            # Example 1: WHERE rejects NULLs -> INNER JOIN
            (
                """SELECT o.*, c.name
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
WHERE c.status = 'active'""",
                """-- c.status = 'active' rejects NULLs, so LEFT JOIN is effectively INNER
SELECT o.*, c.name
FROM orders o
INNER JOIN customers c ON c.id = o.customer_id
WHERE c.status = 'active'"""
            ),
            # Example 2: Intent is to keep non-matches -> move to ON
            (
                """SELECT u.*, p.name as plan_name
FROM users u
LEFT JOIN plans p ON p.id = u.plan_id
WHERE p.is_active = true OR p.id IS NULL""",
                """-- Move filter to ON to preserve LEFT JOIN semantics
SELECT u.*, p.name as plan_name
FROM users u
LEFT JOIN plans p ON p.id = u.plan_id AND p.is_active = true"""
            ),
        ],
        constraints=[
            "Analyze if WHERE rejects NULLs (no IS NULL check for right table)",
            "If rejects NULLs: convert to INNER JOIN",
            "If has OR ... IS NULL pattern: move non-null condition to ON",
            "Add comment explaining the optimization",
        ],
    ),

    "QT-DIST-001": LLMRewritePrompt(
        rule_id="QT-DIST-001",
        rule_name="Unnecessary DISTINCT Removal",
        description="""Remove DISTINCT when it's unnecessary because:
- Selecting a primary key (guarantees uniqueness)
- GROUP BY already produces unique rows
- Join keys guarantee 1:1 relationship""",
        examples=[
            # Example 1: Selecting PK
            (
                """SELECT DISTINCT id, name, email
FROM users
WHERE status = 'active'""",
                """-- id is primary key, DISTINCT is unnecessary
SELECT id, name, email
FROM users
WHERE status = 'active'"""
            ),
            # Example 2: After GROUP BY
            (
                """SELECT DISTINCT department, COUNT(*) as emp_count
FROM employees
GROUP BY department""",
                """-- GROUP BY already guarantees unique rows
SELECT department, COUNT(*) as emp_count
FROM employees
GROUP BY department"""
            ),
            # Example 3: DISTINCT masking join issue
            (
                """SELECT DISTINCT o.id, o.total
FROM orders o
JOIN order_items i ON i.order_id = o.id""",
                """-- DISTINCT masks the 1:many join; if you need order-level data:
SELECT o.id, o.total
FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items i WHERE i.order_id = o.id)
-- Or if items must exist, just: FROM orders o (no join needed for these columns)"""
            ),
        ],
        constraints=[
            "Only remove DISTINCT if uniqueness is guaranteed",
            "If DISTINCT masks a join problem, suggest fix",
            "Add comment explaining why DISTINCT is unnecessary",
            "If unsure about uniqueness, keep DISTINCT and add warning",
        ],
    ),

    "QT-AGG-002": LLMRewritePrompt(
        rule_id="QT-AGG-002",
        rule_name="Pre-Aggregate Before Join",
        description="""When aggregating fact table data after joining to dimensions,
pre-aggregate the fact table first to reduce join volume.""",
        examples=[
            # Example 1: Sum after join
            (
                """SELECT d.region, SUM(s.amount) as total
FROM sales s
JOIN stores d ON d.store_id = s.store_id
GROUP BY d.region""",
                """-- Pre-aggregate sales by store_id before joining
WITH store_totals AS (
    SELECT store_id, SUM(amount) as amount
    FROM sales
    GROUP BY store_id
)
SELECT d.region, SUM(st.amount) as total
FROM store_totals st
JOIN stores d ON d.store_id = st.store_id
GROUP BY d.region"""
            ),
            # Example 2: Multiple aggregates
            (
                """SELECT c.country, p.category,
       COUNT(*) as order_count,
       SUM(o.total) as revenue
FROM orders o
JOIN customers c ON c.id = o.customer_id
JOIN products p ON p.id = o.product_id
GROUP BY c.country, p.category""",
                """-- Pre-aggregate orders
WITH order_agg AS (
    SELECT customer_id, product_id,
           COUNT(*) as order_count,
           SUM(total) as revenue
    FROM orders
    GROUP BY customer_id, product_id
)
SELECT c.country, p.category,
       SUM(oa.order_count) as order_count,
       SUM(oa.revenue) as revenue
FROM order_agg oa
JOIN customers c ON c.id = oa.customer_id
JOIN products p ON p.id = oa.product_id
GROUP BY c.country, p.category"""
            ),
        ],
        constraints=[
            "Pre-aggregate fact table by join keys",
            "Preserve aggregate semantics (SUM of SUMs, etc.)",
            "Only beneficial when fact table is much larger than dimensions",
            "Keep COUNT(*) -> SUM(count) pattern",
        ],
    ),

    "QT-AGG-003": LLMRewritePrompt(
        rule_id="QT-AGG-003",
        rule_name="Remove Redundant GROUP BY Columns",
        description="""Remove columns from GROUP BY that are functionally dependent on other columns.
If you GROUP BY a primary key, other columns from that table are determined.""",
        examples=[
            # Example 1: PK determines other columns
            (
                """SELECT u.id, u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name, u.email""",
                """-- u.id is PK, determines name and email
SELECT u.id, u.name, u.email, COUNT(o.id) as order_count
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id
-- Note: Some databases require all non-aggregated columns in GROUP BY"""
            ),
            # Example 2: With aggregates
            (
                """SELECT d.id, d.name, d.region, d.manager_id,
       SUM(e.salary) as total_salary
FROM departments d
JOIN employees e ON e.dept_id = d.id
GROUP BY d.id, d.name, d.region, d.manager_id""",
                """-- d.id is PK for departments
SELECT d.id, d.name, d.region, d.manager_id,
       SUM(e.salary) as total_salary
FROM departments d
JOIN employees e ON e.dept_id = d.id
GROUP BY d.id"""
            ),
        ],
        constraints=[
            "Only remove if PK/unique constraint guarantees dependency",
            "Check database compatibility (MySQL vs PostgreSQL vs DuckDB)",
            "Add comment noting the functional dependency",
            "If unsure about constraints, keep all columns",
        ],
    ),

    "QT-BOOL-001": LLMRewritePrompt(
        rule_id="QT-BOOL-001",
        rule_name="OR to UNION ALL",
        description="""Convert OR conditions across different columns to UNION ALL
when it enables better index usage. Only safe when branches are disjoint.""",
        examples=[
            # Example 1: OR on different indexed columns
            (
                """SELECT * FROM events
WHERE user_id = 123 OR event_type = 'purchase'""",
                """-- Split OR to enable index usage on each column
SELECT * FROM events WHERE user_id = 123
UNION ALL
SELECT * FROM events WHERE event_type = 'purchase' AND user_id != 123"""
            ),
            # Example 2: With additional conditions
            (
                """SELECT id, name FROM products
WHERE (category = 'Electronics' AND price > 1000)
   OR (brand = 'Apple')""",
                """SELECT id, name FROM products
WHERE category = 'Electronics' AND price > 1000
UNION ALL
SELECT id, name FROM products
WHERE brand = 'Apple'
  AND NOT (category = 'Electronics' AND price > 1000)"""
            ),
        ],
        constraints=[
            "Ensure branches are disjoint to avoid duplicates",
            "Use UNION (not UNION ALL) if disjointness can't be guaranteed",
            "Only beneficial if each branch can use an index",
            "Add exclusion condition to second branch to ensure disjointness",
        ],
    ),

    "QT-PLAN-001": LLMRewritePrompt(
        rule_id="QT-PLAN-001",
        rule_name="Window Function Filter Pushdown",
        description="""When filtering on a partition column with window functions,
push the filter into a subquery before the window to reduce computation.""",
        examples=[
            # Example 1: Filter on partition key
            (
                """SELECT *,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
FROM employees
WHERE department = 'Engineering'""",
                """-- Push filter before window function
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
FROM (
    SELECT * FROM employees WHERE department = 'Engineering'
) filtered"""
            ),
            # Example 2: With ranking filter
            (
                """SELECT * FROM (
    SELECT *,
           RANK() OVER (PARTITION BY region ORDER BY sales DESC) as rnk
    FROM sales_reps
) ranked
WHERE region = 'West' AND rnk <= 5""",
                """-- Push region filter inside
SELECT * FROM (
    SELECT *,
           RANK() OVER (PARTITION BY region ORDER BY sales DESC) as rnk
    FROM sales_reps
    WHERE region = 'West'  -- pushed inside
) ranked
WHERE rnk <= 5"""
            ),
        ],
        constraints=[
            "Only push filters on partition columns",
            "Don't push filters that reference window function results",
            "Preserve window function semantics",
            "Add comment explaining the optimization",
        ],
    ),
}


class LLMRewriterBase(BaseRewriter):
    """Base class for LLM-powered rewriters."""

    # Subclasses must set these
    prompt_key: str = ""
    default_confidence = RewriteConfidence.MEDIUM

    def __init__(self, llm_client=None, metadata=None):
        super().__init__(metadata=metadata)
        self._llm_client = llm_client

    @property
    def llm_client(self):
        if self._llm_client is None and LLM_AVAILABLE:
            self._llm_client = create_llm_client()
        return self._llm_client

    def get_prompt(self) -> LLMRewritePrompt:
        """Get the prompt template for this rewriter."""
        return PROMPTS.get(self.prompt_key)

    def build_prompt(self, sql: str, context: dict = None) -> str:
        """Build the full prompt for the LLM."""
        template = self.get_prompt()
        if not template:
            raise ValueError(f"No prompt template for {self.prompt_key}")

        examples_text = "\n\n".join([
            f"Example {i+1}:\nBefore:\n```sql\n{before}\n```\n\nAfter:\n```sql\n{after}\n```"
            for i, (before, after) in enumerate(template.examples)
        ])

        constraints_text = "\n".join(f"- {c}" for c in template.constraints)

        context_text = ""
        if context:
            context_text = f"\n\nAdditional context:\n{json.dumps(context, indent=2)}"

        return f"""You are a SQL optimization expert. Your task is to rewrite SQL queries for better performance.

Rule: {template.rule_id} - {template.rule_name}

Description:
{template.description}

{examples_text}

Constraints:
{constraints_text}

Now rewrite this SQL query following the pattern above:

```sql
{sql}
```
{context_text}

Output ONLY the rewritten SQL query (with optional comment explaining changes). No explanations outside the SQL."""

    def parse_llm_response(self, response: str) -> Optional[str]:
        """Extract SQL from LLM response."""
        # Try to extract from code block
        match = re.search(r'```sql\s*(.*?)\s*```', response, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try plain code block
        match = re.search(r'```\s*(.*?)\s*```', response, re.DOTALL)
        if match:
            return match.group(1).strip()

        # If no code block, assume entire response is SQL
        return response.strip()

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check if LLM is available."""
        return LLM_AVAILABLE and self.llm_client is not None

    def rewrite(self, node: exp.Expression, context: Any = None) -> RewriteResult:
        """Rewrite using LLM."""
        original_sql = node.sql()

        if not self.can_rewrite(node, context):
            return self._create_failure(original_sql, "LLM client not available")

        try:
            prompt = self.build_prompt(original_sql, context)
            response = self.llm_client.analyze(prompt)
            rewritten_sql = self.parse_llm_response(response)

            if not rewritten_sql:
                return self._create_failure(original_sql, "Could not parse LLM response")

            # Validate the SQL parses
            try:
                rewritten_node = sqlglot.parse_one(rewritten_sql, dialect="duckdb")
            except Exception as e:
                return self._create_failure(
                    original_sql,
                    f"LLM output is not valid SQL: {e}"
                )

            result = self._create_result(
                success=True,
                original_sql=original_sql,
                rewritten_sql=rewritten_sql,
                rewritten_node=rewritten_node,
                confidence=self.default_confidence,
                explanation=f"LLM rewrite using {self.prompt_key}",
            )

            result.add_safety_check(
                name="llm_rewrite",
                result=SafetyCheckResult.WARNING,
                message="LLM-generated rewrite - verify equivalence",
            )

            return result

        except Exception as e:
            return self._create_failure(original_sql, f"LLM error: {str(e)}")


# ============================================================================
# CONCRETE LLM REWRITER IMPLEMENTATIONS
# ============================================================================

@register_rewriter
class KeysetPaginationRewriter(LLMRewriterBase):
    """QT-INT-001: OFFSET pagination -> keyset pagination."""

    rewriter_id = "offset_to_keyset"
    name = "OFFSET to Keyset Pagination"
    description = "Convert OFFSET pagination to cursor-based keyset pagination"
    linked_rule_ids = ("QT-INT-001",)
    prompt_key = "QT-INT-001"
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for OFFSET + ORDER BY + LIMIT pattern."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Must have ORDER BY, LIMIT, and OFFSET
        has_order = node.find(exp.Order) is not None
        has_limit = node.find(exp.Limit) is not None
        has_offset = node.find(exp.Offset) is not None

        return has_order and has_limit and has_offset


@register_rewriter
class TopNPerGroupRewriter(LLMRewriterBase):
    """QT-TOPK-003: Top-N per group optimization."""

    rewriter_id = "topn_per_group"
    name = "Top-N Per Group"
    description = "Optimize top-N per group using window functions"
    linked_rule_ids = ("QT-TOPK-003",)
    prompt_key = "QT-TOPK-003"
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for correlated LIMIT subquery pattern."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Look for IN subquery with LIMIT
        for in_node in node.find_all(exp.In):
            query = in_node.args.get("query")
            if query and isinstance(query, exp.Subquery):
                inner = query.find(exp.Select)
                if inner and inner.find(exp.Limit):
                    return True

        return False


@register_rewriter
class LeftJoinFilterRewriter(LLMRewriterBase):
    """QT-JOIN-001: LEFT JOIN + filter optimization."""

    rewriter_id = "left_join_filter"
    name = "LEFT JOIN Filter Optimization"
    description = "Optimize LEFT JOIN with WHERE filter on right table"
    linked_rule_ids = ("QT-JOIN-001",)
    prompt_key = "QT-JOIN-001"
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for LEFT JOIN with filter on right table."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Need LEFT JOIN
        left_joins = [
            j for j in node.find_all(exp.Join)
            if str(j.args.get("side", "")).upper() == "LEFT"
        ]
        if not left_joins:
            return False

        # Need WHERE clause
        where = node.find(exp.Where)
        if not where:
            return False

        # Check if WHERE references a LEFT JOINed table
        for join in left_joins:
            join_table = join.find(exp.Table)
            if join_table:
                alias = str(join_table.alias or join_table.name).lower()
                for col in where.find_all(exp.Column):
                    if col.table and str(col.table).lower() == alias:
                        return True

        return False


@register_rewriter
class UnnecessaryDistinctRewriter(LLMRewriterBase):
    """QT-DIST-001: Remove unnecessary DISTINCT."""

    rewriter_id = "unnecessary_distinct"
    name = "Remove Unnecessary DISTINCT"
    description = "Remove DISTINCT when uniqueness is guaranteed"
    linked_rule_ids = ("QT-DIST-001",)
    prompt_key = "QT-DIST-001"
    default_confidence = RewriteConfidence.LOW  # Needs metadata verification

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for DISTINCT with potential redundancy."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Must have DISTINCT
        return bool(node.args.get("distinct"))


@register_rewriter
class PreAggregateRewriter(LLMRewriterBase):
    """QT-AGG-002: Pre-aggregate before join."""

    rewriter_id = "pre_aggregate"
    name = "Pre-Aggregate Before Join"
    description = "Pre-aggregate fact table before joining to dimensions"
    linked_rule_ids = ("QT-AGG-002",)
    prompt_key = "QT-AGG-002"
    default_confidence = RewriteConfidence.MEDIUM

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for aggregate + join pattern."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Need aggregates
        agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
        has_agg = any(node.find_all(agg_types))

        # Need joins
        has_join = bool(list(node.find_all(exp.Join)))

        # Need GROUP BY
        has_group = node.find(exp.Group) is not None

        return has_agg and has_join and has_group


@register_rewriter
class GroupByFDRewriter(LLMRewriterBase):
    """QT-AGG-003: Remove redundant GROUP BY columns."""

    rewriter_id = "group_by_fd"
    name = "Simplify GROUP BY via Functional Dependencies"
    description = "Remove GROUP BY columns determined by primary key"
    linked_rule_ids = ("QT-AGG-003",)
    prompt_key = "QT-AGG-003"
    default_confidence = RewriteConfidence.LOW  # Needs metadata

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for multi-column GROUP BY."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        group = node.find(exp.Group)
        if not group:
            return False

        # Need multiple GROUP BY columns
        return len(group.expressions) > 1


@register_rewriter
class OrToUnionRewriter(LLMRewriterBase):
    """QT-BOOL-001: OR across columns -> UNION ALL."""

    rewriter_id = "or_to_union"
    name = "OR to UNION ALL"
    description = "Convert OR conditions to UNION ALL for better index usage"
    linked_rule_ids = ("QT-BOOL-001",)
    prompt_key = "QT-BOOL-001"
    default_confidence = RewriteConfidence.LOW  # May change semantics

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for OR on different columns."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        where = node.find(exp.Where)
        if not where:
            return False

        # Look for top-level OR
        return isinstance(where.this, exp.Or)


@register_rewriter
class WindowPushdownRewriter(LLMRewriterBase):
    """QT-PLAN-001: Window function filter pushdown."""

    rewriter_id = "window_pushdown"
    name = "Window Filter Pushdown"
    description = "Push partition filters before window functions"
    linked_rule_ids = ("QT-PLAN-001",)
    prompt_key = "QT-PLAN-001"
    default_confidence = RewriteConfidence.HIGH

    def can_rewrite(self, node: exp.Expression, context: Any = None) -> bool:
        """Check for window function with filter on partition column."""
        if not super().can_rewrite(node, context):
            return False

        if not isinstance(node, exp.Select):
            return False

        # Need window function
        has_window = bool(list(node.find_all(exp.Window)))

        # Need WHERE
        has_where = node.find(exp.Where) is not None

        return has_window and has_where
