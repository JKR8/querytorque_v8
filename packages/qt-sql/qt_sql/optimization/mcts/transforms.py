"""Transformation library for MCTS SQL optimizer.

Each transformation is a focused LLM prompt that applies ONE type of optimization.
This makes transformations composable and debuggable.

Transformations:
- push_pred: Push predicates into subqueries/joins
- reorder_join: Reorder joins for better selectivity
- materialize_cte: Convert repeated subqueries to CTEs
- inline_cte: Inline CTEs back into main query
- flatten_subq: Convert correlated subqueries to joins
- opt_agg: Optimize GROUP BY, push down aggregates
- remove_redundant: Remove unnecessary DISTINCT, unused columns
- opt_window: Optimize window functions, partition pruning
"""

from enum import Enum
from typing import Optional
import re


class TransformationType(str, Enum):
    """Available transformation types."""

    PUSH_PREDICATE = "push_pred"
    REORDER_JOIN = "reorder_join"
    MATERIALIZE_CTE = "materialize_cte"
    INLINE_CTE = "inline_cte"
    FLATTEN_SUBQUERY = "flatten_subq"
    OPTIMIZE_AGGREGATE = "opt_agg"
    REMOVE_REDUNDANT = "remove_redundant"
    OPTIMIZE_WINDOW = "opt_window"


# Focused prompts for each transformation type
TRANSFORMATION_PROMPTS = {
    TransformationType.PUSH_PREDICATE: """You are a SQL optimizer. Apply ONLY predicate pushdown optimization.

TASK: Push WHERE conditions as close to base tables as possible.

Rules:
1. Move predicates that filter on a single table into that table's subquery or join condition
2. Push predicates through inner joins when safe
3. Push predicates into CTEs if they filter on CTE columns only
4. Do NOT change join types, column lists, or aggregations
5. Preserve exact query semantics - same rows, same values

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.REORDER_JOIN: """You are a SQL optimizer. Apply ONLY join reordering optimization.

TASK: Reorder joins to put most selective/smallest tables first.

Rules:
1. Put tables with strong filter predicates (equality, small ranges) earlier in join order
2. Dimension tables (smaller) should generally come before fact tables (larger)
3. Preserve all predicates exactly - do not add or remove any
4. Do NOT change FROM/JOIN to subqueries or vice versa
5. Keep all column references valid after reordering

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.MATERIALIZE_CTE: """You are a SQL optimizer. Apply ONLY CTE materialization.

TASK: Convert repeated subqueries into Common Table Expressions (CTEs).

Rules:
1. If the same subquery pattern appears multiple times, extract it to a CTE
2. If a complex subquery is used once but would benefit from materialization, make it a CTE
3. Name CTEs descriptively based on what they compute
4. Do NOT change the query logic or results
5. Preserve all column names and orderings

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.INLINE_CTE: """You are a SQL optimizer. Apply ONLY CTE inlining.

TASK: Inline CTEs back into the main query where beneficial.

Rules:
1. Inline CTEs that are used only once
2. Inline simple CTEs (just a table scan with filters)
3. Keep CTEs that are used multiple times or are complex aggregations
4. Do NOT change the query logic or results
5. Preserve all column names and orderings

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.FLATTEN_SUBQUERY: """You are a SQL optimizer. Apply ONLY subquery flattening.

TASK: Convert correlated subqueries to equivalent JOINs.

Rules:
1. Convert EXISTS subqueries to SEMI JOINs or regular JOINs with DISTINCT
2. Convert NOT EXISTS to anti-joins (LEFT JOIN + IS NULL check)
3. Convert scalar subqueries in SELECT to LEFT JOINs when safe
4. Convert IN subqueries to JOINs
5. CRITICAL: Preserve exact semantics - same rows, same values, same cardinality
6. If unsure about semantics preservation, do NOT transform

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.OPTIMIZE_AGGREGATE: """You are a SQL optimizer. Apply ONLY aggregation optimization.

TASK: Optimize GROUP BY operations and push down aggregates.

Rules:
1. Push aggregations into subqueries when they can pre-aggregate
2. Remove unnecessary columns from GROUP BY if they're functionally determined
3. Convert COUNT(DISTINCT x) to more efficient patterns when possible
4. Use FILTER clause instead of CASE WHEN inside aggregates
5. Do NOT change what columns are in the final result
6. Preserve exact grouping semantics

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.REMOVE_REDUNDANT: """You are a SQL optimizer. Apply ONLY redundancy removal.

TASK: Remove unnecessary operations that don't affect results.

Rules:
1. Remove DISTINCT if the query already returns unique rows (e.g., GROUP BY covers all columns)
2. Remove unused columns from subqueries (not the final SELECT)
3. Remove redundant join conditions (already implied by other conditions)
4. Remove redundant ORDER BY in subqueries (when outer query has its own ORDER BY)
5. CRITICAL: Never remove anything that affects the final result
6. When in doubt, keep the clause

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.OPTIMIZE_WINDOW: """You are a SQL optimizer. Apply ONLY window function optimization.

TASK: Optimize window functions and their partition/order clauses.

Rules:
1. Combine multiple window functions with the same OVER clause
2. Add explicit frame bounds if missing (ROWS vs RANGE semantics)
3. Remove unnecessary ORDER BY in window functions for aggregates that don't need it
4. Consider replacing window functions with equivalent GROUP BY when simpler
5. Do NOT change the values produced by window functions
6. Preserve partition and ordering semantics exactly

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",
}


# Dictionary mapping string keys to prompts for easier access
TRANSFORMATION_LIBRARY: dict[str, str] = {
    t.value: TRANSFORMATION_PROMPTS[t]
    for t in TransformationType
}


def get_all_transform_ids() -> list[str]:
    """Get list of all transformation IDs."""
    return [t.value for t in TransformationType]


def apply_transformation(
    query: str,
    transform_id: str,
    llm_client,
    max_retries: int = 2,
) -> tuple[Optional[str], Optional[str]]:
    """Apply a transformation to a query using the LLM.

    Args:
        query: The SQL query to transform.
        transform_id: Which transformation to apply.
        llm_client: LLM client with analyze() method.
        max_retries: Number of retries on failure.

    Returns:
        Tuple of (transformed_sql, error_message).
        If successful, error_message is None.
        If failed, transformed_sql is None.
    """
    if transform_id not in TRANSFORMATION_LIBRARY:
        return None, f"Unknown transformation: {transform_id}"

    prompt_template = TRANSFORMATION_LIBRARY[transform_id]
    prompt = prompt_template.format(query=query)

    for attempt in range(max_retries + 1):
        try:
            response = llm_client.analyze(prompt)
            transformed = extract_sql_from_response(response)

            if transformed is None:
                if attempt < max_retries:
                    continue
                return None, "Failed to extract SQL from LLM response"

            # Basic validation: check it's not empty and looks like SQL
            if not transformed.strip():
                return None, "LLM returned empty SQL"

            # Check it's actually different (transformation had an effect)
            # Normalize whitespace for comparison
            orig_normalized = " ".join(query.split())
            trans_normalized = " ".join(transformed.split())

            if orig_normalized == trans_normalized:
                # No change made - that's okay, just means this transform doesn't apply
                return query, None

            return transformed, None

        except Exception as e:
            if attempt < max_retries:
                continue
            return None, f"LLM error: {str(e)}"

    return None, "Max retries exceeded"


def extract_sql_from_response(response: str) -> Optional[str]:
    """Extract SQL from LLM response.

    Handles various response formats:
    - Plain SQL
    - SQL in markdown code blocks
    - SQL with explanatory text

    Args:
        response: Raw LLM response text.

    Returns:
        Extracted SQL or None if extraction failed.
    """
    if not response:
        return None

    response = response.strip()

    # Try to extract from markdown code block first
    # Match ```sql ... ``` or ``` ... ```
    code_block_pattern = r"```(?:sql)?\s*\n?(.*?)\n?```"
    matches = re.findall(code_block_pattern, response, re.DOTALL | re.IGNORECASE)

    if matches:
        # Return the first code block
        sql = matches[0].strip()
        if sql:
            return sql

    # If no code block, check if response looks like SQL
    # Remove common non-SQL prefixes
    lines = response.split("\n")
    sql_lines = []
    in_sql = False

    for line in lines:
        line_stripped = line.strip()

        # Skip explanatory lines
        if any(line_stripped.lower().startswith(prefix) for prefix in [
            "here", "the optimized", "i ", "this ", "note:", "explanation:",
            "changes:", "result:", "#", "---", "***"
        ]):
            continue

        # Check if line looks like SQL
        if line_stripped.upper().startswith((
            "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
            "DROP", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
            "GROUP", "ORDER", "HAVING", "UNION", "INTERSECT", "EXCEPT", "LIMIT",
            "CASE", "WHEN", "AND", "OR", "(", ")"
        )) or in_sql:
            sql_lines.append(line)
            in_sql = True

    if sql_lines:
        return "\n".join(sql_lines).strip()

    # Last resort: return everything if it looks like SQL
    if response.upper().startswith(("SELECT", "WITH")):
        return response

    return None


def get_transform_description(transform_id: str) -> str:
    """Get a short description of what a transformation does."""
    descriptions = {
        "push_pred": "Push predicates closer to base tables",
        "reorder_join": "Reorder joins for better selectivity",
        "materialize_cte": "Extract repeated subqueries to CTEs",
        "inline_cte": "Inline single-use CTEs",
        "flatten_subq": "Convert correlated subqueries to JOINs",
        "opt_agg": "Optimize aggregations and GROUP BY",
        "remove_redundant": "Remove unnecessary operations",
        "opt_window": "Optimize window functions",
    }
    return descriptions.get(transform_id, transform_id)
