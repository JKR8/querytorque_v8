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
    REMOVE_REDUNDANT = "remove_redundant"
    MULTI_PUSH_PREDICATE = "multi_push_pred"
    # High-value transforms from knowledge base (proven 2x+ speedups)
    OR_TO_UNION = "or_to_union"  # 2.98x on Q15
    CORRELATED_TO_CTE = "correlated_to_cte"  # 2.81x on Q1
    DATE_CTE_ISOLATION = "date_cte_isolate"  # 2.67x on Q15
    CONSOLIDATE_SCANS = "consolidate_scans"  # 1.84x on Q90


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

    TransformationType.MULTI_PUSH_PREDICATE: """You are a SQL optimizer. Apply MULTI-NODE predicate pushdown optimization.

TASK: Push WHERE conditions through multiple CTE/subquery layers to the earliest possible point.

This is different from simple predicate pushdown - you are tracing predicates through MULTIPLE
intermediate nodes (CTEs, derived tables) to push them all the way to base table scans.

Example:
```sql
-- Before: filter at main query
WITH cte1 AS (SELECT customer_id, amount FROM sales),
     cte2 AS (SELECT customer_id, SUM(amount) as total FROM cte1 GROUP BY customer_id)
SELECT * FROM cte2 WHERE customer_id = 100

-- After: filter pushed to cte1 (through cte2's GROUP BY)
WITH cte1 AS (SELECT customer_id, amount FROM sales WHERE customer_id = 100),
     cte2 AS (SELECT customer_id, SUM(amount) as total FROM cte1 GROUP BY customer_id)
SELECT * FROM cte2 WHERE customer_id = 100
```

Rules:
1. Trace filter columns back through CTEs to find source tables
2. Push filters through GROUP BY ONLY if the filter column IS a GROUP BY column
3. Push filters through JOINs when they apply to one side only
4. Do NOT push filters through aggregations on columns that aren't in GROUP BY
5. Do NOT push filters through DISTINCT on columns that aren't in the DISTINCT list
6. Keep the original filter in place (optimizer will eliminate redundancy)
7. Adjust column names if they are aliased differently in intermediate layers
8. Preserve exact query semantics - same rows, same values

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    # =========================================================================
    # HIGH-VALUE TRANSFORMS (proven 2x+ speedups from TPC-DS benchmarks)
    # =========================================================================

    TransformationType.OR_TO_UNION: """You are a SQL optimizer. Apply OR-to-UNION-ALL decomposition.

TASK: Split complex OR conditions into separate queries combined with UNION ALL.

VERIFIED SPEEDUP: 2.98x on TPC-DS Q15 (142ms → 53ms)

When to apply:
- Complex OR condition spanning different columns or value types
- Query does full table scan due to OR preventing index usage
- Each OR branch has different selectivity or access path

Example:
```sql
-- Before: single query with complex OR
SELECT ca_zip, SUM(cs_sales_price)
FROM catalog_sales, customer_address
WHERE cs_bill_addr_sk = ca_address_sk
  AND (substr(ca_zip,1,5) IN ('85669','86197')
       OR ca_state IN ('CA','WA','GA')
       OR cs_sales_price > 500)
GROUP BY ca_zip

-- After: UNION ALL decomposition
WITH branch1 AS (
    SELECT cs_sales_price, ca_zip FROM catalog_sales, customer_address
    WHERE cs_bill_addr_sk = ca_address_sk AND substr(ca_zip,1,5) IN ('85669','86197')
),
branch2 AS (
    SELECT cs_sales_price, ca_zip FROM catalog_sales, customer_address
    WHERE cs_bill_addr_sk = ca_address_sk AND ca_state IN ('CA','WA','GA')
    AND substr(ca_zip,1,5) NOT IN ('85669','86197')  -- avoid duplicates
),
branch3 AS (
    SELECT cs_sales_price, ca_zip FROM catalog_sales, customer_address
    WHERE cs_bill_addr_sk = ca_address_sk AND cs_sales_price > 500
    AND substr(ca_zip,1,5) NOT IN ('85669','86197')
    AND ca_state NOT IN ('CA','WA','GA')  -- avoid duplicates
)
SELECT ca_zip, SUM(cs_sales_price) FROM (
    SELECT * FROM branch1
    UNION ALL SELECT * FROM branch2
    UNION ALL SELECT * FROM branch3
) combined
GROUP BY ca_zip
```

Rules:
1. Split each OR branch into a separate CTE or subquery
2. CRITICAL: Exclude previous conditions to avoid duplicates
3. If exact deduplication is complex, use UNION (not UNION ALL) for final combine
4. Push common filters (like date filters) into each branch
5. Preserve exact result semantics - same rows, same aggregations
6. Only apply when OR spans DIFFERENT columns (not just different values of same column)

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.CORRELATED_TO_CTE: """You are a SQL optimizer. Apply correlated-subquery-to-precomputed-CTE transformation.

TASK: Replace correlated subqueries that compute per-group aggregates with pre-computed CTEs.

VERIFIED SPEEDUP: 2.81x on TPC-DS Q1 (241ms → 86ms)

When to apply:
- Correlated subquery in WHERE computing AVG, SUM, COUNT per group
- Subquery used for threshold comparison (>, <, =)
- Same CTE or table referenced in both main query and correlated subquery

Example:
```sql
-- Before: correlated subquery (O(n²) execution)
WITH ctr AS (
    SELECT store_sk, customer_sk, SUM(fee) AS total
    FROM returns GROUP BY store_sk, customer_sk
)
SELECT * FROM ctr c1
WHERE c1.total > (
    SELECT AVG(total) * 1.2
    FROM ctr c2
    WHERE c1.store_sk = c2.store_sk
)

-- After: pre-computed CTE (O(n) execution)
WITH ctr AS (
    SELECT store_sk, customer_sk, SUM(fee) AS total
    FROM returns GROUP BY store_sk, customer_sk
),
store_avg AS (
    SELECT store_sk, AVG(total) * 1.2 AS threshold
    FROM ctr
    GROUP BY store_sk
)
SELECT c1.*
FROM ctr c1
JOIN store_avg sa ON c1.store_sk = sa.store_sk
WHERE c1.total > sa.threshold
```

Rules:
1. Identify the grouping column in the correlated subquery (the correlation key)
2. Create a new CTE that pre-computes the aggregate GROUP BY that correlation key
3. Replace the correlated subquery with a JOIN to the pre-computed CTE
4. Preserve exact semantics - same rows returned, same comparison logic
5. Handle NULL-safety: if original used WHERE, new JOIN should also exclude NULLs
6. This transforms O(n²) correlated execution to O(n) with a single pre-aggregation

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.DATE_CTE_ISOLATION: """You are a SQL optimizer. Apply date-CTE-isolation transformation.

TASK: Extract date dimension filtering into a small, early CTE for better partition pruning.

VERIFIED SPEEDUP: 1.2-2.7x on TPC-DS Q6, Q15, Q27

When to apply:
- Date dimension joined with filter (d_year, d_qoy, d_month_seq, etc.)
- Same date filter repeated in multiple CTEs or joins
- Complex date range expression
- Fact tables have date partition columns

Example:
```sql
-- Before: date filter embedded in main query
SELECT c.customer_id, SUM(ss.sales)
FROM store_sales ss
JOIN date_dim d ON ss.sold_date_sk = d.d_date_sk
JOIN customer c ON ss.customer_sk = c.customer_sk
WHERE d.d_year = 2001 AND d.d_qoy = 1
GROUP BY c.customer_id

-- After: date filter isolated in small CTE
WITH filtered_dates AS (
    SELECT d_date_sk
    FROM date_dim
    WHERE d_year = 2001 AND d_qoy = 1
)
SELECT c.customer_id, SUM(ss.sales)
FROM store_sales ss
JOIN filtered_dates fd ON ss.sold_date_sk = fd.d_date_sk
JOIN customer c ON ss.customer_sk = c.customer_sk
GROUP BY c.customer_id
```

Why it works:
- Small CTE materializes early (~90 rows for 1 quarter)
- Enables partition pruning on fact tables
- Reduces repeated filter evaluation
- Optimizer can better estimate cardinality

Rules:
1. Identify date dimension joins with selective filters
2. Extract to CTE selecting ONLY the surrogate key column (d_date_sk)
3. Replace date_dim join with join to filtered CTE
4. Remove the date filter from main WHERE clause (now in CTE)
5. If multiple CTEs need same date filter, reuse the date CTE
6. Preserve exact join semantics

Original query:
```sql
{query}
```

Return ONLY the optimized SQL query. No explanations, no markdown, just the SQL.""",

    TransformationType.CONSOLIDATE_SCANS: """You are a SQL optimizer. Apply scan consolidation transformation.

TASK: Combine multiple scans of the same table into a single scan with conditional aggregation.

VERIFIED SPEEDUP: 1.84x on TPC-DS Q90

When to apply:
- Same table appears in multiple CTEs or subqueries
- Each scan has different filter conditions
- Results are combined later (usually via JOIN or arithmetic)
- Heavy I/O queries where scan reduction helps

Example:
```sql
-- Before: multiple scans of same table
WITH morning_sales AS (
    SELECT ws_web_site_sk, SUM(ws_sales_price) as am_sales
    FROM web_sales
    JOIN time_dim ON ws_sold_time_sk = t_time_sk
    WHERE t_hour BETWEEN 8 AND 11
    GROUP BY ws_web_site_sk
),
evening_sales AS (
    SELECT ws_web_site_sk, SUM(ws_sales_price) as pm_sales
    FROM web_sales
    JOIN time_dim ON ws_sold_time_sk = t_time_sk
    WHERE t_hour BETWEEN 19 AND 22
    GROUP BY ws_web_site_sk
)
SELECT m.ws_web_site_sk, m.am_sales, e.pm_sales
FROM morning_sales m JOIN evening_sales e ON m.ws_web_site_sk = e.ws_web_site_sk

-- After: single scan with conditional aggregation
WITH combined_sales AS (
    SELECT
        ws_web_site_sk,
        SUM(CASE WHEN t_hour BETWEEN 8 AND 11 THEN ws_sales_price ELSE 0 END) as am_sales,
        SUM(CASE WHEN t_hour BETWEEN 19 AND 22 THEN ws_sales_price ELSE 0 END) as pm_sales
    FROM web_sales
    JOIN time_dim ON ws_sold_time_sk = t_time_sk
    WHERE t_hour BETWEEN 8 AND 22  -- combined filter range
    GROUP BY ws_web_site_sk
    HAVING SUM(CASE WHEN t_hour BETWEEN 8 AND 11 THEN 1 ELSE 0 END) > 0
       AND SUM(CASE WHEN t_hour BETWEEN 19 AND 22 THEN 1 ELSE 0 END) > 0
)
SELECT ws_web_site_sk, am_sales, pm_sales FROM combined_sales

-- Alternative simpler form:
SELECT
    ws_web_site_sk,
    SUM(CASE WHEN t_hour BETWEEN 8 AND 11 THEN ws_sales_price END) as am_sales,
    SUM(CASE WHEN t_hour BETWEEN 19 AND 22 THEN ws_sales_price END) as pm_sales
FROM web_sales
JOIN time_dim ON ws_sold_time_sk = t_time_sk
WHERE t_hour BETWEEN 8 AND 22
GROUP BY ws_web_site_sk
HAVING COUNT(CASE WHEN t_hour BETWEEN 8 AND 11 THEN 1 END) > 0
   AND COUNT(CASE WHEN t_hour BETWEEN 19 AND 22 THEN 1 END) > 0
```

Rules:
1. Identify repeated scans of the same base table with different filters
2. Use CASE WHEN inside aggregates to compute conditional results
3. Combine filter conditions with OR (or superset range)
4. Use HAVING to filter out groups that had no matches in a category
5. Preserve NULL handling: SUM(CASE WHEN ... THEN x END) gives NULL, not 0
6. For exact semantics, may need COALESCE or explicit ELSE 0
7. Only consolidate when all scans are aggregating (not when row-level results differ)

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
        "remove_redundant": "Remove unnecessary operations",
        "multi_push_pred": "Push predicates through multiple CTE/subquery layers",
        # High-value transforms (proven 2x+ speedups)
        "or_to_union": "Split OR conditions into UNION ALL branches (2.98x)",
        "correlated_to_cte": "Replace correlated subquery with pre-computed CTE (2.81x)",
        "date_cte_isolate": "Isolate date filter into small CTE (2.67x)",
        "consolidate_scans": "Combine multiple scans into conditional aggregation (1.84x)",
    }
    return descriptions.get(transform_id, transform_id)


# =============================================================================
# DAG-Based Transformations (Patch Mode)
# =============================================================================

# DAG-aware prompt templates that ask for node-level rewrites
DAG_TRANSFORMATION_PROMPTS = {
    TransformationType.PUSH_PREDICATE: """Apply ONLY predicate pushdown optimization using node-level rewrites.

TASK: Push WHERE conditions as close to base tables as possible.

{dag_prompt}

Rules:
1. Move predicates that filter on a single table into that node's CTE or subquery
2. Push predicates through inner joins when safe
3. Push predicates into CTEs if they filter on CTE columns only
4. Do NOT change join types, column lists, or aggregations
5. Preserve exact query semantics - same rows, same values
6. Return JSON with rewrites for affected nodes only

Return JSON:
```json
{{"rewrites": {{"node_id": "SELECT ..."}}, "explanation": "..."}}
```""",

    TransformationType.REORDER_JOIN: """Apply ONLY join reordering optimization using node-level rewrites.

TASK: Reorder joins to put most selective/smallest tables first.

{dag_prompt}

Rules:
1. Put tables with strong filter predicates (equality, small ranges) earlier in join order
2. Dimension tables (smaller) should generally come before fact tables (larger)
3. Preserve all predicates exactly - do not add or remove any
4. Do NOT change FROM/JOIN to subqueries or vice versa
5. Keep all column references valid after reordering
6. Only rewrite nodes where join reordering applies

Return JSON:
```json
{{"rewrites": {{"node_id": "SELECT ..."}}, "explanation": "..."}}
```""",

    TransformationType.MATERIALIZE_CTE: """Apply ONLY CTE materialization using node-level rewrites.

TASK: Convert repeated subqueries into Common Table Expressions (CTEs).

{dag_prompt}

Rules:
1. If the same subquery pattern appears multiple times, extract it to a CTE
2. If a complex subquery is used once but would benefit from materialization, make it a CTE
3. Name CTEs descriptively based on what they compute
4. Do NOT change the query logic or results
5. Preserve all column names and orderings
6. For this transform, you may need to create NEW node IDs for new CTEs

Return JSON:
```json
{{"rewrites": {{"new_cte_name": "SELECT ...", "main_query": "SELECT ... FROM new_cte_name ..."}}, "explanation": "..."}}
```""",

    TransformationType.INLINE_CTE: """Apply ONLY CTE inlining using node-level rewrites.

TASK: Inline CTEs back into the main query where beneficial.

{dag_prompt}

Rules:
1. Inline CTEs that are used only once
2. Inline simple CTEs (just a table scan with filters)
3. Keep CTEs that are used multiple times or are complex aggregations
4. Do NOT change the query logic or results
5. Preserve all column names and orderings
6. Remove the inlined CTE and update referencing nodes

Return JSON:
```json
{{"rewrites": {{"main_query": "SELECT ... (with inlined subquery)"}}, "explanation": "..."}}
```""",

    TransformationType.FLATTEN_SUBQUERY: """Apply ONLY subquery flattening using node-level rewrites.

TASK: Convert correlated subqueries to equivalent JOINs or window functions.

{dag_prompt}

Rules:
1. Convert EXISTS subqueries to SEMI JOINs or regular JOINs with DISTINCT
2. Convert NOT EXISTS to anti-joins (LEFT JOIN + IS NULL check)
3. Convert scalar subqueries in SELECT to LEFT JOINs when safe
4. Convert IN subqueries to JOINs
5. Convert correlated subqueries to window functions in the parent node
6. CRITICAL: Preserve exact semantics - same rows, same values, same cardinality
7. If unsure about semantics preservation, do NOT transform

Return JSON:
```json
{{"rewrites": {{"node_id": "SELECT ..."}}, "explanation": "..."}}
```""",

    TransformationType.REMOVE_REDUNDANT: """Apply ONLY redundancy removal using node-level rewrites.

TASK: Remove unnecessary operations that don't affect results.

{dag_prompt}

Rules:
1. Remove DISTINCT if the query already returns unique rows (e.g., GROUP BY covers all columns)
2. Remove unused columns from subqueries/CTEs (not the final SELECT)
3. Remove redundant join conditions (already implied by other conditions)
4. Remove redundant ORDER BY in subqueries (when outer query has its own ORDER BY)
5. Remove unnecessary casts or conversions
6. CRITICAL: Never remove anything that affects the final result
7. When in doubt, keep the clause

Return JSON:
```json
{{"rewrites": {{"node_id": "SELECT ..."}}, "explanation": "..."}}
```""",

    TransformationType.MULTI_PUSH_PREDICATE: """Apply MULTI-NODE predicate pushdown optimization using node-level rewrites.

TASK: Push WHERE conditions through multiple CTE/subquery layers to base table scans.

{dag_prompt}

{pushdown_analysis}

## Strategy

For each pushable predicate path listed above:
1. Add the predicate to the target node's WHERE clause
2. Adjust column names according to the column_chain (columns may be aliased)
3. Keep the original predicate in place (don't remove it)

## Example

Given this pushdown path:
- From: main_query
- To: cte1
- Predicate: customer_id = 100
- Column path: customer_id → customer_id

Before:
```sql
cte1: SELECT customer_id, amount FROM sales
```

After:
```sql
cte1: SELECT customer_id, amount FROM sales WHERE customer_id = 100
```

## Rules

1. Only push predicates marked as "pushable" in the analysis above
2. Push through GROUP BY ONLY if filter column IS a GROUP BY column
3. Adjust column names based on column_chain (left-to-right = outer-to-inner)
4. Keep original filters in place - redundancy is OK
5. For equality predicates (=, IN), always safe to push through GROUP BY on same column
6. For range predicates (<, >, BETWEEN), same rule applies
7. NEVER push filters that would change result set semantics

Return JSON with rewrites for each target node you're modifying:
```json
{{"rewrites": {{"target_node_id": "SELECT ... WHERE pushed_predicate ..."}}, "explanation": "..."}}
```""",

    # =========================================================================
    # HIGH-VALUE DAG TRANSFORMS (proven 2x+ speedups)
    # =========================================================================

    TransformationType.OR_TO_UNION: """Apply OR-to-UNION-ALL decomposition using node-level rewrites.

TASK: Split complex OR conditions into separate CTEs combined with UNION ALL.

VERIFIED SPEEDUP: 2.98x on TPC-DS Q15

{dag_prompt}

Rules:
1. Identify nodes with OR conditions spanning different columns
2. Create new CTE nodes for each OR branch
3. Add exclusion predicates to avoid duplicates
4. Create a combining node with UNION ALL
5. Update parent node to reference the combined CTE

Return JSON:
```json
{{"rewrites": {{"branch1_cte": "SELECT ...", "branch2_cte": "SELECT ...", "combined_cte": "SELECT * FROM branch1 UNION ALL SELECT * FROM branch2", "main_query": "SELECT ... FROM combined_cte ..."}}, "explanation": "..."}}
```""",

    TransformationType.CORRELATED_TO_CTE: """Apply correlated-to-precomputed-CTE transformation using node-level rewrites.

TASK: Replace correlated subqueries with pre-computed CTEs.

VERIFIED SPEEDUP: 2.81x on TPC-DS Q1

{dag_prompt}

Rules:
1. Identify correlated subquery computing aggregate (AVG, SUM, COUNT) with GROUP BY correlation
2. Create new CTE that pre-computes the aggregate grouped by the correlation key
3. Replace correlated subquery with JOIN to the pre-computed CTE
4. Preserve exact filtering semantics

Return JSON:
```json
{{"rewrites": {{"precomputed_agg": "SELECT correlation_key, AGG(...) AS threshold FROM ... GROUP BY correlation_key", "main_query": "SELECT ... FROM ... JOIN precomputed_agg ON ... WHERE ..."}}, "explanation": "..."}}
```""",

    TransformationType.DATE_CTE_ISOLATION: """Apply date-CTE-isolation transformation using node-level rewrites.

TASK: Extract date dimension filtering into a small early CTE.

VERIFIED SPEEDUP: 1.2-2.7x on TPC-DS Q6, Q15, Q27

{dag_prompt}

Rules:
1. Identify date dimension joins with selective filters (d_year, d_qoy, d_month_seq)
2. Create small CTE selecting only d_date_sk with the date filter
3. Replace date_dim joins with joins to the filtered date CTE
4. Preserve exact join semantics

Return JSON:
```json
{{"rewrites": {{"filtered_dates": "SELECT d_date_sk FROM date_dim WHERE d_year = 2001 AND d_qoy = 1", "main_query": "SELECT ... JOIN filtered_dates ON ..."}}, "explanation": "..."}}
```""",

    TransformationType.CONSOLIDATE_SCANS: """Apply scan consolidation transformation using node-level rewrites.

TASK: Combine multiple scans of same table into single scan with conditional aggregation.

VERIFIED SPEEDUP: 1.84x on TPC-DS Q90

{dag_prompt}

Rules:
1. Identify nodes scanning the same table with different filters
2. Combine into single CTE with CASE WHEN inside aggregates
3. Use OR of conditions or superset range for WHERE
4. Add HAVING clauses to preserve original join semantics

Return JSON:
```json
{{"rewrites": {{"combined_scan": "SELECT key, SUM(CASE WHEN cond1 THEN val END) AS val1, SUM(CASE WHEN cond2 THEN val END) AS val2 FROM table GROUP BY key", "main_query": "SELECT ... FROM combined_scan ..."}}, "explanation": "..."}}
```""",
}


def apply_dag_transformation(
    query: str,
    transform_id: str,
    llm_client,
    plan_summary: Optional[dict] = None,
    max_retries: int = 2,
) -> tuple[Optional[str], Optional[str]]:
    """Apply a transformation using DAG-based node patching.

    Instead of asking LLM for full SQL rewrite, this:
    1. Parses query into DAG structure
    2. Asks LLM for node-level patches in JSON format
    3. Applies patches using dag.apply_rewrites()

    Args:
        query: The SQL query to transform.
        transform_id: Which transformation to apply.
        llm_client: LLM client with analyze() method.
        plan_summary: Optional execution plan summary for context.
        max_retries: Number of retries on failure.

    Returns:
        Tuple of (transformed_sql, error_message).
    """
    import json
    from ..sql_dag import SQLDag

    # Check if we have a DAG prompt for this transform
    transform_type = None
    for t in TransformationType:
        if t.value == transform_id:
            transform_type = t
            break

    if transform_type not in DAG_TRANSFORMATION_PROMPTS:
        # Fall back to regular transformation
        return apply_transformation(query, transform_id, llm_client, max_retries)

    # Build DAG
    dag = SQLDag.from_sql(query)
    if not dag.nodes:
        # DAG parsing failed, fall back to regular transform
        return apply_transformation(query, transform_id, llm_client, max_retries)

    # Build DAG prompt
    dag_prompt = dag.to_prompt(include_sql=True, plan_summary=plan_summary)

    # Add node list
    node_list = "Available nodes: " + ", ".join(f"`{n}`" for n in dag.topological_order())
    dag_prompt += f"\n\n{node_list}"

    # For multi_push_pred, include pushdown analysis
    pushdown_analysis = ""
    if transform_type == TransformationType.MULTI_PUSH_PREDICATE:
        try:
            from ..predicate_analysis import analyze_pushdown_opportunities
            analysis = analyze_pushdown_opportunities(dag)
            pushdown_analysis = analysis.to_prompt_context()
            if not pushdown_analysis or not analysis.get_pushable_paths():
                # No pushdown opportunities found
                return query, None
        except Exception as e:
            # Analysis failed, fall back to regular transform
            return apply_transformation(query, transform_id, llm_client, max_retries)

    # Build full prompt
    prompt_template = DAG_TRANSFORMATION_PROMPTS[transform_type]
    prompt = prompt_template.format(
        dag_prompt=dag_prompt,
        pushdown_analysis=pushdown_analysis
    )

    for attempt in range(max_retries + 1):
        try:
            response = llm_client.analyze(prompt)
            rewrites = extract_dag_rewrites(response)

            if not rewrites:
                if attempt < max_retries:
                    continue
                # No rewrites suggested - try regular transform as fallback
                return apply_transformation(query, transform_id, llm_client, max_retries)

            # Apply rewrites using DAG
            transformed = dag.apply_rewrites(rewrites)

            if not transformed or not transformed.strip():
                return None, "DAG apply_rewrites returned empty SQL"

            # Check if actually different
            orig_normalized = " ".join(query.split())
            trans_normalized = " ".join(transformed.split())

            if orig_normalized == trans_normalized:
                return query, None  # No change

            return transformed, None

        except Exception as e:
            if attempt < max_retries:
                continue
            return None, f"DAG transform error: {str(e)}"

    return None, "Max retries exceeded"


def extract_dag_rewrites(response: str) -> Optional[dict[str, str]]:
    """Extract node rewrites from LLM JSON response.

    Handles various response formats:
    - Clean JSON
    - JSON in markdown code blocks
    - JSON with surrounding text

    Args:
        response: Raw LLM response text.

    Returns:
        Dict of {node_id: new_sql} or None if extraction failed.
    """
    import json

    if not response:
        return None

    response = response.strip()

    # Try to extract JSON from code block
    json_pattern = r"```(?:json)?\s*\n?(.*?)\n?```"
    matches = re.findall(json_pattern, response, re.DOTALL | re.IGNORECASE)

    json_str = None
    if matches:
        json_str = matches[0].strip()
    else:
        # Try to find JSON object directly
        brace_start = response.find('{')
        brace_end = response.rfind('}')
        if brace_start != -1 and brace_end != -1:
            json_str = response[brace_start:brace_end + 1]

    if not json_str:
        return None

    try:
        data = json.loads(json_str)

        if isinstance(data, dict):
            # Check for rewrites key
            if "rewrites" in data and isinstance(data["rewrites"], dict):
                return data["rewrites"]
            # Maybe the response is the rewrites dict directly
            if all(isinstance(v, str) for v in data.values()):
                return data

        return None

    except json.JSONDecodeError:
        return None
