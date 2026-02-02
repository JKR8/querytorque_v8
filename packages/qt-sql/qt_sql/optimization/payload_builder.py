"""Structured LLM Payload Builder v2 - YAML/JSON format for DuckDB optimization.

This module generates machine-readable optimization payloads with:
- Engine metadata (version, settings, execution mode)
- Structured constraints with transformation categories
- Schema with storage information
- Relational guarantees (FK relationships, uniqueness)
- Performance evidence with raw EXPLAIN ANALYZE
- Recommended actions catalog
- Knowledge base patterns for semantic rewrites
- Verification requirements
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
import yaml

logger = logging.getLogger(__name__)


# Knowledge base patterns keyed by rule_id prefix
# These explain WHY certain rewrites work and WHAT the optimizer cannot do
KNOWLEDGE_BASE_PATTERNS: dict[str, dict] = {
    "SQL-DUCK-014": {
        "pattern_name": "Grouped TOPN via Window vs LATERAL",
        "execution_model": {
            "window_approach": "Must process ALL rows before numbering/ranking, then filter",
            "lateral_approach": "For each group, finds N rows then STOPS (early termination)",
        },
        "why_faster": "LATERAL with LIMIT enables early termination - stops after N rows per group instead of processing entire dataset",
        "when_to_apply": "When NDV (number of distinct groups) is low relative to total rows",
        "optimizer_limitation": "DuckDB cannot auto-transform because: 1) Semantic rewrite not cost-based, 2) Requires knowing NDV is small, 3) Different query structures",
        "rewrite_rules": [
            "1. Find the PARTITION BY column - its source table becomes the OUTER driving table",
            "2. ALL other tables, joins, and filters go INSIDE the LATERAL subquery",
            "3. Add correlation predicate: WHERE fact.fk = outer.pk",
            "4. Move ORDER BY and add LIMIT N inside LATERAL",
            "5. Remove the outer WHERE rn <= N (replaced by LIMIT)",
            "6. PRESERVE all original output columns - add to outer SELECT or LATERAL SELECT as needed",
        ],
        "rewrite_example": {
            "before": """SELECT * FROM (
    SELECT s.s_store_id, ss.ss_item_sk, ss.ss_sales_price,
           ROW_NUMBER() OVER (PARTITION BY s.s_store_id ORDER BY ss.ss_sales_price DESC) rn
    FROM store_sales ss
    JOIN store s ON ss.ss_store_sk = s.s_store_sk
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE d.d_year = 2001
) ranked WHERE rn <= 5""",
            "after": """SELECT s.s_store_id, ls.ss_item_sk, ls.ss_sales_price
FROM store s,
LATERAL (
    SELECT ss.ss_item_sk, ss.ss_sales_price
    FROM store_sales ss
    JOIN date_dim d ON ss.ss_sold_date_sk = d.d_date_sk
    WHERE ss.ss_store_sk = s.s_store_sk  -- correlation to outer
      AND d.d_year = 2001                 -- filters INSIDE lateral
    ORDER BY ss.ss_sales_price DESC
    LIMIT 5
) ls
ORDER BY s.s_store_id""",
            "explanation": "store is PARTITION BY source -> outer. store_sales, date_dim, and d_year filter -> inside LATERAL",
        },
        "example_speedup": "7M rows -> 2K output: Window processes all 7M, LATERAL processes ~400 groups × ~17K rows each with early stop",
    },
    "SQL-DUCK-011": {
        "pattern_name": "CROSS JOIN UNNEST Filter Pushdown",
        "execution_model": {
            "problem": "Filters on unnested columns may not push into UNNEST operation",
            "result": "Full array expansion before filtering",
        },
        "why_faster": "Pre-filtering arrays avoids expanding elements that will be discarded",
        "rewrite_pattern": {
            "from": "FROM t, UNNEST(t.array_col) li WHERE li.x = value",
            "to": "FROM t, UNNEST(list_filter(t.array_col, x -> x.x = value)) li",
        },
        "optimizer_limitation": "DuckDB's UNNEST pushdown is limited - cannot always push predicates into array operations",
    },
    "SQL-DUCK-012": {
        "pattern_name": "Window Blocks Predicate Pushdown",
        "execution_model": {
            "problem": "Window functions in subquery prevent filter pushdown to base tables",
            "result": "Full table scan before window computation, then filter",
        },
        "why_faster": "Moving filters before window reduces rows processed by expensive window operation",
        "rewrite_pattern": {
            "from": "SELECT * FROM (SELECT *, window_fn() OVER (...) FROM t) WHERE filter_col = x",
            "to": "SELECT *, window_fn() OVER (...) FROM t WHERE filter_col = x",
        },
        "optimizer_limitation": "Optimizer cannot push predicates past window functions due to semantic constraints",
    },
    "SQL-WHERE-010": {
        "pattern_name": "OR Prevents Index Usage",
        "execution_model": {
            "problem": "OR on different columns cannot use single index efficiently",
            "result": "May require full table scan or multiple index scans with union",
        },
        "why_faster": "UNION ALL decomposition allows each branch to use optimal index",
        "rewrite_pattern": {
            "from": "SELECT ... WHERE col1 = x OR col2 = y",
            "to": "SELECT ... WHERE col1 = x UNION ALL SELECT ... WHERE col2 = y AND col1 != x",
        },
        "optimizer_limitation": "Optimizer may not automatically decompose OR into UNION for index utilization",
    },
    "SQL-SUB-007": {
        "pattern_name": "Correlated Subquery to Window Function",
        "execution_model": {
            "problem": "Correlated subquery executes once per outer row - O(n²) behavior",
            "result": "Massive performance degradation on large tables",
        },
        "why_faster": "Window function computes all values in single pass - O(n)",
        "rewrite_pattern": {
            "from": "SELECT *, (SELECT agg(x) FROM t2 WHERE t2.key = t1.key) FROM t1",
            "to": "SELECT *, agg(x) OVER (PARTITION BY key) FROM t1 JOIN t2 ON t1.key = t2.key",
        },
        "optimizer_limitation": "Optimizer cannot automatically convert correlated subqueries to window functions - different semantic structures",
    },
    "SQL-JOIN-008": {
        "pattern_name": "Self-Join to Window Function",
        "execution_model": {
            "problem": "Self-join requires two scans of same table and hash/merge join",
            "result": "2x I/O and join overhead",
        },
        "why_faster": "Window function computes in single table scan",
        "rewrite_pattern": {
            "from": "SELECT a.*, b.prev_val FROM t a LEFT JOIN t b ON a.id = b.id + 1",
            "to": "SELECT *, LAG(val) OVER (ORDER BY id) as prev_val FROM t",
        },
        "optimizer_limitation": "Optimizer cannot recognize self-join patterns that could be window functions",
    },
    "SQL-ORD-005": {
        "pattern_name": "OFFSET Pagination to Keyset Pagination",
        "execution_model": {
            "problem": "OFFSET scans and discards rows - O(offset + limit) work",
            "result": "OFFSET 50000 LIMIT 10 scans 50,010 rows to return 10",
        },
        "why_faster": "Keyset pagination seeks directly to position using index",
        "rewrite_pattern": {
            "from": "SELECT * FROM t ORDER BY created_at DESC LIMIT 10 OFFSET 50000",
            "to": "SELECT * FROM t WHERE (created_at, id) < (@last_ts, @last_id) ORDER BY created_at DESC, id DESC LIMIT 10",
        },
        "optimizer_limitation": "OFFSET semantics (jump to page N) differ from keyset (continue from last). Optimizer cannot transform.",
        "note": "Requires tracking last-seen values in application. Cannot jump to arbitrary page.",
    },
    "SQL-JOIN-011": {
        "pattern_name": "Triangular Join to Window Function",
        "execution_model": {
            "problem": "Self-join with <= creates O(n²) comparisons: (n² + n)/2",
            "result": "10,000 rows = 50 million internal comparisons",
        },
        "why_faster": "Window function with ROWS UNBOUNDED PRECEDING is O(n)",
        "rewrite_pattern": {
            "from": "SELECT a.id, (SELECT SUM(val) FROM t b WHERE b.id <= a.id) FROM t a",
            "to": "SELECT id, SUM(val) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) FROM t",
        },
        "optimizer_limitation": "Optimizer cannot recognize triangular join pattern as running total",
    },
    "SQL-AGG-009": {
        "pattern_name": "COUNT DISTINCT to Approximate Counting",
        "execution_model": {
            "problem": "Exact COUNT(DISTINCT) must track all unique values in memory",
            "result": "Memory-intensive, slow on high-cardinality columns",
        },
        "why_faster": "HyperLogLog uses fixed ~12KB memory regardless of cardinality",
        "rewrite_pattern": {
            "from": "SELECT date, COUNT(DISTINCT user_id) FROM events GROUP BY date",
            "to": "SELECT date, APPROX_COUNT_DISTINCT(user_id) FROM events GROUP BY date",
        },
        "optimizer_limitation": "Semantic change: ~2% error. Only human can accept accuracy trade-off.",
        "note": "Use when exact precision not required. HLL sketches can be merged for reaggregation.",
    },
}

PAYLOAD_VERSION = "duckdb.sqlopt.v1"


def _build_knowledge_base_section(
    issues: list[dict],
    schema_context: Optional[dict] = None,
) -> Optional[dict]:
    """Build knowledge base section with relevant optimization patterns.

    This provides the LLM with:
    - WHY certain rewrites work (execution model differences)
    - WHAT the optimizer cannot do automatically
    - HOW to apply the rewrite pattern
    - WHEN it's beneficial (NDV awareness, etc.)
    """
    if not issues:
        return None

    relevant_patterns = []

    for issue in issues:
        rule_id = issue.get("rule_id", "")

        # Look up pattern in knowledge base
        if rule_id in KNOWLEDGE_BASE_PATTERNS:
            pattern = KNOWLEDGE_BASE_PATTERNS[rule_id].copy()
            pattern["triggered_by"] = rule_id

            # Add NDV context if schema available and pattern benefits from it
            if schema_context and rule_id == "SQL-DUCK-014":
                ndv_hints = _get_ndv_hints_for_topn(schema_context)
                if ndv_hints:
                    pattern["ndv_analysis"] = ndv_hints

            relevant_patterns.append(pattern)

    if not relevant_patterns:
        return None

    return {
        "patterns": relevant_patterns,
        "guidance": (
            "These patterns explain WHY certain rewrites provide speedups. "
            "The optimizer cannot perform these semantic rewrites automatically. "
            "Apply the rewrite patterns when the execution model analysis indicates benefit."
        ),
    }


def _get_ndv_hints_for_topn(schema_context: dict) -> Optional[dict]:
    """Extract NDV hints for grouped TOPN optimization decisions."""
    tables = schema_context.get("tables", [])

    # Look for columns that might be partition keys (category, type, id suffixes)
    low_ndv_candidates = []

    for table in tables:
        row_count = table.get("row_count", 0)
        if row_count == 0:
            continue

        for col in table.get("columns", []):
            col_name = col.get("name", "").lower()
            distinct = col.get("cardinality") or col.get("distinct_count")

            if distinct is None:
                continue

            # Check if this is likely a good partition column (low NDV relative to rows)
            ndv_ratio = distinct / row_count if row_count > 0 else 1

            # Flag columns with < 1% NDV ratio as good LATERAL candidates
            if ndv_ratio < 0.01 and distinct < 1000:
                low_ndv_candidates.append({
                    "table": table.get("name"),
                    "column": col.get("name"),
                    "distinct_values": distinct,
                    "total_rows": row_count,
                    "ndv_ratio": f"{ndv_ratio:.4%}",
                    "lateral_benefit": "HIGH - early termination will skip most rows",
                })

    if low_ndv_candidates:
        return {
            "low_ndv_columns": low_ndv_candidates,
            "recommendation": (
                "These columns have low NDV - LATERAL with LIMIT will dramatically outperform "
                "window functions because it can stop after N rows per group instead of "
                "processing all rows."
            ),
        }

    return None


@dataclass
class PayloadV2Result:
    """Result from building a v2 payload."""
    payload_yaml: str
    payload_dict: dict
    estimated_tokens: int


def build_optimization_payload_v2(
    code: str,
    query_type: str = "sql",
    file_name: str = "query",
    issues: Optional[list[dict]] = None,
    schema_context: Optional[dict] = None,
    execution_plan: Optional[dict] = None,
    explain_analyze_text: Optional[str] = None,
    plan_summary: Optional[dict] = None,
    plan_issues: Optional[list[dict]] = None,
    query_graph: Optional[dict] = None,
    engine_info: Optional[dict] = None,
    constraints: Optional[dict] = None,
    relational_guarantees: Optional[list[str]] = None,
    recommended_actions: Optional[dict] = None,
    use_patch_mode: bool = False,
) -> PayloadV2Result:
    """Build structured YAML payload for SQL optimization.

    Args:
        code: Original SQL to optimize
        query_type: 'sql' or 'dax'
        file_name: Name of the query file
        issues: AST-detected issues
        schema_context: Database schema with tables, columns, indexes
        execution_plan: Parsed execution plan dict
        explain_analyze_text: Raw EXPLAIN ANALYZE output text
        plan_summary: Summarized plan metrics
        plan_issues: Plan-detected performance issues
        query_graph: Data flow graph from SQL parser
        engine_info: Database engine metadata
        constraints: Query constraints (preserve, permitted, forbidden)
        relational_guarantees: FK relationships and uniqueness constraints
        recommended_actions: Catalog of optimization actions
        use_patch_mode: If True, request patch-based response format

    Returns:
        PayloadV2Result with YAML string, dict, and token estimate
    """
    issues = issues or []
    constraints = constraints or {}

    # Build input SQL section first to get filters
    input_sql_section = _build_input_sql_section(code, issues)
    filters = input_sql_section.get("referenced_filters", [])

    # Build structured payload - only include sections with actual data
    payload = {
        "payload_version": PAYLOAD_VERSION,
    }

    # Engine - only if provided
    engine_section = _build_engine_section(engine_info)
    if engine_section:
        payload["engine"] = engine_section

    payload["task"] = _build_task_section(file_name)
    payload["constraints"] = _build_constraints_section(constraints)
    payload["input_sql"] = input_sql_section

    # Schema - only if provided
    schema_section = _build_schema_section(schema_context)
    if schema_section.get("tables"):
        payload["schema"] = schema_section

    # Add relational guarantees if provided or inferable
    if relational_guarantees:
        payload["relational_guarantees"] = relational_guarantees
    elif schema_context:
        inferred = _infer_relational_guarantees(schema_context)
        if inferred:
            payload["relational_guarantees"] = inferred

    # Add performance evidence - only if we have actual data
    perf_evidence = _build_performance_evidence(
        execution_plan=execution_plan,
        explain_analyze_text=explain_analyze_text,
        plan_summary=plan_summary,
        plan_issues=plan_issues,
    )
    if perf_evidence:
        payload["performance_evidence"] = perf_evidence

    # Add data flow if available
    if query_graph:
        payload["data_flow"] = _build_data_flow_section(query_graph)

    # Extract joins from query_graph for recommended actions
    joins_for_actions = None
    if query_graph:
        joins_for_actions = query_graph.get("joins", [])

    # Add recommended actions catalog - deterministic based on actual data
    actions = _build_recommended_actions(
        recommended_actions, schema_context, plan_issues, filters, joins_for_actions
    )
    if actions.get("stats") or actions.get("data_layout"):
        payload["recommended_actions_catalog"] = actions

    # Add knowledge base patterns for high-signal issues only
    high_signal_issues = [i for i in (issues or []) if i.get("severity") in ("critical", "high")]
    knowledge_patterns = _build_knowledge_base_section(high_signal_issues, schema_context)
    if knowledge_patterns:
        payload["optimization_knowledge"] = knowledge_patterns

    # Add output requirements
    payload["output_requirements"] = _build_output_requirements()
    payload["response_format"] = _build_response_format(use_patch_mode)

    # Generate YAML
    yaml_str = yaml.dump(payload, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # Estimate tokens
    estimated_tokens = len(yaml_str) // 4

    return PayloadV2Result(
        payload_yaml=yaml_str,
        payload_dict=payload,
        estimated_tokens=estimated_tokens,
    )


def _build_engine_section(engine_info: Optional[dict]) -> dict:
    """Build engine metadata section from actual engine introspection."""
    if not engine_info:
        # Return None to signal caller must provide engine info
        return None

    return {
        "name": engine_info.get("name", "duckdb"),
        "version": engine_info.get("version"),
        "execution_mode": engine_info.get("execution_mode", "IN_MEMORY"),
        "settings_snapshot": {
            "threads": engine_info.get("threads"),
            "memory_limit": engine_info.get("memory_limit"),
            "temp_directory": engine_info.get("temp_directory", ""),
            "enable_optimizer": engine_info.get("enable_optimizer", True),
            "preserve_insertion_order": engine_info.get("preserve_insertion_order", False),
        }
    }


def get_duckdb_engine_info(connection=None) -> dict:
    """Introspect DuckDB engine settings. Call with active connection."""
    try:
        import duckdb
        conn = connection or duckdb.connect(":memory:")

        version = conn.execute("SELECT version()").fetchone()[0]

        # Get thread count - different methods for different versions
        try:
            threads = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        except Exception:
            try:
                threads = conn.execute("PRAGMA threads").fetchone()[0]
            except Exception:
                import os
                threads = os.cpu_count() or 1

        # Get memory limit
        try:
            memory_limit = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        except Exception:
            memory_limit = "auto"

        # Try to get temp directory
        try:
            temp_dir = conn.execute("SELECT current_setting('temp_directory')").fetchone()[0]
        except Exception:
            temp_dir = ""

        return {
            "name": "duckdb",
            "version": version,
            "execution_mode": "IN_MEMORY",
            "threads": int(threads) if isinstance(threads, (int, str)) and str(threads).isdigit() else threads,
            "memory_limit": str(memory_limit),
            "temp_directory": temp_dir or "",
            "enable_optimizer": True,
            "preserve_insertion_order": False,
        }
    except ImportError:
        return None
    except Exception as e:
        logger.warning(f"Failed to introspect DuckDB: {e}")
        return None


def _build_task_section(file_name: str) -> dict:
    """Build task definition section."""
    return {
        "goal": "produce_complete_optimized_sql_rewrite",
        "deliverables": [
            "optimized_sql",
            "verification_sql",
            "recommended_actions",
        ],
        "dialect": "duckdb_sql",
        "query_name": file_name,
    }


def _build_constraints_section(constraints: dict) -> dict:
    """Build structured constraints section."""
    preserve = constraints.get("preserve", {})
    permitted = constraints.get("permitted", [])
    forbidden = constraints.get("forbidden", [])

    # Map string permissions to structured categories
    permitted_transforms = []
    if "Predicate pushdown" in permitted or not permitted:
        permitted_transforms.append("predicate_pushdown")
    if "Join reordering" in permitted or not permitted:
        permitted_transforms.append("join_reordering")
    permitted_transforms.extend([
        "derived_table_prefilter",  # Prefilter dimension tables in subqueries
        "projection_pruning",       # Remove unused columns internally
    ])
    # Note: CTE usage omitted - DuckDB materializes CTEs by default which can hurt performance

    forbidden_transforms = [
        "remove_or_weaken_filters",
        "add_row_count_limit",              # No LIMIT/TOP/QUALIFY unless in original
        "change_join_types",                # No INNER<->LEFT conversion
        "add_distinct_or_groupby",          # Unless present in original
        "change_null_semantics",            # No COALESCE/IFNULL unless already present
        "introduce_randomness_or_sampling", # No random(), no sampling
        "remove_required_order_by",         # Do not remove ORDER BY from original
        "add_order_by_keys",                # Do not add tiebreaker keys (changes semantics)
    ]

    return {
        "must_preserve": {
            "output_columns": preserve.get("output_columns", []),
            "row_count_exact": preserve.get("row_count"),
            "sort_order": preserve.get("sort_order", ""),
        },
        "permitted_transformations": permitted_transforms,
        "forbidden_transformations": forbidden_transforms,
    }


def _normalize_date_literal(expr: str) -> str:
    """Normalize date literals to typed form: '2024-01-01' -> DATE '2024-01-01'."""
    import re
    # Match date-like strings that aren't already typed
    # Pattern: 'YYYY-MM-DD' not preceded by DATE/TIMESTAMP
    pattern = r"(?<!\bDATE\s)(?<!\bTIMESTAMP\s)'(\d{4}-\d{2}-\d{2})'"
    return re.sub(pattern, r"DATE '\1'", expr)


def _extract_filters_from_sql(code: str) -> list[dict]:
    """Extract WHERE clause predicates deterministically from SQL."""
    import re
    filters = []

    # Find WHERE clause
    where_match = re.search(
        r'\bWHERE\s+(.+?)(?=\s+GROUP\s+BY|\s+ORDER\s+BY|\s+LIMIT|\s+HAVING|\s*;|\s*$)',
        code,
        re.IGNORECASE | re.DOTALL
    )

    if where_match:
        predicates_text = where_match.group(1).strip()
        # Split on AND (but not inside parentheses)
        # Simple split - handles most cases
        parts = re.split(r'\s+AND\s+', predicates_text, flags=re.IGNORECASE)

        for i, pred in enumerate(parts):
            pred = pred.strip()
            if not pred:
                continue

            # Normalize date literals to typed form
            pred_normalized = _normalize_date_literal(pred)

            # Classify filter type
            filter_type = "UNKNOWN"
            if re.search(r'>=|<=|>|<|BETWEEN', pred, re.IGNORECASE):
                filter_type = "RANGE"
            elif re.search(r'\s*=\s*', pred):
                filter_type = "EQUALITY"
            elif re.search(r'\bIN\s*\(', pred, re.IGNORECASE):
                filter_type = "IN_LIST"
            elif re.search(r'\bLIKE\b', pred, re.IGNORECASE):
                filter_type = "LIKE"
            elif re.search(r'\bIS\s+(NOT\s+)?NULL\b', pred, re.IGNORECASE):
                filter_type = "NULL_CHECK"

            filters.append({
                "id": f"F-{filter_type}-{i+1:03d}",
                "expression": pred_normalized,
                "type": filter_type,
            })

    return filters


def _build_input_sql_section(code: str, issues: list[dict]) -> dict:
    """Build input SQL section with referenced filters extracted deterministically."""
    referenced_filters = _extract_filters_from_sql(code)

    result = {
        "sql_text": code,
        "referenced_filters": referenced_filters,
    }

    # Add only HIGH-SIGNAL anti-patterns to prompt (critical/high only)
    # These have >40% optimizer hit rate based on TPC-DS benchmarks
    # Lower severity patterns are logged for user but not sent to LLM
    if issues:
        high_signal = [
            i for i in issues
            if i.get("severity") in ("critical", "high")
        ]
        if high_signal:
            result["detected_issues"] = [
                {
                    "rule_id": i.get("rule_id", "UNKNOWN"),
                    "severity": i.get("severity"),
                    "title": i.get("title", i.get("description", "")),
                    "suggestion": i.get("suggestion", ""),
                }
                for i in high_signal
            ]

    return result


def _build_schema_section(schema_context: Optional[dict]) -> dict:
    """Build schema section with storage information - all values deterministic."""
    if not schema_context:
        return {"tables": []}

    tables = []
    for table in schema_context.get("tables", []):
        columns = []
        for col in table.get("columns", []):
            col_def = {
                "name": col.get("name"),
                "type": col.get("type") or col.get("data_type"),
            }
            # Only add if present - no placeholders
            cardinality = col.get("cardinality") or col.get("distinct_count")
            if cardinality is not None:
                col_def["distinct"] = cardinality
            if col.get("null_ratio") is not None:
                col_def["null_rate"] = col.get("null_ratio")
            columns.append(col_def)

        table_def = {
            "name": table.get("name"),
            "row_count": table.get("row_count", 0),
            "columns": columns,
        }

        # Primary key - only if present
        if table.get("primary_key"):
            pk = table["primary_key"]
            table_def["primary_key"] = pk if isinstance(pk, list) else [pk]

        # Indexes - only if present
        if table.get("indexes"):
            table_def["indexes"] = table["indexes"]

        # Storage info - only include if actually known
        storage = {}
        if table.get("storage_kind"):
            storage["kind"] = table["storage_kind"]
        if table.get("storage_location"):
            storage["location"] = table["storage_location"]
        if table.get("ordering_keys"):
            storage["ordering_keys"] = table["ordering_keys"]
        if table.get("partitioning_keys"):
            storage["partitioning_keys"] = table["partitioning_keys"]

        if storage:
            table_def["storage"] = storage

        tables.append(table_def)

    return {"tables": tables}


def _infer_relational_guarantees(schema_context: dict) -> list[str]:
    """Infer relational guarantees from schema deterministically."""
    guarantees = []
    tables = schema_context.get("tables", [])
    table_map = {t.get("name", "").lower(): t for t in tables}

    # First: uniqueness constraints from primary keys
    for table in tables:
        name = table.get("name", "")
        pk = table.get("primary_key")
        if pk:
            pk_cols = pk if isinstance(pk, list) else [pk]
            guarantees.append(f"{name}.{pk_cols[0]} is unique")

    # Second: FK relationships from naming patterns
    for table in tables:
        table_name = table.get("name", "")
        table_name_lower = table_name.lower()

        for col in table.get("columns", []):
            col_name = col.get("name", "")
            col_name_lower = col_name.lower()

            # Skip if this is a primary key of the same table (not an FK)
            pk = table.get("primary_key", [])
            pk_list = pk if isinstance(pk, list) else [pk]
            if col_name in pk_list:
                continue

            # Check for _id suffix that might be FK
            if col_name_lower.endswith("_id"):
                potential_table = col_name_lower[:-3]  # Remove _id

                # Try singular and plural forms, but NOT the same table
                target_name = None
                target_pk = None
                for variant in [potential_table, f"{potential_table}s"]:
                    if variant in table_map and variant != table_name_lower:
                        target_table = table_map[variant]
                        target_name = target_table.get("name")
                        target_pk = target_table.get("primary_key")
                        break

                if target_name and target_pk:
                    pk_col = target_pk[0] if isinstance(target_pk, list) else target_pk
                    fk_guarantee = f"{table_name}.{col_name} -> {target_name}.{pk_col} is many-to-one"
                    if fk_guarantee not in guarantees:
                        guarantees.append(fk_guarantee)

    return guarantees


def _build_performance_evidence(
    execution_plan: Optional[dict],
    explain_analyze_text: Optional[str],
    plan_summary: Optional[dict],
    plan_issues: Optional[list[dict]],
) -> Optional[dict]:
    """Build performance evidence section - only with actual data, no placeholders."""
    if not execution_plan and not explain_analyze_text and not plan_summary:
        return None

    evidence = {"plan_primary": {}}

    # Add raw EXPLAIN ANALYZE text only if provided
    if explain_analyze_text:
        evidence["plan_primary"]["explain_analyze_text"] = explain_analyze_text

    # Add plan summary metrics
    if plan_summary:
        if plan_summary.get("plan_hash"):
            evidence["plan_primary"]["plan_hash"] = plan_summary["plan_hash"]
        if plan_summary.get("total_time_ms") is not None:
            evidence["plan_primary"]["total_time_ms"] = plan_summary["total_time_ms"]

        # Top operators
        top_ops = []
        for op in plan_summary.get("top_operators", []):
            op_entry = {"op": op.get("op")}
            if op.get("table"):
                op_entry["table"] = op["table"]
            if op.get("time_ms") is not None:
                op_entry["time_ms"] = op["time_ms"]
            if op.get("rows_scanned"):
                op_entry["rows_scanned"] = op["rows_scanned"]
            if op.get("rows_out"):
                op_entry["rows_out"] = op["rows_out"]
            if op.get("cost_pct"):
                op_entry["cost_pct"] = op["cost_pct"]
            top_ops.append(op_entry)
        if top_ops:
            evidence["plan_primary"]["top_operators"] = top_ops

        # Cardinality misestimates
        misest = []
        for m in plan_summary.get("misestimates", []):
            misest.append({
                "op": m.get("op"),
                "estimated": m.get("estimated"),
                "actual": m.get("actual"),
                "ratio": m.get("ratio"),
            })
        if misest:
            evidence["plan_primary"]["cardinality_misestimates"] = misest

    elif execution_plan:
        # Fall back to execution_plan dict
        if execution_plan.get("execution_time_ms") is not None:
            evidence["plan_primary"]["total_time_ms"] = execution_plan["execution_time_ms"]
        if execution_plan.get("actual_rows") is not None:
            evidence["plan_primary"]["actual_rows"] = execution_plan["actual_rows"]
        if execution_plan.get("bottleneck"):
            bn = execution_plan["bottleneck"]
            if isinstance(bn, dict):
                evidence["plan_primary"]["bottleneck"] = {
                    "operator": bn.get("operator"),
                    "cost_pct": bn.get("cost_pct"),
                }
            else:
                evidence["plan_primary"]["bottleneck"] = bn

    # Add plan issues as annotations
    if plan_issues:
        evidence["plan_issues"] = [
            {
                "id": pi.get("rule_id"),
                "issue": pi.get("name"),
                "severity": pi.get("severity"),
                "location": pi.get("location"),
                "suggestion": pi.get("suggestion"),
            }
            for pi in plan_issues
        ]

    return evidence if evidence.get("plan_primary") else None


def _build_data_flow_section(query_graph: dict) -> dict:
    """Build data flow section from query graph."""
    data_flow = query_graph.get("data_flow", {})

    return {
        "execution_order": data_flow.get("execution_order", []),
        "cte_dependencies": data_flow.get("cte_edges", []),
        "base_tables": data_flow.get("base_tables", []),
        "joins": [
            {
                "left": j.get("left", ""),
                "right": j.get("right", ""),
                "type": j.get("type", "INNER"),
                "operator": j.get("operator", "="),
            }
            for j in query_graph.get("joins", [])
        ],
        "summary": query_graph.get("summary", {}),
    }


def _build_recommended_actions(
    recommended_actions: Optional[dict],
    schema_context: Optional[dict],
    plan_issues: Optional[list[dict]],
    filters: Optional[list[dict]] = None,
    joins: Optional[list[dict]] = None,
) -> dict:
    """Build recommended actions catalog - deterministic based on actual data.

    DuckDB-specific: Recommends ordered copies for zonemap pruning rather than
    traditional indexes, since DuckDB ART indexes don't help joins/aggregations.
    """
    if recommended_actions:
        return recommended_actions

    actions = {
        "stats": [],
        "data_layout": [],
        "notes": [
            "DuckDB ART indexes are limited to selective equality/IN probes; "
            "do not recommend new indexes unless query has highly selective filters."
        ],
    }

    # Add ANALYZE for all tables in schema
    if schema_context:
        for table in schema_context.get("tables", []):
            table_name = table.get("name")
            if table_name:
                actions["stats"].append(f"ANALYZE {table_name}")

    # Build data layout suggestions based on plan issues, filters, and joins
    if plan_issues and schema_context:
        for i, issue in enumerate(plan_issues):
            location = issue.get("location", "")
            issue_name = issue.get("name", "").lower()

            # Find which table this issue relates to
            for t in schema_context.get("tables", []):
                table_name = t.get("name", "")
                if table_name.lower() in location.lower():
                    # Collect ordering columns: filter columns first, then join columns
                    order_cols = []
                    join_cols = []

                    # Extract filter columns
                    if filters:
                        for f in filters:
                            expr = f.get("expression", "")
                            for col in t.get("columns", []):
                                col_name = col.get("name", "")
                                # Check if column appears in filter (with table alias or without)
                                if col_name.lower() in expr.lower():
                                    if col_name not in order_cols:
                                        order_cols.append(col_name)

                    # Extract join columns for this table
                    if joins:
                        for j in joins:
                            left = j.get("left", "")
                            right = j.get("right", "")
                            # Check if this table is involved in the join
                            if table_name.lower() in left.lower():
                                col = left.split(".")[-1] if "." in left else left
                                if col not in order_cols and col not in join_cols:
                                    join_cols.append(col)
                            if table_name.lower() in right.lower():
                                col = right.split(".")[-1] if "." in right else right
                                if col not in order_cols and col not in join_cols:
                                    join_cols.append(col)

                    # Combine: filter columns first, then join columns
                    all_order_cols = order_cols + join_cols

                    # DuckDB-specific: recommend ordered copy for scan issues
                    if "seq" in issue_name or "scan" in issue_name:
                        if all_order_cols:
                            actions["data_layout"].append({
                                "id": f"DL-ORDER-{i+1:03d}",
                                "action": "CREATE_ORDERED_COPY",
                                "table": table_name,
                                "order_by": all_order_cols,
                                "purpose": "improve_zonemap_selectivity_and_row_group_pruning",
                                "rationale": (
                                    f"Sorting {table_name} by filter/join columns enables "
                                    "DuckDB to skip row groups via zonemap pruning"
                                ),
                            })

                    if "cardinality" in issue_name or "misestimate" in issue_name:
                        # Ensure ANALYZE is first for this table
                        analyze_cmd = f"ANALYZE {table_name}"
                        if analyze_cmd in actions["stats"]:
                            actions["stats"].remove(analyze_cmd)
                            actions["stats"].insert(0, analyze_cmd)
                    break

    return actions


def _build_output_requirements() -> dict:
    """Build output requirements section."""
    return {
        "optimized_sql_must": [
            "return_exact_same_rows",
            "include_required_order_by",
            "avoid_engine_specific_hints",
        ],
        "verification_harness_must": [
            "prove_row_count_equal",
            "prove_multiset_equality",
            "prove_ordering_spec_present",
        ],
    }


def _build_response_format(use_patch_mode: bool) -> dict:
    """Build explicit response format instructions for the LLM."""
    if use_patch_mode:
        return {
            "type": "patches",
            "format": "json",
            "schema": {
                "patches": [
                    {
                        "issue_id": "SQL-XXX",
                        "search": "exact text to find in original SQL",
                        "replace": "replacement text",
                        "line_hint": 0,
                        "description": "What was fixed",
                    }
                ],
                "explanation": "Brief explanation of changes",
            },
            "example": {
                "patches": [
                    {
                        "issue_id": "SQL-JOIN-001",
                        "search": "FROM customers, orders\nWHERE customers.id = orders.customer_id",
                        "replace": "FROM customers\nINNER JOIN orders ON customers.id = orders.customer_id\nWHERE 1=1",
                        "description": "Convert comma join to explicit INNER JOIN",
                    },
                    {
                        "issue_id": "SQL-SEL-001",
                        "search": "SELECT *",
                        "replace": "SELECT id, name, email",
                        "description": "Replace SELECT * with explicit columns",
                    },
                ],
                "explanation": "Converted implicit joins to explicit INNER JOINs and replaced SELECT *",
            },
            "rules": [
                "search_must_be_EXACT_text_from_original_sql_including_whitespace_and_newlines",
                "copy_search_text_character_for_character_from_input_sql",
                "each_patch_fixes_one_issue",
                "patches_applied_in_order",
                "return_valid_json_only_no_markdown",
            ],
            "forbidden_patterns_in_replacements": [
                "SELECT * - NEVER introduce SELECT * in your patches, always preserve explicit column lists",
                "CROSS JOIN - NEVER introduce CROSS JOIN unless it was in the original",
                "Removing columns - preserve all columns in SELECT clauses",
                "Changing column aliases - preserve original alias names",
            ],
            "critical_warning": "If the original SQL has explicit columns (like 'select col1, col2'), you MUST preserve those exact columns. NEVER replace explicit columns with SELECT *. This will cause validation to fail.",
        }

    return {
        "type": "optimized_sql",
        "format": "json",
        "schema": {
            "optimized_sql": "<complete optimized SQL>",
            "explanation": "Brief explanation of changes",
            "changes": [
                {"rule_id": "SQL-XXX", "description": "What was fixed"}
            ],
        },
        "rules": [
            "return_only_json_no_markdown",
            "optimized_sql_must_be_complete_and_valid",
            "preserve_query_semantics",
        ],
    }


# Convenience function for markdown output (backwards compatibility)
def payload_v2_to_markdown(payload: dict) -> str:
    """Convert v2 payload dict to markdown format."""
    lines = [f"# SQL Optimization Payload (v2)\n"]
    lines.append(f"**Version:** {payload.get('payload_version', 'unknown')}\n")

    # Engine
    engine = payload.get("engine", {})
    lines.append(f"## Engine\n")
    lines.append(f"- **Name:** {engine.get('name', '?')}")
    lines.append(f"- **Version:** {engine.get('version', '?')}")
    lines.append(f"- **Mode:** {engine.get('execution_mode', '?')}\n")

    # Task
    task = payload.get("task", {})
    lines.append(f"## Task\n")
    lines.append(f"- **Goal:** {task.get('goal', '?')}")
    lines.append(f"- **Query:** {task.get('query_name', '?')}\n")

    # Constraints
    constraints = payload.get("constraints", {})
    preserve = constraints.get("must_preserve", {})
    lines.append(f"## Constraints\n")
    lines.append(f"**Must Preserve:**")
    lines.append(f"- Output columns: {preserve.get('output_columns', [])}")
    lines.append(f"- Row count: {preserve.get('row_count_exact', '?')}")
    lines.append(f"- Sort order: {preserve.get('sort_order', '?')}\n")

    # SQL
    input_sql = payload.get("input_sql", {})
    lines.append(f"## SQL\n```sql\n{input_sql.get('sql_text', '')}\n```\n")

    # Schema
    schema = payload.get("schema", {})
    lines.append(f"## Schema\n")
    for table in schema.get("tables", []):
        lines.append(f"**{table.get('name')}** (~{table.get('row_count', 0):,} rows)")
        for col in table.get("columns", []):
            distinct = f" — {col.get('distinct', '?'):,} distinct" if col.get("distinct") else ""
            lines.append(f"  - `{col.get('name')}` {col.get('type')}{distinct}")
        lines.append("")

    # Performance Evidence
    perf = payload.get("performance_evidence", {})
    plan = perf.get("plan_primary", {})
    lines.append(f"## Performance Evidence\n")
    lines.append(f"- **Total Time:** {plan.get('total_time_ms', 0):,.1f}ms")
    lines.append(f"- **Plan Hash:** {plan.get('plan_hash', '?')}\n")

    if plan.get("top_operators"):
        lines.append("### Top Operators")
        lines.append("| Operator | Table | Time (ms) | Rows Out |")
        lines.append("|----------|-------|-----------|----------|")
        for op in plan.get("top_operators", []):
            lines.append(f"| {op.get('op')} | {op.get('table', '-')} | {op.get('time_ms', 0):,.1f} | {op.get('rows_out', 0):,} |")
        lines.append("")

    # Recommended Actions
    actions = payload.get("recommended_actions_catalog", {})
    lines.append(f"## Recommended Actions\n")
    lines.append("**Stats:**")
    for stat in actions.get("stats", []):
        lines.append(f"- `{stat}`")
    lines.append("\n**Data Layout:**")
    for dl in actions.get("data_layout", []):
        lines.append(f"- {dl.get('id')}: {dl.get('action')} on `{dl.get('table')}`")

    return "\n".join(lines)
