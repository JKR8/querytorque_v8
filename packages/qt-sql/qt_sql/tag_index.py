"""Tag-based example index builder for the qt_sql knowledge base.

Builds a tag similarity index from gold examples AND regression examples
in qt_sql/examples/. Each example's original SQL is parsed for keyword/table
tags and indexed for overlap-based matching.

Gold examples (type=gold): proven rewrites to emulate
Regression examples (type=regression): failed rewrites to avoid

Usage:
    python -m qt_sql.tag_index          # Build index
    python -m qt_sql.tag_index --stats  # Show index stats
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).resolve().parent
EXAMPLES_DIR = BASE_DIR / "examples"
MODELS_DIR = BASE_DIR / "models"
TAGS_FILE = MODELS_DIR / "similarity_tags.json"
METADATA_FILE = MODELS_DIR / "similarity_metadata.json"


# =============================================================================
# SQL Normalizer (fingerprinting for similarity)
# =============================================================================

class SQLNormalizer:
    """Normalize SQL queries for similarity comparison.

    Applies transformations based on Percona fingerprinting:
    - Replace all literals (strings, numbers, dates) with placeholders
    - Normalize identifiers to lowercase
    - Remove comments
    - Normalize whitespace

    This ensures structurally similar queries produce similar vectors
    regardless of specific literal values or identifier casing.
    """

    def __init__(self):
        self._placeholder_counter = 0

    def normalize(self, sql: str, dialect: str = "postgres") -> str:
        """Normalize SQL query for similarity comparison.

        Args:
            sql: Raw SQL query
            dialect: SQL dialect for parsing

        Returns:
            Normalized SQL with literals replaced by placeholders
        """
        try:
            import sqlglot
            from sqlglot import exp
            from sqlglot.optimizer import normalize_identifiers

            # Parse SQL
            ast = sqlglot.parse_one(sql, dialect=dialect)

            # Reset placeholder counter for each query
            self._placeholder_counter = 0

            # Replace all literals with placeholders
            ast = ast.transform(self._replace_literals)

            # Normalize identifiers to lowercase
            ast = normalize_identifiers.normalize_identifiers(ast, dialect=dialect)

            # Generate normalized SQL
            normalized = ast.sql(dialect=dialect)

            # Additional whitespace normalization
            normalized = re.sub(r'\s+', ' ', normalized).strip()

            return normalized

        except Exception as e:
            logger.warning(f"SQL normalization failed: {e}")
            # Fallback: basic whitespace normalization
            return re.sub(r'\s+', ' ', sql).strip()

    def _replace_literals(self, node):
        """Replace literal values with neutral constants.

        Uses 0 for numbers and 'x' for strings instead of dialect-specific
        parameter placeholders ($1, %s, ?). This ensures the fingerprinted
        SQL can be parsed back by any dialect for AST vectorization.

        Preserves INTERVAL literals (e.g., '30 day') since replacing them
        produces invalid SQL that can't be re-parsed.
        """
        from sqlglot import exp

        if isinstance(node, exp.Literal):
            # Skip literals inside INTERVAL expressions (e.g., INTERVAL '30 day')
            if node.parent and isinstance(node.parent, exp.Interval):
                return node
            if node.is_string:
                return exp.Literal.string("x")
            return exp.Literal.number(0)

        if isinstance(node, exp.Null):
            return exp.Literal.number(0)

        return node


# =============================================================================
# Tag Extraction
# =============================================================================

# SQL keywords to detect as tags
_SQL_KEYWORDS = {
    "intersect", "except", "union", "rollup", "cube", "grouping",
    "exists", "case", "having", "distinct", "lateral", "recursive",
    "between", "like", "in",
}

# Window function keywords
_WINDOW_KEYWORDS = {"window", "rank", "row_number", "dense_rank", "ntile", "lead", "lag"}


# TPC-DS multi-channel fact tables
_CHANNEL_FACT_TABLES = {"store_sales", "catalog_sales", "web_sales"}

# TPC-DS column prefix → table name (for resolving unqualified columns)
_COL_PREFIX_TO_TABLE = {
    "ss_": "store_sales", "cs_": "catalog_sales", "ws_": "web_sales",
    "sr_": "store_returns", "cr_": "catalog_returns", "wr_": "web_returns",
    "inv_": "inventory",
    "i_": "item", "d_": "date_dim", "s_": "store", "c_": "customer",
    "ca_": "customer_address", "cd_": "customer_demographics",
    "hd_": "household_demographics", "w_": "warehouse", "p_": "promotion",
    "t_": "time_dim", "ib_": "income_band", "r_": "reason",
    "sm_": "ship_mode", "wp_": "web_page", "cp_": "catalog_page",
    "cc_": "call_center",
}


def _resolve_column_table(col_name: str) -> str:
    """Resolve an unqualified TPC-DS column name to its table via prefix.

    Returns table name or empty string if no match.
    Matches longest prefix first to avoid ambiguity (e.g. 'inv_' before 'i_').
    """
    col_lower = col_name.lower()
    # Sort by prefix length descending so longer prefixes match first
    for prefix in sorted(_COL_PREFIX_TO_TABLE, key=len, reverse=True):
        if col_lower.startswith(prefix):
            return _COL_PREFIX_TO_TABLE[prefix]
    return ""


def extract_tags(sql: str, dialect: str = "duckdb") -> Set[str]:
    """Extract tags from SQL using AST analysis with regex fallback.

    Tags include:
    - Table names (lowercased)
    - SQL keywords present (intersect, union, rollup, etc.)
    - Structural patterns (self_join, repeated_scan, multi_cte, correlated_subquery)
    - OR column analysis (or_same_col, or_cross_col, or_branch_count:N)
    - Table repeat by name (repeat:table_name:N)
    - Correlated vs independent subqueries (correlated_sub, independent_sub)
    - Multi-channel pattern (multi_channel)
    - CTE filter status (cte_filtered, cte_unfiltered)
    - Star join pattern (star_join_pattern)
    - LEFT JOIN with right-table WHERE filter (left_join_right_filter)

    Args:
        sql: SQL query text
        dialect: SQL dialect for parsing

    Returns:
        Set of tag strings
    """
    tags: Set[str] = set()

    try:
        import sqlglot
        from sqlglot import exp

        ast = sqlglot.parse_one(sql, dialect=dialect)

        # 1. Table names
        table_names = []
        for t in ast.find_all(exp.Table):
            name = t.name.lower() if t.name else ""
            if name:
                table_names.append(name)
                tags.add(name)

        # 2. SQL keywords via AST node types
        if list(ast.find_all(exp.Intersect)):
            tags.add("intersect")
        if list(ast.find_all(exp.Except)):
            tags.add("except")
        if list(ast.find_all(exp.Union)):
            tags.add("union")
        if list(ast.find_all(exp.Exists)):
            tags.add("exists")
        if list(ast.find_all(exp.Case)):
            tags.add("case")
        if list(ast.find_all(exp.Having)):
            tags.add("having")
        if list(ast.find_all(exp.Distinct)):
            tags.add("distinct")
        if list(ast.find_all(exp.Between)):
            tags.add("between")
        if list(ast.find_all(exp.Like)):
            tags.add("like")
        if list(ast.find_all(exp.In)):
            tags.add("in")
        if list(ast.find_all(exp.Window)):
            tags.add("window")
        if list(ast.find_all(exp.Subquery)):
            tags.add("subquery")
        if list(ast.find_all(exp.CTE)):
            tags.add("cte")
        if list(ast.find_all(exp.Join)):
            tags.add("join")
        if list(ast.find_all(exp.Group)):
            tags.add("group_by")
        if list(ast.find_all(exp.AggFunc)):
            tags.add("aggregate")
        if list(ast.find_all(exp.Order)):
            tags.add("order_by")

        # Window function subtypes — guarded because some sqlglot versions
        # lack exp.Rank / exp.RowNumber as standalone node types
        for _attr, _tag in [("Rank", "rank"), ("RowNumber", "row_number"),
                            ("DenseRank", "dense_rank")]:
            cls = getattr(exp, _attr, None)
            if cls and list(ast.find_all(cls)):
                tags.add(_tag)

        # ROLLUP / CUBE / GROUPING — check via SQL text since sqlglot
        # represents these differently across versions
        sql_upper = sql.upper()
        if "ROLLUP" in sql_upper:
            tags.add("rollup")
        if "CUBE" in sql_upper:
            tags.add("cube")
        if "GROUPING" in sql_upper:
            tags.add("grouping")
        if "LATERAL" in sql_upper:
            tags.add("lateral")
        if re.search(r'\bRECURSIVE\b', sql_upper):
            tags.add("recursive")

        # 3. Structural patterns
        # Self-join: same table appears more than once
        name_counts = Counter(table_names)
        for name, count in name_counts.items():
            if count > 1:
                tags.add("self_join")
                break

        # Repeated scan: a table appears 3+ times
        for name, count in name_counts.items():
            if count >= 3:
                tags.add("repeated_scan")
                break

        # Multi-CTE: 2+ CTEs
        ctes = list(ast.find_all(exp.CTE))
        if len(ctes) >= 2:
            tags.add("multi_cte")

        # Correlated subquery: subquery referencing outer columns
        all_tables = set(table_names)
        for sq in ast.find_all(exp.Subquery):
            sq_tables = {t.name.lower() for t in sq.find_all(exp.Table) if t.name}
            # Also collect aliases inside the subquery
            sq_aliases = set()
            for t in sq.find_all(exp.Table):
                if t.alias:
                    sq_aliases.add(t.alias.lower())
            sq_scope = sq_tables | sq_aliases
            outer_tables = all_tables - sq_tables
            for col in sq.find_all(exp.Column):
                tbl = col.table.lower() if col.table else ""
                if tbl:
                    # Qualified column: if table/alias not in subquery scope,
                    # it references an outer table
                    if tbl not in sq_scope:
                        tags.add("correlated_subquery")
                        break
                else:
                    # Unqualified column: resolve via prefix map
                    resolved = _resolve_column_table(col.name) if col.name else ""
                    if resolved and resolved not in sq_tables and resolved in outer_tables:
                        tags.add("correlated_subquery")
                        break
            if "correlated_subquery" in tags:
                break

        # Left/outer join
        for j in ast.find_all(exp.Join):
            j_str = str(j).lower()
            if "left" in j_str:
                tags.add("left_join")
            if "outer" in j_str:
                tags.add("outer_join")
            if "cross" in j_str:
                tags.add("cross_join")

        # ─── NEW FEATURE 1: OR Column Analysis ───
        # Discriminates or_to_union (cross-col) vs engine-handles-it (same-col)
        _extract_or_column_tags(ast, tags)

        # ─── NEW FEATURE 2: Table Repeat By Name ───
        # Emits repeat:table_name:N for tables appearing 2+ times
        for name, count in name_counts.items():
            if count >= 2:
                tags.add(f"repeat:{name}:{count}")

        # ─── NEW FEATURE 3: Correlated vs Independent Subqueries ───
        # Finer-grained than the binary correlated_subquery tag above
        _extract_subquery_correlation_tags(ast, tags)

        # ─── NEW FEATURE 4: Multi-Channel Pattern ───
        # Detects TPC-DS store/catalog/web sales co-occurrence
        channel_tables = set(table_names) & _CHANNEL_FACT_TABLES
        if len(channel_tables) >= 2:
            tags.add("multi_channel")

        # ─── NEW FEATURE 5: CTE Filter Status ───
        # Detects filtered vs unfiltered CTEs (unfiltered = anti-pattern)
        _extract_cte_filter_tags(ast, ctes, tags)

        # ─── NEW FEATURE 7: Star Join Pattern ───
        # Detects star-schema pattern: 1 fact table + 3+ dimension tables
        _extract_star_join_tags(ast, table_names, tags)

        # ─── NEW FEATURE 8: LEFT JOIN with right-table WHERE filter ───
        # Detects inner_join_conversion opportunity pattern
        _extract_left_join_filter_tags(ast, tags)

    except Exception as e:
        logger.debug(f"AST tag extraction failed, falling back to regex: {e}")
        # Regex fallback for fragments
        _extract_tags_regex(sql, tags)

    # Always do regex fallback for keywords AST might miss
    _extract_tags_regex_keywords(sql, tags)

    return tags


# =============================================================================
# New Feature Helpers (Features 1-5)
# =============================================================================

def _extract_or_column_tags(ast, tags: Set[str]) -> None:
    """Feature 1: OR Column Analysis.

    Walks OR nodes and checks if branches reference the same column
    or different columns. This discriminates:
    - or_same_col: Engine handles efficiently in one scan (don't split)
    - or_cross_col: Candidate for or_to_union transform
    Also emits or_branch_count:N for the total OR branch count.
    """
    from sqlglot import exp

    or_nodes = list(ast.find_all(exp.Or))
    if not or_nodes:
        return

    # Count total OR branches (chain of ORs = N+1 branches for N OR nodes)
    # Walk the OR tree to find root OR chains
    branch_count = _count_or_branches(ast)
    if branch_count >= 2:
        tags.add(f"or_branch_count:{branch_count}")

    # Analyze column references in OR branches
    has_same_col = False
    has_cross_col = False

    for or_node in or_nodes:
        left_cols = _get_comparison_columns(or_node.left)
        right_cols = _get_comparison_columns(or_node.right)

        if left_cols and right_cols:
            # Compare column names (ignore table qualifiers for matching)
            left_names = {c.split(".")[-1] for c in left_cols}
            right_names = {c.split(".")[-1] for c in right_cols}
            if left_names & right_names:
                has_same_col = True
            else:
                has_cross_col = True

    if has_same_col:
        tags.add("or_same_col")
    if has_cross_col:
        tags.add("or_cross_col")


def _count_or_branches(ast) -> int:
    """Count the number of OR branches in the largest OR chain.

    A chain like (A OR B OR C) is represented as OR(OR(A, B), C),
    so we walk left-recursively to count leaf branches.
    """
    from sqlglot import exp

    def _chain_size(node) -> int:
        if not isinstance(node, exp.Or):
            return 1
        return _chain_size(node.left) + _chain_size(node.right)

    max_branches = 0
    for or_node in ast.find_all(exp.Or):
        # Only count from root ORs (parent is not OR)
        if not isinstance(or_node.parent, exp.Or):
            size = _chain_size(or_node)
            max_branches = max(max_branches, size)
    return max_branches


def _get_comparison_columns(node) -> Set[str]:
    """Extract column names referenced in a comparison expression.

    Returns fully qualified names (table.column) where available,
    or just column name if unqualified.
    """
    from sqlglot import exp

    cols = set()
    # Direct comparison (e.g., col = 'value')
    for col in node.find_all(exp.Column):
        name = col.name.lower() if col.name else ""
        if not name:
            continue
        tbl = col.table.lower() if col.table else ""
        cols.add(f"{tbl}.{name}" if tbl else name)
    return cols


def _extract_subquery_correlation_tags(ast, tags: Set[str]) -> None:
    """Feature 3: Correlated vs Independent Subquery analysis.

    For each subquery, determines if it's correlated (references outer scope)
    or independent (self-contained). Also tracks nesting depth.
    Handles both qualified (table.col) and unqualified (col) column refs
    using TPC-DS column prefix resolution.

    Also detects scalar aggregate subqueries used in comparisons
    (the P3 decorrelation pattern).

    Emits:
    - correlated_sub: At least one correlated subquery found
    - independent_sub: At least one independent subquery found
    - sub_nesting_depth:N: Maximum subquery nesting depth
    - scalar_agg_sub: Correlated scalar aggregate subquery that scans a raw
      fact table (P3 decorrelation opportunity — real win)
    - scalar_agg_sub_cte: Correlated scalar aggregate subquery that scans a
      CTE (NOT an opportunity — CTE is auto-materialized, decorrelation
      replaces a cheap scan with a cheap join)
    """
    from sqlglot import exp

    subqueries = list(ast.find_all(exp.Subquery))
    if not subqueries:
        return

    # All tables in the full query (for resolving outer references)
    all_tables = {t.name.lower() for t in ast.find_all(exp.Table) if t.name}

    # Collect CTE names defined in this query
    cte_names = set()
    for cte in ast.find_all(exp.CTE):
        if cte.alias:
            cte_names.add(cte.alias.lower())

    correlated_count = 0
    independent_count = 0
    max_depth = 0

    for sq in subqueries:
        # Calculate nesting depth
        depth = 0
        parent = sq.parent
        while parent:
            if isinstance(parent, exp.Subquery):
                depth += 1
            parent = getattr(parent, "parent", None)

        actual_depth = depth + 1
        max_depth = max(max_depth, actual_depth)

        # Check correlation: does subquery reference columns from outer scope?
        sq_tables = {t.name.lower() for t in sq.find_all(exp.Table) if t.name}
        sq_aliases = set()
        for t in sq.find_all(exp.Table):
            if t.alias:
                sq_aliases.add(t.alias.lower())
        sq_scope = sq_tables | sq_aliases
        outer_tables = all_tables - sq_tables
        is_correlated = False
        for col in sq.find_all(exp.Column):
            tbl = col.table.lower() if col.table else ""
            if tbl:
                if tbl not in sq_scope:
                    is_correlated = True
                    break
            else:
                # Unqualified: resolve via column prefix map
                resolved = _resolve_column_table(col.name) if col.name else ""
                if resolved and resolved not in sq_tables and resolved in outer_tables:
                    is_correlated = True
                    break

        if is_correlated:
            correlated_count += 1
        else:
            independent_count += 1

        # Scalar aggregate subquery: subquery contains aggregate AND
        # is used in a comparison (WHERE col > (SELECT avg(...) ...))
        has_agg = bool(list(sq.find_all(exp.AggFunc)))
        if has_agg:
            parent_node = sq.parent
            comparison_types = (exp.GT, exp.LT, exp.GTE, exp.LTE, exp.EQ, exp.NEQ)
            if isinstance(parent_node, comparison_types):
                # Check if inner FROM tables are all CTEs (no raw fact scan)
                scans_cte_only = sq_tables and sq_tables.issubset(cte_names)
                if scans_cte_only:
                    tags.add("scalar_agg_sub_cte")
                else:
                    tags.add("scalar_agg_sub")

    if correlated_count > 0:
        tags.add("correlated_sub")
    if independent_count > 0:
        tags.add("independent_sub")
    if max_depth >= 1:
        tags.add(f"sub_nesting_depth:{max_depth}")


def _extract_cte_filter_tags(ast, ctes: list, tags: Set[str]) -> None:
    """Feature 5: CTE Filter Status.

    Checks each CTE body for WHERE clauses. Unfiltered CTEs are
    anti-pattern targets (pure overhead, caused 0.85x regression on Q67).

    Emits:
    - cte_filtered: At least one CTE has a WHERE clause
    - cte_unfiltered: At least one CTE lacks a WHERE clause
    """
    from sqlglot import exp

    if not ctes:
        return

    has_filtered = False
    has_unfiltered = False

    for cte in ctes:
        # CTE.this is the subquery body (Select node)
        body = cte.this
        if body is None:
            has_unfiltered = True
            continue

        # Check if the CTE body contains a WHERE clause
        where_nodes = list(body.find_all(exp.Where))
        if where_nodes:
            has_filtered = True
        else:
            has_unfiltered = True

    if has_filtered:
        tags.add("cte_filtered")
    if has_unfiltered:
        tags.add("cte_unfiltered")


# =============================================================================
# Features 7-8: Star Join + LEFT JOIN Right-Filter Detection
# =============================================================================

# TPC-DS fact and dimension tables for star join detection
_FACT_TABLES = {
    "store_sales", "catalog_sales", "web_sales", "inventory",
    "store_returns", "catalog_returns", "web_returns",
}
_DIM_TABLES = {
    "date_dim", "item", "store", "customer", "warehouse",
    "customer_demographics", "customer_address", "promotion",
    "household_demographics", "income_band", "reason",
    "ship_mode", "time_dim", "web_site", "web_page",
    "catalog_page", "call_center",
}


def _extract_star_join_tags(ast, table_names: list, tags: Set[str]) -> None:
    """Feature 7: Star Join Pattern detection.

    Emits star_join_pattern when query has 1+ fact table + 3+ dimension tables.
    This identifies star-schema queries where prefetch_fact_join and
    aggregate_pushdown transforms are most productive.
    """
    fact_count = sum(1 for t in table_names if t in _FACT_TABLES)
    dim_count = len(set(t for t in table_names if t in _DIM_TABLES))
    if fact_count >= 1 and dim_count >= 3:
        tags.add("star_join_pattern")


def _extract_left_join_filter_tags(ast, tags: Set[str]) -> None:
    """Feature 8: Detect LEFT JOINs where WHERE clause filters on right-table column.

    This is the inner_join_conversion opportunity pattern — when a WHERE
    clause on the right table eliminates NULL rows, the LEFT JOIN behaves
    as an INNER JOIN but the optimizer doesn't recognize this.

    Handles both qualified (sr.sr_reason_sk) and unqualified (sr_reason_sk)
    column references using TPC-DS naming conventions.

    Emits: left_join_right_filter
    """
    from sqlglot import exp

    # Find LEFT JOINs and collect right-side table names/aliases
    left_join_tables = set()
    for j in ast.find_all(exp.Join):
        j_str = str(j).lower()
        if "left" in j_str:
            tbl = j.find(exp.Table)
            if tbl and tbl.name:
                left_join_tables.add(tbl.name.lower())
                if tbl.alias:
                    left_join_tables.add(tbl.alias.lower())

    if not left_join_tables:
        return

    # Build column-prefix map for unqualified column detection
    # TPC-DS convention: table "store_returns" → columns start with "sr_"
    _PREFIX_MAP = {
        "store_returns": "sr_", "store_sales": "ss_",
        "catalog_returns": "cr_", "catalog_sales": "cs_",
        "web_returns": "wr_", "web_sales": "ws_",
        "inventory": "inv_",
    }
    left_join_prefixes = set()
    for t in left_join_tables:
        if t in _PREFIX_MAP:
            left_join_prefixes.add(_PREFIX_MAP[t])

    # Check WHERE for references to left-joined tables
    for where in ast.find_all(exp.Where):
        for col in where.find_all(exp.Column):
            # Check qualified reference
            tbl = col.table.lower() if col.table else ""
            if tbl and tbl in left_join_tables:
                tags.add("left_join_right_filter")
                return
            # Check unqualified reference by column prefix
            col_name = col.name.lower() if col.name else ""
            if not tbl and col_name:
                for prefix in left_join_prefixes:
                    if col_name.startswith(prefix):
                        tags.add("left_join_right_filter")
                        return


# =============================================================================
# Feature 6: EXPLAIN Plan Feature Extraction
# =============================================================================

def extract_explain_features(explain_text: str) -> Set[str]:
    """Extract features from EXPLAIN ANALYZE output.

    This is Feature 6: OPTIMIZER_PLAN_SCANS. Unlike features 1-5 which
    operate on SQL AST, this requires actual EXPLAIN output from the engine.

    Use this for collision disambiguation (e.g., pushdown vs single_pass_agg
    on Q9 — both match AST features but differ in plan shape).

    Supports both DuckDB and PostgreSQL EXPLAIN formats.

    Emits:
    - scan_count:N — number of table scan operators
    - nested_loop_present — at least one nested loop join
    - hash_join_present — at least one hash join
    - merge_join_present — at least one merge/sort-merge join
    - seq_scan_tables:table1,table2 — tables accessed via sequential scan

    Args:
        explain_text: Raw EXPLAIN ANALYZE output text

    Returns:
        Set of feature tag strings
    """
    if not explain_text:
        return set()

    tags: Set[str] = set()
    text_upper = explain_text.upper()

    # ── Scan counting ──
    # DuckDB format:  SEQ_SCAN store_sales   or  SEQ_SCAN(store_sales)
    # PG format:      Seq Scan on store_sales ss  (cost=...)
    # Multiple scans can appear on a single line (DuckDB tree rendering)
    scan_pattern = re.compile(
        r'(?:SEQ[_\s]SCAN|TABLE[_\s]SCAN|INDEX[_\s]SCAN'
        r'|INDEX[_\s]ONLY[_\s]SCAN|BITMAP[_\s]HEAP[_\s]SCAN)',
        re.IGNORECASE,
    )
    # Table name extraction: match the scan operator followed by table name
    # DuckDB: "SEQ_SCAN store_sales" or "SEQ_SCAN(store_sales)"
    # PG:     "Seq Scan on store_sales ss" (skip "on", capture table)
    table_pattern = re.compile(
        r'(?:SEQ[_\s]SCAN|TABLE[_\s]SCAN)\s*'
        r'(?:\(\s*([a-z_]\w*)|on\s+([a-z_]\w*)|([a-z_]\w*))',
        re.IGNORECASE,
    )
    scan_count = 0
    seq_scan_tables: list = []

    for line in explain_text.split("\n"):
        # Count all scan operators on this line (DuckDB packs multiple per line)
        matches = scan_pattern.findall(line)
        scan_count += len(matches)

        # Extract table names from sequential/table scans on this line
        for m in table_pattern.finditer(line):
            tbl_name = (m.group(1) or m.group(2) or m.group(3) or "").lower()
            if tbl_name and tbl_name not in ("on", "as", "the"):
                seq_scan_tables.append(tbl_name)

    if scan_count > 0:
        tags.add(f"scan_count:{scan_count}")
    if seq_scan_tables:
        tags.add(f"seq_scan_tables:{','.join(sorted(set(seq_scan_tables)))}")

    # ── Join type detection ──
    # DuckDB uses underscores (HASH_JOIN, NESTED_LOOP_JOIN)
    # PG uses spaces (Hash Join, Nested Loop, Merge Join)
    if re.search(r'NESTED[_\s]*LOOP', text_upper):
        tags.add("nested_loop_present")
    if re.search(r'HASH[_\s]*JOIN', text_upper):
        tags.add("hash_join_present")
    if re.search(r'MERGE[_\s]*JOIN|SORT[_\s]*MERGE', text_upper):
        tags.add("merge_join_present")

    return tags


# =============================================================================
# Precondition Feature Extraction (for transform detection)
# =============================================================================

# Static mapping from extract_tags() output → transform precondition features
_TAG_TO_FEATURE = {
    "group_by": "GROUP_BY",
    "having": "HAVING",
    "case": "CASE_EXPR",
    "date_dim": "DATE_DIM",
    "left_join": "LEFT_JOIN",
    "window": "WINDOW_FUNC",
    "rollup": "ROLLUP",
    "union": "UNION",
    "intersect": "INTERSECT",
    "exists": "EXISTS",
    "cte": "CTE",
    "correlated_sub": "CORRELATED_SUB",
    "scalar_agg_sub": "SCALAR_AGG_SUB",
    "scalar_agg_sub_cte": "SCALAR_AGG_SUB_CTE",
    "between": "BETWEEN",
    "multi_channel": "MULTI_CHANNEL",
    "star_join_pattern": "STAR_JOIN",
    "left_join_right_filter": "LEFT_JOIN_RIGHT_FILTER",
}


def extract_precondition_features(sql: str, dialect: str = "duckdb") -> Set[str]:
    """Extract precondition features for transform detection.

    Maps the low-level tags from extract_tags() to the uppercase feature
    vocabulary used by transforms.json precondition_features, plus derives
    additional threshold-based features via AST analysis.

    Features emitted:
    - Direct mappings: GROUP_BY, HAVING, CASE_EXPR, DATE_DIM, LEFT_JOIN,
      WINDOW_FUNC, ROLLUP, UNION, INTERSECT, EXISTS, CTE, CORRELATED_SUB,
      BETWEEN, MULTI_CHANNEL, STAR_JOIN, LEFT_JOIN_RIGHT_FILTER
    - Aggregate types: AGG_AVG, AGG_SUM, AGG_COUNT
    - OR branches: OR_BRANCH (from or_branch_count:N tags)
    - Subquery thresholds: SCALAR_SUB_2+, SCALAR_SUB_5+, SCALAR_SUB_8+
    - Table thresholds: MULTI_TABLE_5+, TABLE_REPEAT_3+, TABLE_REPEAT_8+
    - EXISTS thresholds: EXISTS_3+

    Args:
        sql: SQL query text
        dialect: SQL dialect for parsing

    Returns:
        Set of uppercase feature strings
    """
    tags = extract_tags(sql, dialect=dialect)
    features: Set[str] = set()

    # 1. Direct tag → feature mapping
    for tag, feature in _TAG_TO_FEATURE.items():
        if tag in tags:
            features.add(feature)

    # 2. OR_BRANCH from or_branch_count:N tags
    for tag in tags:
        if tag.startswith("or_branch_count:"):
            features.add("OR_BRANCH")
            break

    # 3. AST analysis for aggregate types, subquery counts, table counts
    try:
        import sqlglot
        from sqlglot import exp

        ast = sqlglot.parse_one(sql, dialect=dialect)

        # Aggregate type detection
        for agg_node in ast.find_all(exp.AggFunc):
            if isinstance(agg_node, exp.Avg):
                features.add("AGG_AVG")
            elif isinstance(agg_node, exp.Sum):
                features.add("AGG_SUM")
            elif isinstance(agg_node, exp.Count):
                features.add("AGG_COUNT")

        # Subquery counting (exp.Subquery + exp.Exists)
        subquery_count = len(list(ast.find_all(exp.Subquery)))
        exists_count = len(list(ast.find_all(exp.Exists)))
        total_sub = subquery_count + exists_count
        if total_sub >= 2:
            features.add("SCALAR_SUB_2+")
        if total_sub >= 5:
            features.add("SCALAR_SUB_5+")
        if total_sub >= 8:
            features.add("SCALAR_SUB_8+")

        # EXISTS counting threshold
        if exists_count >= 3:
            features.add("EXISTS_3+")

        # Table counting — distinct table names
        table_names = [t.name.lower() for t in ast.find_all(exp.Table) if t.name]
        distinct_tables = set(table_names)
        if len(distinct_tables) >= 5:
            features.add("MULTI_TABLE_5+")

        # Table repeat thresholds — max repetition count
        if table_names:
            name_counts = Counter(table_names)
            max_repeat = max(name_counts.values())
            if max_repeat >= 3:
                features.add("TABLE_REPEAT_3+")
            if max_repeat >= 8:
                features.add("TABLE_REPEAT_8+")

    except Exception as e:
        logger.debug(f"AST feature extraction failed: {e}")

    return features


def _extract_tags_regex(sql: str, tags: Set[str]) -> None:
    """Regex-based tag extraction fallback for SQL fragments."""
    sql_upper = sql.upper()

    # Table name patterns (FROM/JOIN followed by identifier)
    for m in re.finditer(r'(?:FROM|JOIN)\s+([a-zA-Z_]\w*)', sql, re.IGNORECASE):
        tags.add(m.group(1).lower())

    # SQL keywords
    for kw in _SQL_KEYWORDS | _WINDOW_KEYWORDS:
        if re.search(rf'\b{kw.upper()}\b', sql_upper):
            tags.add(kw)

    # Structural
    if "CTE" not in tags and "WITH" in sql_upper:
        tags.add("cte")
    if re.search(r'\bROLLUP\b', sql_upper):
        tags.add("rollup")
    if re.search(r'\bGROUPING\b', sql_upper):
        tags.add("grouping")


def _extract_tags_regex_keywords(sql: str, tags: Set[str]) -> None:
    """Supplement AST tags with regex keyword detection for edge cases."""
    sql_upper = sql.upper()

    # Keywords that AST might miss on fragments
    if "ROLLUP" in sql_upper and "rollup" not in tags:
        tags.add("rollup")
    if "GROUPING" in sql_upper and "grouping" not in tags:
        tags.add("grouping")
    if "CUBE" in sql_upper and "cube" not in tags:
        tags.add("cube")
    if re.search(r'\bOR\b', sql_upper):
        tags.add("or_predicate")


# =============================================================================
# Category Classification
# =============================================================================

def classify_category(tags: Set[str]) -> str:
    """Assign a category based on dominant SQL patterns in tags.

    Categories:
    - set_operations: intersect, except, or union-dominated
    - aggregation_rewrite: rollup, grouping, or case-in-aggregate
    - subquery_elimination: correlated subquery, exists, or in-subquery
    - scan_consolidation: repeated_scan or self_join
    - join_reorder: multi-table joins without the above
    - filter_pushdown: cte + filter-heavy structure
    - general: fallback

    Args:
        tags: Set of tags from extract_tags()

    Returns:
        Category string
    """
    if tags & {"intersect", "except"}:
        return "set_operations"
    if tags & {"rollup", "cube", "grouping"}:
        return "aggregation_rewrite"
    if tags & {"correlated_subquery"} or ("exists" in tags and "subquery" in tags):
        return "subquery_elimination"
    if tags & {"repeated_scan", "self_join"}:
        return "scan_consolidation"
    if "union" in tags and "case" not in tags:
        return "set_operations"
    if "cte" in tags and "multi_cte" not in tags:
        return "filter_pushdown"
    if len(tags & {"join", "self_join", "left_join", "outer_join", "cross_join"}) >= 2:
        return "join_reorder"
    if "case" in tags and "aggregate" in tags:
        return "aggregation_rewrite"
    return "general"


# =============================================================================
# Example Loading
# =============================================================================

# Use qt_sql/examples/ (+ benchmark seed rules) for tag indexing.


def _clean_sql_markers(sql: str) -> str:
    """Remove [xxx]: markers from example SQL and extract main query."""
    # Remove lines with [xxx]: markers
    lines = sql.split('\n')
    clean_lines = []
    for line in lines:
        # Skip pure marker lines like "[customer_total_return] CORRELATED:"
        if re.match(r'^\s*\[[\w_]+\].*:\s*$', line):
            continue
        # Skip "[main_query]:" lines
        if re.match(r'^\s*\[main_query\]:\s*$', line):
            continue
        clean_lines.append(line)

    cleaned = '\n'.join(clean_lines).strip()

    # If there are multiple SQL statements, try to find the main SELECT
    if 'SELECT' in cleaned.upper():
        # Find the last/main SELECT statement
        parts = re.split(r'\n\s*\n', cleaned)  # Split by blank lines
        for part in reversed(parts):
            if 'SELECT' in part.upper() and 'FROM' in part.upper():
                return part.strip()

    return cleaned


def load_examples_for_indexing() -> List[Tuple[str, str, Dict]]:
    """Load examples from multiple directories for indexing.

    Loads from:
    - qt_sql/examples/ (gold patterns)
    - qt_sql/benchmarks/*/state_0/seed/ (seed rules)

    Returns:
        List of (example_id, sql_text, metadata) tuples
    """
    examples = []

    # Load from qt_sql/examples/ (gold) + qt_sql/benchmarks/*/state_0/seed/ (seed rules)
    search_dirs = [EXAMPLES_DIR]
    benchmarks_dir = BASE_DIR / "benchmarks"
    if benchmarks_dir.exists():
        for bm in benchmarks_dir.iterdir():
            seed = bm / "state_0" / "seed"
            if seed.exists():
                search_dirs.append(seed)

    for example_dir in search_dirs:
        if not example_dir.exists():
            continue

        for path in sorted(example_dir.glob("**/*.json")):
            try:
                data = json.loads(path.read_text())
                example_id = data.get("id", path.stem)

                # Get SQL to vectorize - prefer top-level original_sql (always complete)
                # over example.input_slice (often abbreviated with ... or markers)
                example_data = data.get("example", {})
                sql_text = (
                    data.get("original_sql") or
                    example_data.get("before_sql") or
                    example_data.get("input_slice") or
                    ""
                )

                if not sql_text:
                    logger.warning(f"No SQL found in example {example_id}")
                    continue

                # Clean SQL by removing [xxx]: markers
                sql_text = _clean_sql_markers(sql_text)

                if not sql_text:
                    logger.warning(f"Empty SQL after cleaning in {example_id}")
                    continue

                # Extract metadata
                transforms = example_data.get("transforms", [])
                if not transforms and example_data.get("opportunity"):
                    transforms = [example_data["opportunity"].lower()]

                # Get transform from rewrite_sets
                output = example_data.get("output", {})
                rewrite_sets = output.get("rewrite_sets", [])
                if rewrite_sets and not transforms:
                    transforms = [rs.get("transform", "") for rs in rewrite_sets if rs.get("transform")]

                # Get speedup from example or data level
                speedup = data.get("verified_speedup", "unknown")

                # Determine engine from path
                rel = path.relative_to(BASE_DIR)
                parts = rel.parts
                if "duckdb" in parts:
                    source_engine = "duckdb"
                elif "postgres" in parts:
                    source_engine = "postgres"
                elif "seed" in parts:
                    source_engine = "seed"
                else:
                    source_engine = "unknown"

                # Determine example type: gold (positive) or regression (negative)
                example_type = data.get("type", "gold")

                metadata = {
                    "name": data.get("name", example_id),
                    "description": data.get("description", ""),
                    "verified_speedup": speedup,
                    "transforms": transforms,
                    "principle": data.get("principle", ""),
                    "key_insight": example_data.get("key_insight", ""),
                    "benchmark_queries": data.get("benchmark_queries", []),
                    "engine": source_engine,
                    "type": example_type,
                }

                examples.append((example_id, sql_text, metadata))

            except Exception as e:
                logger.warning(f"Failed to load example {path}: {e}")

    return examples


# =============================================================================
# Tag Index Building
# =============================================================================

def _extract_description_tags(description: str) -> Set[str]:
    """Extract signal words from example description text."""
    if not description:
        return set()

    desc_lower = description.lower()
    signal_words = set()

    # Known transform/pattern terms in descriptions
    patterns = [
        "decorrelate", "pushdown", "early_filter", "early filter",
        "date_cte", "date cte", "dimension_cte", "dimension cte",
        "prefetch", "materialize", "single_pass", "single pass",
        "or_to_union", "or to union", "intersect_to_exists",
        "intersect to exists", "union_cte_split", "union cte split",
        "rollup", "grouping", "bitmap", "windowing", "window",
        "correlated", "self-join", "self join", "repeated scan",
    ]

    for pat in patterns:
        if pat in desc_lower:
            # Normalize to underscore form
            signal_words.add(pat.replace(" ", "_").replace("-", "_"))

    return signal_words


def build_tag_index(
    examples: List[Tuple[str, str, Dict]],
) -> Tuple[List[Dict], Dict]:
    """Build tag index from examples.

    For each example, extracts tags from original SQL and description,
    classifies category, and stores in a searchable format.

    Args:
        examples: List of (example_id, sql_text, metadata) tuples

    Returns:
        (tag_entries, metadata_dict)
    """
    if not examples:
        logger.warning("No examples to index")
        return [], {}

    tag_entries = []
    query_metadata = {}

    print(f"Extracting tags from {len(examples)} examples...")

    for i, (example_id, sql_text, meta) in enumerate(examples):
        engine = meta.get("engine", "unknown")
        if engine in ("postgres", "postgresql"):
            dialect = "postgres"
        elif engine == "duckdb":
            dialect = "duckdb"
        else:
            dialect = "duckdb"

        # Extract tags from SQL
        tags = extract_tags(sql_text, dialect=dialect)

        # Add description-based tags
        desc_tags = _extract_description_tags(meta.get("description", ""))
        tags |= desc_tags

        # Classify category
        category = classify_category(tags)

        tag_entry = {
            "id": example_id,
            "tags": sorted(tags),
            "category": category,
            "engine": engine,
            "type": meta.get("type", "gold"),
            "metadata": {
                "name": meta.get("name", example_id),
                "description": meta.get("description", ""),
                "verified_speedup": meta.get("verified_speedup", "unknown"),
                "transforms": meta.get("transforms", []),
                "winning_transform": meta.get("transforms", [""])[0] if meta.get("transforms") else "",
                "principle": meta.get("principle", ""),
                "key_insight": meta.get("key_insight", ""),
                "engine": engine,
                "type": meta.get("type", "gold"),
            },
        }
        tag_entries.append(tag_entry)

        # Also store in flat metadata for backward compat
        query_metadata[example_id] = tag_entry["metadata"]

        print(f"  [{i+1}/{len(examples)}] {example_id}: {len(tags)} tags, category={category}")

    metadata = {
        "query_metadata": query_metadata,
        "index_stats": {
            "total_examples": len(examples),
            "index_type": "tag_overlap",
        },
    }

    print(f"\nBuilt tag index: {len(examples)} examples")

    return tag_entries, metadata


def save_tag_index(tag_entries: List[Dict], metadata: Dict) -> None:
    """Save tag index and metadata to qt_sql/models/."""
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # Save tag index
    with open(TAGS_FILE, 'w') as f:
        json.dump({"examples": tag_entries}, f, indent=2)
    print(f"Saved tag index to {TAGS_FILE}")

    # Save metadata
    with open(METADATA_FILE, 'w') as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {METADATA_FILE}")


def show_index_stats() -> None:
    """Show statistics about the current tag index."""
    if not TAGS_FILE.exists():
        print("No tag index found. Run: python -m qt_sql.tag_index")
        return

    with open(TAGS_FILE) as f:
        data = json.load(f)

    examples = data.get("examples", [])

    print("=" * 60)
    print("qt_sql Tag Index Statistics")
    print("=" * 60)
    print(f"Total examples:   {len(examples)}")
    print()

    # Category distribution
    categories = Counter(ex.get("category", "general") for ex in examples)
    print("Categories:")
    for cat, count in categories.most_common():
        print(f"  {cat}: {count}")
    print()

    # Engine distribution
    engines = Counter(ex.get("engine", "unknown") for ex in examples)
    print("Engines:")
    for eng, count in engines.most_common():
        print(f"  {eng}: {count}")
    print()

    # Type distribution
    types = Counter(ex.get("type", "gold") for ex in examples)
    print("Types:")
    for t, count in types.most_common():
        print(f"  {t}: {count}")
    print()

    print("Indexed Examples:")
    print("-" * 60)
    for ex in sorted(examples, key=lambda x: x["id"]):
        tags = ex.get("tags", [])
        meta = ex.get("metadata", {})
        speedup = meta.get("verified_speedup", "unknown")
        print(f"  {ex['id']}")
        print(f"    category: {ex.get('category', '?')}, speedup: {speedup}")
        print(f"    tags ({len(tags)}): {', '.join(tags[:10])}{'...' if len(tags) > 10 else ''}")


def rebuild_index() -> bool:
    """Rebuild tag index from qt_sql/examples/.

    Returns:
        True if successful, False otherwise
    """
    print("=" * 60)
    print("Building qt_sql Tag Index")
    print("=" * 60)

    # Load examples
    examples = load_examples_for_indexing()
    if not examples:
        print("\nNo examples found in qt_sql/examples/")
        print("Add example JSON files with 'before_sql' or 'input_slice' fields")
        return False

    print(f"\nFound {len(examples)} examples")

    # Build index
    tag_entries, metadata = build_tag_index(examples)

    if not tag_entries:
        print("\nFailed to build index")
        return False

    # Save
    save_tag_index(tag_entries, metadata)

    print("\n" + "=" * 60)
    print("Tag index built successfully!")
    print("=" * 60)

    return True


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build ADO tag index")
    parser.add_argument("--stats", action="store_true", help="Show index statistics")
    parser.add_argument("--rebuild", action="store_true", help="Force rebuild index")

    args = parser.parse_args()

    if args.stats:
        show_index_stats()
    else:
        rebuild_index()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
