"""pg_hint_plan utility — hint catalog, validation, SQL application.

Pure utility module with no LLM dependencies. Provides:
  - PG_HINT_CATALOG: supported hint types with syntax and descriptions
  - HintDirective: dataclass for a single hint
  - validate_hint_directive(): check hint_type + args against catalog
  - build_hint_comment(): combine directives into /*+ ... */ comment
  - apply_hints_to_sql(): prepend hint comment to SQL
  - parse_hint_string(): parse raw hint text into validated directives
  - format_hint_catalog_for_prompt(): markdown for LLM consumption
  - check_hint_plan_available(): probe shared_preload_libraries
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── Hint Catalog ──────────────────────────────────────────────────────────
# Each entry: hint_type -> (category, syntax_template, description, example)

PG_HINT_CATALOG: Dict[str, Tuple[str, str, str, str]] = {
    # Join method hints
    "HashJoin": (
        "join_method",
        "HashJoin(t1 t2)",
        "Force hash join between two tables (use aliases from query).",
        "HashJoin(ss cd)",
    ),
    "NestLoop": (
        "join_method",
        "NestLoop(t1 t2)",
        "Force nested-loop join between two tables.",
        "NestLoop(ss i)",
    ),
    "MergeJoin": (
        "join_method",
        "MergeJoin(t1 t2)",
        "Force merge join between two tables.",
        "MergeJoin(ss sr)",
    ),
    "NoHashJoin": (
        "join_method",
        "NoHashJoin(t1 t2)",
        "Prohibit hash join between two tables.",
        "NoHashJoin(ss cd)",
    ),
    "NoNestLoop": (
        "join_method",
        "NoNestLoop(t1 t2)",
        "Prohibit nested-loop join between two tables.",
        "NoNestLoop(ss cd)",
    ),
    "NoMergeJoin": (
        "join_method",
        "NoMergeJoin(t1 t2)",
        "Prohibit merge join between two tables.",
        "NoMergeJoin(ss sr)",
    ),
    # Scan method hints
    "SeqScan": (
        "scan_method",
        "SeqScan(t)",
        "Force sequential scan on a table.",
        "SeqScan(store_sales)",
    ),
    "IndexScan": (
        "scan_method",
        "IndexScan(t [idx])",
        "Force index scan on a table, optionally specifying the index.",
        "IndexScan(cd cd_pkey)",
    ),
    "BitmapScan": (
        "scan_method",
        "BitmapScan(t [idx])",
        "Force bitmap scan on a table.",
        "BitmapScan(ss ss_sold_date_sk_idx)",
    ),
    "NoSeqScan": (
        "scan_method",
        "NoSeqScan(t)",
        "Prohibit sequential scan on a table.",
        "NoSeqScan(store_sales)",
    ),
    "NoIndexScan": (
        "scan_method",
        "NoIndexScan(t)",
        "Prohibit index scan on a table.",
        "NoIndexScan(cd)",
    ),
    # Join order hints
    "Leading": (
        "join_order",
        "Leading((t1 (t2 t3)))",
        "Force join order. Nested parens = join tree shape. "
        "Simple form: Leading(t1 t2 t3) = left-deep join in that order.",
        "Leading((ss (cd dd)))",
    ),
    # Cardinality hints
    "Rows": (
        "cardinality",
        "Rows(t1 t2 #N)",
        "Override row estimate for a join. #N = absolute, *N = multiply, +N = add.",
        "Rows(ss cd #1000)",
    ),
    # SET hints (apply GUC within the hint comment)
    "Set": (
        "set_guc",
        "Set(param value)",
        "Apply a GUC setting via hint comment. Same effect as SET LOCAL but "
        "embedded in the hint block.",
        "Set(enable_mergejoin off)",
    ),
}

# Valid hint types for quick lookup
_VALID_HINT_TYPES = frozenset(PG_HINT_CATALOG.keys())

# Hint types that require exactly 2 table args
_TWO_TABLE_HINTS = frozenset([
    "HashJoin", "NestLoop", "MergeJoin",
    "NoHashJoin", "NoNestLoop", "NoMergeJoin",
])

# Hint types that take exactly 1 table arg (no index arg)
_EXACT_ONE_TABLE_HINTS = frozenset([
    "SeqScan", "NoSeqScan", "NoIndexScan",
])

# Hint types that take 1 table + optional index name
_TABLE_PLUS_INDEX_HINTS = frozenset([
    "IndexScan", "BitmapScan",
])

# Union for quick membership check
_ONE_TABLE_HINTS = _EXACT_ONE_TABLE_HINTS | _TABLE_PLUS_INDEX_HINTS

# Identifier pattern: a valid SQL alias/table name (no parens, no operators)
_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


@dataclass
class HintDirective:
    """A single pg_hint_plan directive."""
    hint_type: str  # e.g. "HashJoin", "NoNestLoop", "Set"
    args: str       # e.g. "ss cd", "enable_mergejoin off"

    def render(self) -> str:
        """Render as hint text: HashJoin(ss cd)"""
        return f"{self.hint_type}({self.args})"


def validate_hint_directive(
    hint_type: str,
    args: str,
) -> Optional[HintDirective]:
    """Validate a hint directive against the catalog.

    Returns HintDirective if valid, None if invalid.
    """
    if hint_type not in _VALID_HINT_TYPES:
        logger.debug(f"Unknown hint type: {hint_type}")
        return None

    args = args.strip()
    if not args:
        logger.debug(f"Empty args for hint: {hint_type}")
        return None

    # Validate per hint category
    parts = args.split()

    if hint_type in _TWO_TABLE_HINTS:
        # Exactly 2 identifier args
        if len(parts) != 2:
            logger.debug(f"{hint_type} requires exactly 2 table args, got: {args}")
            return None
        if not all(_IDENT_RE.match(p) for p in parts):
            logger.debug(f"{hint_type} args must be identifiers: {args}")
            return None

    elif hint_type in _EXACT_ONE_TABLE_HINTS:
        # Exactly 1 table arg, no index
        if len(parts) != 1:
            logger.debug(f"{hint_type} requires exactly 1 table arg, got: {args}")
            return None
        if not _IDENT_RE.match(parts[0]):
            logger.debug(f"{hint_type} arg must be an identifier: {args}")
            return None

    elif hint_type in _TABLE_PLUS_INDEX_HINTS:
        # 1 table + optional index name
        if len(parts) < 1 or len(parts) > 2:
            logger.debug(f"{hint_type} requires 1-2 args, got: {args}")
            return None
        if not all(_IDENT_RE.match(p) for p in parts):
            logger.debug(f"{hint_type} args must be identifiers: {args}")
            return None

    elif hint_type == "Leading":
        # Leading accepts simple list (Leading(t1 t2 t3)) or nested parens
        # (Leading((t1 (t2 t3)))). Validate balanced parens.
        if not _balanced_parens(args):
            logger.debug(f"Leading hint has unbalanced parens: {args}")
            return None
        # Must contain at least 2 identifiers
        idents = re.findall(r'[A-Za-z_]\w*', args)
        if len(idents) < 2:
            logger.debug(f"Leading hint needs at least 2 tables: {args}")
            return None

    elif hint_type == "Rows":
        # Rows needs table(s) + trailing modifier (#N, *N, +N)
        # Valid: "ss cd #1000", invalid: "#100" (no tables)
        modifier_match = re.search(r'[#*+]\d+\s*$', args)
        if not modifier_match:
            logger.debug(f"Rows hint missing trailing cardinality modifier: {args}")
            return None
        # Check there's at least one identifier before the modifier
        before_modifier = args[:modifier_match.start()].strip()
        table_parts = before_modifier.split()
        if not table_parts or not all(_IDENT_RE.match(p) for p in table_parts):
            logger.debug(f"Rows hint needs table args before modifier: {args}")
            return None

    elif hint_type == "Set":
        # Set needs param + value
        if len(parts) < 2:
            logger.debug(f"Set hint needs param and value: {args}")
            return None

    return HintDirective(hint_type=hint_type, args=args)


def build_hint_comment(directives: List[HintDirective]) -> str:
    """Combine hint directives into a /*+ ... */ comment block.

    Args:
        directives: List of validated HintDirective objects.

    Returns:
        Hint comment string, e.g. "/*+ HashJoin(ss cd) Set(work_mem 512MB) */"
        Returns "" if directives is empty.
    """
    if not directives:
        return ""
    parts = [d.render() for d in directives]
    return "/*+ " + " ".join(parts) + " */"


def apply_hints_to_sql(sql: str, hint_comment: str) -> str:
    """Prepend a hint comment to SQL.

    The hint must appear before the first SELECT/WITH for pg_hint_plan
    to recognize it.

    Args:
        sql: Original SQL query.
        hint_comment: Hint comment from build_hint_comment().

    Returns:
        SQL with hint prepended.
    """
    if not hint_comment:
        return sql
    return hint_comment + "\n" + sql


def parse_hint_string(raw: str) -> List[HintDirective]:
    """Parse raw hint text into validated HintDirective list.

    Handles formats like:
      "HashJoin(ss cd) NoNestLoop(ss i) Set(enable_mergejoin off)"
    or:
      "/*+ HashJoin(ss cd) */"

    Invalid hints are silently stripped.
    """
    # Strip /*+ ... */ wrapper if present
    text = raw.strip()
    if text.startswith("/*+"):
        text = text[3:]
    if text.endswith("*/"):
        text = text[:-2]
    text = text.strip()

    if not text:
        return []

    # Parse HintType(...) tokens, handling nested parens for Leading()
    directives: List[HintDirective] = []
    for hint_type, args in _tokenize_hints(text):
        directive = validate_hint_directive(hint_type, args)
        if directive:
            directives.append(directive)
        else:
            logger.debug(f"Stripped invalid hint: {hint_type}({args})")

    return directives


def _balanced_parens(s: str) -> bool:
    """Check that parentheses in s are balanced."""
    depth = 0
    for ch in s:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def _tokenize_hints(text: str) -> List[tuple]:
    """Tokenize hint text into (hint_type, args) pairs.

    Handles nested parentheses (e.g. Leading((t1 (t2 t3)))).
    """
    results = []
    i = 0
    n = len(text)

    while i < n:
        # Skip whitespace
        while i < n and text[i].isspace():
            i += 1
        if i >= n:
            break

        # Match identifier (hint type)
        if not text[i].isalpha() and text[i] != '_':
            i += 1
            continue

        j = i
        while j < n and (text[j].isalnum() or text[j] == '_'):
            j += 1

        hint_type = text[i:j]
        i = j

        # Skip whitespace
        while i < n and text[i].isspace():
            i += 1

        # Expect opening paren
        if i >= n or text[i] != '(':
            continue
        i += 1  # skip '('

        # Collect args until matching close paren (respecting nesting)
        depth = 1
        start = i
        while i < n and depth > 0:
            if text[i] == '(':
                depth += 1
            elif text[i] == ')':
                depth -= 1
            i += 1

        if depth != 0:
            # Unbalanced — skip this token
            continue

        # args is everything between the outer parens (excluding them)
        args = text[start:i - 1].strip()
        results.append((hint_type, args))

    return results


def format_hint_catalog_for_prompt() -> str:
    """Format the hint catalog as markdown for LLM consumption.

    Returns:
        Markdown string describing available hints with syntax and examples.
    """
    lines = [
        "## pg_hint_plan Hint Catalog",
        "",
        "Use these hints in the `hints` field. Each hint is written as "
        "`HintType(args)`. Multiple hints are space-separated.",
        "",
    ]

    # Group by category
    categories = {}
    for hint_type, (cat, syntax, desc, example) in PG_HINT_CATALOG.items():
        categories.setdefault(cat, []).append(
            (hint_type, syntax, desc, example)
        )

    category_labels = {
        "join_method": "Join Method Hints",
        "scan_method": "Scan Method Hints",
        "join_order": "Join Order Hints",
        "cardinality": "Cardinality Hints",
        "set_guc": "SET via Hint",
    }

    for cat, entries in categories.items():
        lines.append(f"### {category_labels.get(cat, cat)}")
        for hint_type, syntax, desc, example in entries:
            lines.append(f"- **{syntax}**: {desc}")
            lines.append(f"  Example: `{example}`")
        lines.append("")

    lines.append("### Rules")
    lines.append("- Use table ALIASES from the query, not full table names.")
    lines.append("- Join hints require exactly the two tables being joined.")
    lines.append("- Leading() controls the full join tree — use sparingly.")
    lines.append("- Set() hints apply GUC changes inside the hint block.")
    lines.append("- Combine hints: `HashJoin(ss cd) NoNestLoop(ss i) "
                  "Set(enable_mergejoin off)`")

    return "\n".join(lines)


def check_hint_plan_available(dsn: str) -> bool:
    """Check if pg_hint_plan is loaded in shared_preload_libraries.

    Args:
        dsn: PostgreSQL DSN string.

    Returns:
        True if pg_hint_plan is available, False otherwise.
    """
    try:
        from .execution.factory import PostgresConfig

        config = PostgresConfig.from_dsn(dsn)
        executor = config.get_executor()
        with executor:
            rows = executor.execute(
                "SHOW shared_preload_libraries"
            )
            if rows:
                libs = str(rows[0].get("shared_preload_libraries", ""))
                return "pg_hint_plan" in libs
    except Exception as e:
        logger.warning(f"Could not check pg_hint_plan availability: {e}")
    return False
