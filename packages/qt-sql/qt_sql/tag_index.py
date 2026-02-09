"""Tag-based example index builder for ADO knowledge base.

Builds a tag similarity index from gold examples AND regression examples
in ado/examples/. Each example's original SQL is parsed for keyword/table
tags and indexed for overlap-based matching.

Gold examples (type=gold): proven rewrites to emulate
Regression examples (type=regression): failed rewrites to avoid

Usage:
    python -m ado.tag_index          # Build index
    python -m ado.tag_index --stats  # Show index stats
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


def extract_tags(sql: str, dialect: str = "duckdb") -> Set[str]:
    """Extract tags from SQL using AST analysis with regex fallback.

    Tags include:
    - Table names (lowercased)
    - SQL keywords present (intersect, union, rollup, etc.)
    - Structural patterns (self_join, repeated_scan, multi_cte, correlated_subquery)

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

        # Window function subtypes
        if list(ast.find_all(exp.Rank)):
            tags.add("rank")
        if list(ast.find_all(exp.RowNumber)):
            tags.add("row_number")

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
        for sq in ast.find_all(exp.Subquery):
            # Simple heuristic: if subquery contains Column refs that don't
            # match tables inside the subquery, it's likely correlated
            sq_tables = {t.name.lower() for t in sq.find_all(exp.Table) if t.name}
            for col in sq.find_all(exp.Column):
                tbl = col.table.lower() if col.table else ""
                if tbl and tbl not in sq_tables:
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

    except Exception as e:
        logger.debug(f"AST tag extraction failed, falling back to regex: {e}")
        # Regex fallback for fragments
        _extract_tags_regex(sql, tags)

    # Always do regex fallback for keywords AST might miss
    _extract_tags_regex_keywords(sql, tags)

    return tags


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

# ADO is PostgreSQL-focused - only use ado/examples/ (DSB catalog rules)
# Do NOT load qt_sql/optimization/examples/ - those are DuckDB TPC-DS gold examples


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
    - ado/examples/ (generic PostgreSQL patterns)
    - qt_sql/optimization/examples/ (verified TPC-DS gold examples)

    Returns:
        List of (example_id, sql_text, metadata) tuples
    """
    examples = []

    # Load from ado/examples/ (gold) + ado/benchmarks/*/state_0/seed/ (seed rules)
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
    """Save tag index and metadata to ado/models/."""
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
        print("No tag index found. Run: python -m ado.tag_index")
        return

    with open(TAGS_FILE) as f:
        data = json.load(f)

    examples = data.get("examples", [])

    print("=" * 60)
    print("ADO Tag Index Statistics")
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
    """Rebuild tag index from ado/examples/.

    Returns:
        True if successful, False otherwise
    """
    print("=" * 60)
    print("Building ADO Tag Index")
    print("=" * 60)

    # Load examples
    examples = load_examples_for_indexing()
    if not examples:
        print("\nNo examples found in ado/examples/")
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
