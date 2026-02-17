"""Synthetic data validation for SQL equivalence checking.

Generates synthetic data matching query schema, executes both original
and optimized queries on it, and compares results. Used as Gate 3
(semantic validation) in the beam patch pipeline.

Ported from qt-synth/validator.py.
"""

import argparse
from collections import deque
import hashlib
import logging
import time

import sqlglot
from sqlglot import exp
import duckdb
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional, Callable
import json
import re
import math


logger = logging.getLogger(__name__)


TYPE_PROTOTYPE_TOKENS = {
    'INTEGER': [
        'id', 'key', 'sk', 'fk', 'count', 'qty', 'quantity', 'number', 'num',
        'year', 'month', 'day', 'week', 'quarter', 'dow', 'dom', 'moy', 'qoy',
        'seq', 'offset', 'age', 'rank', 'manager', 'dep', 'hour', 'minute',
        'second', 'code',
    ],
    'DECIMAL(18,2)': [
        'amount', 'amt', 'price', 'cost', 'fee', 'tax', 'discount', 'profit',
        'loss', 'revenue', 'sales', 'rate', 'ratio', 'margin', 'balance',
        'total', 'net', 'gross', 'pct', 'percent', 'score',
    ],
    'DATE': [
        'date', 'dt', 'time', 'timestamp', 'sold', 'ship', 'returned', 'birth',
        'create', 'created', 'update', 'updated', 'start', 'end', 'effective',
        'expiry', 'exp',
    ],
    'VARCHAR(100)': [
        'name', 'desc', 'description', 'type', 'category', 'state', 'city',
        'email', 'address', 'phone', 'status', 'gender', 'country', 'county',
        'street', 'comment', 'note', 'class',
    ],
}


def _identifier_tokens(identifier: str) -> List[str]:
    """Tokenize an identifier for lexical similarity matching."""
    # Split camelCase and normalize separators.
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', identifier).lower()
    raw_parts = [p for p in re.split(r'[^a-z0-9]+', s) if p]

    tokens: List[str] = []
    for part in raw_parts:
        pieces = [p for p in re.split(r'(?<=\D)(?=\d)|(?<=\d)(?=\D)', part) if p]
        tokens.extend(pieces)
    return tokens


def _token_vector(tokens: List[str]) -> Dict[str, float]:
    vec: Dict[str, float] = {}
    for tok in tokens:
        vec[tok] = vec.get(tok, 0.0) + 1.0
    return vec


def _cosine_similarity(vec_a: Dict[str, float], vec_b: Dict[str, float]) -> float:
    if not vec_a or not vec_b:
        return 0.0
    dot = 0.0
    for k, v in vec_a.items():
        dot += v * vec_b.get(k, 0.0)
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)


TYPE_PROTOTYPE_VECTORS = {
    col_type: _token_vector(tokens)
    for col_type, tokens in TYPE_PROTOTYPE_TOKENS.items()
}
TYPE_PROTOTYPE_TOKEN_SETS = {
    col_type: set(tokens)
    for col_type, tokens in TYPE_PROTOTYPE_TOKENS.items()
}


def infer_type_by_similarity(col_name: str) -> Tuple[Optional[str], float]:
    """Infer a column type using lexical vector similarity."""
    col_tokens = _identifier_tokens(col_name)
    if not col_tokens:
        return None, 0.0

    # Drop short prefix-like tokens (e.g., ss, ws, ca) from the similarity
    # signal while preserving semantically meaningful short identifiers.
    content_tokens = [
        t for t in col_tokens
        if len(t) > 2 or t in {'id', 'sk', 'fk', 'dt'}
    ] or col_tokens

    col_vec = _token_vector(content_tokens)
    content_set = set(content_tokens)
    best_type = None
    best_score = 0.0
    second_score = 0.0

    for col_type, proto_vec in TYPE_PROTOTYPE_VECTORS.items():
        cosine = _cosine_similarity(col_vec, proto_vec)
        overlap = len(content_set & TYPE_PROTOTYPE_TOKEN_SETS[col_type]) / max(1, len(content_set))
        score = 0.6 * overlap + 0.4 * cosine
        if score > best_score:
            second_score = best_score
            best_score = score
            best_type = col_type
        elif score > second_score:
            second_score = score

    # Conservative acceptance: require a minimum score + separation.
    if best_type and best_score >= 0.28 and (best_score - second_score) >= 0.02:
        return best_type, best_score
    return None, best_score


def _singularize(token: str) -> str:
    """Best-effort singularization for table/column token matching."""
    if token.endswith('ies') and len(token) > 3:
        return token[:-3] + 'y'
    if token.endswith('ses') and len(token) > 3:
        return token[:-2]
    if token.endswith('s') and not token.endswith('ss') and len(token) > 1:
        return token[:-1]
    return token


def _table_abbreviation(table_name: str) -> str:
    """Return abbreviation from table tokens (e.g., customer_address -> ca)."""
    tokens = [t for t in table_name.lower().split('_') if t]
    if not tokens:
        return ''
    if len(tokens) == 1:
        return tokens[0][0]
    return ''.join(t[0] for t in tokens)


def _table_name_variants(table_name: str) -> Set[str]:
    """Return lexical variants for matching column/table naming conventions."""
    table_lower = table_name.lower()
    tokens = [t for t in table_lower.split('_') if t]

    variants = {table_lower, _singularize(table_lower)}
    for tok in tokens:
        variants.add(tok)
        variants.add(_singularize(tok))
        if len(tok) >= 3:
            variants.add(tok[:3])
        if len(tok) >= 4:
            variants.add(tok[:4])

    if tokens:
        compact = ''.join(tokens)
        variants.add(compact)
        variants.add(_singularize(compact))
        variants.add(tokens[-1])
        variants.add(_singularize(tokens[-1]))

    return {v for v in variants if v}


def get_table_from_column(col_name: str, table_names: Optional[Set[str]] = None) -> Optional[str]:
    """Infer likely source table from column naming + available tables.

    The scoring is deliberately conservative: if two tables tie, return None.
    """
    if not table_names:
        return None

    col_lower = col_name.lower()
    parts = [p for p in col_lower.split('_') if p]
    prefix = parts[0] if parts else col_lower
    body_tokens = set(parts[1:] if len(parts) > 1 else [])

    best_table = None
    best_score = 0
    is_tie = False

    for table_name in table_names:
        table_lower = table_name.lower()
        table_tokens = [t for t in table_lower.split('_') if t]
        variants = _table_name_variants(table_name)
        abbrev = _table_abbreviation(table_name)

        score = 0

        if col_lower.startswith(f"{table_lower}_"):
            score += 14
        singular_table = _singularize(table_lower)
        if singular_table != table_lower and col_lower.startswith(f"{singular_table}_"):
            score += 13
        if prefix in variants:
            score += 12
        if abbrev and prefix == abbrev:
            score += 15 if len(abbrev) >= 2 else 8
        # Data warehouse dimensions often use a single-letter prefix from
        # the first token (e.g., d_year for date_dim, t_time for time_dim).
        if (
            len(prefix) == 1
            and table_tokens
            and table_lower.endswith('_dim')
            and prefix == table_tokens[0][0]
        ):
            score += 11
        if body_tokens.intersection(variants):
            score += 2
        score += len(body_tokens.intersection(set(table_tokens)))

        if score > best_score:
            best_score = score
            best_table = table_name
            is_tie = False
        elif score == best_score and score > 0:
            is_tie = True

    if is_tie or best_score == 0:
        return None
    return best_table


def get_table_for_column(col_name: str, tables: Dict[str, Dict]) -> Optional[str]:
    """Resolve a column to a table using schema ownership, then name heuristics."""
    col_lower = col_name.lower()
    owners = []
    for table_name, info in tables.items():
        for existing_col in info.get('columns', {}):
            if existing_col.lower() == col_lower:
                owners.append(table_name)
                break

    if len(owners) == 1:
        return owners[0]
    if len(owners) > 1:
        return None

    return get_table_from_column(col_name, set(tables.keys()))


def find_primary_key_column(table_name: str, column_names: List[str]) -> Optional[str]:
    """Best-effort PK detection for synthetic FK wiring."""
    if not column_names:
        return None

    lower_to_original = {c.lower(): c for c in column_names}
    table_variants = _table_name_variants(table_name)
    table_tokens = [t for t in table_name.lower().split('_') if t]
    last_token = _singularize(table_tokens[-1]) if table_tokens else table_name.lower()
    table_lower = table_name.lower()
    singular_table = _singularize(table_lower)
    abbrev = _table_abbreviation(table_name)

    # First pass: exact canonical names.
    exact_candidates = [
        f"{table_lower}_sk",
        f"{singular_table}_sk",
        f"{table_lower}_id",
        f"{singular_table}_id",
    ]
    for candidate in exact_candidates:
        if candidate in lower_to_original:
            return lower_to_original[candidate]

    key_candidates = [
        c for c in lower_to_original
        if c.endswith('_sk') or c.endswith('_id') or c == 'id'
    ]
    if not key_candidates:
        return None

    def _score(col_lower: str) -> int:
        score = 0
        if col_lower == 'id':
            score += 4
        if col_lower.endswith('_sk'):
            score += 2
        if col_lower.endswith(f'_{last_token}_sk') or col_lower.endswith(f'_{last_token}_id'):
            score += 8
        if 'date' in table_tokens and col_lower.endswith('_date_sk'):
            score += 6
        if abbrev and col_lower.startswith(f'{abbrev}_'):
            score += 3
        if 'date' in col_lower or 'time' in col_lower:
            score -= 2
        return score

    ranked = sorted(
        [(_score(c), c) for c in key_candidates],
        key=lambda x: (x[0], len(x[1])),
        reverse=True,
    )
    best_score = ranked[0][0]
    best = [c for s, c in ranked if s == best_score]

    # Ambiguous table with many similarly plausible keys (common in fact tables):
    # avoid forcing a single synthetic PK.
    if len(best) > 1:
        return None

    chosen = best[0]
    # Weak-signal fallback only if this table has exactly one key-like column.
    if best_score <= 0 and len(key_candidates) > 1:
        return None
    # Multi-key fact-like tables (e.g., *_sales, *_returns) often have no
    # single PK in query-projected columns; avoid forcing one on weak signal.
    if len(key_candidates) > 1 and best_score < 8:
        return None
    return lower_to_original[chosen]


class SchemaExtractor:
    """Extracts table/column info from SQL using SQLGlot AST."""
    
    def __init__(self, sql: str):
        self.sql = sql
        self.parsed = sqlglot.parse_one(sql)
        
    def extract_tables(self) -> Dict[str, Dict]:
        """Extract all tables with their columns from the query."""
        tables = {}
        cte_names = set()
        
        # First, collect CTE names (these should not be treated as base tables)
        for cte in self.parsed.find_all(exp.CTE):
            cte_names.add(cte.alias)
        
        # Get all table references (excluding CTEs)
        for table in self.parsed.find_all(exp.Table):
            table_name = table.name
            
            # Skip if this is actually a CTE reference
            if table_name in cte_names:
                continue
            
            alias = table.alias

            if table_name not in tables:
                tables[table_name] = {
                    'alias': alias,
                    'aliases': set([alias]) if alias else set(),
                    'columns': {},
                    'key': f"{table_name}_sk"  # assume surrogate key pattern
                }
            else:
                if alias:
                    tables[table_name].setdefault('aliases', set()).add(alias)
                    if not tables[table_name].get('alias'):
                        tables[table_name]['alias'] = alias
        
        # Extract columns using explicit qualifiers + generic name heuristics
        self._extract_columns_from_expression(self.parsed, tables, cte_names)
        
        return tables
    
    def _extract_columns_from_expression(self, expr, tables: Dict, cte_names: Set[str] = None):
        """Recursively extract column references from any expression."""
        cte_names = cte_names or set()

        # Build alias map
        alias_map = {}
        for t_name, t_info in tables.items():
            if t_info.get('alias'):
                alias_map[t_info['alias']] = t_name
            for alias in t_info.get('aliases', set()):
                if alias:
                    alias_map[alias] = t_name
            alias_map[t_name] = t_name

        # Collect all derived table/CTE aliases in the expression
        derived_aliases = set(cte_names)
        for subq in expr.find_all(exp.Subquery):
            if subq.alias:
                derived_aliases.add(subq.alias)
        for derived in expr.find_all(exp.DerivedTable):
            if hasattr(derived, 'alias') and derived.alias:
                derived_aliases.add(derived.alias)

        # Bug 1 fix: Collect output column aliases from CTEs and derived tables
        # These are NOT base table columns (e.g., d_week_seq1 from Q2 subquery)
        derived_col_aliases = set()
        for cte in expr.find_all(exp.CTE):
            cte_select = cte.find(exp.Select)
            if cte_select:
                for sel_col in cte_select.expressions:
                    if hasattr(sel_col, 'alias') and sel_col.alias:
                        derived_col_aliases.add(sel_col.alias)
        for subq in expr.find_all(exp.Subquery):
            sub_select = subq.find(exp.Select)
            if sub_select:
                for sel_col in sub_select.expressions:
                    if hasattr(sel_col, 'alias') and sel_col.alias:
                        derived_col_aliases.add(sel_col.alias)

        def _strict_prefix_table_match(col_name: str, candidate_tables: Set[str]) -> Optional[str]:
            col_lower = col_name.lower()
            parts = [p for p in col_lower.split('_') if p]
            if not parts:
                return None
            prefix = parts[0]
            matched: List[str] = []
            for table_name in candidate_tables:
                table_lower = table_name.lower()
                table_tokens = [t for t in table_lower.split('_') if t]
                variants = _table_name_variants(table_name)
                abbrev = _table_abbreviation(table_name)
                is_match = False
                if prefix in variants:
                    is_match = True
                elif abbrev and prefix == abbrev:
                    is_match = True
                elif (
                    len(prefix) == 1
                    and table_tokens
                    and table_lower.endswith('_dim')
                    and prefix == table_tokens[0][0]
                ):
                    is_match = True
                if is_match:
                    matched.append(table_name)
            if len(matched) == 1:
                return matched[0]
            return None

        def _direct_scope_sources(select_node: Optional[exp.Select]) -> Tuple[Set[str], bool]:
            """Return (base_tables, has_non_base_source) for a SELECT scope."""
            if select_node is None:
                return set(), False
            base_tables: Set[str] = set()
            has_non_base = False

            source_nodes: List[exp.Expression] = []
            from_expr = select_node.args.get('from')
            if from_expr is not None and getattr(from_expr, 'this', None) is not None:
                source_nodes.append(from_expr.this)
            for join_expr in select_node.args.get('joins') or []:
                if getattr(join_expr, 'this', None) is not None:
                    source_nodes.append(join_expr.this)

            for src in source_nodes:
                if isinstance(src, exp.Table):
                    src_name = src.name
                    if src_name in cte_names:
                        has_non_base = True
                    elif src_name in tables:
                        base_tables.add(src_name)
                    else:
                        # Unknown table reference: treat conservatively as non-base.
                        has_non_base = True
                else:
                    # Subquery / derived source at this scope.
                    has_non_base = True

            return base_tables, has_non_base

        unresolved_unqualified: List[Tuple[str, str, Set[str], bool]] = []

        # Find all columns in the expression
        for col in expr.find_all(exp.Column):
            table_name = col.table
            col_name = col.name
            col_lower = col_name.lower()

            if table_name:
                # Column has explicit table reference
                if table_name in cte_names or table_name in derived_aliases:
                    continue
                real_table = alias_map.get(table_name, table_name)
                if real_table in tables and col_name not in tables[real_table]['columns']:
                    col_type = self._infer_column_type(col_name)
                    tables[real_table]['columns'][col_name] = {
                        'type': col_type,
                        'nullable': True
                    }
            else:
                select_scope = col.find_ancestor(exp.Select)
                scope_base_tables, scope_has_non_base = _direct_scope_sources(select_scope)
                if not scope_base_tables:
                    continue

                # Prefer strict prefix/abbrev matching within the current SELECT scope.
                matched_table = _strict_prefix_table_match(col_name, scope_base_tables)
                if not matched_table:
                    # In mixed base+derived scopes, avoid heuristic remapping of
                    # unqualified columns; they frequently belong to derived sources.
                    if scope_has_non_base:
                        continue
                    # If this token is a derived alias, do not force it onto a base table.
                    if col_name in derived_col_aliases:
                        continue
                    if len(scope_base_tables) == 1:
                        matched_table = next(iter(scope_base_tables))
                    else:
                        matched_table = get_table_from_column(col_name, scope_base_tables)
                    if not matched_table:
                        parts = [p for p in col_lower.split('_') if p]
                        prefix = parts[0] if parts else ''
                        if prefix:
                            family_tables: List[str] = []
                            marker = f"{prefix}_"
                            for candidate_table in scope_base_tables:
                                existing_cols = tables.get(candidate_table, {}).get('columns', {})
                                if any(str(ec).lower().startswith(marker) for ec in existing_cols.keys()):
                                    family_tables.append(candidate_table)
                            if len(family_tables) == 1:
                                matched_table = family_tables[0]

                if matched_table and matched_table in tables:
                    if col_name not in tables[matched_table]['columns']:
                        col_type = self._infer_column_type(col_name)
                        tables[matched_table]['columns'][col_name] = {
                            'type': col_type,
                            'nullable': True
                        }
                else:
                    unresolved_unqualified.append(
                        (col_name, col_lower, set(scope_base_tables), scope_has_non_base)
                    )

        # Second pass: resolve previously ambiguous unqualified columns after
        # first-pass strong matches have seeded prefix families.
        for col_name, col_lower, scope_base_tables, scope_has_non_base in unresolved_unqualified:
            if not scope_base_tables or scope_has_non_base:
                continue
            if col_name in derived_col_aliases:
                continue

            matched_table = _strict_prefix_table_match(col_name, scope_base_tables)
            if not matched_table:
                if len(scope_base_tables) == 1:
                    matched_table = next(iter(scope_base_tables))
                else:
                    matched_table = get_table_from_column(col_name, scope_base_tables)
            if not matched_table:
                parts = [p for p in col_lower.split('_') if p]
                prefix = parts[0] if parts else ''
                if prefix:
                    marker = f"{prefix}_"
                    family_tables: List[str] = []
                    for candidate_table in scope_base_tables:
                        existing_cols = tables.get(candidate_table, {}).get('columns', {})
                        if any(str(ec).lower().startswith(marker) for ec in existing_cols.keys()):
                            family_tables.append(candidate_table)
                    if len(family_tables) == 1:
                        matched_table = family_tables[0]

            if matched_table and matched_table in tables and col_name not in tables[matched_table]['columns']:
                col_type = self._infer_column_type(col_name)
                tables[matched_table]['columns'][col_name] = {
                    'type': col_type,
                    'nullable': True,
                }
    
    def _infer_column_type(self, col_name: str) -> str:
        """Infer column type from column name."""
        col_lower = col_name.lower()
        
        # Surrogate keys and IDs (most specific)
        if col_lower.endswith('_sk') or col_lower.endswith('_id') or col_lower == 'id':
            return 'INTEGER'
        elif col_lower.endswith('_key'):
            return 'INTEGER'

        # Time-of-day attributes are commonly numeric in benchmark schemas
        # (e.g., time_dim.t_time), even when the token contains "time".
        if col_lower in {'t_time', 'time', 'hour', 'minute', 'second'}:
            return 'INTEGER'
        if (
            col_lower.endswith('_time')
            and 'timestamp' not in col_lower
            and 'date' not in col_lower
        ):
            return 'INTEGER'
        if 'sq_ft' in col_lower or 'square_feet' in col_lower:
            return 'INTEGER'
        
        # Date-related columns - check BEFORE numeric patterns
        # Match columns ending in _date, _dt, or named 'date', 'sales_date', 'order_date', etc.
        if col_lower in ['date', 'd_date', 'start_date', 'end_date', 'create_date', 'update_date']:
            return 'DATE'
        elif col_lower.endswith('_date') or col_lower.endswith('_dt'):
            return 'DATE'
        elif col_lower in ['d_year', 'd_month', 'd_day', 'd_quarter', 'd_week', 'd_moy', 'd_qoy',
                           'd_dom', 'd_dow', 'd_fy_year', 'd_fy_quarter_seq', 'd_fy_week_seq',
                           'd_week_seq', 'd_month_seq', 'd_quarter_seq',
                           'd_first_dom', 'd_last_dom', 'd_same_day_ly', 'd_same_day_lq']:
            return 'INTEGER'
        elif col_lower.endswith('_year') or col_lower.endswith('_month') or col_lower.endswith('_quarter'):
            return 'INTEGER'
        elif col_lower in ['year', 'month', 'moy', 'qoy', 'dom']:
            return 'INTEGER'

        # Name/description columns should remain textual even when they include
        # date-like tokens (e.g., d_day_name).
        if any(str_col in col_lower for str_col in ['name', 'desc', 'description', 'type', 'category', 'state', 'city', 'email', 'address', 'phone']):
            return 'VARCHAR(100)'
        if 'country' in col_lower or 'county' in col_lower:
            return 'VARCHAR(100)'

        # Numeric columns - be careful not to match date-related columns
        if any(num_col in col_lower for num_col in ['qty', 'quantity', 'number', 'count', 'seq', 'year', 'month', 'day', 'week']):
            return 'INTEGER'
        elif any(
            num_col in col_lower
            for num_col in [
                'amt', 'amount', 'price', 'cost', 'fee', 'tax', 'discount', 'profit', 'loss',
                'cash', 'credit', 'charge', 'sales', 'revenue', 'total', 'net', 'gross',
                'wholesale', 'list', 'coupon', 'return', 'ratio', 'rate', 'margin'
            ]
        ):
            return 'DECIMAL(18,2)'
        # 'sales' and 'revenue' should be numeric, but 'sales_date' should be date (handled above)
        elif col_lower.endswith('sales') or col_lower.endswith('revenue'):
            return 'DECIMAL(18,2)'
        elif (col_lower == 'count' or col_lower.endswith('_count') or col_lower.startswith('count_')):
            return 'INTEGER'
        
        # String columns
        if col_lower.endswith('_id') and not col_lower.endswith('_sk'):
            return 'VARCHAR(50)'

        # Fallback: lexical vector similarity over column-name tokens.
        sim_type, _ = infer_type_by_similarity(col_name)
        if sim_type:
            return sim_type

        return 'VARCHAR(50)'


class SyntheticDataGenerator:
    """Generates synthetic data for tables to produce ~1000 row query results."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, all_schemas: Dict = None):
        self.conn = conn
        self.random = random.Random(42)  # reproducible
        self.all_schemas = all_schemas or {}
        self.foreign_key_values = {}  # Store FK values for referential integrity
        self.filter_matched_values = {}  # Store PK values that match common filters
        self.fk_anchor_values = {}  # Stable FK subsets to improve multi-fact overlap
        # Tracked per-table join-key columns used for composite FK correlation.
        self.table_column_values: Dict[str, Dict[str, List[Any]]] = {}
        self.table_primary_keys: Dict[str, str] = {}
        self.fk_anchor_row_indexes: Dict[str, List[int]] = {}

    @staticmethod
    def _is_join_key_col(col_name: str) -> bool:
        col_lower = col_name.lower()
        if col_lower.endswith('_sk') or col_lower.endswith('_id') or col_lower == 'id':
            return True
        if col_lower.endswith('_order_number') or col_lower.endswith('_ticket_number'):
            return True
        return col_lower in {'order_number', 'ticket_number'}

    def _row_year_from_date_fk(
        self,
        foreign_keys: Dict[str, Tuple[str, str]],
        row_context: Optional[Dict[str, Any]],
    ) -> Optional[int]:
        if not row_context:
            return None
        for _child_col, (parent_table, _parent_col) in foreign_keys.items():
            parent_lower = str(parent_table).lower()
            is_date_parent = (
                parent_lower == "date_dim"
                or (parent_lower.endswith("_dim") and "date" in parent_lower)
            )
            if not is_date_parent:
                continue
            parent_idx = row_context.get(f"__parent_row_idx__:{parent_table}")
            if parent_idx is None:
                continue
            years = self.table_column_values.get(parent_table, {}).get("d_year")
            if years and 0 <= int(parent_idx) < len(years):
                try:
                    return int(years[int(parent_idx)])
                except (TypeError, ValueError):
                    return None
        return None
        
    def generate_table_data(self, table_name: str, schema: Dict, row_count: int = 1000, 
                           foreign_keys: Dict = None):
        """Generate and insert synthetic data for a table."""
        columns = schema['columns']
        foreign_keys = foreign_keys or {}
        
        # Build INSERT statement
        col_names = list(columns.keys())
        if not col_names:
            col_names = [schema['key']]  # fallback to surrogate key
            columns = {schema['key']: {'type': 'INTEGER', 'nullable': False}}
        
        placeholders = ', '.join(['?' for _ in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"

        # Detect PK up front so value generation can avoid forcing sequential
        # values on non-PK key-like columns.
        pk_col = find_primary_key_column(table_name, col_names)
        
        # Generate rows with table-local deterministic RNG so changing row count
        # of one table doesn't perturb data generation in other tables.
        seed_input = f"{table_name}|{row_count}"
        seed = int(hashlib.sha256(seed_input.encode("utf-8")).hexdigest()[:16], 16)
        prior_rng = self.random
        self.random = random.Random(seed)
        try:
            rows = []
            fk_cols = [c for c in col_names if c.lower() in foreign_keys]
            non_fk_cols = [c for c in col_names if c.lower() not in foreign_keys]
            generation_cols = fk_cols + non_fk_cols
            for i in range(row_count):
                row_ctx: Dict[str, Any] = {}
                row_values: Dict[str, Any] = {}
                for col_name in generation_cols:
                    col_info = columns[col_name]
                    value = self._generate_value(
                        col_name,
                        col_info['type'],
                        i,
                        row_count,
                        foreign_keys,
                        table_name,
                        primary_key_col=pk_col,
                        row_context=row_ctx,
                    )
                    row_values[col_name] = value
                rows.append(tuple(row_values[c] for c in col_names))
        finally:
            self.random = prior_rng
        
        # Bulk insert using VALUES clause (790x faster than executemany on DuckDB)
        batch_size = 500
        col_list = ', '.join(col_names)
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            values_parts = []
            for row in batch:
                vals = []
                for v in row:
                    if v is None:
                        vals.append('NULL')
                    elif isinstance(v, str):
                        vals.append("'" + v.replace("'", "''") + "'")
                    elif isinstance(v, float):
                        vals.append(str(v))
                    else:
                        vals.append(str(v))
                values_parts.append('(' + ', '.join(vals) + ')')
            self.conn.execute(f"INSERT INTO {table_name} ({col_list}) VALUES {', '.join(values_parts)}")

        # Track join-key columns so children can reuse coherent parent row combos.
        tracked_cols: Dict[str, List[Any]] = {}
        for col_idx, col_name in enumerate(col_names):
            is_pk = bool(pk_col and col_name.lower() == pk_col.lower())
            if not is_pk and not self._is_join_key_col(col_name):
                continue
            tracked_cols[col_name.lower()] = [row[col_idx] for row in rows]
        if tracked_cols:
            self.table_column_values[table_name] = tracked_cols
        
        # Store PK values for this table so other tables can reference them as FKs.
        if pk_col:
            if table_name not in self.foreign_key_values:
                self.foreign_key_values[table_name] = []
            col_idx = col_names.index(pk_col)
            for row in rows:
                self.foreign_key_values[table_name].append(row[col_idx])
            self.table_primary_keys[table_name] = pk_col.lower()
            
            # For dimension tables, also track which PK values match common filter conditions
            self._track_filter_matched_values(table_name, pk_col, col_names, rows)
    
    def _parse_decimal_type(self, col_type: str) -> Dict:
        """Parse DECIMAL(precision, scale) to extract precision and scale."""
        upper = (col_type or "").upper()
        if "DECIMAL" not in upper:
            return None

        # DECIMAL(p, s)
        match_full = re.search(r"DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", upper)
        if match_full:
            return {
                "precision": int(match_full.group(1)),
                "scale": int(match_full.group(2)),
            }

        # DECIMAL(p)
        match_single = re.search(r"DECIMAL\s*\(\s*(\d+)\s*\)", upper)
        if match_single:
            return {
                "precision": int(match_single.group(1)),
                "scale": 0,
            }

        # Bare DECIMAL default for synthetic generation.
        return {"precision": 18, "scale": 2}
    
    def _generate_value(self, col_name: str, col_type: str, row_idx: int, total_rows: int,
                       foreign_keys: Dict = None, table_name: str = None,
                       primary_key_col: Optional[str] = None,
                       row_context: Optional[Dict[str, Any]] = None):
        """Generate a single synthetic value."""
        col_name_lower = col_name.lower()
        pk_lower = primary_key_col.lower() if primary_key_col else None
        foreign_keys = foreign_keys or {}
        col_type_upper = col_type.upper()

        decimal_info = self._parse_decimal_type(col_type)

        def _remember_date_dim_value(value: Any) -> None:
            if row_context is None or not table_name or table_name.lower() != 'date_dim':
                return
            try:
                if col_name_lower in {'d_year', 'd_fy_year'}:
                    row_context['__date_dim_year'] = int(float(value))
                elif col_name_lower in {'d_month', 'd_moy'}:
                    month_i = int(float(value))
                    if month_i < 1:
                        month_i = 1
                    elif month_i > 12:
                        month_i = 12
                    row_context['__date_dim_month'] = month_i
            except (TypeError, ValueError):
                return

        # Check if filter_literal_values has a value for this table+column
        # Inject matching values ~70% of the time so WHERE filters produce results
        filter_vals = getattr(self, 'filter_literal_values', {})
        if table_name and table_name in filter_vals and col_name in filter_vals[table_name]:
            vals = filter_vals[table_name][col_name]
            if vals:
                plain_literals = [
                    v for v in vals
                    if not (isinstance(v, str) and (v.startswith('BETWEEN:') or (':' in v and v and v[0] in '><')))
                ]
                is_numeric_col = any(t in col_type_upper for t in ('INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE'))
                is_measure_col = any(
                    tok in col_name_lower
                    for tok in ('fee', 'amount', 'amt', 'sales', 'price', 'cost', 'tax', 'revenue', 'profit', 'loss', 'total')
                )
                if is_numeric_col and is_measure_col and len(plain_literals) > 1:
                    # Keep value choice stable per FK parent bucket so grouped
                    # aggregates see meaningful low/high variance.
                    bucket_seed = 0
                    if row_context:
                        parent_idxs = [
                            int(v) for k, v in row_context.items()
                            if k.startswith('__parent_row_idx__:') and isinstance(v, (int, float))
                        ]
                        if parent_idxs:
                            bucket_seed = sum(parent_idxs)
                    chosen = plain_literals[bucket_seed % len(plain_literals)]
                else:
                    chosen = vals[row_idx % len(vals)]
                # Deterministic cycling ensures every filtered column gets
                # satisfying values even under highly selective workloads.
                # Handle BETWEEN ranges
                if isinstance(chosen, str) and chosen.startswith('BETWEEN:'):
                    _, low, high = chosen.split(':', 2)
                    if 'DATE' in col_type or 'date' in col_name_lower:
                        try:
                            low_d = datetime.strptime(low, '%Y-%m-%d')
                            high_d = datetime.strptime(high, '%Y-%m-%d')
                            span = max(1, (high_d - low_d).days)
                            offset = row_idx % (span + 1)
                            return (low_d + timedelta(days=offset)).strftime('%Y-%m-%d')
                        except ValueError:
                            pass
                    elif 'INTEGER' in col_type:
                        return self.random.randint(int(low), int(high))
                    else:
                        low_f = float(low)
                        high_f = float(high)
                        span = max(high_f - low_f, 1.0)
                        return low_f + ((row_idx % 1000) / 1000.0) * span
                # Handle comparison operators (>:, >=:, <:)
                elif isinstance(chosen, str) and ':' in chosen and chosen[0] in '><':
                    op, v = chosen.split(':', 1)
                    # Date comparisons
                    if 'DATE' in col_type or 'date' in col_name_lower:
                        try:
                            base = datetime.strptime(v, '%Y-%m-%d')
                            if op == '>':
                                return (base + timedelta(days=1 + (row_idx % 365))).strftime('%Y-%m-%d')
                            if op == '>=':
                                return (base + timedelta(days=(row_idx % 365))).strftime('%Y-%m-%d')
                            if op == '<':
                                return (base - timedelta(days=1 + (row_idx % 365))).strftime('%Y-%m-%d')
                            if op == '<=':
                                return (base - timedelta(days=(row_idx % 365))).strftime('%Y-%m-%d')
                        except ValueError:
                            pass
                    # Numeric comparisons
                    try:
                        if 'INTEGER' in col_type:
                            n = int(float(v))
                            if op == '>':
                                return n + 1 + (row_idx % 50)
                            if op == '>=':
                                return n + (row_idx % 50)
                            if op == '<':
                                return max(0, n - 1 - (row_idx % 50))
                            if op == '<=':
                                return max(0, n - (row_idx % 50))
                        if 'DECIMAL' in col_type:
                            n = float(v)
                            if op == '>':
                                return round(n + 0.01 + (row_idx % 50) * 0.1, 2)
                            if op == '>=':
                                return round(n + (row_idx % 50) * 0.1, 2)
                            if op == '<':
                                return round(n - 0.01 - (row_idx % 50) * 0.1, 2)
                            if op == '<=':
                                return round(n - (row_idx % 50) * 0.1, 2)
                    except (ValueError, TypeError):
                        pass
                else:
                    # Direct equality / IN value
                    if 'INTEGER' in col_type:
                        try:
                            out = int(chosen)
                            _remember_date_dim_value(out)
                            return out
                        except (ValueError, TypeError):
                            pass
                    elif 'DECIMAL' in col_type:
                        try:
                            out = float(chosen)
                            _remember_date_dim_value(out)
                            return out
                        except (ValueError, TypeError):
                            pass
                    else:
                        _remember_date_dim_value(chosen)
                        return str(chosen)
        
        # Check if this is a foreign key column
        fk_target = foreign_keys.get(col_name_lower)
        if fk_target:
            target_table, target_col = fk_target
            target_col_lower = str(target_col).lower()
            parent_row_key = f"__parent_row_idx__:{target_table}"
            target_lower = target_table.lower()
            is_temporal_dim = (
                target_lower in {'date_dim', 'time_dim'}
                or ('date' in target_lower and target_lower.endswith('_dim'))
                or ('time' in target_lower and target_lower.endswith('_dim'))
            )

            # Composite-key correlation: for multiple child columns pointing to
            # one parent table, pin all to the same parent row in this output row.
            parent_tracked_cols = self.table_column_values.get(target_table, {})
            tracked_target_vals = parent_tracked_cols.get(target_col_lower)
            if tracked_target_vals:
                if is_temporal_dim:
                    return self.random.choice(tracked_target_vals)

                parent_idx = row_context.get(parent_row_key) if row_context else None
                if parent_idx is None:
                    same_parent_fk_count = sum(
                        1 for _child_col, (parent_t, _parent_c) in foreign_keys.items()
                        if parent_t == target_table
                    )
                    # For composite joins (e.g., order_number + item_sk), cover
                    # the full parent domain to maximize matching combinations.
                    if same_parent_fk_count > 1:
                        matched_parent_pks = self.filter_matched_values.get(target_table, [])
                        pk_col_lower = self.table_primary_keys.get(target_table)
                        pk_vals = parent_tracked_cols.get(pk_col_lower) if pk_col_lower else None
                        if matched_parent_pks and pk_vals:
                            matched_set = set(matched_parent_pks)
                            allowed = [i for i, v in enumerate(pk_vals) if v in matched_set]
                            if allowed:
                                parent_idx = allowed[row_idx % len(allowed)]
                            else:
                                parent_idx = row_idx % len(tracked_target_vals)
                        else:
                            parent_idx = row_idx % len(tracked_target_vals)
                    else:
                        anchors = self.fk_anchor_row_indexes.get(target_table)
                        if not anchors:
                            anchors = []
                            # Prefer parent rows whose PK satisfies known filters.
                            matched_parent_pks = self.filter_matched_values.get(target_table, [])
                            pk_col_lower = self.table_primary_keys.get(target_table)
                            pk_vals = parent_tracked_cols.get(pk_col_lower) if pk_col_lower else None
                            if matched_parent_pks and pk_vals:
                                matched_set = set(matched_parent_pks)
                                allowed = [i for i, v in enumerate(pk_vals) if v in matched_set]
                                if allowed:
                                    anchor_size = min(64, len(allowed))
                                    anchors = allowed[:anchor_size]

                            if not anchors:
                                n_vals = len(tracked_target_vals)
                                anchor_size = max(8, min(64, n_vals))
                                anchors = list(range(anchor_size))
                            self.fk_anchor_row_indexes[target_table] = anchors
                        parent_idx = anchors[row_idx % len(anchors)]
                    if row_context is not None:
                        row_context[parent_row_key] = parent_idx
                if 0 <= parent_idx < len(tracked_target_vals):
                    return tracked_target_vals[parent_idx]

            # Prefer filter-matched values if available
            if target_table in self.filter_matched_values and self.filter_matched_values[target_table]:
                candidates = self.filter_matched_values[target_table]
                if is_temporal_dim:
                    return self.random.choice(candidates)

                anchors = self.fk_anchor_values.get(target_table)
                if not anchors:
                    uniq = list(dict.fromkeys(candidates))
                    anchor_size = max(8, min(64, len(uniq)))
                    anchors = uniq[:anchor_size]
                    self.fk_anchor_values[target_table] = anchors
                return anchors[row_idx % len(anchors)]
            elif target_table in self.foreign_key_values:
                fk_vals = self.foreign_key_values[target_table]
                if fk_vals:
                    # Randomly select from existing FK values
                    return self.random.choice(fk_vals)
        
        # Surrogate keys - sequential
        is_numeric_type = any(
            t in col_type_upper for t in ('INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE')
        )
        if pk_lower and col_name_lower == pk_lower:
            if decimal_info:
                max_val = 10 ** (decimal_info['precision'] - decimal_info['scale']) - 1
                val = min(row_idx + 1, max_val)
            else:
                val = row_idx + 1
            return val

        # Keep date_dim attributes internally consistent so self-joins on week/
        # month/quarter sequences have realistic hit rates.
        if table_name and table_name.lower() == 'date_dim':
            def _fixed_filter_int(*candidates: str) -> Optional[int]:
                table_filters = filter_vals.get(table_name, {})
                for candidate in candidates:
                    vals = table_filters.get(candidate)
                    if not vals:
                        continue
                    for raw_val in vals:
                        txt = str(raw_val).strip()
                        if not txt:
                            continue
                        if txt.startswith('BETWEEN:'):
                            continue
                        if ':' in txt and txt[0] in {'>', '<'}:
                            continue
                        try:
                            return int(float(txt.strip("'\"")))
                        except (TypeError, ValueError):
                            continue
                return None

            base_date = datetime(1990, 1, 1) + timedelta(days=(row_idx % (365 * 40)))
            year_for_seq = None
            month_for_seq = None
            if row_context is not None:
                year_for_seq = row_context.get('__date_dim_year')
                month_for_seq = row_context.get('__date_dim_month')
            if year_for_seq is None:
                year_for_seq = _fixed_filter_int('d_year', 'd_fy_year')
            if month_for_seq is None:
                month_for_seq = _fixed_filter_int('d_moy', 'd_month')
            if year_for_seq is None:
                year_for_seq = base_date.year
            if month_for_seq is None:
                month_for_seq = base_date.month
            try:
                year_for_seq = int(year_for_seq)
            except (TypeError, ValueError):
                year_for_seq = base_date.year
            try:
                month_for_seq = int(month_for_seq)
            except (TypeError, ValueError):
                month_for_seq = base_date.month
            if month_for_seq < 1:
                month_for_seq = 1
            elif month_for_seq > 12:
                month_for_seq = 12

            day_for_date = 1 + (row_idx % 28)
            try:
                current_date = datetime(year_for_seq, month_for_seq, day_for_date)
            except ValueError:
                current_date = datetime(max(1900, min(2100, year_for_seq)), month_for_seq, 1)

            if col_name_lower == 'd_date_sk':
                return row_idx + 1
            if col_name_lower in {'d_date'} or col_name_lower.endswith('_date'):
                return current_date.strftime('%Y-%m-%d')
            if col_name_lower in {'d_year', 'd_fy_year'}:
                _remember_date_dim_value(year_for_seq)
                return year_for_seq
            if col_name_lower in {'d_month', 'd_moy'}:
                _remember_date_dim_value(month_for_seq)
                return month_for_seq
            if col_name_lower in {'d_day', 'd_dom'}:
                return current_date.day
            if col_name_lower == 'd_day_name':
                day_names = [
                    'Monday', 'Tuesday', 'Wednesday', 'Thursday',
                    'Friday', 'Saturday', 'Sunday',
                ]
                return day_names[current_date.weekday()]
            if col_name_lower in {'d_quarter', 'd_qoy'}:
                return ((month_for_seq - 1) // 3) + 1
            if col_name_lower in {'d_week_seq', 'd_fy_week_seq'}:
                return 1 + (row_idx // 7)
            if col_name_lower == 'd_month_seq':
                return 1 + (year_for_seq - 1990) * 12 + (month_for_seq - 1)
            if col_name_lower in {'d_quarter_seq', 'd_fy_quarter_seq'}:
                return 1 + (year_for_seq - 1990) * 4 + ((month_for_seq - 1) // 3)
            if col_name_lower == 'd_dow':
                return ((current_date.weekday() + 1) % 7) + 1

        # Date/time-like columns
        if col_name_lower in {'d_date', 'date'}:
            base_date = datetime(2000, 1, 1)
            offset = row_idx % (365 * 25)  # 25 years of stable synthetic dates
            return (base_date + timedelta(days=offset)).strftime('%Y-%m-%d')
        elif col_name_lower in {'d_year', 'year'} or col_name_lower.endswith('_year'):
            return 1990 + (row_idx % 36)
        elif col_name_lower in {'d_month', 'd_moy', 'month', 'moy'} or col_name_lower.endswith('_month'):
            return 1 + (row_idx % 12)
        elif col_name_lower in {'d_day', 'day', 'dom'} or col_name_lower.endswith('_day'):
            return 1 + (row_idx % 28)
        elif col_name_lower in {'d_quarter', 'd_qoy', 'quarter', 'qoy'} or col_name_lower.endswith('_quarter'):
            return 1 + (row_idx % 4)
        elif is_numeric_type and (
            col_name_lower.endswith('_date_sk')
            or col_name_lower.endswith('_time_sk')
            or col_name_lower.endswith('_date_id')
            or col_name_lower.endswith('_time_id')
        ):
            key_domain = max(50, min(1000, max(10, total_rows // 4)))
            if decimal_info:
                max_val = 10 ** (decimal_info['precision'] - decimal_info['scale']) - 1
                return self.random.randint(1, min(key_domain, max_val))
            return self.random.randint(1, key_domain)
        elif 'date' in col_name_lower or col_type == 'DATE':
            base_date = datetime(2000, 1, 1)
            offset = row_idx % (365 * 25)
            return (base_date + timedelta(days=offset)).strftime('%Y-%m-%d')
        
        # Numeric types
        if 'DECIMAL' in col_type or 'INTEGER' in col_type or 'BIGINT' in col_type:
            if decimal_info:
                precision = decimal_info['precision']
                scale = decimal_info['scale']
                max_val = 10 ** (precision - scale) - 1
                
                if 'return_amount' in col_name_lower or 'return_amt' in col_name_lower:
                    val = self.random.uniform(0, min(200, max_val))
                    return round(val, scale)
                elif 'qty' in col_name_lower or 'quantity' in col_name_lower:
                    return self.random.randint(1, min(100, max_val))
                elif 'amt' in col_name_lower or 'amount' in col_name_lower or 'sales' in col_name_lower:
                    val = self.random.uniform(10, min(10000, max_val))
                    return round(val, scale)
                elif 'price' in col_name_lower or 'cost' in col_name_lower:
                    val = self.random.uniform(1, min(500, max_val))
                    return round(val, scale)
                elif 'fee' in col_name_lower or 'tax' in col_name_lower:
                    val = self.random.uniform(0, min(100, max_val))
                    return round(val, scale)
                elif col_name_lower.endswith('_sk') or col_name_lower.endswith('_id'):
                    # Non-PK key-like attributes use a moderate domain so
                    # grouped dimensions overlap across years/channels.
                    key_domain = max(20, min(200, max(10, total_rows // 20)))
                    return self.random.randint(1, min(key_domain, max_val))
                else:
                    # Keep generic decimal measures in a safe practical range
                    # to avoid downstream cast overflow (e.g. DECIMAL(12,2)).
                    val = self.random.uniform(1, min(10000, max_val))
                    return round(val, scale)
            
            # Regular INTEGER/BIGINT
            if 'return_quantity' in col_name_lower:
                return self.random.randint(0, 20)
            elif 'qty' in col_name_lower or 'quantity' in col_name_lower:
                sales_year = self._row_year_from_date_fk(foreign_keys, row_context)
                if sales_year == 2001:
                    return self.random.randint(30, 100)
                if sales_year == 2002:
                    return self.random.randint(1, 60)
                return self.random.randint(1, 100)
            elif 'amt' in col_name_lower or 'amount' in col_name_lower or 'sales' in col_name_lower:
                return round(self.random.uniform(10.0, 10000.0), 2)
            elif 'price' in col_name_lower or 'cost' in col_name_lower:
                return round(self.random.uniform(1.0, 500.0), 2)
            elif 'fee' in col_name_lower or 'tax' in col_name_lower:
                return round(self.random.uniform(0.0, 100.0), 2)
            elif col_name_lower.endswith('_sk') or col_name_lower.endswith('_id'):
                key_domain = max(20, min(200, max(10, total_rows // 20)))
                return self.random.randint(1, key_domain)
            else:
                return self.random.randint(1, 100000)
        
        # String types
        if 'VARCHAR' in col_type:
            if 'state' in col_name_lower or col_name_lower == 's_state':
                states = [
                    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
                ]
                return states[row_idx % len(states)]
            elif 'city' in col_name_lower:
                cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
                         'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
                         'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte']
                return cities[row_idx % len(cities)]
            elif col_name_lower.endswith('_id') or col_name_lower == 'id':
                id_prefix = ''.join(part[:1] for part in (table_name or 'id').split('_')).upper()
                return f"{id_prefix}{row_idx:07d}"
            elif 'name' in col_name_lower:
                return f"Name_{row_idx}_{self.random.randint(1000, 9999)}"
            elif 'type' in col_name_lower:
                types = ['A', 'B', 'C', 'D', 'E']
                return types[row_idx % len(types)]
            elif 'category' in col_name_lower:
                # Include common filter values used in queries
                categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports',
                            'Home', 'Garden', 'Toys', 'Beauty', 'Health'] * 5
                return categories[row_idx % len(categories)]
            else:
                return f"VAL_{row_idx % 1000}_{self.random.randint(100, 999)}"
        
        return None
    
    def _track_filter_matched_values(self, table_name: str, pk_col: str, col_names: List[str], rows: List[tuple]):
        """Track PK values available for downstream FK assignment."""
        pk_idx = col_names.index(pk_col)
        self.filter_matched_values[table_name] = [row[pk_idx] for row in rows]


class SchemaFromDB:
    """Extracts full schema from a reference database (DuckDB or PostgreSQL).

    When a reference DB is provided, it is the authoritative source for
    column types and table schemas. AST extraction only identifies which
    tables are used; the reference DB supplies correct types for ALL columns.

    Supports:
      - DuckDB file paths (e.g., /path/to/db.duckdb)
      - PostgreSQL DSNs (e.g., postgres://user:pass@host:port/db)
    """

    # Map PG types to DuckDB-compatible types for CREATE TABLE
    PG_TYPE_MAP = {
        'integer': 'INTEGER',
        'bigint': 'BIGINT',
        'smallint': 'SMALLINT',
        'numeric': 'DECIMAL(18,2)',
        'real': 'FLOAT',
        'double precision': 'DOUBLE',
        'character varying': 'VARCHAR',
        'character': 'VARCHAR',
        'text': 'VARCHAR',
        'boolean': 'BOOLEAN',
        'date': 'DATE',
        'timestamp without time zone': 'TIMESTAMP',
        'timestamp with time zone': 'TIMESTAMPTZ',
        'time without time zone': 'TIME',
        'interval': 'INTERVAL',
        'bytea': 'BLOB',
    }

    # DSN schemes that SchemaFromDB can handle
    _SUPPORTED_SCHEMES = ('postgres://', 'postgresql://', 'duckdb://')

    @classmethod
    def supports_dsn(cls, dsn: str) -> bool:
        """Return True if this DSN scheme is supported for schema extraction.

        In-memory databases (:memory:, duckdb:///:memory:) are excluded —
        they have no persistent tables to extract schemas from.
        """
        if dsn is None:
            return False
        lower = dsn.lower()
        # In-memory has no tables to introspect
        if lower == ':memory:' or lower == 'duckdb:///:memory:':
            return False
        # Bare file paths with DuckDB extensions
        if lower.endswith('.duckdb') or lower.endswith('.db'):
            return True
        return any(lower.startswith(s) for s in cls._SUPPORTED_SCHEMES)

    @staticmethod
    def _normalize_duckdb_dsn(dsn: str) -> str:
        """Strip duckdb:// prefix so DuckDB gets a plain file path or :memory:."""
        if dsn.lower().startswith('duckdb://'):
            path = dsn[len('duckdb://'):]
            return path or ':memory:'
        return dsn

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._cache: Dict[str, Dict] = {}
        self._all_tables: Set[str] = set()
        self._is_pg = db_path.startswith('postgres://') or db_path.startswith('postgresql://')
        self._load_all_tables()

    def _pg_connect(self):
        """Connect to PostgreSQL via psycopg2."""
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError(
                "psycopg2 is required for PostgreSQL schema extraction. "
                "Install psycopg2 or psycopg2-binary."
            ) from e
        return psycopg2.connect(self.db_path)

    def _load_all_tables(self):
        """Cache the set of all table names in the reference DB."""
        if self._is_pg:
            conn = self._pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT DISTINCT table_name FROM information_schema.columns "
                    "WHERE table_schema = 'public'"
                )
                self._all_tables = {r[0] for r in cur.fetchall()}
            finally:
                conn.close()
        else:
            db_file = self._normalize_duckdb_dsn(self.db_path)
            conn = duckdb.connect(db_file, read_only=True)
            try:
                rows = conn.execute(
                    "SELECT DISTINCT table_name FROM information_schema.columns"
                ).fetchall()
                self._all_tables = {r[0] for r in rows}
            finally:
                conn.close()

    def _map_pg_type(self, pg_type: str) -> str:
        """Map a PostgreSQL type name to a DuckDB-compatible type."""
        return self.PG_TYPE_MAP.get(pg_type.lower(), 'VARCHAR')

    def get_table_schema(self, table_name: str) -> Dict:
        """Get full schema for a table from the reference DB.

        Returns all columns with their real types. Results are cached.
        PG types are mapped to DuckDB-compatible types.
        """
        if table_name in self._cache:
            return self._cache[table_name]

        if table_name not in self._all_tables:
            self._cache[table_name] = {}
            return {}

        if self._is_pg:
            conn = self._pg_connect()
            try:
                cur = conn.cursor()
                cur.execute(
                    "SELECT column_name, data_type, is_nullable, "
                    "       numeric_precision, numeric_scale, "
                    "       character_maximum_length "
                    "FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = %s "
                    "ORDER BY ordinal_position",
                    (table_name,)
                )
                columns = {}
                for row in cur.fetchall():
                    col_name, data_type, is_nullable = row[0], row[1], row[2]
                    num_prec, num_scale, char_len = row[3], row[4], row[5]
                    # Build precise DuckDB type from PG metadata
                    if data_type == 'numeric' and num_prec is not None:
                        col_type = f'DECIMAL({num_prec},{num_scale or 0})'
                    elif data_type in ('character varying', 'character') and char_len:
                        col_type = f'VARCHAR({char_len})'
                    else:
                        col_type = self._map_pg_type(data_type)
                    columns[col_name] = {
                        'type': col_type,
                        'nullable': is_nullable == 'YES'
                    }
                self._cache[table_name] = columns
                return columns
            finally:
                conn.close()
        else:
            db_file = self._normalize_duckdb_dsn(self.db_path)
            conn = duckdb.connect(db_file, read_only=True)
            try:
                result = conn.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()

                columns = {}
                for col_name, data_type, is_nullable in result:
                    columns[col_name] = {
                        'type': data_type,
                        'nullable': is_nullable == 'YES'
                    }

                self._cache[table_name] = columns
                return columns
            finally:
                conn.close()


class SyntheticValidator:
    """Main orchestrator for synthetic data validation."""

    _REPAIR_ACTION_TYPES = {
        "set_filter_values",
        "remove_filter",
        "add_fk",
        "set_row_count",
        "scale_row_counts",
        "set_generation_order",
        "set_min_rows",
        "set_max_rows",
    }
    _COVERAGE_SWARM_STRATEGIES: Tuple[Tuple[str, str], ...] = (
        ("coverage_filter_roots", "Coverage role: prioritize FILTER tables first; top up only the most selective root table."),
        ("coverage_filter_plus_bridge", "Coverage role: top up one FILTER table and one direct FK bridge/fact neighbor."),
        ("coverage_single_table_conservative", "Coverage role: one conservative top-up to minimize runtime and churn."),
    )
    _ADVERSARIAL_SWARM_STRATEGIES: Tuple[Tuple[str, str], ...] = (
        ("adversarial_edge_case", "Adversarial role: add rows likely to expose semantic drift while staying targeted and add-only."),
        ("adversarial_stuck_recovery", "Adversarial role: if prior attempts stalled, switch to a different targeted table pair."),
    )

    def __init__(
        self,
        reference_db: str = None,
        dialect: str = 'duckdb',
        llm_provider: Optional[str] = None,
        llm_model: Optional[str] = None,
        llm_max_retries: int = 1,
        repair_analyze_fn: Optional[Callable[[str], str]] = None,
        repair_mode: str = "add_only",
        llm_swarm_size: int = 1,
        llm_swarm_temperature_min: float = 0.0,
        llm_swarm_temperature_max: float = 0.5,
        llm_swarm_max_history: int = 6,
        progress_to_console: bool = True,
        coverage_time_budget_s: int = 60,
        adversarial_time_budget_s: int = 60,
        adversarial_log_path: str = "packages/qt-sql/qt_sql/validation/adversarial_efforts.jsonl",
    ):
        self.conn = duckdb.connect(':memory:')
        self.reference_db = reference_db
        self.dialect = dialect.lower()
        # LLM repair has been removed; keep legacy args for call-site compatibility.
        self.llm_provider = llm_provider
        self.llm_model = llm_model
        self._repair_analyze_fn = repair_analyze_fn
        mode = str(repair_mode or "add_only").strip().lower()
        self.repair_mode = mode if mode in {"add_only", "hybrid"} else "add_only"
        self.llm_swarm_size = max(1, int(llm_swarm_size))
        self.llm_swarm_temperature_min = float(llm_swarm_temperature_min)
        self.llm_swarm_temperature_max = float(llm_swarm_temperature_max)
        if self.llm_swarm_temperature_min > self.llm_swarm_temperature_max:
            self.llm_swarm_temperature_min, self.llm_swarm_temperature_max = (
                self.llm_swarm_temperature_max,
                self.llm_swarm_temperature_min,
            )
        self.llm_swarm_max_history = max(1, int(llm_swarm_max_history))
        self.progress_to_console = bool(progress_to_console)
        self.coverage_time_budget_s = max(0, int(coverage_time_budget_s))
        self.adversarial_time_budget_s = max(0, int(adversarial_time_budget_s))
        self.adversarial_log_path = str(adversarial_log_path)
        self._repair_llm_client = None
        self._repair_llm_initialized = False
        # Only create SchemaFromDB for supported DSN schemes
        if reference_db and SchemaFromDB.supports_dsn(reference_db):
            self.schema_extractor = SchemaFromDB(reference_db)
        else:
            self.schema_extractor = None

    def _emit_progress(self, message: str) -> None:
        if not self.progress_to_console:
            return
        ts = datetime.now().strftime("%H:%M:%S")
        print(f"[synthetic {ts}] {message}", flush=True)

    def _append_adversarial_effort(
        self,
        payload: Dict[str, Any],
        log_path: Optional[str] = None,
    ) -> None:
        path = Path(log_path or self.adversarial_log_path)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            line = json.dumps(payload, default=str)
            with path.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception as e:
            logger.debug("Failed to append adversarial effort log %s: %s", path, e)

    def _get_repair_llm_client(self):
        """LLM repair removed; retained as a no-op compatibility hook."""
        return None

    def _analyze_repair_prompt(self, prompt: str, temperature: Optional[float] = None) -> Optional[str]:
        _ = prompt
        _ = temperature
        return None

    @staticmethod
    def _extract_json_object(raw_text: str) -> Optional[Dict[str, Any]]:
        """Best-effort JSON object extraction from model output."""
        if not raw_text:
            return None

        text = raw_text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)

        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                return obj
        except Exception:
            pass

        # Try to decode the first valid JSON object embedded in free-form text.
        decoder = json.JSONDecoder()
        for i, ch in enumerate(text):
            if ch != "{":
                continue
            try:
                obj, _ = decoder.raw_decode(text[i:])
            except Exception:
                continue
            if isinstance(obj, dict):
                return obj

        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            candidate = text[start:end + 1]
            try:
                obj = json.loads(candidate)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                return None
        return None

    def _build_fallback_repair_plan(
        self,
        actual_rows: int,
        min_rows: int,
        max_rows: int,
        tables: Dict[str, Dict],
        table_row_counts: Dict[str, int],
        filter_values: Dict[str, Dict[str, list]],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        avoid_table_pairs: Optional[Set[Tuple[str, ...]]] = None,
        table_penalties: Optional[Dict[str, float]] = None,
        attempt: int = 1,
    ) -> Optional[Dict[str, Any]]:
        """Deterministic bounded repair planner."""
        if min_rows <= 0 or actual_rows >= min_rows:
            return None

        targeted_tables = self._select_targeted_topup_tables(
            tables=tables,
            table_row_counts=table_row_counts,
            filter_values=filter_values,
            fk_relationships=fk_relationships,
            max_tables=2,
            avoid_table_pairs=avoid_table_pairs,
            table_penalties=table_penalties,
        )

        actions: List[Dict[str, Any]] = []
        if targeted_tables:
            deficit_ratio = float(min_rows) / float(max(1, actual_rows))
            if deficit_ratio >= 20:
                base_multiplier = 2.8
            elif deficit_ratio >= 10:
                base_multiplier = 2.4
            elif deficit_ratio >= 5:
                base_multiplier = 2.1
            elif deficit_ratio >= 2:
                base_multiplier = 1.8
            else:
                base_multiplier = 1.5

            for idx, table_name in enumerate(targeted_tables):
                current = int(table_row_counts.get(table_name, 1000))
                has_fk = bool(fk_relationships.get(table_name))
                if min_rows >= 100:
                    floor = 12000 if has_fk else 6000
                    cap = 60000 if has_fk else 25000
                else:
                    floor = 8000 if has_fk else 4000
                    cap = 40000 if has_fk else 20000
                attempt_boost = max(0.0, min(1.0, (attempt - 1) * 0.25))
                mult = base_multiplier + (0.2 if idx == 0 else 0.0) + attempt_boost
                cap = min(200000, int(cap * (1.0 + attempt_boost)))
                target = min(200000, max(floor, min(cap, int(current * mult))))
                if target > current:
                    actions.append({
                        "type": "set_row_count",
                        "table": table_name,
                        "row_count": target,
                    })

        if not actions:
            base = max(1, int(actual_rows))
            needed_ratio = float(min_rows) / float(base)
            multiplier = max(1.3, min(3.0, needed_ratio * 1.15))
            actions = [
                {
                    "type": "scale_row_counts",
                    "multiplier": round(multiplier, 2),
                }
            ]

        if max_rows < min_rows:
            actions.append({"type": "set_max_rows", "value": int(min_rows)})

        return {
            "actions": actions,
            "note": (
                "Fallback repair: DAG-targeted table top-up (filter roots and "
                "their close FK neighbors) to improve hit rate with bounded cost."
            ),
        }

    def _select_targeted_topup_tables(
        self,
        tables: Dict[str, Dict],
        table_row_counts: Dict[str, int],
        filter_values: Dict[str, Dict[str, list]],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        max_tables: int = 2,
        avoid_table_pairs: Optional[Set[Tuple[str, ...]]] = None,
        table_penalties: Optional[Dict[str, float]] = None,
    ) -> List[str]:
        """Rank tables for selective top-up using a join-DAG walk from filters."""
        if not tables or max_tables <= 0:
            return []
        avoid_table_pairs = avoid_table_pairs or set()
        table_penalties = table_penalties or {}

        table_names = [t for t in tables.keys()]
        table_set = set(table_names)
        adjacency: Dict[str, Set[str]] = {t: set() for t in table_names}
        incoming: Dict[str, int] = {t: 0 for t in table_names}
        outgoing: Dict[str, int] = {t: 0 for t in table_names}

        for child_table, child_fks in fk_relationships.items():
            if child_table not in table_set:
                continue
            for _child_col, (parent_table, _parent_col) in child_fks.items():
                if parent_table not in table_set or parent_table == child_table:
                    continue
                adjacency[child_table].add(parent_table)
                adjacency[parent_table].add(child_table)
                outgoing[child_table] += 1
                incoming[parent_table] += 1

        roots = [t for t in table_names if t in filter_values]
        depth_map: Dict[str, int] = {}
        if roots:
            q = deque((root, 0) for root in roots)
            while q:
                node, depth = q.popleft()
                old_depth = depth_map.get(node)
                if old_depth is not None and old_depth <= depth:
                    continue
                depth_map[node] = depth
                if depth >= 2:
                    continue
                for nbr in adjacency.get(node, ()):
                    q.append((nbr, depth + 1))

        if depth_map:
            candidates = [t for t in table_names if t in depth_map]
        else:
            candidates = list(table_names)

        filter_strength: Dict[str, float] = {}
        for table_name, col_map in filter_values.items():
            score = 0.0
            if not isinstance(col_map, dict):
                continue
            for _col, vals in col_map.items():
                if not isinstance(vals, list):
                    vals = [vals]
                if any(isinstance(v, str) and v.startswith("BETWEEN:") for v in vals):
                    score += 3.0
                n = len([v for v in vals if v is not None])
                if n <= 1:
                    score += 2.5
                elif n <= 3:
                    score += 2.0
                else:
                    score += 1.0
            filter_strength[table_name] = score

        scored: List[Tuple[float, int, str]] = []
        for table_name in candidates:
            score = 0.0
            depth = depth_map.get(table_name, 3)
            if roots:
                if table_name in roots:
                    score += 120.0
                elif depth == 1:
                    score += 80.0
                elif depth == 2:
                    score += 40.0
            score += outgoing.get(table_name, 0) * 12.0
            score += incoming.get(table_name, 0) * 8.0
            score += len(adjacency.get(table_name, ())) * 4.0
            score += filter_strength.get(table_name, 0.0) * 18.0
            current_rows = int(table_row_counts.get(table_name, 1000))
            if current_rows < 5000:
                score += 8.0
            elif current_rows > 80000:
                score -= 10.0
            score -= float(table_penalties.get(table_name, 0.0)) * 22.0
            # Tie-breakers: lower current rows first, then lexical stability.
            scored.append((score, -current_rows, table_name))

        scored.sort(key=lambda x: (-x[0], -x[1], x[2]))
        ranked = [t for _s, _neg_rows, t in scored]
        if not ranked:
            return []

        first = ranked[0]
        selected = [first]

        if max_tables >= 2:
            second = None
            for cand in ranked[1:]:
                pair = tuple(sorted((first, cand)))
                if pair in avoid_table_pairs:
                    continue
                second = cand
                break
            if second is None and len(ranked) > 1:
                second = ranked[1]
            if second and second != first:
                selected.append(second)

        if roots and len(selected) >= 2 and all(t in roots for t in selected):
            non_roots = [t for t in ranked if t not in roots]
            for cand in non_roots:
                pair = tuple(sorted((selected[0], cand)))
                if pair in avoid_table_pairs:
                    continue
                selected[-1] = cand
                break

        # Preserve order while removing duplicates.
        selected = list(dict.fromkeys(selected))
        return selected

    def _build_table_depth_map(
        self,
        tables: Dict[str, Dict],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
    ) -> Dict[str, int]:
        table_names = set(tables.keys())
        roots = [t for t in filter_values.keys() if t in table_names]
        if not roots:
            return {}

        adjacency: Dict[str, Set[str]] = {t: set() for t in table_names}
        for child_table, child_fks in fk_relationships.items():
            if child_table not in table_names:
                continue
            for _col, (parent_table, _parent_col) in child_fks.items():
                if parent_table not in table_names or parent_table == child_table:
                    continue
                adjacency[child_table].add(parent_table)
                adjacency[parent_table].add(child_table)

        depth_map: Dict[str, int] = {}
        queue = deque((root, 0) for root in roots)
        while queue:
            table_name, depth = queue.popleft()
            prev = depth_map.get(table_name)
            if prev is not None and prev <= depth:
                continue
            depth_map[table_name] = depth
            if depth >= 3:
                continue
            for neighbor in adjacency.get(table_name, ()):
                queue.append((neighbor, depth + 1))
        return depth_map

    @staticmethod
    def _action_signature(action: Dict[str, Any]) -> str:
        action_type = str(action.get("type", "")).strip()
        if action_type == "set_row_count":
            table = str(action.get("table", ""))
            return f"set_row_count:{table}"
        if action_type == "scale_row_counts":
            return "scale_row_counts"
        if action_type == "set_max_rows":
            return "set_max_rows"
        if action_type == "set_filter_values":
            table = str(action.get("table", ""))
            column = str(action.get("column", ""))
            return f"set_filter_values:{table}.{column}"
        return action_type or "unknown"

    def _compact_repair_log(self, repair_log: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if not repair_log:
            return []

        compact: List[Dict[str, Any]] = []
        for entry in repair_log[-self.llm_swarm_max_history:]:
            if not isinstance(entry, dict):
                continue
            actions = entry.get("plan_actions")
            signatures: List[str] = []
            if isinstance(actions, list):
                signatures = [
                    self._action_signature(a)
                    for a in actions
                    if isinstance(a, dict)
                ][:4]
            if not signatures:
                fallback_actions = entry.get("actions")
                if isinstance(fallback_actions, list):
                    signatures = [str(x)[:80] for x in fallback_actions[:4]]
            compact.append({
                "attempt": int(entry.get("attempt", 0)),
                "source": str(entry.get("source", "")),
                "rows_before": int(entry.get("rows_before", 0) or 0),
                "rows_after": int(entry.get("rows_after", 0) or 0),
                "row_delta": int(entry.get("row_delta", 0) or 0),
                "regressed": bool(entry.get("regressed", False)),
                "actions": signatures,
            })
        return compact

    def _failed_action_signatures(self, repair_log: Optional[List[Dict[str, Any]]]) -> Set[Tuple[str, ...]]:
        failed: Set[Tuple[str, ...]] = set()
        if not repair_log:
            return failed
        for entry in repair_log:
            if not isinstance(entry, dict):
                continue
            row_delta = entry.get("row_delta")
            if row_delta is None:
                continue
            try:
                delta = int(row_delta)
            except (TypeError, ValueError):
                continue
            if delta > 0:
                continue
            actions = entry.get("plan_actions")
            if not isinstance(actions, list):
                continue
            sigs = sorted({
                self._action_signature(a)
                for a in actions
                if isinstance(a, dict)
            })
            if sigs:
                failed.add(tuple(sigs))
        return failed

    def _score_repair_plan(
        self,
        actions: List[Dict[str, Any]],
        tables: Dict[str, Dict],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
        table_row_counts: Dict[str, int],
        repair_log: Optional[List[Dict[str, Any]]],
        phase: str = "coverage",
    ) -> float:
        if not actions:
            return -120.0

        roots = set(t for t in filter_values.keys() if t in tables)
        depth_map = self._build_table_depth_map(tables, fk_relationships, filter_values)
        failed_signatures = self._failed_action_signatures(repair_log)

        regressed_tables: Set[str] = set()
        for entry in repair_log or []:
            if not isinstance(entry, dict) or not entry.get("regressed"):
                continue
            for action in entry.get("plan_actions") or []:
                if isinstance(action, dict) and str(action.get("type", "")) == "set_row_count":
                    table_name = str(action.get("table", ""))
                    if table_name:
                        regressed_tables.add(table_name)

        phase_name = str(phase or "coverage").strip().lower()
        is_adversarial = phase_name.startswith("adversarial")
        score = 0.0
        targeted_tables: Set[str] = set()
        candidate_sigs: List[str] = []
        for action in actions:
            if not isinstance(action, dict):
                score -= 40.0
                continue
            action_type = str(action.get("type", "")).strip()
            candidate_sigs.append(self._action_signature(action))
            if action_type == "set_row_count":
                table = str(action.get("table", ""))
                if table not in tables:
                    score -= 80.0
                    continue
                targeted_tables.add(table)
                try:
                    target_rows = int(action.get("row_count"))
                except (TypeError, ValueError):
                    score -= 60.0
                    continue
                current_rows = int(table_row_counts.get(table, 1000))
                ratio = float(target_rows) / float(max(1, current_rows))
                if target_rows <= current_rows:
                    score -= 15.0
                else:
                    score += 12.0
                if 1.25 <= ratio <= 3.2:
                    score += 12.0
                elif ratio <= 5.0:
                    score += 4.0
                else:
                    score -= 10.0
                depth = depth_map.get(table, 4)
                if table in roots:
                    score += 25.0
                elif depth == 1:
                    score += 16.0
                elif depth == 2:
                    score += 8.0
                if table in regressed_tables:
                    score -= 8.0
                if is_adversarial and ratio >= 2.0:
                    score += 4.0
            elif action_type == "scale_row_counts":
                score -= 55.0 if is_adversarial else 45.0
            elif action_type == "set_max_rows":
                score -= 2.0 if is_adversarial else 1.0
            else:
                score -= 35.0

        if len(actions) > 3:
            score -= 25.0
        if len(targeted_tables) > 2:
            score -= 20.0
        if not targeted_tables:
            score -= 20.0

        sig_tuple = tuple(sorted(set(s for s in candidate_sigs if s)))
        if sig_tuple and sig_tuple in failed_signatures:
            score -= 22.0
        if is_adversarial and sig_tuple and sig_tuple not in failed_signatures:
            score += 3.0
        return score

    @staticmethod
    def _normalize_repair_action(action: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize common model field aliases to canonical action schema."""
        if not isinstance(action, dict):
            return {}

        normalized = dict(action)
        action_type = str(normalized.get("type", "")).strip()

        if action_type == "set_row_count":
            if "row_count" not in normalized:
                for alias in ("count", "rows", "n", "value"):
                    if alias in normalized:
                        normalized["row_count"] = normalized.get(alias)
                        break
        elif action_type == "scale_row_counts":
            if "multiplier" not in normalized:
                for alias in ("scale", "factor", "multiple", "value"):
                    if alias in normalized:
                        normalized["multiplier"] = normalized.get(alias)
                        break
        elif action_type in {"set_min_rows", "set_max_rows"}:
            if "value" not in normalized:
                for alias in ("rows", "count", "row_count"):
                    if alias in normalized:
                        normalized["value"] = normalized.get(alias)
                        break

        return normalized

    def _normalize_repair_actions(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for action in actions:
            if not isinstance(action, dict):
                continue
            normalized.append(self._normalize_repair_action(action))
        return normalized

    def _build_compact_repair_context(
        self,
        tables: Dict[str, Dict],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
        table_row_counts: Dict[str, int],
        generation_order: List[str],
    ) -> Dict[str, Any]:
        include_tables: List[str] = []
        seen: Set[str] = set()

        def _push(table_name: str) -> None:
            if table_name in tables and table_name not in seen:
                include_tables.append(table_name)
                seen.add(table_name)

        for t_name in filter_values.keys():
            _push(t_name)
        for t_name in self._select_targeted_topup_tables(
            tables=tables,
            table_row_counts=table_row_counts,
            filter_values=filter_values,
            fk_relationships=fk_relationships,
            max_tables=4,
        ):
            _push(t_name)
        for t_name in generation_order:
            _push(t_name)
            if len(include_tables) >= 12:
                break
        if not include_tables:
            include_tables = list(tables.keys())[:12]
        include_tables = include_tables[:12]
        include_set = set(include_tables)

        table_columns = {
            t: list((tables.get(t, {}) or {}).get("columns", {}).keys())[:8]
            for t in include_tables
        }
        row_counts = {t: int(table_row_counts.get(t, 1000)) for t in include_tables}
        compact_filters: Dict[str, Dict[str, List[Any]]] = {}
        for t in include_tables:
            col_map = filter_values.get(t, {})
            if not isinstance(col_map, dict):
                continue
            compact_filters[t] = {
                str(col): (vals if isinstance(vals, list) else [vals])[:4]
                for col, vals in list(col_map.items())[:4]
            }

        compact_fks: Dict[str, Dict[str, Tuple[str, str]]] = {}
        for child_table, child_fks in fk_relationships.items():
            if child_table not in include_set:
                continue
            entries = {}
            for child_col, (parent_table, parent_col) in child_fks.items():
                if parent_table in include_set:
                    entries[child_col] = (parent_table, parent_col)
            if entries:
                compact_fks[child_table] = entries

        return {
            "tables": table_columns,
            "filters": compact_filters,
            "fks": compact_fks,
            "row_counts": row_counts,
            "gen_order": [t for t in generation_order if t in include_set][:12],
        }

    def _swarm_variants(self, attempt: int, phase: str = "coverage") -> List[Dict[str, Any]]:
        count = max(1, self.llm_swarm_size)
        min_t = float(self.llm_swarm_temperature_min)
        max_t = float(self.llm_swarm_temperature_max)
        span = max_t - min_t
        variants: List[Dict[str, Any]] = []
        phase_name = str(phase or "coverage").strip().lower()
        strategies = (
            self._ADVERSARIAL_SWARM_STRATEGIES
            if phase_name.startswith("adversarial")
            else self._COVERAGE_SWARM_STRATEGIES
        )
        n_profiles = max(1, len(strategies))
        for idx in range(count):
            profile_idx = (attempt - 1 + idx) % n_profiles
            strategy_id, strategy_focus = strategies[profile_idx]
            if count <= 1:
                temp = min_t
            else:
                temp = min_t + span * (idx / float(count - 1))
            variants.append({
                "index": idx,
                "strategy_id": strategy_id,
                "strategy_focus": strategy_focus,
                "temperature": round(temp, 3),
            })
        return variants

    def _build_repair_prompt(
        self,
        *,
        sql: str,
        last_error: Optional[str],
        actual_rows: int,
        min_rows: int,
        max_rows: int,
        attempt: int,
        variant: Dict[str, Any],
        phase: str,
        compact_context: Dict[str, Any],
        repair_log_context: List[Dict[str, Any]],
    ) -> str:
        allowed_types = (
            "set_row_count|scale_row_counts|set_max_rows"
            if self.repair_mode == "add_only"
            else "set_filter_values|remove_filter|add_fk|set_row_count|scale_row_counts|set_generation_order|set_min_rows|set_max_rows"
        )
        add_only_rule = (
            "- ADD-ONLY: only set_row_count/scale_row_counts/set_max_rows.\n"
            if self.repair_mode == "add_only"
            else ""
        )
        strategy_id = str(variant.get("strategy_id", "default"))
        phase_name = str(phase or "coverage").strip().lower()
        is_adversarial = phase_name.startswith("adversarial") or strategy_id.startswith("adversarial")
        role_line = (
            "ROLE=adversarial: add rows likely to expose semantic differences "
            "(boundary values, rare join-key combos, skewed aggregates) while staying valid.\n"
            if is_adversarial
            else "ROLE=coverage: add rows to maximize query hit-rate through selective predicates and key joins.\n"
        )
        example_line = (
            "EXAMPLE={\"actions\":[{\"type\":\"set_row_count\",\"table\":\"store_returns\",\"row_count\":26000},"
            "{\"type\":\"set_row_count\",\"table\":\"web_returns\",\"row_count\":24000}],"
            "\"note\":\"Adversarial: add edge-case overlap rows to create rows present in one result and absent in the other.\"}\n"
            if is_adversarial
            else "EXAMPLE={\"actions\":[{\"type\":\"set_row_count\",\"table\":\"store_returns\",\"row_count\":22000},"
            "{\"type\":\"set_row_count\",\"table\":\"date_dim\",\"row_count\":8000}],"
            "\"note\":\"Coverage: top up selective fact + filter dimension to raise final row count (e.g., 2 -> 100).\"}\n"
        )
        sql_compact = re.sub(r"\s+", " ", sql).strip()
        if len(sql_compact) > 2200:
            sql_compact = sql_compact[:2200] + " ..."
        compact_json = json.dumps(compact_context, separators=(",", ":"))
        repair_log_json = json.dumps(repair_log_context, separators=(",", ":"))
        return (
            "You are a SQL synthetic-data repair planner.\n"
            "Output ONLY JSON object with keys: actions (array), note (string).\n"
            "No markdown. No extra text.\n"
            f"Allowed action types: {allowed_types}\n"
            "Rules:\n"
            "- Use 1-3 actions max.\n"
            "- Do not invent tables/columns.\n"
            "- Prefer targeted set_row_count on FILTER tables and one-hop FK neighbors.\n"
            "- Change at most 2 tables via set_row_count.\n"
            "- For set_row_count use ~1.5x-2.5x from current; keep between 2000 and 60000.\n"
            "- Avoid global scale_row_counts unless no targeted option exists.\n"
            f"{role_line}"
            f"{example_line}"
            f"{add_only_rule}"
            f"ATTEMPT={attempt}\n"
            f"SWARM_STRATEGY={strategy_id}\n"
            f"SWARM_FOCUS={variant.get('strategy_focus','')}\n"
            f"ROW_STATUS=actual:{actual_rows},min:{min_rows},max:{max_rows}\n"
            f"LAST_ERROR={last_error or ''}\n"
            f"REPAIR_LOG={repair_log_json}\n"
            f"CONTEXT={compact_json}\n"
            f"SQL={sql_compact}\n"
        )

    def _request_llm_repair_plan(
        self,
        sql: str,
        last_error: Optional[str],
        actual_rows: int,
        min_rows: int,
        max_rows: int,
        tables: Dict[str, Dict],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
        table_row_counts: Dict[str, int],
        generation_order: List[str],
        attempt: int,
        repair_log: Optional[List[Dict[str, Any]]] = None,
        phase: str = "coverage",
    ) -> Optional[Dict[str, Any]]:
        _ = (
            sql,
            last_error,
            actual_rows,
            min_rows,
            max_rows,
            tables,
            fk_relationships,
            filter_values,
            table_row_counts,
            generation_order,
            attempt,
            repair_log,
            phase,
        )
        return None

    def _filter_actions_for_mode(self, actions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if self.repair_mode != "add_only":
            return actions
        allowed = {"set_row_count", "scale_row_counts", "set_max_rows"}
        filtered: List[Dict[str, Any]] = []
        for action in actions:
            if not isinstance(action, dict):
                continue
            action_type = str(action.get("type", "")).strip()
            if action_type in allowed:
                filtered.append(action)
        return filtered

    def _apply_llm_repair_actions(
        self,
        actions: List[Dict[str, Any]],
        tables: Dict[str, Dict],
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
        table_row_counts: Dict[str, int],
        generation_order: List[str],
        min_rows: int,
        max_rows: int,
    ) -> Tuple[bool, List[str], int, int, List[str]]:
        """Apply safe deterministic mutations from a repair plan."""
        changed = False
        notes: List[str] = []
        current_order = list(generation_order)

        for action in actions:
            if not isinstance(action, dict):
                continue
            action_type = str(action.get("type", "")).strip()
            if action_type not in self._REPAIR_ACTION_TYPES:
                continue

            if action_type == "set_filter_values":
                table = action.get("table")
                col = action.get("column")
                vals = action.get("values", [])
                if table not in tables:
                    continue
                real_col = self._resolve_column_name(table, str(col), tables) if col else None
                if not real_col:
                    continue
                if not isinstance(vals, list):
                    vals = [vals]
                clean_vals = [v for v in vals if v is not None]
                if not clean_vals:
                    continue
                filter_values.setdefault(table, {})[real_col] = clean_vals
                changed = True
                notes.append(f"set_filter_values({table}.{real_col})")
                continue

            if action_type == "remove_filter":
                table = action.get("table")
                col = action.get("column")
                if table not in filter_values:
                    continue
                real_col = self._resolve_column_name(table, str(col), tables) if col else None
                if not real_col or real_col not in filter_values.get(table, {}):
                    continue
                del filter_values[table][real_col]
                changed = True
                notes.append(f"remove_filter({table}.{real_col})")
                continue

            if action_type == "add_fk":
                child_table = action.get("child_table")
                child_col = action.get("child_column")
                parent_table = action.get("parent_table")
                parent_col = action.get("parent_column")
                if child_table not in tables or parent_table not in tables:
                    continue
                real_child_col = self._resolve_column_name(child_table, str(child_col), tables) if child_col else None
                real_parent_col = self._resolve_column_name(parent_table, str(parent_col), tables) if parent_col else None
                if not real_child_col or not real_parent_col:
                    continue
                fk_relationships.setdefault(child_table, {})[real_child_col.lower()] = (parent_table, real_parent_col)
                changed = True
                notes.append(f"add_fk({child_table}.{real_child_col}->{parent_table}.{real_parent_col})")
                continue

            if action_type == "set_row_count":
                table = action.get("table")
                row_count = action.get("row_count")
                if table not in tables:
                    continue
                try:
                    n = int(row_count)
                except (TypeError, ValueError):
                    continue
                n = max(10, min(200000, n))
                table_row_counts[table] = n
                changed = True
                notes.append(f"set_row_count({table}={n})")
                continue

            if action_type == "scale_row_counts":
                try:
                    mult = float(action.get("multiplier"))
                except (TypeError, ValueError):
                    continue
                mult = max(0.5, min(10.0, mult))
                for table_name in table_row_counts:
                    table_row_counts[table_name] = max(
                        10, min(200000, int(table_row_counts[table_name] * mult))
                    )
                changed = True
                notes.append(f"scale_row_counts(x{mult:.2f})")
                continue

            if action_type == "set_generation_order":
                order = action.get("order")
                if not isinstance(order, list):
                    continue
                normalized = [str(t) for t in order if str(t) in tables]
                if len(normalized) != len(tables):
                    continue
                if set(normalized) != set(tables.keys()):
                    continue
                current_order = normalized
                changed = True
                notes.append("set_generation_order")
                continue

            if action_type == "set_min_rows":
                try:
                    min_rows = max(0, int(action.get("value")))
                except (TypeError, ValueError):
                    continue
                if max_rows < min_rows:
                    max_rows = min_rows
                changed = True
                notes.append(f"set_min_rows({min_rows})")
                continue

            if action_type == "set_max_rows":
                try:
                    max_rows = max(0, int(action.get("value")))
                except (TypeError, ValueError):
                    continue
                if max_rows < min_rows:
                    min_rows = max_rows
                changed = True
                notes.append(f"set_max_rows({max_rows})")
                continue

        return changed, notes, min_rows, max_rows, current_order

    def _requires_reference_schema_for_star(self, sql: str) -> bool:
        """Return True when SELECT * cannot be validated safely in pure AST mode."""
        if self.schema_extractor is not None:
            return False
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return False
        if not any(True for _ in parsed.find_all(exp.Star)):
            return False
        try:
            tables = SchemaExtractor(sql).extract_tables()
        except Exception:
            return False
        return any(not info.get('columns') for info in tables.values())

    def validate(self, sql_file: str, target_rows: int = 1000, min_rows: int = None, max_rows: int = None) -> Dict[str, Any]:
        """Run full validation pipeline."""

        # 1. Read SQL — handle multi-statement files (take first statement)
        with open(sql_file, 'r') as f:
            raw_sql = f.read()

        # Strip comments and split on ';' to handle multi-statement files
        statements = [s.strip() for s in raw_sql.split(';') if s.strip()]
        # Filter out comment-only segments
        statements = [s for s in statements if not all(
            line.strip().startswith('--') or not line.strip()
            for line in s.split('\n')
        )]
        sql = statements[0] if statements else raw_sql
        if len(statements) > 1:
            logger.debug("Multi-statement file (%d statements), validating first", len(statements))

        # Handle row range
        if min_rows is None:
            min_rows = 0  # Allow 0 rows for complex filter queries
        if max_rows is None:
            max_rows = target_rows * 20  # More lenient upper bound

        logger.debug("Input SQL file: %s", sql_file)
        logger.debug("Target output rows: %d (range: %d-%d)", target_rows, min_rows, max_rows)
        coverage_started_at = time.monotonic()
        coverage_deadline = (
            coverage_started_at + float(self.coverage_time_budget_s)
            if self.coverage_time_budget_s > 0
            else None
        )
        self._emit_progress(
            f"start file={sql_file} target={target_rows} range={min_rows}-{max_rows} "
            f"coverage_budget_s={self.coverage_time_budget_s}"
        )

        # 1b. Transpile to DuckDB if source dialect differs
        if self.dialect != 'duckdb':
            logger.debug("Transpiling from %s to duckdb...", self.dialect)
            try:
                transpiled = sqlglot.transpile(sql, read=self.dialect, write='duckdb')
                sql = '\n'.join(transpiled)
                logger.debug("Transpiled OK (%d chars)", len(sql))
            except Exception as e:
                return {
                    'success': False, 'error': f'Transpile failed: {e}',
                    'tables_created': [], 'actual_rows': 0,
                    'min_rows': min_rows, 'max_rows': max_rows,
                }

        # 1c. Resolve ambiguous column references (ORDER BY without table qualifier)
        sql = self._resolve_ambiguous_columns(sql)

        # 2. Extract schema using SQLGlot (always parse as duckdb after transpile)
        logger.debug("Extracting schema with SQLGlot AST...")
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        logger.debug("Found %d tables: %s", len(tables), list(tables.keys()))

        for t_name, t_info in tables.items():
            logger.debug("  %s: %s", t_name, list(t_info['columns'].keys()))

        # 3. Override schema from reference DB if available
        #    Reference DB is authoritative for column types — AST extraction
        #    only discovers which tables are used in the query.
        logger.debug("Creating schema in DuckDB...")
        if self.schema_extractor:
            logger.debug("Loading real schemas from: %s", self.reference_db)
            for table_name in list(tables.keys()):
                ref_schema = self.schema_extractor.get_table_schema(table_name)
                if ref_schema:
                    # In reference-schema mode, trust database metadata as
                    # authoritative to avoid AST heuristic false positives
                    # creating ambiguous synthetic schemas.
                    tables[table_name]['columns'] = dict(ref_schema)
                    logger.debug("  %s: %d cols from reference DB", table_name, len(ref_schema))
        
        # 4. Detect FK relationships from JOIN conditions + heuristics, create indexes
        fk_relationships = self._detect_fk_from_joins(sql, tables)
        fk_heuristic = self._detect_foreign_keys(sql, tables)
        # Merge: join-based FKs take priority over heuristic
        for table_name, fks in fk_heuristic.items():
            if table_name not in fk_relationships:
                fk_relationships[table_name] = {}
            for col, target in fks.items():
                if col not in fk_relationships[table_name]:
                    fk_relationships[table_name][col] = target

        # 5. Extract filter values from WHERE clause for data generation
        filter_values = self._extract_filter_values(sql, tables)
        join_graph = self._build_join_column_graph(sql, tables)
        filter_values = self._propagate_filter_values_across_joins(filter_values, join_graph, tables)

        # 6. Generate synthetic data
        logger.debug("Generating synthetic data...")
        # Estimate rows needed per table based on query complexity
        table_row_counts = self._estimate_row_counts(sql, tables, target_rows)
        generation_order = self._get_table_generation_order(tables, fk_relationships)

        logger.debug("Generation order: %s", generation_order)
        logger.debug("FK relationships: %s", fk_relationships)
        scalar_uniques = self._detect_scalar_subquery_uniques(sql, tables)

        def _populate_synthetic_data(row_multiplier: int = 1):
            # Reset schema/data for each synthesis attempt.
            self._create_schema(tables)
            self._create_indexes(tables, sql)

            generator = SyntheticDataGenerator(self.conn, all_schemas=tables)
            generator.filter_literal_values = filter_values

            for table_name in generation_order:
                schema = tables[table_name]
                base_count = table_row_counts.get(table_name, 1000)
                row_count = max(10, min(base_count * row_multiplier, 200000))
                table_fks = fk_relationships.get(table_name, {})
                table_kind = "fact" if table_fks else "dimension"
                logger.debug("  %s: %d rows (%s, x%d), FKs: %s", table_name, row_count, table_kind, row_multiplier, table_fks)
                generator.generate_table_data(table_name, schema, row_count, foreign_keys=table_fks)
                # Narrow PK candidates on each generated table using direct predicates.
                self._update_filter_matched_pks(generator, tables, [table_name], filter_values)
                # Reverse-walk: filtered child rows constrain parent key domain.
                self._reverse_propagate_parent_key_matches(
                    generator, table_name, tables, fk_relationships, filter_values
                )

            logger.debug("Available FK values: %s", list(generator.foreign_key_values.keys()))

            # Deduplicate columns used by scalar subqueries.
            if scalar_uniques:
                logger.debug("Enforcing scalar subquery uniqueness...")
                for table_name, col_set in scalar_uniques:
                    cols_csv = ', '.join(col_set)
                    try:
                        self.conn.execute(f"""
                            CREATE TABLE {table_name}__dedup AS
                            SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER (
                                    PARTITION BY {cols_csv} ORDER BY 1
                                ) AS __rn FROM {table_name}
                            ) WHERE __rn = 1
                        """)
                        self.conn.execute(f"DROP TABLE {table_name}")
                        self.conn.execute(f"ALTER TABLE {table_name}__dedup RENAME TO {table_name}")
                        self.conn.execute(f"ALTER TABLE {table_name} DROP COLUMN __rn")
                        remaining = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                        logger.debug("  %s: deduped on (%s), %d rows remain", table_name, cols_csv, remaining)
                    except Exception as e:
                        logger.debug("  %s: dedup failed: %s", table_name, e)

        def _run_with_current_context() -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
            _populate_synthetic_data(1)

            logger.debug("Running query...")
            exec_sql = sql
            best_success = None
            last_error = None

            # Adaptive retries: if query executes but remains below threshold,
            # repopulate with more synthetic data to increase hit probability.
            for attempt_idx, multiplier in enumerate([1, 3, 10]):
                if attempt_idx > 0 and min_rows > 0 and best_success and best_success.get('actual_rows', 0) < min_rows:
                    logger.debug("Repopulating synthetic data with row multiplier x%d", multiplier)
                    _populate_synthetic_data(multiplier)

                for _ in range(4):
                    try:
                        result = self.conn.execute(exec_sql).fetchall()
                        actual_rows = len(result)
                        in_range = min_rows <= actual_rows <= max_rows
                        logger.debug("Query returned %d rows (target range: %d-%d)", actual_rows, min_rows, max_rows)

                        columns = [desc[0] for desc in self.conn.description] if self.conn.description else []
                        payload = {
                            'success': True,
                            'target_rows': target_rows,
                            'min_rows': min_rows,
                            'max_rows': max_rows,
                            'actual_rows': actual_rows,
                            'in_range': in_range,
                            'columns': columns,
                            'sample_results': result[:10] if result else [],
                            'tables_created': list(tables.keys())
                        }

                        if in_range or min_rows == 0:
                            return payload, None

                        # Success but not in range: try a larger synth set.
                        best_success = payload
                        break
                    except Exception as e:
                        last_error = str(e)
                        if "Ambiguous reference to column name" in last_error:
                            fixed_sql = self._resolve_ambiguous_from_error(exec_sql, last_error)
                            if fixed_sql != exec_sql:
                                exec_sql = fixed_sql
                                continue
                        break

            if best_success:
                return best_success, last_error
            return None, last_error

        llm_repairs = []
        current_attempt = 0
        best_payload: Optional[Dict[str, Any]] = None
        avoid_table_pairs: Set[Tuple[str, ...]] = set()
        table_penalties: Dict[str, float] = {}

        def _capture_best_payload(candidate: Optional[Dict[str, Any]]) -> None:
            nonlocal best_payload
            if candidate is None:
                return
            if not candidate.get('success', False):
                return
            rows = int(candidate.get('actual_rows', 0))
            if best_payload is None or rows > int(best_payload.get('actual_rows', 0)):
                best_payload = dict(candidate)

        def _prefer_best_payload(current_payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
            if best_payload is None:
                return current_payload
            if current_payload is None:
                chosen = dict(best_payload)
            else:
                cur_rows = int(current_payload.get('actual_rows', 0))
                best_rows = int(best_payload.get('actual_rows', 0))
                chosen = dict(best_payload) if best_rows > cur_rows else current_payload
            if chosen is not None:
                chosen['in_range'] = min_rows <= int(chosen.get('actual_rows', 0)) <= max_rows
            return chosen

        def _attach_coverage_meta(payload_obj: Optional[Dict[str, Any]], *, timeout: bool) -> Optional[Dict[str, Any]]:
            if payload_obj is None:
                return None
            payload_obj['coverage_timeout'] = bool(timeout)
            payload_obj['coverage_elapsed_s'] = round(max(0.0, time.monotonic() - coverage_started_at), 3)
            return payload_obj

        while True:
            payload, last_error = _run_with_current_context()
            _capture_best_payload(payload)
            actual_rows_now = payload.get('actual_rows', 0) if payload else 0
            elapsed_s = max(0.0, time.monotonic() - coverage_started_at)
            remaining_s = (
                max(0.0, coverage_deadline - time.monotonic())
                if coverage_deadline is not None
                else None
            )
            self._emit_progress(
                f"round={current_attempt} rows={actual_rows_now} in_range={payload.get('in_range', False) if payload else False} "
                f"error={'none' if not last_error else str(last_error)[:120]} "
                f"elapsed_s={elapsed_s:.1f}"
                + (f" remaining_s={remaining_s:.1f}" if remaining_s is not None else "")
            )

            # Close out previous repair attempt with observed row count after re-run.
            if llm_repairs and llm_repairs[-1].get('rows_after') is None:
                llm_repairs[-1]['rows_after'] = actual_rows_now
                llm_repairs[-1]['in_range_after'] = payload.get('in_range', False) if payload else None
                before = llm_repairs[-1].get('rows_before')
                after = llm_repairs[-1].get('rows_after')
                if isinstance(before, int) and isinstance(after, int):
                    llm_repairs[-1]['row_delta'] = after - before
                    llm_repairs[-1]['regressed'] = after < before
                    set_row_tables = []
                    for action in llm_repairs[-1].get('plan_actions', []) or []:
                        if not isinstance(action, dict):
                            continue
                        if str(action.get('type', '')) != 'set_row_count':
                            continue
                        table_name = str(action.get('table', ''))
                        if table_name:
                            set_row_tables.append(table_name)
                    if not set_row_tables:
                        for note in llm_repairs[-1].get('actions', []) or []:
                            match = re.match(r"set_row_count\(([^=]+)=", str(note))
                            if match:
                                set_row_tables.append(match.group(1))
                    unique_tables = sorted(set(set_row_tables))
                    if len(unique_tables) >= 2 and after <= before:
                        avoid_table_pairs.add(tuple(unique_tables))
                    for tbl in unique_tables:
                        if after < before:
                            table_penalties[tbl] = float(table_penalties.get(tbl, 0.0)) + 2.0
                        elif after == before:
                            table_penalties[tbl] = float(table_penalties.get(tbl, 0.0)) + 1.0
                        else:
                            table_penalties[tbl] = max(
                                0.0,
                                float(table_penalties.get(tbl, 0.0)) - 1.0,
                            )
                    self._emit_progress(
                        f"attempt={llm_repairs[-1].get('attempt')} delta={llm_repairs[-1].get('row_delta')} "
                        f"rows={before}->{after}"
                    )

            needs_repair = (payload is None) or (not payload.get('in_range', True))

            if not needs_repair:
                if payload is not None:
                    payload['llm_repair_attempts'] = current_attempt
                    payload['llm_repair_history'] = llm_repairs
                    return _attach_coverage_meta(payload, timeout=False)
                break

            if coverage_deadline is not None and time.monotonic() >= coverage_deadline:
                self._emit_progress(
                    f"coverage_timeout reached after {elapsed_s:.1f}s; returning best observed payload"
                )
                chosen_payload = _prefer_best_payload(payload)
                if chosen_payload is not None:
                    chosen_payload['llm_repair_attempts'] = current_attempt
                    chosen_payload['llm_repair_history'] = llm_repairs
                    return _attach_coverage_meta(chosen_payload, timeout=True)
                return {
                    'success': False,
                    'error': last_error or 'Coverage timeout before any successful execution',
                    'tables_created': list(tables.keys()),
                    'llm_repair_attempts': current_attempt,
                    'llm_repair_history': llm_repairs,
                    'coverage_timeout': True,
                    'coverage_elapsed_s': round(elapsed_s, 3),
                }

            # Single-pass product mode: no repair retries.
            chosen_payload = _prefer_best_payload(payload)
            if chosen_payload is not None:
                chosen_payload['llm_repair_attempts'] = 0
                chosen_payload['llm_repair_history'] = []
                return _attach_coverage_meta(chosen_payload, timeout=False)
            logger.debug("Query failed: %s", last_error)
            return {
                'success': False,
                'error': last_error or 'Unknown query execution error',
                'tables_created': list(tables.keys()),
                'llm_repair_attempts': 0,
                'llm_repair_history': [],
                'coverage_timeout': False,
                'coverage_elapsed_s': round(max(0.0, time.monotonic() - coverage_started_at), 3),
            }

            plan = self._build_fallback_repair_plan(
                actual_rows=actual_rows_now,
                min_rows=min_rows,
                max_rows=max_rows,
                tables=tables,
                table_row_counts=table_row_counts,
                filter_values=filter_values,
                fk_relationships=fk_relationships,
                avoid_table_pairs=avoid_table_pairs,
                table_penalties=table_penalties,
                attempt=current_attempt + 1,
            )
            plan_source = "deterministic" if plan else "none"
            if not plan:
                chosen_payload = _prefer_best_payload(payload)
                if chosen_payload is not None:
                    chosen_payload['llm_repair_attempts'] = current_attempt
                    chosen_payload['llm_repair_history'] = llm_repairs
                    return _attach_coverage_meta(chosen_payload, timeout=False)
                logger.debug("Query failed without repair plan: %s", last_error)
                return {
                    'success': False,
                    'error': last_error or 'Unknown query execution error',
                    'tables_created': list(tables.keys()),
                    'llm_repair_attempts': current_attempt,
                    'llm_repair_history': llm_repairs,
                    'coverage_timeout': False,
                    'coverage_elapsed_s': round(max(0.0, time.monotonic() - coverage_started_at), 3),
                }

            plan_actions = self._filter_actions_for_mode(plan.get('actions', []))
            self._emit_progress(
                f"plan source={plan_source} actions={json.dumps(plan_actions)[:220]}"
            )
            changed, notes, min_rows, max_rows, generation_order = self._apply_llm_repair_actions(
                actions=plan_actions,
                tables=tables,
                fk_relationships=fk_relationships,
                filter_values=filter_values,
                table_row_counts=table_row_counts,
                generation_order=generation_order,
                min_rows=min_rows,
                max_rows=max_rows,
            )
            if not changed:
                fallback_plan = self._build_fallback_repair_plan(
                    actual_rows=actual_rows_now,
                    min_rows=min_rows,
                    max_rows=max_rows,
                    tables=tables,
                    table_row_counts=table_row_counts,
                    filter_values=filter_values,
                    fk_relationships=fk_relationships,
                    avoid_table_pairs=avoid_table_pairs,
                    table_penalties=table_penalties,
                    attempt=current_attempt + 1,
                )
                if fallback_plan:
                    plan = fallback_plan
                    plan_source = "deterministic"
                    plan_actions = self._filter_actions_for_mode(plan.get('actions', []))
                    changed, notes, min_rows, max_rows, generation_order = self._apply_llm_repair_actions(
                        actions=plan_actions,
                        tables=tables,
                        fk_relationships=fk_relationships,
                        filter_values=filter_values,
                        table_row_counts=table_row_counts,
                        generation_order=generation_order,
                        min_rows=min_rows,
                        max_rows=max_rows,
                    )
            if not changed:
                chosen_payload = _prefer_best_payload(payload)
                if chosen_payload is not None:
                    chosen_payload['llm_repair_attempts'] = current_attempt
                    chosen_payload['llm_repair_history'] = llm_repairs
                    return _attach_coverage_meta(chosen_payload, timeout=False)
                logger.debug("Repair plan made no changes")
                return {
                    'success': False,
                    'error': last_error or 'Unknown query execution error',
                    'tables_created': list(tables.keys()),
                    'llm_repair_attempts': current_attempt,
                    'llm_repair_history': llm_repairs,
                    'coverage_timeout': False,
                    'coverage_elapsed_s': round(max(0.0, time.monotonic() - coverage_started_at), 3),
                }

            if "set_generation_order" not in notes:
                generation_order = self._get_table_generation_order(tables, fk_relationships)
            current_attempt += 1
            llm_repairs.append({
                'attempt': current_attempt,
                'source': plan_source,
                'note': plan.get('note', ''),
                'actions': notes,
                'plan_actions': [a for a in plan_actions if isinstance(a, dict)],
                'rows_before': actual_rows_now,
                'rows_after': None,
                'in_range_after': None,
                'last_error': last_error,
                'swarm': plan.get('_swarm'),
            })
    
    def validate_sql_pair(
        self,
        original_sql: str,
        optimized_sql: str,
        target_rows: int = 100,
    ) -> Dict[str, Any]:
        """Validate that optimized SQL produces same results as original.

        Uses synthetic data — no real database execution needed beyond
        schema introspection. Each call creates a fresh in-memory DuckDB
        connection so results don't leak between validations.

        Args:
            original_sql: Original query SQL string.
            optimized_sql: Optimized query SQL string.
            target_rows: Number of synthetic rows per table (default 100).

        Returns:
            Dict with keys:
                match: bool — True if results are equivalent
                orig_success: bool — original query executed
                opt_success: bool — optimized query executed
                orig_rows: int — row count from original
                opt_rows: int — row count from optimized
                orig_error: str|None — error from original
                opt_error: str|None — error from optimized
                row_count_match: bool — row counts equal
                reason: str — human-readable explanation
        """
        # Fresh connection per validation to avoid state leakage.
        self.conn = duckdb.connect(':memory:')

        # 1) Transpile both queries to DuckDB if needed.
        orig_exec = original_sql
        opt_exec = optimized_sql
        if self.dialect != 'duckdb':
            try:
                orig_exec = '\n'.join(sqlglot.transpile(original_sql, read=self.dialect, write='duckdb'))
                opt_exec = '\n'.join(sqlglot.transpile(optimized_sql, read=self.dialect, write='duckdb'))
            except Exception as e:
                return {
                    'match': False,
                    'orig_success': False,
                    'opt_success': False,
                    'orig_rows': 0,
                    'opt_rows': 0,
                    'orig_error': f'Transpile failed: {e}',
                    'opt_error': f'Transpile failed: {e}',
                    'row_count_match': False,
                    'reason': f'Transpile failed: {e}',
                }

        orig_exec = self._resolve_ambiguous_columns(orig_exec)
        opt_exec = self._resolve_ambiguous_columns(opt_exec)

        # Pure AST mode cannot safely validate SELECT * without concrete schema.
        if self._requires_reference_schema_for_star(orig_exec) or self._requires_reference_schema_for_star(opt_exec):
            msg = "Low-confidence schema: SELECT * requires reference DB schema in synthetic mode"
            return {
                'match': False,
                'orig_success': False,
                'opt_success': False,
                'orig_rows': 0,
                'opt_rows': 0,
                'orig_error': msg,
                'opt_error': msg,
                'row_count_match': False,
                'reason': msg,
            }

        # 2) Extract schema from BOTH queries and merge.
        try:
            orig_tables = SchemaExtractor(orig_exec).extract_tables()
            opt_tables = SchemaExtractor(opt_exec).extract_tables()
        except Exception as e:
            return {
                'match': False,
                'orig_success': False,
                'opt_success': False,
                'orig_rows': 0,
                'opt_rows': 0,
                'orig_error': f'Schema extraction failed: {e}',
                'opt_error': f'Schema extraction failed: {e}',
                'row_count_match': False,
                'reason': f'Schema extraction failed: {e}',
            }

        merged_tables: Dict[str, Dict[str, Any]] = {}
        for source in (orig_tables, opt_tables):
            for table_name, table_info in source.items():
                if table_name not in merged_tables:
                    merged_tables[table_name] = {
                        'columns': dict(table_info.get('columns', {})),
                        'alias': table_info.get('alias'),
                        'key': table_info.get('key', f'{table_name}_sk'),
                    }
                    continue
                if not merged_tables[table_name].get('alias') and table_info.get('alias'):
                    merged_tables[table_name]['alias'] = table_info.get('alias')
                for col_name, col_info in table_info.get('columns', {}).items():
                    merged_tables[table_name]['columns'].setdefault(col_name, dict(col_info))

        # 3) Apply authoritative reference schema when available.
        if self.schema_extractor:
            for table_name in list(merged_tables.keys()):
                ref_schema = self.schema_extractor.get_table_schema(table_name)
                if ref_schema:
                    merged_tables[table_name]['columns'] = dict(ref_schema)

        # 4) Detect FK relationships from both query shapes.
        fk_relationships = self._detect_fk_from_joins(orig_exec, merged_tables)
        fk_sources = [
            self._detect_fk_from_joins(opt_exec, merged_tables),
            self._detect_foreign_keys(orig_exec, merged_tables),
            self._detect_foreign_keys(opt_exec, merged_tables),
        ]
        for fk_map in fk_sources:
            for table_name, fks in fk_map.items():
                dst = fk_relationships.setdefault(table_name, {})
                for col, target in fks.items():
                    dst.setdefault(col, target)

        # 5) Merge filter literals from both queries and propagate over join graph.
        filter_values: Dict[str, Dict[str, list]] = {}
        for extracted in (
            self._extract_filter_values(orig_exec, merged_tables),
            self._extract_filter_values(opt_exec, merged_tables),
        ):
            for table_name, col_map in extracted.items():
                dst_cols = filter_values.setdefault(table_name, {})
                for col_name, vals in col_map.items():
                    dst_vals = dst_cols.setdefault(col_name, [])
                    self._append_unique(dst_vals, list(vals))

        join_graph = self._build_join_column_graph(orig_exec, merged_tables)
        opt_graph = self._build_join_column_graph(opt_exec, merged_tables)
        for node, neighbors in opt_graph.items():
            join_graph.setdefault(node, set()).update(neighbors)
        filter_values = self._propagate_filter_values_across_joins(filter_values, join_graph, merged_tables)

        # 6) Estimate rows and keep mutable generation state for coverage/adversarial phases.
        orig_counts = self._estimate_row_counts(orig_exec, merged_tables, target_rows)
        opt_counts = self._estimate_row_counts(opt_exec, merged_tables, target_rows)
        table_row_counts = {
            table_name: max(int(orig_counts.get(table_name, 1000)), int(opt_counts.get(table_name, 1000)))
            for table_name in merged_tables
        }
        generation_order = self._get_table_generation_order(merged_tables, fk_relationships)

        def _populate_pair_data() -> None:
            self._create_schema(merged_tables)
            self._create_indexes(merged_tables, orig_exec)
            self._create_indexes(merged_tables, opt_exec)

            generator = SyntheticDataGenerator(self.conn, all_schemas=merged_tables)
            generator.filter_literal_values = filter_values

            for table_name in generation_order:
                schema = merged_tables[table_name]
                row_count = max(10, min(200000, int(table_row_counts.get(table_name, 1000))))
                table_fks = fk_relationships.get(table_name, {})
                generator.generate_table_data(
                    table_name=table_name,
                    schema=schema,
                    row_count=row_count,
                    foreign_keys=table_fks,
                )
                self._update_filter_matched_pks(generator, merged_tables, [table_name], filter_values)
                self._reverse_propagate_parent_key_matches(
                    generator,
                    table_name,
                    merged_tables,
                    fk_relationships,
                    filter_values,
                )

        def _execute_pair_once() -> Dict[str, Any]:
            try:
                orig_rows_local = self.conn.execute(orig_exec).fetchall()
            except Exception as e:
                return {
                    "orig_success": False,
                    "opt_success": False,
                    "orig_rows": [],
                    "opt_rows": [],
                    "orig_error": str(e),
                    "opt_error": None,
                    "reason": f"Original query failed: {e}",
                }

            try:
                opt_rows_local = self.conn.execute(opt_exec).fetchall()
            except Exception as e:
                return {
                    "orig_success": True,
                    "opt_success": False,
                    "orig_rows": orig_rows_local,
                    "opt_rows": [],
                    "orig_error": None,
                    "opt_error": str(e),
                    "reason": f"Optimized query failed: {e}",
                }

            return {
                "orig_success": True,
                "opt_success": True,
                "orig_rows": orig_rows_local,
                "opt_rows": opt_rows_local,
                "orig_error": None,
                "opt_error": None,
                "reason": "executed",
            }

        def _compare_rows(orig_rows_local: List[Any], opt_rows_local: List[Any]) -> Tuple[bool, bool, str]:
            orig_count_local = len(orig_rows_local)
            opt_count_local = len(opt_rows_local)
            if orig_count_local != opt_count_local:
                return False, False, f"Row count mismatch: original {orig_count_local} vs optimized {opt_count_local}"

            def _result_hash(rows):
                sorted_rows = sorted(str(r) for r in rows)
                return hashlib.md5('\n'.join(sorted_rows).encode()).hexdigest()

            if _result_hash(orig_rows_local) == _result_hash(opt_rows_local):
                return True, True, "Results match (synthetic data)"

            sorted_orig = sorted(str(r) for r in orig_rows_local)
            sorted_opt = sorted(str(r) for r in opt_rows_local)
            first_diff = None
            for i, (a, b) in enumerate(zip(sorted_orig, sorted_opt)):
                if a != b:
                    first_diff = f"Row {i}: orig={a[:80]} vs opt={b[:80]}"
                    break
            return False, True, f"Value mismatch: {first_diff or 'unknown difference'}"

        # Coverage phase: initial synthesis/execution.
        _populate_pair_data()
        exec_result = _execute_pair_once()
        if not exec_result.get("orig_success", False):
            return {
                'match': False,
                'orig_success': False,
                'opt_success': False,
                'orig_rows': 0,
                'opt_rows': 0,
                'orig_error': exec_result.get("orig_error"),
                'opt_error': exec_result.get("opt_error"),
                'row_count_match': False,
                'reason': exec_result.get("reason", "Original query failed"),
            }
        if not exec_result.get("opt_success", False):
            return {
                'match': False,
                'orig_success': True,
                'opt_success': False,
                'orig_rows': len(exec_result.get("orig_rows", [])),
                'opt_rows': 0,
                'orig_error': exec_result.get("orig_error"),
                'opt_error': exec_result.get("opt_error"),
                'row_count_match': False,
                'reason': exec_result.get("reason", "Optimized query failed"),
            }

        orig_rows = exec_result["orig_rows"]
        opt_rows = exec_result["opt_rows"]
        match, row_count_match, reason = _compare_rows(orig_rows, opt_rows)
        base_result: Dict[str, Any] = {
            'match': match,
            'orig_success': True,
            'opt_success': True,
            'orig_rows': len(orig_rows),
            'opt_rows': len(opt_rows),
            'orig_error': None,
            'opt_error': None,
            'row_count_match': row_count_match,
            'reason': reason,
            'adversarial_attempts': 0,
            'adversarial_timeout': False,
            'adversarial_elapsed_s': 0.0,
            'adversarial_history': [],
        }
        if not match:
            return base_result

        return base_result

    @staticmethod
    def _build_alias_map(parsed: exp.Expression) -> Tuple[Dict[str, str], Set[str]]:
        """Map SQL aliases to base tables, excluding CTE names."""
        alias_map: Dict[str, str] = {}
        cte_names: Set[str] = set()
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                cte_names.add(cte.alias)
        for table in parsed.find_all(exp.Table):
            if table.name in cte_names:
                continue
            if table.alias:
                alias_map[table.alias] = table.name
            alias_map[table.name] = table.name
        return alias_map, cte_names

    @staticmethod
    def _is_numeric_col_type(col_type: str) -> bool:
        upper = (col_type or '').upper()
        return any(t in upper for t in ('INT', 'DECIMAL', 'NUMERIC', 'DOUBLE', 'FLOAT', 'REAL'))

    @staticmethod
    def _sql_literal(value: Any, col_type: str) -> str:
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, (int, float)):
            return str(value)

        txt = str(value)
        if SyntheticValidator._is_numeric_col_type(col_type):
            try:
                float(txt)
                return txt
            except (ValueError, TypeError):
                pass
        return "'" + txt.replace("'", "''") + "'"

    @staticmethod
    def _append_unique(target: List[Any], values: List[Any]) -> None:
        seen = set(target)
        for v in values:
            if v not in seen:
                target.append(v)
                seen.add(v)

    def _resolve_column_name(self, table_name: str, col_name: str, tables: Dict) -> Optional[str]:
        """Resolve case-insensitive column reference to schema-preserving name."""
        if table_name not in tables:
            return None
        for existing_col in tables[table_name].get('columns', {}):
            if existing_col.lower() == col_name.lower():
                return existing_col
        return None

    def _resolve_column_ref(
        self,
        col_expr: exp.Column,
        alias_map: Dict[str, str],
        tables: Dict,
        cte_lineage: Optional[Dict[str, Dict[str, Tuple[str, str]]]] = None,
    ) -> Optional[Tuple[str, str]]:
        table_name = alias_map.get(col_expr.table, col_expr.table)
        if cte_lineage and table_name in cte_lineage:
            mapped = cte_lineage[table_name].get(col_expr.name.lower())
            if mapped:
                return mapped
        if not table_name or table_name not in tables:
            table_name = get_table_for_column(col_expr.name, tables)
        if not table_name or table_name not in tables:
            return None
        real_col = self._resolve_column_name(table_name, col_expr.name, tables)
        if not real_col:
            return None
        return table_name, real_col

    def _build_join_column_graph(self, sql: str, tables: Dict) -> Dict[Tuple[str, str], Set[Tuple[str, str]]]:
        """Build an undirected graph of column equalities from JOIN/WHERE."""
        graph: Dict[Tuple[str, str], Set[Tuple[str, str]]] = {}
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return graph

        alias_map, _ = self._build_alias_map(parsed)
        cte_lineage: Dict[str, Dict[str, Tuple[str, str]]] = {}

        # Include CTE aliases so outer predicates on CTE outputs can resolve.
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                alias_map[cte.alias] = cte.alias
        for table in parsed.find_all(exp.Table):
            if table.alias:
                alias_map[table.alias] = table.name
            alias_map[table.name] = table.name

        def _iter_select_nodes(node: Optional[exp.Expression]) -> List[exp.Select]:
            if node is None:
                return []
            if isinstance(node, exp.Select):
                return [node]
            if isinstance(node, exp.Subquery):
                return _iter_select_nodes(node.this)
            if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
                return _iter_select_nodes(node.left) + _iter_select_nodes(node.right)
            found = node.find(exp.Select)
            return [found] if found is not None else []

        # Build direct CTE output-column lineage to base columns.
        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            if not cte_name:
                continue
            out_map: Dict[str, Tuple[str, str]] = {}
            for select in _iter_select_nodes(cte.this):
                local_alias: Dict[str, str] = {}
                for table in select.find_all(exp.Table):
                    if table.alias:
                        local_alias[table.alias] = table.name
                    local_alias[table.name] = table.name
                for sel in select.expressions:
                    out_col = getattr(sel, "alias_or_name", None)
                    if not out_col:
                        continue
                    source_expr = sel.this if isinstance(sel, exp.Alias) else sel
                    if not isinstance(source_expr, exp.Column):
                        continue
                    source_table = local_alias.get(source_expr.table, source_expr.table)
                    mapped: Optional[Tuple[str, str]] = None
                    if source_table in tables:
                        source_col = self._resolve_column_name(source_table, source_expr.name, tables)
                        if source_col:
                            mapped = (source_table, source_col)
                    elif source_table in cte_lineage:
                        mapped = cte_lineage[source_table].get(source_expr.name.lower())
                    else:
                        guessed_table = get_table_for_column(source_expr.name, tables)
                        if guessed_table and guessed_table in tables:
                            source_col = self._resolve_column_name(guessed_table, source_expr.name, tables)
                            if source_col:
                                mapped = (guessed_table, source_col)
                    if mapped and out_col.lower() not in out_map:
                        out_map[out_col.lower()] = mapped
            if out_map:
                cte_lineage[cte_name] = out_map

        eq_sources: List[exp.EQ] = []
        for join in parsed.find_all(exp.Join):
            eq_sources.extend(join.find_all(exp.EQ))
        for where in parsed.find_all(exp.Where):
            for eq in where.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                    eq_sources.append(eq)

        for eq in eq_sources:
            if not (isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column)):
                continue
            left_ref = self._resolve_column_ref(eq.left, alias_map, tables, cte_lineage)
            right_ref = self._resolve_column_ref(eq.right, alias_map, tables, cte_lineage)
            if not left_ref or not right_ref or left_ref == right_ref:
                continue
            graph.setdefault(left_ref, set()).add(right_ref)
            graph.setdefault(right_ref, set()).add(left_ref)

        return graph

    def _propagate_filter_values_across_joins(
        self,
        filter_values: Dict[str, Dict[str, list]],
        join_graph: Dict[Tuple[str, str], Set[Tuple[str, str]]],
        tables: Dict,
    ) -> Dict[str, Dict[str, list]]:
        """Propagate literal predicates across equality-joined columns."""
        propagated: Dict[str, Dict[str, list]] = {}
        # Normalize existing filters to canonical column names first.
        for table_name, col_map in filter_values.items():
            if table_name not in propagated:
                propagated[table_name] = {}
            for col_name, values in col_map.items():
                canonical_col = self._resolve_column_name(table_name, col_name, tables)
                if canonical_col is None:
                    canonical_col = col_name
                propagated[table_name].setdefault(canonical_col, [])
                self._append_unique(propagated[table_name][canonical_col], list(values))

        visited: Set[Tuple[str, str]] = set()
        for start in join_graph:
            if start in visited:
                continue
            stack = [start]
            component: List[Tuple[str, str]] = []
            while stack:
                node = stack.pop()
                if node in visited:
                    continue
                visited.add(node)
                component.append(node)
                for neighbor in join_graph.get(node, ()):
                    if neighbor not in visited:
                        stack.append(neighbor)

            merged_values: List[Any] = []
            for table_name, col_name in component:
                vals = propagated.get(table_name, {}).get(col_name, [])
                self._append_unique(merged_values, vals)
            if not merged_values:
                continue
            for table_name, col_name in component:
                propagated.setdefault(table_name, {}).setdefault(col_name, [])
                self._append_unique(propagated[table_name][col_name], merged_values)

        return propagated

    def _build_filter_conditions(
        self,
        table_name: str,
        tables: Dict,
        filter_values: Dict[str, Dict[str, list]],
    ) -> List[str]:
        """Build SQL WHERE conditions from extracted literal predicates."""
        table_filters = filter_values.get(table_name, {})
        conditions: List[str] = []

        for col, vals in table_filters.items():
            real_col = self._resolve_column_name(table_name, col, tables) or col
            if real_col not in tables.get(table_name, {}).get('columns', {}):
                continue
            col_type = tables[table_name]['columns'][real_col]['type']
            eq_vals = []
            for val in vals:
                if isinstance(val, str) and val.startswith('BETWEEN:'):
                    _, low, high = val.split(':', 2)
                    conditions.append(
                        f"{real_col} BETWEEN {self._sql_literal(low, col_type)} AND {self._sql_literal(high, col_type)}"
                    )
                elif isinstance(val, str) and ':' in val and val[0] in '><':
                    op, v = val.split(':', 1)
                    conditions.append(f"{real_col} {op} {self._sql_literal(v, col_type)}")
                else:
                    eq_vals.append(val)

            if len(eq_vals) == 1:
                conditions.append(f"{real_col} = {self._sql_literal(eq_vals[0], col_type)}")
            elif len(eq_vals) > 1:
                in_list = ', '.join(self._sql_literal(v, col_type) for v in eq_vals)
                conditions.append(f"{real_col} IN ({in_list})")

        return conditions

    def _reverse_propagate_parent_key_matches(
        self,
        generator,
        table_name: str,
        tables: Dict,
        fk_relationships: Dict[str, Dict[str, Tuple[str, str]]],
        filter_values: Dict[str, Dict[str, list]],
    ) -> None:
        """Reverse-walk predicates through FK joins to constrain parent keys."""
        table_fks = fk_relationships.get(table_name, {})
        if not table_fks or table_name not in filter_values:
            return

        conditions = self._build_filter_conditions(table_name, tables, filter_values)
        if not conditions:
            return
        where = ' AND '.join(conditions)

        for fk_col, (parent_table, _) in table_fks.items():
            if parent_table not in tables:
                continue
            real_fk_col = self._resolve_column_name(table_name, fk_col, tables)
            if not real_fk_col:
                continue
            try:
                rows = self.conn.execute(
                    f"SELECT DISTINCT {real_fk_col} FROM {table_name} WHERE {where}"
                ).fetchall()
            except Exception:
                continue

            matched_parent_keys = [r[0] for r in rows if r and r[0] is not None]
            if not matched_parent_keys:
                continue
            matched_parent_keys = list(dict.fromkeys(matched_parent_keys))

            existing = generator.filter_matched_values.get(parent_table, [])
            if existing:
                matched_set = set(matched_parent_keys)
                narrowed = [k for k in existing if k in matched_set]
            else:
                narrowed = matched_parent_keys

            if narrowed:
                generator.filter_matched_values[parent_table] = narrowed
                logger.debug(
                    "  reverse-propagated %d keys from %s.%s to %s",
                    len(narrowed), table_name, real_fk_col, parent_table
                )

    def _update_filter_matched_pks(self, generator, tables, dim_tables, filter_values):
        """Query generated data to find PKs matching literal WHERE predicates."""
        for table_name in dim_tables:
            if table_name not in filter_values:
                continue

            # Find the PK column
            pk_col = find_primary_key_column(
                table_name,
                list(tables[table_name]['columns'].keys()),
            )
            if not pk_col:
                continue

            conditions = self._build_filter_conditions(table_name, tables, filter_values)
            if not conditions:
                continue

            where = ' AND '.join(conditions)
            try:
                result = self.conn.execute(f"SELECT {pk_col} FROM {table_name} WHERE {where}").fetchall()
                matched_pks = [r[0] for r in result]
                if matched_pks:
                    generator.filter_matched_values[table_name] = matched_pks
                    logger.debug("  %s: %d PKs match query filters", table_name, len(matched_pks))
            except Exception as e:
                logger.debug("  %s: filter PK query failed: %s", table_name, e)

    def _create_schema(self, tables: Dict):
        """Create tables in DuckDB."""
        for table_name, schema in tables.items():
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            columns = schema['columns']
            if not columns:
                columns = {schema['key']: {'type': 'INTEGER', 'nullable': False}}
            
            col_defs = []
            for col_name, col_info in columns.items():
                nullable = '' if col_info.get('nullable') else 'NOT NULL'
                col_defs.append(f"{col_name} {col_info['type']} {nullable}")
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            self.conn.execute(create_sql)
            logger.debug("  Created: %s", table_name)
    
    def _create_indexes(self, tables: Dict, sql: str):
        """Create indexes on join columns for better query performance."""
        parsed = sqlglot.parse_one(sql)
        join_columns = {}

        def _is_key_col(col_name: str) -> bool:
            col_lower = col_name.lower()
            return (
                col_lower.endswith('_sk')
                or col_lower.endswith('_id')
                or col_lower == 'id'
                or col_lower.endswith('_order_number')
                or col_lower.endswith('_ticket_number')
                or col_lower in {'order_number', 'ticket_number'}
            )
        
        for join in parsed.find_all(exp.Join):
            for eq in join.find_all(exp.EQ):
                left = eq.left
                right = eq.right
                
                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    left_table = left.table
                    left_col = left.name
                    right_table = right.table
                    right_col = right.name
                    
                    if left_table in tables:
                        if left_table not in join_columns:
                            join_columns[left_table] = set()
                        join_columns[left_table].add(left_col)
                    
                    if right_table in tables:
                        if right_table not in join_columns:
                            join_columns[right_table] = set()
                        join_columns[right_table].add(right_col)
        
        for where in parsed.find_all(exp.Where):
            for eq in where.find_all(exp.EQ):
                left = eq.left
                right = eq.right

                if isinstance(left, exp.Column) and isinstance(right, exp.Column):
                    left_table = left.table or get_table_for_column(left.name, tables)
                    left_col = left.name
                    right_table = right.table or get_table_for_column(right.name, tables)
                    right_col = right.name

                    if _is_key_col(left_col) or _is_key_col(right_col):
                        if left_table and left_table in tables:
                            if left_table not in join_columns:
                                join_columns[left_table] = set()
                            join_columns[left_table].add(left_col)
                        if right_table and right_table in tables:
                            if right_table not in join_columns:
                                join_columns[right_table] = set()
                            join_columns[right_table].add(right_col)
        
        for table_name, columns in join_columns.items():
            for col_name in columns:
                try:
                    idx_name = f"idx_{table_name}_{col_name}"
                    self.conn.execute(f"CREATE INDEX {idx_name} ON {table_name}({col_name})")
                except Exception:
                    pass
    
    def _detect_fk_from_joins(self, sql: str, tables: Dict) -> Dict[str, Dict[str, Tuple[str, str]]]:
        """Extract exact FK relationships from JOIN and WHERE conditions."""
        fk_relationships = {}
        parsed = sqlglot.parse_one(sql)

        def _is_key_col(col_name: str) -> bool:
            col_lower = col_name.lower()
            return (
                col_lower.endswith('_sk')
                or col_lower.endswith('_id')
                or col_lower == 'id'
                or col_lower.endswith('_order_number')
                or col_lower.endswith('_ticket_number')
                or col_lower in {'order_number', 'ticket_number'}
            )

        # Build alias map
        alias_map = {}
        cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            cte_names.add(cte.alias)
        for table in parsed.find_all(exp.Table):
            if table.name not in cte_names:
                if table.alias:
                    alias_map[table.alias] = table.name
                alias_map[table.name] = table.name

        # Extract from JOIN ON conditions and WHERE col=col equalities
        eq_sources = []
        for join in parsed.find_all(exp.Join):
            eq_sources.extend(join.find_all(exp.EQ))
        for where in parsed.find_all(exp.Where):
            for eq in where.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                    eq_sources.append(eq)

        for eq in eq_sources:
            if not (isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column)):
                continue
            left_table = alias_map.get(eq.left.table, eq.left.table)
            right_table = alias_map.get(eq.right.table, eq.right.table)
            left_col = eq.left.name
            right_col = eq.right.name

            # Resolve missing table qualifiers using schema-aware heuristics
            if not left_table or left_table not in tables:
                left_table = get_table_for_column(left_col, tables)
            if not right_table or right_table not in tables:
                right_table = get_table_for_column(right_col, tables)

            if not left_table or not right_table:
                continue
            if left_table == right_table:
                continue
            if left_table not in tables or right_table not in tables:
                continue
            if not (_is_key_col(left_col) or _is_key_col(right_col)):
                continue

            left_pk = find_primary_key_column(left_table, list(tables[left_table]['columns'].keys()))
            right_pk = find_primary_key_column(right_table, list(tables[right_table]['columns'].keys()))
            left_is_pk = bool(left_pk and left_col.lower() == left_pk.lower())
            right_is_pk = bool(right_pk and right_col.lower() == right_pk.lower())

            # Strong signal: FK usually points to a parent PK.
            if left_is_pk and not right_is_pk:
                fk_relationships.setdefault(right_table, {})[right_col.lower()] = (left_table, left_col)
                continue
            if right_is_pk and not left_is_pk:
                fk_relationships.setdefault(left_table, {})[left_col.lower()] = (right_table, right_col)
                continue

            # Determine FK direction using key-column count heuristic.
            left_key_count = sum(1 for c in tables[left_table]['columns'] if _is_key_col(c))
            right_key_count = sum(1 for c in tables[right_table]['columns'] if _is_key_col(c))

            if left_key_count > right_key_count:
                # left table has more key-like cols -> likely fact table -> left col is FK
                if left_table not in fk_relationships:
                    fk_relationships[left_table] = {}
                fk_relationships[left_table][left_col.lower()] = (right_table, right_col)
            elif right_key_count > left_key_count:
                # right table has more key-like cols -> likely fact table -> right col is FK
                if right_table not in fk_relationships:
                    fk_relationships[right_table] = {}
                fk_relationships[right_table][right_col.lower()] = (left_table, left_col)
            else:
                # Equal _sk count: use schema/name inference to pick PK owner.
                left_guessed = get_table_for_column(left_col, tables)
                right_guessed = get_table_for_column(right_col, tables)
                if left_guessed == left_table and right_guessed != right_table:
                    # left column belongs to its table → PK, right is FK
                    if right_table not in fk_relationships:
                        fk_relationships[right_table] = {}
                    fk_relationships[right_table][right_col.lower()] = (left_table, left_col)
                else:
                    # default: longer column name is the FK (more prefixes)
                    if len(left_col) > len(right_col):
                        if left_table not in fk_relationships:
                            fk_relationships[left_table] = {}
                        fk_relationships[left_table][left_col.lower()] = (right_table, right_col)
                    else:
                        if right_table not in fk_relationships:
                            fk_relationships[right_table] = {}
                        fk_relationships[right_table][right_col.lower()] = (left_table, left_col)

        return fk_relationships

    def _extract_filter_values(self, sql: str, tables: Dict) -> Dict[str, Dict[str, list]]:
        """Extract literal filter values from WHERE clauses.

        Returns: {table_name: {column_name: [values]}}
        """
        filter_values: Dict[str, Dict[str, List[Any]]] = {}
        parsed = sqlglot.parse_one(sql)

        # Build alias map (including CTE references for outer predicates)
        alias_map = {}
        cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            cte_names.add(cte.alias)
        for table in parsed.find_all(exp.Table):
            if table.alias:
                alias_map[table.alias] = table.name
            alias_map[table.name] = table.name

        def _append_filter_value(table_name: str, col_name: str, value: Any) -> None:
            table_map = filter_values.setdefault(table_name, {})
            col_values = table_map.setdefault(col_name, [])
            if value not in col_values:
                col_values.append(value)

        def _format_const_number(v: float) -> str:
            if abs(v - round(v)) < 1e-9:
                return str(int(round(v)))
            text = f"{v:.12f}".rstrip('0').rstrip('.')
            return text if text else "0"

        def _extract_date_literal(node: Optional[exp.Expression]) -> Optional[datetime]:
            if node is None:
                return None
            if isinstance(node, exp.Paren):
                return _extract_date_literal(node.this)
            if isinstance(node, exp.Cast):
                target = node.args.get("to")
                target_txt = target.sql().upper() if target is not None else ""
                if "DATE" in target_txt:
                    return _extract_date_literal(node.this)
            if isinstance(node, exp.Literal):
                txt = str(node.this).strip("'\"")
                for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
                    try:
                        return datetime.strptime(txt, fmt)
                    except ValueError:
                        continue
            return None

        def _extract_interval_days(node: Optional[exp.Expression]) -> Optional[int]:
            if not isinstance(node, exp.Interval):
                return None
            unit = node.args.get("unit")
            unit_txt = unit.sql().upper() if unit is not None else "DAY"
            if "DAY" not in unit_txt:
                return None
            val_node = node.this
            if isinstance(val_node, exp.Literal):
                try:
                    return int(float(str(val_node.this).strip("'\"")))
                except (TypeError, ValueError):
                    return None
            return None

        def _extract_const_value(node: Optional[exp.Expression]) -> Optional[str]:
            if node is None:
                return None
            if isinstance(node, exp.Literal):
                return str(node.this)
            if isinstance(node, exp.Paren):
                return _extract_const_value(node.this)
            if isinstance(node, exp.Neg):
                inner = _extract_const_value(node.this)
                if inner is None:
                    return None
                try:
                    return _format_const_number(-float(inner))
                except (TypeError, ValueError):
                    return None
            if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
                # Date arithmetic constants: DATE +/- INTERVAL n DAY.
                if isinstance(node, (exp.Add, exp.Sub)):
                    left_date = _extract_date_literal(node.left)
                    right_date = _extract_date_literal(node.right)
                    left_days = _extract_interval_days(node.left)
                    right_days = _extract_interval_days(node.right)
                    if left_date is not None and right_days is not None:
                        out_date = left_date + timedelta(days=right_days if isinstance(node, exp.Add) else -right_days)
                        return out_date.strftime("%Y-%m-%d")
                    if right_date is not None and left_days is not None and isinstance(node, exp.Add):
                        out_date = right_date + timedelta(days=left_days)
                        return out_date.strftime("%Y-%m-%d")

                left = _extract_const_value(node.left)
                right = _extract_const_value(node.right)
                if left is None or right is None:
                    return None
                try:
                    l = float(left)
                    r = float(right)
                    if isinstance(node, exp.Add):
                        out = l + r
                    elif isinstance(node, exp.Sub):
                        out = l - r
                    elif isinstance(node, exp.Mul):
                        out = l * r
                    else:
                        if r == 0:
                            return None
                        out = l / r
                    return _format_const_number(out)
                except (TypeError, ValueError):
                    return None
            if isinstance(node, exp.Cast):
                return _extract_const_value(node.this)
            return None

        def _extract_const_float(node: Optional[exp.Expression]) -> Optional[float]:
            txt = _extract_const_value(node)
            if txt is None:
                return None
            try:
                return float(txt)
            except (TypeError, ValueError):
                return None

        def _iter_select_nodes(node: Optional[exp.Expression]) -> List[exp.Select]:
            if node is None:
                return []
            if isinstance(node, exp.Select):
                return [node]
            if isinstance(node, exp.Subquery):
                return _iter_select_nodes(node.this)
            if isinstance(node, (exp.Union, exp.Intersect, exp.Except)):
                return _iter_select_nodes(node.left) + _iter_select_nodes(node.right)
            found = node.find(exp.Select)
            return [found] if found is not None else []

        def _resolve_source_column(
            col_expr: exp.Column,
            local_alias: Dict[str, str],
        ) -> Optional[Tuple[str, str]]:
            src_table = local_alias.get(col_expr.table, col_expr.table)
            if not src_table or src_table not in tables:
                src_table = get_table_for_column(col_expr.name, tables)
            if not src_table or src_table not in tables:
                return None
            src_col = self._resolve_column_name(src_table, col_expr.name, tables) or col_expr.name
            return src_table, src_col

        def _extract_linear_terms(
            node: Optional[exp.Expression],
            local_alias: Dict[str, str],
            scale: float = 1.0,
        ) -> Optional[List[Tuple[str, str, float]]]:
            if node is None:
                return None
            if isinstance(node, exp.Paren):
                return _extract_linear_terms(node.this, local_alias, scale)
            if isinstance(node, exp.Cast):
                return _extract_linear_terms(node.this, local_alias, scale)
            if isinstance(node, exp.Neg):
                return _extract_linear_terms(node.this, local_alias, -scale)
            if isinstance(node, exp.Column):
                resolved = _resolve_source_column(node, local_alias)
                if not resolved:
                    return None
                table_name, col_name = resolved
                return [(table_name, col_name, scale)]
            if isinstance(node, exp.Add):
                left_terms = _extract_linear_terms(node.left, local_alias, scale)
                right_terms = _extract_linear_terms(node.right, local_alias, scale)
                if left_terms is None or right_terms is None:
                    return None
                return left_terms + right_terms
            if isinstance(node, exp.Sub):
                left_terms = _extract_linear_terms(node.left, local_alias, scale)
                right_terms = _extract_linear_terms(node.right, local_alias, -scale)
                if left_terms is None or right_terms is None:
                    return None
                return left_terms + right_terms
            if isinstance(node, exp.Mul):
                left_const = _extract_const_float(node.left)
                right_const = _extract_const_float(node.right)
                if left_const is not None:
                    return _extract_linear_terms(node.right, local_alias, scale * left_const)
                if right_const is not None:
                    return _extract_linear_terms(node.left, local_alias, scale * right_const)
                return None
            if isinstance(node, exp.Div):
                right_const = _extract_const_float(node.right)
                if right_const is None or abs(right_const) < 1e-12:
                    return None
                return _extract_linear_terms(node.left, local_alias, scale / right_const)
            return None

        def _column_epsilon(table_name: str, col_name: str) -> float:
            col = self._resolve_column_name(table_name, col_name, tables) or col_name
            col_info = tables.get(table_name, {}).get("columns", {}).get(col, {})
            if isinstance(col_info, dict):
                col_type = str(col_info.get("type", ""))
            else:
                col_type = str(col_info or "")
            return 1.0 if "INT" in col_type.upper() else 0.01

        def _invert_op(op: str) -> str:
            return {
                ">": "<",
                ">=": "<=",
                "<": ">",
                "<=": ">=",
                "=": "=",
            }.get(op, op)

        # Build best-effort lineage for CTE output aliases:
        # - direct columns: cte_col -> base_table.base_col
        # - aggregate expressions: sidecar carrying (agg, linear terms)
        cte_lineage: Dict[str, Dict[str, Tuple[str, str]]] = {}
        cte_agg_sidecar: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}
        for cte in parsed.find_all(exp.CTE):
            cte_name = cte.alias
            if not cte_name:
                continue
            cte_query = cte.this
            select_nodes = _iter_select_nodes(cte_query)
            if not select_nodes:
                continue

            out_map: Dict[str, Tuple[str, str]] = {}
            agg_map: Dict[str, List[Dict[str, Any]]] = {}
            for select in select_nodes:
                local_alias: Dict[str, str] = {}
                for table in select.find_all(exp.Table):
                    if table.alias:
                        local_alias[table.alias] = table.name
                    local_alias[table.name] = table.name

                for sel in select.expressions:
                    out_col = getattr(sel, "alias_or_name", None)
                    if not out_col:
                        continue
                    out_col_l = out_col.lower()

                    source_expr = sel.this if isinstance(sel, exp.Alias) else sel
                    if isinstance(source_expr, exp.Column):
                        resolved = _resolve_source_column(source_expr, local_alias)
                        if resolved and out_col_l not in out_map:
                            out_map[out_col_l] = resolved

                    agg_node: Optional[exp.Expression] = None
                    if isinstance(source_expr, (exp.Sum, exp.Avg, exp.Min, exp.Max)):
                        agg_node = source_expr
                    else:
                        agg_node = source_expr.find(exp.Sum) or source_expr.find(exp.Avg)
                        if agg_node is None:
                            agg_node = source_expr.find(exp.Min) or source_expr.find(exp.Max)
                    if agg_node is None:
                        continue
                    agg_arg = agg_node.this
                    terms = _extract_linear_terms(agg_arg, local_alias)
                    if not terms:
                        continue
                    agg_map.setdefault(out_col_l, []).append(
                        {
                            "agg": agg_node.key.lower(),
                            "terms": terms,
                        }
                    )

            if out_map:
                cte_lineage[cte_name] = out_map
            if agg_map:
                cte_agg_sidecar[cte_name] = agg_map

        def _resolve_filter_column(col_expr: exp.Column) -> Optional[Tuple[str, str]]:
            table_ref = alias_map.get(col_expr.table, col_expr.table)
            col_name = col_expr.name

            if table_ref in tables:
                real_col = self._resolve_column_name(table_ref, col_name, tables)
                if real_col:
                    return table_ref, real_col

            # Predicate on a CTE output column: map back to base column if known.
            if table_ref in cte_lineage:
                mapped = cte_lineage[table_ref].get(col_name.lower())
                if mapped:
                    return mapped

            guessed_table = get_table_for_column(col_name, tables)
            if guessed_table and guessed_table in tables:
                real_col = self._resolve_column_name(guessed_table, col_name, tables)
                if real_col:
                    return guessed_table, real_col
            return None

        def _resolve_substring_prefix_target(
            node: Optional[exp.Expression],
        ) -> Optional[Tuple[str, str, int]]:
            if not isinstance(node, exp.Substring):
                return None
            source_col = node.this
            if not isinstance(source_col, exp.Column):
                return None
            start_txt = _extract_const_value(node.args.get("start"))
            length_txt = _extract_const_value(node.args.get("length"))
            try:
                start_pos = int(float(start_txt)) if start_txt is not None else 1
                prefix_len = int(float(length_txt)) if length_txt is not None else None
            except (TypeError, ValueError):
                return None
            if start_pos != 1 or not prefix_len or prefix_len <= 0:
                return None
            resolved = _resolve_filter_column(source_col)
            if not resolved:
                return None
            table_name, col_name = resolved
            return table_name, col_name, prefix_len

        def _apply_aggregate_sidecar_constraint(
            col_expr: exp.Column,
            op: str,
            const_node: Optional[exp.Expression],
        ) -> bool:
            table_ref = alias_map.get(col_expr.table, col_expr.table)
            if table_ref not in cte_agg_sidecar:
                return False
            agg_specs = cte_agg_sidecar[table_ref].get(col_expr.name.lower(), [])
            if not agg_specs:
                return False

            threshold = _extract_const_float(const_node)
            if threshold is None:
                return False

            applied = False
            for spec in agg_specs:
                agg_kind = str(spec.get("agg", "")).lower()
                if agg_kind not in {"sum", "avg"}:
                    continue
                terms = spec.get("terms") or []
                if not terms:
                    continue

                # Generic single-term linear aggregate: SUM(k*x) op c -> x op c/k.
                if len(terms) == 1:
                    t_table, t_col, coef = terms[0]
                    if abs(coef) < 1e-12:
                        continue
                    eff_op = op
                    eff_threshold = threshold / coef
                    if coef < 0:
                        eff_op = _invert_op(eff_op)
                    if eff_op in {">", ">=", "<", "<=", "="}:
                        _append_filter_value(t_table, t_col, f"{eff_op}:{_format_const_number(eff_threshold)}" if eff_op != "=" else _format_const_number(eff_threshold))
                        applied = True
                    continue

                # Two-term SUM/AVG linear form: (a - b) op c.
                pos_terms = [t for t in terms if t[2] > 0]
                neg_terms = [t for t in terms if t[2] < 0]
                if len(pos_terms) != 1 or len(neg_terms) != 1:
                    continue

                pos_table, pos_col, pos_coef = pos_terms[0]
                neg_table, neg_col, neg_coef = neg_terms[0]
                if abs(pos_coef - 1.0) > 1e-9 or abs(neg_coef + 1.0) > 1e-9:
                    continue

                pos_eps = _column_epsilon(pos_table, pos_col)
                if op == ">":
                    pos_bound = threshold + pos_eps
                    _append_filter_value(pos_table, pos_col, f">:{_format_const_number(pos_bound)}")
                    _append_filter_value(neg_table, neg_col, "0")
                    applied = True
                elif op == ">=":
                    _append_filter_value(pos_table, pos_col, f">=:{_format_const_number(threshold)}")
                    _append_filter_value(neg_table, neg_col, "0")
                    applied = True
                elif op == "<":
                    pos_bound = threshold - pos_eps
                    _append_filter_value(pos_table, pos_col, f"<:{_format_const_number(pos_bound)}")
                    _append_filter_value(neg_table, neg_col, "0")
                    applied = True
                elif op == "<=":
                    _append_filter_value(pos_table, pos_col, f"<=:{_format_const_number(threshold)}")
                    _append_filter_value(neg_table, neg_col, "0")
                    applied = True
                elif op == "=":
                    _append_filter_value(pos_table, pos_col, _format_const_number(threshold))
                    _append_filter_value(neg_table, neg_col, "0")
                    applied = True

            return applied

        def _extract_avg_multiplier(
            node: Optional[exp.Expression],
            expected_col: str,
        ) -> Optional[float]:
            if node is None:
                return None
            if isinstance(node, exp.Paren):
                return _extract_avg_multiplier(node.this, expected_col)
            if isinstance(node, exp.Cast):
                return _extract_avg_multiplier(node.this, expected_col)
            if isinstance(node, exp.Alias):
                return _extract_avg_multiplier(node.this, expected_col)

            def _avg_target(expr_node: Optional[exp.Expression]) -> Optional[exp.Column]:
                if not isinstance(expr_node, exp.Avg):
                    return None
                target = expr_node.this
                return target if isinstance(target, exp.Column) else None

            if isinstance(node, exp.Avg):
                target = _avg_target(node)
                if target and target.name.lower() == expected_col.lower():
                    return 1.0
                return None
            if isinstance(node, exp.Mul):
                left_avg = _avg_target(node.left)
                right_avg = _avg_target(node.right)
                left_const = _extract_const_float(node.left)
                right_const = _extract_const_float(node.right)
                if left_avg and right_const is not None and left_avg.name.lower() == expected_col.lower():
                    return float(right_const)
                if right_avg and left_const is not None and right_avg.name.lower() == expected_col.lower():
                    return float(left_const)
                return None
            if isinstance(node, exp.Div):
                left_avg = _avg_target(node.left)
                right_const = _extract_const_float(node.right)
                if left_avg and right_const and abs(right_const) > 1e-12 and left_avg.name.lower() == expected_col.lower():
                    return 1.0 / float(right_const)
                return None
            return None

        def _apply_correlated_avg_sidecar_constraint(
            col_expr: exp.Column,
            op: str,
            subquery_node: Optional[exp.Expression],
        ) -> bool:
            table_ref = alias_map.get(col_expr.table, col_expr.table)
            if table_ref not in cte_agg_sidecar:
                return False

            if not isinstance(subquery_node, (exp.Subquery, exp.Select)):
                return False
            sub_select = subquery_node.find(exp.Select) if isinstance(subquery_node, exp.Subquery) else subquery_node
            if sub_select is None or not sub_select.expressions:
                return False

            # Ensure the RHS aggregate references the same projected metric alias.
            mult = _extract_avg_multiplier(sub_select.expressions[0], col_expr.name)
            if mult is None or not (0.0 < mult < 2.0):
                return False

            agg_specs = cte_agg_sidecar.get(table_ref, {}).get(col_expr.name.lower(), [])
            if not agg_specs:
                return False

            # Resolve correlated key(s) from scalar subquery WHERE:
            # one side outer reference, other side local subquery alias.
            local_aliases: Set[str] = set()
            for sub_table in sub_select.find_all(exp.Table):
                local_aliases.add(sub_table.alias_or_name)
                if sub_table.alias:
                    local_aliases.add(sub_table.alias)

            correlated_inner_cols: List[str] = []
            sub_where = sub_select.find(exp.Where)
            if sub_where is not None:
                for sub_eq in sub_where.find_all(exp.EQ):
                    if not isinstance(sub_eq.left, exp.Column) or not isinstance(sub_eq.right, exp.Column):
                        continue
                    left_local = bool(sub_eq.left.table in local_aliases)
                    right_local = bool(sub_eq.right.table in local_aliases)
                    if left_local == right_local:
                        continue
                    inner_col = sub_eq.left if left_local else sub_eq.right
                    correlated_inner_cols.append(inner_col.name)

            # For N=2 witness rows:
            # x > k*avg(x) => x > (k/(2-k))*v. Emit low + high boundary-friendly values.
            slope = float(mult) / max(1e-9, (2.0 - float(mult)))
            if slope <= 0.0:
                return False

            applied = False
            for spec in agg_specs:
                agg_kind = str(spec.get("agg", "")).lower()
                if agg_kind not in {"sum", "avg"}:
                    continue
                terms = spec.get("terms") or []
                if len(terms) != 1:
                    continue
                t_table, t_col, coef = terms[0]
                if abs(coef) < 1e-12:
                    continue

                eff_op = op
                if coef < 0:
                    eff_op = _invert_op(eff_op)

                if eff_op not in {">", ">="}:
                    continue

                eps = _column_epsilon(t_table, t_col)
                villain = max(eps * 10.0, 1.0)
                hero_floor = slope * villain
                hero_low = hero_floor + (eps if eff_op == ">" else 0.0)
                hero_high = max(hero_low + eps, hero_low * 4.0)

                _append_filter_value(t_table, t_col, _format_const_number(villain))
                _append_filter_value(t_table, t_col, _format_const_number(hero_low))
                _append_filter_value(t_table, t_col, _format_const_number(hero_high))
                applied = True

                # Bind correlated grouping keys to a stable bucket value so
                # witness rows land in the same correlation partition.
                lineage = cte_lineage.get(table_ref, {})
                for corr_col in correlated_inner_cols:
                    mapped = lineage.get(corr_col.lower())
                    if mapped:
                        key_table, key_col = mapped
                        _append_filter_value(key_table, key_col, "1")

            return applied

        substring_prefix_links: List[Tuple[Tuple[str, str, int], Tuple[str, str, int]]] = []

        for where in parsed.find_all(exp.Where):
            # Equality filters: col = literal
            for eq in where.find_all(exp.EQ):
                left_col = eq.left if isinstance(eq.left, exp.Column) else None
                right_col = eq.right if isinstance(eq.right, exp.Column) else None
                left_val = _extract_const_value(eq.left)
                right_val = _extract_const_value(eq.right)

                left_prefix = _resolve_substring_prefix_target(eq.left)
                right_prefix = _resolve_substring_prefix_target(eq.right)
                if left_prefix and right_prefix and left_prefix[2] == right_prefix[2]:
                    substring_prefix_links.append((left_prefix, right_prefix))

                if left_col and right_val is not None:
                    resolved = _resolve_filter_column(left_col)
                    if resolved:
                        table, col = resolved
                        _append_filter_value(table, col, right_val)
                    else:
                        _apply_aggregate_sidecar_constraint(left_col, "=", eq.right)
                if right_col and left_val is not None:
                    resolved = _resolve_filter_column(right_col)
                    if resolved:
                        table, col = resolved
                        _append_filter_value(table, col, left_val)
                    else:
                        _apply_aggregate_sidecar_constraint(right_col, "=", eq.left)

            # IN filters: col IN (val1, val2, ...)
            for in_expr in where.find_all(exp.In):
                resolved = None
                substring_prefix_len: Optional[int] = None
                target_expr = in_expr.this

                if isinstance(target_expr, exp.Column):
                    resolved = _resolve_filter_column(target_expr)
                else:
                    prefix_target = _resolve_substring_prefix_target(target_expr)
                    if prefix_target:
                        resolved = (prefix_target[0], prefix_target[1])
                        substring_prefix_len = prefix_target[2]

                if resolved:
                    table, col = resolved
                    for v in in_expr.expressions:
                        if not isinstance(v, exp.Literal):
                            continue
                        value_txt = str(v.this)
                        if substring_prefix_len is not None:
                            value_txt = value_txt.strip("'\"")[:substring_prefix_len]
                        _append_filter_value(table, col, value_txt)

            # BETWEEN: col BETWEEN low AND high
            for between in where.find_all(exp.Between):
                if isinstance(between.this, exp.Column):
                    resolved = _resolve_filter_column(between.this)
                    if resolved:
                        table, col = resolved
                        low = _extract_const_value(between.args.get('low'))
                        high = _extract_const_value(between.args.get('high'))
                        if low is not None and high is not None:
                            filter_values.setdefault(table, {})[col] = [f"BETWEEN:{low}:{high}"]

            # GT/GTE/LT: col > literal
            for cmp_cls, op in [(exp.GT, '>'), (exp.GTE, '>='), (exp.LT, '<'), (exp.LTE, '<=')]:
                for cmp in where.find_all(cmp_cls):
                    left_col = cmp.left if isinstance(cmp.left, exp.Column) else None
                    right_col = cmp.right if isinstance(cmp.right, exp.Column) else None
                    right_subquery = cmp.right if isinstance(cmp.right, (exp.Subquery, exp.Select)) else None
                    left_subquery = cmp.left if isinstance(cmp.left, (exp.Subquery, exp.Select)) else None
                    right_val = _extract_const_value(cmp.right)
                    left_val = _extract_const_value(cmp.left)

                    if left_col and right_subquery is not None:
                        _apply_correlated_avg_sidecar_constraint(left_col, op, right_subquery)
                    if right_col and left_subquery is not None:
                        _apply_correlated_avg_sidecar_constraint(right_col, _invert_op(op), left_subquery)

                    if left_col and right_val is not None:
                        resolved = _resolve_filter_column(left_col)
                        if resolved:
                            table, col = resolved
                            _append_filter_value(table, col, f"{op}:{right_val}")
                        else:
                            _apply_aggregate_sidecar_constraint(left_col, op, cmp.right)
                    if right_col and left_val is not None:
                        inv = _invert_op(op)
                        resolved = _resolve_filter_column(right_col)
                        if resolved:
                            table, col = resolved
                            _append_filter_value(table, col, f"{inv}:{left_val}")
                        else:
                            _apply_aggregate_sidecar_constraint(right_col, inv, cmp.left)

        def _equality_prefixes(values: List[Any], prefix_len: int) -> List[str]:
            out: List[str] = []
            for raw_val in values:
                txt = str(raw_val).strip()
                if not txt or txt.startswith("BETWEEN:"):
                    continue
                if ":" in txt and txt[0] in {">", "<"}:
                    continue
                pref = txt.strip("'\"")[:prefix_len]
                if pref and pref not in out:
                    out.append(pref)
            return out

        for left, right in substring_prefix_links:
            l_table, l_col, l_len = left
            r_table, r_col, r_len = right
            if l_len != r_len:
                continue
            l_vals = filter_values.get(l_table, {}).get(l_col, [])
            r_vals = filter_values.get(r_table, {}).get(r_col, [])
            l_pref = _equality_prefixes(l_vals, l_len)
            r_pref = _equality_prefixes(r_vals, r_len)
            if l_pref and not r_pref:
                for p in l_pref:
                    _append_filter_value(r_table, r_col, p)
            elif r_pref and not l_pref:
                for p in r_pref:
                    _append_filter_value(l_table, l_col, p)
            elif l_pref and r_pref:
                common = [p for p in l_pref if p in set(r_pref)]
                if common:
                    filter_values.setdefault(l_table, {})[l_col] = common
                    filter_values.setdefault(r_table, {})[r_col] = common

        return filter_values

    def _detect_foreign_keys(self, sql: str, tables: Dict) -> Dict[str, Dict[str, Tuple[str, str]]]:
        """Detect foreign key relationships using generic naming conventions."""
        fk_relationships = {}
        all_tables = set(tables.keys())

        def _same_name_col(target_table: str, col_name: str) -> Optional[str]:
            """Return case-preserving matching column name in target table."""
            col_lower = col_name.lower()
            for existing_col in tables.get(target_table, {}).get('columns', {}):
                if existing_col.lower() == col_lower:
                    return existing_col
            return None

        for table_name in tables:
            table_fks = {}
            table_cols = list(tables[table_name]['columns'].keys())
            table_pk = find_primary_key_column(table_name, table_cols)

            for col_name in table_cols:
                col_lower = col_name.lower()
                if not (
                    col_lower.endswith('_sk')
                    or col_lower.endswith('_id')
                    or col_lower.endswith('_order_number')
                    or col_lower.endswith('_ticket_number')
                    or col_lower in {'order_number', 'ticket_number'}
                ):
                    continue

                # Skip likely PK column in current table.
                if table_pk and col_lower == table_pk.lower():
                    continue

                candidate_tables = []

                guessed = get_table_from_column(col_name, all_tables - {table_name})
                if guessed:
                    candidate_tables.append(guessed)

                parts = [p for p in col_lower.split('_') if p]
                if len(parts) >= 2:
                    body_tokens = parts[1:-1] if len(parts) > 2 else [parts[0]]
                    for known_table in tables:
                        if known_table == table_name:
                            continue
                        variants = _table_name_variants(known_table)
                        if any(tok in variants for tok in body_tokens):
                            candidate_tables.append(known_table)

                # Deduplicate while preserving order.
                seen = set()
                ordered_candidates = []
                for cand in candidate_tables:
                    if cand not in seen:
                        seen.add(cand)
                        ordered_candidates.append(cand)

                for target_table in ordered_candidates:
                    target_pk = find_primary_key_column(
                        target_table, list(tables[target_table]['columns'].keys())
                    )
                    if target_pk:
                        table_fks[col_lower] = (target_table, target_pk)
                        break

                    same_name = _same_name_col(target_table, col_name)
                    if same_name:
                        table_fks[col_lower] = (target_table, same_name)
                        break

            if table_fks:
                fk_relationships[table_name] = table_fks

        return fk_relationships
    
    def _get_table_generation_order(self, tables: Dict, fk_relationships: Dict) -> List[str]:
        """Determine table generation order (referenced tables first)."""
        dependencies = {}
        
        for table_name in tables:
            dependencies[table_name] = set()
        
        for table_name, fks in fk_relationships.items():
            for _, (target_table, _) in fks.items():
                if target_table in tables:
                    dependencies[table_name].add(target_table)
        
        ordered = []
        visited = set()
        visiting = set()
        
        def visit(t):
            if t in visited:
                return
            if t in visiting:
                # Break cycles defensively: keep stable insertion order.
                return
            visiting.add(t)
            for dep in sorted(dependencies.get(t, [])):
                visit(dep)
            visiting.remove(t)
            visited.add(t)
            ordered.append(t)
        
        for table_name in tables:
            visit(table_name)
        
        return ordered
    
    @staticmethod
    def _resolve_ambiguous_columns(sql: str) -> str:
        """Resolve ambiguous ORDER BY column references using AST.

        DuckDB is stricter than PG about unqualified column references when the
        same column appears in multiple FROM sources. This function:
        1. Finds unqualified columns in ORDER BY
        2. Looks for the same column name qualified in GROUP BY or SELECT
        3. Adds the qualifier to make the reference unambiguous

        Returns the fixed SQL string, or the original if no changes needed.
        """
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return sql

        # Find the outermost SELECT (skip CTEs/subqueries)
        outer_select = parsed.find(exp.Select)
        if outer_select is None:
            return sql

        order = parsed.find(exp.Order)
        if order is None:
            return sql

        # Build a map: unqualified_col_name -> set(table qualifiers)
        # from GROUP BY and SELECT expressions.
        qualified_map: Dict[str, Set[str]] = {}

        # Check GROUP BY for qualified columns
        group = parsed.find(exp.Group)
        if group:
            for col in group.find_all(exp.Column):
                if col.table and col.name:
                    qualified_map.setdefault(col.name, set()).add(col.table)

        # Check SELECT for qualified columns (if not in GROUP BY)
        for sel_expr in outer_select.expressions:
            for col in sel_expr.find_all(exp.Column):
                if col.table and col.name:
                    qualified_map.setdefault(col.name, set()).add(col.table)

        # Now fix unqualified ORDER BY columns
        modified = False
        for col in order.find_all(exp.Column):
            if col.table or col.name not in qualified_map:
                continue
            candidates = qualified_map[col.name]
            # Only safe when the candidate table is unique.
            if len(candidates) == 1:
                table_name = next(iter(candidates))
                col.set('table', exp.to_identifier(table_name))
                modified = True

        if modified:
            return parsed.sql(dialect='duckdb')
        return sql

    @staticmethod
    def _resolve_ambiguous_from_error(sql: str, error_msg: str) -> str:
        """Resolve a specific ambiguous-column binder error from DuckDB text.

        Example error:
          Ambiguous reference to column name "inv_item_sk"
          (use: "inventory.inv_item_sk" or "item.inv_item_sk")
        """
        m = re.search(
            r'Ambiguous reference to column name "([^"]+)"\s*\(use:\s*"([^"]+)"\s*or\s*"([^"]+)"\)',
            error_msg,
        )
        if not m:
            return sql

        col_name = m.group(1)
        ref_a = m.group(2)
        ref_b = m.group(3)

        candidates = []
        for ref in (ref_a, ref_b):
            if "." not in ref:
                continue
            table, ref_col = ref.split(".", 1)
            if ref_col.lower() == col_name.lower():
                candidates.append(table)

        if not candidates:
            return sql

        chosen = get_table_from_column(col_name, set(candidates)) or candidates[0]

        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return sql

        modified = False
        for col in parsed.find_all(exp.Column):
            if not col.table and col.name.lower() == col_name.lower():
                col.set('table', exp.to_identifier(chosen))
                modified = True

        if not modified:
            return sql
        return parsed.sql(dialect='duckdb')

    def _detect_scalar_subquery_uniques(self, sql: str, tables: Dict) -> List[Tuple[str, List[str]]]:
        """Detect scalar subqueries and return (table, filter_cols) pairs.

        Scalar subqueries appear in:
          - EQ comparisons: col = (SELECT x FROM t WHERE ...)
          - BETWEEN bounds: col BETWEEN (SELECT ...) AND (SELECT ...)
          - IN with scalar: d_date IN (SELECT d_date ... WHERE col = (SELECT ...))
        These MUST return exactly 1 row. We extract the equality-filter
        columns from each such subquery so the caller can deduplicate.
        """
        results = []
        try:
            parsed = sqlglot.parse_one(sql)
        except Exception:
            return results

        # Build alias map for table resolution
        alias_map = {}
        cte_names = set()
        for cte in parsed.find_all(exp.CTE):
            cte_names.add(cte.alias)
        for table in parsed.find_all(exp.Table):
            if table.name not in cte_names:
                if table.alias:
                    alias_map[table.alias] = table.name
                alias_map[table.name] = table.name

        def _extract_scalar_info(subquery_node):
            """Extract (table, filter_cols) from a scalar subquery node."""
            sub = subquery_node.find(exp.Select) if isinstance(subquery_node, exp.Subquery) else subquery_node
            if sub is None:
                return None
            # DISTINCT scalar subqueries already encode their own cardinality
            # contract; forcing table-level dedup can collapse join support rows.
            if sub.find(exp.Distinct):
                return None
            sub_tables = list(sub.find_all(exp.Table))
            if not sub_tables:
                return None
            sub_table = sub_tables[0]
            real_table = alias_map.get(sub_table.name, sub_table.name)
            if real_table not in tables:
                return None
            sub_where = sub.find(exp.Where)
            if not sub_where:
                return None
            filter_cols = []
            for sub_eq in sub_where.find_all(exp.EQ):
                if isinstance(sub_eq.left, exp.Column) and isinstance(sub_eq.right, (exp.Literal, exp.Add, exp.Sub)):
                    col_table = alias_map.get(sub_eq.left.table, sub_eq.left.table)
                    if not col_table or col_table == real_table or col_table == sub_table.alias:
                        col_name = sub_eq.left.name
                        if col_name in tables[real_table]['columns']:
                            filter_cols.append(col_name)
            if filter_cols:
                return (real_table, filter_cols)
            return None

        # Collect candidate scalar subquery nodes from all expression types
        scalar_nodes = []
        # EQ: col = (SELECT ...)
        for eq in parsed.find_all(exp.EQ):
            for side in [eq.left, eq.right]:
                if isinstance(side, (exp.Subquery, exp.Select)):
                    scalar_nodes.append(side)
        # BETWEEN: col BETWEEN (SELECT ...) AND (SELECT ...)
        for between in parsed.find_all(exp.Between):
            low = between.args.get('low')
            high = between.args.get('high')
            for bound in [low, high]:
                if isinstance(bound, (exp.Subquery, exp.Select)):
                    scalar_nodes.append(bound)
        # Nested: IN (SELECT ... WHERE col = (SELECT ...))
        for in_expr in parsed.find_all(exp.In):
            for nested_eq in in_expr.find_all(exp.EQ):
                for side in [nested_eq.left, nested_eq.right]:
                    if isinstance(side, (exp.Subquery, exp.Select)):
                        scalar_nodes.append(side)

        for node in scalar_nodes:
            info = _extract_scalar_info(node)
            if info:
                results.append(info)

        # Deduplicate (same table + same columns)
        seen = set()
        unique_results = []
        for table, cols in results:
            key = (table, tuple(sorted(cols)))
            if key not in seen:
                seen.add(key)
                unique_results.append((table, cols))
        return unique_results

    def _estimate_row_counts(self, sql: str, tables: Dict, target_rows: int) -> Dict[str, int]:
        """Estimate rows needed per table to get target output rows."""
        parsed = sqlglot.parse_one(sql)
        
        joins = list(parsed.find_all(exp.Join))
        join_count = len(joins)
        
        has_agg = any(parsed.find_all(exp.AggFunc))
        has_group_by = bool(list(parsed.find_all(exp.Group)))
        has_limit = bool(list(parsed.find_all(exp.Limit)))
        has_where = bool(list(parsed.find_all(exp.Where)))
        has_cte = bool(list(parsed.find_all(exp.CTE)))
        has_subquery = any(parsed.find_all(exp.Subquery))
        
        # Detect heavy analytic query shape (complex CTE/subquery join graph + filters)
        is_heavy_analytic_shape = has_cte and has_subquery and join_count >= 2 and has_where
        
        # Row estimation based on query complexity
        if is_heavy_analytic_shape:
            # Deep analytic queries need substantial data, but avoid excessive runtime.
            base_rows = 1000
        elif has_cte or has_subquery:
            base_rows = min(target_rows * 20, 20000)
        elif has_agg and has_group_by:
            # GROUP BY queries produce limited output, need less input data
            base_rows = min(target_rows * 3, 5000)
        elif has_where and join_count > 1:
            base_rows = min(target_rows * (join_count + 1), 5000)
        elif has_limit:
            base_rows = min(target_rows * 2, 3000)
        elif join_count > 0:
            base_rows = min(target_rows * (join_count + 1), 5000)
        else:
            base_rows = min(target_rows * 2, 3000)
        
        # Moderate increase for WHERE clauses
        if has_where and not is_heavy_analytic_shape:
            filter_count = 0
            for where in parsed.find_all(exp.Where):
                for eq in where.find_all(exp.EQ):
                    filter_count += 1
                for in_op in where.find_all(exp.In):
                    filter_count += 1
            base_rows = int(base_rows * (1.2 + filter_count * 0.2))
        
        # Ensure reasonable bounds
        base_rows = max(base_rows, 100)  # Minimum for meaningful results
        base_rows = min(base_rows, 20000)  # Maximum to avoid timeouts
        
        counts = {}
        for table_name in tables:
            counts[table_name] = base_rows
        
        return counts


def main():
    parser = argparse.ArgumentParser(description='Synthetic Data Validation Tool')
    parser.add_argument('sql_file', help='Path to SQL file with SELECT query')
    parser.add_argument('--target-rows', type=int, default=1000, 
                        help='Target number of output rows (default: 1000)')
    parser.add_argument('--min-rows', type=int, default=None,
                        help='Minimum acceptable rows (default: target_rows/10)')
    parser.add_argument('--max-rows', type=int, default=None,
                        help='Maximum acceptable rows (default: target_rows*10)')
    parser.add_argument('--output', '-o', help='Output results to JSON file')
    parser.add_argument('--reference-db', help='Reference DuckDB for real column types')
    parser.add_argument('--dialect', default='duckdb',
                        help='Source SQL dialect (duckdb, postgres, snowflake). '
                             'Non-DuckDB dialects are transpiled via sqlglot.')

    args = parser.parse_args()

    validator = SyntheticValidator(reference_db=args.reference_db, dialect=args.dialect)
    result = validator.validate(args.sql_file, args.target_rows, args.min_rows, args.max_rows)
    
    print("\n" + "="*50)
    print("RESULTS")
    print("="*50)
    
    if result['success']:
        in_range = result.get('in_range', True)
        status = "✅" if in_range else "⚠️"
        print(f"{status} Success!")
        print(f"   Output rows: {result['actual_rows']} (target range: {result['min_rows']}-{result['max_rows']})")
        print(f"   Tables created: {result['tables_created']}")
        print(f"   Columns: {result['columns']}")
        
        if result['sample_results']:
            print(f"\n   Sample results (first 5 rows):")
            for i, row in enumerate(result['sample_results'][:5]):
                print(f"   Row {i+1}: {row}")
    else:
        print(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"\n💾 Results saved to: {args.output}")
    
    return 0 if result['success'] else 1


if __name__ == '__main__':
    exit(main())
