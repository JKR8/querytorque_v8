"""Synthetic data validation for SQL equivalence checking.

Generates synthetic data matching query schema, executes both original
and optimized queries on it, and compares results. Used as Gate 3
(semantic validation) in the beam patch pipeline.

Ported from research/synthetic_validator/validator.py.
"""

import argparse
import hashlib
import logging
import os
import tempfile

import sqlglot
from sqlglot import exp
import duckdb
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Any, Optional
import json
import re


logger = logging.getLogger(__name__)


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

    # Strongest signal: table-specific surrogate/natural key.
    for variant in sorted(table_variants, key=len, reverse=True):
        for suffix in ('_sk', '_id'):
            candidate = f'{variant}{suffix}'
            if candidate in lower_to_original:
                return lower_to_original[candidate]

    # Common warehouse pattern: prefixed table key (e.g., c_customer_sk).
    for col_lower, col in lower_to_original.items():
        for variant in table_variants:
            if col_lower.endswith(f'_{variant}_sk') or col_lower.endswith(f'_{variant}_id'):
                return col

    # Generic fallback.
    for col_lower, col in lower_to_original.items():
        if col_lower.endswith('_sk'):
            return col
    for col_lower, col in lower_to_original.items():
        if col_lower.endswith('_id'):
            return col
    if 'id' in lower_to_original:
        return lower_to_original['id']

    return None


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
            
            tables[table_name] = {
                'alias': alias,
                'columns': {},
                'key': f"{table_name}_sk"  # assume surrogate key pattern
            }
        
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
                # Try generic naming convention against available tables
                matched_table = get_table_from_column(col_name, set(tables.keys()))
                # If this token is a derived alias and cannot be mapped to a
                # concrete base table, treat it as derived and skip.
                if not matched_table and col_name in derived_col_aliases:
                    continue
                if matched_table and matched_table in tables:
                    if col_name not in tables[matched_table]['columns']:
                        col_type = self._infer_column_type(col_name)
                        tables[matched_table]['columns'][col_name] = {
                            'type': col_type,
                            'nullable': True
                        }
    
    def _infer_column_type(self, col_name: str) -> str:
        """Infer column type from column name."""
        col_lower = col_name.lower()
        
        # Surrogate keys and IDs (most specific)
        if col_lower.endswith('_sk') or col_lower.endswith('_id') or col_lower == 'id':
            return 'INTEGER'
        elif col_lower.endswith('_key'):
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
        
        # Numeric columns - be careful not to match date-related columns
        if any(num_col in col_lower for num_col in ['qty', 'quantity', 'number']):
            return 'INTEGER'
        elif any(num_col in col_lower for num_col in ['amt', 'amount', 'price', 'cost', 'fee', 'tax', 'discount', 'profit', 'loss']):
            return 'DECIMAL(18,2)'
        # 'sales' and 'revenue' should be numeric, but 'sales_date' should be date (handled above)
        elif col_lower.endswith('sales') or col_lower.endswith('revenue'):
            return 'DECIMAL(18,2)'
        elif (col_lower == 'count' or col_lower.endswith('_count') or col_lower.startswith('count_')):
            return 'INTEGER'
        
        # String columns
        elif any(str_col in col_lower for str_col in ['name', 'desc', 'description', 'type', 'category', 'state', 'city', 'email', 'address', 'phone']):
            return 'VARCHAR(100)'
        elif col_lower.endswith('_id') and not col_lower.endswith('_sk'):
            return 'VARCHAR(50)'
        else:
            return 'VARCHAR(50)'


class SyntheticDataGenerator:
    """Generates synthetic data for tables to produce ~1000 row query results."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection, all_schemas: Dict = None):
        self.conn = conn
        self.random = random.Random(42)  # reproducible
        self.all_schemas = all_schemas or {}
        self.foreign_key_values = {}  # Store FK values for referential integrity
        self.filter_matched_values = {}  # Store PK values that match common filters
        
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
        
        # Generate rows
        rows = []
        for i in range(row_count):
            row = []
            for col_name in col_names:
                col_info = columns[col_name]
                value = self._generate_value(col_name, col_info['type'], i, row_count, foreign_keys, table_name)
                row.append(value)
            rows.append(tuple(row))
        
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
        
        # Store PK values for this table so other tables can reference them as FKs.
        pk_col = find_primary_key_column(table_name, col_names)
        
        if pk_col:
            if table_name not in self.foreign_key_values:
                self.foreign_key_values[table_name] = []
            col_idx = col_names.index(pk_col)
            for row in rows:
                self.foreign_key_values[table_name].append(row[col_idx])
            
            # For dimension tables, also track which PK values match common filter conditions
            self._track_filter_matched_values(table_name, pk_col, col_names, rows)
    
    def _parse_decimal_type(self, col_type: str) -> Dict:
        """Parse DECIMAL(precision, scale) to extract precision and scale."""
        if 'DECIMAL' in col_type.upper():
            match = re.search(r'DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)', col_type.upper())
            if match:
                return {
                    'precision': int(match.group(1)),
                    'scale': int(match.group(2))
                }
        return None
    
    def _generate_value(self, col_name: str, col_type: str, row_idx: int, total_rows: int,
                       foreign_keys: Dict = None, table_name: str = None):
        """Generate a single synthetic value."""
        col_name_lower = col_name.lower()
        foreign_keys = foreign_keys or {}
        col_type_upper = col_type.upper()

        decimal_info = self._parse_decimal_type(col_type)

        # Check if filter_literal_values has a value for this table+column
        # Inject matching values ~70% of the time so WHERE filters produce results
        filter_vals = getattr(self, 'filter_literal_values', {})
        if table_name and table_name in filter_vals and col_name in filter_vals[table_name]:
            vals = filter_vals[table_name][col_name]
            if vals:
                # Deterministic cycling ensures every filtered column gets
                # satisfying values even under highly selective workloads.
                chosen = vals[row_idx % len(vals)]
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
                            return int(chosen)
                        except (ValueError, TypeError):
                            pass
                    elif 'DECIMAL' in col_type:
                        try:
                            return float(chosen)
                        except (ValueError, TypeError):
                            pass
                    else:
                        return str(chosen)
        
        # Check if this is a foreign key column
        fk_target = foreign_keys.get(col_name_lower)
        if fk_target:
            target_table, target_col = fk_target
            # Prefer filter-matched values if available
            if target_table in self.filter_matched_values and self.filter_matched_values[target_table]:
                return self.random.choice(self.filter_matched_values[target_table])
            elif target_table in self.foreign_key_values:
                fk_vals = self.foreign_key_values[target_table]
                if fk_vals:
                    # Randomly select from existing FK values
                    return self.random.choice(fk_vals)
        
        # Surrogate keys - sequential
        is_numeric_type = any(
            t in col_type_upper for t in ('INT', 'DECIMAL', 'BIGINT', 'SMALLINT', 'FLOAT', 'DOUBLE')
        )
        if col_name_lower.endswith('_sk') or col_name_lower == 'id' or (
            col_name_lower.endswith('_id') and is_numeric_type
        ):
            if decimal_info:
                max_val = 10 ** (decimal_info['precision'] - decimal_info['scale']) - 1
                val = min(row_idx + 1, max_val)
            else:
                val = row_idx + 1
            return val
        
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
                
                if 'qty' in col_name_lower or 'quantity' in col_name_lower:
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
                    return self.random.randint(1, min(total_rows, max_val))
                else:
                    return self.random.randint(1, max(10, max_val // 10))
            
            # Regular INTEGER/BIGINT
            if 'qty' in col_name_lower or 'quantity' in col_name_lower:
                return self.random.randint(1, 100)
            elif 'amt' in col_name_lower or 'amount' in col_name_lower or 'sales' in col_name_lower:
                return round(self.random.uniform(10.0, 10000.0), 2)
            elif 'price' in col_name_lower or 'cost' in col_name_lower:
                return round(self.random.uniform(1.0, 500.0), 2)
            elif 'fee' in col_name_lower or 'tax' in col_name_lower:
                return round(self.random.uniform(0.0, 100.0), 2)
            elif col_name_lower.endswith('_sk') or col_name_lower.endswith('_id'):
                return self.random.randint(1, min(total_rows, 5000))
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
        import psycopg2
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
    
    def __init__(self, reference_db: str = None, dialect: str = 'duckdb'):
        self.conn = duckdb.connect(':memory:')
        self.reference_db = reference_db
        self.dialect = dialect.lower()
        # Only create SchemaFromDB for supported DSN schemes
        if reference_db and SchemaFromDB.supports_dsn(reference_db):
            self.schema_extractor = SchemaFromDB(reference_db)
        else:
            self.schema_extractor = None

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

        # 6. Generate synthetic data
        logger.debug("Generating synthetic data...")
        # Estimate rows needed per table based on query complexity
        table_row_counts = self._estimate_row_counts(sql, tables, target_rows)

        # Classify dimension vs fact tables
        dim_tables = []
        fact_tables = []

        for t in tables:
            if t in fk_relationships and fk_relationships[t]:
                fact_tables.append(t)
            else:
                dim_tables.append(t)
        
        logger.debug("Dimension tables: %s", dim_tables)
        logger.debug("Fact tables: %s", fact_tables)
        logger.debug("FK relationships: %s", fk_relationships)
        scalar_uniques = self._detect_scalar_subquery_uniques(sql, tables)

        def _populate_synthetic_data(row_multiplier: int = 1):
            # Reset schema/data for each synthesis attempt.
            self._create_schema(tables)
            self._create_indexes(tables, sql)

            generator = SyntheticDataGenerator(self.conn, all_schemas=tables)
            generator.filter_literal_values = filter_values

            # Generate dimension tables first
            for table_name in dim_tables:
                schema = tables[table_name]
                base_count = table_row_counts.get(table_name, 1000)
                row_count = max(10, min(base_count * row_multiplier, 200000))
                logger.debug("  %s: %d rows (dimension, x%d)", table_name, row_count, row_multiplier)
                generator.generate_table_data(table_name, schema, row_count, foreign_keys={})

            # Ensure fact FK values point to dimension rows that pass WHERE.
            self._update_filter_matched_pks(generator, tables, dim_tables, filter_values)
            logger.debug("Available FK values: %s", list(generator.foreign_key_values.keys()))

            # Generate fact tables with FKs pointing to dimension tables
            for table_name in fact_tables:
                schema = tables[table_name]
                base_count = table_row_counts.get(table_name, 1000)
                row_count = max(10, min(base_count * row_multiplier, 200000))
                table_fks = fk_relationships.get(table_name, {})
                logger.debug("  %s: %d rows (fact, x%d), FKs: %s", table_name, row_count, row_multiplier, table_fks)
                generator.generate_table_data(table_name, schema, row_count, foreign_keys=table_fks)

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

        _populate_synthetic_data(1)

        # 7. Run query
        logger.debug("Running query...")
        exec_sql = sql
        best_success = None
        last_error = None

        # Adaptive retries: if query executes but yields 0 rows, repopulate with
        # more synthetic data to increase predicate/join hit probability.
        for attempt_idx, multiplier in enumerate([1, 3]):
            if attempt_idx > 0 and min_rows > 0 and best_success and best_success.get('actual_rows', 0) == 0:
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

                    if in_range or min_rows == 0 or actual_rows > 0:
                        return payload

                    # Success with 0 rows, not in range: try larger synth set.
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
            return best_success

        logger.debug("Query failed: %s", last_error)
        return {
            'success': False,
            'error': last_error or 'Unknown query execution error',
            'tables_created': list(tables.keys())
        }
    
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
        # Fresh connection per validation to avoid state leakage
        self.conn = duckdb.connect(':memory:')

        # 1. Validate original query — this sets up synthetic tables
        with tempfile.NamedTemporaryFile(
            mode='w', suffix='.sql', delete=False
        ) as f:
            f.write(original_sql)
            orig_file = f.name

        try:
            orig_result = self.validate(orig_file, target_rows=target_rows)
        finally:
            os.unlink(orig_file)

        if not orig_result['success']:
            return {
                'match': False,
                'orig_success': False,
                'opt_success': False,
                'orig_rows': 0,
                'opt_rows': 0,
                'orig_error': orig_result.get('error', 'Unknown error'),
                'opt_error': None,
                'row_count_match': False,
                'reason': f"Original query failed: {orig_result.get('error', 'unknown')}",
            }

        # 2. Transpile optimized SQL if non-DuckDB dialect
        exec_sql = optimized_sql
        if self.dialect != 'duckdb':
            try:
                transpiled = sqlglot.transpile(
                    exec_sql, read=self.dialect, write='duckdb'
                )
                exec_sql = '\n'.join(transpiled)
            except Exception as e:
                return {
                    'match': False,
                    'orig_success': True,
                    'opt_success': False,
                    'orig_rows': orig_result['actual_rows'],
                    'opt_rows': 0,
                    'orig_error': None,
                    'opt_error': f'Transpile failed: {e}',
                    'row_count_match': False,
                    'reason': f"Optimized query transpile failed: {e}",
                }

        # Resolve ambiguous ORDER BY columns
        exec_sql = self._resolve_ambiguous_columns(exec_sql)

        # 3. Execute optimized query on same synthetic data
        try:
            opt_rows = self.conn.execute(exec_sql).fetchall()
        except Exception as e:
            return {
                'match': False,
                'orig_success': True,
                'opt_success': False,
                'orig_rows': orig_result['actual_rows'],
                'opt_rows': 0,
                'orig_error': None,
                'opt_error': str(e)[:500],
                'row_count_match': False,
                'reason': f"Optimized query failed: {str(e)[:200]}",
            }

        # 4. Compare results
        orig_rows = orig_result['sample_results']
        orig_count = orig_result['actual_rows']
        opt_count = len(opt_rows)

        if orig_count != opt_count:
            return {
                'match': False,
                'orig_success': True,
                'opt_success': True,
                'orig_rows': orig_count,
                'opt_rows': opt_count,
                'orig_error': None,
                'opt_error': None,
                'row_count_match': False,
                'reason': f"Row count mismatch: original {orig_count} vs optimized {opt_count}",
            }

        # Hash comparison (order-independent)
        def _result_hash(rows):
            sorted_rows = sorted(str(r) for r in rows)
            content = '\n'.join(sorted_rows)
            return hashlib.md5(content.encode()).hexdigest()

        # For original, we only have sample_results (first 10)
        # Re-execute original to get full results for comparison
        try:
            full_orig = self.conn.execute(
                self._resolve_ambiguous_columns(
                    '\n'.join(sqlglot.transpile(original_sql, read=self.dialect, write='duckdb'))
                ) if self.dialect != 'duckdb' else original_sql
            ).fetchall()
        except Exception:
            full_orig = orig_rows  # fallback to sample

        orig_hash = _result_hash(full_orig)
        opt_hash = _result_hash(opt_rows)

        if orig_hash == opt_hash:
            return {
                'match': True,
                'orig_success': True,
                'opt_success': True,
                'orig_rows': orig_count,
                'opt_rows': opt_count,
                'orig_error': None,
                'opt_error': None,
                'row_count_match': True,
                'reason': 'Results match (synthetic data)',
            }

        # Find first differing row for diagnostics
        sorted_orig = sorted(str(r) for r in full_orig)
        sorted_opt = sorted(str(r) for r in opt_rows)
        first_diff = None
        for i, (a, b) in enumerate(zip(sorted_orig, sorted_opt)):
            if a != b:
                first_diff = f"Row {i}: orig={a[:80]} vs opt={b[:80]}"
                break

        return {
            'match': False,
            'orig_success': True,
            'opt_success': True,
            'orig_rows': orig_count,
            'opt_rows': opt_count,
            'orig_error': None,
            'opt_error': None,
            'row_count_match': True,
            'reason': f"Value mismatch: {first_diff or 'unknown difference'}",
        }

    def _update_filter_matched_pks(self, generator, tables, dim_tables, filter_values):
        """Query generated dimension data to find PKs matching actual WHERE filters."""
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

            # Build WHERE clause from filter values
            # Group equality values per column to use IN() instead of AND
            conditions = []
            for col, vals in filter_values[table_name].items():
                if col not in tables[table_name]['columns']:
                    continue
                col_type = tables[table_name]['columns'][col]['type']
                eq_vals = []
                for val in vals:
                    if isinstance(val, str) and val.startswith('BETWEEN:'):
                        _, low, high = val.split(':', 2)
                        conditions.append(f"{col} BETWEEN '{low}' AND '{high}'")
                    elif isinstance(val, str) and ':' in val and val[0] in '><':
                        op, v = val.split(':', 1)
                        conditions.append(f"{col} {op} {v}")
                    else:
                        eq_vals.append(val)
                # Emit single = or IN() for equality values
                if len(eq_vals) == 1:
                    v = eq_vals[0]
                    if 'INT' in col_type or 'DECIMAL' in col_type:
                        conditions.append(f"{col} = {v}")
                    else:
                        conditions.append(f"{col} = '{v}'")
                elif len(eq_vals) > 1:
                    if 'INT' in col_type or 'DECIMAL' in col_type:
                        in_list = ', '.join(str(v) for v in eq_vals)
                    else:
                        in_list = ', '.join(f"'{v}'" for v in eq_vals)
                    conditions.append(f"{col} IN ({in_list})")

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
            return col_lower.endswith('_sk') or col_lower.endswith('_id') or col_lower == 'id'
        
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
            return col_lower.endswith('_sk') or col_lower.endswith('_id') or col_lower == 'id'

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
        filter_values = {}
        parsed = sqlglot.parse_one(sql)

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

        for where in parsed.find_all(exp.Where):
            # Equality filters: col = literal
            for eq in where.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Literal):
                    table = alias_map.get(eq.left.table, eq.left.table)
                    if not table:
                        table = get_table_for_column(eq.left.name, tables)
                    if table and table in tables:
                        col = eq.left.name
                        val = eq.right.this
                        if table not in filter_values:
                            filter_values[table] = {}
                        if col not in filter_values[table]:
                            filter_values[table][col] = []
                        filter_values[table][col].append(val)

            # IN filters: col IN (val1, val2, ...)
            for in_expr in where.find_all(exp.In):
                if isinstance(in_expr.this, exp.Column):
                    table = alias_map.get(in_expr.this.table, in_expr.this.table)
                    if not table:
                        table = get_table_for_column(in_expr.this.name, tables)
                    if table and table in tables:
                        col = in_expr.this.name
                        if table not in filter_values:
                            filter_values[table] = {}
                        if col not in filter_values[table]:
                            filter_values[table][col] = []
                        for v in in_expr.expressions:
                            if isinstance(v, exp.Literal):
                                filter_values[table][col].append(v.this)

            # BETWEEN: col BETWEEN low AND high
            for between in where.find_all(exp.Between):
                if isinstance(between.this, exp.Column):
                    table = alias_map.get(between.this.table, between.this.table)
                    if not table:
                        table = get_table_for_column(between.this.name, tables)
                    if table and table in tables:
                        col = between.this.name
                        low = between.args.get('low')
                        high = between.args.get('high')
                        if isinstance(low, exp.Literal) and isinstance(high, exp.Literal):
                            if table not in filter_values:
                                filter_values[table] = {}
                            filter_values[table][col] = [f"BETWEEN:{low.this}:{high.this}"]

            # GT/GTE/LT: col > literal
            for cmp_cls, op in [(exp.GT, '>'), (exp.GTE, '>='), (exp.LT, '<'), (exp.LTE, '<=')]:
                for cmp in where.find_all(cmp_cls):
                    if isinstance(cmp.left, exp.Column) and isinstance(cmp.right, exp.Literal):
                        table = alias_map.get(cmp.left.table, cmp.left.table)
                        if not table:
                            table = get_table_for_column(cmp.left.name, tables)
                        if table and table in tables:
                            col = cmp.left.name
                            if table not in filter_values:
                                filter_values[table] = {}
                            if col not in filter_values[table]:
                                filter_values[table][col] = []
                            filter_values[table][col].append(f"{op}:{cmp.right.this}")

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
                if not (col_lower.endswith('_sk') or col_lower.endswith('_id')):
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
            for col, (target_table, target_col) in fks.items():
                if target_table in tables:
                    dependencies[table_name].add(target_table)
        
        ordered = []
        visited = set()
        
        def visit(t):
            if t in visited:
                return
            for dep in dependencies.get(t, []):
                visit(dep)
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

        # Build a map: unqualified_col_name -> table qualifier
        # from GROUP BY and SELECT expressions
        qualified_map = {}  # col_name -> table_name

        # Check GROUP BY for qualified columns
        group = parsed.find(exp.Group)
        if group:
            for col in group.find_all(exp.Column):
                if col.table and col.name:
                    qualified_map[col.name] = col.table

        # Check SELECT for qualified columns (if not in GROUP BY)
        for sel_expr in outer_select.expressions:
            for col in sel_expr.find_all(exp.Column):
                if col.table and col.name and col.name not in qualified_map:
                    qualified_map[col.name] = col.table

        # Now fix unqualified ORDER BY columns
        modified = False
        for col in order.find_all(exp.Column):
            if not col.table and col.name in qualified_map:
                col.set('table', exp.to_identifier(qualified_map[col.name]))
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
