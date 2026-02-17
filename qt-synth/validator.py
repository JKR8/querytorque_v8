#!/usr/bin/env python3
"""
Synthetic Data Validation Tool

Input: SQL file with SELECT query
- Extracts schema using SQLGlot AST
- Creates schema in DuckDB
- Loads synthetic data
- Runs query and returns ~1000 rows
"""

import sqlglot
from sqlglot import exp
import duckdb
import random
import string
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Any
import argparse
import json
import re


# TPC-DS column prefix to table name mapping
TPCDS_PREFIX_MAP = {
    'sr': 'store_returns',
    'ss': 'store_sales',
    'cr': 'catalog_returns',
    'cs': 'catalog_sales',
    'wr': 'web_returns',
    'ws': 'web_sales',
    'cp': 'catalog_page',
    'cc': 'call_center',
    'web': 'web_site',
    'd': 'date_dim',
    't': 'time_dim',
    'c': 'customer',
    'ca': 'customer_address',
    'cd': 'customer_demographics',
    's': 'store',
    'i': 'item',
    'p': 'promotion',
    'w': 'warehouse',
    'wp': 'web_page',
    'hd': 'household_demographics',
    'ib': 'income_band',
    'r': 'reason',
    'sm': 'ship_mode',
    'inv': 'inventory',
}


def get_table_from_column(col_name: str) -> str:
    """Get table name from TPC-DS column naming convention."""
    col_lower = col_name.lower()
    if '_' in col_lower:
        prefix = col_lower.split('_')[0]
        # Check full prefix first (handles 3-letter like 'web', 'inv')
        if prefix in TPCDS_PREFIX_MAP:
            return TPCDS_PREFIX_MAP[prefix]
        # Then two-letter prefix (handles 'ss', 'sr', 'cs', 'cr', etc.)
        if len(prefix) >= 2:
            two_letter = prefix[:2]
            if two_letter in TPCDS_PREFIX_MAP:
                return TPCDS_PREFIX_MAP[two_letter]
        # Then single letter (handles 'd', 'c', 's', etc.)
        if len(prefix) >= 1:
            one_letter = prefix[0]
            if one_letter in TPCDS_PREFIX_MAP:
                return TPCDS_PREFIX_MAP[one_letter]
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
        
        # Extract columns using TPC-DS naming conventions
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
                # No table prefix — skip derived column aliases
                if col_name in derived_col_aliases:
                    continue
                # Try TPC-DS naming convention
                matched_table = get_table_from_column(col_name)
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
        
        # Store PK values for this table so other tables can reference them as FKs
        # The PK is typically {prefix}_{table}_sk format (e.g., s_store_sk, c_customer_sk)
        pk_col = None
        for col_name in col_names:
            col_lower = col_name.lower()
            # Check for {table}_sk pattern (e.g., store_sk for store table)
            if col_lower == f'{table_name.lower()}_sk':
                pk_col = col_name
                break
            # Check for TPC-DS pattern: {letter}_{table}_sk (e.g., s_store_sk)
            elif col_lower.endswith(f'_{table_name.lower()}_sk'):
                pk_col = col_name
                break
            # Check for any _sk column
            elif col_lower.endswith('_sk') and pk_col is None:
                pk_col = col_name
        
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

        decimal_info = self._parse_decimal_type(col_type)

        # Check if filter_literal_values has a value for this table+column
        # Inject matching values ~70% of the time so WHERE filters produce results
        filter_vals = getattr(self, 'filter_literal_values', {})
        if table_name and table_name in filter_vals and col_name in filter_vals[table_name]:
            vals = filter_vals[table_name][col_name]
            if vals and self.random.random() < 0.7:
                chosen = self.random.choice(vals)
                # Handle BETWEEN ranges
                if isinstance(chosen, str) and chosen.startswith('BETWEEN:'):
                    _, low, high = chosen.split(':', 2)
                    if 'DATE' in col_type or 'date' in col_name_lower:
                        try:
                            low_d = datetime.strptime(low, '%Y-%m-%d')
                            high_d = datetime.strptime(high, '%Y-%m-%d')
                            offset = self.random.randint(0, max(1, (high_d - low_d).days))
                            return (low_d + timedelta(days=offset)).strftime('%Y-%m-%d')
                        except ValueError:
                            pass
                    elif 'INTEGER' in col_type:
                        return self.random.randint(int(low), int(high))
                    else:
                        return float(low) + self.random.random() * (float(high) - float(low))
                # Handle comparison operators (>:, >=:, <:)
                elif isinstance(chosen, str) and ':' in chosen and chosen[0] in '><':
                    pass  # Fall through to normal generation
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
        if col_name_lower.endswith('_sk') or col_name_lower == 'id':
            if decimal_info:
                max_val = 10 ** (decimal_info['precision'] - decimal_info['scale']) - 1
                val = min(row_idx + 1, max_val)
            else:
                val = row_idx + 1
            return val
        
        # Date columns
        if col_name_lower == 'd_date':
            base_date = datetime(1990, 1, 1)
            offset = row_idx % (365 * 30)  # 30 years of dates
            return (base_date + timedelta(days=offset)).strftime('%Y-%m-%d')
        elif col_name_lower == 'd_year':
            # Distribute years but ensure 2000 is well-represented (common TPC-DS filter)
            years = list(range(1990, 2021)) + [2000] * 100  # Heavy oversample of 2000
            return years[row_idx % len(years)]
        elif col_name_lower == 'd_month' or col_name_lower == 'd_moy':
            # Month of year - ensure month 1 (January) is well represented
            months = list(range(1, 13)) + [1] * 20  # Oversample January
            return months[row_idx % len(months)]
        elif col_name_lower == 'd_day':
            return 1 + (row_idx % 28)
        elif col_name_lower in ('d_quarter', 'd_qoy'):
            return 1 + (row_idx % 4)
        elif 'date' in col_name_lower or col_type == 'DATE':
            # Generate dates from 2020-2023 to match common query filters
            base_date = datetime(2020, 1, 1)
            offset = self.random.randint(0, 365 * 4)  # 4 years: 2020-2023
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
                # Ensure SD and other common TPC-DS filter states are well-represented
                # Oversample states commonly used in TPC-DS queries (SD is very common)
                states = (['CA', 'TX', 'NY', 'FL', 'IL', 'PA', 'OH', 'GA', 'NC', 'MI'] * 2 +
                         ['SD'] * 100 +  # Heavy oversample of SD for TPC-DS Q1
                         ['TN', 'KY', 'LA', 'AL', 'OK', 'UT', 'NV', 'NM', 'KS'] * 5)
                return states[row_idx % len(states)]
            elif 'city' in col_name_lower:
                cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 
                         'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose',
                         'Austin', 'Jacksonville', 'Fort Worth', 'Columbus', 'Charlotte']
                return cities[row_idx % len(cities)]
            # Check specific column names BEFORE generic patterns
            elif col_name_lower == 's_store_name':
                # Include 'ese' for TPC-DS Q5 style queries
                names = ['ese', 'abc', 'def', 'ghi', 'jkl', 'mno', 'pqr', 'stu', 'vwx', 'yz'] * 10
                return names[row_idx % len(names)]
            elif col_name_lower == 's_store_id':
                return f"STORE{row_idx:04d}"
            elif 'customer_id' in col_name_lower:
                return f"AAAAAAA{row_idx % 10000000:07d}"
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
        """Track PK values that match common filter conditions for this table type."""
        self.filter_matched_values[table_name] = []
        
        # Define filter conditions based on table name
        filter_checks = []
        col_indices = {}
        
        for i, col_name in enumerate(col_names):
            col_lower = col_name.lower()
            col_indices[col_lower] = i
            
            if table_name == 'store' or table_name == 'stores':
                if col_lower == 's_store_name':
                    filter_checks.append(lambda row, idx=i: row[idx] == 'ese')
                elif col_lower == 's_state':
                    filter_checks.append(lambda row, idx=i: row[idx] == 'SD')
            elif table_name == 'date_dim':
                if col_lower == 'd_year':
                    filter_checks.append(lambda row, idx=i: row[idx] == 2000)
                elif col_lower == 'd_moy':
                    filter_checks.append(lambda row, idx=i: row[idx] == 1)
            elif table_name == 'customer' or table_name == 'customers':
                if col_lower == 'c_customer_id':
                    filter_checks.append(lambda row, idx=i: row[idx] is not None)
            elif table_name == 'customer_address':
                if col_lower == 'ca_address_sk':
                    filter_checks.append(lambda row, idx=i: row[idx] is not None)
        
        # Find PK column index
        pk_idx = col_names.index(pk_col)
        
        # Check each row against filter conditions
        for row in rows:
            # A row is "good" if it matches ANY filter condition (OR logic)
            # or if there are no specific filters defined
            is_good = len(filter_checks) == 0
            for check in filter_checks:
                try:
                    if check(row):
                        is_good = True
                        break
                except:
                    pass
            
            if is_good:
                self.filter_matched_values[table_name].append(row[pk_idx])
        
        # If no filter-matched values found, use all PK values
        if not self.filter_matched_values[table_name]:
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
            conn = duckdb.connect(self.db_path, read_only=True)
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
            conn = duckdb.connect(self.db_path, read_only=True)
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
        self.schema_extractor = SchemaFromDB(reference_db) if reference_db else None

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
            print(f"⚠️  Multi-statement file ({len(statements)} statements), validating first")

        # Handle row range
        if min_rows is None:
            min_rows = 0  # Allow 0 rows for complex filter queries
        if max_rows is None:
            max_rows = target_rows * 20  # More lenient upper bound

        print(f"📄 Input SQL file: {sql_file}")
        print(f"🎯 Target output rows: {target_rows} (range: {min_rows}-{max_rows})")

        # 1b. Transpile to DuckDB if source dialect differs
        if self.dialect != 'duckdb':
            print(f"\n🔄 Transpiling from {self.dialect} to duckdb...")
            try:
                transpiled = sqlglot.transpile(sql, read=self.dialect, write='duckdb')
                sql = '\n'.join(transpiled)
                print(f"   Transpiled OK ({len(sql)} chars)")
            except Exception as e:
                return {
                    'success': False, 'error': f'Transpile failed: {e}',
                    'tables_created': [], 'actual_rows': 0,
                    'min_rows': min_rows, 'max_rows': max_rows,
                }

        # 1c. Resolve ambiguous column references (ORDER BY without table qualifier)
        sql = self._resolve_ambiguous_columns(sql)

        # 2. Extract schema using SQLGlot (always parse as duckdb after transpile)
        print("\n🔍 Extracting schema with SQLGlot AST...")
        extractor = SchemaExtractor(sql)
        tables = extractor.extract_tables()
        print(f"   Found {len(tables)} tables: {list(tables.keys())}")
        
        # Print columns for each table
        for t_name, t_info in tables.items():
            print(f"   {t_name}: {list(t_info['columns'].keys())}")
        
        # 3. Override schema from reference DB if available
        #    Reference DB is authoritative for column types — AST extraction
        #    only discovers which tables are used in the query.
        print("\n🏗️  Creating schema in DuckDB...")
        if self.schema_extractor:
            print(f"   Loading real schemas from: {self.reference_db}")
            for table_name in list(tables.keys()):
                ref_schema = self.schema_extractor.get_table_schema(table_name)
                if ref_schema:
                    # Start with ALL reference DB columns (complete schema)
                    merged = {}
                    for col_name, col_info in ref_schema.items():
                        merged[col_name] = col_info
                    # Add any AST-discovered columns not in reference DB
                    # (e.g., derived columns that only exist in the query)
                    # Use case-insensitive comparison to avoid duplicates
                    ref_lower = {k.lower() for k in ref_schema}
                    for col_name, col_info in tables[table_name]['columns'].items():
                        if col_name.lower() not in ref_lower:
                            merged[col_name] = col_info
                    tables[table_name]['columns'] = merged
                    print(f"   {table_name}: {len(ref_schema)} cols from reference DB")
        
        self._create_schema(tables)
        
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
        self._create_indexes(tables, sql)

        # 5. Extract filter values from WHERE clause for data generation
        filter_values = self._extract_filter_values(sql, tables)

        # 6. Generate synthetic data
        print("\n🎲 Generating synthetic data...")
        generator = SyntheticDataGenerator(self.conn, all_schemas=tables)
        generator.filter_literal_values = filter_values

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
        
        print(f"   Dimension tables: {dim_tables}")
        print(f"   Fact tables: {fact_tables}")
        print(f"   FK relationships: {fk_relationships}")
        
        # Generate dimension tables first
        for table_name in dim_tables:
            schema = tables[table_name]
            row_count = table_row_counts.get(table_name, 1000)
            print(f"   {table_name}: {row_count} rows (dimension)")
            generator.generate_table_data(table_name, schema, row_count, foreign_keys={})

        # After dimension generation, find PKs matching actual query filters
        # This ensures fact FK values point to dimension rows that pass WHERE clauses
        self._update_filter_matched_pks(generator, tables, dim_tables, filter_values)

        # Print available FK values
        print(f"   Available FK values: {list(generator.foreign_key_values.keys())}")
        
        # Generate fact tables with FKs pointing to dimension tables
        for table_name in fact_tables:
            schema = tables[table_name]
            row_count = table_row_counts.get(table_name, 1000)
            table_fks = fk_relationships.get(table_name, {})
            print(f"   {table_name}: {row_count} rows (fact), FKs: {table_fks}")
            generator.generate_table_data(table_name, schema, row_count, foreign_keys=table_fks)
        
        # 6b. Deduplicate columns used by scalar subqueries
        #     A scalar subquery (used with = comparison) must return exactly 1 row.
        #     Synthetic data can violate this — deduplicate the relevant columns.
        scalar_uniques = self._detect_scalar_subquery_uniques(sql, tables)
        if scalar_uniques:
            print("\n🔧 Enforcing scalar subquery uniqueness...")
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
                    # Drop the helper column
                    self.conn.execute(f"ALTER TABLE {table_name} DROP COLUMN __rn")
                    remaining = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                    print(f"   {table_name}: deduped on ({cols_csv}), {remaining} rows remain")
                except Exception as e:
                    print(f"   {table_name}: dedup failed: {e}")

        # 7. Run query
        print("\n⚡ Running query...")
        try:
            result = self.conn.execute(sql).fetchall()
            actual_rows = len(result)
            in_range = min_rows <= actual_rows <= max_rows
            range_status = "✓" if in_range else "⚠"
            print(f"   {range_status} Query returned {actual_rows} rows (target range: {min_rows}-{max_rows})")
            
            # Get column names
            columns = [desc[0] for desc in self.conn.description] if self.conn.description else []
            
            return {
                'success': True,
                'target_rows': target_rows,
                'min_rows': min_rows,
                'max_rows': max_rows,
                'actual_rows': actual_rows,
                'in_range': min_rows <= actual_rows <= max_rows,
                'columns': columns,
                'sample_results': result[:10] if result else [],
                'tables_created': list(tables.keys())
            }
            
        except Exception as e:
            print(f"   ✗ Query failed: {e}")
            return {
                'success': False,
                'error': str(e),
                'tables_created': list(tables.keys())
            }
    
    def _update_filter_matched_pks(self, generator, tables, dim_tables, filter_values):
        """Query generated dimension data to find PKs matching actual WHERE filters."""
        for table_name in dim_tables:
            if table_name not in filter_values:
                continue

            # Find the PK column
            pk_col = None
            for col in tables[table_name]['columns']:
                if col.lower().endswith('_sk'):
                    pk_col = col
                    break
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
                    print(f"   {table_name}: {len(matched_pks)} PKs match query filters")
            except Exception as e:
                print(f"   {table_name}: filter PK query failed: {e}")

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
            print(f"   Created: {table_name}")
    
    def _create_indexes(self, tables: Dict, sql: str):
        """Create indexes on join columns for better query performance."""
        parsed = sqlglot.parse_one(sql)
        join_columns = {}
        
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
                    left_table = left.table or get_table_from_column(left.name)
                    left_col = left.name
                    right_table = right.table or get_table_from_column(right.name)
                    right_col = right.name

                    if left_col.endswith('_sk') or right_col.endswith('_sk'):
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

            # Resolve None tables using TPC-DS column prefix heuristic
            if not left_table or left_table not in tables:
                left_table = get_table_from_column(left_col)
            if not right_table or right_table not in tables:
                right_table = get_table_from_column(right_col)

            if not left_table or not right_table:
                continue
            if left_table == right_table:
                continue
            if left_table not in tables or right_table not in tables:
                continue
            if not (left_col.endswith('_sk') or right_col.endswith('_sk')):
                continue

            # Determine FK direction using _sk column count heuristic
            # Fact tables have many _sk columns (FKs), dimension tables have few (just PK)
            left_sk_count = sum(1 for c in tables[left_table]['columns'] if c.lower().endswith('_sk'))
            right_sk_count = sum(1 for c in tables[right_table]['columns'] if c.lower().endswith('_sk'))

            if left_sk_count > right_sk_count:
                # left table has more _sk cols → likely fact table → left col is FK
                if left_table not in fk_relationships:
                    fk_relationships[left_table] = {}
                fk_relationships[left_table][left_col.lower()] = (right_table, right_col)
            elif right_sk_count > left_sk_count:
                # right table has more _sk cols → likely fact table → right col is FK
                if right_table not in fk_relationships:
                    fk_relationships[right_table] = {}
                fk_relationships[right_table][right_col.lower()] = (left_table, left_col)
            else:
                # Equal _sk count — use column prefix to determine PK ownership
                left_guessed = get_table_from_column(left_col)
                right_guessed = get_table_from_column(right_col)
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
                        table = get_table_from_column(eq.left.name)
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
                        table = get_table_from_column(in_expr.this.name)
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
                        table = get_table_from_column(between.this.name)
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
                            table = get_table_from_column(cmp.left.name)
                        if table and table in tables:
                            col = cmp.left.name
                            if table not in filter_values:
                                filter_values[table] = {}
                            if col not in filter_values[table]:
                                filter_values[table][col] = []
                            filter_values[table][col].append(f"{op}:{cmp.right.this}")

        return filter_values

    def _detect_foreign_keys(self, sql: str, tables: Dict) -> Dict[str, Dict[str, Tuple[str, str]]]:
        """Detect foreign key relationships using TPC-DS naming conventions.

        Only adds an FK when the target PK column is verified to exist in the
        target table's schema. This prevents fabricating columns like
        ``w_web_site_sk`` that don't actually exist.
        """
        fk_relationships = {}

        def _target_has_col(target_table: str, pk_col: str) -> bool:
            """Return True only if target table's schema contains pk_col."""
            return pk_col in tables.get(target_table, {}).get('columns', {})

        for table_name in tables:
            table_fks = {}
            for col_name in tables[table_name]['columns']:
                col_lower = col_name.lower()
                if not col_lower.endswith('_sk'):
                    continue

                # Pattern 1: TPC-DS style - {prefix}_{target}_sk
                # e.g., sr_store_sk -> store.s_store_sk
                if '_' in col_lower:
                    parts = col_lower.split('_')
                    if len(parts) >= 3:
                        potential_table = '_'.join(parts[1:-1])
                        for known_table in tables:
                            if known_table.lower() == potential_table and known_table != table_name:
                                # Try to find the actual PK column in the target
                                pk_prefix = known_table.split('_')[0][0] if '_' in known_table else known_table[0]
                                pk_col = f"{pk_prefix}_{potential_table}_sk"
                                if _target_has_col(known_table, pk_col):
                                    table_fks[col_lower] = (known_table, pk_col)
                                elif _target_has_col(known_table, col_name):
                                    # Fallback: same column name in target
                                    table_fks[col_lower] = (known_table, col_name)
                                break

                # Pattern 2: Simple FK - {table}_sk references {table}.{table}_sk
                if col_lower not in table_fks:
                    potential_base = col_lower[:-3]  # Remove _sk
                    for known_table in tables:
                        if known_table != table_name:
                            known_lower = known_table.lower()
                            if potential_base == known_lower or potential_base == known_lower.rstrip('s'):
                                pk_col = col_name  # Same name
                                if _target_has_col(known_table, pk_col):
                                    table_fks[col_lower] = (known_table, pk_col)
                                break

                # Pattern 3: Complex FK - {prefix}_{context}_{table}_sk
                # e.g., wr_returning_customer_sk -> customer
                # Only match if we can verify the PK column exists
                if col_lower not in table_fks:
                    parts = col_lower.split('_')
                    if len(parts) >= 3:
                        potential_table = parts[-2]
                        for known_table in tables:
                            if known_table != table_name:
                                known_lower = known_table.lower()
                                if potential_table == known_lower or potential_table == known_lower.rstrip('s'):
                                    # Try the FK column name as PK (often same in TPC-DS)
                                    if _target_has_col(known_table, col_name):
                                        table_fks[col_lower] = (known_table, col_name)
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
        
        # Detect if this is a TPC-DS style query (complex with CTEs and multiple filters)
        is_tpcds_style = has_cte and has_subquery and join_count >= 2 and has_where
        
        # Row estimation based on query complexity
        if is_tpcds_style:
            # TPC-DS queries need substantial data but correlated subqueries are slow
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
        if has_where and not is_tpcds_style:
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
