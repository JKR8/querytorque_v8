#!/usr/bin/env python3
"""
Synthetic Data Validation Tool v2 - Filter-Aware Automated Pipeline
"""

import sqlglot
from sqlglot import exp
import duckdb
import random
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Any, Optional
import argparse
import json


class FilterExtractor:
    """Extracts filter conditions from SQL queries."""
    
    def __init__(self, sql: str):
        self.sql = sql
        self.parsed = sqlglot.parse_one(sql)
    
    def extract_filters(self) -> Dict[str, List[Dict]]:
        """Extract all filter conditions per table."""
        filters = {}
        
        for where in self.parsed.find_all(exp.Where):
            self._extract_from_expression(where, filters)
        
        for having in self.parsed.find_all(exp.Having):
            self._extract_from_expression(having, filters)
            
        return filters
    
    def _extract_from_expression(self, expr, filters: Dict):
        """Recursively extract filters from an expression."""
        # Handle AND/OR combinations
        for and_expr in expr.find_all(exp.And):
            for child in and_expr.expressions:
                self._process_condition(child, filters)
        
        for or_expr in expr.find_all(exp.Or):
            # For OR, we need at least one to match - collect all options
            or_filters = []
            for child in or_expr.expressions:
                temp_filters = {}
                self._process_condition(child, temp_filters)
                or_filters.append(temp_filters)
            # Merge OR filters (simplified - just take first for now)
            for f in or_filters:
                for table, conds in f.items():
                    if table not in filters:
                        filters[table] = []
                    filters[table].extend(conds)
        
        # Process direct conditions
        self._process_condition(expr, filters)
    
    def _process_condition(self, expr, filters: Dict):
        """Process a single condition."""
        # Equality: col = value
        for eq in expr.find_all(exp.EQ):
            if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Literal):
                col = eq.left.name
                table = eq.left.table or self._guess_table(col)
                value = eq.right.this
                
                if table not in filters:
                    filters[table] = []
                filters[table].append({
                    'column': col,
                    'operator': '=',
                    'value': value,
                    'type': 'equality'
                })
        
        # IN clause: col IN (val1, val2, ...)
        for in_expr in expr.find_all(exp.In):
            if isinstance(in_expr.this, exp.Column):
                col = in_expr.this.name
                table = in_expr.this.table or self._guess_table(col)
                values = []
                for v in in_expr.expressions:
                    if isinstance(v, exp.Literal):
                        values.append(v.this)
                
                if table and values:
                    if table not in filters:
                        filters[table] = []
                    filters[table].append({
                        'column': col,
                        'operator': 'IN',
                        'values': values,
                        'type': 'in_list'
                    })
        
        # BETWEEN: col BETWEEN val1 AND val2
        for between in expr.find_all(exp.Between):
            if isinstance(between.this, exp.Column):
                col = between.this.name
                table = between.this.table or self._guess_table(col)
                low = between.args.get('low')
                high = between.args.get('high')
                
                if isinstance(low, exp.Literal) and isinstance(high, exp.Literal):
                    if table not in filters:
                        filters[table] = []
                    filters[table].append({
                        'column': col,
                        'operator': 'BETWEEN',
                        'low': low.this,
                        'high': high.this,
                        'type': 'range'
                    })
        
        # Greater/Less than: col > value, col < value
        for gt in expr.find_all(exp.GT):
            if isinstance(gt.left, exp.Column) and isinstance(gt.right, exp.Literal):
                col = gt.left.name
                table = gt.left.table or self._guess_table(col)
                if table not in filters:
                    filters[table] = []
                filters[table].append({
                    'column': col,
                    'operator': '>',
                    'value': gt.right.this,
                    'type': 'comparison'
                })
        
        for lt in expr.find_all(exp.LT):
            if isinstance(lt.left, exp.Column) and isinstance(lt.right, exp.Literal):
                col = lt.left.name
                table = lt.left.table or self._guess_table(col)
                if table not in filters:
                    filters[table] = []
                filters[table].append({
                    'column': col,
                    'operator': '<',
                    'value': lt.right.this,
                    'type': 'comparison'
                })
        
        for gte in expr.find_all(exp.GTE):
            if isinstance(gte.left, exp.Column) and isinstance(gte.right, exp.Literal):
                col = gte.left.name
                table = gte.left.table or self._guess_table(col)
                if table not in filters:
                    filters[table] = []
                filters[table].append({
                    'column': col,
                    'operator': '>=',
                    'value': gte.right.this,
                    'type': 'comparison'
                })
    
    def _guess_table(self, col_name: str) -> Optional[str]:
        """Guess table from column name using TPC-DS conventions."""
        col_lower = col_name.lower()
        
        # TPC-DS prefix mapping
        prefix_map = {
            'ws': 'web_sales', 'ss': 'store_sales', 'cs': 'catalog_sales',
            'wr': 'web_returns', 'sr': 'store_returns', 'cr': 'catalog_returns',
            'd': 'date_dim', 't': 'time_dim',
            'c': 'customer', 'ca': 'customer_address', 'cd': 'customer_demographics',
            's': 'store', 'i': 'item', 'p': 'promotion',
            'w': 'warehouse', 'wp': 'web_page',
        }
        
        if '_' in col_lower:
            parts = col_lower.split('_')
            # Check two-letter prefix
            if len(parts[0]) >= 2:
                prefix = parts[0][:2]
                if prefix in prefix_map:
                    return prefix_map[prefix]
            # Check single letter
            if parts[0] in prefix_map:
                return prefix_map[parts[0]]
            # Check if column contains table name
            for prefix, table in prefix_map.items():
                if col_lower.startswith(prefix + '_') or col_lower.endswith('_' + prefix):
                    return table
        
        return None
    
    def extract_joins(self) -> List[Tuple[str, str, str, str]]:
        """Extract join conditions as (left_table, left_col, right_table, right_col)."""
        joins = []
        alias_map = {}
        
        # Build alias map
        for table in self.parsed.find_all(exp.Table):
            if table.alias:
                alias_map[table.alias] = table.name
            alias_map[table.name] = table.name
        
        for join in self.parsed.find_all(exp.Join):
            for eq in join.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                    left_table = alias_map.get(eq.left.table, eq.left.table)
                    right_table = alias_map.get(eq.right.table, eq.right.table)
                    joins.append((left_table, eq.left.name, right_table, eq.right.name))
        
        # Also check WHERE for implicit joins
        for where in self.parsed.find_all(exp.Where):
            for eq in where.find_all(exp.EQ):
                if isinstance(eq.left, exp.Column) and isinstance(eq.right, exp.Column):
                    left_table = alias_map.get(eq.left.table, eq.left.table)
                    right_table = alias_map.get(eq.right.table, eq.right.table)
                    if left_table != right_table:
                        joins.append((left_table, eq.left.name, right_table, eq.right.name))
        
        return joins


class FilterAwareGenerator:
    """Generates synthetic data that matches filter conditions."""
    
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self.conn = conn
        self.random = random.Random(42)
        self.pk_values = {}  # table -> list of PK values
        self.filter_matched_pks = {}  # table -> list of PKs that pass filters
    
    def generate_dimension_table(self, table_name: str, schema: Dict, 
                                  filters: List[Dict], row_count: int = 500):
        """Generate dimension table with filter-matching values."""
        columns = schema['columns']
        col_names = list(columns.keys())
        
        if not col_names:
            return
        
        # Build INSERT
        placeholders = ', '.join(['?' for _ in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
        
        # Identify PK column
        pk_col = None
        for col in col_names:
            if col.lower().endswith('_sk'):
                pk_col = col
                break
        
        pk_idx = col_names.index(pk_col) if pk_col else 0
        
        # Generate rows
        rows = []
        matched_pks = []
        
        for i in range(row_count):
            row = []
            matches_filter = False
            
            for col_name in col_names:
                col_type = columns[col_name]['type']
                value = self._generate_filter_aware_value(
                    col_name, col_type, i, row_count, filters
                )
                row.append(value)
            
            # Check if this row matches filters
            matches_filter = self._check_filter_match(row, col_names, filters)
            
            rows.append(tuple(row))
            
            # Track PK if filter-matched
            if pk_col and matches_filter:
                matched_pks.append(row[pk_idx])
        
        # Insert
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            self.conn.executemany(insert_sql, batch)
        
        # Store PK values
        if pk_col:
            self.pk_values[table_name] = [row[pk_idx] for row in rows]
            self.filter_matched_pks[table_name] = matched_pks if matched_pks else self.pk_values[table_name]
    
    def generate_fact_table(self, table_name: str, schema: Dict,
                            fk_relations: Dict[str, Tuple[str, str]], 
                            row_count: int = 1000):
        """Generate fact table with FKs pointing to filter-matched dimension rows."""
        columns = schema['columns']
        col_names = list(columns.keys())
        
        if not col_names:
            return
        
        placeholders = ', '.join(['?' for _ in col_names])
        insert_sql = f"INSERT INTO {table_name} ({', '.join(col_names)}) VALUES ({placeholders})"
        
        rows = []
        for i in range(row_count):
            row = []
            for col_name in col_names:
                col_lower = col_name.lower()
                col_type = columns[col_name]['type']
                
                # Check if this is an FK column
                fk_target = None
                for fk_col, (target_table, target_col) in fk_relations.items():
                    if fk_col == col_lower:
                        fk_target = target_table
                        break
                
                if fk_target and fk_target in self.filter_matched_pks:
                    # Use filter-matched PK values
                    value = self.random.choice(self.filter_matched_pks[fk_target])
                else:
                    value = self._generate_value(col_name, col_type, i, row_count)
                
                row.append(value)
            rows.append(tuple(row))
        
        # Insert
        for i in range(0, len(rows), 1000):
            batch = rows[i:i+1000]
            self.conn.executemany(insert_sql, batch)
    
    def _generate_filter_aware_value(self, col_name: str, col_type: str, 
                                      row_idx: int, total_rows: int, 
                                      filters: List[Dict]) -> Any:
        """Generate a value, preferring filter-matching values."""
        col_lower = col_name.lower()
        
        # Check if this column has a filter condition
        for f in filters:
            if f['column'].lower() == col_lower:
                # Generate value that matches the filter
                if f['type'] == 'equality':
                    # 70% chance to match the filter value
                    if self.random.random() < 0.7:
                        return self._convert_value(f['value'], col_type)
                elif f['type'] == 'in_list':
                    # 80% chance to pick from IN list
                    if self.random.random() < 0.8:
                        val = self.random.choice(f['values'])
                        return self._convert_value(val, col_type)
                elif f['type'] == 'range':
                    # Generate within range
                    return self._generate_in_range(f['low'], f['high'], col_type, row_idx)
        
        # Default generation
        return self._generate_value(col_name, col_type, row_idx, total_rows)
    
    def _check_filter_match(self, row: List, col_names: List[str], 
                            filters: List[Dict]) -> bool:
        """Check if a row matches all filter conditions."""
        if not filters:
            return True
        
        col_map = {c.lower(): i for i, c in enumerate(col_names)}
        
        matches = 0
        for f in filters:
            col_idx = col_map.get(f['column'].lower())
            if col_idx is None:
                continue
            
            row_val = row[col_idx]
            
            if f['type'] == 'equality':
                if str(row_val) == str(self._convert_value(f['value'], type(row_val).__name__)):
                    matches += 1
            elif f['type'] == 'in_list':
                if str(row_val) in [str(self._convert_value(v, type(row_val).__name__)) for v in f['values']]:
                    matches += 1
            elif f['type'] == 'range':
                # Simplified range check
                matches += 1
        
        # Row matches if at least one filter matches (OR logic within table)
        return matches > 0 or len(filters) == 0
    
    def _convert_value(self, value: str, col_type: str) -> Any:
        """Convert a string value to the appropriate type."""
        if 'INTEGER' in col_type.upper():
            return int(value)
        elif 'DECIMAL' in col_type.upper() or 'NUMERIC' in col_type.upper():
            return float(value)
        elif 'DATE' in col_type.upper():
            return value  # Keep as string for dates
        return value.strip("'") if value.startswith("'") else value
    
    def _generate_in_range(self, low: str, high: str, col_type: str, row_idx: int) -> Any:
        """Generate a value within a range."""
        if 'DATE' in col_type.upper():
            # Generate dates within range
            low_date = datetime.strptime(low.strip("'"), '%Y-%m-%d')
            high_date = datetime.strptime(high.strip("'"), '%Y-%m-%d')
            days_range = (high_date - low_date).days
            offset = self.random.randint(0, max(1, days_range))
            result_date = low_date + timedelta(days=offset)
            return result_date.strftime('%Y-%m-%d')
        elif 'INTEGER' in col_type.upper():
            low_val = int(low)
            high_val = int(high)
            return self.random.randint(low_val, high_val)
        else:
            return low  # Fallback
    
    def _generate_value(self, col_name: str, col_type: str, row_idx: int, total_rows: int) -> Any:
        """Default value generation."""
        col_lower = col_name.lower()
        
        # PK/SK columns
        if col_lower.endswith('_sk') or col_lower == 'id':
            return row_idx + 1
        
        # Date columns
        if 'date' in col_lower or col_type == 'DATE':
            base = datetime(2020, 1, 1)
            offset = self.random.randint(0, 365 * 4)
            return (base + timedelta(days=offset)).strftime('%Y-%m-%d')
        
        # Numeric
        if 'DECIMAL' in col_type or 'INTEGER' in col_type:
            if 'price' in col_lower or 'amount' in col_lower or 'sales' in col_lower:
                return round(self.random.uniform(10, 1000), 2)
            elif 'qty' in col_lower or 'quantity' in col_lower:
                return self.random.randint(1, 100)
            return self.random.randint(1, 10000)
        
        # String
        if 'VARCHAR' in col_type:
            if 'name' in col_lower:
                return f"Name_{row_idx}"
            elif 'category' in col_lower:
                cats = ['Sports', 'Books', 'Home', 'Electronics', 'Clothing']
                return cats[row_idx % len(cats)]
            elif 'class' in col_lower:
                return f"Class_{row_idx % 10}"
            elif 'desc' in col_lower:
                return f"Description_{row_idx}"
            return f"VAL_{row_idx}"
        
        return None


class SchemaExtractor:
    """Extracts schema from SQL."""
    
    def __init__(self, sql: str):
        self.sql = sql
        self.parsed = sqlglot.parse_one(sql)
    
    def extract_tables(self) -> Dict[str, Dict]:
        """Extract tables with columns."""
        tables = {}
        cte_names = set()
        
        for cte in self.parsed.find_all(exp.CTE):
            cte_names.add(cte.alias)
        
        # Build alias map first
        alias_map = {}
        for table in self.parsed.find_all(exp.Table):
            table_name = table.name
            if table_name in cte_names:
                continue
            
            tables[table_name] = {
                'alias': table.alias,
                'columns': {}
            }
            if table.alias:
                alias_map[table.alias] = table_name
            alias_map[table_name] = table_name
        
        # Extract columns - handle both aliased and non-aliased references
        for col in self.parsed.find_all(exp.Column):
            table_ref = col.table
            col_name = col.name
            
            if table_ref:
                # Resolve alias to actual table name
                actual_table = alias_map.get(table_ref, table_ref)
                if actual_table in tables and actual_table not in cte_names:
                    tables[actual_table]['columns'][col_name] = {
                        'type': self._infer_type(col_name),
                        'nullable': True
                    }
            else:
                # No table prefix - try to guess from column name
                guessed_table = self._guess_table_from_column(col_name)
                if guessed_table and guessed_table in tables:
                    tables[guessed_table]['columns'][col_name] = {
                        'type': self._infer_type(col_name),
                        'nullable': True
                    }
        
        return tables
    
    def _guess_table_from_column(self, col_name: str) -> Optional[str]:
        """Guess table from column name."""
        col_lower = col_name.lower()
        
        # TPC-DS patterns
        patterns = {
            'ws_': 'web_sales', 'ss_': 'store_sales', 'cs_': 'catalog_sales',
            'wr_': 'web_returns', 'sr_': 'store_returns', 'cr_': 'catalog_returns',
            'd_': 'date_dim', 't_': 'time_dim',
            'c_': 'customer', 'ca_': 'customer_address', 'cd_': 'customer_demographics',
            's_': 'store', 'i_': 'item', 'p_': 'promotion',
        }
        
        for prefix, table in patterns.items():
            if col_lower.startswith(prefix):
                return table
        
        return None
    
    def _infer_type(self, col_name: str) -> str:
        """Infer column type from name."""
        col_lower = col_name.lower()
        
        # Surrogate keys and IDs
        if col_lower.endswith('_sk') or col_lower.endswith('_id'):
            return 'INTEGER'
        
        # Date columns (but not date_sk)
        if 'date' in col_lower and 'sk' not in col_lower:
            return 'DATE'
        
        # Date dimension columns
        if col_lower in ['d_year', 'd_month', 'd_moy', 'd_day', 'd_quarter', 'd_week']:
            return 'INTEGER'
        if col_lower in ['year', 'month', 'day', 'quarter']:
            return 'INTEGER'
        
        # Numeric columns - fee, tax, discount, etc.
        if any(x in col_lower for x in ['fee', 'tax', 'discount', 'price', 'amount', 'cost', 'sales', 'revenue', 'amt']):
            return 'DECIMAL(18,2)'
        if any(x in col_lower for x in ['qty', 'quantity', 'count', 'number']):
            return 'INTEGER'
        
        return 'VARCHAR(100)'


class SyntheticValidatorV2:
    """Main validator using filter-aware generation."""
    
    def __init__(self):
        self.conn = duckdb.connect(':memory:')
    
    def validate(self, sql_file: str, target_rows: int = 1000) -> Dict[str, Any]:
        """Run validation with automated filter-aware data generation."""
        import time
        start_time = time.time()
        
        # Read SQL
        with open(sql_file, 'r') as f:
            sql = f.read()
        
        # Step 1: Parse and analyze
        filter_extractor = FilterExtractor(sql)
        filters = filter_extractor.extract_filters()
        joins = filter_extractor.extract_joins()
        
        schema_extractor = SchemaExtractor(sql)
        tables = schema_extractor.extract_tables()
        
        # Step 2: Build FK relationships from joins
        fk_relations = {}
        for left_t, left_c, right_t, right_c in joins:
            if left_t in tables and right_t in tables:
                # Determine which is FK (usually the longer column name or fact table pattern)
                if left_c.endswith('_sk') and right_c.endswith('_sk'):
                    # Simple heuristic: the one with more prefixes is likely the FK
                    if len(left_c.split('_')) > len(right_c.split('_')):
                        if left_t not in fk_relations:
                            fk_relations[left_t] = {}
                        fk_relations[left_t][left_c.lower()] = (right_t, right_c)
                    else:
                        if right_t not in fk_relations:
                            fk_relations[right_t] = {}
                        fk_relations[right_t][right_c.lower()] = (left_t, left_c)
        
        # Step 3: Determine dimension vs fact tables
        dim_tables = [t for t in tables if t not in fk_relations]
        fact_tables = [t for t in tables if t in fk_relations]
        
        # Step 4: Create schema
        for table_name, schema in tables.items():
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            columns = schema['columns']
            if not columns:
                columns = {'id': {'type': 'INTEGER', 'nullable': False}}
            
            col_defs = []
            pk_col = None
            for col_name, col_info in columns.items():
                col_defs.append(f"{col_name} {col_info['type']}")
                if col_name.lower().endswith('_sk'):
                    pk_col = col_name
            
            create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
            self.conn.execute(create_sql)
            
            # Create index on PK/SK columns for faster joins
            if pk_col:
                self.conn.execute(f"CREATE INDEX idx_{table_name}_{pk_col} ON {table_name}({pk_col})")
        
        # Step 5: Generate data
        generator = FilterAwareGenerator(self.conn)
        
        # Generate dimensions first with filter-aware values
        for table_name in dim_tables:
            table_filters = filters.get(table_name, [])
            generator.generate_dimension_table(
                table_name, tables[table_name], table_filters, row_count=150
            )
        
        # Generate facts with FKs to filter-matched dimension rows
        for table_name in fact_tables:
            generator.generate_fact_table(
                table_name, tables[table_name], 
                fk_relations.get(table_name, {}), 
                row_count=300
            )
        
        # Step 6: Execute query
        try:
            result = self.conn.execute(sql).fetchall()
            actual_rows = len(result)
            elapsed = time.time() - start_time
            
            return {
                'success': True,
                'actual_rows': actual_rows,
                'target_rows': target_rows,
                'elapsed_seconds': round(elapsed, 2),
                'tables': list(tables.keys()),
                'sample': result[:5] if result else []
            }
        except Exception as e:
            elapsed = time.time() - start_time
            return {
                'success': False,
                'error': str(e),
                'elapsed_seconds': round(elapsed, 2)
            }


def main():
    parser = argparse.ArgumentParser(description='Synthetic Data Validation Tool v2')
    parser.add_argument('sql_file', help='SQL file to validate')
    parser.add_argument('--target-rows', type=int, default=1000)
    args = parser.parse_args()
    
    validator = SyntheticValidatorV2()
    result = validator.validate(args.sql_file, args.target_rows)
    
    print(f"\n{'='*50}")
    print("RESULTS")
    print(f"{'='*50}")
    print(f"Time: {result['elapsed_seconds']}s")
    
    if result['success']:
        print(f"✅ Success! Rows: {result['actual_rows']}")
        print(f"Tables: {result['tables']}")
        if result['sample']:
            print(f"\nSample:")
            for row in result['sample']:
                print(f"  {row}")
    else:
        print(f"❌ Failed: {result.get('error')}")
    
    return 0 if result['success'] else 1


if __name__ == '__main__':
    exit(main())
