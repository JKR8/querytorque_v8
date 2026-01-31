"""SQL Remediation Payload Generator.

Creates structured payloads containing everything an LLM needs to fix SQL issues:
- Full SQL query code
- Schema context (tables, columns, joins) - always extracted from query
- Index metadata (enriched from DB when available)
- Issue details with fix patterns
- Query structure from SQLParser

Flow:
1. Always parse SQL and extract tables, columns, joins, CTEs
2. If DB connection provided, validate base tables and enrich with metadata
3. CTEs are always derived from query (they don't exist in information_schema)
"""

from dataclasses import dataclass, field
from typing import Optional, Protocol, Any
from abc import abstractmethod
import json

from ..parsers.sql_parser import SQLParser, QueryGraph


class DBConnection(Protocol):
    """Protocol for database connections used for schema validation."""

    @abstractmethod
    def execute(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        """Execute a query and return results as list of dicts."""
        ...


@dataclass
class SchemaValidationResult:
    """Result of validating extracted schema against database."""
    table_name: str
    exists: bool
    row_count: int = 0
    columns: list[dict] = field(default_factory=list)  # {name, data_type, is_nullable, is_primary_key}
    indexes: list[dict] = field(default_factory=list)  # {name, columns, is_unique}
    error: Optional[str] = None


@dataclass
class SQLTableInfo:
    """Table metadata for schema context."""
    name: str
    row_count: int = 0
    column_count: int = 0
    size_bytes: int = 0
    node_type: str = "base_table"  # base_table, cte, subquery
    validated: bool = False  # True if validated against DB
    exists: Optional[bool] = None  # None=not checked, True=exists, False=not found
    # SOTA additions for cost-based optimization
    indexes: list[str] = field(default_factory=list)  # e.g., ["idx_users_email"]
    primary_key: list[str] = field(default_factory=list)  # e.g., ["id"]


@dataclass
class SQLColumnInfo:
    """Column metadata for schema context."""
    name: str
    table: str
    data_type: str = "unknown"
    is_indexed: bool = False
    is_primary_key: bool = False
    is_foreign_key: bool = False
    cardinality: int = 0


@dataclass
class SQLIndexInfo:
    """Index metadata for optimization recommendations."""
    name: str
    table: str
    columns: list[str] = field(default_factory=list)
    is_unique: bool = False
    is_clustered: bool = False


@dataclass
class SQLForeignKeyInfo:
    """Foreign key metadata for join optimization."""
    from_table: str
    from_column: str
    to_table: str
    to_column: str


@dataclass
class SQLJoinInfo:
    """Join relationship from query analysis."""
    left_table: str
    left_column: str
    right_table: str
    right_column: str
    join_type: str = "INNER"
    operator: str = "="


@dataclass
class SQLRemediationPayload:
    """Complete remediation payload for SQL query."""
    query_name: str
    sql_code: str
    mode: str  # "full_schema" or "extracted"

    # Schema context
    tables: list[SQLTableInfo] = field(default_factory=list)
    columns: list[SQLColumnInfo] = field(default_factory=list)
    joins: list[SQLJoinInfo] = field(default_factory=list)

    # Full schema mode extras (optional)
    indexes: list[SQLIndexInfo] = field(default_factory=list)
    foreign_keys: list[SQLForeignKeyInfo] = field(default_factory=list)

    # Query structure from parser
    query_graph: dict = field(default_factory=dict)

    # Issues detected
    issues: list[dict] = field(default_factory=list)

    # Summary stats
    total_issues: int = 0
    critical_issues: int = 0
    high_issues: int = 0

    def to_dict(self) -> dict:
        """Export as JSON-serializable dict."""
        return {
            "query": self.query_name,
            "mode": self.mode,
            "summary": {
                "total_issues": self.total_issues,
                "critical_issues": self.critical_issues,
                "high_issues": self.high_issues,
            },
            "schema_context": {
                "tables": [
                    {
                        "name": t.name,
                        "row_count": t.row_count,
                        "column_count": t.column_count,
                        "size_bytes": t.size_bytes,
                        "type": t.node_type,
                        "validated": t.validated,
                        "exists": t.exists,
                        "indexes": t.indexes,
                        "primary_key": t.primary_key,
                    }
                    for t in self.tables
                ],
                "columns": [
                    {
                        "name": c.name,
                        "table": c.table,
                        "data_type": c.data_type,
                        "is_indexed": c.is_indexed,
                        "is_primary_key": c.is_primary_key,
                        "is_foreign_key": c.is_foreign_key,
                    }
                    for c in self.columns
                ],
                "joins": [
                    {
                        "left": f"{j.left_table}.{j.left_column}",
                        "right": f"{j.right_table}.{j.right_column}",
                        "type": j.join_type,
                        "operator": j.operator,
                    }
                    for j in self.joins
                ],
                "indexes": [
                    {
                        "name": i.name,
                        "table": i.table,
                        "columns": i.columns,
                        "is_unique": i.is_unique,
                        "is_clustered": i.is_clustered,
                    }
                    for i in self.indexes
                ],
                "foreign_keys": [
                    {
                        "from": f"{fk.from_table}.{fk.from_column}",
                        "to": f"{fk.to_table}.{fk.to_column}",
                    }
                    for fk in self.foreign_keys
                ],
            },
            "query_graph": self.query_graph,
            "sql_code": self.sql_code,
            "issues": self.issues,
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_prompt(self) -> str:
        """Generate a markdown prompt for LLM consumption."""
        lines = [
            f"# SQL Remediation Request",
            f"",
            f"## Query: {self.query_name}",
            f"",
            f"**Mode:** {self.mode.replace('_', ' ').title()}",
            f"",
            f"Please optimize the following SQL query to fix the identified anti-patterns.",
            f"",
        ]

        # Schema context section
        lines.extend([
            f"---",
            f"",
            f"## Schema Context",
            f"",
        ])

        # Tables
        if self.tables:
            lines.extend([
                f"### Tables Referenced",
                f"",
                f"| Table | Type | Rows | Columns |",
                f"|-------|------|------|---------|",
            ])
            for t in sorted(self.tables, key=lambda x: x.row_count, reverse=True):
                row_str = f"{t.row_count:,}" if t.row_count > 0 else "unknown"
                col_str = str(t.column_count) if t.column_count > 0 else "-"
                lines.append(f"| {t.name} | {t.node_type} | {row_str} | {col_str} |")
            lines.append("")

        # Columns
        if self.columns:
            lines.extend([
                f"### Columns Used",
                f"",
                f"| Column | Table | Type | Indexed | Key |",
                f"|--------|-------|------|---------|-----|",
            ])
            for c in self.columns[:30]:  # Limit to 30
                idx_flag = "Yes" if c.is_indexed else ""
                key_flag = "PK" if c.is_primary_key else ("FK" if c.is_foreign_key else "")
                lines.append(f"| {c.name} | {c.table} | {c.data_type} | {idx_flag} | {key_flag} |")
            if len(self.columns) > 30:
                lines.append(f"| ... | ({len(self.columns) - 30} more columns) | | | |")
            lines.append("")

        # Joins
        if self.joins:
            lines.extend([
                f"### Join Relationships",
                f"",
                f"| Left | Right | Type | Operator |",
                f"|------|-------|------|----------|",
            ])
            for j in self.joins:
                lines.append(f"| {j.left_table}.{j.left_column} | {j.right_table}.{j.right_column} | {j.join_type} | {j.operator} |")
            lines.append("")

        # Indexes (if available)
        if self.indexes:
            lines.extend([
                f"### Existing Indexes",
                f"",
                f"| Index | Table | Columns | Unique | Clustered |",
                f"|-------|-------|---------|--------|-----------|",
            ])
            for i in self.indexes:
                cols = ", ".join(i.columns)
                unique = "Yes" if i.is_unique else ""
                clustered = "Yes" if i.is_clustered else ""
                lines.append(f"| {i.name} | {i.table} | {cols} | {unique} | {clustered} |")
            lines.append("")

        # Foreign Keys (if available)
        if self.foreign_keys:
            lines.extend([
                f"### Foreign Keys",
                f"",
            ])
            for fk in self.foreign_keys:
                lines.append(f"- `{fk.from_table}.{fk.from_column}` → `{fk.to_table}.{fk.to_column}`")
            lines.append("")

        # SQL Code
        lines.extend([
            f"---",
            f"",
            f"## Current SQL Code",
            f"",
            f"```sql",
            self.sql_code,
            f"```",
            f"",
        ])

        # Issues
        if self.issues:
            lines.extend([
                f"---",
                f"",
                f"## Issues to Fix",
                f"",
            ])
            for issue in self.issues:
                rule_id = issue.get('rule_id', 'UNKNOWN')
                title = issue.get('title', issue.get('name', 'Issue'))
                severity = issue.get('severity', 'medium')
                line = issue.get('line', '')
                description = issue.get('description', '')
                recommendation = issue.get('recommendation', '')
                code_snippet = issue.get('code_snippet', '')
                why_bad = issue.get('why_bad', '')
                what_good = issue.get('what_good_looks_like', '')

                lines.extend([
                    f"### [{rule_id}] {title}",
                    f"",
                    f"**Severity:** {severity.upper()}",
                ])
                if line:
                    lines.append(f"**Line:** {line}")
                lines.append(f"")
                if description:
                    lines.append(f"**Problem:** {description}")
                    lines.append(f"")
                if code_snippet:
                    lines.extend([
                        f"**Code:**",
                        f"```sql",
                        code_snippet,
                        f"```",
                        f"",
                    ])
                if why_bad:
                    lines.append(f"**Why it's bad:** {why_bad}")
                    lines.append(f"")
                if recommendation:
                    lines.append(f"**Fix:** {recommendation}")
                    lines.append(f"")
                if what_good:
                    lines.extend([
                        f"**Example of correct pattern:**",
                        f"```sql",
                        what_good,
                        f"```",
                        f"",
                    ])
            lines.append("")

        # Instructions
        lines.extend([
            f"---",
            f"",
            f"## Instructions",
            f"",
            f"Please rewrite the SQL query to fix all listed issues:",
            f"",
            f"1. **Use only the columns and tables listed above** - do not invent column names",
            f"2. **Preserve query semantics** - the optimized query must return the same results",
            f"3. **Consider table sizes** when choosing join order (if row counts are known)",
            f"4. **Check for index coverage** - ensure filter/join columns are indexed",
            f"5. **Explain your changes** briefly for each fix",
            f"",
        ])

        if self.mode == "extracted":
            lines.extend([
                f"**Note:** Schema was extracted from query. Column types and indexes are not known.",
                f"Recommendations about indexes are suggestions that should be verified against actual schema.",
                f"",
            ])

        lines.extend([
            f"Return your fix in this format:",
            f"",
            f"```",
            f"## Optimized SQL",
            f"",
            f"```sql",
            f"<your optimized query>",
            f"```",
            f"",
            f"## Changes Made",
            f"- <bullet points explaining each fix>",
            f"",
            f"## Index Recommendations",
            f"- <any indexes that would help if not present>",
            f"",
            f"## Expected Improvement",
            f"- <estimated performance gain, e.g., '2-5x'>",
            f"```",
        ])

        return "\n".join(lines)


class SQLRemediationPayloadGenerator:
    """Generates remediation payloads from SQL queries.

    Flow:
    1. Always parse and extract schema from SQL
    2. Optionally validate/enrich base tables with DB connection
    3. CTEs stay as extracted (not in information_schema)
    """

    def __init__(self, dialect: str = "snowflake"):
        self.dialect = dialect
        self.parser = SQLParser(dialect=dialect)

    def generate(
        self,
        sql: str,
        query_name: str = "query.sql",
        db_connection: Optional[DBConnection] = None,
        issues: Optional[list[dict]] = None,
    ) -> SQLRemediationPayload:
        """Generate remediation payload from SQL query.

        Args:
            sql: The SQL query string
            query_name: Name/filename of the query
            db_connection: Optional DB connection for validating base tables
            issues: Optional list of detected issues

        Returns:
            SQLRemediationPayload ready for LLM consumption
        """
        # Step 1: Always parse and extract from SQL
        graph = self.parser.parse(sql)
        graph_dict = graph.to_dict()

        # Step 2: Extract schema from parsed query
        tables, columns = self._extract_from_graph(graph)
        joins = self._extract_joins(graph)

        # Step 3: If DB connection, validate and enrich base tables
        indexes = []
        foreign_keys = []
        if db_connection:
            mode = "validated"
            tables, columns, indexes, foreign_keys = self._validate_and_enrich(
                db_connection, tables, columns, graph
            )
        else:
            mode = "extracted"

        # Count issues by severity
        issues = issues or []
        critical_count = sum(1 for i in issues if i.get('severity') == 'critical')
        high_count = sum(1 for i in issues if i.get('severity') == 'high')

        return SQLRemediationPayload(
            query_name=query_name,
            sql_code=sql,
            mode=mode,
            tables=tables,
            columns=columns,
            joins=joins,
            indexes=indexes,
            foreign_keys=foreign_keys,
            query_graph=graph_dict,
            issues=issues,
            total_issues=len(issues),
            critical_issues=critical_count,
            high_issues=high_count,
        )

    def _validate_and_enrich(
        self,
        db: DBConnection,
        tables: list[SQLTableInfo],
        columns: list[SQLColumnInfo],
        graph: QueryGraph,
    ) -> tuple[list[SQLTableInfo], list[SQLColumnInfo], list[SQLIndexInfo], list[SQLForeignKeyInfo]]:
        """Validate base tables against DB and enrich with metadata.

        CTEs are skipped - they only exist in the query.
        """
        enriched_tables = []
        enriched_columns = list(columns)  # Start with extracted columns
        indexes = []
        foreign_keys = []

        for table in tables:
            if table.node_type == "cte":
                # CTEs don't exist in DB, keep as extracted
                enriched_tables.append(table)
                continue

            # Validate base table exists and get metadata
            validation = self._validate_table(db, table.name)

            table.validated = True
            table.exists = validation.exists

            if validation.exists:
                table.row_count = validation.row_count
                table.column_count = len(validation.columns)

                # Populate SOTA fields: indexes and primary_key
                table.indexes = [idx['name'] for idx in validation.indexes]
                table.primary_key = [
                    col['name'] for col in validation.columns
                    if col.get('is_primary_key')
                ]

                # Enrich columns with actual types
                for col_info in validation.columns:
                    # Update existing extracted column or add new one
                    existing = next(
                        (c for c in enriched_columns
                         if c.table == table.name and c.name == col_info['name']),
                        None
                    )
                    if existing:
                        existing.data_type = col_info.get('data_type', 'unknown')
                        existing.is_primary_key = col_info.get('is_primary_key', False)
                    else:
                        enriched_columns.append(SQLColumnInfo(
                            name=col_info['name'],
                            table=table.name,
                            data_type=col_info.get('data_type', 'unknown'),
                            is_primary_key=col_info.get('is_primary_key', False),
                        ))

                # Add indexes
                for idx_info in validation.indexes:
                    indexes.append(SQLIndexInfo(
                        name=idx_info['name'],
                        table=table.name,
                        columns=idx_info.get('columns', []),
                        is_unique=idx_info.get('is_unique', False),
                    ))
                    # Mark indexed columns
                    for col_name in idx_info.get('columns', []):
                        for col in enriched_columns:
                            if col.table == table.name and col.name == col_name:
                                col.is_indexed = True

            enriched_tables.append(table)

        return enriched_tables, enriched_columns, indexes, foreign_keys

    def _validate_table(self, db: DBConnection, table_name: str) -> SchemaValidationResult:
        """Validate a single table against information_schema."""
        result = SchemaValidationResult(table_name=table_name, exists=False)

        try:
            # Check if table exists and get row count estimate
            # Using standard information_schema queries (works for most DBs)
            table_query = """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_name = %s
                LIMIT 1
            """
            table_rows = db.execute(table_query, (table_name,))

            if not table_rows:
                return result

            result.exists = True

            # Get columns
            columns_query = """
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """
            col_rows = db.execute(columns_query, (table_name,))

            for row in col_rows:
                result.columns.append({
                    'name': row.get('column_name'),
                    'data_type': row.get('data_type', 'unknown'),
                    'is_nullable': row.get('is_nullable') == 'YES',
                    'is_primary_key': False,  # Will be updated from constraints
                })

            # Get primary key columns
            pk_query = """
                SELECT column_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                WHERE tc.table_name = %s AND tc.constraint_type = 'PRIMARY KEY'
            """
            try:
                pk_rows = db.execute(pk_query, (table_name,))
                pk_columns = {row.get('column_name') for row in pk_rows}
                for col in result.columns:
                    if col['name'] in pk_columns:
                        col['is_primary_key'] = True
            except Exception:
                pass  # PK query may not work on all DBs

            # Get indexes (DB-specific, may fail)
            # This is a simplified approach - real implementation would vary by dialect
            try:
                idx_query = """
                    SELECT
                        index_name,
                        column_name,
                        non_unique
                    FROM information_schema.statistics
                    WHERE table_name = %s
                    ORDER BY index_name, seq_in_index
                """
                idx_rows = db.execute(idx_query, (table_name,))

                # Group by index name
                idx_map = {}
                for row in idx_rows:
                    idx_name = row.get('index_name')
                    if idx_name not in idx_map:
                        idx_map[idx_name] = {
                            'name': idx_name,
                            'columns': [],
                            'is_unique': not row.get('non_unique', True),
                        }
                    idx_map[idx_name]['columns'].append(row.get('column_name'))

                result.indexes = list(idx_map.values())
            except Exception:
                pass  # Index query may not work on all DBs

        except Exception as e:
            result.error = str(e)

        return result

    def _extract_from_graph(
        self,
        graph: QueryGraph,
    ) -> tuple[list[SQLTableInfo], list[SQLColumnInfo]]:
        """Extract schema context from parsed query graph.

        Returns tables and columns. CTEs are marked as node_type="cte".
        """
        tables = []
        columns = []

        # Extract tables from nodes
        for name, node in graph.nodes.items():
            if name == "__output__":
                continue
            tables.append(SQLTableInfo(
                name=name,
                node_type=node.node_type,
                column_count=len(node.columns_output),
            ))

        # Extract columns from graph
        seen_columns = set()
        for node in graph.nodes.values():
            for col_ref in node.columns_used:
                if col_ref in seen_columns:
                    continue
                seen_columns.add(col_ref)

                # Parse "table.column" format
                if '.' in col_ref:
                    parts = col_ref.split('.', 1)
                    table = parts[0]
                    col = parts[1]
                else:
                    table = ""
                    col = col_ref

                columns.append(SQLColumnInfo(
                    name=col,
                    table=table,
                ))

        return tables, columns

    def _extract_joins(self, graph: QueryGraph) -> list[SQLJoinInfo]:
        """Extract join info from query graph."""
        joins = []
        for join in graph.joins:
            joins.append(SQLJoinInfo(
                left_table=join.left_table,
                left_column=join.left_column,
                right_table=join.right_table,
                right_column=join.right_column,
                join_type=join.join_type,
                operator=join.operator,
            ))
        return joins


def generate_sql_remediation_payload(
    sql: str,
    query_name: str = "query.sql",
    db_connection: Optional[DBConnection] = None,
    issues: Optional[list[dict]] = None,
    dialect: str = "snowflake",
) -> dict:
    """Convenience function to generate SQL remediation payload.

    Args:
        sql: The SQL query string
        query_name: Name/filename of the query
        db_connection: Optional DB connection for validating base tables
        issues: Optional list of detected issues
        dialect: SQL dialect for parsing

    Returns:
        Dictionary representation of the payload
    """
    generator = SQLRemediationPayloadGenerator(dialect=dialect)
    payload = generator.generate(sql, query_name, db_connection, issues)
    return payload.to_dict()


def generate_sql_remediation_prompt(
    sql: str,
    query_name: str = "query.sql",
    db_connection: Optional[DBConnection] = None,
    issues: Optional[list[dict]] = None,
    dialect: str = "snowflake",
) -> str:
    """Convenience function to generate SQL remediation prompt.

    Args:
        sql: The SQL query string
        query_name: Name/filename of the query
        db_connection: Optional DB connection for validating base tables
        issues: Optional list of detected issues
        dialect: SQL dialect for parsing

    Returns:
        Markdown prompt for LLM consumption
    """
    generator = SQLRemediationPayloadGenerator(dialect=dialect)
    payload = generator.generate(sql, query_name, db_connection, issues)
    return payload.to_prompt()
